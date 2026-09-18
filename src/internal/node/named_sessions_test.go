package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type sessionManagerFixture struct {
	sync.Mutex
	sessions map[string]herdrsession.Session
	starts   int
	queries  int
	err      error
}

func (m *sessionManagerFixture) List(context.Context) ([]herdrsession.Session, error) {
	m.Lock()
	defer m.Unlock()
	var values []herdrsession.Session
	for _, value := range m.sessions {
		values = append(values, value)
	}
	return values, m.err
}

func (m *sessionManagerFixture) Status(_ context.Context, name string) (herdrsession.Session, error) {
	m.Lock()
	defer m.Unlock()
	m.queries++
	return m.sessions[name], m.err
}

func (m *sessionManagerFixture) Ensure(_ context.Context, name string) (herdrsession.Session, error) {
	m.Lock()
	defer m.Unlock()
	m.starts++
	return m.sessions[name], m.err
}

func (m *sessionManagerFixture) replace(name string) {
	m.Lock()
	defer m.Unlock()
	value := m.sessions[name]
	value.Incarnation = strings.Repeat("c", 64)
	m.sessions[name] = value
}

func namedHandler(t *testing.T) (*commandHandler, *sessionManagerFixture) {
	t.Helper()
	manager := &sessionManagerFixture{sessions: map[string]herdrsession.Session{
		"one": {Name: "one", Status: "ready", Incarnation: strings.Repeat("a", 64), SocketPath: "PRIVATE-one"},
		"two": {Name: "two", Status: "ready", Incarnation: strings.Repeat("b", 64), SocketPath: "PRIVATE-two"},
	}}
	h := &commandHandler{journal: testJournal(t), nodeID: "test-node",
		sessions: manager, sessionsNegotiated: true, agentNegotiated: true, verifyCoordinator: allowCoordinator}
	if !h.sessionsReady() || h.agentReady() {
		t.Fatal("session manager must be ready independently of the absent default session")
	}
	return h, manager
}

func TestNamedAgentRoutesIdentityAndReplaysOriginalReceipt(t *testing.T) {
	h, manager := namedHandler(t)
	calls := 0
	h.controlAgent = func(ctx context.Context, config herdr.Config, command *pb.AgentControl) (*pb.AgentControlResult, error) {
		calls++
		if config.SocketPath != "PRIVATE-"+command.Target.SessionName || config.CheckSession == nil {
			t.Fatal("selected endpoint or final session check missing")
		}
		if err := config.CheckSession(ctx); err != nil {
			return nil, err
		}
		return &pb.AgentControlResult{Target: proto.Clone(command.Target).(*pb.AgentTarget), ObservedStatus: "working"}, nil
	}
	for _, name := range []string{"one", "two"} {
		command := agentCommand()
		command.AgentControl.Target.SessionName = name
		command.AgentControl.Target.SessionIncarnation = manager.sessions[name].Incarnation
		first, err := h.handle(context.Background(), command, time.Now())
		if err != nil || first.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
			t.Fatalf("named effect: %v %v", first, err)
		}
		manager.replace(name)
		replay, err := h.handle(context.Background(), command, time.Now())
		if err != nil || !proto.Equal(first, replay) {
			t.Fatalf("replacement changed exact durable replay: %v %v", replay, err)
		}
		next := proto.Clone(command).(*pb.Command)
		next.CommandId, next.IdempotencyKey = protocol.NewCommandID(), protocol.NewCommandID()
		rejected, err := h.handle(context.Background(), next, time.Now())
		if err != nil || rejected.Detail != "session_replaced" {
			t.Fatalf("reused pane identity escaped session fence: %v %v", rejected, err)
		}
		conflict := proto.Clone(command).(*pb.Command)
		conflict.AgentControl.Target.SessionIncarnation = manager.sessions[name].Incarnation
		if _, err := h.handle(context.Background(), conflict, time.Now()); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("retry mutated the original session pin: %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("expected one effect per session, got %d", calls)
	}
}

func TestNamedWorkspaceCapturesActualIncarnationAndDetectsReplacement(t *testing.T) {
	for _, restart := range []bool{false, true} {
		h, manager := namedHandler(t)
		h.workspaceNegotiated, h.workspacePolicy = true, workspacePolicy(t)
		h.ensureWorkspace = func(ctx context.Context, config herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			if config.SocketPath != "PRIVATE-one" {
				t.Fatal("workspace used configured default instead of selected session")
			}
			if restart {
				manager.replace("one")
				return nil, config.CheckSession(ctx)
			}
			return workspaceSuccess(binding, true), nil
		}
		command := workspaceCommand()
		command.WorkspaceEnsure.SessionName = "one"
		result, err := h.handle(context.Background(), command, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if restart {
			if result.Detail != "session_replaced" || result.WorkspaceEnsure != nil {
				t.Fatalf("replacement not rejected before workspace effect: %v", result)
			}
		} else if result.WorkspaceEnsure.GetSessionIncarnation() != strings.Repeat("a", 64) || result.WorkspaceEnsure.GetSessionName() != "one" {
			t.Fatalf("workspace receipt omitted resolved actual session: %v", result)
		}
	}
}

func TestNamedQueryPinsDiscoveryAndDropsReplacementOutput(t *testing.T) {
	for _, restart := range []bool{false, true} {
		h, manager := namedHandler(t)
		ctx, cancel := context.WithCancel(context.Background())
		q := newAgentQueries(ctx, Options{InstanceID: "test-node", QueryAgent: func(_ context.Context, config herdr.Config, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			if config.SocketPath != "PRIVATE-two" {
				t.Fatal("query selected wrong native session")
			}
			if restart {
				manager.replace("two")
			}
			return &pb.AgentQueryResult{Agent: &pb.AgentView{Target: &pb.AgentTarget{PaneId: "p1", TerminalId: "t1"},
				WorkspaceId: "w1", TabId: "tab1", Provider: "copilot", Status: "idle"}}, nil
		}}, h)
		request := &pb.AgentQueryRequest{NodeInstanceId: "test-node", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_GET,
			Target: &pb.AgentTarget{PaneId: "p1", SessionName: "two"}, TimeoutMs: 1000}
		err := q.start(&pb.AgentQuery{QueryId: protocol.NewCommandID(), Request: request, ExpiresAt: timestamppb.New(time.Now().Add(time.Second))})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		result := <-q.results
		cancel()
		q.join()
		if restart {
			if result.ErrorCode != "session_replaced" || result.Agent != nil {
				t.Fatalf("replaced session leaked a rebound agent: %v", result)
			}
		} else if result.Agent.GetTarget().GetSessionIncarnation() != strings.Repeat("b", 64) || result.Agent.Target.SessionName != "two" {
			t.Fatalf("discovery target was not fully pinned: %v", result)
		}
	}
}

func TestCustomDefaultCannotLaunchInstalledDefault(t *testing.T) {
	h, manager := namedHandler(t)
	h.herdrConfig.SocketPath = "PRIVATE-configured"
	manager.sessions["default"] = herdrsession.Session{Name: "default", Status: "stopped", SocketPath: "PRIVATE-installed"}
	command := probe()
	command.CommandType, command.SessionEnsure = protocol.SessionEnsureCommandType, &pb.SessionEnsure{Name: "default"}
	result, err := h.handle(context.Background(), command, time.Now())
	if err != nil || result.Detail != "session_default_unmanaged" || manager.starts != 0 {
		t.Fatalf("custom default was substituted: %v %v starts=%d", result, err, manager.starts)
	}
	config, _, err := h.resolveSession(context.Background(), "", "")
	if err != nil || config.SocketPath != "PRIVATE-configured" {
		t.Fatal("empty selection did not preserve configured endpoint")
	}
}

func TestSessionInventoryDiscoveryFailureAndPublishCancellation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		h, manager := namedHandler(t)
		manager.sessions = map[string]herdrsession.Session{"stopped": {Name: "stopped", Status: "stopped", SocketPath: "PRIVATE-path"}}
		if fail {
			manager.err = herdrsession.ErrUnavailable
		}
		done := make(chan error, 1)
		stopped := errors.New("publisher stopped")
		go func() {
			done <- h.observeSessions(context.Background(), func(inventory *pb.SessionInventory) error {
				if fail && inventory.ErrorCode != "session_manager_unavailable" {
					t.Error("discovery error was not explicit")
				}
				data, err := protojson.Marshal(inventory)
				if err != nil || strings.Contains(string(data), "PRIVATE") {
					t.Error("session inventory leaked endpoint paths")
				}
				return stopped
			})
		}()
		select {
		case err := <-done:
			if !errors.Is(err, stopped) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("failed publication did not cancel and join discovery")
		}
	}
}
