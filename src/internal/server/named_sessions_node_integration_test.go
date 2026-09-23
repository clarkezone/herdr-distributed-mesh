package server

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type namedNodeManager struct {
	mu       sync.Mutex
	sessions map[string]herdrsession.Session
	listed   []herdrsession.Session
	ensure   func(context.Context, string) (herdrsession.Session, error)
	calls    atomic.Int32
}

func (m *namedNodeManager) List(ctx context.Context) ([]herdrsession.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listed != nil {
		return append([]herdrsession.Session(nil), m.listed...), ctx.Err()
	}
	values := make([]herdrsession.Session, 0, len(m.sessions))
	for _, value := range m.sessions {
		values = append(values, value)
	}
	return values, ctx.Err()
}

func (m *namedNodeManager) freezeInventory(freeze bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listed = nil
	if freeze {
		m.listed = make([]herdrsession.Session, 0, len(m.sessions))
		for _, value := range m.sessions {
			m.listed = append(m.listed, value)
		}
	}
}

func (m *namedNodeManager) Status(ctx context.Context, name string) (herdrsession.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return herdrsession.Session{}, err
	}
	if value, ok := m.sessions[name]; ok {
		return value, nil
	}
	return herdrsession.Session{}, herdrsession.ErrUnavailable
}

func (m *namedNodeManager) put(value herdrsession.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions == nil {
		m.sessions = make(map[string]herdrsession.Session)
	}
	m.sessions[value.Name] = value
}

func (m *namedNodeManager) Ensure(ctx context.Context, name string) (herdrsession.Session, error) {
	m.calls.Add(1)
	if value, err := m.Status(ctx, name); err == nil && value.Status == "ready" {
		return value, nil
	}
	if m.ensure == nil {
		return herdrsession.Session{}, herdrsession.ErrStart
	}
	value, err := m.ensure(ctx, name)
	if err == nil {
		m.put(value)
	}
	return value, err
}

type namedNodeRun struct {
	client *countedNodeClient
	acks   chan *pb.CommandAck
	done   chan struct{}
	err    error
	stop   func()
	checks atomic.Int32
}

type namedNodeClient struct {
	*countedNodeClient
	acks chan *pb.CommandAck
}

type namedNodeStream struct {
	grpc.BidiStreamingClient[pb.NodeEnvelope, pb.NodeEnvelope]
	acks chan *pb.CommandAck
}

func (c *namedNodeClient) Connect(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pb.NodeEnvelope, pb.NodeEnvelope], error) {
	stream, err := c.countedNodeClient.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &namedNodeStream{BidiStreamingClient: stream, acks: c.acks}, nil
}

func (s *namedNodeStream) Recv() (*pb.NodeEnvelope, error) {
	envelope, err := s.BidiStreamingClient.Recv()
	if ack := envelope.GetCommandAck(); ack != nil {
		select {
		case s.acks <- proto.Clone(ack).(*pb.CommandAck):
		default:
		}
	}
	return envelope, err
}

func startNamedNode(t *testing.T, h *commandHarness, journal string, manager node.SessionManager, drop bool, configure func(*node.Options)) *namedNodeRun {
	t.Helper()
	run := &namedNodeRun{client: &countedNodeClient{NodeControlClient: pb.NewNodeControlClient(h.connection)},
		done: make(chan struct{}), acks: make(chan *pb.CommandAck, 64)}
	run.client.dropResult.Store(drop)
	options := node.Options{
		InstanceID: "node-1", RequiredServerTag: node.DefaultRequiredServerTag,
		CommandJournalPath: journal, ManagedProjects: projects.NewManaged("node-1"),
		SessionManager: manager, HeartbeatInterval: 20 * time.Millisecond,
		VerifyServerPeer: func(ctx context.Context, address string) error {
			if address != "bufconn" {
				return errors.New("wrong actual named-session coordinator peer")
			}
			run.checks.Add(1)
			return ctx.Err()
		},
	}
	if configure != nil {
		configure(&options)
	}
	if options.HerdrSocket != "" {
		t.Fatal("named-session fixture must not configure a default Herdr endpoint")
	}
	ctx, cancel := context.WithCancel(commandPeer(context.Background(), "node"))
	go func() {
		defer close(run.done)
		_, run.err = node.RunSession(ctx, &namedNodeClient{countedNodeClient: run.client, acks: run.acks}, options)
	}()
	var once sync.Once
	run.stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-run.done:
			case <-time.After(5 * time.Second):
				t.Error("named-session node did not join its workers")
			}
		})
	}
	t.Cleanup(run.stop)
	return run
}

func namedNodeContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitNamedNode(t *testing.T, h *commandHarness, run *namedNodeRun, predicate func(*pb.NodeView) bool) *pb.NodeView {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 6*time.Second)
	defer cancel()
	var last *pb.NodeView
	for {
		list, err := pb.NewFleetClient(h.connection).ListNodes(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Nodes) == 1 {
			last = list.Nodes[0]
			if predicate(last) {
				return last
			}
		}
		select {
		case <-run.done:
			t.Fatalf("node exited before readiness: %v; last=%v", run.err, last)
		case <-ctx.Done():
			t.Fatalf("named-session readiness did not arrive: %v", last)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertNoNamedDefault(t *testing.T, view *pb.NodeView) {
	t.Helper()
	if view.Herdr.GetStatus() != "disabled" || len(view.Herdr.Workspaces) != 0 || view.WorkspaceReady || view.WorktreeReady || view.AgentReady {
		t.Fatalf("named-session node asserted configured-default state/readiness: %v", view)
	}
}

func namedEnsureRequest(key, name string) *pb.SubmitCommandRequest {
	return &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: key,
		CommandType: protocol.SessionEnsureCommandType, SessionEnsure: &pb.SessionEnsure{Name: name},
		Ttl: durationpb.New(5 * time.Second)}
}

func submitNamedNode(t *testing.T, h *commandHarness, request *pb.SubmitCommandRequest) *pb.CommandRecord {
	t.Helper()
	record, err := pb.NewFleetClient(h.connection).SubmitCommand(namedNodeContext(t), request)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestNamedSessionsNodeEnsureFailuresAndMissingManager(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status pb.CommandStatus
		detail string
	}{
		{"start-failed", herdrsession.ErrStart, pb.CommandStatus_COMMAND_STATUS_REJECTED, "session_start_failed"},
		{"starting", herdrsession.ErrStarting, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "startup_uncertain"},
		{"unavailable", herdrsession.ErrUnavailable, pb.CommandStatus_COMMAND_STATUS_REJECTED, "session_manager_unavailable"},
		{"unsupported", herdrsession.ErrUnsupported, pb.CommandStatus_COMMAND_STATUS_REJECTED, "unsupported_protocol"},
		{"capacity", herdrsession.ErrCapacity, pb.CommandStatus_COMMAND_STATUS_REJECTED, "session_capacity"},
		{"startup-deadline", context.DeadlineExceeded, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "startup_uncertain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := managedTestRoot(t)
			h := newCommandHarness(t, filepath.Join(root, "server"))
			manager := &namedNodeManager{ensure: func(ctx context.Context, _ string) (herdrsession.Session, error) {
				if test.err == context.DeadlineExceeded {
					<-ctx.Done()
					return herdrsession.Session{}, ctx.Err()
				}
				return herdrsession.Session{}, test.err
			}}
			run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), manager, false, nil)
			view := waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.CommandReady && v.SessionsReady })
			assertNoNamedDefault(t, view)
			request := namedEnsureRequest("failure", "build")
			if test.err == context.DeadlineExceeded {
				request.Ttl = durationpb.New(500 * time.Millisecond)
			}
			first := submitNamedNode(t, h, request)
			result := awaitCommand(t, h, first.Command.CommandId, test.status)
			if result.Detail != test.detail || result.SessionEnsure != nil || run.checks.Load() == 0 {
				t.Fatalf("wrong verified ensure failure: %v; peer checks=%d", result, run.checks.Load())
			}
			retry := submitNamedNode(t, h, request)
			if !proto.Equal(result, retry) || manager.calls.Load() != 1 {
				t.Fatalf("failure retry repeated ensure or changed journaled outcome: %v", retry)
			}
		})
	}
	t.Run("missing-manager", func(t *testing.T) {
		root := managedTestRoot(t)
		h := newCommandHarness(t, filepath.Join(root, "server"))
		run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), nil, false, func(options *node.Options) {
			options.ManagedProjects = nil
		})
		view := waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.CommandReady })
		assertNoNamedDefault(t, view)
		if view.SessionsReady {
			t.Fatal("missing manager was advertised ready")
		}
		client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
		list, err := client.ListSessions(ctx, &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
		if err != nil || list.ErrorCode != "session_manager_unavailable" {
			t.Fatalf("missing manager list: %v %v", list, err)
		}
		if _, err := client.SubmitCommand(ctx, namedEnsureRequest("missing", "build")); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("missing manager admitted ensure: %v", err)
		}
		if run.client.count.Load() != 0 {
			t.Fatal("unavailable ensure reached node")
		}
	})
}

func TestNamedSessionsNodeCanceledEnsureReplaysUncertainOutcome(t *testing.T) {
	root := managedTestRoot(t)
	serverRoot, journal := filepath.Join(root, "server"), filepath.Join(root, "node", "journal.db")
	h := newCommandHarness(t, serverRoot)
	started, canceled := make(chan struct{}), make(chan struct{})
	manager := &namedNodeManager{ensure: func(ctx context.Context, _ string) (herdrsession.Session, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return herdrsession.Session{}, ctx.Err()
	}}
	run := startNamedNode(t, h, journal, manager, false, nil)
	waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.SessionsReady })
	request := namedEnsureRequest("cancel-start", "build")
	first := submitNamedNode(t, h, request)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("ensure never entered native manager")
	}
	run.stop()
	select {
	case <-canceled:
	default:
		t.Fatal("session shutdown did not cancel manager ensure")
	}
	cache, err := state.OpenNodeJournal(context.Background(), journal, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	recoverErr := cache.Recover(context.Background())
	pending, readErr := cache.PendingResults(context.Background())
	closeErr := cache.Close()
	if recoverErr != nil || readErr != nil || closeErr != nil || len(pending) != 1 ||
		pending[0].CommandId != first.Command.CommandId || pending[0].Status != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE ||
		pending[0].Detail != "startup_uncertain" {
		t.Fatalf("shutdown did not preserve recoverable uncertain startup: %v recover=%v read=%v close=%v", pending, recoverErr, readErr, closeErr)
	}
	h.stop()
	h = newCommandHarness(t, serverRoot)
	run = startNamedNode(t, h, journal, manager, false, nil)
	waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.SessionsReady })
	select {
	case ack := <-run.acks:
		if ack.CommandId != first.Command.CommandId || ack.Status != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE {
			t.Fatalf("wrong replay acknowledgement: %v", ack)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovered canceled ensure was not replayed and acknowledged")
	}
	// Acknowledgement must not discard the native startup uncertainty in favor
	// of the coordinator's earlier, generic disconnect fallback.
	ctx := namedNodeContext(t)
	client := pb.NewFleetClient(h.connection)
	result, err := client.GetCommand(ctx, &pb.GetCommandRequest{CommandId: first.Command.CommandId})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE || result.Detail != "startup_uncertain" {
		t.Errorf("acknowledged canceled ensure replay lost node journal detail: node=%v coordinator=%v", pending[0], result)
	}
	if retry := submitNamedNode(t, h, request); !proto.Equal(retry, result) || manager.calls.Load() != 1 || run.client.count.Load() != 0 {
		t.Fatalf("uncertain start was reexecuted by replay/retry: %v", retry)
	}
}

func namedIncarnation(letter string) string { return strings.Repeat(letter, 64) }
