//go:build windows

package server

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestNamedAgentConcurrentClientsThenCrossClientReadStop(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	h := newCommandHarness(t, filepath.Join(root, "server"))
	fleet := pb.NewFleetClient(h.connection)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	firstCtx, secondCtx := commandPeer(ctx, "client"), commandPeer(ctx, "client2")
	if _, err := fleet.RegisterProject(firstCtx, &pb.RegisterProjectRequest{NodeInstanceId: "node-1",
		ProjectId: "AgentFlow", CheckoutPath: checkout}); err != nil {
		t.Fatal(err)
	}
	fake := newFakeWorkspaceHerdr(t, checkout, false, false)
	manager := &namedNodeManager{}
	manager.put(herdrsession.Session{Name: "main", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: fake.path})
	var starts, reads, stops atomic.Int32
	run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), manager, false, func(options *node.Options) {
		options.EnableAgentControl = true
		options.ResolveLifecycleWorkspace = func(ctx context.Context, config herdr.Config, _ string, _ projects.Binding) (string, error) {
			return checkout, config.CheckSession(ctx)
		}
		options.StartAgent = func(ctx context.Context, config herdr.Config, _ herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
			starts.Add(1)
			value := lifecycleFakeInitial()
			for _, stage := range []herdr.LifecycleStage{herdr.LifecycleCreatePane, herdr.LifecycleLaunch, herdr.LifecyclePrompt} {
				for _, before := range []bool{true, false} {
					if err := lifecycleFakeEvent(ctx, config, hook, &value, stage, before); err != nil {
						return value, err
					}
				}
			}
			return value, nil
		}
		options.QueryAgent = func(ctx context.Context, config herdr.Config, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			reads.Add(1)
			return &pb.AgentQueryResult{Agent: &pb.AgentView{Target: proto.Clone(request.Target).(*pb.AgentTarget),
				WorkspaceId: "w1", TabId: "tab:2", Provider: "copilot", Status: "idle", InteractiveReady: true},
				Text: "hello"}, config.CheckSession(ctx)
		}
		options.StopAgent = func(ctx context.Context, config herdr.Config, request herdr.StopAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
			stops.Add(1)
			value := lifecycleFakeInitial()
			value.Handle = request.Handle
			for _, before := range []bool{true, false} {
				if err := lifecycleFakeEvent(ctx, config, hook, &value, herdr.LifecycleClosePane, before); err != nil {
					return value, err
				}
			}
			return value, nil
		}
	})
	waitNamedProject(t, h, run)
	waitNamedInventory(t, h, run, map[string]string{"main": namedIncarnation("a")})
	ready := make(chan struct{})
	type admission struct {
		record *pb.CommandRecord
		err    error
	}
	admissions := make(chan admission, 2)
	var group sync.WaitGroup
	for i, peerCtx := range []context.Context{firstCtx, secondCtx} {
		group.Add(1)
		go func() {
			defer group.Done()
			<-ready
			record, err := fleet.SubmitCommand(peerCtx, &pb.SubmitCommandRequest{NodeInstanceId: "node-1",
				IdempotencyKey: []string{"desktop-start", "laptop-start"}[i], CommandType: protocol.AgentStartCommandType,
				AgentStart: &pb.AgentStart{ProjectId: "AgentFlow", WorkspaceId: "w1", Name: "smoke", Provider: "copilot",
					SessionName: "main", InitialPrompt: "private task text"}})
			admissions <- admission{record, err}
		}()
	}
	close(ready)
	group.Wait()
	close(admissions)
	var winner *pb.CommandRecord
	for result := range admissions {
		if result.err == nil {
			if winner != nil {
				t.Fatal("two clients admitted the same agent name")
			}
			winner = result.record
		} else if status.Code(result.err) != codes.FailedPrecondition {
			t.Fatalf("unexpected name collision result: %v", result.err)
		}
	}
	if winner == nil {
		t.Fatal("neither client admitted a start")
	}
	for !protocol.IsTerminalCommand(winner.Status) {
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
		var err error
		winner, err = fleet.GetCommand(secondCtx, &pb.GetCommandRequest{CommandId: winner.Command.CommandId})
		if err != nil {
			t.Fatal(err)
		}
	}
	if winner.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED || starts.Load() != 1 {
		t.Fatalf("expected one real launch, got %d: %v", starts.Load(), winner)
	}
	observerCtx := secondCtx
	if winner.Command.Actor.ActorId == "client-2" {
		observerCtx = firstCtx
	}
	named, err := fleet.ResolveNamedAgent(observerCtx, &pb.ResolveNamedAgentRequest{NodeInstanceId: "node-1", Name: "smoke", SessionName: "main"})
	if err != nil || !named.GetInitialPromptPresent() || strings.Contains(named.String(), "private task text") ||
		!proto.Equal(named.GetRecord().GetAgentLifecycle().GetHandle(), winner.AgentLifecycle.Handle) {
		t.Fatalf("cross-client redacted resolution lost original handle: %v %v", named, err)
	}
	handle := named.Record.AgentLifecycle.Handle
	options := control.Options{FleetClient: fleet, RequiredServerTag: "tag:server", Output: io.Discard}
	if err := control.Agent(observerCtx, options, &pb.AgentQueryRequest{NodeInstanceId: "node-1", Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ,
		Target: handle.Target, Lines: 10}, nil, "", protocol.DefaultCommandTTL); err != nil {
		t.Fatal(err)
	}
	if err := control.StopAgent(observerCtx, options, "node-1", "cross-client-stop", &pb.AgentStop{
		Target: handle.Target, WorkspaceId: handle.WorkspaceId, TabId: handle.TabId, Provider: handle.Provider}, protocol.DefaultCommandTTL); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 || stops.Load() != 1 || starts.Load() != 1 {
		t.Fatalf("cross-client read/stop changed operation counts: start=%d read=%d stop=%d", starts.Load(), reads.Load(), stops.Load())
	}
}
