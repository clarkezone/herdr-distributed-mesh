//go:build windows

package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func waitNamedProject(t *testing.T, h *commandHarness, run *namedNodeRun) *pb.ProjectRecord {
	t.Helper()
	client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	for {
		record, err := client.GetProject(ctx, &pb.GetProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow"})
		if err != nil {
			t.Fatal(err)
		}
		if record.Readiness == "applied" && record.Applied.GetGeneration() == 1 {
			view := waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.CommandReady && v.SessionsReady })
			assertNoNamedDefault(t, view)
			return record
		}
		select {
		case <-run.done:
			t.Fatalf("node ended before project ACK: %v", run.err)
		case <-ctx.Done():
			t.Fatalf("project ACK depended on a running Herdr: %v", record)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitNamedInventory(t *testing.T, h *commandHarness, run *namedNodeRun, identities map[string]string) *pb.NodeView {
	t.Helper()
	view := waitNamedNode(t, h, run, func(v *pb.NodeView) bool {
		if !v.SessionsReady || len(v.Sessions) != len(identities) {
			return false
		}
		for name, incarnation := range identities {
			session := findSession(v, name)
			if session == nil || session.Incarnation != incarnation || session.Status != "ready" || session.Herdr.GetStatus() != "ready" {
				return false
			}
		}
		return true
	})
	assertNoNamedDefault(t, view)
	list, err := pb.NewFleetClient(h.connection).ListSessions(namedNodeContext(t), &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
	if err != nil || list.ErrorCode != "" || len(list.Sessions) != len(identities) {
		t.Fatalf("live named sessions not listed: %v %v", list, err)
	}
	for _, session := range list.Sessions {
		if identities[session.Name] != session.Incarnation {
			t.Fatalf("list lost native incarnation: %v", list)
		}
	}
	raw, err := protojson.Marshal(list)
	if err != nil || strings.Contains(string(raw), `\\.\pipe`) || strings.Contains(string(raw), "socketPath") {
		t.Fatalf("list leaked native endpoint: %s %v", raw, err)
	}
	return view
}

func TestNamedSessionsNodeBootstrapAndBothJournalReplay(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-result=%t", drop), func(t *testing.T) {
			root := managedTestRoot(t)
			checkout := managedCheckout(t, root)
			serverRoot, journal := filepath.Join(root, "server"), filepath.Join(root, "node", "journal.db")
			h := newCommandHarness(t, serverRoot)
			client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
			project, err := client.RegisterProject(ctx, &pb.RegisterProjectRequest{
				NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout})
			if err != nil || project.Readiness != "offline" {
				t.Fatalf("offline project registration: %v %v", project, err)
			}
			var starts atomic.Int32
			manager := &namedNodeManager{ensure: func(_ context.Context, name string) (herdrsession.Session, error) {
				starts.Add(1)
				fake := newFakeWorkspaceHerdr(t, checkout, false, false)
				return herdrsession.Session{Name: name, Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: fake.path}, nil
			}}
			run := startNamedNode(t, h, journal, manager, drop, nil)
			project = waitNamedProject(t, h, run)
			if project.Applied.CheckoutPath != checkout || project.Applied.WorktreeRoot != checkout+"-worktrees" || starts.Load() != 0 {
				t.Fatalf("project bootstrap required a native server or lost binding: %v; starts=%d", project, starts.Load())
			}
			waitNamedNode(t, h, run, func(v *pb.NodeView) bool {
				return v.SessionsReceivedAt != nil && len(v.Sessions) == 0 && v.SessionsReady
			})
			list, err := client.ListSessions(ctx, &pb.ListSessionsRequest{NodeInstanceId: "node-1"})
			if err != nil || len(list.Sessions) != 0 || list.ErrorCode != "" {
				t.Fatalf("empty manager inventory not available before first start: %v %v", list, err)
			}
			unauthorized, stop := context.WithTimeout(commandPeer(context.Background(), "denied"), 3*time.Second)
			_, err = client.SubmitCommand(unauthorized, namedEnsureRequest("denied", "build"))
			stop()
			if status.Code(err) != codes.PermissionDenied || manager.calls.Load() != 0 {
				t.Fatalf("unauthorized ensure reached manager: %v", err)
			}
			request := namedEnsureRequest("ensure-build", "build")
			first := submitNamedNode(t, h, request)
			if drop {
				select {
				case <-run.done:
					if run.err == nil {
						t.Fatal("result-loss fault did not end stream")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("journaled result loss did not end node")
				}
				awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
			} else {
				result := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
				if result.SessionEnsure.GetIncarnation() != namedIncarnation("a") {
					t.Fatalf("ensure lost actual native identity: %v", result)
				}
			}
			if manager.calls.Load() != 1 || starts.Load() != 1 || run.client.count.Load() != 1 || run.checks.Load() < 2 {
				t.Fatalf("unexpected effect/dispatch/verification counts: ensure=%d starts=%d dispatch=%d checks=%d",
					manager.calls.Load(), starts.Load(), run.client.count.Load(), run.checks.Load())
			}
			run.stop()
			h.stop()
			h = newCommandHarness(t, serverRoot)
			run = startNamedNode(t, h, journal, manager, false, nil)
			waitNamedProject(t, h, run)
			waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a")})
			completed := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			if completed.SessionEnsure.GetName() != "build" || completed.SessionEnsure.GetIncarnation() != namedIncarnation("a") {
				t.Fatalf("node journal replay lost typed result: %v", completed)
			}
			if retry := submitNamedNode(t, h, request); !proto.Equal(retry, completed) || manager.calls.Load() != 1 || run.client.count.Load() != 0 {
				t.Fatalf("both-journal restart changed same-key outcome or repeated effect: %v", retry)
			}
			fresh := submitNamedNode(t, h, namedEnsureRequest("reensure-build", "build"))
			reensure := awaitCommand(t, h, fresh.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
			if !proto.Equal(reensure.SessionEnsure, completed.SessionEnsure) || manager.calls.Load() != 2 || starts.Load() != 1 {
				t.Fatalf("fresh-key ensure did not reuse native session: %v", reensure)
			}
		})
	}
}

func namedAgentRequest(name, incarnation string, kind pb.AgentQueryKind) *pb.AgentQueryRequest {
	request := agentRequest(kind)
	request.Target.SessionName, request.Target.SessionIncarnation = name, incarnation
	return request
}

func namedAgentControl(key, name, incarnation string, action pb.AgentControlAction) *pb.SubmitCommandRequest {
	control := &pb.AgentControl{Action: action, Target: namedAgentRequest(name, incarnation, pb.AgentQueryKind_AGENT_QUERY_KIND_GET).Target}
	if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT {
		control.Keys = []string{"enter"}
	}
	return &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: key, CommandType: protocol.AgentControlCommandType, AgentControl: control}
}

func TestNamedSessionsNodeRoutesDuplicateIDsAndFencesRestart(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	h := newCommandHarness(t, filepath.Join(root, "server"))
	client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	if _, err := client.RegisterProject(ctx, &pb.RegisterProjectRequest{
		NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout}); err != nil {
		t.Fatal(err)
	}
	build, review := newFakeWorkspaceHerdr(t, checkout, false, false), newFakeWorkspaceHerdr(t, checkout, false, false)
	manager := &namedNodeManager{}
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: build.path})
	manager.put(herdrsession.Session{Name: "review", Status: "ready", Incarnation: namedIncarnation("b"), SocketPath: review.path})
	var effects, queries atomic.Int32
	configure := func(options *node.Options) {
		options.EnableAgentControl = true
		validate := func(config herdr.Config, target *pb.AgentTarget) error {
			value, err := manager.Status(context.Background(), target.SessionName)
			if err != nil || value.SocketPath != config.SocketPath || value.Incarnation != target.SessionIncarnation ||
				target.PaneId != "p1" || target.TerminalId != "t1" || target.AgentSessionId != "a1" {
				return fmt.Errorf("selected endpoint/identity mismatch: config=%v target=%v session=%v", config, target, value)
			}
			return nil
		}
		options.QueryAgent = func(_ context.Context, config herdr.Config, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			queries.Add(1)
			if err := validate(config, request.Target); err != nil {
				t.Error(err)
				return nil, err
			}
			result := agentResponse(&pb.AgentQuery{Request: request})
			if request.Kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
				result.Text = request.Target.SessionName + " isolated output"
			}
			return result, nil
		}
		options.ControlAgent = func(_ context.Context, config herdr.Config, control *pb.AgentControl) (*pb.AgentControlResult, error) {
			effects.Add(1)
			if err := validate(config, control.Target); err != nil {
				t.Error(err)
				return nil, err
			}
			if control.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT || len(control.Keys) != 1 || control.Keys[0] != "enter" {
				t.Errorf("explicit input changed in transit: %v", control)
			}
			return &pb.AgentControlResult{Target: proto.Clone(control.Target).(*pb.AgentTarget), ObservedStatus: "idle", StateChangeSeq: 2}, nil
		}
	}
	journal := filepath.Join(root, "node", "journal.db")
	run := startNamedNode(t, h, journal, manager, false, configure)
	waitNamedProject(t, h, run)
	waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a"), "review": namedIncarnation("b")})
	var buildRequest *pb.SubmitCommandRequest
	var buildReceipt *pb.CommandRecord
	for _, name := range []string{"build", "review"} {
		value, _ := manager.Status(ctx, name)
		workspace := managedWorkspaceRequest("workspace-" + name)
		workspace.WorkspaceEnsure.SessionName, workspace.WorkspaceEnsure.SessionIncarnation = name, value.Incarnation
		first := submitNamedNode(t, h, workspace)
		created := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
		if !created.WorkspaceEnsure.GetCreated() || created.WorkspaceEnsure.GetWorkspaceId() != "w1" ||
			created.WorkspaceEnsure.GetSessionName() != name || created.WorkspaceEnsure.GetSessionIncarnation() != value.Incarnation {
			t.Fatalf("workspace was not created in selected session: %v", created)
		}
		for _, kind := range []pb.AgentQueryKind{pb.AgentQueryKind_AGENT_QUERY_KIND_GET, pb.AgentQueryKind_AGENT_QUERY_KIND_READ} {
			pin := value.Incarnation
			if kind == pb.AgentQueryKind_AGENT_QUERY_KIND_GET {
				pin = ""
			}
			result, err := client.QueryAgent(ctx, namedAgentRequest(name, pin, kind))
			if err != nil || result.GetErrorCode() != "" || result.GetAgent().GetTarget().GetSessionIncarnation() != value.Incarnation ||
				result.GetAgent().GetWorkspaceId() != "w1" || (kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ && result.Text != name+" isolated output") {
				t.Fatalf("duplicate w1/p1 selected query failed: %v %v", result, err)
			}
		}
		request := namedAgentControl("input-"+name, name, value.Incarnation, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
		first = submitNamedNode(t, h, request)
		result := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
		if result.AgentControl.GetTarget().GetSessionName() != name || result.AgentControl.GetTarget().GetSessionIncarnation() != value.Incarnation {
			t.Fatalf("input receipt lost selected session identity: %v", result)
		}
		if name == "build" {
			buildRequest, buildReceipt = request, result
		}
	}
	if build.creates.Load() != 1 || review.creates.Load() != 1 || effects.Load() != 2 || queries.Load() != 4 {
		t.Fatal("duplicate IDs aliased selected-session effects")
	}
	// Native Status sees replacement before the next discovery snapshot. The
	// coordinator still admits the old pin, so only the real node can fence it.
	manager.freezeInventory(true)
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("c"), SocketPath: build.path})
	result, err := client.QueryAgent(ctx, namedAgentRequest("build", namedIncarnation("a"), pb.AgentQueryKind_AGENT_QUERY_KIND_GET))
	if err != nil || result.GetErrorCode() != "session_replaced" {
		t.Fatalf("node did not freshly fence replacement ahead of inventory: %v %v", result, err)
	}
	staleAtNode := proto.Clone(buildRequest).(*pb.SubmitCommandRequest)
	staleAtNode.IdempotencyKey = "stale-before-inventory"
	first := submitNamedNode(t, h, staleAtNode)
	rejected := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
	if rejected.Detail != "session_replaced" || effects.Load() != 2 || queries.Load() != 4 {
		t.Fatalf("stale native input/query pin reached adapter: %v", rejected)
	}
	staleWorkspace := managedWorkspaceRequest("stale-workspace-before-inventory")
	staleWorkspace.WorkspaceEnsure.SessionName, staleWorkspace.WorkspaceEnsure.SessionIncarnation = "build", namedIncarnation("a")
	first = submitNamedNode(t, h, staleWorkspace)
	rejected = awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_REJECTED)
	if rejected.Detail != "session_replaced" || build.creates.Load() != 1 {
		t.Fatalf("stale native workspace pin reached IPC: %v", rejected)
	}
	run.stop()
	// Reuse the endpoint and every pane/terminal/agent ID, but replace native
	// identity. Neither the node reconnect nor the pane IDs may revive a pin.
	manager.freezeInventory(false)
	run = startNamedNode(t, h, journal, manager, false, configure)
	waitNamedProject(t, h, run)
	waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("c"), "review": namedIncarnation("b")})
	if retry := submitNamedNode(t, h, buildRequest); !proto.Equal(retry, buildReceipt) || effects.Load() != 2 {
		t.Fatalf("replacement changed historical same-key input: %v", retry)
	}
	stale := proto.Clone(buildRequest).(*pb.SubmitCommandRequest)
	stale.IdempotencyKey = "stale-after-restart"
	if _, err := client.SubmitCommand(ctx, stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reused pane accepted stale input pin: %v", err)
	}
	if _, err := client.QueryAgent(ctx, namedAgentRequest("build", namedIncarnation("a"), pb.AgentQueryKind_AGENT_QUERY_KIND_GET)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reused pane accepted stale query pin: %v", err)
	}
	stale.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: "AgentFlow", SessionName: "build", SessionIncarnation: namedIncarnation("a")}
	stale.AgentControl, stale.CommandType, stale.IdempotencyKey = nil, protocol.WorkspaceEnsureCommandType, "stale-workspace"
	if _, err := client.SubmitCommand(ctx, stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reused workspace accepted stale pin: %v", err)
	}
	if effects.Load() != 2 || queries.Load() != 4 || run.client.count.Load() != 0 {
		t.Fatal("stale pin reached node executor")
	}
	for name, incarnation := range map[string]string{"build": namedIncarnation("c"), "review": namedIncarnation("b")} {
		result, err := client.QueryAgent(ctx, namedAgentRequest(name, "", pb.AgentQueryKind_AGENT_QUERY_KIND_GET))
		if err != nil || result.GetAgent().GetTarget().GetSessionIncarnation() != incarnation {
			t.Fatalf("fresh query did not resolve independent current session: %v %v", result, err)
		}
		first := submitNamedNode(t, h, namedAgentControl("current-"+name, name, incarnation, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT))
		awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	}
	if effects.Load() != 4 || build.creates.Load() != 1 || review.creates.Load() != 1 {
		t.Fatal("replacement changed unrelated session/workspace effects")
	}
}

func TestNamedSessionsNodeSlowEnsureAndCanceledWaitDoNotBlockInterrupt(t *testing.T) {
	root := managedTestRoot(t)
	h := newCommandHarness(t, filepath.Join(root, "server"))
	fake := newFakeWorkspaceHerdr(t, "", false, false)
	started, canceled := make(chan struct{}), make(chan struct{})
	waitStarted, waitCanceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	manager := &namedNodeManager{ensure: func(ctx context.Context, _ string) (herdrsession.Session, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return herdrsession.Session{}, ctx.Err()
	}}
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: fake.path})
	var controls atomic.Int32
	run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), manager, false, func(options *node.Options) {
		options.EnableAgentControl = true
		options.QueryAgent = func(ctx context.Context, config herdr.Config, request *pb.AgentQueryRequest) (*pb.AgentQueryResult, error) {
			if config.SocketPath != fake.path {
				t.Errorf("wait routed away from named session: %v", config)
			}
			close(waitStarted)
			<-ctx.Done()
			close(waitCanceled)
			<-release
			return nil, ctx.Err()
		}
		options.ControlAgent = func(_ context.Context, config herdr.Config, control *pb.AgentControl) (*pb.AgentControlResult, error) {
			if config.SocketPath != fake.path || control.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
				t.Errorf("interrupt routed to wrong endpoint/action: %v %v", config, control)
			}
			controls.Add(1)
			return &pb.AgentControlResult{Target: proto.Clone(control.Target).(*pb.AgentTarget), ObservedStatus: "idle", StateChangeSeq: 3}, nil
		}
	})
	before := waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a")}).LastSeen.AsTime()
	client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	waitDone := make(chan error, 1)
	go func() {
		_, err := client.QueryAgent(waitCtx, namedAgentRequest("build", namedIncarnation("a"), pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT))
		waitDone <- err
	}()
	select {
	case <-waitStarted:
	case err := <-waitDone:
		t.Fatalf("named wait rejected before node executor: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("named wait did not reach node")
	}
	ensure := submitNamedNode(t, h, namedEnsureRequest("slow-start", "slow"))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow ensure did not enter native manager")
	}
	cancelWait()
	select {
	case err := <-waitDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("wait caller cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait caller did not cancel")
	}
	select {
	case <-waitCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("wait cancellation not forwarded while ensure was blocked")
	}
	interrupt := namedAgentControl("interrupt", "build", namedIncarnation("a"), pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT)
	first := submitNamedNode(t, h, interrupt)
	completed := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if controls.Load() != 1 || completed.Detail != "agent_interrupt_sent" {
		t.Fatalf("blocked ensure/wait prevented interrupt: %v", completed)
	}
	select {
	case <-canceled:
		t.Fatal("interrupt only completed after slow ensure ended")
	default:
	}
	select {
	case <-release:
		t.Fatal("interrupt required canceled wait executor to return")
	default:
	}
	view := waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.LastSeen.AsTime().After(before) })
	assertNoNamedDefault(t, view)
	unblock()
	run.stop()
	select {
	case <-canceled:
	default:
		t.Fatalf("shutdown did not cancel slow ensure %s", ensure.Command.CommandId)
	}
}

func TestNamedSessionsNodeWorktreeUsesSelectedIPCWithoutDefault(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	base := worktreeGit(t, checkout, "rev-parse", "HEAD")
	h := newCommandHarness(t, filepath.Join(root, "server"))
	client, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	if _, err := client.RegisterProject(ctx, &pb.RegisterProjectRequest{
		NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout}); err != nil {
		t.Fatal(err)
	}
	selected := &fakeWorktreeHerdr{checkout: checkout, destination: filepath.Join(checkout+"-worktrees", "task-one"), base: base}
	selected.path = listenFakeHerdr(t, selected.serve)
	other := newFakeWorkspaceHerdr(t, checkout, false, false)
	manager := &namedNodeManager{}
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: selected.path})
	manager.put(herdrsession.Session{Name: "review", Status: "ready", Incarnation: namedIncarnation("b"), SocketPath: other.path})
	run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), manager, false, nil)
	waitNamedProject(t, h, run)
	waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a"), "review": namedIncarnation("b")})
	request := worktreeRequest("selected-worktree")
	request.WorktreeCreate.BindingRevision, request.WorktreeCreate.BaseCommit, request.WorktreeCreate.Branch = "", "", ""
	request.WorktreeCreate.SessionName, request.WorktreeCreate.SessionIncarnation = "build", namedIncarnation("a")
	first := submitNamedNode(t, h, request)
	result := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if result.WorktreeCreate.GetSessionName() != "build" || result.WorktreeCreate.GetSessionIncarnation() != namedIncarnation("a") ||
		result.WorktreeCreate.GetBaseCommit() != base || result.WorktreeCreate.GetWorkspaceId() != "w2" ||
		selected.creates.Load() != 1 || other.creates.Load() != 0 {
		t.Fatalf("managed worktree escaped selected native endpoint: %v", result)
	}
	if worktreeGit(t, selected.destination, "rev-parse", "HEAD") != base ||
		worktreeGit(t, selected.destination, "symbolic-ref", "--short", "HEAD") != "task-one" ||
		worktreeGit(t, checkout, "symbolic-ref", "--short", "HEAD") != "main" {
		t.Fatal("selected worktree changed source branch or used wrong base")
	}
	if retry := submitNamedNode(t, h, request); !proto.Equal(retry, result) || selected.creates.Load() != 1 {
		t.Fatalf("selected worktree same-key retry repeated native effect: %v", retry)
	}
}
