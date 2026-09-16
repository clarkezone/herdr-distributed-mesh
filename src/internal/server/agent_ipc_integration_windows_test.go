//go:build windows

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeAgentIPC struct {
	path, output              string
	mu                        sync.Mutex
	status, terminal          string
	marker, echo              string
	markerReads               int
	sequence                  uint64
	prompts, interrupts       atomic.Int32
	replaceOnRead             atomic.Bool
	waitStarted, waitCanceled chan struct{}
}

func newFakeAgentIPC(t *testing.T) *fakeAgentIPC {
	t.Helper()
	f := &fakeAgentIPC{output: "OUTPUT-ONLY-" + protocol.NewCommandID(), status: "idle", terminal: "t1", sequence: 1,
		waitStarted: make(chan struct{}, 8), waitCanceled: make(chan struct{}, 8)}
	f.path = listenFakeHerdr(t, f.serve)
	return f
}

func (f *fakeAgentIPC) view() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return map[string]any{
		"pane_id": "p1", "terminal_id": f.terminal, "workspace_id": "w1", "tab_id": "tab1",
		"agent": "copilot", "agent_status": f.status, "interactive_ready": true, "launch_pending": false,
		"revision": f.sequence, "state_change_seq": f.sequence, "focused": false,
		"agent_session": map[string]any{"kind": "id", "value": "a1", "agent": "copilot", "source": "synthetic"},
	}
}

func (f *fakeAgentIPC) serve(t *testing.T, conn net.Conn) {
	var request struct {
		ID, Method string
		Params     map[string]json.RawMessage
	}
	if json.NewDecoder(conn).Decode(&request) != nil {
		return
	}
	if strings.HasPrefix(request.Method, "agent.") {
		var target string
		if json.Unmarshal(request.Params["target"], &target) != nil || target != "p1" {
			t.Error("agent IPC escaped the exact pane target")
			return
		}
	}
	var result any
	switch request.Method {
	case "ping":
		result = map[string]any{"type": "pong", "protocol": 18, "version": "0.7.5-preview"}
	case "events.subscribe":
		result = map[string]any{"type": "subscription_started"}
	case "session.snapshot":
		result = map[string]any{"type": "session_snapshot", "snapshot": map[string]any{
			"protocol": 18, "version": "0.7.5-preview", "workspaces": []any{}, "tabs": []any{},
			"panes": []any{}, "agents": []any{}, "layouts": []any{}, "focused_workspace_id": nil}}
	case "agent.get":
		result = map[string]any{"type": "agent_info", "agent": f.view()}
	case "agent.prompt":
		var text string
		if json.Unmarshal(request.Params["text"], &text) != nil || len(request.Params) != 2 {
			t.Error("unexpected prompt payload")
			return
		}
		var marker string
		if text != "synthetic prompt" {
			quoted := strings.Split(text, "\"")
			if len(quoted) != 5 || quoted[1] != "M_" || !protocol.ValidCommandID(quoted[3]) {
				t.Error("headless prompt must separate its marker fragments")
				return
			}
			expectedPrompt, expectedMarker := headlessMarkerPrompt(quoted[3])
			if text != expectedPrompt || strings.Contains(text, expectedMarker) {
				t.Error("headless prompt echoed its expected marker")
				return
			}
			marker = expectedMarker
		}
		f.prompts.Add(1)
		f.mu.Lock()
		f.status, f.sequence = "working", f.sequence+1
		if marker != "" {
			f.status, f.marker, f.echo = "idle", marker, text
			f.markerReads = 0
		}
		f.mu.Unlock()
		result = map[string]any{"type": "agent_prompted", "agent": f.view()}
	case "agent.send_keys":
		var keys []string
		if json.Unmarshal(request.Params["keys"], &keys) != nil || !slices.Equal(keys, []string{"esc", "esc"}) || len(request.Params) != 2 {
			t.Error("interrupt must deliver exactly one request containing [esc,esc]")
			return
		}
		f.interrupts.Add(1)
		f.mu.Lock()
		f.status, f.sequence = "idle", f.sequence+1
		f.mu.Unlock()
		result = map[string]any{"type": "ok"}
	case "agent.read":
		var source, format string
		var strip bool
		var lines uint32
		if json.Unmarshal(request.Params["source"], &source) != nil || source != "recent_unwrapped" ||
			json.Unmarshal(request.Params["format"], &format) != nil || format != "text" ||
			json.Unmarshal(request.Params["strip_ansi"], &strip) != nil || !strip ||
			json.Unmarshal(request.Params["lines"], &lines) != nil || lines == 0 || len(request.Params) != 5 {
			t.Error("unexpected read payload")
			return
		}
		if f.replaceOnRead.Swap(false) {
			f.mu.Lock()
			f.terminal = "replacement"
			f.mu.Unlock()
		}
		f.mu.Lock()
		output := f.output
		if f.marker != "" {
			f.markerReads++
			output = f.echo
			if f.markerReads > 1 {
				output += "\n" + f.marker
			}
		}
		f.mu.Unlock()
		result = map[string]any{"type": "pane_read", "read": map[string]any{
			"pane_id": "p1", "workspace_id": "w1", "tab_id": "tab1", "source": source, "format": format,
			"text": output, "revision": 3, "truncated": false}}
	case "agent.wait":
		var until []string
		var timeout uint32
		if json.Unmarshal(request.Params["until"], &until) != nil ||
			json.Unmarshal(request.Params["timeout_ms"], &timeout) != nil || timeout == 0 || timeout > 10000 {
			t.Error("unbounded or malformed wait")
			return
		}
		view := f.view()
		if slices.Contains(until, view["agent_status"].(string)) {
			result = map[string]any{"type": "agent_info", "agent": view}
		} else {
			f.waitStarted <- struct{}{}
			_, _ = io.Copy(io.Discard, conn)
			f.waitCanceled <- struct{}{}
			return
		}
	default:
		t.Errorf("unexpected synthetic agent method %q", request.Method)
		return
	}
	if json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "result": result}) != nil {
		return
	}
	if request.Method == "events.subscribe" {
		_, _ = io.Copy(io.Discard, conn)
	}
}

type runningAgentNode struct {
	client *countedNodeClient
	done   chan struct{}
	stop   func()
	err    error
}

func startRealAdapterNode(t *testing.T, h *commandHarness, socket, path string, heartbeatInterval time.Duration) *runningAgentNode {
	t.Helper()
	ctx, cancel := context.WithCancel(commandPeer(context.Background(), "node"))
	running := &runningAgentNode{client: &countedNodeClient{NodeControlClient: pb.NewNodeControlClient(h.connection)}, done: make(chan struct{})}
	go func() {
		defer close(running.done)
		_, running.err = node.RunSession(ctx, running.client, node.Options{
			InstanceID: "node-1", HerdrSocket: socket, CommandJournalPath: path, EnableAgentControl: true,
			RequiredServerTag: node.DefaultRequiredServerTag, HeartbeatInterval: heartbeatInterval,
			VerifyServerPeer: func(_ context.Context, address string) error {
				if address != "bufconn" {
					return fmt.Errorf("unexpected actual coordinator peer address")
				}
				return nil
			},
		})
	}()
	running.stop = func() {
		cancel()
		select {
		case <-running.done:
		case <-time.After(5 * time.Second):
			t.Error("agent session did not join before journal shutdown")
		}
	}
	t.Cleanup(running.stop)
	return running
}

func waitAgentNodeReady(t *testing.T, ctx context.Context, fleet pb.FleetClient) *pb.NodeView {
	t.Helper()
	for {
		list, err := fleet.ListNodes(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatal("cannot read isolated fleet readiness")
		}
		if len(list.Nodes) == 1 && list.Nodes[0].AgentReady {
			return list.Nodes[0]
		}
		select {
		case <-ctx.Done():
			t.Fatal("agent node did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func controlRequest(target *pb.AgentTarget, action pb.AgentControlAction) *pb.SubmitCommandRequest {
	control := &pb.AgentControl{Target: proto.Clone(target).(*pb.AgentTarget), Action: action}
	if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
		control.Text = "synthetic prompt"
	}
	return &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: protocol.NewCommandID(),
		CommandType: protocol.AgentControlCommandType, Ttl: durationpb.New(10 * time.Second), AgentControl: control}
}

func waitAgentReceipt(ctx context.Context, fleet pb.FleetClient, id string) (*pb.CommandRecord, error) {
	for {
		record, err := fleet.GetCommand(ctx, &pb.GetCommandRequest{CommandId: id})
		if err != nil || protocol.IsTerminalCommand(record.GetStatus()) {
			return record, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertQueryOutputAbsent(t *testing.T, root, output string) {
	t.Helper()
	for _, directory := range []string{"node", "coordinator"} {
		files, err := filepath.Glob(filepath.Join(root, directory, "*.db*"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatal("expected real SQLite journal files")
		}
		for _, path := range files {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), output) {
				t.Fatal("query output entered a SQLite journal")
			}
		}
	}
}

func TestAgentNamedPipeRealJournalsCancelInterruptReconnectAndFence(t *testing.T) {
	root := t.TempDir()
	fake := newFakeAgentIPC(t)
	h := newCommandHarness(t, root)
	fleet := pb.NewFleetClient(h.connection)
	path := filepath.Join(root, "node", "commands.db")
	running := startRealAdapterNode(t, h, fake.path, path, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 20*time.Second)
	defer cancel()
	waitAgentNodeReady(t, ctx, fleet)
	get, err := fleet.QueryAgent(ctx, agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_GET))
	if err != nil || get.GetAgent() == nil || get.Agent.Target.AgentSessionId != "a1" {
		t.Fatal("real adapter GET failed")
	}
	target := proto.Clone(get.Agent.Target).(*pb.AgentTarget)
	promptTarget := proto.Clone(target).(*pb.AgentTarget)
	promptTarget.AgentSessionId = ""
	prompt, err := fleet.SubmitCommand(ctx, controlRequest(promptTarget, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT))
	if err != nil {
		t.Fatal(err)
	}
	prompt, err = waitAgentReceipt(ctx, fleet, prompt.Command.CommandId)
	if err != nil || prompt.GetDetail() != "agent_prompt_sent" || !proto.Equal(prompt.GetAgentControl().GetTarget(), promptTarget) {
		t.Fatal("prompt receipt did not preserve the original target")
	}
	waitRequest := agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT)
	waitRequest.Until = []string{"unknown"}
	waitCtx, cancelWait := context.WithCancel(ctx)
	waitDone := make(chan error, 1)
	go func() { _, err := fleet.QueryAgent(waitCtx, waitRequest); waitDone <- err }()
	select {
	case <-fake.waitStarted:
	case <-ctx.Done():
		t.Fatal("WAIT did not reach actual named-pipe adapter")
	}
	before := waitAgentNodeReady(t, ctx, fleet).LastSeen.AsTime()
	readCtx := commandPeer(context.Background(), "client2")
	readCtx, cancelRead := context.WithTimeout(readCtx, 3*time.Second)
	defer cancelRead()
	read, err := fleet.QueryAgent(readCtx, agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_READ))
	if err != nil || read.GetText() != fake.output {
		t.Fatal("concurrent real adapter READ failed")
	}
	for !waitAgentNodeReady(t, ctx, fleet).LastSeen.AsTime().After(before) {
		time.Sleep(10 * time.Millisecond)
	}
	interruptDone := make(chan error, 1)
	go func() {
		record, err := fleet.SubmitCommand(ctx, controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT))
		if err == nil {
			record, err = waitAgentReceipt(ctx, fleet, record.Command.CommandId)
			if err == nil && (record.GetDetail() != "agent_interrupt_sent" || record.GetStatus() != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED) {
				err = fmt.Errorf("interrupt delivery receipt missing")
			}
		}
		interruptDone <- err
	}()
	cancelWait()
	if err := <-waitDone; status.Code(err) != codes.Canceled {
		t.Fatalf("WAIT cancellation status: %v", status.Code(err))
	}
	select {
	case <-fake.waitCanceled:
	case <-ctx.Done():
		t.Fatal("cancel did not close the matching local wait")
	}
	if err := <-interruptDone; err != nil {
		t.Fatal(err)
	}
	if fake.interrupts.Load() != 1 || fake.prompts.Load() != 1 {
		t.Fatal("mutation was retried or interrupt was split into multiple requests")
	}
	fake.replaceOnRead.Store(true)
	changed, err := fleet.QueryAgent(ctx, agentRequest(pb.AgentQueryKind_AGENT_QUERY_KIND_READ))
	if err != nil || changed.GetErrorCode() != "target_changed" || changed.Agent != nil || changed.Text != "" {
		t.Fatal("replaced terminal returned output")
	}
	mismatch, err := fleet.SubmitCommand(ctx, controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT))
	if err != nil {
		t.Fatal(err)
	}
	mismatch, err = waitAgentReceipt(ctx, fleet, mismatch.Command.CommandId)
	if err != nil || mismatch.GetDetail() != "target_changed" || fake.interrupts.Load() != 1 {
		t.Fatal("mismatched target reached interrupt IPC")
	}
	fake.mu.Lock()
	fake.terminal = "t1"
	fake.mu.Unlock()
	go func() { _, err := fleet.QueryAgent(ctx, waitRequest); waitDone <- err }()
	select {
	case <-fake.waitStarted:
	case <-ctx.Done():
		t.Fatal("reconnect wait did not start")
	}
	h.api.fleet.mu.Lock()
	retired := h.api.fleet.nodes["node-1"]
	h.api.fleet.mu.Unlock()
	running.client.dropResult.Store(true)
	replayRequest := controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT)
	lost, err := fleet.SubmitCommand(ctx, replayRequest)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-running.done:
	case <-ctx.Done():
		t.Fatal("lost receipt did not terminate and join the session")
	}
	if err := <-waitDone; status.Code(err) != codes.Unavailable {
		t.Fatalf("disconnected query status: %v", status.Code(err))
	}
	if err := h.api.finishAgentQuery(retired, &pb.AgentQueryResult{QueryId: protocol.NewCommandID(), ErrorCode: "canceled"}); status.Code(err) != codes.Aborted {
		t.Fatal("retired stream result accepted")
	}
	h.stop()
	reopened := newCommandHarness(t, root)
	reconnected := startRealAdapterNode(t, reopened, fake.path, path, 20*time.Millisecond)
	fleet = pb.NewFleetClient(reopened.connection)
	waitAgentNodeReady(t, ctx, fleet)
	replayed := awaitCommand(t, reopened, lost.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if replayed.GetDetail() != "agent_interrupt_sent" || !proto.Equal(replayed.GetAgentControl().GetTarget(), target) {
		t.Fatal("durable interrupt receipt did not replay after both journals reopened")
	}
	retry, err := fleet.SubmitCommand(ctx, replayRequest)
	if err != nil || retry.Command.CommandId != lost.Command.CommandId || fake.interrupts.Load() != 2 {
		t.Fatal("reconnect repeated an already durable interrupt")
	}
	reconnected.stop()
	reopened.stop()
	assertQueryOutputAbsent(t, root, fake.output)
}

func TestAgentHeadlessOptInHarnessWithSyntheticPipe(t *testing.T) {
	fake := newFakeAgentIPC(t)
	t.Setenv("HERDR_MESH_AGENT_TEST_SOCKET", fake.path)
	t.Setenv("HERDR_MESH_AGENT_TEST_PANE", "p1")
	t.Run("headless-path", TestAgentLiveHeadlessQueryAndDurableInterrupt)
	if fake.interrupts.Load() != 1 || fake.prompts.Load() != 1 {
		t.Fatal("headless test must send exactly one marker prompt and one interrupt")
	}
	fake.mu.Lock()
	reads := fake.markerReads
	fake.mu.Unlock()
	if reads < 2 {
		t.Fatal("headless test accepted idle prompt echo without reading generated output")
	}
}
