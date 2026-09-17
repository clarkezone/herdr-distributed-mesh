//go:build windows

package server

import (
	"bytes"
	"context"
	"errors"
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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func lifecycleFakeEvent(ctx context.Context, config herdr.Config, hook herdr.LifecycleHook, value *herdr.LifecycleResult,
	stage herdr.LifecycleStage, before bool) error {
	outcome := herdr.LifecycleConfirmed
	if before {
		outcome = herdr.LifecycleUnknown
	}
	switch stage {
	case herdr.LifecycleCreatePane:
		value.Pane = outcome
		if !before {
			value.Handle.TabID, value.Handle.PaneID, value.Handle.TerminalID = "tab:2", "pane:2", "term:2"
			value.Handle.Provider = "unknown"
		}
	case herdr.LifecycleLaunch:
		value.Launch = outcome
		if !before {
			value.Handle.Provider, value.ObservedStatus = "copilot", "idle"
		}
	case herdr.LifecyclePrompt:
		value.Prompt = outcome
	case herdr.LifecycleClosePane:
		value.Stop = outcome
	}
	if err := hook(ctx, herdr.LifecycleEvent{Stage: stage, Before: before, Result: *value}); err != nil {
		return err
	}
	return config.CheckSession(ctx)
}

func lifecycleFakeInitial() herdr.LifecycleResult {
	return herdr.LifecycleResult{Handle: herdr.LifecycleHandle{WorkspaceID: "w1"},
		Pane: herdr.LifecycleNotAttempted, Launch: herdr.LifecycleNotAttempted,
		Prompt: herdr.LifecycleNotAttempted, Stop: herdr.LifecycleNotAttempted}
}

func TestLifecycleEndToEndReservedStopInterruptAndNoPromptAfterStop(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	h := newCommandHarness(t, filepath.Join(root, "server"))
	fleet, clientCtx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	if _, err := fleet.RegisterProject(clientCtx, &pb.RegisterProjectRequest{NodeInstanceId: "node-1",
		ProjectId: "AgentFlow", CheckoutPath: checkout}); err != nil {
		t.Fatal(err)
	}
	fake := newFakeWorkspaceHerdr(t, checkout, false, false)
	manager := &namedNodeManager{}
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: fake.path})
	waiting, stopped := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var starts, prompts, stops, interrupts atomic.Int32
	run := startNamedNode(t, h, filepath.Join(root, "node", "journal.db"), manager, false, func(options *node.Options) {
		options.EnableAgentControl = true
		options.ResolveLifecycleWorkspace = func(ctx context.Context, config herdr.Config, workspace string, binding projects.Binding) (string, error) {
			if workspace != "w1" || binding.Path != checkout {
				return "", errors.New("wrong selected checkout")
			}
			return checkout, config.CheckSession(ctx)
		}
		options.StartAgent = func(ctx context.Context, config herdr.Config, request herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
			starts.Add(1)
			value := lifecycleFakeInitial()
			for _, before := range []bool{true, false} {
				if err := lifecycleFakeEvent(ctx, config, hook, &value, herdr.LifecycleCreatePane, before); err != nil {
					return value, err
				}
			}
			if err := lifecycleFakeEvent(ctx, config, hook, &value, herdr.LifecycleLaunch, true); err != nil {
				return value, err
			}
			close(waiting)
			select {
			case <-stopped:
			case <-ctx.Done():
				return value, ctx.Err()
			}
			// Simulate fresh get finding the pane gone. The real adapter has
			// separate fake-IPC regression tests for this exact preflight.
			return value, herdr.ErrAgentChanged
		}
		options.ControlAgent = func(ctx context.Context, config herdr.Config, action *pb.AgentControl) (*pb.AgentControlResult, error) {
			if err := config.CheckSession(ctx); err != nil {
				return nil, err
			}
			if action.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
				prompts.Add(1)
			} else {
				interrupts.Add(1)
			}
			return &pb.AgentControlResult{Target: action.Target, ObservedStatus: "idle"}, nil
		}
		options.StopAgent = func(ctx context.Context, config herdr.Config, request herdr.StopAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
			value := lifecycleFakeInitial()
			value.Handle = request.Handle
			if request.Handle.PaneID != "pane:2" || request.Handle.WorkspaceID != "w1" {
				return value, herdr.ErrAgentChanged
			}
			if err := lifecycleFakeEvent(ctx, config, hook, &value, herdr.LifecycleClosePane, true); err != nil {
				return value, err
			}
			stops.Add(1)
			once.Do(func() { close(stopped) })
			return value, lifecycleFakeEvent(ctx, config, hook, &value, herdr.LifecycleClosePane, false)
		}
	})
	waitNamedProject(t, h, run)
	before := waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a")}).LastSeen.AsTime()
	start := &pb.AgentStart{ProjectId: "AgentFlow", WorkspaceId: "w1", Name: "task", Provider: "copilot",
		SessionName: "build", StartupTimeoutMs: 30000, InitialPrompt: "tiny task"}
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "launch",
		CommandType: protocol.AgentStartCommandType, AgentStart: start}
	first := submitNamedNode(t, h, request)
	select {
	case <-waiting:
	case <-clientCtx.Done():
		t.Fatal("startup not dispatched")
	}
	var partial *pb.CommandRecord
	for {
		var err error
		partial, err = fleet.GetCommand(clientCtx, &pb.GetCommandRequest{CommandId: first.Command.CommandId})
		if err != nil {
			t.Fatal(err)
		}
		if partial.AgentLifecycle.GetSequence() == 3 {
			break
		}
		select {
		case <-clientCtx.Done():
			t.Fatal("before-launch checkpoint did not reach coordinator")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if partial.Status != pb.CommandStatus_COMMAND_STATUS_RUNNING || partial.AgentLifecycle.Handle.Target.TerminalId != "term:2" {
		t.Fatal("running receipt did not preserve partial target")
	}
	blocked := proto.Clone(request).(*pb.SubmitCommandRequest)
	blocked.IdempotencyKey = "new-key-evasion"
	if _, err := fleet.SubmitCommand(clientCtx, blocked); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pending launch allowed another key: %v", err)
	}
	target := proto.Clone(partial.AgentLifecycle.Handle.Target).(*pb.AgentTarget)
	input := controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	if _, err := fleet.SubmitCommand(clientCtx, input); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ordinary prompt interleaved pending initial prompt: %v", err)
	}
	interrupt := submitNamedNode(t, h, controlRequest(target, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT))
	awaitCommand(t, h, interrupt.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	waitNamedNode(t, h, run, func(v *pb.NodeView) bool { return v.LastSeen.AsTime().After(before) })
	var output bytes.Buffer
	options := control.Options{FleetClient: fleet, RequiredServerTag: "tag:herdr-mesh-server", JSON: true, Output: &output}
	if err := control.StopAgent(clientCtx, options, "node-1", "stop", &pb.AgentStop{Target: target,
		WorkspaceId: "w1", TabId: "tab:2", Provider: "copilot"}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	stopReceipt := new(pb.CommandRecord)
	if err := protojson.Unmarshal(output.Bytes(), stopReceipt); err != nil || stopReceipt.AgentLifecycle.GetStopOutcome() != "confirmed" {
		t.Fatalf("shared control path lost stop receipt: %v", err)
	}
	final := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_INDETERMINATE)
	if final.AgentLifecycle.GetLaunchOutcome() != "unknown" || final.AgentLifecycle.PromptOutcome != "not_attempted" {
		t.Fatal("stopped startup became ready or sent a prompt")
	}
	replay := submitNamedNode(t, h, request)
	if !proto.Equal(replay, final) || starts.Load() != 1 || stops.Load() != 1 || interrupts.Load() != 1 || prompts.Load() != 0 {
		t.Fatal("replay duplicated effects or urgent lane failed")
	}
	if _, err := manager.Status(clientCtx, "build"); err != nil || manager.calls.Load() != 0 {
		t.Fatal("stop altered the native session manager")
	}
}

func TestLifecycleEndToEndLostFinalResultReplaysWithoutRelaunch(t *testing.T) {
	root := managedTestRoot(t)
	checkout := managedCheckout(t, root)
	h := newCommandHarness(t, filepath.Join(root, "server"))
	fleet, ctx := pb.NewFleetClient(h.connection), namedNodeContext(t)
	if _, err := fleet.RegisterProject(ctx, &pb.RegisterProjectRequest{NodeInstanceId: "node-1", ProjectId: "AgentFlow", CheckoutPath: checkout}); err != nil {
		t.Fatal(err)
	}
	fake := newFakeWorkspaceHerdr(t, checkout, false, false)
	manager := &namedNodeManager{}
	manager.put(herdrsession.Session{Name: "build", Status: "ready", Incarnation: namedIncarnation("a"), SocketPath: fake.path})
	var starts atomic.Int32
	configure := func(options *node.Options) {
		options.EnableAgentControl = true
		options.ResolveLifecycleWorkspace = func(context.Context, herdr.Config, string, projects.Binding) (string, error) { return checkout, nil }
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
	}
	journal := filepath.Join(root, "node", "journal.db")
	run := startNamedNode(t, h, journal, manager, true, configure)
	waitNamedProject(t, h, run)
	waitNamedInventory(t, h, run, map[string]string{"build": namedIncarnation("a")})
	request := &pb.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "lost",
		CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{ProjectId: "AgentFlow", WorkspaceId: "w1",
			Name: "task", Provider: "copilot", SessionName: "build", InitialPrompt: strings.Repeat("p", protocol.MaxAgentPromptBytes)}}
	first := submitNamedNode(t, h, request)
	select {
	case <-run.done:
	case <-ctx.Done():
		t.Fatal("injected result loss did not end stream")
	}
	run.stop()
	run = startNamedNode(t, h, journal, manager, false, configure)
	final := awaitCommand(t, h, first.Command.CommandId, pb.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if starts.Load() != 1 || final.AgentLifecycle.GetSequence() != 6 || final.AgentLifecycle.PromptOutcome != "confirmed" ||
		final.Command.AgentStart.InitialPrompt != "" || len(final.Command.SubmittedRequest.AgentStart.InitialPrompt) != protocol.MaxAgentPromptBytes {
		t.Fatal("reconnect lost cumulative receipt, duplicated prompt storage, or relaunched")
	}
	replay := submitNamedNode(t, h, request)
	if !proto.Equal(final, replay) {
		t.Fatal("lost-ack retry changed its immutable receipt")
	}
	run.stop()
}
