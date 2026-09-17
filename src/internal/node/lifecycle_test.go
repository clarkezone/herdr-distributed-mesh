package node

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func lifecycleHandler(t *testing.T) (*commandHandler, *sessionManagerFixture) {
	t.Helper()
	h, manager := namedHandler(t)
	h.lifecycleNegotiated = true
	h.managedProjects = projects.NewManaged(h.nodeID)
	cache := h.journal.(projectJournal)
	if err := h.restoreProjects(context.Background(), cache, nil); err != nil {
		t.Fatal(err)
	}
	applyManaged(t, h, managedConfig(t, 1))
	h.resolveLifecycleWorkspace = func(ctx context.Context, config herdr.Config, workspace string, binding projects.Binding) (string, error) {
		if workspace != "ws:1" || config.CheckSession == nil || binding.ProjectID != "project" {
			t.Fatal("workspace resolution lost selected binding/session")
		}
		return binding.Path, config.CheckSession(ctx)
	}
	return h, manager
}

func lifecycleStartCommand(t *testing.T) *pb.Command {
	t.Helper()
	c := probe()
	r, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: c.TargetId, IdempotencyKey: c.IdempotencyKey,
		CommandType: protocol.AgentStartCommandType, Ttl: c.Ttl, AgentStart: &pb.AgentStart{ProjectId: "project",
			WorkspaceId: "ws:1", Name: "agent", Provider: "copilot", SessionName: "one",
			SessionIncarnation: strings.Repeat("a", 64), InitialPrompt: "tiny task"}})
	if err != nil {
		t.Fatal(err)
	}
	c.CommandType, c.SubmittedRequest = r.CommandType, r
	c.AgentStart = proto.Clone(r.AgentStart).(*pb.AgentStart)
	c.AgentStart.BindingRevision, c.AgentStart.InitialPrompt = "managed:1", ""
	deadline, err := protocol.LifecycleExecutionDeadline(c.ExpiresAt.AsTime(), c.AgentStart.StartupTimeoutMs)
	if err != nil {
		t.Fatal(err)
	}
	c.ExecutionExpiresAt = timestamppb.New(deadline)
	return c
}

func lifecycleEmit(ctx context.Context, config herdr.Config, hook herdr.LifecycleHook, value *herdr.LifecycleResult,
	stage herdr.LifecycleStage, before bool) error {
	outcome := herdr.LifecycleConfirmed
	if before {
		outcome = herdr.LifecycleUnknown
	}
	switch stage {
	case herdr.LifecycleCreatePane:
		value.Pane = outcome
		if !before {
			value.Handle = herdr.LifecycleHandle{WorkspaceID: "ws:1", TabID: "tab:2", PaneID: "pane:2", TerminalID: "term:2", Provider: "unknown"}
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

func initialLifecycle() herdr.LifecycleResult {
	return herdr.LifecycleResult{Handle: herdr.LifecycleHandle{WorkspaceID: "ws:1"},
		Pane: herdr.LifecycleNotAttempted, Launch: herdr.LifecycleNotAttempted,
		Prompt: herdr.LifecycleNotAttempted, Stop: herdr.LifecycleNotAttempted}
}

func TestLifecycleNodeSuccessPartialCancellationAndReplay(t *testing.T) {
	for _, cancelAt := range []int{0, 1, 2, 3, 4, 5, 6} {
		t.Run(string(rune('0'+cancelAt)), func(t *testing.T) {
			h, _ := lifecycleHandler(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := lifecycleStartCommand(t)
			var starts atomic.Int32
			h.startAgent = func(ctx context.Context, config herdr.Config, request herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
				starts.Add(1)
				if request.InitialPrompt != "tiny task" || request.StartupTimeout != 30*time.Second || request.Cwd == "" {
					t.Fatal("effective launch body lost startup/prompt/cwd")
				}
				value := initialLifecycle()
				count := 0
				for _, stage := range []herdr.LifecycleStage{herdr.LifecycleCreatePane, herdr.LifecycleLaunch, herdr.LifecyclePrompt} {
					for _, before := range []bool{true, false} {
						if err := lifecycleEmit(ctx, config, hook, &value, stage, before); err != nil {
							return value, err
						}
						count++
						if count == cancelAt {
							cancel()
							return value, ctx.Err()
						}
					}
				}
				return value, nil
			}
			result, err := h.handle(ctx, c, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			want := pb.CommandStatus_COMMAND_STATUS_FAILED
			if cancelAt == 0 || cancelAt == 6 {
				want = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED
			} else if cancelAt%2 == 1 {
				want = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
			}
			if result.Status != want || result.AgentLifecycle == nil {
				t.Fatalf("wrong durable cancellation receipt: %v", result)
			}
			replay, err := h.handle(context.Background(), c, time.Now())
			if err != nil || !proto.Equal(result, replay) || starts.Load() != 1 {
				t.Fatalf("exact replay launched twice/lost evidence: %v", err)
			}
		})
	}
}

type failingLifecycleJournal struct {
	commandJournal
	lifecycleJournal
	failSequence uint32
	afterCommit  bool
}

func (j failingLifecycleJournal) SaveLifecycle(ctx context.Context, id string, receipt *pb.AgentLifecycleReceipt) error {
	if receipt.Sequence != j.failSequence {
		return j.lifecycleJournal.SaveLifecycle(ctx, id, receipt)
	}
	if j.afterCommit {
		if err := j.lifecycleJournal.SaveLifecycle(ctx, id, receipt); err != nil {
			return err
		}
	}
	return errors.New("checkpoint storage fault")
}

func TestLifecycleNodeHookFailureNeverResendsAndReloadsDurableEvidence(t *testing.T) {
	for _, committed := range []bool{false, true} {
		h, _ := lifecycleHandler(t)
		h.journal = failingLifecycleJournal{commandJournal: h.journal, lifecycleJournal: h.journal.(lifecycleJournal),
			failSequence: 1, afterCommit: committed}
		var effects int
		h.startAgent = func(ctx context.Context, config herdr.Config, _ herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
			value := initialLifecycle()
			err := lifecycleEmit(ctx, config, hook, &value, herdr.LifecycleCreatePane, true)
			if err == nil {
				effects++
			}
			return value, errors.Join(herdr.ErrLifecyclePersistence, err)
		}
		c := lifecycleStartCommand(t)
		result, err := h.handle(context.Background(), c, time.Now())
		if err != nil || effects != 0 {
			t.Fatalf("failed hook dispatched or lost result: %v", err)
		}
		want := "not_attempted"
		if committed {
			want = "unknown"
		}
		if result.AgentLifecycle.PaneOutcome != want {
			t.Fatal("returned non-durable observations instead of reloading checkpoint")
		}
		if _, err := h.handle(context.Background(), c, time.Now()); err != nil || effects != 0 {
			t.Fatal("hook failure retried a side effect")
		}
	}
}

func TestLifecycleNodeStopAndInterruptProceedWhileStartupWaits(t *testing.T) {
	h, _ := lifecycleHandler(t)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	h.startAgent = func(ctx context.Context, config herdr.Config, _ herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
		value := initialLifecycle()
		for _, stage := range []herdr.LifecycleStage{herdr.LifecycleCreatePane, herdr.LifecycleLaunch} {
			for _, before := range []bool{true, false} {
				if stage == herdr.LifecycleLaunch && !before {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
					}
					return value, herdr.ErrAgentIndeterminate
				}
				if err := lifecycleEmit(ctx, config, hook, &value, stage, before); err != nil {
					return value, err
				}
			}
		}
		return value, nil
	}
	c := lifecycleStartCommand(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := h.handle(ctx, c, time.Now()); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("startup did not reach its bounded wait")
	}
	interrupt := agentCommand()
	interrupt.AgentControl = &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT,
		Target: &pb.AgentTarget{PaneId: "pane:2", TerminalId: "term:2", SessionName: "one", SessionIncarnation: strings.Repeat("a", 64)}}
	h.controlAgent = func(ctx context.Context, config herdr.Config, action *pb.AgentControl) (*pb.AgentControlResult, error) {
		if err := config.CheckSession(ctx); err != nil {
			return nil, err
		}
		return &pb.AgentControlResult{Target: action.Target, ObservedStatus: "idle"}, nil
	}
	interrupted, err := h.handle(ctx, interrupt, time.Now())
	if err != nil || interrupted.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatalf("startup blocked interrupt: %v %v", interrupted, err)
	}
	stop := probe()
	stop.CommandType = protocol.AgentStopCommandType
	stop.AgentStop = &pb.AgentStop{WorkspaceId: "ws:1", TabId: "tab:2", Provider: "copilot", Target: interrupt.AgentControl.Target}
	stop.SubmittedRequest = &pb.SubmitCommandRequest{NodeInstanceId: stop.TargetId, IdempotencyKey: stop.IdempotencyKey,
		CommandType: stop.CommandType, Ttl: stop.Ttl, AgentStop: proto.Clone(stop.AgentStop).(*pb.AgentStop)}
	h.stopAgent = func(ctx context.Context, config herdr.Config, request herdr.StopAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
		value := initialLifecycle()
		value.Handle = request.Handle
		for _, before := range []bool{true, false} {
			if err := lifecycleEmit(ctx, config, hook, &value, herdr.LifecycleClosePane, before); err != nil {
				return value, err
			}
		}
		return value, nil
	}
	stopped, err := h.handle(ctx, stop, time.Now())
	if err != nil || stopped.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatalf("startup blocked pinned stop: %v %v", stopped, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
