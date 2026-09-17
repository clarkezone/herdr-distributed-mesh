package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
)

type lifecycleAfterCheckpoint struct {
	commandJournal
	lifecycleJournal
	after          func(*pb.AgentLifecycleReceipt)
	failCorrection bool
}

func (j lifecycleAfterCheckpoint) SaveLifecycle(ctx context.Context, id string, value *pb.AgentLifecycleReceipt) error {
	if j.failCorrection && value.Sequence == 4 {
		return errors.New("correction could not be persisted")
	}
	if err := j.lifecycleJournal.SaveLifecycle(ctx, id, value); err != nil {
		return err
	}
	j.after(value)
	return nil
}

func TestLifecycleNodeRefreshAfterDurableIntentAndFailedCorrection(t *testing.T) {
	for _, replacement := range []string{"session", "workspace"} {
		for _, failCorrection := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/correction-failure=%t", replacement, failCorrection), func(t *testing.T) {
				h, manager := lifecycleHandler(t)
				moved := false
				other := t.TempDir()
				h.resolveLifecycleWorkspace = func(_ context.Context, _ herdr.Config, _ string, binding projects.Binding) (string, error) {
					if moved {
						return other, nil
					}
					return binding.Path, nil
				}
				h.journal = lifecycleAfterCheckpoint{commandJournal: h.journal, lifecycleJournal: h.journal.(lifecycleJournal),
					failCorrection: failCorrection, after: func(value *pb.AgentLifecycleReceipt) {
						if value.Sequence == 3 {
							if replacement == "session" {
								manager.replace("one")
							} else {
								moved = true
							}
						}
					}}
				launches, prompts := 0, 0
				h.startAgent = func(ctx context.Context, config herdr.Config, _ herdr.StartAgentRequest, hook herdr.LifecycleHook) (herdr.LifecycleResult, error) {
					value := initialLifecycle()
					for _, before := range []bool{true, false} {
						if err := lifecycleEmit(ctx, config, hook, &value, herdr.LifecycleCreatePane, before); err != nil {
							return value, err
						}
					}
					if err := lifecycleEmit(ctx, config, hook, &value, herdr.LifecycleLaunch, true); err != nil {
						value.Launch = herdr.LifecycleNotAttempted
						correction := hook(ctx, herdr.LifecycleEvent{Stage: herdr.LifecycleLaunch, Result: value})
						if correction != nil {
							return value, errors.Join(err, correction, herdr.ErrLifecyclePersistence)
						}
						return value, err
					}
					launches++
					prompts++
					return value, nil
				}
				result, err := h.handle(context.Background(), lifecycleStartCommand(t), time.Now())
				if err != nil || launches != 0 || prompts != 0 || result.AgentLifecycle.GetPaneOutcome() != "confirmed" {
					t.Fatalf("replacement dispatched or lost confirmed prefix: %v %v", result, err)
				}
				want := "not_attempted"
				if failCorrection {
					want = "unknown"
				}
				if result.AgentLifecycle.LaunchOutcome != want {
					t.Fatalf("incorrect durable correction: %v", result)
				}
			})
		}
	}
}
