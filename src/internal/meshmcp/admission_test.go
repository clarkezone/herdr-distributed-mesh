package meshmcp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type pendingTool struct {
	cancel context.CancelFunc
	done   <-chan toolOutcome
}

type toolOutcome struct {
	result *mcp.CallToolResult
	err    error
}

func beginTool(t *testing.T, client *mcp.ClientSession, operation Operation, input any) pendingTool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	done := make(chan toolOutcome, 1)
	go func() {
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: string(operation), Arguments: input})
		done <- toolOutcome{result, err}
	}()
	return pendingTool{cancel, done}
}

func finishTool(t *testing.T, pending pendingTool) toolOutcome {
	t.Helper()
	select {
	case outcome := <-pending.done:
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatal("tool call did not finish")
		return toolOutcome{}
	}
}

func expectAdmission(t *testing.T, started <-chan Operation, operation Operation) {
	t.Helper()
	select {
	case got := <-started:
		if got != operation {
			t.Fatalf("admitted %s, want %s", got, operation)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not reach the injected application", operation)
	}
}

func trackActive(active, peak *atomic.Int32) func() {
	current := active.Add(1)
	for {
		previous := peak.Load()
		if previous >= current || peak.CompareAndSwap(previous, current) {
			break
		}
	}
	return func() { active.Add(-1) }
}

func expectCapacityRecovery(t *testing.T, client *mcp.ClientSession, operation Operation, input any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: string(operation), Arguments: input})
		if err != nil || result == nil {
			t.Fatalf("capacity did not recover: result=%v err=%v", result, err)
		}
		if !result.IsError {
			return
		}
		if len(result.Content) == 0 || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "active call limit") {
			t.Fatalf("unexpected capacity recovery error: %+v", result.Content)
		}
		select {
		case <-ctx.Done():
			t.Fatal("cancelled callback did not release its lane")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestCriticalLaneSurvivesOrdinarySaturationAndCancellation(t *testing.T) {
	for _, urgentOperation := range []Operation{InterruptAgent, StopAgent} {
		t.Run(string(urgentOperation), func(t *testing.T) {
			otherUrgent := StopAgent
			if urgentOperation == StopAgent {
				otherUrgent = InterruptAgent
			}
			const total = 3
			var active, peak atomic.Int32
			started := make(chan Operation, 8)
			ordinaryRelease, urgentRelease := make(chan struct{}), make(chan struct{})
			server, err := New(func(ctx context.Context, operation Operation, _ any) (any, error) {
				defer trackActive(&active, &peak)()
				started <- operation
				var release <-chan struct{}
				switch operation {
				case WaitAgent, StartAgent:
					release = ordinaryRelease
				case urgentOperation:
					release = urgentRelease
				default:
					return map[string]bool{"completed": true}, nil
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return map[string]bool{"completed": true}, nil
				}
			}, Options{MaxActiveCalls: total, CallTimeout: 4 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			first, second := connectServer(t, server), connectServer(t, server)
			waitInput := fixtures()[WaitAgent].(WaitAgentInput)
			waitInput.TimeoutMS = MaxWaitMilliseconds
			waiting := beginTool(t, first, WaitAgent, waitInput)
			expectAdmission(t, started, WaitAgent)
			startup := beginTool(t, second, StartAgent, fixtures()[StartAgent])
			expectAdmission(t, started, StartAgent)
			errorText(t, callTool(t, second, ListNodes, PageInput{}), "ordinary active call limit")

			urgent := beginTool(t, first, urgentOperation, fixtures()[urgentOperation])
			expectAdmission(t, started, urgentOperation)
			if active.Load() != total {
				t.Fatalf("reserved lane did not use the configured total: active=%d", active.Load())
			}
			errorText(t, callTool(t, second, otherUrgent, fixtures()[otherUrgent]), "critical-control active call limit")
			errorText(t, callTool(t, second, ListNodes, PageInput{}), "ordinary active call limit")
			if _, err := second.ListTools(context.Background(), nil); err != nil {
				t.Fatalf("discovery was blocked by application saturation: %v", err)
			}

			waiting.cancel()
			if outcome := finishTool(t, waiting); !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("ordinary call cancellation changed: %+v", outcome)
			}
			expectCapacityRecovery(t, second, ListNodes, PageInput{})
			// Recovery of ordinary capacity must not steal or release the occupied
			// critical slot.
			errorText(t, callTool(t, second, otherUrgent, fixtures()[otherUrgent]), "critical-control active call limit")

			urgent.cancel()
			if outcome := finishTool(t, urgent); !errors.Is(outcome.err, context.Canceled) {
				t.Fatalf("critical call cancellation changed: %+v", outcome)
			}
			expectCapacityRecovery(t, second, otherUrgent, fixtures()[otherUrgent])
			close(ordinaryRelease)
			if outcome := finishTool(t, startup); outcome.err != nil || outcome.result == nil || outcome.result.IsError {
				t.Fatalf("ordinary startup did not complete: %+v", outcome)
			}
			if active.Load() != 0 || peak.Load() != total {
				t.Fatalf("configured TOTAL bound changed or callbacks leaked: active=%d peak=%d total=%d", active.Load(), peak.Load(), total)
			}
		})
	}
}

func TestCriticalTimeoutRetainsSlotUntilCallbackExits(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var active, peak, criticalCalls atomic.Int32
	client := connect(t, func(_ context.Context, operation Operation, _ any) (any, error) {
		defer trackActive(&active, &peak)()
		if operation == StopAgent {
			if criticalCalls.Add(1) == 1 {
				close(started)
			}
			<-release
		}
		return map[string]bool{"completed": true}, nil
	}, Options{MaxActiveCalls: 2, CallTimeout: 100 * time.Millisecond})
	stopping := beginTool(t, client, StopAgent, fixtures()[StopAgent])
	await(t, started)
	outcome := finishTool(t, stopping)
	if outcome.err != nil || outcome.result == nil {
		t.Fatalf("missing bounded timeout result: %+v", outcome)
	}
	errorText(t, outcome.result, "deadline exceeded")
	for range 2 {
		errorText(t, callTool(t, client, InterruptAgent, fixtures()[InterruptAgent]), "critical-control active call limit")
		errorText(t, callTool(t, client, StopAgent, fixtures()[StopAgent]), "critical-control active call limit")
	}
	if result := callTool(t, client, ListNodes, PageInput{}); result.IsError {
		t.Fatalf("critical timeout consumed the ordinary lane: %+v", result.Content)
	}
	if criticalCalls.Load() != 1 || active.Load() != 1 || peak.Load() != 2 {
		t.Fatalf("timed-out critical work exceeded the total or released early: calls=%d active=%d peak=%d",
			criticalCalls.Load(), active.Load(), peak.Load())
	}
}

func TestCriticalReservationRejectsIncompatibleTotal(t *testing.T) {
	_, err := New(func(context.Context, Operation, any) (any, error) { return []string{}, nil }, Options{MaxActiveCalls: 1})
	if err == nil || !strings.Contains(err.Error(), "one total slot is reserved") {
		t.Fatalf("one-slot configuration was silently raised or unclearly rejected: %v", err)
	}
}
