package herdr

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func lifecycleBeforeStageSteps(request StartAgentRequest, stage LifecycleStage) []step {
	switch stage {
	case LifecycleCreatePane:
		return lifecyclePrepareSteps(request)[:3]
	case LifecycleLaunch:
		return lifecyclePrepareSteps(request)[:7]
	case LifecyclePrompt:
		return append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agentFixture()))
	default:
		return lifecycleStopSteps()[:4]
	}
}

func lifecycleRefreshedStageSteps(request StartAgentRequest, stage LifecycleStage) []step {
	switch stage {
	case LifecycleCreatePane:
		return lifecyclePrepareSteps(request)[:5]
	case LifecycleLaunch:
		return lifecyclePrepareSteps(request)
	case LifecyclePrompt:
		return append(lifecycleBeforeStageSteps(request, stage), agentGetStep(agentFixture()))
	default:
		return lifecycleStopSteps()
	}
}

func lifecycleCompletedStageSteps(request StartAgentRequest, stage LifecycleStage) []step {
	switch stage {
	case LifecycleCreatePane:
		return lifecyclePrepareSteps(request)[:6]
	case LifecycleLaunch:
		return lifecycleLaunchSteps(request, agentFixture())
	case LifecyclePrompt:
		return append(lifecycleRefreshedStageSteps(request, stage),
			agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": agentFixture()},
				map[string]any{"target": "pane:1", "text": request.InitialPrompt}))
	default:
		return append(lifecycleStopSteps(), lifecycleCloseStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleWorkspaceStep())
	}
}

func lifecycleRunFencedStage(ctx context.Context, config Config, request StartAgentRequest, stage LifecycleStage,
	hook LifecycleHook, dial dialFunc) (LifecycleResult, error) {
	if stage == LifecycleClosePane {
		return stopAgent(ctx, config, lifecycleStopRequest(), hook, dial)
	}
	return startAgent(ctx, config, request, hook, dial)
}

func lifecycleAssertEarlierEvidence(t *testing.T, stage LifecycleStage, result LifecycleResult) {
	t.Helper()
	if stage == LifecycleLaunch || stage == LifecyclePrompt {
		if result.Pane != LifecycleConfirmed || result.Handle.PaneID != "pane:1" || result.Handle.TerminalID != "terminal:1" {
			t.Fatal("earlier confirmed pane evidence was lost")
		}
	}
	if stage == LifecyclePrompt && (result.Launch != LifecycleConfirmed || result.Handle.Provider != "copilot" ||
		result.Handle.AgentSessionID == "") {
		t.Fatal("earlier confirmed launch or provider-session evidence was lost")
	}
	if stage == LifecycleClosePane && result.Handle != lifecycleStopRequest().Handle {
		t.Fatal("stop lost the explicitly selected original handle")
	}
}

func TestLifecycleSessionRestartInBeforeHookPersistsNoDispatch(t *testing.T) {
	for _, stage := range []LifecycleStage{LifecycleCreatePane, LifecycleLaunch, LifecyclePrompt, LifecycleClosePane} {
		for _, failCorrection := range []bool{false, true} {
			name := string(stage)
			if failCorrection {
				name += "_correction_failure"
			}
			t.Run(name, func(t *testing.T) {
				request := lifecycleRequest(t)
				request.InitialPrompt = "must not be resent"
				config := testConfig()
				replaced := false
				sessionErr := errors.New("selected session replaced")
				config.CheckSession = func(context.Context) error {
					if replaced {
						return sessionErr
					}
					return nil
				}
				var persisted []LifecycleEvent
				hook := func(_ context.Context, event LifecycleEvent) error {
					if event.Stage == stage && !event.Before && failCorrection {
						return errors.New("correction not persisted")
					}
					persisted = append(persisted, event)
					if event.Before && event.Stage == stage {
						replaced = true
					}
					return nil
				}
				steps := lifecycleBeforeStageSteps(request, stage)
				result, err := lifecycleRunFencedStage(context.Background(), config, request, stage, hook, agentDial(t, steps...))
				if !errors.Is(err, sessionErr) {
					t.Fatalf("lost session rejection: %v", err)
				}
				last := persisted[len(persisted)-1]
				want := LifecycleNotAttempted
				if failCorrection {
					want = LifecycleUnknown
					if !errors.Is(err, ErrLifecyclePersistence) || !errors.Is(err, ErrAgentIndeterminate) || !last.Before {
						t.Fatal("failed correction did not preserve durable uncertainty")
					}
				} else if last.Before {
					t.Fatal("proven pre-dispatch rejection was not persisted")
				}
				if *lifecycleStageOutcome(&result, stage) != want || *lifecycleStageOutcome(&last.Result, stage) != want {
					t.Fatalf("wrong evidence result=%+v checkpoint=%+v", result, last)
				}
				lifecycleAssertEarlierEvidence(t, stage, result)
			})
		}
	}
}

func TestLifecycleSessionRestartDuringRefreshRejectsBeforeIPC(t *testing.T) {
	for _, stage := range []LifecycleStage{LifecycleCreatePane, LifecycleLaunch, LifecyclePrompt, LifecycleClosePane} {
		t.Run(string(stage), func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "must not be sent"
			config := testConfig()
			var replaced atomic.Bool
			sessionErr := errors.New("session replaced during target refresh")
			config.CheckSession = func(context.Context) error {
				if replaced.Load() {
					return sessionErr
				}
				return nil
			}
			steps := lifecycleRefreshedStageSteps(request, stage)
			last := steps[len(steps)-1]
			steps[len(steps)-1] = step{last.method, func(conn net.Conn, request testRequest) {
				replaced.Store(true)
				last.serve(conn, request)
			}}
			var events []LifecycleEvent
			result, err := lifecycleRunFencedStage(context.Background(), config, request, stage, lifecycleJournal(&events), agentDial(t, steps...))
			if !errors.Is(err, sessionErr) || *lifecycleStageOutcome(&result, stage) != LifecycleNotAttempted ||
				events[len(events)-1].Before {
				t.Fatalf("refresh crossed session fence: result=%+v err=%v", result, err)
			}
			lifecycleAssertEarlierEvidence(t, stage, result)
		})
	}
}

func TestLifecyclePostEffectSessionRestartPreservesOnlyPriorEvidence(t *testing.T) {
	for _, stage := range []LifecycleStage{LifecycleCreatePane, LifecycleLaunch, LifecyclePrompt, LifecycleClosePane} {
		t.Run(string(stage), func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "test task"
			config := testConfig()
			var replaced atomic.Bool
			sessionErr := errors.New("post-effect session replaced")
			config.CheckSession = func(context.Context) error {
				if replaced.Load() {
					return sessionErr
				}
				return nil
			}
			method := map[LifecycleStage]string{LifecycleCreatePane: "tab.create", LifecycleLaunch: "agent.start",
				LifecyclePrompt: "agent.prompt", LifecycleClosePane: "pane.close"}[stage]
			steps := lifecycleCompletedStageSteps(request, stage)
			for i, original := range steps {
				if original.method == method {
					steps[i] = step{original.method, func(conn net.Conn, request testRequest) {
						replaced.Store(true)
						original.serve(conn, request)
					}}
					break
				}
			}
			var events []LifecycleEvent
			result, err := lifecycleRunFencedStage(context.Background(), config, request, stage, lifecycleJournal(&events), agentDial(t, steps...))
			if !errors.Is(err, sessionErr) || !errors.Is(err, ErrAgentIndeterminate) ||
				*lifecycleStageOutcome(&result, stage) != LifecycleUnknown {
				t.Fatalf("unverified session effect was confirmed: result=%+v err=%v", result, err)
			}
			if stage == LifecycleCreatePane && result.Handle.PaneID != "" {
				t.Fatal("adopted a pane from an unverified incarnation")
			}
			if stage == LifecycleLaunch && result.Handle.Provider != "unknown" {
				t.Fatal("adopted provider evidence from an unverified incarnation")
			}
			last := events[len(events)-1]
			if last.Before || !reflect.DeepEqual(last.Result, result) {
				t.Fatal("post-effect uncertainty was not persisted")
			}
			lifecycleAssertEarlierEvidence(t, stage, result)
		})
	}
}

func TestLifecycleRefreshesTargetsAfterBeforeHook(t *testing.T) {
	for _, tc := range []struct {
		stage LifecycleStage
		kind  string
		want  error
	}{
		{LifecycleCreatePane, "workspace", ErrAgentChanged},
		{LifecycleLaunch, "terminal", ErrAgentChanged},
		{LifecycleLaunch, "busy", ErrAgentBusy},
		{LifecyclePrompt, "terminal", ErrAgentChanged},
		{LifecyclePrompt, "provider_session", ErrAgentChanged},
		{LifecyclePrompt, "provider", ErrAgentChanged},
		{LifecyclePrompt, "stopped", ErrAgentUnavailable},
		{LifecyclePrompt, "blocked", ErrAgentBlocked},
		{LifecycleClosePane, "last_pane", ErrLifecycleLastPane},
		{LifecycleClosePane, "provider_session", ErrAgentChanged},
		{LifecycleClosePane, "stopped", ErrAgentUnavailable},
	} {
		t.Run(string(tc.stage)+"_"+tc.kind, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "must not be sent"
			steps := lifecycleBeforeStageSteps(request, tc.stage)
			var refresh []step
			switch tc.stage {
			case LifecycleCreatePane:
				refresh = []step{agentReply("workspace.get", "workspace_info",
					map[string]any{"workspace": workspaceInfo("ws:replaced", "")}, map[string]any{"workspace_id": "ws:1"})}
			case LifecycleLaunch:
				pane := lifecycleShell()
				if tc.kind == "terminal" {
					pane["terminal_id"] = "replaced"
				} else {
					pane["agent_status"] = "working"
				}
				refresh = []step{lifecyclePaneStep(pane)}
			case LifecyclePrompt:
				agent := agentFixture()
				switch tc.kind {
				case "terminal":
					agent["terminal_id"] = "replaced"
				case "provider_session":
					agent["agent_session"].(map[string]any)["value"] = "replaced"
				case "provider":
					agent["agent"], agent["agent_session"] = "claude", nil
				case "blocked":
					agent["agent_status"] = "blocked"
				}
				refresh = []step{agentGetStep(agent)}
				if tc.kind == "stopped" {
					refresh = []step{lifecycleAPIError("agent.get", "not_found")}
				}
			case LifecycleClosePane:
				refresh = []step{lifecycleWorkspaceStep()}
				switch tc.kind {
				case "last_pane":
					refresh = append(refresh, lifecycleListStep(agentFixture()))
				case "stopped":
					refresh = append(refresh, lifecycleListStep(lifecycleOtherPane()))
				default:
					agent := agentFixture()
					agent["agent_session"].(map[string]any)["value"] = "replaced"
					refresh = append(refresh, lifecycleListStep(agentFixture(), lifecycleOtherPane()), agentGetStep(agent))
				}
			}
			hookRan := false
			first := refresh[0]
			refresh[0] = step{first.method, func(conn net.Conn, request testRequest) {
				if !hookRan {
					t.Error("target was not refreshed after the durable before-hook")
				}
				first.serve(conn, request)
			}}
			steps = append(steps, refresh...)
			var events []LifecycleEvent
			hook := func(_ context.Context, event LifecycleEvent) error {
				events = append(events, event)
				if event.Before && event.Stage == tc.stage {
					hookRan = true
				}
				return nil
			}
			result, err := lifecycleRunFencedStage(context.Background(), testConfig(), request, tc.stage, hook, agentDial(t, steps...))
			if !errors.Is(err, tc.want) || *lifecycleStageOutcome(&result, tc.stage) != LifecycleNotAttempted {
				t.Fatalf("stale target dispatched: result=%+v err=%v", result, err)
			}
			last := events[len(events)-1]
			if last.Before || *lifecycleStageOutcome(&last.Result, tc.stage) != LifecycleNotAttempted {
				t.Fatal("target rejection did not correct the persisted unknown intent")
			}
			lifecycleAssertEarlierEvidence(t, tc.stage, result)
		})
	}
}

func TestLifecycleCreateRefreshesCheckoutAfterPersistence(t *testing.T) {
	request := lifecycleRequest(t)
	archived := filepath.Join(t.TempDir(), "old-checkout")
	var events []LifecycleEvent
	hook := func(_ context.Context, event LifecycleEvent) error {
		events = append(events, event)
		if event.Before {
			if err := os.Rename(request.Cwd, archived); err != nil {
				return err
			}
			return os.Mkdir(request.Cwd, 0700)
		}
		return nil
	}
	result, err := startAgent(context.Background(), testConfig(), request, hook,
		agentDial(t, lifecycleBeforeStageSteps(request, LifecycleCreatePane)...))
	if !errors.Is(err, ErrAgentPrecondition) || result.Pane != LifecycleNotAttempted ||
		len(events) != 2 || events[1].Before || events[1].Result.Pane != LifecycleNotAttempted {
		t.Fatalf("replaced checkout accepted: result=%+v err=%v", result, err)
	}
}

func TestLifecycleCancellationAfterIntentPersistsNoDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var events []LifecycleEvent
	hook := func(ctx context.Context, event LifecycleEvent) error {
		if event.Before {
			cancel()
		} else if ctx.Err() != nil {
			t.Fatal("correction received a canceled persistence context")
		}
		events = append(events, event)
		return nil
	}
	result := newLifecycleResult()
	err := lifecycleEffect(ctx, hook, LifecycleCreatePane, &result, func() error {
		t.Fatal("effect ran after before-hook cancellation")
		return nil
	})
	if !errors.Is(err, context.Canceled) || result.Pane != LifecycleNotAttempted ||
		len(events) != 2 || events[1].Result.Pane != LifecycleNotAttempted {
		t.Fatalf("missing no-dispatch correction: result=%+v err=%v", result, err)
	}
}

func TestLifecycleSessionRestartInAfterHookRejectsNextEffect(t *testing.T) {
	for _, next := range []LifecycleStage{LifecycleLaunch, LifecyclePrompt} {
		t.Run(string(next), func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "must not be sent"
			previous := LifecycleCreatePane
			if next == LifecyclePrompt {
				previous = LifecycleLaunch
			}
			replaced := false
			sessionErr := errors.New("session replaced during after-hook")
			config := testConfig()
			config.CheckSession = func(context.Context) error {
				if replaced {
					return sessionErr
				}
				return nil
			}
			var events []LifecycleEvent
			hook := func(_ context.Context, event LifecycleEvent) error {
				events = append(events, event)
				if !event.Before && event.Stage == previous {
					replaced = true
				}
				return nil
			}
			result, err := startAgent(context.Background(), config, request, hook,
				agentDial(t, lifecycleBeforeStageSteps(request, next)...))
			if !errors.Is(err, sessionErr) || *lifecycleStageOutcome(&result, next) != LifecycleNotAttempted ||
				events[len(events)-1].Before {
				t.Fatalf("after-hook replacement escaped fence: result=%+v err=%v", result, err)
			}
			lifecycleAssertEarlierEvidence(t, next, result)
		})
	}
}

func TestLifecyclePostSessionCheckSurvivesMutationCancellation(t *testing.T) {
	request := lifecycleRequest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := testConfig()
	postChecked := false
	config.CheckSession = func(checkCtx context.Context) error {
		if ctx.Err() != nil {
			postChecked = true
			if _, ok := checkCtx.Deadline(); !ok || checkCtx.Err() != nil {
				t.Error("post-effect session verification did not receive a fresh bounded context")
			}
		}
		return nil
	}
	steps := append(lifecyclePrepareSteps(request), step{"agent.start", func(conn net.Conn, _ testRequest) {
		cancel()
		conn.Close()
	}})
	var events []LifecycleEvent
	result, err := startAgent(ctx, config, request, lifecycleJournal(&events), agentDial(t, steps...))
	if !postChecked || !errors.Is(err, context.Canceled) || !errors.Is(err, ErrAgentIndeterminate) ||
		result.Pane != LifecycleConfirmed || result.Launch != LifecycleUnknown ||
		len(events) != 4 || events[3].Before {
		t.Fatalf("canceled effect lost verification or evidence: result=%+v err=%v", result, err)
	}
}

func TestLifecycleChecksSessionAroundEveryEffect(t *testing.T) {
	for _, stage := range []LifecycleStage{LifecyclePrompt, LifecycleClosePane} {
		t.Run(string(stage), func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "test task"
			config := testConfig()
			var current LifecycleStage
			checks := map[LifecycleStage]int{}
			config.CheckSession = func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
					t.Error("session check was unbounded or canceled")
				}
				checks[current]++
				return nil
			}
			hook := func(_ context.Context, event LifecycleEvent) error {
				if event.Before {
					current = event.Stage
				} else if checks[event.Stage] != 3 {
					t.Error("missing pre-refresh, pre-IPC, or post-effect session check")
				}
				return nil
			}
			steps := lifecycleCompletedStageSteps(request, stage)
			for i, original := range steps {
				switch original.method {
				case "tab.create", "agent.start", "agent.prompt", "pane.close":
					steps[i] = step{original.method, func(conn net.Conn, request testRequest) {
						if checks[current] != 2 {
							t.Error("mutation was not immediately preceded by session revalidation")
						}
						original.serve(conn, request)
					}}
				}
			}
			result, err := lifecycleRunFencedStage(context.Background(), config, request, stage, hook, agentDial(t, steps...))
			if err != nil || *lifecycleStageOutcome(&result, stage) != LifecycleConfirmed {
				t.Fatalf("valid fenced operation failed: result=%+v err=%v", result, err)
			}
		})
	}
}
