package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func lifecycleRequest(t *testing.T) StartAgentRequest {
	t.Helper()
	return StartAgentRequest{WorkspaceID: "ws:1", Cwd: t.TempDir(), Name: "mesh-agent", Provider: "copilot"}
}

func lifecycleShell() map[string]any {
	value := agentFixture()
	delete(value, "agent")
	delete(value, "agent_session")
	value["agent_status"] = "unknown"
	delete(value, "interactive_ready")
	delete(value, "launch_pending")
	return value
}

func lifecycleOtherPane() map[string]any {
	value := lifecycleShell()
	value["pane_id"], value["terminal_id"], value["tab_id"] = "pane:other", "terminal:other", "tab:other"
	return value
}

func lifecycleWorkspaceStep() step {
	return agentReply("workspace.get", "workspace_info",
		map[string]any{"workspace": workspaceInfo("ws:1", "")}, map[string]any{"workspace_id": "ws:1"})
}

func lifecycleListStep(panes ...map[string]any) step {
	if panes == nil {
		panes = []map[string]any{}
	}
	return agentReply("pane.list", "pane_list", map[string]any{"panes": panes}, map[string]any{})
}

func lifecycleCreateStep(request StartAgentRequest, pane map[string]any) step {
	tab := map[string]any{"tab_id": "tab:1", "workspace_id": "ws:1", "pane_count": 1,
		"number": 2, "label": "", "focused": false, "agent_status": "unknown"}
	return agentReply("tab.create", "tab_created", map[string]any{"tab": tab, "root_pane": pane},
		map[string]any{"workspace_id": request.WorkspaceID, "cwd": request.Cwd, "focus": false})
}

func lifecyclePaneStep(pane map[string]any) step {
	return agentReply("pane.get", "pane_info", map[string]any{"pane": pane}, map[string]any{"pane_id": "pane:1"})
}

func lifecycleStartStep(request StartAgentRequest, agent map[string]any) step {
	timeout := request.StartupTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	args := []string{}
	if request.Provider == "copilot" {
		args = []string{"--no-auto-update"}
	}
	return agentReply("agent.start", "agent_started",
		map[string]any{"agent": agent, "argv": append([]string{request.Provider}, args...)},
		map[string]any{"name": request.Name, "kind": request.Provider, "pane_id": "pane:1",
			"args": args, "timeout_ms": uint64(timeout / time.Millisecond)})
}

func lifecyclePrepareSteps(request StartAgentRequest) []step {
	return []step{agentPongStep(), lifecycleWorkspaceStep(), lifecycleListStep(lifecycleOtherPane()),
		lifecycleWorkspaceStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleCreateStep(request, lifecycleShell()),
		lifecyclePaneStep(lifecycleShell()), lifecyclePaneStep(lifecycleShell())}
}

func lifecycleJournal(events *[]LifecycleEvent) LifecycleHook {
	return func(ctx context.Context, event LifecycleEvent) error {
		if _, bounded := ctx.Deadline(); !bounded || ctx.Err() != nil {
			return errors.New("invalid persistence context")
		}
		*events = append(*events, event)
		return nil
	}
}

func lifecycleLaunchSteps(request StartAgentRequest, agent map[string]any) []step {
	return append(lifecyclePrepareSteps(request), lifecycleStartStep(request, agent), agentGetStep(agent))
}

func lifecycleDiscard(context.Context, LifecycleEvent) error { return nil }

func lifecycleLostResponse(method string) step {
	return step{method, func(conn net.Conn, request testRequest) { conn.Close() }}
}

func lifecycleAPIError(method, code string) step {
	return step{method, func(conn net.Conn, request testRequest) {
		json.NewEncoder(conn).Encode(map[string]any{"id": request.ID,
			"error": map[string]any{"code": code, "message": "PRIVATE-native-error"}})
	}}
}

func TestLifecycleStartActualSchemaAndSeparatePrompt(t *testing.T) {
	request := lifecycleRequest(t)
	request.InitialPrompt = "PRIVATE-prompt"
	working := agentFixture()
	working["agent_status"] = "working"
	steps := append(lifecycleLaunchSteps(request, agentFixture()),
		agentGetStep(agentFixture()), agentGetStep(agentFixture()),
		agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": working},
			map[string]any{"target": "pane:1", "text": request.InitialPrompt}))
	var events []LifecycleEvent
	result, err := startAgent(context.Background(), testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
	if err != nil || result.Pane != LifecycleConfirmed || result.Launch != LifecycleConfirmed ||
		result.Prompt != LifecycleConfirmed || result.Stop != LifecycleNotAttempted ||
		result.Handle.TerminalID != "terminal:1" || result.Handle.AgentSessionID == "" || result.ObservedStatus != "working" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(events) != 6 {
		t.Fatalf("events=%v", events)
	}
	for i, stage := range []LifecycleStage{LifecycleCreatePane, LifecycleLaunch, LifecyclePrompt} {
		before, after := events[2*i], events[2*i+1]
		if before.Stage != stage || !before.Before || after.Stage != stage || after.Before ||
			*lifecycleStageOutcome(&before.Result, stage) != LifecycleUnknown ||
			*lifecycleStageOutcome(&after.Result, stage) != LifecycleConfirmed {
			t.Fatal("incorrect intent/outcome ordering")
		}
	}
	raw, _ := json.Marshal(events)
	if strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), request.Cwd) {
		t.Fatal("journal leaked prompt, output, name, or checkout")
	}
}

func TestLifecycleStartSupportedProviderAndTimeoutBounds(t *testing.T) {
	for _, provider := range []string{"copilot", "claude", "codex", "gemini", "pi", "opencode"} {
		t.Run(provider, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.Provider, request.StartupTimeout = provider, 3001*time.Millisecond
			agent := agentFixture()
			agent["agent"], agent["agent_session"] = provider, nil
			steps := lifecycleLaunchSteps(request, agent)
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if err != nil || result.Launch != LifecycleConfirmed || result.Prompt != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	for _, mutate := range []func(*StartAgentRequest){
		func(r *StartAgentRequest) { r.Provider = "powershell" },
		func(r *StartAgentRequest) { r.Provider = "copilot --allow-all" },
		func(r *StartAgentRequest) { r.Name = "a;echo x" },
		func(r *StartAgentRequest) { r.WorkspaceID = "" },
		func(r *StartAgentRequest) { r.Cwd = "relative" },
		func(r *StartAgentRequest) { r.StartupTimeout = 3 * time.Second },
		func(r *StartAgentRequest) { r.StartupTimeout = 3000999 * time.Microsecond },
		func(r *StartAgentRequest) { r.StartupTimeout = 5*time.Minute + time.Nanosecond },
		func(r *StartAgentRequest) { r.StartupTimeout = -time.Second },
		func(r *StartAgentRequest) { r.InitialPrompt = " \n " },
		func(r *StartAgentRequest) { r.InitialPrompt = "\x1b[31m" },
		func(r *StartAgentRequest) { r.InitialPrompt = strings.Repeat("x", 8193) },
	} {
		request := lifecycleRequest(t)
		mutate(&request)
		result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t))
		if !errors.Is(err, ErrAgentPrecondition) || result.Pane != LifecycleNotAttempted {
			t.Fatalf("invalid request result=%+v err=%v", result, err)
		}
	}
}

func TestLifecycleLostMutationResponsesNeverRetry(t *testing.T) {
	for _, method := range []string{"tab.create", "agent.start", "agent.prompt"} {
		t.Run(method, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "task"
			steps := lifecyclePrepareSteps(request)
			switch method {
			case "tab.create":
				steps = append(steps[:5], lifecycleLostResponse(method))
			case "agent.start":
				steps = append(steps, lifecycleLostResponse(method))
			case "agent.prompt":
				steps = append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agentFixture()), agentGetStep(agentFixture()), lifecycleLostResponse(method))
			}
			var events []LifecycleEvent
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) || events[len(events)-1].Before {
				t.Fatalf("result=%+v err=%v events=%v", result, err, events)
			}
			switch method {
			case "tab.create":
				if result.Pane != LifecycleUnknown || result.Launch != LifecycleNotAttempted || result.Handle.PaneID != "" {
					t.Fatal(result)
				}
			case "agent.start":
				if result.Pane != LifecycleConfirmed || result.Launch != LifecycleUnknown || result.Prompt != LifecycleNotAttempted {
					t.Fatal(result)
				}
			case "agent.prompt":
				if result.Launch != LifecycleConfirmed || result.Prompt != LifecycleUnknown {
					t.Fatal(result)
				}
			}
		})
	}
}

func TestLifecycleBusyShellAndMissingProviderAreNotRetried(t *testing.T) {
	for _, code := range []string{"shell_not_ready", "agent_not_found", "startup_timeout"} {
		t.Run(code, func(t *testing.T) {
			request := lifecycleRequest(t)
			steps := append(lifecyclePrepareSteps(request), lifecycleAPIError("agent.start", code))
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) || result.Pane != LifecycleConfirmed || result.Launch != LifecycleUnknown ||
				result.Prompt != LifecycleNotAttempted || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleRejectsBusyOrReplacedCreatedPane(t *testing.T) {
	for _, field := range []string{"terminal_id", "workspace_id", "tab_id", "agent", "agent_status"} {
		t.Run(field, func(t *testing.T) {
			request := lifecycleRequest(t)
			pane := lifecycleShell()
			switch field {
			case "agent":
				pane[field] = "claude"
			case "agent_status":
				pane[field] = "working"
			default:
				pane[field] = "replaced"
			}
			steps := lifecyclePrepareSteps(request)
			steps[len(steps)-1] = lifecyclePaneStep(pane)
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if err == nil || result.Pane != LifecycleConfirmed || result.Launch != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleLaunchIdentityIsRequired(t *testing.T) {
	for _, field := range []string{"terminal_id", "workspace_id", "tab_id", "agent"} {
		t.Run(field, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "task"
			agent := agentFixture()
			agent[field] = "replaced"
			if field == "agent" {
				agent["agent_session"] = nil
			}
			steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, agent))
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) || result.Launch != LifecycleUnknown || result.Prompt != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecyclePromptPreflightPreservesSuccessfulLaunch(t *testing.T) {
	for _, state := range []string{"working", "blocked", "unknown", "replaced", "session_changed", "not_ready"} {
		t.Run(state, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "task"
			agent := agentFixture()
			switch state {
			case "replaced":
				agent["terminal_id"] = "new-terminal"
			case "session_changed":
				agent["agent_session"].(map[string]any)["value"] = "new-session"
			case "not_ready":
				agent["interactive_ready"] = false
			default:
				agent["agent_status"] = state
			}
			steps := append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agent))
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if err == nil || result.Launch != LifecycleConfirmed || result.Prompt != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecyclePersistenceGatesEachEffect(t *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4, 5, 6} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "task"
			steps := lifecyclePrepareSteps(request)
			switch failAt {
			case 1:
				steps = steps[:3]
			case 2:
				steps = steps[:6]
			case 3:
				steps = steps[:7]
			case 4:
				steps = lifecycleLaunchSteps(request, agentFixture())
			case 5:
				steps = append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agentFixture()))
			case 6:
				steps = append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agentFixture()), agentGetStep(agentFixture()),
					agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": agentFixture()},
						map[string]any{"target": "pane:1", "text": "task"}))
			}
			calls := 0
			hook := func(context.Context, LifecycleEvent) error {
				calls++
				if calls == failAt {
					return errors.New("PRIVATE-storage-error")
				}
				return nil
			}
			result, err := startAgent(context.Background(), testConfig(), request, hook, agentDial(t, steps...))
			if !errors.Is(err, ErrLifecyclePersistence) || calls != failAt || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
		})
	}
}

func lifecycleStopRequest() StopAgentRequest {
	return StopAgentRequest{Handle: LifecycleHandle{WorkspaceID: "ws:1", TabID: "tab:1",
		PaneID: "pane:1", TerminalID: "terminal:1", Provider: "copilot",
		AgentSessionID: "12345678-1234-1234-1234-123456789abc"}}
}

func lifecycleStopSteps() []step {
	return []step{agentPongStep(), lifecycleWorkspaceStep(), lifecycleListStep(agentFixture(), lifecycleOtherPane()),
		agentGetStep(agentFixture()), lifecycleWorkspaceStep(), lifecycleListStep(agentFixture(), lifecycleOtherPane()),
		agentGetStep(agentFixture())}
}

func lifecycleCloseStep() step {
	return agentReply("pane.close", "ok", nil, map[string]any{"pane_id": "pane:1"})
}

func TestLifecycleStopOnlyVerifiedPanePreservesWorkspace(t *testing.T) {
	steps := append(lifecycleStopSteps(), lifecycleCloseStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleWorkspaceStep())
	var events []LifecycleEvent
	result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleJournal(&events), agentDial(t, steps...))
	if err != nil || result.Stop != LifecycleConfirmed || len(events) != 2 ||
		events[0].Stage != LifecycleClosePane || events[0].Result.Stop != LifecycleUnknown ||
		events[1].Result.Stop != LifecycleConfirmed || result.Pane != LifecycleNotAttempted {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
}

func TestLifecycleStopLastPaneMissingAndReplacement(t *testing.T) {
	for _, mode := range []string{"last", "missing", "terminal", "provider", "session", "workspace", "tab", "fresh_get"} {
		t.Run(mode, func(t *testing.T) {
			pane := agentFixture()
			steps := lifecycleStopSteps()[:2]
			switch mode {
			case "last":
				steps = append(steps, lifecycleListStep(pane))
			case "missing":
				steps = append(steps, lifecycleListStep(lifecycleOtherPane()))
			case "fresh_get":
				steps = append(steps, lifecycleListStep(pane, lifecycleOtherPane()))
				pane = agentFixture()
				pane["terminal_id"] = "replacement"
				steps = append(steps, agentGetStep(pane))
			default:
				switch mode {
				case "terminal":
					pane["terminal_id"] = "replacement"
				case "provider":
					pane["agent"], pane["agent_session"] = "claude", nil
				case "session":
					pane["agent_session"].(map[string]any)["value"] = "new-session"
				case "workspace":
					pane["workspace_id"] = "ws:other"
				case "tab":
					pane["tab_id"] = "tab:other"
				}
				steps = append(steps, lifecycleListStep(pane, lifecycleOtherPane()))
			}
			result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleDiscard, agentDial(t, steps...))
			if err == nil || result.Stop != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if mode == "last" && !errors.Is(err, ErrLifecycleLastPane) {
				t.Fatal(err)
			}
		})
	}
}

func TestLifecycleStopUnknownUnlessAbsenceAndWorkspaceConfirmed(t *testing.T) {
	for _, mode := range []string{"lost", "still_present", "pane_reused", "terminal_moved", "list_failed", "workspace_gone"} {
		t.Run(mode, func(t *testing.T) {
			steps := lifecycleStopSteps()
			switch mode {
			case "lost":
				steps = append(steps, lifecycleLostResponse("pane.close"))
			case "list_failed":
				steps = append(steps, lifecycleCloseStep(), lifecycleLostResponse("pane.list"))
			case "workspace_gone":
				steps = append(steps, lifecycleCloseStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleAPIError("workspace.get", "not_found"))
			default:
				pane := agentFixture()
				if mode == "pane_reused" {
					pane["terminal_id"] = "new-terminal"
				}
				if mode == "terminal_moved" {
					pane["pane_id"], pane["workspace_id"] = "pane:new", "ws:other"
				}
				steps = append(steps, lifecycleCloseStep(), lifecycleListStep(pane, lifecycleOtherPane()))
			}
			result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleDiscard, agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) || result.Stop != LifecycleUnknown {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleCanceledMutationStillPersistsUnknown(t *testing.T) {
	request := lifecycleRequest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	steps := append(lifecyclePrepareSteps(request), step{"agent.start", func(conn net.Conn, _ testRequest) {
		cancel()
		var b [1]byte
		conn.Read(b[:])
	}})
	var events []LifecycleEvent
	result, err := startAgent(ctx, testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrAgentIndeterminate) ||
		result.Launch != LifecycleUnknown || len(events) != 4 ||
		events[3].Before || !reflect.DeepEqual(events[3].Result, result) {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
}

func TestLifecycleRequiresHookAndValidPinnedStopTarget(t *testing.T) {
	request := lifecycleRequest(t)
	if _, err := startAgent(context.Background(), testConfig(), request, nil, agentDial(t)); !errors.Is(err, ErrAgentPrecondition) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*StopAgentRequest){
		func(r *StopAgentRequest) { r.Handle.TerminalID = "" },
		func(r *StopAgentRequest) { r.Handle.WorkspaceID = "" },
		func(r *StopAgentRequest) { r.Handle.Provider = "unknown" },
	} {
		stop := lifecycleStopRequest()
		mutate(&stop)
		if _, err := stopAgent(context.Background(), testConfig(), stop, lifecycleDiscard, agentDial(t)); !errors.Is(err, ErrAgentPrecondition) {
			t.Fatal(err)
		}
	}
}

func TestLifecycleCreateRejectsReusedOrMismatchedIdentities(t *testing.T) {
	for _, field := range []string{"pane_id", "terminal_id", "tab_id", "workspace_id", "focused"} {
		t.Run(field, func(t *testing.T) {
			request := lifecycleRequest(t)
			pane := lifecycleShell()
			switch field {
			case "pane_id":
				pane[field] = "pane:other"
			case "terminal_id":
				pane[field] = "terminal:other"
			case "tab_id":
				pane[field] = "tab:other"
			case "workspace_id":
				pane[field] = "ws:other"
			case "focused":
				pane[field] = true
			}
			steps := lifecyclePrepareSteps(request)[:5]
			steps = append(steps, lifecycleCreateStep(request, pane))
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if err == nil || result.Launch != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleMalformedMutationAcksStayUnknown(t *testing.T) {
	for _, method := range []string{"tab.create", "agent.start", "agent.prompt"} {
		t.Run(method, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "task"
			steps := lifecyclePrepareSteps(request)
			switch method {
			case "tab.create":
				// tab_created has root_pane, not pane.
				steps = append(steps[:5], agentReply(method, "tab_created",
					map[string]any{"pane": lifecycleShell()}, map[string]any{
						"workspace_id": request.WorkspaceID, "cwd": request.Cwd, "focus": false}))
			case "agent.start":
				// argv is required even though it is never exposed to callers.
				steps = append(steps, step{method, func(conn net.Conn, request testRequest) {
					reply(conn, request, map[string]any{"type": "agent_started", "agent": agentFixture()})
				}})
			case "agent.prompt":
				replaced := agentFixture()
				replaced["terminal_id"] = "replacement"
				steps = append(lifecycleLaunchSteps(request, agentFixture()), agentGetStep(agentFixture()), agentGetStep(agentFixture()),
					agentReply(method, "agent_prompted", map[string]any{"agent": replaced},
						map[string]any{"target": "pane:1", "text": "task"}))
			}
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if method == "agent.prompt" && (result.Launch != LifecycleConfirmed || result.Prompt != LifecycleUnknown) {
				t.Fatal("uncertain prompt lost the confirmed launch")
			}
		})
	}
}

func lifecyclePendingAgent() map[string]any {
	pending := lifecycleShell()
	pending["interactive_ready"], pending["launch_pending"] = false, true
	return pending
}

func TestLifecycleNativePendingAcknowledgementWaitsWithoutRestart(t *testing.T) {
	request := lifecycleRequest(t)
	request.InitialPrompt = "task"
	pending := lifecyclePendingAgent()
	steps := append(lifecyclePrepareSteps(request),
		lifecycleStartStep(request, pending), agentGetStep(pending), agentGetStep(agentFixture()),
		agentGetStep(agentFixture()), agentGetStep(agentFixture()), agentReply("agent.prompt", "agent_prompted",
			map[string]any{"agent": agentFixture()}, map[string]any{"target": "pane:1", "text": "task"}))
	var events []LifecycleEvent
	result, err := startAgent(context.Background(), testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
	if err != nil || result.Launch != LifecycleConfirmed || result.Prompt != LifecycleConfirmed ||
		result.Handle.Provider != "copilot" || result.Handle.AgentSessionID == "" || len(events) != 6 {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
	if events[2].Result.Launch != LifecycleUnknown || events[3].Result.Launch != LifecycleConfirmed {
		t.Fatal("launch was not journaled as unknown until fresh readiness")
	}
}

func TestLifecyclePendingLaunchTimeoutDoesNotPromptOrRestart(t *testing.T) {
	request := lifecycleRequest(t)
	request.InitialPrompt = "must not be delivered"
	pending := lifecyclePendingAgent()
	steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, pending), agentGetStep(pending))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var events []LifecycleEvent
	result, err := startAgent(ctx, testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
	if !errors.Is(err, ErrAgentIndeterminate) || !errors.Is(err, context.DeadlineExceeded) ||
		result.Launch != LifecycleUnknown || result.Prompt != LifecycleNotAttempted ||
		result.Handle.TerminalID != "terminal:1" || len(events) != 4 || events[3].Before {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
}

func TestLifecycleReadinessRejectsReplacementAndUnknownDelivery(t *testing.T) {
	for _, field := range []string{"terminal_id", "workspace_id", "tab_id", "agent", "agent_session", "lost_get"} {
		t.Run(field, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "must not be delivered"
			ack := lifecyclePendingAgent()
			changed := agentFixture()
			switch field {
			case "agent_session":
				ack = agentFixture()
				ack["interactive_ready"], ack["launch_pending"] = false, true
				changed["agent_session"].(map[string]any)["value"] = "replacement"
			case "agent":
				changed["agent"], changed["agent_session"] = "claude", nil
			case "lost_get":
			default:
				changed[field] = "replacement"
			}
			steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, ack))
			if field == "lost_get" {
				steps = append(steps, lifecycleLostResponse("agent.get"))
			} else {
				steps = append(steps, agentGetStep(changed))
			}
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
			if !errors.Is(err, ErrAgentIndeterminate) || result.Launch != LifecycleUnknown || result.Prompt != LifecycleNotAttempted {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleBlockedStartupIsNotReadyAndCannotPrompt(t *testing.T) {
	for _, state := range []string{"detected", "pending", "ready_ack"} {
		t.Run(state, func(t *testing.T) {
			request := lifecycleRequest(t)
			request.InitialPrompt = "must not be delivered"
			ack := lifecyclePendingAgent()
			blocked := agentFixture()
			blocked["agent_status"] = "blocked"
			if state == "pending" {
				blocked = lifecyclePendingAgent()
				blocked["agent_status"] = "blocked"
			}
			if state == "ready_ack" {
				ack = blocked
			}
			steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, ack), agentGetStep(blocked))
			var events []LifecycleEvent
			result, err := startAgent(context.Background(), testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
			if !errors.Is(err, ErrAgentBlocked) || result.Pane != LifecycleConfirmed || result.Launch != LifecycleUnknown ||
				result.Prompt != LifecycleNotAttempted || result.ObservedStatus != "blocked" || result.Handle.TerminalID != "terminal:1" ||
				len(events) != 4 || events[3].Before || events[3].Result.ObservedStatus != "blocked" {
				t.Fatalf("result=%+v err=%v events=%v", result, err, events)
			}
		})
	}
}

func TestLifecycleReadyAcknowledgementStillRequiresFreshGet(t *testing.T) {
	request := lifecycleRequest(t)
	replaced := agentFixture()
	replaced["terminal_id"] = "replacement"
	steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, agentFixture()), agentGetStep(replaced))
	result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard, agentDial(t, steps...))
	if !errors.Is(err, ErrAgentIndeterminate) || !errors.Is(err, ErrAgentChanged) || result.Launch != LifecycleUnknown {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestLifecycleStartupUsesOwnBudget(t *testing.T) {
	request := lifecycleRequest(t)
	request.StartupTimeout = 3001 * time.Millisecond
	config := testConfig()
	config.RequestTimeout = 100 * time.Millisecond
	start := lifecycleStartStep(request, agentFixture())
	steps := append(lifecyclePrepareSteps(request), step{start.method, func(conn net.Conn, request testRequest) {
		time.Sleep(150 * time.Millisecond)
		start.serve(conn, request)
	}}, agentGetStep(agentFixture()))
	result, err := startAgent(context.Background(), config, request, lifecycleDiscard, agentDial(t, steps...))
	if err != nil || result.Launch != LifecycleConfirmed {
		t.Fatalf("generic request timeout truncated startup: result=%+v err=%v", result, err)
	}
}

func TestLifecycleStartupDeadlineDoesNotResend(t *testing.T) {
	request := lifecycleRequest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	steps := append(lifecyclePrepareSteps(request), step{"agent.start", func(conn net.Conn, _ testRequest) {
		var b [1]byte
		conn.Read(b[:])
	}})
	var events []LifecycleEvent
	before := time.Now()
	result, err := startAgent(ctx, testConfig(), request, lifecycleJournal(&events), agentDial(t, steps...))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrAgentIndeterminate) ||
		result.Launch != LifecycleUnknown || result.Pane != LifecycleConfirmed || len(events) != 4 ||
		time.Since(before) > time.Second {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
}

func TestLifecycleStopPersistenceGatesClose(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "after"}[before], func(t *testing.T) {
			steps := lifecycleStopSteps()
			if before {
				steps = steps[:4]
			} else {
				steps = append(steps, lifecycleCloseStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleWorkspaceStep())
			}
			hook := func(_ context.Context, event LifecycleEvent) error {
				if event.Before == before {
					return errors.New("storage unavailable")
				}
				return nil
			}
			result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), hook, agentDial(t, steps...))
			if !errors.Is(err, ErrLifecyclePersistence) ||
				(before && result.Stop != LifecycleNotAttempted) || (!before && result.Stop != LifecycleConfirmed) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestLifecycleUnsupportedProtocolCannotMutate(t *testing.T) {
	pong := agentReply("ping", "pong", map[string]any{"version": "0.7.5", "protocol": 19}, map[string]any{})
	if result, err := startAgent(context.Background(), testConfig(), lifecycleRequest(t), lifecycleDiscard, agentDial(t, pong)); !errors.Is(err, ErrAgentUnsupported) || result.Pane != LifecycleNotAttempted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleDiscard, agentDial(t, pong)); !errors.Is(err, ErrAgentUnsupported) || result.Stop != LifecycleNotAttempted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
