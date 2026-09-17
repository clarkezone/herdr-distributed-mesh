package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

var (
	ErrLifecyclePersistence = errors.New("herdr: lifecycle persistence failed")
	ErrLifecycleLastPane    = errors.New("herdr: refusing to close the workspace's last pane")
)

// LifecycleOutcome describes an effect, not provider task success. Unknown
// includes a lost/error/malformed response: even an API error is not evidence
// that a multi-step native operation made no changes.
type LifecycleOutcome string

const (
	LifecycleNotAttempted LifecycleOutcome = "not_attempted"
	LifecycleUnknown      LifecycleOutcome = "unknown"
	LifecycleConfirmed    LifecycleOutcome = "confirmed"
)

type LifecycleStage string

const (
	LifecycleCreatePane LifecycleStage = "create_pane"
	LifecycleLaunch     LifecycleStage = "launch"
	LifecyclePrompt     LifecycleStage = "prompt"
	LifecycleClosePane  LifecycleStage = "close_pane"
)

// LifecycleHandle contains only bounded identity evidence. The caller must
// additionally pin the node, selected Herdr session and its incarnation; pane
// and terminal IDs alone are not unique across sessions or server restarts.
type LifecycleHandle struct {
	WorkspaceID    string
	TabID          string
	PaneID         string
	TerminalID     string
	AgentSessionID string
	Provider       string
}

// LifecycleResult always accompanies errors, preserving confirmed earlier
// stages. Launch is confirmed only by freshly observed interactive readiness. Prompt is
// separately confirmed submission, never task completion. Stop is confirmed
// pane/terminal absence with the workspace still present, not graceful provider
// exit or cancellation of detached/cloud work.
// Blocked startup retains its handle/status and returns ErrAgentBlocked with
// Launch=Unknown; it is not a ready launch or permission to retry the start.
type LifecycleResult struct {
	Handle         LifecycleHandle
	Pane           LifecycleOutcome
	Launch         LifecycleOutcome
	Prompt         LifecycleOutcome
	Stop           LifecycleOutcome
	ObservedStatus string
}

// LifecycleEvent is a value-only, prompt/path/output-free journal snapshot.
// Before=true records intent (the stage is Unknown) BEFORE the IPC side effect.
// Before=false records the observed outcome AFTER it, including uncertainty.
type LifecycleEvent struct {
	Stage  LifecycleStage
	Before bool
	Result LifecycleResult
}

// LifecycleHook must durably persist the event before returning nil. It is
// required, called synchronously, and must honor its bounded context. The caller
// must atomically claim a command's first intent, reject duplicate/replayed
// claims, and serialize mutations of the same target. Hooks must not initiate
// side effects. After-effects hooks receive a fresh three-second context even
// if execution was canceled. A hook failure stops all subsequent effects.
//
// On recovery, a saved Before event is uncertain, not permission to repeat the
// operation. This adapter deliberately does not resume or retry mutations.
// Callers persist the final returned result/error in their command journal too.
// A proven rejection after intent but before dispatch is recorded as
// NotAttempted. If that correction cannot be persisted, the returned stage
// remains Unknown so recovery cannot mistake the saved intent for safe replay.
type LifecycleHook func(context.Context, LifecycleEvent) error

// StartAgentRequest has no command, environment, or provider-argument escape
// hatch. Cwd must be the caller's already resolved, admitted local checkout for
// the explicitly selected workspace; project/generation validation remains the
// caller's responsibility. Names are bounded identifier tokens, not shell text.
type StartAgentRequest struct {
	WorkspaceID    string
	Cwd            string
	Name           string
	Provider       string
	StartupTimeout time.Duration
	InitialPrompt  string
}

type StopAgentRequest struct {
	Handle LifecycleHandle
}

// SupportedLifecycleProvider is the canonical protocol-18 agent.start allowlist.
// Support is not evidence of installation, authentication, or headless readiness.
func SupportedLifecycleProvider(provider string) bool {
	return protocol.SupportedLifecycleProvider(provider)
}

// StartAgent creates one dedicated tab/root pane with focus:false, then asks
// Herdr to start its canonical provider, then freshly observes interactive
// readiness within the startup budget. Protocol 18 can acknowledge agent_started
// with an unknown provider and launch_pending=true before detection completes.
// It never reuses an existing pane or cleans up partial artifacts. Native
// agent.start verifies the interactive shell prompt; pane metadata alone cannot
// establish that an unidentified foreground process is a ready shell.
//
// Config MUST identify the caller's pinned local session endpoint. When supplied,
// CheckSession runs after intent persistence, around refreshed preconditions,
// and after each effect. No persistence hook separates those fresh checks from
// mutation IPC. The caller owns the expected session incarnation.
// Protocol 18 has no expected-terminal CAS. All identity checks have a trusted
// local-client race between observation and mutation, including last-pane checks.
func StartAgent(ctx context.Context, config Config, request StartAgentRequest, hook LifecycleHook) (LifecycleResult, error) {
	return startAgent(ctx, config, request, hook, dialLocal)
}

func startAgent(ctx context.Context, config Config, request StartAgentRequest, hook LifecycleHook, dial dialFunc) (LifecycleResult, error) {
	result := newLifecycleResult()
	if request.StartupTimeout == 0 {
		request.StartupTimeout = 30 * time.Second
	}
	if hook == nil || !idPattern.MatchString(request.WorkspaceID) || !idPattern.MatchString(request.Name) ||
		!SupportedLifecycleProvider(request.Provider) || !validWorkspacePath(request.Cwd) ||
		request.StartupTimeout/time.Millisecond <= 3000 || request.StartupTimeout > 5*time.Minute ||
		(request.InitialPrompt != "" && protocol.ValidateAgentControlInput(
			pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, request.InitialPrompt, nil) != nil) {
		return result, ErrAgentPrecondition
	}
	info, err := lifecycleDirectoryInfo(request.Cwd)
	if err != nil {
		return result, ErrAgentPrecondition
	}
	ctx, cancel := context.WithTimeout(ctx, request.StartupTimeout+30*time.Second)
	defer cancel()
	o, err := lifecycleObserver(ctx, config, dial)
	if err != nil {
		return result, err
	}
	if err := o.lifecycleWorkspace(ctx, request.WorkspaceID); err != nil {
		return result, err
	}
	existing, err := o.lifecyclePanes(ctx)
	if err != nil {
		return result, err
	}
	result.Handle.WorkspaceID = request.WorkspaceID
	err = o.lifecycleGuardedEffect(ctx, hook, LifecycleCreatePane, &result, func() error {
		current, err := lifecycleDirectoryInfo(request.Cwd)
		if err != nil || !os.SameFile(info, current) {
			return ErrAgentPrecondition
		}
		if err := o.lifecycleWorkspace(ctx, request.WorkspaceID); err != nil {
			return err
		}
		existing, err = o.lifecyclePanes(ctx)
		return err
	}, func() error {
		created, err := o.agentRequest(ctx, "tab.create", "tab_created", struct {
			WorkspaceID string `json:"workspace_id"`
			Cwd         string `json:"cwd"`
			Focus       bool   `json:"focus"`
		}{request.WorkspaceID, request.Cwd, false})
		if err != nil {
			return agentFailure(ctx, ErrAgentIndeterminate)
		}
		pane, err := parseAgentInfo(created["root_pane"])
		var tab, root object
		var focused bool
		if err != nil || required(created, "tab", &tab) != nil ||
			required(created, "root_pane", &root) != nil || required(root, "focused", &focused) != nil {
			return ErrAgentIndeterminate
		}
		entity, err := sanitizeEntity(tab, "tab_id")
		var count uint32
		if err != nil || required(tab, "pane_count", &count) != nil || count != 1 ||
			pane.WorkspaceId != request.WorkspaceID || entity.WorkspaceId != request.WorkspaceID ||
			entity.Id != pane.TabId || entity.Focused {
			return ErrAgentIndeterminate
		}
		for _, prior := range existing {
			if prior.Target.PaneId == pane.Target.PaneId || prior.Target.TerminalId == pane.Target.TerminalId ||
				prior.TabId == pane.TabId {
				return ErrAgentIndeterminate
			}
		}
		result.Handle = lifecycleHandle(pane)
		result.Pane = LifecycleConfirmed
		if pane.Provider != "unknown" || focused {
			return ErrAgentPrecondition
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	// Check the created terminal again after the journal write, not whichever
	// pane is focused. Herdr does the authoritative shell-readiness check.
	if err := o.lifecycleShellReady(ctx, result.Handle); err != nil {
		return result, err
	}
	err = o.lifecycleGuardedEffect(ctx, hook, LifecycleLaunch, &result, func() error {
		return o.lifecycleShellReady(ctx, result.Handle)
	}, func() error {
		args := []string{}
		if request.Provider == "copilot" {
			// Installed protocol-18 PowerShell startup fails with empty args;
			// disable provider self-update without exposing arbitrary arguments.
			args = []string{"--no-auto-update"}
		}
		ordinaryTimeout := o.config.RequestTimeout
		o.config.RequestTimeout = request.StartupTimeout + time.Second
		defer func() { o.config.RequestTimeout = ordinaryTimeout }()
		startCtx, cancelStart := context.WithTimeout(ctx, o.config.RequestTimeout)
		defer cancelStart()
		started, err := o.agentRequest(startCtx, "agent.start", "agent_started", struct {
			Name      string   `json:"name"`
			Kind      string   `json:"kind"`
			PaneID    string   `json:"pane_id"`
			Args      []string `json:"args"`
			TimeoutMS uint64   `json:"timeout_ms"`
		}{request.Name, request.Provider, result.Handle.PaneID, args, uint64(request.StartupTimeout / time.Millisecond)})
		if err != nil {
			return agentFailure(startCtx, ErrAgentIndeterminate)
		}
		agent, err := parseAgentInfo(started["agent"])
		var argv []string
		if err != nil || required(started, "argv", &argv) != nil || len(argv) == 0 ||
			!lifecycleMatches(result.Handle, agent, false) ||
			(agent.Provider != request.Provider && !(agent.Provider == "unknown" && agent.LaunchPending)) {
			return ErrAgentIndeterminate
		}
		result.Handle = lifecycleHandle(agent)
		result.ObservedStatus = agent.Status
		if err := o.waitLifecycleLaunch(startCtx, request.Provider, &result); err != nil {
			return err
		}
		result.Launch = LifecycleConfirmed
		return nil
	})
	if err != nil || request.InitialPrompt == "" {
		return result, err
	}
	if err := o.lifecyclePromptReady(ctx, &result); err != nil {
		return result, err
	}
	err = o.lifecycleGuardedEffect(ctx, hook, LifecyclePrompt, &result, func() error {
		return o.lifecyclePromptReady(ctx, &result)
	}, func() error {
		prompted, err := o.agentRequest(ctx, "agent.prompt", "agent_prompted", struct {
			Target string `json:"target"`
			Text   string `json:"text"`
		}{result.Handle.PaneID, request.InitialPrompt})
		if err != nil {
			return agentFailure(ctx, ErrAgentIndeterminate)
		}
		after, err := parseAgentInfo(prompted["agent"])
		if err != nil || !lifecycleMatches(result.Handle, after, true) {
			return ErrAgentIndeterminate
		}
		result.ObservedStatus = after.Status
		result.Prompt = LifecycleConfirmed
		return nil
	})
	return result, err
}

func (o *observer) waitLifecycleLaunch(ctx context.Context, provider string, result *LifecycleResult) error {
	for {
		observed, err := o.getAgent(ctx, lifecycleTarget(result.Handle))
		if err != nil {
			return agentFailure(ctx, errors.Join(ErrAgentIndeterminate, err))
		}
		if !lifecycleMatches(result.Handle, observed, false) ||
			(observed.Provider != provider && observed.Provider != "unknown") {
			return errors.Join(ErrAgentIndeterminate, ErrAgentChanged)
		}
		result.Handle = lifecycleHandle(observed)
		result.ObservedStatus = observed.Status
		if observed.Status == "blocked" {
			return ErrAgentBlocked
		}
		if observed.Provider == provider && observed.InteractiveReady && !observed.LaunchPending {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return agentFailure(ctx, ErrAgentIndeterminate)
		case <-timer.C:
		}
	}
}

// StopAgent closes only a freshly verified agent pane. It refuses a last-pane
// close to preserve the enclosing workspace. It does not send keys, delete a
// workspace/worktree, stop a server, or kill a PID. Native pane absence does not
// prove graceful shutdown or cancellation of provider work outside that pane.
func StopAgent(ctx context.Context, config Config, request StopAgentRequest, hook LifecycleHook) (LifecycleResult, error) {
	return stopAgent(ctx, config, request, hook, dialLocal)
}

func stopAgent(ctx context.Context, config Config, request StopAgentRequest, hook LifecycleHook, dial dialFunc) (LifecycleResult, error) {
	result := newLifecycleResult()
	handle := request.Handle
	if hook == nil || !idPattern.MatchString(handle.WorkspaceID) || !idPattern.MatchString(handle.TabID) ||
		protocol.ValidateAgentTarget(lifecycleTarget(handle), true) != nil || !SupportedLifecycleProvider(handle.Provider) {
		return result, ErrAgentPrecondition
	}
	result.Handle = handle
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	o, err := lifecycleObserver(ctx, config, dial)
	if err != nil {
		return result, err
	}
	if err := o.lifecycleStopReady(ctx, &result); err != nil {
		return result, err
	}
	err = o.lifecycleGuardedEffect(ctx, hook, LifecycleClosePane, &result, func() error {
		return o.lifecycleStopReady(ctx, &result)
	}, func() error {
		if _, err := o.agentRequest(ctx, "pane.close", "ok", struct {
			PaneID string `json:"pane_id"`
		}{handle.PaneID}); err != nil {
			return agentFailure(ctx, ErrAgentIndeterminate)
		}
		after, err := o.lifecyclePanes(ctx)
		if err != nil {
			return agentFailure(ctx, ErrAgentIndeterminate)
		}
		for _, pane := range after {
			if pane.Target.PaneId == handle.PaneID || pane.Target.TerminalId == handle.TerminalID {
				return ErrAgentIndeterminate
			}
		}
		if err := o.lifecycleWorkspace(ctx, handle.WorkspaceID); err != nil {
			return agentFailure(ctx, ErrAgentIndeterminate)
		}
		result.Stop = LifecycleConfirmed
		return nil
	})
	return result, err
}

func newLifecycleResult() LifecycleResult {
	return LifecycleResult{Pane: LifecycleNotAttempted, Launch: LifecycleNotAttempted,
		Prompt: LifecycleNotAttempted, Stop: LifecycleNotAttempted}
}

func lifecycleObserver(ctx context.Context, config Config, dial dialFunc) (*observer, error) {
	if config.RequestTimeout > 30*time.Second {
		return nil, ErrAgentPrecondition
	}
	return agentObserver(ctx, config, dial)
}

func lifecycleEffect(ctx context.Context, hook LifecycleHook, stage LifecycleStage, result *LifecycleResult, effect func() error) error {
	if ctx.Err() != nil {
		return agentFailure(ctx, ErrAgentUnavailable)
	}
	intent := *result
	*lifecycleStageOutcome(&intent, stage) = LifecycleUnknown
	if err := hook(ctx, LifecycleEvent{Stage: stage, Before: true, Result: intent}); err != nil {
		return ErrLifecyclePersistence
	}
	*result = intent
	var err error
	if ctx.Err() != nil {
		err = &lifecyclePreDispatch{agentFailure(ctx, ErrAgentUnavailable)}
	} else {
		err = effect()
	}
	var rejected *lifecyclePreDispatch
	if errors.As(err, &rejected) {
		*lifecycleStageOutcome(result, stage) = LifecycleNotAttempted
		err = rejected.err
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if persistErr := hook(persistCtx, LifecycleEvent{Stage: stage, Result: *result}); persistErr != nil {
		if *lifecycleStageOutcome(result, stage) == LifecycleNotAttempted {
			*lifecycleStageOutcome(result, stage) = LifecycleUnknown
			return errors.Join(err, ErrLifecyclePersistence, ErrAgentIndeterminate)
		}
		return errors.Join(err, ErrLifecyclePersistence)
	}
	return err
}

func lifecycleStageOutcome(result *LifecycleResult, stage LifecycleStage) *LifecycleOutcome {
	switch stage {
	case LifecycleCreatePane:
		return &result.Pane
	case LifecycleLaunch:
		return &result.Launch
	case LifecyclePrompt:
		return &result.Prompt
	default:
		return &result.Stop
	}
}

func lifecycleHandle(agent *pb.AgentView) LifecycleHandle {
	return LifecycleHandle{WorkspaceID: agent.WorkspaceId, TabID: agent.TabId,
		PaneID: agent.Target.PaneId, TerminalID: agent.Target.TerminalId,
		AgentSessionID: agent.Target.AgentSessionId, Provider: agent.Provider}
}

func lifecycleTarget(handle LifecycleHandle) *pb.AgentTarget {
	return &pb.AgentTarget{PaneId: handle.PaneID, TerminalId: handle.TerminalID, AgentSessionId: handle.AgentSessionID}
}

func lifecycleMatches(handle LifecycleHandle, agent *pb.AgentView, provider bool) bool {
	return agent != nil && handle.WorkspaceID == agent.WorkspaceId && handle.TabID == agent.TabId &&
		handle.PaneID == agent.Target.PaneId && handle.TerminalID == agent.Target.TerminalId &&
		(handle.AgentSessionID == "" || handle.AgentSessionID == agent.Target.AgentSessionId) &&
		(!provider || handle.Provider == agent.Provider)
}

func (o *observer) lifecycleWorkspace(ctx context.Context, workspaceID string) error {
	result, err := o.agentRequest(ctx, "workspace.get", "workspace_info", struct {
		WorkspaceID string `json:"workspace_id"`
	}{workspaceID})
	if err != nil {
		return agentFailure(ctx, ErrAgentUnavailable)
	}
	id, _, err := workspaceIdentity(result["workspace"])
	if err != nil {
		return ErrAgentUnavailable
	}
	if id != workspaceID {
		return ErrAgentChanged
	}
	return nil
}

func (o *observer) lifecyclePane(ctx context.Context, handle LifecycleHandle) (*pb.AgentView, error) {
	result, err := o.agentRequest(ctx, "pane.get", "pane_info", struct {
		PaneID string `json:"pane_id"`
	}{handle.PaneID})
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	pane, err := parseAgentInfo(result["pane"])
	if err != nil {
		return nil, ErrAgentUnavailable
	}
	if !lifecycleMatches(handle, pane, false) {
		return nil, ErrAgentChanged
	}
	return pane, nil
}

func (o *observer) lifecyclePanes(ctx context.Context) ([]*pb.AgentView, error) {
	result, err := o.rpc(ctx, "pane.list", "pane_list")
	if err != nil {
		return nil, agentFailure(ctx, ErrAgentUnavailable)
	}
	var raw []json.RawMessage
	if required(result, "panes", &raw) != nil || len(raw) > maxEntities {
		return nil, ErrAgentUnavailable
	}
	panes := make([]*pb.AgentView, 0, len(raw))
	ids, terminals := map[string]bool{}, map[string]bool{}
	for _, entry := range raw {
		pane, err := parseAgentInfo(entry)
		if err != nil || ids[pane.GetTarget().GetPaneId()] || terminals[pane.GetTarget().GetTerminalId()] {
			return nil, ErrAgentUnavailable
		}
		ids[pane.Target.PaneId], terminals[pane.Target.TerminalId] = true, true
		panes = append(panes, pane)
	}
	return panes, nil
}
