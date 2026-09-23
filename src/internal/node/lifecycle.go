package node

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/protobuf/proto"
)

type lifecycleJournal interface {
	SaveLifecycle(context.Context, string, *pb.AgentLifecycleReceipt) error
	GetLifecycle(context.Context, string) (*pb.AgentLifecycleReceipt, error)
	PinAgentCommand(context.Context, string, string) error
}

type startAgentExecutor func(context.Context, herdr.Config, herdr.StartAgentRequest, herdr.LifecycleHook) (herdr.LifecycleResult, error)
type stopAgentExecutor func(context.Context, herdr.Config, herdr.StopAgentRequest, herdr.LifecycleHook) (herdr.LifecycleResult, error)

func (h *commandHandler) lifecycleScopeAvailable(name string) bool {
	return h.lifecycleNegotiated && (name != "" || !strings.HasPrefix(strings.ToLower(h.herdrConfig.SocketPath), `\\.\pipe\`))
}

func (h *commandHandler) resolveLifecycleSession(ctx context.Context, name, expected string) (herdr.Config, string, error) {
	if name == "" && expected == "" {
		var err error
		expected, err = herdrsession.SocketIdentity(h.herdrConfig.SocketPath)
		if err != nil {
			return herdr.Config{}, "", sessionFailure("session_unavailable")
		}
	}
	return h.resolveSession(ctx, name, expected)
}

func lifecycleReceipt(value herdr.LifecycleResult, name, incarnation, workspace string) *pb.AgentLifecycleReceipt {
	h := value.Handle
	if h.WorkspaceID == "" {
		h.WorkspaceID = workspace
	}
	return &pb.AgentLifecycleReceipt{
		Handle: &pb.AgentLifecycleHandle{WorkspaceId: h.WorkspaceID, TabId: h.TabID, Provider: h.Provider,
			Target: &pb.AgentTarget{PaneId: h.PaneID, TerminalId: h.TerminalID, AgentSessionId: h.AgentSessionID,
				SessionName: name, SessionIncarnation: incarnation}},
		PaneOutcome: string(value.Pane), LaunchOutcome: string(value.Launch),
		PromptOutcome: string(value.Prompt), StopOutcome: string(value.Stop), ObservedStatus: value.ObservedStatus,
	}
}

func nativeLifecycleHandle(stop *pb.AgentStop) herdr.LifecycleHandle {
	return herdr.LifecycleHandle{WorkspaceID: stop.WorkspaceId, TabID: stop.TabId, Provider: stop.Provider,
		PaneID: stop.Target.PaneId, TerminalID: stop.Target.TerminalId, AgentSessionID: stop.Target.AgentSessionId}
}

func lifecycleErrorDetail(err error) string {
	var session sessionFailure
	switch {
	case errors.Is(err, state.ErrLifecycleUnresolved):
		return "lifecycle_unresolved"
	case errors.Is(err, herdr.ErrLifecyclePersistence):
		return "persistence_failed"
	case errors.As(err, &session):
		return sessionError(err)
	case errors.Is(err, herdr.ErrAgentBlocked):
		return "agent_blocked"
	case errors.Is(err, herdr.ErrAgentBusy):
		return "agent_busy"
	case errors.Is(err, herdr.ErrAgentChanged):
		return "target_changed"
	case errors.Is(err, herdr.ErrAgentUnsupported):
		return "unsupported"
	case errors.Is(err, herdr.ErrAgentUnavailable):
		return "herdr_unavailable"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "deadline_expired"
	default:
		return "precondition_failed"
	}
}

func (h *commandHandler) lifecycleMutation(ctx context.Context, command *pb.Command) (*pb.CommandResult, error) {
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_REJECTED, Detail: "precondition_failed"}
	journal, ok := h.journal.(lifecycleJournal)
	if !ok || !h.lifecycleNegotiated || h.verifyCoordinator == nil {
		return result, nil
	}
	name, expected := protocol.CommandSession(command)
	if !h.selectedAgentReady(name) {
		return result, nil
	}
	effect, cancel := context.WithDeadline(ctx, protocol.CommandExecutionDeadline(command))
	defer cancel()
	config, incarnation, err := h.resolveLifecycleSession(effect, name, expected)
	if err != nil {
		result.Detail = lifecycleErrorDetail(err)
		return result, nil
	}
	var binding projects.Binding
	if command.AgentStart != nil {
		binding, err = h.resolveManaged(command.AgentStart.ProjectId, command.AgentStart.BindingRevision)
		if err != nil {
			result.Detail = "project_not_authorized"
			return result, nil
		}
	}
	baseCheck := config.CheckSession
	var selectedCwd string
	resolveWorkspace := h.resolveLifecycleWorkspace
	if resolveWorkspace == nil {
		resolveWorkspace = herdr.ResolveLifecycleWorkspace
	}
	config.ProjectResolver = h.managedProjects
	config.CheckSession = func(check context.Context) error {
		verify, stop := context.WithTimeout(check, workspacePeerVerificationTimeout)
		defer stop()
		if err := h.verifyCoordinator(verify); err != nil {
			return sessionFailure("authorization_changed")
		}
		if command.AgentStart != nil {
			current, err := h.resolveManaged(command.AgentStart.ProjectId, command.AgentStart.BindingRevision)
			if err != nil || current.ValidatePath() != nil || binding.ValidatePath() != nil {
				return herdr.ErrAgentPrecondition
			}
			if selectedCwd != "" {
				readConfig := config
				readConfig.CheckSession = baseCheck
				cwd, err := resolveWorkspace(verify, readConfig, command.AgentStart.WorkspaceId, current)
				if err != nil {
					return err
				}
				if !sameEndpoint(cwd, selectedCwd) {
					return herdr.ErrAgentChanged
				}
			}
		}
		return baseCheck(verify)
	}
	initial := herdr.LifecycleResult{Pane: herdr.LifecycleNotAttempted, Launch: herdr.LifecycleNotAttempted,
		Prompt: herdr.LifecycleNotAttempted, Stop: herdr.LifecycleNotAttempted}
	workspace := command.AgentStart.GetWorkspaceId()
	if command.AgentStop != nil {
		initial.Handle = nativeLifecycleHandle(command.AgentStop)
		workspace = command.AgentStop.WorkspaceId
	}
	receipt := lifecycleReceipt(initial, name, incarnation, workspace)
	if err := journal.SaveLifecycle(effect, command.CommandId, receipt); err != nil {
		if errors.Is(err, state.ErrLifecycleUnresolved) {
			result.Detail = "lifecycle_unresolved"
			return result, nil
		}
		return nil, storageError("initial lifecycle checkpoint", err)
	}
	publish := func(check context.Context, value *pb.AgentLifecycleReceipt) error {
		if h.lifecycleProgress == nil {
			return nil
		}
		select {
		case h.lifecycleProgress <- &pb.CommandProgress{CommandId: command.CommandId, AgentLifecycle: proto.Clone(value).(*pb.AgentLifecycleReceipt)}:
			return nil
		case <-check.Done():
			return check.Err()
		}
	}
	hook := func(check context.Context, event herdr.LifecycleEvent) error {
		next := lifecycleReceipt(event.Result, name, incarnation, workspace)
		next.Stages = append(proto.Clone(receipt).(*pb.AgentLifecycleReceipt).Stages,
			&pb.AgentLifecycleStage{Stage: string(event.Stage), Before: event.Before, Outcome: protocol.LifecycleStageOutcome(next, string(event.Stage))})
		next.Sequence = uint32(len(next.Stages))
		if err := journal.SaveLifecycle(check, command.CommandId, next); err != nil {
			return err
		}
		receipt = next
		return publish(check, next)
	}
	if err = config.CheckSession(effect); err == nil {
		if command.AgentStart != nil {
			var start *pb.AgentStart
			start, err = protocol.EffectiveAgentStart(command)
			if err == nil {
				var cwd string
				cwd, err = resolveWorkspace(effect, config, start.WorkspaceId, binding)
				if err == nil {
					selectedCwd = cwd
					execute := h.startAgent
					if execute == nil {
						execute = herdr.StartAgent
					}
					_, err = execute(effect, config, herdr.StartAgentRequest{WorkspaceID: start.WorkspaceId,
						Cwd: cwd, Name: start.Name, Provider: start.Provider,
						StartupTimeout: time.Duration(start.StartupTimeoutMs) * time.Millisecond, InitialPrompt: start.InitialPrompt}, hook)
				}
			}
		} else {
			execute := h.stopAgent
			if execute == nil {
				execute = herdr.StopAgent
			}
			_, err = execute(effect, config, herdr.StopAgentRequest{Handle: nativeLifecycleHandle(command.AgentStop)}, hook)
		}
	}
	// A failed hook can have committed despite a lost acknowledgement. Reload
	// the journal instead of substituting the adapter's non-durable observations.
	read, stop := context.WithTimeout(context.WithoutCancel(ctx), journalOperationTimeout)
	defer stop()
	receipt, readErr := journal.GetLifecycle(read, command.CommandId)
	if readErr != nil {
		return nil, storageError("read final lifecycle checkpoint", readErr)
	}
	result.AgentLifecycle, result.Detail = receipt, lifecycleErrorDetail(err)
	switch {
	case protocol.LifecycleHasUnknown(receipt):
		result.Status = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE
		if err == nil {
			result.Detail = "lifecycle_uncertain"
		}
	case protocol.LifecycleComplete(receipt, command):
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, "agent_started"
		if command.AgentStop != nil {
			result.Detail = "agent_stopped"
		}
	case protocol.LifecycleHasEffect(receipt):
		result.Status = pb.CommandStatus_COMMAND_STATUS_FAILED
		if err == nil {
			result.Detail = "lifecycle_partial"
		}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_TIMED_OUT, "deadline_expired"
	}
	return result, nil
}
