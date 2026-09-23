package protocol

import (
	"errors"
	"slices"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func SupportedLifecycleProvider(provider string) bool {
	return slices.Contains([]string{"pi", "claude", "codex", "gemini", "cursor", "devin", "agy", "cline",
		"omp", "mastracode", "opencode", "copilot", "kimi", "kiro", "droid",
		"amp", "grok", "hermes", "kilo", "qodercli", "maki"}, provider)
}

func ValidateAgentStart(value *pb.AgentStart) error {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		!commandToken.MatchString(value.ProjectId) || !commandToken.MatchString(value.BindingRevision) ||
		!commandToken.MatchString(value.WorkspaceId) || !commandToken.MatchString(value.Name) ||
		!SupportedLifecycleProvider(value.Provider) {
		return errors.New("start requires project/revision, workspace, name and supported provider")
	}
	if _, err := NormalizeAgentStartupTimeout(value.StartupTimeoutMs); err != nil {
		return err
	}
	if value.InitialPrompt != "" {
		if err := ValidateAgentControlInput(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, value.InitialPrompt, nil); err != nil {
			return err
		}
	}
	return ValidateSessionSelector(value.SessionName, value.SessionIncarnation, false)
}

func ValidateAgentStop(value *pb.AgentStop) error {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		ValidateAgentTarget(value.Target, true) != nil || !ValidSessionIncarnation(value.Target.SessionIncarnation) ||
		!commandToken.MatchString(value.WorkspaceId) || !commandToken.MatchString(value.TabId) ||
		!SupportedLifecycleProvider(value.Provider) {
		return errors.New("stop requires exact session incarnation, workspace, tab, pane, terminal and provider")
	}
	return nil
}

// EffectiveAgentStart reconstructs the prompt stored once in SubmittedRequest.
func EffectiveAgentStart(command *pb.Command) (*pb.AgentStart, error) {
	if command == nil || command.AgentStart == nil || ValidateSubmittedRequest(command) != nil {
		return nil, errors.New("invalid lifecycle submitted fingerprint")
	}
	value := proto.Clone(command.AgentStart).(*pb.AgentStart)
	value.InitialPrompt = command.SubmittedRequest.AgentStart.InitialPrompt
	return value, nil
}

func validateLifecycleSubmitted(c *pb.Command, r *pb.SubmitCommandRequest) error {
	if c.CommandType == AgentStopCommandType {
		if !proto.Equal(c.AgentStop, r.AgentStop) {
			return errors.New("stop fingerprint mismatch")
		}
		return nil
	}
	if c.AgentStart == nil || r.AgentStart == nil || c.AgentStart.InitialPrompt != "" {
		return errors.New("resolved start must store its prompt only in the submitted request")
	}
	want := proto.Clone(r.AgentStart).(*pb.AgentStart)
	got := c.AgentStart
	if want.BindingRevision != "" && want.BindingRevision != got.BindingRevision ||
		!matchesSession(got.SessionName, got.SessionIncarnation, want.SessionName, want.SessionIncarnation) {
		return errors.New("resolved start project/session mismatch")
	}
	want.BindingRevision, want.SessionIncarnation, want.InitialPrompt = got.BindingRevision, got.SessionIncarnation, ""
	if !proto.Equal(want, got) {
		return errors.New("resolved start changed original operator input")
	}
	return nil
}

func CommandExecutionDeadline(command *pb.Command) time.Time {
	if command.ExecutionExpiresAt != nil {
		return command.ExecutionExpiresAt.AsTime()
	}
	return command.ExpiresAt.AsTime()
}

func LifecycleStageOutcome(r *pb.AgentLifecycleReceipt, stage string) string {
	switch stage {
	case "create_pane":
		return r.PaneOutcome
	case "launch":
		return r.LaunchOutcome
	case "prompt":
		return r.PromptOutcome
	case "close_pane":
		return r.StopOutcome
	}
	return ""
}

func ValidateLifecycleReceipt(r *pb.AgentLifecycleReceipt, command *pb.Command) error {
	if r == nil || command == nil || !IsLifecycleCommand(command.CommandType) ||
		proto.Size(r) > MaxLifecycleReceiptBytes || len(r.ProtoReflect().GetUnknown()) != 0 ||
		len(r.Stages) > MaxLifecycleEvents || int(r.Sequence) != len(r.Stages) {
		return errors.New("invalid lifecycle checkpoint bounds")
	}
	for _, value := range []string{r.PaneOutcome, r.LaunchOutcome, r.PromptOutcome, r.StopOutcome} {
		if !slices.Contains([]string{"not_attempted", "unknown", "confirmed"}, value) {
			return errors.New("invalid lifecycle outcome")
		}
	}
	if r.ObservedStatus != "" && !validAgentStatus(r.ObservedStatus) {
		return errors.New("invalid lifecycle readiness")
	}
	h := r.Handle
	if h == nil || h.Target == nil || len(h.ProtoReflect().GetUnknown()) != 0 ||
		len(h.Target.ProtoReflect().GetUnknown()) != 0 || !ValidSessionIncarnation(h.Target.SessionIncarnation) ||
		ValidateSessionSelector(h.Target.SessionName, h.Target.SessionIncarnation, true) != nil {
		return errors.New("checkpoint requires a pinned native incarnation")
	}
	for _, id := range []string{h.WorkspaceId, h.TabId, h.Target.PaneId, h.Target.TerminalId, h.Target.AgentSessionId} {
		if id != "" && !commandToken.MatchString(id) {
			return errors.New("invalid lifecycle handle")
		}
	}
	if command.AgentStart != nil {
		w := command.AgentStart
		if h.WorkspaceId != w.WorkspaceId || !matchesSession(h.Target.SessionName, h.Target.SessionIncarnation, w.SessionName, w.SessionIncarnation) ||
			(h.Provider != "" && h.Provider != "unknown" && h.Provider != w.Provider) || r.StopOutcome != "not_attempted" {
			return errors.New("start receipt identity mismatch")
		}
		if r.PaneOutcome == "confirmed" && (h.TabId == "" || h.Target.PaneId == "" || h.Target.TerminalId == "") ||
			r.LaunchOutcome == "confirmed" && (r.PaneOutcome != "confirmed" || h.Provider != w.Provider) ||
			r.PromptOutcome != "not_attempted" && (r.LaunchOutcome != "confirmed" || command.SubmittedRequest.GetAgentStart().GetInitialPrompt() == "") {
			return errors.New("invalid start stage dependencies")
		}
	} else if w := command.AgentStop; w == nil || h.WorkspaceId != w.WorkspaceId || h.TabId != w.TabId ||
		h.Provider != w.Provider || !proto.Equal(h.Target, w.Target) ||
		r.PaneOutcome != "not_attempted" || r.LaunchOutcome != "not_attempted" || r.PromptOutcome != "not_attempted" {
		return errors.New("stop receipt identity mismatch")
	}
	last := map[string]string{"create_pane": "not_attempted", "launch": "not_attempted", "prompt": "not_attempted", "close_pane": "not_attempted"}
	for i, event := range r.Stages {
		if event == nil || len(event.ProtoReflect().GetUnknown()) != 0 || last[event.Stage] == "" {
			return errors.New("invalid lifecycle stage")
		}
		if command.AgentStart != nil && event.Stage == "close_pane" || command.AgentStop != nil && event.Stage != "close_pane" {
			return errors.New("unexpected lifecycle stage")
		}
		if event.Before {
			if event.Outcome != "unknown" || last[event.Stage] != "not_attempted" ||
				(i > 0 && r.Stages[i-1].Before) || event.Stage == "launch" && last["create_pane"] != "confirmed" ||
				event.Stage == "prompt" && last["launch"] != "confirmed" {
				return errors.New("invalid lifecycle intent order")
			}
			for _, prior := range r.Stages[:i] {
				if prior.Stage == event.Stage {
					return errors.New("lifecycle effect may not be retried")
				}
			}
		} else if i == 0 || !r.Stages[i-1].Before || r.Stages[i-1].Stage != event.Stage ||
			!slices.Contains([]string{"not_attempted", "unknown", "confirmed"}, event.Outcome) {
			return errors.New("lifecycle completion has no matching intent")
		}
		last[event.Stage] = event.Outcome
	}
	for stage, outcome := range last {
		if LifecycleStageOutcome(r, stage) != outcome {
			return errors.New("lifecycle summary disagrees with durable stages")
		}
	}
	return nil
}

func ValidateLifecycleAdvance(old, next *pb.AgentLifecycleReceipt, command *pb.Command) error {
	if err := ValidateLifecycleReceipt(next, command); err != nil {
		return err
	}
	if old == nil {
		return nil
	}
	if err := ValidateLifecycleReceipt(old, command); err != nil {
		return err
	}
	if len(old.Stages) > len(next.Stages) {
		return errors.New("lifecycle checkpoint regressed")
	}
	for i, event := range old.Stages {
		if !proto.Equal(event, next.Stages[i]) {
			return errors.New("lifecycle checkpoint rewrote durable evidence")
		}
	}
	a, b := old.Handle, next.Handle
	for _, pair := range [][2]string{{a.WorkspaceId, b.WorkspaceId}, {a.TabId, b.TabId},
		{a.Target.PaneId, b.Target.PaneId}, {a.Target.TerminalId, b.Target.TerminalId},
		{a.Target.AgentSessionId, b.Target.AgentSessionId}, {a.Target.SessionName, b.Target.SessionName},
		{a.Target.SessionIncarnation, b.Target.SessionIncarnation}} {
		if pair[0] != "" && pair[0] != pair[1] {
			return errors.New("lifecycle checkpoint changed a pinned identity")
		}
	}
	if a.Provider != "" && a.Provider != "unknown" && a.Provider != b.Provider {
		return errors.New("lifecycle checkpoint changed provider")
	}
	if len(old.Stages) == len(next.Stages) && !proto.Equal(old, next) {
		return errors.New("lifecycle checkpoint changed without a stage")
	}
	return nil
}

func LifecycleHasUnknown(r *pb.AgentLifecycleReceipt) bool {
	return r != nil && slices.Contains([]string{r.PaneOutcome, r.LaunchOutcome, r.PromptOutcome, r.StopOutcome}, "unknown")
}

func LifecycleHasEffect(r *pb.AgentLifecycleReceipt) bool {
	return r != nil && (r.PaneOutcome != "not_attempted" || r.LaunchOutcome != "not_attempted" ||
		r.PromptOutcome != "not_attempted" || r.StopOutcome != "not_attempted")
}

func LifecycleComplete(r *pb.AgentLifecycleReceipt, c *pb.Command) bool {
	if r == nil {
		return false
	}
	if c.AgentStop != nil {
		return r.StopOutcome == "confirmed"
	}
	return r.PaneOutcome == "confirmed" && r.LaunchOutcome == "confirmed" &&
		(c.SubmittedRequest.GetAgentStart().GetInitialPrompt() == "" || r.PromptOutcome == "confirmed")
}

func ValidLifecycleDetail(detail string) bool {
	return slices.Contains([]string{"agent_started", "agent_stopped", "lifecycle_partial", "lifecycle_uncertain",
		"lifecycle_unresolved", "node_restarted", "server_restarted", "node_disconnected", "deadline_expired",
		"journal_full", "precondition_failed", "target_changed", "agent_busy", "agent_blocked", "herdr_unavailable",
		"unsupported", "authorization_changed", "project_not_authorized", "persistence_failed"}, detail) || ValidSessionError(detail)
}

func ValidateLifecycleResult(result *pb.CommandResult, command *pb.Command) error {
	if result == nil || result.CommandId != command.CommandId || !ValidLifecycleDetail(result.Detail) ||
		len(result.ProtoReflect().GetUnknown()) != 0 || result.Payload != nil || result.WorkspaceEnsure != nil ||
		result.WorktreeCreate != nil || result.AgentControl != nil || result.SessionEnsure != nil {
		return errors.New("invalid lifecycle result")
	}
	r := result.AgentLifecycle
	if result.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED &&
		(result.Detail == "agent_started" || result.Detail == "agent_stopped") {
		return errors.New("unsuccessful lifecycle result has a success detail")
	}
	if r != nil {
		if err := ValidateLifecycleReceipt(r, command); err != nil {
			return err
		}
	}
	switch result.Status {
	case pb.CommandStatus_COMMAND_STATUS_SUCCEEDED:
		if LifecycleComplete(r, command) &&
			(command.AgentStart != nil && result.Detail == "agent_started" || command.AgentStop != nil && result.Detail == "agent_stopped") {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		if r == nil || LifecycleHasUnknown(r) || result.Detail == "persistence_failed" ||
			slices.Contains([]string{"node_disconnected", "server_restarted", "deadline_expired"}, result.Detail) {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_FAILED:
		if LifecycleHasEffect(r) && !LifecycleHasUnknown(r) && !LifecycleComplete(r, command) {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		if !LifecycleHasEffect(r) && result.Detail == "deadline_expired" {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_REJECTED:
		if !LifecycleHasEffect(r) {
			return nil
		}
	}
	return errors.New("lifecycle status contradicts effect evidence")
}
