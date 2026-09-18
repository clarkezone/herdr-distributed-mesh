package protocol

import (
	"errors"
	"regexp"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
)

const SessionEnsureCommandType = "sessions.ensure.v1"
const SessionManageCapability = "sessions.manage.v1"

var sessionIncarnation = regexp.MustCompile(`^[a-f0-9]{64}$`)

func ValidSessionIncarnation(value string) bool { return sessionIncarnation.MatchString(value) }

// An empty name selects the configured default, which may also be pinned.
// Missing selectors retain the historical, unmanaged default identity.
func ValidateSessionSelector(name, incarnation string, requireIncarnation bool) error {
	if (name != "" && herdrsession.ValidateName(name) != nil) ||
		(incarnation != "" && !ValidSessionIncarnation(incarnation)) ||
		(requireIncarnation && name != "" && incarnation == "") {
		return errors.New("invalid session name or incarnation")
	}
	return nil
}

func ValidateSessionEnsure(value *pb.SessionEnsure) error {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || herdrsession.ValidateName(value.Name) != nil {
		return errors.New("session ensure requires a portable session name")
	}
	return nil
}

// Session selection failures are safe categories, never native diagnostics.
func ValidSessionError(code string) bool {
	switch code {
	case "session_manager_unavailable", "session_unavailable", "session_replaced",
		"session_start_failed", "unsupported_protocol", "session_default_unmanaged",
		"session_capacity", "precondition_failed":
		return true
	}
	return false
}

func ValidateSessionEnsureResult(result *pb.CommandResult) error {
	if result == nil || !ValidCommandID(result.CommandId) || len(result.ProtoReflect().GetUnknown()) != 0 ||
		result.Payload != nil || result.WorkspaceEnsure != nil || result.WorktreeCreate != nil || result.AgentControl != nil || result.AgentLifecycle != nil {
		return errors.New("invalid session ensure result")
	}
	if result.Status == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		value := result.SessionEnsure
		if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
			herdrsession.ValidateName(value.Name) != nil || !ValidSessionIncarnation(value.Incarnation) ||
			value.Status != "ready" || value.ErrorCode != "" || value.Herdr != nil ||
			value.HerdrReceivedAt != nil || value.Stale || result.Detail != "session_ready" {
			return errors.New("session success requires a ready actual incarnation")
		}
		return nil
	}
	if result.SessionEnsure != nil {
		return errors.New("unsuccessful session ensure contains success data")
	}
	switch result.Status {
	case pb.CommandStatus_COMMAND_STATUS_REJECTED:
		if result.Detail == "journal_full" || result.Detail == "authorization_changed" || ValidSessionError(result.Detail) {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		if result.Detail == "deadline_expired" {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		if result.Detail == "node_restarted" || result.Detail == "startup_uncertain" {
			return nil
		}
	}
	return errors.New("invalid session ensure outcome")
}

func matchesSession(actualName, actualIncarnation, name, incarnation string) bool {
	return actualName == name && (incarnation == "" || actualIncarnation == incarnation)
}

func CommandSession(command *pb.Command) (name, incarnation string) {
	if command == nil {
		return "", ""
	}
	switch command.CommandType {
	case AgentStartCommandType:
		return command.AgentStart.GetSessionName(), command.AgentStart.GetSessionIncarnation()
	case AgentStopCommandType:
		return command.AgentStop.GetTarget().GetSessionName(), command.AgentStop.GetTarget().GetSessionIncarnation()
	case WorkspaceEnsureCommandType:
		return command.WorkspaceEnsure.GetSessionName(), command.WorkspaceEnsure.GetSessionIncarnation()
	case WorktreeCreateCommandType:
		return command.WorktreeCreate.GetSessionName(), command.WorktreeCreate.GetSessionIncarnation()
	case AgentControlCommandType:
		return command.AgentControl.GetTarget().GetSessionName(), command.AgentControl.GetTarget().GetSessionIncarnation()
	case SessionEnsureCommandType:
		return command.SessionEnsure.GetName(), ""
	default:
		return "", ""
	}
}
