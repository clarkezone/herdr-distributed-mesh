package protocol

import (
	"errors"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

const WorkspaceEnsureCommandType = "workspace.ensure.v1"
const WorkspaceEnsureCapability = "commands.workspace-ensure.v1"

func ValidateWorkspaceEnsure(request *pb.WorkspaceEnsure) error {
	if request == nil || !commandToken.MatchString(request.ProjectId) || !commandToken.MatchString(request.BindingRevision) ||
		len(request.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("workspace ensure requires a bounded project ID and binding revision")
	}
	return nil
}

func ValidateWorkspaceResult(result *pb.CommandResult) error {
	if result == nil || !ValidCommandID(result.CommandId) || result.Payload != nil || len(result.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid workspace result")
	}
	if result.Status == pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		value := result.WorkspaceEnsure
		if value == nil || !commandToken.MatchString(value.ProjectId) || !commandToken.MatchString(value.BindingRevision) ||
			!commandToken.MatchString(value.WorkspaceId) || len(value.ProtoReflect().GetUnknown()) != 0 {
			return errors.New("workspace success requires a bounded typed result")
		}
		if (value.Created && result.Detail == "workspace_created") || (!value.Created && result.Detail == "workspace_present") {
			return nil
		}
		return errors.New("workspace success detail disagrees with result")
	}
	if result.WorkspaceEnsure != nil {
		return errors.New("unsuccessful workspace result must not contain success data")
	}
	switch result.Status {
	case pb.CommandStatus_COMMAND_STATUS_REJECTED:
		switch result.Detail {
		case "journal_full", "project_unresolved", "project_not_authorized", "precondition_failed", "ambiguous_workspace", "herdr_unavailable", "authorization_changed":
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_TIMED_OUT:
		if result.Detail == "deadline_expired" {
			return nil
		}
	case pb.CommandStatus_COMMAND_STATUS_INDETERMINATE:
		if result.Detail == "node_restarted" || result.Detail == "herdr_outcome_unknown" {
			return nil
		}
	}
	return errors.New("invalid workspace outcome")
}

func ValidateCommandResult(result *pb.CommandResult) error {
	if ValidateProbeResult(result) == nil {
		return nil
	}
	return ValidateWorkspaceResult(result)
}

func ValidateResultForCommand(result *pb.CommandResult, command *pb.Command) error {
	if result == nil || command == nil || result.CommandId != command.CommandId {
		return errors.New("command result identity mismatch")
	}
	switch command.CommandType {
	case ProbeCommandType:
		return ValidateProbeResult(result)
	case WorkspaceEnsureCommandType:
		if err := ValidateWorkspaceResult(result); err != nil {
			return err
		}
		if result.WorkspaceEnsure != nil && (command.WorkspaceEnsure == nil ||
			result.WorkspaceEnsure.ProjectId != command.WorkspaceEnsure.ProjectId ||
			result.WorkspaceEnsure.BindingRevision != command.WorkspaceEnsure.BindingRevision) {
			return errors.New("workspace result binding mismatch")
		}
		return nil
	default:
		return errors.New("unsupported command type")
	}
}
