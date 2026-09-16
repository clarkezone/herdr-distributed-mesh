package protocol

import (
	"errors"
	"regexp"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

const WorktreeCreateCommandType = "worktree.create.v1"
const WorktreeCreateCapability = "commands.worktree-create.v1"

var worktreeLeaf = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
var reservedLeaf = regexp.MustCompile(`^(con|prn|aux|nul|com[0-9]|lpt[0-9])$`)
var commitID = regexp.MustCompile(`^([a-f0-9]{40}|[a-f0-9]{64})$`)

// Names also become local path components and Git refs, so use one portable subset.
func ValidWorktreeName(name string) bool {
	return worktreeLeaf.MatchString(name) && !reservedLeaf.MatchString(name)
}

func ValidateWorktreeCreate(request *pb.WorktreeCreate) error {
	if request == nil || !commandToken.MatchString(request.ProjectId) || !commandToken.MatchString(request.BindingRevision) ||
		!ValidWorktreeName(request.Name) || !ValidWorktreeName(request.Branch) || !commitID.MatchString(request.BaseCommit) ||
		len(request.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("worktree create requires project/revision, portable name/branch, and a full lowercase commit ID")
	}
	return nil
}

func ValidateWorktreeResult(result *pb.CommandResult) error {
	if result == nil || !ValidCommandID(result.CommandId) || result.Payload != nil || result.WorkspaceEnsure != nil || result.AgentControl != nil ||
		len(result.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("invalid worktree result")
	}
	if result.Status != pb.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		if result.WorktreeCreate != nil {
			return errors.New("unsuccessful worktree result contains success data")
		}
		return ValidateWorkspaceResult(result)
	}
	value := result.WorktreeCreate
	if result.Detail != "worktree_created" || value == nil || !commandToken.MatchString(value.WorkspaceId) ||
		len(value.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("worktree success requires confirmed typed result")
	}
	return ValidateWorktreeCreate(&pb.WorktreeCreate{
		ProjectId: value.ProjectId, BindingRevision: value.BindingRevision, Name: value.Name, Branch: value.Branch, BaseCommit: value.BaseCommit,
	})
}

func CommandProject(command *pb.Command) (projectID, revision string) {
	if command == nil {
		return "", ""
	}

	switch command.CommandType {
	case WorkspaceEnsureCommandType:
		return command.WorkspaceEnsure.GetProjectId(), command.WorkspaceEnsure.GetBindingRevision()
	case WorktreeCreateCommandType:
		return command.WorktreeCreate.GetProjectId(), command.WorktreeCreate.GetBindingRevision()
	default:
		return "", ""
	}
}

func RequestProject(request *pb.SubmitCommandRequest) (projectID, revision string) {
	if request == nil {
		return "", ""
	}
	switch request.CommandType {
	case WorkspaceEnsureCommandType:
		return request.WorkspaceEnsure.GetProjectId(), request.WorkspaceEnsure.GetBindingRevision()
	case WorktreeCreateCommandType:
		return request.WorktreeCreate.GetProjectId(), request.WorktreeCreate.GetBindingRevision()
	default:
		return "", ""
	}
}
