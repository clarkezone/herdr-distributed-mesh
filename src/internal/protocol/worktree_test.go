package protocol

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func testWorktree() *pb.Command {
	command := testProbe()
	command.CommandType = WorktreeCreateCommandType
	command.WorktreeCreate = &pb.WorktreeCreate{ProjectId: "AgentFlow", BindingRevision: "r2", Name: "task-one", Branch: "task-one", BaseCommit: strings.Repeat("a", 40)}
	return command
}

func TestWorktreeInputIsPortableAndImmutable(t *testing.T) {
	for _, good := range []string{"a", "task-123", "task_one", strings.Repeat("a", 64)} {
		if !ValidWorktreeName(good) {
			t.Fatalf("valid leaf %q rejected", good)
		}
	}
	for _, bad := range []string{"", "../escape", `C:\escape`, "a/b", `a\b`, "--option", ".", "a.lock", "a.", "a ", "Uppercase", "con", "com1", "lpt9", strings.Repeat("a", 65)} {
		if ValidWorktreeName(bad) {
			t.Fatalf("unsafe leaf %q accepted", bad)
		}
	}
	for _, bad := range []string{"HEAD", "main", "refs/heads/main", strings.Repeat("A", 40), strings.Repeat("f", 41), "--help"} {
		command := testWorktree()
		command.WorktreeCreate.BaseCommit = bad
		if ValidateCommand(command, command.TargetId) == nil {
			t.Fatal("mutable or invalid base accepted")
		}
	}
	command := testWorktree()
	command.WorktreeCreate.BaseCommit = strings.Repeat("f", 64)
	if err := ValidateCommand(command, command.TargetId); err != nil {
		t.Fatal(err)
	}
	request := &pb.SubmitCommandRequest{NodeInstanceId: command.TargetId, CommandType: command.CommandType, IdempotencyKey: "create-1", WorktreeCreate: command.WorktreeCreate}
	copy, err := NormalizeCommandRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	copy.WorktreeCreate.Name = "different"
	if request.WorktreeCreate.Name != "task-one" {
		t.Fatal("normalization aliases mutable caller input")
	}
	project, revision := CommandProject(command)
	if project != "AgentFlow" || revision != "r2" {
		t.Fatal("command project not bound")
	}
	if p, r := RequestProject(request); p != project || r != revision {
		t.Fatal("request/command binding disagreement")
	}
}

func TestWorktreeRejectsMixedCommandsAndResults(t *testing.T) {
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.WorktreeCreate = nil },
		func(c *pb.Command) {
			c.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: "AgentFlow", BindingRevision: "r2"}
		},
		func(c *pb.Command) { c.CommandType = ProbeCommandType },
		func(c *pb.Command) { c.CommandType = WorkspaceEnsureCommandType },
		func(c *pb.Command) { c.WorktreeCreate.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
		func(c *pb.Command) { c.Actor.Role = pb.Role_ROLE_NODE },
	} {
		command := testWorktree()
		mutate(command)
		if ValidateCommand(command, command.TargetId) == nil {
			t.Fatal("invalid worktree command accepted")
		}
	}
	command := testWorktree()
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "worktree_created",
		WorktreeCreate: &pb.WorktreeCreateResult{ProjectId: "AgentFlow", BindingRevision: "r2", WorkspaceId: "w2", Name: "task-one", Branch: "task-one", BaseCommit: strings.Repeat("a", 40)}}
	if err := ValidateResultForCommand(result, command); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.CommandResult){
		func(r *pb.CommandResult) { r.WorktreeCreate.ProjectId = "other" },
		func(r *pb.CommandResult) { r.WorktreeCreate.BindingRevision = "r3" },
		func(r *pb.CommandResult) { r.WorktreeCreate.Name = "other" },
		func(r *pb.CommandResult) { r.WorktreeCreate.Branch = "other" },
		func(r *pb.CommandResult) { r.WorktreeCreate.BaseCommit = strings.Repeat("b", 40) },
		func(r *pb.CommandResult) { r.WorktreeCreate.WorkspaceId = `C:\private` },
		func(r *pb.CommandResult) { r.WorkspaceEnsure = &pb.WorkspaceEnsureResult{} },
		func(r *pb.CommandResult) {
			r.Status = pb.CommandStatus_COMMAND_STATUS_REJECTED
			r.Detail = "precondition_failed"
		},
		func(r *pb.CommandResult) { r.WorktreeCreate.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
	} {
		copy := proto.Clone(result).(*pb.CommandResult)
		mutate(copy)
		if ValidateResultForCommand(copy, command) == nil {
			t.Fatal("mismatched worktree result accepted")
		}
	}
	probe := testProbe()
	probe.CommandId = command.CommandId
	if ValidateResultForCommand(result, probe) == nil {
		t.Fatal("worktree result completed probe")
	}
	workspace := workspaceCommand()
	workspace.CommandId = command.CommandId
	if ValidateResultForCommand(result, workspace) == nil {
		t.Fatal("worktree result completed workspace ensure")
	}
}
