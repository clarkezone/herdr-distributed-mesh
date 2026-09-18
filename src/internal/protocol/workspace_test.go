package protocol

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func workspaceCommand() *pb.Command {
	command := testProbe()
	command.CommandType = WorkspaceEnsureCommandType
	command.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: "AgentFlow", BindingRevision: "r1"}
	return command
}

func TestWorkspaceContractDoesNotBroadenProbeOrAllowPaths(t *testing.T) {
	command := workspaceCommand()
	if err := ValidateCommand(command, command.TargetId); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProbeCommand(command, command.TargetId); err == nil {
		t.Fatal("probe validator accepted mutation")
	}
	for name, change := range map[string]func(*pb.Command){
		"missing binding":  func(c *pb.Command) { c.WorkspaceEnsure = nil },
		"missing revision": func(c *pb.Command) { c.WorkspaceEnsure.BindingRevision = "" },
		"path":             func(c *pb.Command) { c.WorkspaceEnsure.ProjectId = `C:\repo` },
		"traversal":        func(c *pb.Command) { c.WorkspaceEnsure.ProjectId = "../other" },
		"oversized":        func(c *pb.Command) { c.WorkspaceEnsure.ProjectId = strings.Repeat("a", 129) },
		"unknown":          func(c *pb.Command) { c.WorkspaceEnsure.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
		"probe smuggling":  func(c *pb.Command) { c.CommandType = ProbeCommandType },
	} {
		t.Run(name, func(t *testing.T) {
			copy := proto.Clone(command).(*pb.Command)
			change(copy)
			if err := ValidateCommand(copy, copy.TargetId); err == nil {
				t.Fatal("invalid mutation accepted")
			}
		})
	}
	request := &pb.SubmitCommandRequest{NodeInstanceId: command.TargetId, CommandType: command.CommandType, IdempotencyKey: "key", WorkspaceEnsure: command.WorkspaceEnsure}
	copy, err := NormalizeCommandRequest(request)
	if err != nil || copy.Ttl.AsDuration() != DefaultCommandTTL || request.Ttl != nil {
		t.Fatal("normalization failed", err)
	}
	copy.WorkspaceEnsure.BindingRevision = "different"
	if request.WorkspaceEnsure.BindingRevision != "r1" {
		t.Fatal("normalization aliases caller input")
	}
	if _, err := NormalizeProbeRequest(request); err == nil {
		t.Fatal("probe request accepted workspace")
	}
}

func TestWorkspaceResultIsBoundToOriginalProject(t *testing.T) {
	command := workspaceCommand()
	result := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "workspace_created",
		WorkspaceEnsure: &pb.WorkspaceEnsureResult{ProjectId: "AgentFlow", BindingRevision: "r1", WorkspaceId: "w1", Created: true}}
	if err := ValidateResultForCommand(result, command); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*pb.CommandResult){
		"project":          func(r *pb.CommandResult) { r.WorkspaceEnsure.ProjectId = "other" },
		"revision":         func(r *pb.CommandResult) { r.WorkspaceEnsure.BindingRevision = "r2" },
		"command":          func(r *pb.CommandResult) { r.CommandId = NewCommandID() },
		"missing result":   func(r *pb.CommandResult) { r.WorkspaceEnsure = nil },
		"created mismatch": func(r *pb.CommandResult) { r.WorkspaceEnsure.Created = false },
		"result on failure": func(r *pb.CommandResult) {
			r.Status = pb.CommandStatus_COMMAND_STATUS_REJECTED
			r.Detail = "precondition_failed"
		},
		"unknown": func(r *pb.CommandResult) { r.WorkspaceEnsure.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			copy := proto.Clone(result).(*pb.CommandResult)
			change(copy)
			if err := ValidateResultForCommand(copy, command); err == nil {
				t.Fatal("mismatched result accepted")
			}
		})
	}
	probe := testProbe()
	probe.CommandId = command.CommandId
	if err := ValidateResultForCommand(result, probe); err == nil {
		t.Fatal("workspace result completed probe")
	}
	result.WorkspaceEnsure = nil
	result.Status, result.Detail = pb.CommandStatus_COMMAND_STATUS_INDETERMINATE, "herdr_outcome_unknown"
	if err := ValidateResultForCommand(result, command); err != nil {
		t.Fatal(err)
	}
	if err := ValidateResultForCommand(result, probe); err == nil {
		t.Fatal("workspace error broadened probe results")
	}
}
