package protocol

import (
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestManagedCommandDefaultsPreserveOriginalFingerprint(t *testing.T) {
	for _, worktree := range []bool{false, true} {
		r := &pb.SubmitCommandRequest{NodeInstanceId: "node", IdempotencyKey: "key", CommandType: WorkspaceEnsureCommandType,
			WorkspaceEnsure: &pb.WorkspaceEnsure{ProjectId: "my-project_2"}}
		if worktree {
			r.CommandType, r.WorkspaceEnsure = WorktreeCreateCommandType, nil
			r.WorktreeCreate = &pb.WorktreeCreate{ProjectId: "my-project_2", Name: "task-one"}
		}
		normalized, err := NormalizeCommandRequest(r)
		if err != nil {
			t.Fatal(err)
		}
		c := &pb.Command{CommandId: NewCommandID(), TargetId: r.NodeInstanceId, IdempotencyKey: r.IdempotencyKey,
			CommandType: r.CommandType, Ttl: normalized.Ttl, ExpiresAt: timestamppb.New(time.Now().Add(normalized.Ttl.AsDuration())),
			Actor: &pb.Actor{ActorId: "client", Role: pb.Role_ROLE_CONTROLLER}, SubmittedRequest: normalized}
		if worktree {
			c.WorktreeCreate = proto.Clone(r.WorktreeCreate).(*pb.WorktreeCreate)
			c.WorktreeCreate.BindingRevision, c.WorktreeCreate.Branch = ProjectRevision(1), "task-one"
		} else {
			c.WorkspaceEnsure = proto.Clone(r.WorkspaceEnsure).(*pb.WorkspaceEnsure)
			c.WorkspaceEnsure.BindingRevision = ProjectRevision(1)
		}
		if err := ValidateCommand(c, "node"); err != nil {
			t.Fatal(err)
		}
		if _, revision := RequestProject(c.SubmittedRequest); revision != "" {
			t.Fatal("original default overwritten")
		}
		bad := proto.Clone(c).(*pb.Command)
		bad.SubmittedRequest.NodeInstanceId = "another"
		if ValidateCommand(bad, "node") == nil {
			t.Fatal("original identity mismatch accepted")
		}
		bad = proto.Clone(c).(*pb.Command)
		bad.SubmittedRequest = nil
		if worktree && ValidateCommand(bad, "node") == nil {
			t.Fatal("historical empty base accepted")
		}
	}
}

func TestManagedConfigurationBoundsAndSanitizedAcknowledgements(t *testing.T) {
	if ValidateProjectRegistration(&pb.RegisterProjectRequest{NodeInstanceId: "node", ProjectId: "Operator:chosen-id_42", CheckoutPath: `C:\checkout`}) != nil {
		t.Fatal("generic ASCII ID rejected")
	}
	for _, id := range []string{"", "../path", "project with spaces", "caf\u00e9"} {
		if ValidateProjectRegistration(&pb.RegisterProjectRequest{NodeInstanceId: "node", ProjectId: id, CheckoutPath: `C:\checkout`}) == nil {
			t.Fatal("invalid project accepted", id)
		}
	}
	ack := &pb.ProjectAck{ProjectId: "project", Generation: 1, Status: "invalid", ErrorCode: "invalid_path"}
	if ValidateProjectAck(ack) != nil {
		t.Fatal("sanitized invalid result rejected")
	}
	ack.ErrorCode = `open C:\private\checkout failed`
	if ValidateProjectAck(ack) == nil {
		t.Fatal("raw filesystem diagnostic accepted")
	}
}
