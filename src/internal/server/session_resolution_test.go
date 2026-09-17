package server

import (
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestNamedProjectAdmissionPinsSeparateResolvedIncarnation(t *testing.T) {
	for _, workspace := range []bool{false, true} {
		h := newCommandHarness(t, t.TempDir())
		h.api.workspacePolicy = coordinatorWorktreePolicy(t)
		entry := namedEntry(t, h)
		request := worktreeRequest("resolve-session")
		request.WorktreeCreate.SessionName = "build"
		if workspace {
			request.CommandType = protocol.WorkspaceEnsureCommandType
			request.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: request.WorktreeCreate.ProjectId,
				BindingRevision: request.WorktreeCreate.BindingRevision, SessionName: "build"}
			request.WorktreeCreate = nil
		}
		original, err := protocol.NormalizeCommandRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		record, err := h.api.SubmitCommand(agentPeer("client"), request)
		if err != nil {
			t.Fatal(err)
		}
		_, incarnation := protocol.CommandSession(record.Command)
		if incarnation != strings.Repeat("a", 64) || !proto.Equal(record.Command.SubmittedRequest, original) {
			t.Fatalf("admission failed to separate original selector from resolved target: %v", record.Command)
		}
		entry.view.Sessions[0].Incarnation = strings.Repeat("b", 64)
		dispatched, err := h.api.prepareCommand(entry, <-entry.outbound)
		if err != nil || dispatched != nil {
			t.Fatalf("queued mutation rebound to replacement session: %v %v", dispatched, err)
		}
		retry, err := h.api.SubmitCommand(agentPeer("client"), request)
		if err != nil || retry.Command.CommandId != record.Command.CommandId ||
			retry.Status != pb.CommandStatus_COMMAND_STATUS_REJECTED ||
			!proto.Equal(retry.Command.SubmittedRequest, original) {
			t.Fatalf("exact retry was resolved again: %v %v", retry, err)
		}
		changed := proto.Clone(request).(*pb.SubmitCommandRequest)
		if changed.WorkspaceEnsure != nil {
			changed.WorkspaceEnsure.SessionIncarnation = incarnation
		} else {
			changed.WorktreeCreate.SessionIncarnation = incarnation
		}
		if _, err := h.api.SubmitCommand(agentPeer("client"), changed); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("explicit selector retroactively changed original retry identity: %v", err)
		}
	}
}
