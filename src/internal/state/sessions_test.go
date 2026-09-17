package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

var sessionTestIncarnation = strings.Repeat("a", 64)

func requireSessionConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("want command conflict, got %v", err)
	}
}

func sessionCommand(id int) *pb.Command {
	command := probeCommand(id, "actor", "ensure-key", "node", commandTestTime)
	command.CommandType = protocol.SessionEnsureCommandType
	command.SessionEnsure = &pb.SessionEnsure{Name: "worker"}
	return command
}

func TestSessionEnsureReceiptsDurable(t *testing.T) {
	outcomes := []struct {
		status pb.CommandStatus
		detail string
	}{
		{statusSucceeded, "session_ready"}, {statusIndeterminate, "startup_uncertain"},
		{statusRejected, "session_manager_unavailable"}, {statusRejected, "session_unavailable"},
		{statusRejected, "session_replaced"}, {statusRejected, "session_start_failed"},
		{statusRejected, "unsupported_protocol"}, {statusRejected, "session_default_unmanaged"},
		{statusRejected, "session_capacity"}, {statusRejected, "precondition_failed"},
	}
	for _, outcome := range outcomes {
		t.Run(outcome.detail, func(t *testing.T) {
			ctx := context.Background()
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s, j := openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			requireOK(t, s.Bind(ctx, "stable", "node"))
			command := sessionCommand(1)
			admitWorkspace(t, s, command, true)
			got, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if !claimed || got != nil {
				t.Fatal("session ensure not claimed")
			}
			result := probeResult(command, outcome.status, outcome.detail)
			if outcome.status == statusSucceeded {
				result.SessionEnsure = &pb.SessionView{Name: "worker", Incarnation: sessionTestIncarnation, Status: "ready"}
			}
			requireOK(t, j.Complete(ctx, result))
			record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) {
				t.Fatal("session result omitted from coordinator receipt")
			}
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			requireOK(t, j.Recover(ctx))
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], result) {
				t.Fatal("session receipt lost across restart")
			}
			stored, err := s.GetCommand(ctx, "actor", command.CommandId)
			requireOK(t, err)
			if !proto.Equal(stored, record) {
				t.Fatal("coordinator receipt changed across restart")
			}
			got, claimed, err = j.Claim(ctx, command)
			requireOK(t, err)
			if claimed || !proto.Equal(got, result) {
				t.Fatal("retry reexecuted durable session ensure")
			}
			requireOK(t, j.Acknowledge(ctx, result.CommandId, result.Status))
			requireOK(t, j.Complete(ctx, result))
			pending, err = j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("duplicate completion reset acknowledgement")
			}
			if result.SessionEnsure != nil {
				bad := proto.Clone(result).(*pb.CommandResult)
				bad.SessionEnsure.Incarnation = strings.Repeat("b", 64)
				requireSessionConflict(t, j.Complete(ctx, bad))
				_, err := s.FinishCommand(ctx, "node", "stable", bad, commandTestTime.Add(3*time.Second))
				requireSessionConflict(t, err)
			}
		})
	}
}

func TestSessionEnsureIntentRecoveryNeverReexecutes(t *testing.T) {
	for _, recover := range []bool{false, true} {
		ctx := context.Background()
		path := testPath(t)
		j := openTestNodeJournal(t, path)
		command := sessionCommand(1)
		_, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if !claimed {
			t.Fatal("session not claimed")
		}
		requireOK(t, j.Close())
		j = openTestNodeJournal(t, path)
		if recover {
			requireOK(t, j.Recover(ctx))
		}
		result, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if claimed || result.GetStatus() != statusIndeterminate || result.GetDetail() != "startup_uncertain" || result.SessionEnsure != nil {
			t.Fatal("uncertain startup reexecuted or reported successful")
		}
		bad := proto.Clone(command).(*pb.Command)
		bad.SessionEnsure.Name = "different"
		if _, _, err := j.Claim(ctx, bad); !errors.Is(err, ErrCommandConflict) {
			t.Fatal("name drift did not conflict", err)
		}
	}
}

func TestSessionQualifiedOriginalAndResolvedIdentities(t *testing.T) {
	for _, kind := range []string{protocol.WorkspaceEnsureCommandType, protocol.WorktreeCreateCommandType, protocol.AgentControlCommandType, protocol.SessionEnsureCommandType} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s := openTestStore(t, testPath(t))
			requireOK(t, s.Bind(ctx, "stable", "node"))
			command := sessionCommand(1)
			switch kind {
			case protocol.WorkspaceEnsureCommandType:
				command = workspaceCommand(1, "project")
				command.WorkspaceEnsure.SessionName, command.WorkspaceEnsure.SessionIncarnation = "worker", sessionTestIncarnation
			case protocol.WorktreeCreateCommandType:
				command = worktreeCommand(1, "project")
				command.WorktreeCreate.SessionName, command.WorktreeCreate.SessionIncarnation = "worker", sessionTestIncarnation
			case protocol.AgentControlCommandType:
				command = agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT)
				command.AgentControl.Target.SessionName, command.AgentControl.Target.SessionIncarnation = "worker", sessionTestIncarnation
			}
			request := &pb.SubmitCommandRequest{NodeInstanceId: command.TargetId, IdempotencyKey: command.IdempotencyKey,
				CommandType: kind, Ttl: command.Ttl, WorkspaceEnsure: command.WorkspaceEnsure, WorktreeCreate: command.WorktreeCreate,
				AgentControl: command.AgentControl, SessionEnsure: command.SessionEnsure}
			command.SubmittedRequest = proto.Clone(request).(*pb.SubmitCommandRequest)
			_, created, err := s.CreateCommand(ctx, command, commandTestTime)
			requireOK(t, err)
			if !created {
				t.Fatal("new session-qualified command not admitted")
			}
			retry := proto.Clone(command).(*pb.Command)
			retry.CommandId = protocol.NewCommandID()
			_, created, err = s.CreateCommand(ctx, retry, commandTestTime)
			requireOK(t, err)
			if created {
				t.Fatal("exact original request did not replay")
			}
			for _, incarnation := range []bool{false, true} {
				if kind == protocol.SessionEnsureCommandType && incarnation {
					continue
				}
				drift := proto.Clone(command).(*pb.Command)
				name, pin := "different", sessionTestIncarnation
				if incarnation {
					name, pin = "worker", strings.Repeat("b", 64)
				}
				switch kind {
				case protocol.WorkspaceEnsureCommandType:
					drift.WorkspaceEnsure.SessionName, drift.WorkspaceEnsure.SessionIncarnation = name, pin
					drift.SubmittedRequest.WorkspaceEnsure = proto.Clone(drift.WorkspaceEnsure).(*pb.WorkspaceEnsure)
				case protocol.WorktreeCreateCommandType:
					drift.WorktreeCreate.SessionName, drift.WorktreeCreate.SessionIncarnation = name, pin
					drift.SubmittedRequest.WorktreeCreate = proto.Clone(drift.WorktreeCreate).(*pb.WorktreeCreate)
				case protocol.AgentControlCommandType:
					drift.AgentControl.Target.SessionName, drift.AgentControl.Target.SessionIncarnation = name, pin
					drift.SubmittedRequest.AgentControl = proto.Clone(drift.AgentControl).(*pb.AgentControl)
				case protocol.SessionEnsureCommandType:
					drift.SessionEnsure.Name = name
					drift.SubmittedRequest.SessionEnsure.Name = name
				}
				_, _, err := s.CreateCommand(ctx, drift, commandTestTime)
				requireSessionConflict(t, err)
				if matchingNodeCommand(command, drift) {
					t.Fatal("node execution fingerprint ignored session identity")
				}
			}
		})
	}
}

func TestSessionResolutionDoesNotReplaceSubmittedIdentity(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, testPath(t))
	requireOK(t, s.Bind(ctx, "stable", "node"))
	command := workspaceCommand(1, "project")
	command.WorkspaceEnsure.SessionName = "worker"
	command.SubmittedRequest = &pb.SubmitCommandRequest{NodeInstanceId: command.TargetId, IdempotencyKey: command.IdempotencyKey,
		CommandType: command.CommandType, Ttl: command.Ttl, WorkspaceEnsure: proto.Clone(command.WorkspaceEnsure).(*pb.WorkspaceEnsure)}
	command.SubmittedRequest.WorkspaceEnsure.BindingRevision = ""
	command.WorkspaceEnsure.BindingRevision, command.WorkspaceEnsure.SessionIncarnation = protocol.ProjectRevision(1), sessionTestIncarnation
	original, _, err := s.CreateCommand(ctx, command, commandTestTime)
	requireOK(t, err)
	drift := proto.Clone(command).(*pb.Command)
	drift.WorkspaceEnsure.BindingRevision, drift.WorkspaceEnsure.SessionIncarnation = protocol.ProjectRevision(2), strings.Repeat("b", 64)
	got, created, err := s.CreateCommand(ctx, drift, commandTestTime)
	requireOK(t, err)
	if created || !proto.Equal(original, got) || matchingNodeCommand(command, drift) {
		t.Fatal("submitted replay identity conflated with resolved execution identity")
	}
	command.SubmittedRequest, drift.SubmittedRequest = nil, nil
	if matchingRequest(command, drift) {
		t.Fatal("legacy resolved-body retry fingerprint changed")
	}
}
