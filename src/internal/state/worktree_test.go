package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func worktreeCommand(id int, project string) *pb.Command {
	command := probeCommand(id, "actor", fmt.Sprintf("key%d", id), "node", commandTestTime)
	command.CommandType = protocol.WorktreeCreateCommandType
	command.WorktreeCreate = &pb.WorktreeCreate{
		ProjectId: project, BindingRevision: "revision1", Name: "feature-one",
		Branch: "feature-branch", BaseCommit: strings.Repeat("a", 40),
	}
	return command
}

func worktreeResult(command *pb.Command) *pb.CommandResult {
	value := command.WorktreeCreate
	return &pb.CommandResult{
		CommandId: command.CommandId, Status: statusSucceeded, Detail: "worktree_created",
		WorktreeCreate: &pb.WorktreeCreateResult{
			ProjectId: value.ProjectId, BindingRevision: value.BindingRevision, WorkspaceId: "workspace1",
			Name: value.Name, Branch: value.Branch, BaseCommit: value.BaseCommit,
		},
	}
}

func TestWorktreeOutcomesDurableAndImmutable(t *testing.T) {
	for _, outcome := range []struct {
		status pb.CommandStatus
		detail string
	}{
		{statusSucceeded, "worktree_created"}, {statusTimedOut, "deadline_expired"},
		{statusRejected, "journal_full"}, {statusRejected, "project_unresolved"},
		{statusRejected, "project_not_authorized"}, {statusRejected, "precondition_failed"},
		{statusRejected, "ambiguous_workspace"}, {statusRejected, "herdr_unavailable"},
		{statusRejected, "authorization_changed"},
		{statusIndeterminate, "node_restarted"}, {statusIndeterminate, "herdr_outcome_unknown"},
	} {
		t.Run(outcome.detail, func(t *testing.T) {
			ctx := context.Background()
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s := commandStoreAtPath(t, coordinatorPath)
			j := openTestNodeJournal(t, nodePath)
			command := worktreeCommand(1, "project1")
			if outcome.status == statusSucceeded {
				command.WorktreeCreate.Name = strings.Repeat("n", 64)
				command.WorktreeCreate.Branch = strings.Repeat("b", 64)
				command.WorktreeCreate.BaseCommit = strings.Repeat("a", 64)
			}
			admitWorkspace(t, s, command, true)
			got, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if got != nil || !claimed {
				t.Fatal("new worktree intent was not claimed")
			}
			result := probeResult(command, outcome.status, outcome.detail)
			if outcome.status == statusSucceeded {
				result = worktreeResult(command)
			}
			requireOK(t, j.Complete(ctx, result))
			record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) || !proto.Equal(record.Command, command) || len(record.Audit) != 3 {
				t.Fatal("worktree result, request, or audit lost")
			}
			for i, status := range []pb.CommandStatus{statusAccepted, statusRunning, outcome.status} {
				if record.Audit[i].Status != status || !record.Audit[i].OccurredAt.AsTime().Equal(commandTestTime.Add(time.Duration(i)*time.Second)) {
					t.Fatal("worktree audit order changed")
				}
			}
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], result) {
				t.Fatal("typed pending result did not survive restart")
			}
			requireOK(t, j.Acknowledge(ctx, result.CommandId, result.Status))
			requireOK(t, j.Complete(ctx, result))
			pending, err = j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("duplicate completion reset acknowledgement")
			}
			for _, replay := range []*pb.CommandResult{
				result, probeResult(command, statusIndeterminate, "node_restarted"),
				probeResult(command, statusIndeterminate, "herdr_outcome_unknown"),
			} {
				_, err := s.FinishCommand(ctx, "node", "wrong-stable", replay, commandTestTime.Add(3*time.Second))
				requireConflict(t, err)
				duplicate, err := s.FinishCommand(ctx, "node", "stable", replay, commandTestTime.Add(3*time.Second))
				requireOK(t, err)
				if !proto.Equal(duplicate, record) {
					t.Fatal("duplicate or uncertainty receipt rewrote typed result or audit")
				}
			}
			if outcome.status == statusSucceeded {
				changed := proto.Clone(result).(*pb.CommandResult)
				changed.WorktreeCreate.WorkspaceId = "different-workspace"
				if _, err := s.FinishCommand(ctx, "node", "stable", changed, commandTestTime.Add(4*time.Second)); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed terminal result accepted: %v", err)
				}
				if err := j.Complete(ctx, changed); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed node terminal result accepted: %v", err)
				}
			}
			requireOK(t, s.Close())
			s = openTestStore(t, coordinatorPath)
			stored, err := s.GetCommand(ctx, "actor", command.CommandId)
			requireOK(t, err)
			if !proto.Equal(stored, record) {
				t.Fatal("receipts changed persistent record")
			}
		})
	}
}

func TestWorktreeImmutableRetryIdentity(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := worktreeCommand(1, "project1")
	original := admitWorkspace(t, s, command, false)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.WorktreeCreate.ProjectId = "different-project" },
		func(c *pb.Command) { c.WorktreeCreate.BindingRevision = "revision2" },
		func(c *pb.Command) { c.WorktreeCreate.Name = "other-name" },
		func(c *pb.Command) { c.WorktreeCreate.Branch = "other-branch" },
		func(c *pb.Command) { c.WorktreeCreate.BaseCommit = strings.Repeat("b", 64) },
		func(c *pb.Command) {
			c.CommandType, c.WorktreeCreate = protocol.WorkspaceEnsureCommandType, nil
			c.WorkspaceEnsure = workspaceCommand(1, "project1").WorkspaceEnsure
		},
		func(c *pb.Command) { c.CommandType, c.WorktreeCreate = protocol.ProbeCommandType, nil },
	} {
		changed := proto.Clone(command).(*pb.Command)
		mutate(changed)
		if _, _, err := s.CreateCommand(ctx, changed, commandTestTime); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed retry accepted: %v", err)
		}
		if _, _, err := j.Claim(ctx, changed); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed intent accepted: %v", err)
		}
	}
	retry := proto.Clone(command).(*pb.Command)
	retry.CommandId = fmt.Sprintf("%032x", 2)
	retry.ExpiresAt = timestamppb.New(commandTestTime.Add(time.Hour))
	got, created, err := s.CreateCommand(ctx, retry, commandTestTime.Add(time.Hour))
	requireOK(t, err)
	if created || !proto.Equal(got, original) {
		t.Fatal("canonical retry rewrote original worktree admission")
	}
	retry = proto.Clone(command).(*pb.Command)
	retry.Ttl = durationpb.New(time.Nanosecond)
	if _, _, err := s.CreateCommand(ctx, retry, commandTestTime); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("coordinator ignored original TTL: %v", err)
	}
	result, claimed, err := j.Claim(ctx, retry)
	requireOK(t, err)
	if claimed || !proto.Equal(result, interruptedNodeResult(command.CommandId)) {
		t.Fatal("node wire TTL changed retry identity")
	}
}

func invalidWorktreeResults() map[string]func(*pb.CommandResult) {
	return map[string]func(*pb.CommandResult){
		"project":   func(r *pb.CommandResult) { r.WorktreeCreate.ProjectId = "other-project" },
		"revision":  func(r *pb.CommandResult) { r.WorktreeCreate.BindingRevision = "other-revision" },
		"name":      func(r *pb.CommandResult) { r.WorktreeCreate.Name = "other-name" },
		"branch":    func(r *pb.CommandResult) { r.WorktreeCreate.Branch = "other-branch" },
		"base":      func(r *pb.CommandResult) { r.WorktreeCreate.BaseCommit = strings.Repeat("b", 64) },
		"workspace": func(r *pb.CommandResult) { r.WorktreeCreate.WorkspaceId = "" },
		"missing":   func(r *pb.CommandResult) { r.WorktreeCreate = nil },
		"detail":    func(r *pb.CommandResult) { r.Detail = "workspace_created" },
		"rejected":  func(r *pb.CommandResult) { r.Status, r.Detail = statusRejected, "journal_full" },
		"timedout":  func(r *pb.CommandResult) { r.Status, r.Detail = statusTimedOut, "deadline_expired" },
		"uncertain": func(r *pb.CommandResult) { r.Status, r.Detail = statusIndeterminate, "herdr_outcome_unknown" },
		"payload":   func(r *pb.CommandResult) { r.Payload = &structpb.Struct{} },
		"mixed": func(r *pb.CommandResult) {
			r.WorkspaceEnsure = workspaceResult(workspaceCommand(1, "project1"), true).WorkspaceEnsure
		},
		"unknown": func(r *pb.CommandResult) { r.WorktreeCreate.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
		"probe":   func(r *pb.CommandResult) { r.Detail, r.WorktreeCreate = "pong", nil },
		"workspace-kind": func(r *pb.CommandResult) {
			r.Detail, r.WorktreeCreate = "workspace_created", nil
			r.WorkspaceEnsure = workspaceResult(workspaceCommand(1, "project1"), true).WorkspaceEnsure
		},
	}
}

func TestWorktreeRejectsInvalidAdmissionsAndResults(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.Payload = &structpb.Struct{} },
		func(c *pb.Command) { c.Preconditions = &structpb.Struct{} },
		func(c *pb.Command) { c.WorktreeCreate = nil },
		func(c *pb.Command) { c.WorktreeCreate.BindingRevision = "" },
		func(c *pb.Command) { c.WorktreeCreate.Name = "../escape" },
		func(c *pb.Command) { c.WorktreeCreate.Branch = "Uppercase" },
		func(c *pb.Command) { c.WorktreeCreate.Name = strings.Repeat("a", 65) },
		func(c *pb.Command) { c.WorktreeCreate.BaseCommit = strings.Repeat("a", 39) },
		func(c *pb.Command) { c.WorktreeCreate.BaseCommit = strings.Repeat("a", 65) },
		func(c *pb.Command) { c.WorkspaceEnsure = workspaceCommand(1, "project1").WorkspaceEnsure },
		func(c *pb.Command) { c.WorktreeCreate.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
	} {
		command := worktreeCommand(1, "project1")
		mutate(command)
		if got, created, err := s.CreateCommand(ctx, command, commandTestTime); err == nil || got != nil || created {
			t.Fatal("invalid worktree admission persisted")
		}
		if got, claimed, err := j.Claim(ctx, command); err == nil || got != nil || claimed {
			t.Fatal("invalid worktree intent claimed")
		}
	}
	if rowCount(t, s, "SELECT count(*) FROM commands") != 0 || rowCount(t, j.store, "SELECT count(*) FROM node_commands") != 0 {
		t.Fatal("invalid request consumed journal capacity")
	}
	command := worktreeCommand(1, "project1")
	running := admitWorkspace(t, s, command, true)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for name, mutate := range invalidWorktreeResults() {
		t.Run(name, func(t *testing.T) {
			result := worktreeResult(command)
			mutate(result)
			if _, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); err == nil {
				t.Fatal("invalid coordinator completion accepted")
			}
			if err := j.Complete(ctx, result); err == nil {
				t.Fatal("invalid node completion accepted")
			}
		})
	}
	stored, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(stored, running) {
		t.Fatal("invalid results changed immutable record")
	}
	for _, other := range []*pb.Command{
		workspaceCommand(2, "project2"), probeCommand(3, "actor", "key3", "node", commandTestTime),
	} {
		admitWorkspace(t, s, other, true)
		_, _, err := j.Claim(ctx, other)
		requireOK(t, err)
		result := worktreeResult(command)
		result.CommandId = other.CommandId
		if _, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); err == nil {
			t.Fatal("worktree result completed another command kind")
		}
		if err := j.Complete(ctx, result); err == nil {
			t.Fatal("worktree result completed another node command kind")
		}
	}
	requireOK(t, j.Complete(ctx, worktreeResult(command)))
	_, err = s.FinishCommand(ctx, "node", "stable", worktreeResult(command), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestWorktreeCrossTypeProjectQuarantine(t *testing.T) {
	factories := []struct {
		name string
		make func(int, string) *pb.Command
	}{{"workspace", workspaceCommand}, {"worktree", worktreeCommand}}
	for _, first := range factories {
		for _, next := range factories {
			for _, outcome := range []string{"running", "node_restarted", "herdr_outcome_unknown"} {
				t.Run(first.name+"/"+next.name+"/"+outcome, func(t *testing.T) {
					ctx := context.Background()
					path := testPath(t)
					j := openTestNodeJournal(t, path)
					original := first.make(1, "project1")
					got, claimed, err := j.Claim(ctx, original)
					requireOK(t, err)
					if got != nil || !claimed {
						t.Fatal("initial project mutation was not claimed")
					}
					if outcome != "running" {
						requireOK(t, j.Complete(ctx, probeResult(original, statusIndeterminate, outcome)))
						requireOK(t, j.Acknowledge(ctx, original.CommandId, statusIndeterminate))
					}
					var rejected *pb.CommandResult
					for phase := 0; phase < 2; phase++ {
						if phase == 1 {
							requireOK(t, j.Close())
							j = openTestNodeJournal(t, path)
						}
						command := next.make(2+phase, "project1")
						command.Actor.ActorId = "new-actor"
						if command.WorkspaceEnsure != nil {
							command.WorkspaceEnsure.BindingRevision = "new-revision"
						} else {
							command.WorktreeCreate.BindingRevision = "new-revision"
							command.WorktreeCreate.Name, command.WorktreeCreate.Branch = "other-name", "other-branch"
							command.WorktreeCreate.BaseCommit = strings.Repeat("b", 64)
						}
						rejected, claimed, err = j.Claim(ctx, command)
						requireOK(t, err)
						if claimed || !proto.Equal(rejected, probeResult(command, statusRejected, "project_unresolved")) {
							t.Fatal("type, actor, key, revision, or restart bypassed stable project quarantine")
						}
						retry, claimed, err := j.Claim(ctx, command)
						requireOK(t, err)
						if claimed || !proto.Equal(retry, rejected) {
							t.Fatal("exact rejected retry did not return original result")
						}
					}
					pending, err := j.PendingResults(ctx)
					requireOK(t, err)
					if len(pending) != 2 {
						t.Fatalf("quarantine did not retain two pending rejections: %d", len(pending))
					}
					requireOK(t, j.Recover(ctx))
					got, claimed, err = j.Claim(ctx, original)
					requireOK(t, err)
					detail := outcome
					if outcome == "running" {
						detail = "node_restarted"
					}
					if claimed || !proto.Equal(got, probeResult(original, statusIndeterminate, detail)) {
						t.Fatal("original retry was reinterpreted as a quarantine rejection")
					}
					for _, command := range []*pb.Command{
						next.make(4, "different-project"), probeCommand(5, "actor", "probe", "node", commandTestTime),
					} {
						got, claimed, err := j.Claim(ctx, command)
						requireOK(t, err)
						if !claimed || got != nil {
							t.Fatal("project quarantine blocked unrelated work")
						}
					}
				})
			}
		}
	}
}

func TestWorktreeAcceptedAuthorizationRejection(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	command := worktreeCommand(1, "project1")
	admitWorkspace(t, s, command, false)
	for _, detail := range []string{"", "journal_full", "project_not_authorized"} {
		if _, err := s.RejectCommand(ctx, "node", command.CommandId, detail, commandTestTime); err == nil {
			t.Fatal("unsupported admission rejection accepted")
		}
	}
	_, err := s.RejectCommand(ctx, "other-node", command.CommandId, "authorization_changed", commandTestTime)
	requireConflict(t, err)
	rejected, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(time.Second))
	requireOK(t, err)
	if rejected.Status != statusRejected || rejected.Detail != "authorization_changed" || rejected.WorktreeCreate != nil ||
		rejected.WorkspaceEnsure != nil || len(rejected.Audit) != 2 || !proto.Equal(rejected.Command, command) {
		t.Fatal("authorization rejection lost immutable request or audit")
	}
	got, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if !proto.Equal(got, rejected) {
		t.Fatal("duplicate rejection changed terminal record")
	}
	got, dispatched, err := s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if dispatched || !proto.Equal(got, rejected) {
		t.Fatal("revoked admission dispatched")
	}
	running := admitWorkspace(t, s, worktreeCommand(2, "project2"), true)
	got, err = s.RejectCommand(ctx, "node", running.Command.CommandId, "authorization_changed", commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if !proto.Equal(got, running) {
		t.Fatal("authorization rejection rewrote dispatched worktree")
	}
}

func TestWorktreeTypedCompletionRollbackAndRecovery(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := worktreeCommand(1, "project1")
	running := admitWorkspace(t, s, command, true)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for _, fixture := range []struct {
		store *Store
		table string
	}{{s, "commands"}, {j.store, "node_commands"}} {
		_, err := fixture.store.conn.ExecContext(ctx, "CREATE TRIGGER fail_worktree_complete BEFORE UPDATE ON "+fixture.table+
			" BEGIN SELECT RAISE(ABORT, 'injected typed completion failure'); END")
		requireOK(t, err)
	}
	result := worktreeResult(command)
	if got, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); err == nil || got != nil {
		t.Fatal("failed worktree completion returned success")
	}
	if err := j.Complete(ctx, result); err == nil {
		t.Fatal("failed node worktree completion returned success")
	}
	stored, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(stored, running) || rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL AND delivered = 0") != 1 {
		t.Fatal("failed completion persisted a partial typed result")
	}
	for _, store := range []*Store{s, j.store} {
		_, err := store.conn.ExecContext(ctx, "DROP TRIGGER fail_worktree_complete")
		requireOK(t, err)
	}
	requireOK(t, s.RecoverCommands(ctx, commandTestTime.Add(2*time.Second)))
	requireOK(t, j.Complete(ctx, result))
	record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(3*time.Second))
	requireOK(t, err)
	if !proto.Equal(recordResult(record), result) || len(record.Audit) != 4 ||
		record.Audit[2].Status != statusIndeterminate || record.Audit[2].Detail != "server_restarted" {
		t.Fatal("late typed result failed to reconcile coordinator restart uncertainty")
	}
}

func TestWorktreeStoredResultCorruptionRefused(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for fault, mutate := range invalidWorktreeResults() {
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				command := worktreeCommand(1, "project1")
				result := worktreeResult(command)
				var s *Store
				var record *pb.CommandRecord
				owner := "server"
				if kind == coordinatorKind {
					s = commandStoreAtPath(t, path)
					admitWorkspace(t, s, command, true)
					var err error
					record, err = s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
					requireOK(t, err)
				} else {
					j := openTestNodeJournal(t, path)
					s, owner = j.store, "node"
					_, _, err := j.Claim(ctx, command)
					requireOK(t, err)
					requireOK(t, j.Complete(ctx, result))
				}
				mutate(result)
				var message proto.Message = result
				statement := "UPDATE node_commands SET status = ?, result = ?"
				if record != nil {
					if fault == "payload" {
						// Records have no untyped result field; reject an untyped command instead.
						record.Command.Payload = result.Payload
					}
					record.Status, record.Detail = result.Status, result.Detail
					record.Audit[2].Status, record.Audit[2].Detail = result.Status, result.Detail
					record.WorktreeCreate, record.WorkspaceEnsure = result.WorktreeCreate, result.WorkspaceEnsure
					message, statement = record, "UPDATE commands SET status = ?, record = ?"
				}
				payload, err := proto.Marshal(message)
				requireOK(t, err)
				_, err = s.conn.ExecContext(ctx, statement, result.Status, payload)
				requireOK(t, err)
				requireOK(t, s.Close())
				before, err := os.ReadFile(path)
				requireOK(t, err)
				opened, err := openStore(ctx, path, owner, kind)
				if opened != nil {
					requireOK(t, opened.Close())
				}
				if err == nil {
					t.Fatal("corrupt typed journal reopened")
				}
				after, err := os.ReadFile(path)
				requireOK(t, err)
				if !bytes.Equal(before, after) {
					t.Fatal("corrupt journal was rewritten")
				}
			})
		}
	}
}
