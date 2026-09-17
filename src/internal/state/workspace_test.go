package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func workspaceCommand(id int, project string) *pb.Command {
	command := probeCommand(id, "actor", fmt.Sprintf("key%d", id), "node", commandTestTime)
	command.CommandType = protocol.WorkspaceEnsureCommandType
	command.WorkspaceEnsure = &pb.WorkspaceEnsure{ProjectId: project, BindingRevision: "revision1"}
	return command
}

func workspaceResult(command *pb.Command, created bool) *pb.CommandResult {
	detail := "workspace_present"
	if created {
		detail = "workspace_created"
	}
	return &pb.CommandResult{
		CommandId: command.CommandId, Status: statusSucceeded, Detail: detail,
		WorkspaceEnsure: &pb.WorkspaceEnsureResult{
			ProjectId: command.WorkspaceEnsure.ProjectId, BindingRevision: command.WorkspaceEnsure.BindingRevision,
			WorkspaceId: "workspace1", Created: created,
		},
	}
}

func admitWorkspace(t *testing.T, s *Store, command *pb.Command, dispatch bool) *pb.CommandRecord {
	t.Helper()
	record, created, err := s.CreateCommand(context.Background(), command, commandTestTime)
	requireOK(t, err)
	if !created {
		t.Fatal("workspace admission was not new")
	}
	if dispatch {
		var dispatched bool
		record, dispatched, err = s.DispatchCommand(context.Background(), "node", command.CommandId, commandTestTime.Add(time.Second))
		requireOK(t, err)
		if !dispatched {
			t.Fatal("workspace admission was not dispatched")
		}
	}
	return record
}

func TestWorkspaceOutcomesDurableAndImmutable(t *testing.T) {
	outcomes := []struct {
		status pb.CommandStatus
		detail string
	}{
		{statusSucceeded, "workspace_created"}, {statusSucceeded, "workspace_present"},
		{statusTimedOut, "deadline_expired"},
		{statusRejected, "journal_full"}, {statusRejected, "project_unresolved"},
		{statusRejected, "project_not_authorized"}, {statusRejected, "precondition_failed"},
		{statusRejected, "ambiguous_workspace"}, {statusRejected, "herdr_unavailable"},
		{statusRejected, "authorization_changed"},
		{statusIndeterminate, "node_restarted"}, {statusIndeterminate, "herdr_outcome_unknown"},
	}
	for _, outcome := range outcomes {
		t.Run(outcome.detail, func(t *testing.T) {
			ctx := context.Background()
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s := openTestStore(t, coordinatorPath)
			requireOK(t, s.Bind(ctx, "stable", "node"))
			j := openTestNodeJournal(t, nodePath)
			command := workspaceCommand(1, "project1")
			admitWorkspace(t, s, command, true)
			got, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if got != nil || !claimed {
				t.Fatal("new workspace intent not claimed")
			}
			result := probeResult(command, outcome.status, outcome.detail)
			if outcome.status == statusSucceeded {
				result = workspaceResult(command, outcome.detail == "workspace_created")
			}
			requireOK(t, j.Complete(ctx, result))
			record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) || len(record.Audit) != 3 || !proto.Equal(record.Command, command) {
				t.Fatal("typed outcome, immutable command, or audit lost")
			}
			for i, status := range []pb.CommandStatus{statusAccepted, statusRunning, outcome.status} {
				if record.Audit[i].Status != status || !record.Audit[i].OccurredAt.AsTime().Equal(commandTestTime.Add(time.Duration(i)*time.Second)) {
					t.Fatal("workspace audit order changed")
				}
			}
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s = openTestStore(t, coordinatorPath)
			j = openTestNodeJournal(t, nodePath)
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], result) {
				t.Fatal("typed result did not survive restart")
			}
			requireOK(t, j.Acknowledge(ctx, result.CommandId, result.Status))
			requireOK(t, j.Complete(ctx, result))
			pending, err = j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("duplicate completion reset acknowledgement")
			}
			duplicate, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(3*time.Second))
			requireOK(t, err)
			if !proto.Equal(duplicate, record) {
				t.Fatal("duplicate result rewrote immutable audit")
			}
			for _, detail := range []string{"node_restarted", "herdr_outcome_unknown"} {
				replay := probeResult(command, statusIndeterminate, detail)
				_, err := s.FinishCommand(ctx, "node", "wrong-stable", replay, commandTestTime.Add(4*time.Second))
				requireConflict(t, err)
				got, err := s.FinishCommand(ctx, "node", "stable", replay, commandTestTime.Add(4*time.Second))
				requireOK(t, err)
				if !proto.Equal(got, record) {
					t.Fatal("uncertainty replay overwrote known typed outcome or audit")
				}
			}
			if outcome.status == statusSucceeded {
				changed := proto.Clone(result).(*pb.CommandResult)
				changed.WorkspaceEnsure.WorkspaceId = "other-workspace"
				if _, err := s.FinishCommand(ctx, "node", "stable", changed, commandTestTime.Add(5*time.Second)); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed typed terminal result did not conflict: %v", err)
				}
				if err := j.Complete(ctx, changed); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed node terminal result did not conflict: %v", err)
				}
			}
			requireOK(t, s.Close())
			s = openTestStore(t, coordinatorPath)
			stored, err := s.GetCommand(ctx, "actor", command.CommandId)
			requireOK(t, err)
			if !proto.Equal(stored, record) {
				t.Fatal("replays changed persistent outcome")
			}
		})
	}
}

func TestWorkspaceResultBindingAndTypeValidation(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := workspaceCommand(1, "project1")
	running := admitWorkspace(t, s, command, true)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for _, change := range []func(*pb.CommandResult){
		func(r *pb.CommandResult) { r.WorkspaceEnsure.ProjectId = "other-project" },
		func(r *pb.CommandResult) { r.WorkspaceEnsure.BindingRevision = "other-revision" },
		func(r *pb.CommandResult) { r.WorkspaceEnsure.WorkspaceId = "" },
		func(r *pb.CommandResult) { r.WorkspaceEnsure.Created = false },
		func(r *pb.CommandResult) { r.WorkspaceEnsure = nil },
		func(r *pb.CommandResult) { r.Detail = "pong"; r.WorkspaceEnsure = nil },
		func(r *pb.CommandResult) { r.Status = statusRejected; r.Detail = "journal_full" },
		func(r *pb.CommandResult) { r.Payload = &structpb.Struct{} },
		func(r *pb.CommandResult) { r.WorkspaceEnsure.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
	} {
		result := workspaceResult(command, true)
		change(result)
		if _, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); err == nil {
			t.Fatal("invalid workspace completion accepted")
		}
		if err := j.Complete(ctx, result); err == nil {
			t.Fatal("invalid node workspace completion accepted")
		}
	}
	stored, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(stored, running) {
		t.Fatal("invalid result modified admission")
	}
	probe := createTestCommand(t, s, 2).Command
	_, _, err = s.DispatchCommand(ctx, "node", probe.CommandId, commandTestTime)
	requireOK(t, err)
	_, _, err = j.Claim(ctx, probe)
	requireOK(t, err)
	foreign := workspaceResult(command, true)
	foreign.CommandId = probe.CommandId
	if _, err := s.FinishCommand(ctx, "node", "stable", foreign, commandTestTime.Add(2*time.Second)); err == nil {
		t.Fatal("workspace result finished probe")
	}
	if err := j.Complete(ctx, foreign); err == nil {
		t.Fatal("workspace result completed node probe")
	}
	requireOK(t, j.Complete(ctx, workspaceResult(command, true)))
	_, err = s.FinishCommand(ctx, "node", "stable", workspaceResult(command, true), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestWorkspaceImmutableRetryIdentity(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := workspaceCommand(1, "project1")
	original := admitWorkspace(t, s, command, false)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.WorkspaceEnsure.ProjectId = "other-project" },
		func(c *pb.Command) { c.WorkspaceEnsure.BindingRevision = "revision2" },
		func(c *pb.Command) { c.CommandType = protocol.ProbeCommandType; c.WorkspaceEnsure = nil },
	} {
		changed := proto.Clone(command).(*pb.Command)
		mutate(changed)
		if _, _, err := s.CreateCommand(ctx, changed, commandTestTime); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed workspace retry accepted: %v", err)
		}
		if _, _, err := j.Claim(ctx, changed); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed workspace intent accepted: %v", err)
		}
	}
	retry := proto.Clone(command).(*pb.Command)
	retry.CommandId = fmt.Sprintf("%032x", 2)
	retry.ExpiresAt = timestamppb.New(commandTestTime.Add(time.Hour))
	record, created, err := s.CreateCommand(ctx, retry, commandTestTime.Add(time.Hour))
	requireOK(t, err)
	if created || !proto.Equal(record, original) {
		t.Fatal("coordinator retry rewrote original workspace binding")
	}
	retry = proto.Clone(command).(*pb.Command)
	retry.Ttl = durationpb.New(time.Nanosecond)
	if _, _, err := s.CreateCommand(ctx, retry, commandTestTime); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("coordinator ignored original TTL: %v", err)
	}
	result, claimed, err := j.Claim(ctx, retry)
	requireOK(t, err)
	if claimed || !proto.Equal(result, interruptedNodeResult(command.CommandId)) {
		t.Fatal("remaining wire TTL changed node retry identity")
	}
}

func TestWorkspaceTypedCompletionRollback(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := workspaceCommand(1, "project1")
	running := admitWorkspace(t, s, command, true)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for _, fixture := range []struct {
		store *Store
		table string
	}{{s, "commands"}, {j.store, "node_commands"}} {
		_, err := fixture.store.conn.ExecContext(ctx, "CREATE TRIGGER fail_workspace_complete BEFORE UPDATE ON "+fixture.table+
			" BEGIN SELECT RAISE(ABORT, 'injected typed completion failure'); END")
		requireOK(t, err)
	}
	result := workspaceResult(command, true)
	if got, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); err == nil || got != nil {
		t.Fatal("failed typed completion returned success")
	}
	if err := j.Complete(ctx, result); err == nil {
		t.Fatal("failed node typed completion returned success")
	}
	stored, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(stored, running) {
		t.Fatal("failed typed completion changed status, result, or audit")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL AND delivered = 0"); count != 1 {
		t.Fatal("failed typed completion changed node intent")
	}
	for _, store := range []*Store{s, j.store} {
		_, err := store.conn.ExecContext(ctx, "DROP TRIGGER fail_workspace_complete")
		requireOK(t, err)
	}
	requireOK(t, j.Complete(ctx, result))
	_, err = s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestWorkspaceRejectsUntypedAdmissions(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	for _, mutate := range []func(*pb.Command){
		func(c *pb.Command) { c.Payload = &structpb.Struct{} },
		func(c *pb.Command) { c.Preconditions = &structpb.Struct{} },
		func(c *pb.Command) { c.WorkspaceEnsure = nil },
		func(c *pb.Command) { c.WorkspaceEnsure.BindingRevision = "" },
		func(c *pb.Command) { c.WorkspaceEnsure.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
	} {
		command := workspaceCommand(1, "project1")
		mutate(command)
		if record, created, err := s.CreateCommand(ctx, command, commandTestTime); err == nil || created || record != nil {
			t.Fatal("invalid workspace admission persisted")
		}
		if result, claimed, err := j.Claim(ctx, command); err == nil || claimed || result != nil {
			t.Fatal("invalid workspace intent granted execution")
		}
	}
	if rowCount(t, s, "SELECT count(*) FROM commands") != 0 || rowCount(t, j.store, "SELECT count(*) FROM node_commands") != 0 {
		t.Fatal("invalid workspace request consumed journal capacity")
	}
}

func TestWorkspaceAcceptedAuthorizationRejection(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	command := workspaceCommand(1, "project1")
	admitted := admitWorkspace(t, s, command, false)
	for _, detail := range []string{"", "journal_full", "project_not_authorized"} {
		if _, err := s.RejectCommand(ctx, "node", command.CommandId, detail, commandTestTime); err == nil {
			t.Fatal("unsupported admission rejection accepted")
		}
	}
	_, err := s.RejectCommand(ctx, "other-node", command.CommandId, "authorization_changed", commandTestTime)
	requireConflict(t, err)
	_, err = s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(-time.Second))
	if err == nil {
		t.Fatal("out-of-order authorization audit accepted")
	}
	_, err = s.conn.ExecContext(ctx, `CREATE TRIGGER fail_workspace_reject BEFORE UPDATE ON commands
		BEGIN SELECT RAISE(ABORT, 'injected rejection failure'); END`)
	requireOK(t, err)
	if _, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime); err == nil {
		t.Fatal("failed rejection returned success")
	}
	got, err := s.GetCommand(ctx, "actor", command.CommandId)
	requireOK(t, err)
	if !proto.Equal(got, admitted) {
		t.Fatal("failed rejection persisted partial audit")
	}
	_, err = s.conn.ExecContext(ctx, "DROP TRIGGER fail_workspace_reject")
	requireOK(t, err)
	rejected, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(time.Second))
	requireOK(t, err)
	if rejected.Status != statusRejected || rejected.Detail != "authorization_changed" ||
		rejected.WorkspaceEnsure != nil || len(rejected.Audit) != 2 || !proto.Equal(rejected.Command, command) {
		t.Fatal("authorization rejection lost immutable admission or audit")
	}
	duplicate, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if !proto.Equal(duplicate, rejected) {
		t.Fatal("repeated rejection rewrote terminal record")
	}
	got, dispatched, err := s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if dispatched || !proto.Equal(got, rejected) {
		t.Fatal("revoked admission dispatched")
	}
	running := admitWorkspace(t, s, workspaceCommand(2, "project2"), true)
	got, err = s.RejectCommand(ctx, "node", running.Command.CommandId, "authorization_changed", commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if !proto.Equal(got, running) {
		t.Fatal("authorization rejection rewrote dispatched command")
	}
	probe := createTestCommand(t, s, 3)
	if _, err := s.RejectCommand(ctx, "node", probe.Command.CommandId, "authorization_changed", commandTestTime); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("workspace admission rejection applied to probe: %v", err)
	}
}

func TestWorkspaceCoordinatorLifecycleRecovery(t *testing.T) {
	for _, mode := range []string{"restart", "disconnect", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := commandStore(t)
			accepted := admitWorkspace(t, s, workspaceCommand(1, "project1"), false)
			running := admitWorkspace(t, s, workspaceCommand(2, "project2"), true)
			now := commandTestTime.Add(10 * time.Second)
			reason, acceptedStatus := "server_restarted", statusUnavailable
			switch mode {
			case "restart":
				requireOK(t, s.RecoverCommands(ctx, now))
			case "disconnect":
				reason = "node_disconnected"
				requireOK(t, s.LoseNodeCommands(ctx, "node", now))
			case "expiry":
				reason, acceptedStatus = "deadline_expired", statusTimedOut
				requireOK(t, s.ExpireCommands(ctx, now))
			}
			got, err := s.GetCommand(ctx, "actor", accepted.Command.CommandId)
			requireOK(t, err)
			if got.Status != acceptedStatus || got.Detail != reason {
				t.Fatal("undispatched workspace recovery changed")
			}
			got, err = s.GetCommand(ctx, "actor", running.Command.CommandId)
			requireOK(t, err)
			if got.Status != statusIndeterminate || got.Detail != reason || len(got.Audit) != 3 {
				t.Fatal("running workspace recovery lost uncertainty audit")
			}
			result := workspaceResult(running.Command, true)
			got, err = s.FinishCommand(ctx, "node", "stable", result, now.Add(time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(got), result) || len(got.Audit) != 4 {
				t.Fatal("late typed result failed to reconcile coordinator uncertainty")
			}
		})
	}
}

func TestProjectMutationQuarantineConcurrentAndRestart(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	j := openTestNodeJournal(t, path)
	type claim struct {
		command *pb.Command
		result  *pb.CommandResult
		claimed bool
		err     error
	}
	const contenders = 12
	results := make(chan claim, contenders)
	start := make(chan struct{})
	for i := 1; i <= contenders; i++ {
		command := workspaceCommand(i, "project1")
		if i%2 == 0 {
			command = worktreeCommand(i, "project1")
			command.WorktreeCreate.BindingRevision = fmt.Sprintf("revision%d", i)
		} else {
			command.WorkspaceEnsure.BindingRevision = fmt.Sprintf("revision%d", i)
		}
		command.Actor.ActorId = fmt.Sprintf("actor%d", i)
		go func() {
			<-start
			result, claimed, err := j.Claim(ctx, command)
			results <- claim{command, result, claimed, err}
		}()
	}
	close(start)
	var winner *pb.Command
	var rejected claim
	for i := 0; i < contenders; i++ {
		got := <-results
		requireOK(t, got.err)
		if got.claimed {
			if winner != nil || got.result != nil {
				t.Fatal("simultaneous claims executed more than one project operation")
			}
			winner = got.command
		} else {
			if !proto.Equal(got.result, probeResult(got.command, statusRejected, "project_unresolved")) {
				t.Fatal("quarantined command was not durably rejected")
			}
			rejected = got
		}
	}
	if winner == nil {
		t.Fatal("no project operation was claimed")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands"); count != contenders {
		t.Fatal("quarantine did not retain every new command")
	}
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	requireOK(t, j.Recover(ctx))
	requireOK(t, j.Acknowledge(ctx, winner.CommandId, statusIndeterminate))
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	next := workspaceCommand(100, "project1")
	next.Actor.ActorId = "new-actor"
	next.WorkspaceEnsure.BindingRevision = "new-revision"
	result, claimed, err := j.Claim(ctx, next)
	requireOK(t, err)
	if claimed || !proto.Equal(result, probeResult(next, statusRejected, "project_unresolved")) {
		t.Fatal("restart, acknowledgement, actor, key, or revision bypassed quarantine")
	}
	for _, known := range []claim{
		{command: winner, result: interruptedNodeResult(winner.CommandId)},
		rejected,
	} {
		result, claimed, err := j.Claim(ctx, known.command)
		requireOK(t, err)
		if claimed || !proto.Equal(result, known.result) {
			t.Fatal("known command did not return original outcome before quarantine")
		}
	}
	for _, command := range []*pb.Command{
		workspaceCommand(101, "different-project"),
		probeCommand(102, "actor", "probe", "node", commandTestTime),
	} {
		result, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if !claimed || result != nil {
			t.Fatal("project quarantine blocked unrelated work")
		}
	}
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != contenders {
		t.Fatalf("quarantine results missing or acknowledged uncertainty replayed: %d", len(pending))
	}
}

func TestWorkspaceKnownOutcomesDoNotQuarantine(t *testing.T) {
	for _, detail := range []string{"workspace_created", "workspace_present", "journal_full", "deadline_expired", "herdr_outcome_unknown"} {
		t.Run(detail, func(t *testing.T) {
			ctx := context.Background()
			j := openTestNodeJournal(t, testPath(t))
			command := workspaceCommand(1, "project1")
			_, _, err := j.Claim(ctx, command)
			requireOK(t, err)
			result := workspaceResult(command, detail == "workspace_created")
			switch detail {
			case "journal_full":
				result = probeResult(command, statusRejected, detail)
			case "deadline_expired":
				result = probeResult(command, statusTimedOut, detail)
			case "herdr_outcome_unknown":
				result = probeResult(command, statusIndeterminate, detail)
			}
			requireOK(t, j.Complete(ctx, result))
			next := workspaceCommand(2, "project1")
			got, claimed, err := j.Claim(ctx, next)
			requireOK(t, err)
			if detail == "herdr_outcome_unknown" {
				if claimed || !proto.Equal(got, probeResult(next, statusRejected, "project_unresolved")) {
					t.Fatal("Herdr uncertainty did not quarantine project")
				}
			} else if !claimed || got != nil {
				t.Fatal("known prior outcome incorrectly quarantined project")
			}
		})
	}
}

func TestWorkspaceQuarantineCapacityAndRollback(t *testing.T) {
	ctx := context.Background()
	j := openTestNodeJournal(t, testPath(t))
	command := workspaceCommand(1, "project1")
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, `CREATE TRIGGER fail_quarantine BEFORE INSERT ON node_commands
		BEGIN SELECT RAISE(ABORT, 'injected quarantine failure'); END`)
	requireOK(t, err)
	next := workspaceCommand(2, "project1")
	if result, claimed, err := j.Claim(ctx, next); err == nil || claimed || result != nil {
		t.Fatal("failed quarantine insert returned a successful rejection")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands"); count != 1 {
		t.Fatal("failed quarantine left partial row")
	}
	_, err = j.store.conn.ExecContext(ctx, "DROP TRIGGER fail_quarantine")
	requireOK(t, err)
	requireOK(t, j.store.transaction(ctx, func(tx *sql.Tx) error {
		for i := 2; i < maxCommands; i++ {
			command := probeCommand(i, "actor", fmt.Sprintf("key%d", i), "node", commandTestTime)
			payload, err := marshalCommandMessage(command)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO node_commands(command_id, status, delivered, command) VALUES (?, ?, 0, ?)",
				command.CommandId, statusRunning, payload); err != nil {
				return err
			}
		}
		return nil
	}))
	next = workspaceCommand(maxCommands, "project1")
	result, claimed, err := j.Claim(ctx, next)
	requireOK(t, err)
	if claimed || result.GetDetail() != "project_unresolved" {
		t.Fatal("last capacity slot did not retain quarantine rejection")
	}
	requireOK(t, j.Acknowledge(ctx, next.CommandId, statusRejected))
	if _, claimed, err := j.Claim(ctx, workspaceCommand(maxCommands+1, "project1")); claimed || !errors.Is(err, ErrCommandCapacity) {
		t.Fatalf("quarantine bypassed 4096 row bound: %v", err)
	}
	got, claimed, err := j.Claim(ctx, next)
	requireOK(t, err)
	if claimed || !proto.Equal(got, result) || rowCount(t, j.store, "SELECT count(*) FROM node_commands") != maxCommands {
		t.Fatal("full journal lost original rejection or removed tombstones")
	}
}

func TestWorkspaceSchemaMigrationPreservesProbeBytes(t *testing.T) {
	ctx := context.Background()
	coordinatorPath, nodePath := testPath(t), testPath(t)
	s := openTestStore(t, coordinatorPath)
	requireOK(t, s.Bind(ctx, "stable", "node"))
	requireOK(t, s.SaveNode(ctx, node("stable", "node")))
	record := createTestCommand(t, s, 1)
	_, _, err := s.DispatchCommand(ctx, "node", record.Command.CommandId, commandTestTime)
	requireOK(t, err)
	_, err = s.FinishCommand(ctx, "node", "stable", probeResult(record.Command, statusSucceeded, "pong"), commandTestTime)
	requireOK(t, err)
	j := openTestNodeJournal(t, nodePath)
	command := claimTestCommand(t, j, 1)
	requireOK(t, j.Complete(ctx, probeResult(command, statusSucceeded, "pong")))
	requireOK(t, j.Acknowledge(ctx, command.CommandId, statusSucceeded))
	var coordinatorBytes, fleetBytes, commandBytes, resultBytes []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands").Scan(&coordinatorBytes))
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&fleetBytes))
	requireOK(t, j.store.conn.QueryRowContext(ctx, "SELECT command, result FROM node_commands").Scan(&commandBytes, &resultBytes))
	removeLifecycleSchema(t, s, coordinatorKind)
	removeLifecycleSchema(t, j.store, nodeKind)
	_, err = s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 2")
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 1")
	requireOK(t, err)
	requireOK(t, s.Close())
	requireOK(t, j.Close())
	s = openTestStore(t, coordinatorPath)
	j = openTestNodeJournal(t, nodePath)
	if rowCount(t, s, "PRAGMA user_version") != 7 || rowCount(t, j.store, "PRAGMA user_version") != 6 {
		t.Fatal("workspace migration did not fence old binaries")
	}
	var gotCoordinator, gotFleet, gotCommand, gotResult []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands").Scan(&gotCoordinator))
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&gotFleet))
	requireOK(t, j.store.conn.QueryRowContext(ctx, "SELECT command, result FROM node_commands").Scan(&gotCommand, &gotResult))
	if !bytes.Equal(gotCoordinator, coordinatorBytes) || !bytes.Equal(gotFleet, fleetBytes) ||
		!bytes.Equal(gotCommand, commandBytes) || !bytes.Equal(gotResult, resultBytes) {
		t.Fatal("migration rewrote retained probe or fleet bytes")
	}
	if rowCount(t, j.store, "SELECT delivered FROM node_commands") != 1 {
		t.Fatal("migration forgot acknowledgement")
	}
	admitWorkspace(t, s, workspaceCommand(2, "project1"), true)
	if _, claimed, err := j.Claim(ctx, workspaceCommand(2, "project1")); err != nil || !claimed {
		t.Fatalf("migrated journal rejected workspace command: %v", err)
	}
}

func TestWorkspaceMigrationFailsClosedWithoutRewriting(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for _, fault := range []string{"wire", "missing", "owner", "kind", "workspace", "fleet", "binding"} {
			if kind == nodeKind && (fault == "fleet" || fault == "binding") {
				continue
			}
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				var s *Store
				owner := "server"
				if kind == nodeKind {
					j := openTestNodeJournal(t, path)
					s, owner = j.store, "node"
					if fault == "workspace" {
						_, _, err := j.Claim(ctx, workspaceCommand(1, "project1"))
						requireOK(t, err)
					} else {
						claimTestCommand(t, j, 1)
					}
					removeLifecycleSchema(t, s, nodeKind)
					_, err := s.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 1")
					requireOK(t, err)
				} else {
					s = commandStoreAtPath(t, path)
					if fault == "workspace" {
						admitWorkspace(t, s, workspaceCommand(1, "project1"), false)
					} else {
						createTestCommand(t, s, 1)
					}
					requireOK(t, s.SaveNode(ctx, node("stable", "node")))
					removeLifecycleSchema(t, s, coordinatorKind)
					_, err := s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 2")
					requireOK(t, err)
				}
				statement := ""
				switch fault {
				case "wire":
					statement = "UPDATE commands SET record = x'ff'"
					if kind == nodeKind {
						statement = "UPDATE node_commands SET command = x'ff'"
					}
				case "missing":
					statement = "DROP TABLE commands"
					if kind == nodeKind {
						statement = "DROP TABLE node_commands"
					}
				case "owner":
					owner = "wrong-owner"
				case "kind":
					statement = "PRAGMA application_id = 123"
				case "fleet":
					statement = "UPDATE latest_nodes SET payload = x'ff'"
				case "binding":
					statement = "INSERT INTO bindings(stable_id, instance_id) VALUES (' ', 'other')"
				}
				if statement != "" {
					_, err := s.conn.ExecContext(ctx, statement)
					requireOK(t, err)
				}
				requireOK(t, s.Close())
				before, err := os.ReadFile(path)
				requireOK(t, err)
				for i := 0; i < 2; i++ {
					opened, err := openStore(ctx, path, owner, kind)
					if opened != nil {
						requireOK(t, opened.Close())
					}
					if err == nil || errors.Is(err, ErrLocked) {
						t.Fatalf("invalid old journal migrated or leaked lock: %v", err)
					}
					after, err := os.ReadFile(path)
					requireOK(t, err)
					if !bytes.Equal(before, after) {
						t.Fatal("failed migration changed schema version or stored bytes")
					}
				}
			})
		}
	}
}

func commandStoreAtPath(t *testing.T, path string) *Store {
	t.Helper()
	s := openTestStore(t, path)
	requireOK(t, s.Bind(context.Background(), "stable", "node"))
	return s
}

func TestWorkspaceStoredResultCorruptionRefused(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for _, fault := range []string{"project", "revision", "created", "missing", "audit"} {
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				command := workspaceCommand(1, "project1")
				result := workspaceResult(command, true)
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
				switch fault {
				case "project":
					result.WorkspaceEnsure.ProjectId = "foreign"
				case "revision":
					result.WorkspaceEnsure.BindingRevision = "foreign"
				case "created":
					result.WorkspaceEnsure.Created = false
				case "missing":
					result.WorkspaceEnsure = nil
				case "audit":
					result.Detail = "pong"
				}
				var message proto.Message = result
				statement := "UPDATE node_commands SET result = ?"
				if record != nil {
					record.WorkspaceEnsure = result.WorkspaceEnsure
					if fault == "audit" {
						record.Audit[1].Detail = "pong"
					}
					message = record
					statement = "UPDATE commands SET record = ?"
				}
				payload, err := proto.Marshal(message)
				requireOK(t, err)
				_, err = s.conn.ExecContext(ctx, statement, payload)
				requireOK(t, err)
				requireOK(t, s.Close())
				before, err := os.ReadFile(path)
				requireOK(t, err)
				opened, err := openStore(ctx, path, owner, kind)
				if opened != nil {
					requireOK(t, opened.Close())
				}
				if err == nil {
					t.Fatal("corrupt typed result reopened")
				}
				after, err := os.ReadFile(path)
				requireOK(t, err)
				if !bytes.Equal(before, after) {
					t.Fatal("corrupt typed result rewritten")
				}
			})
		}
	}
}
