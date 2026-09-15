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
	"google.golang.org/protobuf/types/known/timestamppb"
)

var commandTestTime = time.Unix(1800000000, 123456789)

func probeCommand(id int, actor, key, target string, now time.Time) *pb.Command {
	return &pb.Command{
		CommandId: fmt.Sprintf("%032x", id), IdempotencyKey: key, TargetId: target,
		CommandType: protocol.ProbeCommandType,
		Actor:       &pb.Actor{ActorId: actor, Role: pb.Role_ROLE_CONTROLLER},
		Ttl:         durationpb.New(protocol.DefaultCommandTTL),
		ExpiresAt:   timestamppb.New(now.Add(protocol.DefaultCommandTTL)),
	}
}

func probeResult(command *pb.Command, status pb.CommandStatus, detail string) *pb.CommandResult {
	return &pb.CommandResult{CommandId: command.CommandId, Status: status, Detail: detail}
}

func commandStore(t *testing.T) *Store {
	t.Helper()
	s := openTestStore(t, testPath(t))
	requireOK(t, s.Bind(context.Background(), "stable", "node"))
	return s
}

func createTestCommand(t *testing.T, s *Store, id int) *pb.CommandRecord {
	t.Helper()
	record, created, err := s.CreateCommand(context.Background(), probeCommand(id, "actor", fmt.Sprintf("key%d", id), "node", commandTestTime), commandTestTime)
	requireOK(t, err)
	if !created {
		t.Fatal("new command was not admitted")
	}
	return record
}

func downgradeToVersionOne(t *testing.T, s *Store) {
	t.Helper()
	for _, statement := range []string{"DROP TABLE commands", "PRAGMA application_id = 0", "PRAGMA user_version = 1"} {
		_, err := s.conn.ExecContext(context.Background(), statement)
		requireOK(t, err)
	}
}

func TestCommandActorScopedIdempotency(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	original := createTestCommand(t, s, 1)
	retry := proto.Clone(original.Command).(*pb.Command)
	retry.CommandId = fmt.Sprintf("%032x", 2)
	retry.ExpiresAt = timestamppb.New(commandTestTime.Add(time.Hour))
	got, created, err := s.CreateCommand(ctx, retry, commandTestTime.Add(time.Hour))
	requireOK(t, err)
	if created || !proto.Equal(got, original) {
		t.Fatal("matching retry changed immutable admission")
	}
	for _, change := range []func(*pb.Command){
		func(command *pb.Command) { command.TargetId = "different" },
		func(command *pb.Command) { command.Ttl = durationpb.New(time.Second) },
	} {
		conflict := proto.Clone(retry).(*pb.Command)
		change(conflict)
		_, _, err := s.CreateCommand(ctx, conflict, commandTestTime)
		if !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("request conflict returned %v", err)
		}
	}
	other := probeCommand(3, "other-actor", "key1", "node", commandTestTime)
	_, created, err = s.CreateCommand(ctx, other, commandTestTime)
	requireOK(t, err)
	if !created {
		t.Fatal("idempotency key was not actor-scoped")
	}
	got, err = s.FindCommand(ctx, "actor", "key1")
	requireOK(t, err)
	if got.Command.CommandId != original.Command.CommandId {
		t.Fatal("actor-scoped lookup returned wrong command")
	}
	if _, err := s.GetCommand(ctx, "other-actor", original.Command.CommandId); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("other actor could read command: %v", err)
	}
	other.CommandId = original.Command.CommandId
	other.IdempotencyKey = "different-key"
	if _, _, err := s.CreateCommand(ctx, other, commandTestTime); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("command ID collision returned %v", err)
	}
	unbound := probeCommand(4, "actor", "unbound", "not-bound", commandTestTime)
	_, _, err = s.CreateCommand(ctx, unbound, commandTestTime)
	requireConflict(t, err)
}

func TestCommandDispatchOwnershipAndOrderedAudit(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	original := createTestCommand(t, s, 1)
	command := original.Command
	result := probeResult(command, statusSucceeded, "pong")
	if _, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("undispatched command accepted a result: %v", err)
	}
	_, _, err := s.DispatchCommand(ctx, "wrong-node", command.CommandId, commandTestTime)
	requireConflict(t, err)
	record, dispatched, err := s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime.Add(time.Second))
	requireOK(t, err)
	if !dispatched || record.Status != statusRunning || !proto.Equal(record.Command, command) {
		t.Fatal("dispatch must persist RUNNING without rewriting original TTL or command")
	}
	_, dispatched, err = s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if dispatched {
		t.Fatal("command dispatched twice")
	}
	_, err = s.FinishCommand(ctx, "node", "foreign-stable", result, commandTestTime.Add(2*time.Second))
	requireConflict(t, err)
	requireOK(t, s.Bind(ctx, "other-stable", "other-node"))
	_, err = s.FinishCommand(ctx, "other-node", "other-stable", result, commandTestTime.Add(2*time.Second))
	requireConflict(t, err)
	record, err = s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if record.Status != statusSucceeded || len(record.Audit) != 3 {
		t.Fatal("completion or audit missing")
	}
	for i, status := range []pb.CommandStatus{statusAccepted, statusRunning, statusSucceeded} {
		if record.Audit[i].Status != status || !record.Audit[i].OccurredAt.AsTime().Equal(commandTestTime.Add(time.Duration(i)*time.Second)) {
			t.Fatal("audit order or time changed")
		}
	}
	duplicate, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(3*time.Second))
	requireOK(t, err)
	if !proto.Equal(duplicate, record) {
		t.Fatal("duplicate completion appended audit or rewrote outcome")
	}
	if _, err := s.FinishCommand(ctx, "node", "stable", probeResult(command, statusTimedOut, "deadline_expired"), commandTestTime.Add(4*time.Second)); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("known terminal result overwritten: %v", err)
	}
	if original.Status != statusAccepted || len(original.Audit) != 1 {
		t.Fatal("returned record aliased later changes")
	}
}

func TestCommandRecoveryExpiryAndReconciliation(t *testing.T) {
	for _, mode := range []string{"recovery", "disconnect", "expiry"} {
		for _, outcome := range []struct {
			status pb.CommandStatus
			detail string
		}{{statusSucceeded, "pong"}, {statusTimedOut, "deadline_expired"}, {statusRejected, "journal_full"}} {
			t.Run(fmt.Sprintf("%s/%s", mode, outcome.detail), func(t *testing.T) {
				ctx := context.Background()
				s := commandStore(t)
				accepted := createTestCommand(t, s, 1)
				running := createTestCommand(t, s, 2)
				_, _, err := s.DispatchCommand(ctx, "node", running.Command.CommandId, commandTestTime)
				requireOK(t, err)
				now := commandTestTime.Add(10 * time.Second)
				reason := "server_restarted"
				acceptedStatus := statusUnavailable
				switch mode {
				case "recovery":
					requireOK(t, s.RecoverCommands(ctx, now))
				case "disconnect":
					reason = "node_disconnected"
					requireOK(t, s.LoseNodeCommands(ctx, "node", now))
				case "expiry":
					reason = "deadline_expired"
					acceptedStatus = statusTimedOut
					requireOK(t, s.ExpireCommands(ctx, now.Add(-time.Nanosecond)))
					before, err := s.GetCommand(ctx, "actor", running.Command.CommandId)
					requireOK(t, err)
					if before.Status != statusRunning {
						t.Fatal("command expired before the exact deadline")
					}
					requireOK(t, s.ExpireCommands(ctx, now))
				}
				a, err := s.GetCommand(ctx, "actor", accepted.Command.CommandId)
				requireOK(t, err)
				if a.Status != acceptedStatus || a.Detail != reason {
					t.Fatal("undispatched command incorrectly marked uncertain")
				}
				if _, err := s.FinishCommand(ctx, "node", "stable", probeResult(accepted.Command, statusSucceeded, "pong"), now); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("never-dispatched command became success: %v", err)
				}
				r, err := s.FinishCommand(ctx, "node", "stable", probeResult(running.Command, statusIndeterminate, "node_restarted"), now)
				requireOK(t, err)
				if r.Status != statusIndeterminate || r.Detail != reason || len(r.Audit) != 3 {
					t.Fatal("node indeterminate result rewrote internal uncertainty reason")
				}
				r, err = s.FinishCommand(ctx, "node", "stable", probeResult(running.Command, outcome.status, outcome.detail), now.Add(time.Second))
				requireOK(t, err)
				if r.Status != outcome.status || r.Detail != outcome.detail || len(r.Audit) != 4 {
					t.Fatal("late known result did not reconcile uncertainty")
				}
				requireOK(t, s.RecoverCommands(ctx, now.Add(2*time.Second)))
				requireOK(t, s.ExpireCommands(ctx, now.Add(2*time.Second)))
				requireOK(t, s.LoseNodeCommands(ctx, "node", now.Add(2*time.Second)))
				after, err := s.GetCommand(ctx, "actor", r.Command.CommandId)
				requireOK(t, err)
				if !proto.Equal(after, r) {
					t.Fatal("maintenance rewrote a terminal result")
				}
			})
		}
	}
}

func TestIndeterminateReplayAcknowledgesReceiptNotKnownOutcome(t *testing.T) {
	for _, terminal := range []struct {
		status     pb.CommandStatus
		detail     string
		dispatched bool
	}{
		{statusSucceeded, "pong", true},
		{statusTimedOut, "deadline_expired", true},
		{statusRejected, "journal_full", true},
		{statusTimedOut, "deadline_expired", false},
		{statusUnavailable, "node_disconnected", false},
	} {
		t.Run(fmt.Sprintf("%s/dispatched=%t", terminal.status, terminal.dispatched), func(t *testing.T) {
			ctx := context.Background()
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s := openTestStore(t, coordinatorPath)
			requireOK(t, s.Bind(ctx, "stable", "node"))
			admitted := createTestCommand(t, s, 1)
			j := openTestNodeJournal(t, nodePath)
			result, claimed, err := j.Claim(ctx, admitted.Command)
			requireOK(t, err)
			if !claimed || result != nil {
				t.Fatal("expected new durable node intent")
			}
			now := commandTestTime.Add(10 * time.Second)
			if terminal.dispatched {
				_, _, err = s.DispatchCommand(ctx, "node", admitted.Command.CommandId, commandTestTime)
				requireOK(t, err)
				_, err = s.FinishCommand(ctx, "node", "stable", probeResult(admitted.Command, terminal.status, terminal.detail), now)
				requireOK(t, err)
			} else if terminal.status == statusUnavailable {
				requireOK(t, s.LoseNodeCommands(ctx, "node", now))
			} else {
				requireOK(t, s.ExpireCommands(ctx, now))
			}
			known, err := s.GetCommand(ctx, "actor", admitted.Command.CommandId)
			requireOK(t, err)
			requireOK(t, j.Recover(ctx))
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || pending[0].Status != statusIndeterminate {
				t.Fatal("expected retained node uncertainty replay")
			}
			_, err = s.FinishCommand(ctx, "node", "foreign-stable", pending[0], now)
			requireConflict(t, err)
			for i := 0; i < 2; i++ {
				got, err := s.FinishCommand(ctx, "node", "stable", pending[0], now.Add(time.Duration(i)*time.Second))
				requireOK(t, err)
				if !proto.Equal(got, known) {
					t.Fatal("uncertainty replay rewrote known outcome or audit")
				}
			}
			if err := j.Acknowledge(ctx, pending[0].CommandId, known.Status); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("coordinator outcome must not replace the node's receipt status: %v", err)
			}
			requireOK(t, j.Acknowledge(ctx, pending[0].CommandId, pending[0].Status))
			requireOK(t, j.Close())
			requireOK(t, s.Close())
			j = openTestNodeJournal(t, nodePath)
			s = openTestStore(t, coordinatorPath)
			pending, err = j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("acknowledged replay would recur after reconnect")
			}
			got, err := s.GetCommand(ctx, "actor", known.Command.CommandId)
			requireOK(t, err)
			if !proto.Equal(got, known) {
				t.Fatal("known coordinator outcome was not preserved durably")
			}
		})
	}
}

func TestExpiredCommandNeverDispatches(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	record := createTestCommand(t, s, 1)
	record, dispatched, err := s.DispatchCommand(ctx, "node", record.Command.CommandId, record.Command.ExpiresAt.AsTime())
	requireOK(t, err)
	if dispatched || record.Status != statusTimedOut || len(record.Audit) != 2 {
		t.Fatal("expired accepted command was dispatched")
	}
}

func TestCommandTransitionsRollbackAtomically(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	first := createTestCommand(t, s, 1)
	second := createTestCommand(t, s, 2)
	_, err := s.conn.ExecContext(ctx, `CREATE TRIGGER fail_second_command BEFORE UPDATE ON commands
		WHEN old.command_id = '00000000000000000000000000000002'
		BEGIN SELECT RAISE(ABORT, 'injected command write failure'); END`)
	requireOK(t, err)
	if err := s.RecoverCommands(ctx, commandTestTime.Add(time.Second)); err == nil {
		t.Fatal("injected transition failure ignored")
	}
	for _, original := range []*pb.CommandRecord{first, second} {
		got, err := s.GetCommand(ctx, "actor", original.Command.CommandId)
		requireOK(t, err)
		if !proto.Equal(got, original) {
			t.Fatal("bulk recovery or audit committed partially")
		}
	}
	_, err = s.conn.ExecContext(ctx, "DROP TRIGGER fail_second_command")
	requireOK(t, err)
	_, _, err = s.DispatchCommand(ctx, "node", first.Command.CommandId, commandTestTime.Add(-time.Second))
	if err == nil {
		t.Fatal("backwards audit time accepted")
	}
	got, err := s.GetCommand(ctx, "actor", first.Command.CommandId)
	requireOK(t, err)
	if !proto.Equal(got, first) {
		t.Fatal("invalid audit time persisted")
	}
}

func TestCoordinatorMigrationPreservesVersionOneData(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	requireOK(t, s.Bind(ctx, "stable", "node"))
	view := node("stable", "node")
	requireOK(t, s.SaveNode(ctx, view))
	downgradeToVersionOne(t, s)
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	if version := rowCount(t, s, "PRAGMA user_version"); version != 3 {
		t.Fatalf("migration version = %d", version)
	}
	fleet, err := s.LoadFleet(ctx)
	requireOK(t, err)
	if len(fleet) != 1 || !proto.Equal(fleet[0], view) {
		t.Fatal("migration lost fleet")
	}
	requireConflict(t, s.Bind(ctx, "stable", "other"))
	requireConflict(t, s.Bind(ctx, "other", "node"))
	record := createTestCommand(t, s, 1)
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	stored, err := s.GetCommand(ctx, "actor", record.Command.CommandId)
	requireOK(t, err)
	if !proto.Equal(stored, record) {
		t.Fatal("command admission did not survive reopening")
	}
	if stored.Status != statusAccepted {
		t.Fatal("Open implicitly recovered commands")
	}
	requireOK(t, s.RecoverCommands(ctx, commandTestTime.Add(time.Second)))
}

func TestVersionOneMigrationFailsWithoutPartialSchemaChanges(t *testing.T) {
	for _, statement := range []string{
		"ALTER TABLE bindings ADD COLUMN unexpected TEXT",
		"CREATE TABLE commands_target_status (collision TEXT)",
	} {
		t.Run(statement, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "node"))
			requireOK(t, s.SaveNode(ctx, node("stable", "node")))
			downgradeToVersionOne(t, s)
			_, err := s.conn.ExecContext(ctx, statement)
			requireOK(t, err)
			requireOK(t, s.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			for i := 0; i < 2; i++ {
				opened, err := Open(ctx, path, "server")
				if opened != nil {
					requireOK(t, opened.Close())
				}
				if err == nil || errors.Is(err, ErrLocked) {
					t.Fatalf("corrupt schema or failed migration was accepted or leaked ownership: %v", err)
				}
				after, err := os.ReadFile(path)
				requireOK(t, err)
				if !bytes.Equal(before, after) {
					t.Fatal("migration failure changed schema, version, identity, or prior data")
				}
			}
		})
	}
}

func TestJournalAdmissionRollback(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	_, err := s.conn.ExecContext(ctx, `CREATE TRIGGER fail_admission BEFORE INSERT ON commands
		BEGIN SELECT RAISE(ABORT, 'injected admission failure'); END`)
	requireOK(t, err)
	if record, created, err := s.CreateCommand(ctx, probeCommand(1, "actor", "key1", "node", commandTestTime), commandTestTime); err == nil || record != nil || created {
		t.Fatal("failed admission returned a successful or partial record")
	}
	if count := rowCount(t, s, "SELECT count(*) FROM commands"); count != 0 {
		t.Fatal("failed admission retained an idempotency record or audit")
	}
	j := openTestNodeJournal(t, testPath(t))
	_, err = j.store.conn.ExecContext(ctx, `CREATE TRIGGER fail_claim BEFORE INSERT ON node_commands
		BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END`)
	requireOK(t, err)
	if result, claimed, err := j.Claim(ctx, probeCommand(1, "actor", "key1", "node", commandTestTime)); err == nil || claimed || result != nil {
		t.Fatal("failed claim granted permission to execute")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands"); count != 0 {
		t.Fatal("failed claim retained partial intent")
	}
}

func TestJournalMissingOrAlteredSchemaRefused(t *testing.T) {
	for _, statement := range []string{
		"DROP TABLE commands",
		"DROP INDEX commands_target_status",
		"DROP INDEX commands_status_expiry",
		"ALTER TABLE bindings ADD COLUMN unexpected TEXT",
	} {
		t.Run(statement, func(t *testing.T) {
			path := testPath(t)
			s := openTestStore(t, path)
			_, err := s.conn.ExecContext(context.Background(), statement)
			requireOK(t, err)
			requireOK(t, s.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			other, err := Open(context.Background(), path, "server")
			if other != nil {
				requireOK(t, other.Close())
			}
			if err == nil {
				t.Fatal("invalid journal schema opened")
			}
			after, err := os.ReadFile(path)
			requireOK(t, err)
			if !bytes.Equal(before, after) {
				t.Fatal("invalid schema was repaired")
			}
		})
	}
}

func TestCommandRecordCorruptionRefused(t *testing.T) {
	for _, kind := range []string{"index", "wire", "oversized", "unknown", "audit", "audit count", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			s := openTestStore(t, path)
			requireOK(t, s.Bind(ctx, "stable", "node"))
			record := createTestCommand(t, s, 1)
			var payload []byte
			switch kind {
			case "index":
				_, err := s.conn.ExecContext(ctx, "UPDATE commands SET status = ?", statusRunning)
				requireOK(t, err)
			case "wire":
				payload = []byte{0xff}
			case "oversized":
				payload = make([]byte, maxCommandBytes+1)
			case "unknown":
				record.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "audit":
				record.Audit[0].Status = statusSucceeded
			case "audit count":
				for len(record.Audit) <= maxCommandAudit {
					record.Audit = append(record.Audit, proto.Clone(record.Audit[0]).(*pb.CommandAudit))
				}
			case "deadline":
				record.Command.Ttl = durationpb.New(time.Second)
			}
			if kind != "index" {
				if payload == nil {
					var err error
					payload, err = proto.Marshal(record)
					requireOK(t, err)
				}
				_, err := s.conn.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON")
				requireOK(t, err)
				_, err = s.conn.ExecContext(ctx, "UPDATE commands SET record = ?", payload)
				requireOK(t, err)
			}
			if got, err := s.GetCommand(ctx, "actor", record.Command.CommandId); err == nil || got != nil {
				t.Fatal("corrupt command returned successfully")
			}
			requireOK(t, s.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			opened, err := Open(ctx, path, "server")
			if opened != nil {
				requireOK(t, opened.Close())
			}
			if err == nil {
				t.Fatal("corrupt command journal reopened")
			}
			after, err := os.ReadFile(path)
			requireOK(t, err)
			if !bytes.Equal(before, after) {
				t.Fatal("corrupt journal was rewritten")
			}
		})
	}
}

func seedCoordinatorCapacity(t *testing.T, s *Store) *pb.CommandRecord {
	t.Helper()
	ctx := context.Background()
	template := createTestCommand(t, s, 1)
	requireOK(t, s.transaction(ctx, func(tx *sql.Tx) error {
		for i := 2; i < maxCommands; i++ {
			record := proto.Clone(template).(*pb.CommandRecord)
			record.Command.CommandId = fmt.Sprintf("%032x", i)
			record.Command.IdempotencyKey = fmt.Sprintf("key%d", i)
			payload, err := marshalCommandMessage(record)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO commands(command_id, actor_id, idempotency_key, target_id,
				expires_seconds, expires_nanos, status, record) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				record.Command.CommandId, "actor", record.Command.IdempotencyKey, "node", record.Command.ExpiresAt.Seconds,
				record.Command.ExpiresAt.Nanos, record.Status, payload)
			if err != nil {
				return err
			}
		}
		return nil
	}))
	return template
}

func TestCoordinatorCommandCapacityRetainsRetriesAndResults(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	first := seedCoordinatorCapacity(t, s)
	createTestCommand(t, s, maxCommands)
	if _, created, err := s.CreateCommand(ctx, probeCommand(maxCommands+1, "actor", "over-capacity", "node", commandTestTime), commandTestTime); !errors.Is(err, ErrCommandCapacity) || created {
		t.Fatalf("command capacity did not reject new admission: %v", err)
	}
	retry := proto.Clone(first.Command).(*pb.Command)
	retry.CommandId = fmt.Sprintf("%032x", maxCommands+1)
	if record, created, err := s.CreateCommand(ctx, retry, commandTestTime); err != nil || created || record.Command.CommandId != first.Command.CommandId {
		t.Fatalf("capacity blocked matching retry: %v", err)
	}
	_, _, err := s.DispatchCommand(ctx, "node", first.Command.CommandId, commandTestTime)
	requireOK(t, err)
	_, err = s.FinishCommand(ctx, "node", "stable", probeResult(first.Command, statusSucceeded, "pong"), commandTestTime)
	requireOK(t, err)
	if count := rowCount(t, s, "SELECT count(*) FROM commands"); count != maxCommands {
		t.Fatal("capacity handling removed retained records")
	}
}
