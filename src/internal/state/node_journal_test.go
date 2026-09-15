package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func openTestNodeJournal(t *testing.T, path string) *NodeJournal {
	t.Helper()
	journal, err := OpenNodeJournal(context.Background(), path, "node")
	requireOK(t, err)
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	return journal
}

func claimTestCommand(t *testing.T, j *NodeJournal, id int) *pb.Command {
	t.Helper()
	command := probeCommand(id, "actor", fmt.Sprintf("key%d", id), "node", commandTestTime)
	result, claimed, err := j.Claim(context.Background(), command)
	requireOK(t, err)
	if !claimed || result != nil {
		t.Fatal("new node command did not commit an execution intent")
	}
	return command
}

func TestNodeJournalDurableResultDeliveryAndTombstone(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	j := openTestNodeJournal(t, path)
	command := claimTestCommand(t, j, 1)
	result := probeResult(command, statusSucceeded, "pong")
	requireOK(t, j.Complete(ctx, result))
	requireOK(t, j.Complete(ctx, result))
	requireOK(t, j.Close())
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 1 || !proto.Equal(pending[0], result) {
		t.Fatal("unacknowledged result did not survive restart")
	}
	if err := j.Acknowledge(ctx, command.CommandId, statusTimedOut); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("conflicting acknowledgement accepted: %v", err)
	}
	requireOK(t, j.Acknowledge(ctx, command.CommandId, statusSucceeded))
	requireOK(t, j.Acknowledge(ctx, command.CommandId, statusSucceeded))
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	pending, err = j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 0 {
		t.Fatal("acknowledgement did not survive restart")
	}
	// The persisted deadline can be long past; returning known outcomes is safe.
	retry := proto.Clone(command).(*pb.Command)
	retry.Ttl = durationpb.New(time.Nanosecond)
	got, claimed, err := j.Claim(ctx, retry)
	requireOK(t, err)
	if claimed || !proto.Equal(got, result) {
		t.Fatal("acknowledged tombstone allowed re-execution")
	}
	requireOK(t, j.Complete(ctx, result))
	pending, err = j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 0 {
		t.Fatal("duplicate completion forgot an acknowledgement")
	}
	if err := j.Complete(ctx, probeResult(command, statusTimedOut, "deadline_expired")); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("known node result overwritten: %v", err)
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands"); count != 1 {
		t.Fatal("acknowledgement removed the retained command")
	}
}

func TestNodeJournalClaimConflictsAndRecovery(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	j := openTestNodeJournal(t, path)
	command := claimTestCommand(t, j, 1)
	for _, mutate := range []func(*pb.Command){
		func(command *pb.Command) { command.Actor.ActorId = "other-actor" },
		func(command *pb.Command) { command.IdempotencyKey = "other-key" },
		func(command *pb.Command) {
			command.ExpiresAt = timestamppb.New(command.ExpiresAt.AsTime().Add(time.Second))
		},
	} {
		changed := proto.Clone(command).(*pb.Command)
		mutate(changed)
		if _, claimed, err := j.Claim(ctx, changed); !errors.Is(err, ErrCommandConflict) || claimed {
			t.Fatalf("changed execution intent accepted: %v", err)
		}
	}
	claimTestCommand(t, j, 2)
	requireOK(t, j.Close())
	j = openTestNodeJournal(t, path)
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 0 {
		t.Fatal("Open performed implicit recovery")
	}
	result, claimed, err := j.Claim(ctx, command)
	requireOK(t, err)
	if claimed || !proto.Equal(result, interruptedNodeResult(command.CommandId)) {
		t.Fatal("existing RUNNING claim was executed twice")
	}
	requireOK(t, j.Recover(ctx))
	requireOK(t, j.Recover(ctx))
	pending, err = j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 2 || pending[0].CommandId >= pending[1].CommandId {
		t.Fatal("recovered results are not complete and sorted")
	}
	for _, result := range pending {
		if !proto.Equal(result, interruptedNodeResult(result.CommandId)) {
			t.Fatal("recovered intent must remain indeterminate")
		}
	}
	if err := j.Complete(ctx, probeResult(command, statusSucceeded, "pong")); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("recovery allowed a new local outcome: %v", err)
	}
	expired := probeCommand(3, "actor", "expired", "node", time.Unix(1, 0))
	if result, claimed, err := j.Claim(ctx, expired); err != nil || !claimed || result != nil {
		t.Fatalf("expired command could not be durably claimed for timeout: %v", err)
	}
	requireOK(t, j.Complete(ctx, probeResult(expired, statusTimedOut, "deadline_expired")))
	known, claimed, err := j.Claim(ctx, expired)
	requireOK(t, err)
	if claimed || !proto.Equal(known, probeResult(expired, statusTimedOut, "deadline_expired")) {
		t.Fatal("expired known outcome was not returned from its tombstone")
	}
}

func TestNodeJournalCompletionAndRecoveryRollback(t *testing.T) {
	ctx := context.Background()
	j := openTestNodeJournal(t, testPath(t))
	first := claimTestCommand(t, j, 1)
	claimTestCommand(t, j, 2)
	_, err := j.store.conn.ExecContext(ctx, `CREATE TRIGGER fail_node_update BEFORE UPDATE ON node_commands
		WHEN old.command_id = '00000000000000000000000000000002'
		BEGIN SELECT RAISE(ABORT, 'injected node write failure'); END`)
	requireOK(t, err)
	if err := j.Recover(ctx); err == nil {
		t.Fatal("injected node recovery failure ignored")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL"); count != 2 {
		t.Fatal("node recovery committed partially")
	}
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 0 {
		t.Fatal("failed recovery published a result")
	}
	_, err = j.store.conn.ExecContext(ctx, "DROP TRIGGER fail_node_update")
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, `CREATE TRIGGER fail_node_complete BEFORE UPDATE ON node_commands
		BEGIN SELECT RAISE(ABORT, 'injected node completion failure'); END`)
	requireOK(t, err)
	if err := j.Complete(ctx, probeResult(first, statusSucceeded, "pong")); err == nil {
		t.Fatal("injected node completion failure ignored")
	}
	if count := rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL"); count != 2 {
		t.Fatal("failed completion changed intent")
	}
}

func TestNodeJournalCapacityAndBoundedPendingResults(t *testing.T) {
	ctx := context.Background()
	j := openTestNodeJournal(t, testPath(t))
	first := claimTestCommand(t, j, 1)
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
	claimTestCommand(t, j, maxCommands)
	overflow := probeCommand(maxCommands+1, "actor", "overflow", "node", commandTestTime)
	if _, claimed, err := j.Claim(ctx, overflow); !errors.Is(err, ErrCommandCapacity) || claimed {
		t.Fatalf("node journal capacity accepted new command: %v", err)
	}
	requireOK(t, j.Complete(ctx, probeResult(first, statusSucceeded, "pong")))
	if result, claimed, err := j.Claim(ctx, first); err != nil || claimed || result.Status != statusSucceeded {
		t.Fatalf("capacity blocked retained result: %v", err)
	}
	requireOK(t, j.Recover(ctx))
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != maxCommands {
		t.Fatalf("want exactly 4096 pending results, got %d", len(pending))
	}
	requireOK(t, j.Acknowledge(ctx, first.CommandId, statusSucceeded))
	pending, err = j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != maxCommands-1 {
		t.Fatal("capacity blocked acknowledgement")
	}
	if _, claimed, err := j.Claim(ctx, overflow); !errors.Is(err, ErrCommandCapacity) || claimed {
		t.Fatal("acknowledgement deleted a tombstone to make space")
	}
}

func TestJournalKindsOwnersAndLockIsolation(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	coordinatorPath := filepath.Join(base, "coordinator", "mesh.db")
	nodePath := filepath.Join(base, "node", "journal.db")
	s := openTestStore(t, coordinatorPath)
	j := openTestNodeJournal(t, nodePath)
	for _, check := range []func() error{
		func() error { _, err := OpenNodeJournal(ctx, coordinatorPath, "server"); return err },
		func() error { _, err := Open(ctx, nodePath, "node"); return err },
		func() error { _, err := OpenNodeJournal(ctx, nodePath, "different"); return err },
	} {
		if err := check(); !errors.Is(err, ErrLocked) {
			t.Fatalf("live ownership check did not precede kind/identity check: %v", err)
		}
	}
	requireOK(t, s.Close())
	requireOK(t, j.Close())
	for _, check := range []func() error{
		func() error { _, err := OpenNodeJournal(ctx, coordinatorPath, "server"); return err },
		func() error { _, err := Open(ctx, nodePath, "node"); return err },
	} {
		if err := check(); err == nil || errors.Is(err, ErrLocked) {
			t.Fatalf("database kind not isolated or failed open leaked lock: %v", err)
		}
	}
	if _, err := OpenNodeJournal(ctx, nodePath, "wrong-node"); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("node database owner reassigned: %v", err)
	}
	j = openTestNodeJournal(t, nodePath)
	if count := rowCount(t, j.store, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('metadata', 'bindings', 'latest_nodes', 'commands')"); count != 0 {
		t.Fatal("node journal contains coordinator tables")
	}
	requireOK(t, j.Close())
	s = openTestStore(t, coordinatorPath)
	downgradeToVersionOne(t, s)
	requireOK(t, s.Close())
	if _, err := OpenNodeJournal(ctx, coordinatorPath, "server"); err == nil {
		t.Fatal("legacy coordinator accepted as node journal")
	}
	openTestStore(t, coordinatorPath)
}

func TestNodeJournalCorruptionRefused(t *testing.T) {
	for _, kind := range []string{"command", "result", "oversized command", "oversized result", "running result", "status", "target", "delivered", "missing table", "missing metadata", "future version"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			j := openTestNodeJournal(t, path)
			command := claimTestCommand(t, j, 1)
			if kind != "running result" {
				requireOK(t, j.Complete(ctx, probeResult(command, statusSucceeded, "pong")))
			}
			_, err := j.store.conn.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON")
			requireOK(t, err)
			switch kind {
			case "command":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET command = ?", []byte{0xff})
			case "result":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET result = ?", []byte{0xff})
			case "oversized command":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET command = ?", make([]byte, maxCommandBytes+1))
			case "oversized result", "running result":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET result = ?", make([]byte, maxCommandBytes+1))
			case "status":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET status = ?", statusTimedOut)
			case "target":
				command.TargetId = "other-node"
				payload, marshalErr := proto.Marshal(command)
				requireOK(t, marshalErr)
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET command = ?", payload)
			case "delivered":
				_, err = j.store.conn.ExecContext(ctx, "UPDATE node_commands SET delivered = 2")
			case "missing table":
				_, err = j.store.conn.ExecContext(ctx, "DROP TABLE node_commands")
			case "missing metadata":
				_, err = j.store.conn.ExecContext(ctx, "DELETE FROM node_metadata")
			case "future version":
				_, err = j.store.conn.ExecContext(ctx, "PRAGMA user_version = 3")
			}
			requireOK(t, err)
			if kind != "missing metadata" && kind != "future version" {
				if _, _, err := j.Claim(ctx, probeCommand(1, "actor", "key1", "node", commandTestTime)); err == nil {
					t.Fatal("corrupt node row accepted")
				}
			}
			requireOK(t, j.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			opened, err := OpenNodeJournal(ctx, path, "node")
			if opened != nil {
				requireOK(t, opened.Close())
			}
			if err == nil {
				t.Fatal("corrupt node journal reopened")
			}
			after, err := os.ReadFile(path)
			requireOK(t, err)
			if !bytes.Equal(before, after) {
				t.Fatal("node journal corruption was repaired")
			}
		})
	}
}

func TestJournalInvalidInputsAndCanceledContexts(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	j := openTestNodeJournal(t, testPath(t))
	command := createTestCommand(t, s, 1).Command
	claimTestCommand(t, j, 1)
	result := probeResult(command, statusSucceeded, "pong")
	for _, invalid := range []*pb.Command{nil, {}, proto.Clone(command).(*pb.Command)} {
		if invalid != nil && invalid.CommandId != "" {
			invalid.Payload = &structpb.Struct{}
		}
		if _, _, err := s.CreateCommand(ctx, invalid, commandTestTime); err == nil {
			t.Fatal("invalid coordinator command accepted")
		}
		if _, _, err := j.Claim(ctx, invalid); err == nil {
			t.Fatal("invalid node command accepted")
		}
	}
	for _, invalid := range []*pb.CommandResult{nil, {}, {CommandId: command.CommandId, Status: statusSucceeded, Detail: "raw error"}} {
		if _, err := s.FinishCommand(ctx, "node", "stable", invalid, commandTestTime); err == nil {
			t.Fatal("invalid coordinator result accepted")
		}
		if err := j.Complete(ctx, invalid); err == nil {
			t.Fatal("invalid node result accepted")
		}
	}
	if err := j.Complete(ctx, probeResult(probeCommand(99, "actor", "unknown", "node", commandTestTime), statusSucceeded, "pong")); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("unclaimed completion returned %v", err)
	}
	if err := j.Acknowledge(ctx, command.CommandId, statusRunning); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("acknowledgement accepted RUNNING: %v", err)
	}
	for _, invalid := range []func() error{
		func() error { _, err := s.FindCommand(ctx, "", "key1"); return err },
		func() error { _, err := s.GetCommand(ctx, "actor", ""); return err },
		func() error { _, _, err := s.DispatchCommand(ctx, "", command.CommandId, commandTestTime); return err },
		func() error { return s.LoseNodeCommands(ctx, "", commandTestTime) },
		func() error { return j.Acknowledge(ctx, "", statusSucceeded) },
	} {
		if err := invalid(); err == nil {
			t.Fatal("empty journal ID accepted")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, operation := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := s.FindCommand(ctx, "actor", "key1"); return err },
		func(ctx context.Context) error { _, err := s.GetCommand(ctx, "actor", command.CommandId); return err },
		func(ctx context.Context) error {
			_, _, err := s.CreateCommand(ctx, command, commandTestTime)
			return err
		},
		func(ctx context.Context) error {
			_, _, err := s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime)
			return err
		},
		func(ctx context.Context) error {
			_, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime)
			return err
		},
		func(ctx context.Context) error { return s.LoseNodeCommands(ctx, "node", commandTestTime) },
		func(ctx context.Context) error { return s.ExpireCommands(ctx, commandTestTime) },
		func(ctx context.Context) error { return s.RecoverCommands(ctx, commandTestTime) },
		func(ctx context.Context) error { _, _, err := j.Claim(ctx, command); return err },
		func(ctx context.Context) error { return j.Complete(ctx, result) },
		func(ctx context.Context) error { _, err := j.PendingResults(ctx); return err },
		func(ctx context.Context) error { return j.Acknowledge(ctx, command.CommandId, statusSucceeded) },
		func(ctx context.Context) error { return j.Recover(ctx) },
	} {
		if err := operation(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled journal operation returned %v", err)
		}
	}
}

func TestNodeJournalProcessCrashAndLockRecovery(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"claim", "complete"} {
		t.Run(mode, func(t *testing.T) {
			path := testPath(t)
			runNodeJournalChild(t, path, mode)
			j := openTestNodeJournal(t, path)
			runNodeJournalChild(t, path, "locked")
			command := probeCommand(1, "actor", "key1", "node", commandTestTime)
			result, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if claimed {
				t.Fatal("crashed child command was granted execution again")
			}
			expected := interruptedNodeResult(command.CommandId)
			if mode == "complete" {
				expected = probeResult(command, statusSucceeded, "pong")
			}
			if !proto.Equal(result, expected) {
				t.Fatal("committed crash recovery outcome was lost")
			}
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], expected) {
				t.Fatal("crash result is not pending delivery")
			}
		})
	}
}

func runNodeJournalChild(t *testing.T, path, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNodeJournalOwnedChild$")
	command.Env = append(os.Environ(), "HERDR_NODE_JOURNAL_TEST_CHILD="+mode, "HERDR_NODE_JOURNAL_TEST_DB="+path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("owned node journal child: %v\n%s", err, output)
	}
}

func TestNodeJournalOwnedChild(t *testing.T) {
	mode := os.Getenv("HERDR_NODE_JOURNAL_TEST_CHILD")
	if mode == "" {
		return
	}
	ctx := context.Background()
	j, err := OpenNodeJournal(ctx, os.Getenv("HERDR_NODE_JOURNAL_TEST_DB"), "node")
	if mode == "locked" {
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("expected live journal lock refusal, got %v", err)
		}
		return
	}
	requireOK(t, err)
	command := claimTestCommand(t, j, 1)
	if mode == "complete" {
		requireOK(t, j.Complete(ctx, probeResult(command, statusSucceeded, "pong")))
	} else if mode != "claim" {
		t.Fatal("unknown owned node journal child mode")
	}
	os.Exit(0)
}
