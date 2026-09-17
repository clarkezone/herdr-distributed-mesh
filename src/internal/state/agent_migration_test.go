package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

func TestAgentMigrationPreservesMixedJournals(t *testing.T) {
	ctx := context.Background()
	coordinatorPath, nodePath := testPath(t), testPath(t)
	s, j := commandStoreAtPath(t, coordinatorPath), openTestNodeJournal(t, nodePath)
	requireOK(t, s.SaveNode(ctx, node("stable", "node")))
	var fleetBytes []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&fleetBytes))
	type snapshot struct {
		command                                *pb.Command
		result                                 *pb.CommandResult
		recordBytes, commandBytes, resultBytes []byte
		delivered                              int
	}
	var snapshots []snapshot
	for i, command := range []*pb.Command{
		probeCommand(1, "actor", "key1", "node", commandTestTime),
		workspaceCommand(2, "project2"), worktreeCommand(3, "project3"),
		worktreeCommand(4, "project4"), workspaceCommand(5, "project5"),
	} {
		admitWorkspace(t, s, command, true)
		_, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if !claimed {
			t.Fatal("fixture command was not claimed")
		}
		value := snapshot{command: command}
		switch i {
		case 0:
			value.result = probeResult(command, statusSucceeded, "pong")
		case 1:
			value.result = workspaceResult(command, false)
		case 2:
			value.result = worktreeResult(command)
		case 3:
			value.result = probeResult(command, statusIndeterminate, "herdr_outcome_unknown")
		}
		if value.result != nil {
			requireOK(t, j.Complete(ctx, value.result))
			_, err := s.FinishCommand(ctx, "node", "stable", value.result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			if i != 2 {
				requireOK(t, j.Acknowledge(ctx, command.CommandId, value.result.Status))
			}
		}
		requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands WHERE command_id = ?", command.CommandId).Scan(&value.recordBytes))
		requireOK(t, j.store.conn.QueryRowContext(ctx,
			"SELECT command, result, delivered FROM node_commands WHERE command_id = ?", command.CommandId).
			Scan(&value.commandBytes, &value.resultBytes, &value.delivered))
		snapshots = append(snapshots, value)
	}
	removeLifecycleSchema(t, s, coordinatorKind)
	removeLifecycleSchema(t, j.store, nodeKind)
	_, err := s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 4")
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 3")
	requireOK(t, err)
	requireOK(t, s.Close())
	requireOK(t, j.Close())
	s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
	if rowCount(t, s, "PRAGMA user_version") != 7 || rowCount(t, j.store, "PRAGMA user_version") != 6 {
		t.Fatal("agent migration failed to fence older binaries")
	}
	var gotFleet []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&gotFleet))
	if !bytes.Equal(fleetBytes, gotFleet) || rowCount(t, s, "SELECT count(*) FROM bindings WHERE stable_id = 'stable' AND instance_id = 'node'") != 1 {
		t.Fatal("migration changed fleet bytes or bindings")
	}
	for _, value := range snapshots {
		var recordBytes, commandBytes, resultBytes []byte
		var delivered int
		requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands WHERE command_id = ?", value.command.CommandId).Scan(&recordBytes))
		requireOK(t, j.store.conn.QueryRowContext(ctx,
			"SELECT command, result, delivered FROM node_commands WHERE command_id = ?", value.command.CommandId).
			Scan(&commandBytes, &resultBytes, &delivered))
		if !bytes.Equal(recordBytes, value.recordBytes) || !bytes.Equal(commandBytes, value.commandBytes) ||
			!bytes.Equal(resultBytes, value.resultBytes) || delivered != value.delivered {
			t.Fatal("migration rewrote a retained request, receipt, audit, or ACK")
		}
	}
	pending, err := j.PendingResults(ctx)
	requireOK(t, err)
	if len(pending) != 1 || !proto.Equal(pending[0], snapshots[2].result) {
		t.Fatal("migration changed pending typed worktree receipt")
	}
	for i, project := range []string{"project4", "project5"} {
		command := workspaceCommand(10+i, project)
		got, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if claimed || !proto.Equal(got, probeResult(command, statusRejected, "project_unresolved")) {
			t.Fatal("migration weakened retained project quarantine")
		}
	}
	command := agentCommand(20, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
	admitWorkspace(t, s, command, true)
	_, claimed, err := j.Claim(ctx, command)
	requireOK(t, err)
	if !claimed {
		t.Fatal("migrated mixed journal did not admit agent control")
	}
	requireOK(t, j.Complete(ctx, agentResult(command)))
	_, err = s.FinishCommand(ctx, "node", "stable", agentResult(command), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestAgentMigrationFailsClosedWithoutRewriting(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for _, fault := range []string{"agent-in-old-schema", "wire", "missing-index", "fleet", "binding"} {
			if kind == nodeKind && (fault == "fleet" || fault == "binding") {
				continue
			}
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				command := worktreeCommand(1, "project1")
				if fault == "agent-in-old-schema" {
					command = agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
				}
				var s *Store
				owner, table, column, index := "server", "commands", "record", "commands_target_status"
				if kind == coordinatorKind {
					s = commandStoreAtPath(t, path)
					admitWorkspace(t, s, command, true)
					requireOK(t, s.SaveNode(ctx, node("stable", "node")))
					removeLifecycleSchema(t, s, coordinatorKind)
					_, err := s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 4")
					requireOK(t, err)
				} else {
					j := openTestNodeJournal(t, path)
					s, owner, table, column, index = j.store, "node", "node_commands", "command", "node_commands_pending"
					_, _, err := j.Claim(ctx, command)
					requireOK(t, err)
					removeLifecycleSchema(t, s, nodeKind)
					_, err = s.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 3")
					requireOK(t, err)
				}
				statement := ""
				switch fault {
				case "wire":
					statement = "UPDATE " + table + " SET " + column + " = x'ff'"
				case "missing-index":
					statement = "DROP INDEX " + index
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
					if err == nil || errors.Is(err, ErrLocked) || errors.Is(err, ErrCommandConflict) {
						t.Fatalf("invalid old journal migrated, masked corruption, or leaked lock: %v", err)
					}
					after, err := os.ReadFile(path)
					requireOK(t, err)
					if !bytes.Equal(before, after) {
						t.Fatal("failed migration rewrote original database")
					}
				}
			})
		}
	}
}

func TestAgentStoredCorruptionAndWriteFailuresRemainFatal(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		faults := invalidAgentResults()
		faults["database-write"] = nil
		for fault, change := range faults {
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
				exact := agentResult(command)
				var s *Store
				var finish func(*pb.CommandResult) error
				table, column, owner := "commands", "record", "server"
				if kind == coordinatorKind {
					s = commandStoreAtPath(t, path)
					admitWorkspace(t, s, command, true)
					finish = func(r *pb.CommandResult) error {
						_, err := s.FinishCommand(ctx, "node", "stable", r, commandTestTime.Add(2*time.Second))
						return err
					}
				} else {
					j := openTestNodeJournal(t, path)
					s, table, column, owner = j.store, "node_commands", "result", "node"
					_, _, err := j.Claim(ctx, command)
					requireOK(t, err)
					finish = func(r *pb.CommandResult) error { return j.Complete(ctx, r) }
				}
				if fault == "database-write" {
					_, err := s.conn.ExecContext(ctx, "CREATE TRIGGER fail_agent_update BEFORE UPDATE ON "+table+
						" BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
					requireOK(t, err)
				} else {
					requireOK(t, finish(exact))
					corrupt := proto.Clone(exact).(*pb.CommandResult)
					change(corrupt)
					var message proto.Message = corrupt
					if kind == coordinatorKind {
						record, err := s.GetCommand(ctx, "actor", command.CommandId)
						requireOK(t, err)
						record.Status, record.Detail = corrupt.Status, corrupt.Detail
						record.AgentControl, record.WorkspaceEnsure = corrupt.AgentControl, corrupt.WorkspaceEnsure
						record.Audit[len(record.Audit)-1].Status = corrupt.Status
						record.Audit[len(record.Audit)-1].Detail = corrupt.Detail
						message = record
					}
					payload, err := proto.Marshal(message)
					requireOK(t, err)
					_, err = s.conn.ExecContext(ctx, "UPDATE "+table+" SET status = ?, "+column+" = ?", corrupt.Status, payload)
					requireOK(t, err)
				}
				if err := finish(exact); err == nil || errors.Is(err, ErrCommandConflict) || errors.Is(err, ErrIdentityConflict) {
					t.Fatalf("stored failure was masked as caller conflict: %v", err)
				}
				requireOK(t, s.Close())
				if fault != "database-write" {
					opened, err := openStore(ctx, path, owner, kind)
					if opened != nil {
						requireOK(t, opened.Close())
					}
					if err == nil || errors.Is(err, ErrCommandConflict) {
						t.Fatalf("stored corruption accepted on reopen or masked as caller conflict: %v", err)
					}
				}
			})
		}
	}
}
