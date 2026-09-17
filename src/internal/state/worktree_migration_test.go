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

func TestWorktreeMigrationPreservesWorkspaceAndProbeJournals(t *testing.T) {
	ctx := context.Background()
	coordinatorPath, nodePath := testPath(t), testPath(t)
	s := commandStoreAtPath(t, coordinatorPath)
	j := openTestNodeJournal(t, nodePath)
	view := node("stable", "node")
	view.WorkspaceReady = true
	requireOK(t, s.SaveNode(ctx, view))
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
		workspaceCommand(2, "project2"), workspaceCommand(3, "project3"), workspaceCommand(4, "project4"),
	} {
		admitWorkspace(t, s, command, true)
		_, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if !claimed {
			t.Fatal("legacy fixture was not claimed")
		}
		value := snapshot{command: command}
		switch i {
		case 0:
			value.result = probeResult(command, statusSucceeded, "pong")
		case 1:
			value.result = workspaceResult(command, true)
		case 2:
			value.result = probeResult(command, statusIndeterminate, "herdr_outcome_unknown")
		}
		if value.result != nil {
			requireOK(t, j.Complete(ctx, value.result))
			_, err := s.FinishCommand(ctx, "node", "stable", value.result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			requireOK(t, j.Acknowledge(ctx, command.CommandId, value.result.Status))
		}
		requireOK(t, s.conn.QueryRowContext(ctx, "SELECT record FROM commands WHERE command_id = ?", command.CommandId).Scan(&value.recordBytes))
		requireOK(t, j.store.conn.QueryRowContext(ctx,
			"SELECT command, result, delivered FROM node_commands WHERE command_id = ?", command.CommandId).
			Scan(&value.commandBytes, &value.resultBytes, &value.delivered))
		snapshots = append(snapshots, value)
	}
	removeLifecycleSchema(t, s, coordinatorKind)
	removeLifecycleSchema(t, j.store, nodeKind)
	_, err := s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 3")
	requireOK(t, err)
	_, err = j.store.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 2")
	requireOK(t, err)
	requireOK(t, s.Close())
	requireOK(t, j.Close())
	s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
	if rowCount(t, s, "PRAGMA user_version") != 7 || rowCount(t, j.store, "PRAGMA user_version") != 6 {
		t.Fatal("worktree migration did not fence workspace-only binaries")
	}
	var gotFleet []byte
	requireOK(t, s.conn.QueryRowContext(ctx, "SELECT payload FROM latest_nodes").Scan(&gotFleet))
	if !bytes.Equal(fleetBytes, gotFleet) || rowCount(t, s, "SELECT count(*) FROM bindings WHERE stable_id = 'stable' AND instance_id = 'node'") != 1 {
		t.Fatal("migration changed fleet bytes or identity binding")
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
			t.Fatal("migration rewrote retained request, typed result, audit, or receipt")
		}
		if value.result != nil {
			got, claimed, err := j.Claim(ctx, value.command)
			requireOK(t, err)
			if claimed || !proto.Equal(got, value.result) {
				t.Fatal("migration changed exact legacy retry")
			}
		}
	}
	for i, project := range []string{"project3", "project4"} {
		command := worktreeCommand(10+i, project)
		got, claimed, err := j.Claim(ctx, command)
		requireOK(t, err)
		if claimed || !proto.Equal(got, probeResult(command, statusRejected, "project_unresolved")) {
			t.Fatal("migrated workspace uncertainty or intent failed to quarantine worktree")
		}
	}
	command := worktreeCommand(20, "project2")
	admitWorkspace(t, s, command, true)
	_, claimed, err := j.Claim(ctx, command)
	requireOK(t, err)
	if !claimed {
		t.Fatal("known legacy workspace success incorrectly quarantined worktree")
	}
	requireOK(t, j.Complete(ctx, worktreeResult(command)))
	_, err = s.FinishCommand(ctx, "node", "stable", worktreeResult(command), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestWorktreeMigrationFailsClosedWithoutRewriting(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for _, fault := range []string{"wire", "missing", "owner", "kind", "worktree", "fleet", "binding", "typed-result"} {
			if kind == nodeKind && (fault == "fleet" || fault == "binding") {
				continue
			}
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				path := testPath(t)
				command := workspaceCommand(1, "project1")
				if fault == "worktree" {
					command = worktreeCommand(1, "project1")
				}
				var s *Store
				owner := "server"
				if kind == coordinatorKind {
					s = commandStoreAtPath(t, path)
					admitWorkspace(t, s, command, true)
					requireOK(t, s.SaveNode(ctx, node("stable", "node")))
					removeLifecycleSchema(t, s, coordinatorKind)
					_, err := s.conn.ExecContext(ctx, "DROP TABLE projects; PRAGMA user_version = 3")
					requireOK(t, err)
				} else {
					j := openTestNodeJournal(t, path)
					s, owner = j.store, "node"
					_, _, err := j.Claim(ctx, command)
					requireOK(t, err)
					removeLifecycleSchema(t, s, nodeKind)
					_, err = s.conn.ExecContext(ctx, "DROP TABLE node_projects; PRAGMA user_version = 2")
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
					statement = "DROP INDEX commands_target_status"
					if kind == nodeKind {
						statement = "DROP INDEX node_commands_pending"
					}
				case "owner":
					owner = "wrong-owner"
				case "kind":
					statement = "PRAGMA application_id = 123"
				case "fleet":
					statement = "UPDATE latest_nodes SET payload = x'ff'"
				case "binding":
					statement = "INSERT INTO bindings(stable_id, instance_id) VALUES (' ', 'other')"
				case "typed-result":
					result := workspaceResult(command, true)
					result.WorkspaceEnsure.BindingRevision = "wrong-revision"
					var message proto.Message = result
					query := "UPDATE node_commands SET status = ?, result = ?"
					if kind == coordinatorKind {
						record, err := s.GetCommand(ctx, "actor", command.CommandId)
						requireOK(t, err)
						record.Status, record.Detail = result.Status, result.Detail
						record.WorkspaceEnsure = result.WorkspaceEnsure
						record.Audit = append(record.Audit, &pb.CommandAudit{
							Status: result.Status, Detail: result.Detail, OccurredAt: record.UpdatedAt,
						})
						message, query = record, "UPDATE commands SET status = ?, record = ?"
					}
					payload, err := proto.Marshal(message)
					requireOK(t, err)
					_, err = s.conn.ExecContext(ctx, query, result.Status, payload)
					requireOK(t, err)
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
						t.Fatalf("invalid workspace-era journal migrated or leaked lock: %v", err)
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
