package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestLifecycleInputConflictsDoNotMaskStorageFailures(t *testing.T) {
	for _, operation := range []string{"progress", "completion"} {
		for _, fault := range []string{"stored-checkpoint", "database-write"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				ctx := context.Background()
				s := commandStore(t)
				c := lifecycleCommand(t, 1)
				admitWorkspace(t, s, c, true)
				receipts := lifecycleCheckpoints()
				requireOK(t, s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: receipts[2]}))
				if fault == "stored-checkpoint" {
					record, err := s.GetOperatorCommand(ctx, c.CommandId)
					requireOK(t, err)
					record.AgentLifecycle.PaneOutcome = "invalid"
					payload, err := proto.Marshal(record)
					requireOK(t, err)
					_, err = s.conn.ExecContext(ctx, "UPDATE commands SET record = ?", payload)
					requireOK(t, err)
				} else {
					_, err := s.conn.ExecContext(ctx, "CREATE TRIGGER fail_lifecycle_update BEFORE UPDATE ON commands "+
						"BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
					requireOK(t, err)
				}
				var err error
				if operation == "progress" {
					err = s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: receipts[3]})
				} else {
					_, err = s.FinishCommand(ctx, "node", "stable", &pb.CommandResult{CommandId: c.CommandId,
						Status: statusSucceeded, Detail: "agent_started", AgentLifecycle: receipts[6]}, commandTestTime.Add(2*time.Second))
				}
				if err == nil || errors.Is(err, ErrCommandConflict) || errors.Is(err, ErrIdentityConflict) {
					t.Fatalf("lifecycle storage failure was masked as invalid input: %v", err)
				}
			})
		}
	}
}

func TestTypedResultMismatchIsInputConflict(t *testing.T) {
	ctx := context.Background()
	for _, command := range []*pb.Command{
		worktreeCommand(1, "project1"), workspaceCommand(1, "project1"),
		agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
		probeCommand(1, "actor", "key1", "node", commandTestTime),
	} {
		t.Run(command.CommandType, func(t *testing.T) {
			s := commandStore(t)
			j := openTestNodeJournal(t, testPath(t))
			requireOK(t, s.Bind(ctx, "other-stable", "other-node"))
			running := admitWorkspace(t, s, command, true)
			_, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if !claimed {
				t.Fatal("initial command was not claimed")
			}
			exact := probeResult(command, statusSucceeded, "pong")
			switch command.CommandType {
			case protocol.AgentControlCommandType:
				exact = agentResult(command)
			case protocol.WorkspaceEnsureCommandType:
				exact = workspaceResult(command, true)
			case protocol.WorktreeCreateCommandType:
				exact = worktreeResult(command)
			}
			got, err := s.FinishCommand(ctx, "other-node", "other-stable", exact, commandTestTime.Add(2*time.Second))
			if got != nil || !errors.Is(err, ErrIdentityConflict) {
				t.Fatalf("foreign result was not an identity conflict: %v", err)
			}
			candidates := []*pb.CommandResult{
				worktreeResult(worktreeCommand(1, "project1")),
				workspaceResult(workspaceCommand(1, "project1"), true),
				probeResult(command, statusSucceeded, "pong"),
				agentResult(agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)),
			}
			if command.WorktreeCreate != nil {
				for _, field := range []string{"project", "revision", "name", "branch", "base"} {
					changed := proto.Clone(exact).(*pb.CommandResult)
					invalidWorktreeResults()[field](changed)
					candidates = append(candidates, changed)
				}
			}
			if command.WorkspaceEnsure != nil {
				for _, field := range []string{"project", "revision"} {
					changed := proto.Clone(exact).(*pb.CommandResult)
					if field == "project" {
						changed.WorkspaceEnsure.ProjectId = "different-project"
					} else {
						changed.WorkspaceEnsure.BindingRevision = "different-revision"
					}
					candidates = append(candidates, changed)
				}
			}
			for _, result := range candidates {
				if proto.Equal(result, exact) {
					continue
				}
				requireOK(t, protocol.ValidateCommandResult(result))
				got, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
				if got != nil || !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("mismatched result was not an input conflict: %v", err)
				}
				if err := j.Complete(ctx, result); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("mismatched node result was not an input conflict: %v", err)
				}
				stored, err := s.GetCommand(ctx, "actor", command.CommandId)
				requireOK(t, err)
				if !proto.Equal(stored, running) || stored.Status != statusRunning ||
					stored.WorkspaceEnsure != nil || stored.WorktreeCreate != nil {
					t.Fatal("conflicting result changed running command or success data")
				}
				if rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL AND delivered = 0") != 1 {
					t.Fatal("conflicting result changed running node intent")
				}
			}
			requireOK(t, j.Complete(ctx, exact))
			got, err = s.FinishCommand(ctx, "node", "stable", exact, commandTestTime.Add(3*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(got), exact) || len(got.Audit) != 3 {
				t.Fatal("exact result did not complete command after conflicts")
			}
		})
	}
}

func TestTypedResultConflictsDoNotMaskStorageFailures(t *testing.T) {
	for _, kind := range []databaseKind{coordinatorKind, nodeKind} {
		for _, fault := range []string{"stored-result", "database-write"} {
			t.Run(fmt.Sprintf("%d/%s", kind, fault), func(t *testing.T) {
				ctx := context.Background()
				command := worktreeCommand(1, "project1")
				result := worktreeResult(command)
				var s *Store
				var finish func(*pb.CommandResult) error
				table, column := "commands", "record"
				if kind == coordinatorKind {
					s = commandStore(t)
					admitWorkspace(t, s, command, true)
					finish = func(result *pb.CommandResult) error {
						_, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
						return err
					}
				} else {
					j := openTestNodeJournal(t, testPath(t))
					s, table, column = j.store, "node_commands", "result"
					_, _, err := j.Claim(ctx, command)
					requireOK(t, err)
					finish = func(result *pb.CommandResult) error { return j.Complete(ctx, result) }
				}
				if fault == "stored-result" {
					requireOK(t, finish(result))
					var payload []byte
					requireOK(t, s.conn.QueryRowContext(ctx, "SELECT "+column+" FROM "+table).Scan(&payload))
					var message proto.Message
					if kind == coordinatorKind {
						record := new(pb.CommandRecord)
						requireOK(t, proto.Unmarshal(payload, record))
						record.WorktreeCreate.Branch = "corrupt-branch"
						message = record
					} else {
						stored := new(pb.CommandResult)
						requireOK(t, proto.Unmarshal(payload, stored))
						stored.WorktreeCreate.Branch = "corrupt-branch"
						message = stored
					}
					payload, err := proto.Marshal(message)
					requireOK(t, err)
					_, err = s.conn.ExecContext(ctx, "UPDATE "+table+" SET "+column+" = ?", payload)
					requireOK(t, err)
				} else {
					_, err := s.conn.ExecContext(ctx, "CREATE TRIGGER fail_result_update BEFORE UPDATE ON "+table+
						" BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
					requireOK(t, err)
				}
				if err := finish(result); err == nil || errors.Is(err, ErrCommandConflict) || errors.Is(err, ErrIdentityConflict) {
					t.Fatalf("storage failure was masked as an input conflict: %v", err)
				}
			})
		}
	}
}
