package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func agentCommand(id int, action pb.AgentControlAction) *pb.Command {
	command := probeCommand(id, "actor", fmt.Sprintf("key%d", id), "node", commandTestTime)
	command.CommandType = protocol.AgentControlCommandType
	command.AgentControl = &pb.AgentControl{
		Action: action, Target: &pb.AgentTarget{PaneId: "pane1", TerminalId: "terminal1", AgentSessionId: "session1"},
	}
	switch action {
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT:
		command.AgentControl.Text = "Continue the requested work."
	case pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT:
		command.AgentControl.Keys = []string{"y", "enter"}
	}
	return command
}

func agentResult(command *pb.Command) *pb.CommandResult {
	return &pb.CommandResult{
		CommandId: command.CommandId, Status: statusSucceeded,
		Detail: protocol.AgentControlSuccessDetail(command.AgentControl.Action),
		AgentControl: &pb.AgentControlResult{
			Target:         proto.Clone(command.AgentControl.Target).(*pb.AgentTarget),
			ObservedStatus: "idle", StateChangeSeq: 42,
		},
	}
}

func TestAgentOutcomesDurableAndImmutable(t *testing.T) {
	outcomes := []struct {
		status pb.CommandStatus
		detail string
		action pb.AgentControlAction
	}{
		{statusSucceeded, "agent_prompt_sent", pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT},
		{statusSucceeded, "agent_input_sent", pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT},
		{statusSucceeded, "agent_interrupt_sent", pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT},
	}
	for _, detail := range []string{"journal_full", "precondition_failed", "agent_busy", "agent_blocked",
		"target_changed", "herdr_unavailable", "unsupported", "authorization_changed", "deadline_expired",
		"node_restarted", "herdr_outcome_unknown"} {
		status := statusRejected
		if detail == "deadline_expired" {
			status = statusTimedOut
		} else if detail == "node_restarted" || detail == "herdr_outcome_unknown" {
			status = statusIndeterminate
		}
		outcomes = append(outcomes, struct {
			status pb.CommandStatus
			detail string
			action pb.AgentControlAction
		}{status, detail, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT})
	}
	for _, outcome := range outcomes {
		t.Run(outcome.detail, func(t *testing.T) {
			ctx := context.Background()
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s, j := commandStoreAtPath(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			command := agentCommand(1, outcome.action)
			if outcome.action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
				command.AgentControl.Text = strings.Repeat("x", 8192)
			}
			admitWorkspace(t, s, command, true)
			got, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if got != nil || !claimed {
				t.Fatal("new agent intent not claimed")
			}
			result := probeResult(command, outcome.status, outcome.detail)
			if outcome.status == statusSucceeded {
				result = agentResult(command)
			}
			requireOK(t, j.Complete(ctx, result))
			record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) || !proto.Equal(record.Command, command) || len(record.Audit) != 3 {
				t.Fatal("agent receipt, request, or audit changed")
			}
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], result) {
				t.Fatal("pending receipt lost after restart")
			}
			if err := j.Acknowledge(ctx, command.CommandId, statusRunning); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("incorrect ACK status accepted: %v", err)
			}
			requireOK(t, j.Acknowledge(ctx, command.CommandId, result.Status))
			requireOK(t, j.Complete(ctx, result))
			got, claimed, err = j.Claim(ctx, command)
			requireOK(t, err)
			if claimed || !proto.Equal(got, result) {
				t.Fatal("replay resent agent input")
			}
			pending, err = j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("duplicate completion or claim reset ACK")
			}
			for _, replay := range []*pb.CommandResult{result, interruptedNodeResult(command.CommandId)} {
				duplicate, err := s.FinishCommand(ctx, "node", "stable", replay, commandTestTime.Add(3*time.Second))
				requireOK(t, err)
				if !proto.Equal(duplicate, record) {
					t.Fatal("replay rewrote retained receipt")
				}
			}
			if outcome.status == statusSucceeded {
				for _, change := range []func(*pb.CommandResult){
					func(r *pb.CommandResult) { r.AgentControl.ObservedStatus = "working" },
					func(r *pb.CommandResult) { r.AgentControl.StateChangeSeq++ },
				} {
					changed := proto.Clone(result).(*pb.CommandResult)
					change(changed)
					if _, err := s.FinishCommand(ctx, "node", "stable", changed, commandTestTime.Add(4*time.Second)); !errors.Is(err, ErrCommandConflict) {
						t.Fatalf("changed receipt accepted: %v", err)
					}
					if err := j.Complete(ctx, changed); !errors.Is(err, ErrCommandConflict) {
						t.Fatalf("changed node receipt accepted: %v", err)
					}
				}
			}
		})
	}
}

func TestAgentImmutableRequestFingerprint(t *testing.T) {
	ctx := context.Background()
	for _, action := range []pb.AgentControlAction{pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT} {
		t.Run(action.String(), func(t *testing.T) {
			s, j := commandStore(t), openTestNodeJournal(t, testPath(t))
			command := agentCommand(1, action)
			original := admitWorkspace(t, s, command, false)
			_, _, err := j.Claim(ctx, command)
			requireOK(t, err)
			changes := []func(*pb.Command){
				func(c *pb.Command) { c.AgentControl.Target.PaneId = "pane2" },
				func(c *pb.Command) { c.AgentControl.Target.TerminalId = "terminal2" },
				func(c *pb.Command) { c.AgentControl.Target.AgentSessionId = "session2" },
				func(c *pb.Command) { c.AgentControl.Target.AgentSessionId = "" },
				func(c *pb.Command) {
					c.AgentControl = agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT).AgentControl
				},
				func(c *pb.Command) { c.AgentControl, c.CommandType = nil, protocol.ProbeCommandType },
			}
			if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
				changes = append(changes, func(c *pb.Command) { c.AgentControl.Text += " " })
			} else {
				changes = append(changes,
					func(c *pb.Command) { c.AgentControl.Keys[0] = "n" },
					func(c *pb.Command) { c.AgentControl.Keys = []string{"enter", "y"} })
			}
			for _, change := range changes {
				retry := proto.Clone(command).(*pb.Command)
				change(retry)
				if _, _, err := s.CreateCommand(ctx, retry, commandTestTime); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed request accepted: %v", err)
				}
				if _, _, err := j.Claim(ctx, retry); !errors.Is(err, ErrCommandConflict) {
					t.Fatalf("changed node intent accepted: %v", err)
				}
			}
			retry := proto.Clone(command).(*pb.Command)
			retry.CommandId = fmt.Sprintf("%032x", 2)
			retry.ExpiresAt = timestamppb.New(commandTestTime.Add(time.Hour))
			got, created, err := s.CreateCommand(ctx, retry, commandTestTime.Add(time.Hour))
			requireOK(t, err)
			if created || !proto.Equal(got, original) {
				t.Fatal("same-key retry rewrote original admission")
			}
			retry = proto.Clone(command).(*pb.Command)
			retry.Ttl = durationpb.New(time.Nanosecond)
			if _, _, err := s.CreateCommand(ctx, retry, commandTestTime); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("coordinator discarded original TTL: %v", err)
			}
			result, claimed, err := j.Claim(ctx, retry)
			requireOK(t, err)
			if claimed || !proto.Equal(result, interruptedNodeResult(command.CommandId)) {
				t.Fatal("pending retry resent agent input instead of preserving uncertainty")
			}
		})
	}
}

func invalidAgentResults() map[string]func(*pb.CommandResult) {
	return map[string]func(*pb.CommandResult){
		"pane":           func(r *pb.CommandResult) { r.AgentControl.Target.PaneId = "pane2" },
		"terminal":       func(r *pb.CommandResult) { r.AgentControl.Target.TerminalId = "terminal2" },
		"session":        func(r *pb.CommandResult) { r.AgentControl.Target.AgentSessionId = "session2" },
		"action":         func(r *pb.CommandResult) { r.Detail = "agent_input_sent" },
		"missing":        func(r *pb.CommandResult) { r.AgentControl = nil },
		"status":         func(r *pb.CommandResult) { r.AgentControl.ObservedStatus = "invalid" },
		"failure-data":   func(r *pb.CommandResult) { r.Status, r.Detail = statusRejected, "agent_busy" },
		"unknown":        func(r *pb.CommandResult) { r.AgentControl.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
		"target-unknown": func(r *pb.CommandResult) { r.AgentControl.Target.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
		"mixed": func(r *pb.CommandResult) {
			r.WorkspaceEnsure = workspaceResult(workspaceCommand(1, "project1"), true).WorkspaceEnsure
		},
	}
}

func TestAgentInvalidResultsAreInputConflicts(t *testing.T) {
	ctx := context.Background()
	s, j := commandStore(t), openTestNodeJournal(t, testPath(t))
	command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	running := admitWorkspace(t, s, command, true)
	_, _, err := j.Claim(ctx, command)
	requireOK(t, err)
	for name, change := range invalidAgentResults() {
		t.Run(name, func(t *testing.T) {
			result := agentResult(command)
			change(result)
			if _, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(2*time.Second)); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("invalid external receipt was not a conflict: %v", err)
			}
			if err := j.Complete(ctx, result); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("invalid external node receipt was not a conflict: %v", err)
			}
			stored, err := s.GetCommand(ctx, "actor", command.CommandId)
			requireOK(t, err)
			if !proto.Equal(stored, running) || rowCount(t, j.store, "SELECT count(*) FROM node_commands WHERE status = 2 AND result IS NULL") != 1 {
				t.Fatal("invalid result changed persistent intent")
			}
		})
	}
	requireOK(t, j.Complete(ctx, agentResult(command)))
	_, err = s.FinishCommand(ctx, "node", "stable", agentResult(command), commandTestTime.Add(2*time.Second))
	requireOK(t, err)
}

func TestAgentRecoveryAndProjectNamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	for _, recovery := range []string{"server", "disconnect", "expiry"} {
		t.Run(recovery, func(t *testing.T) {
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s, j := commandStoreAtPath(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
			admitWorkspace(t, s, command, true)
			_, _, err := j.Claim(ctx, command)
			requireOK(t, err)
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s, j = openTestStore(t, coordinatorPath), openTestNodeJournal(t, nodePath)
			requireOK(t, j.Recover(ctx))
			now := commandTestTime.Add(20 * time.Second)
			switch recovery {
			case "server":
				requireOK(t, s.RecoverCommands(ctx, now))
			case "disconnect":
				requireOK(t, s.LoseNodeCommands(ctx, "node", now))
			case "expiry":
				requireOK(t, s.ExpireCommands(ctx, now))
			}
			record, err := s.GetCommand(ctx, "actor", command.CommandId)
			requireOK(t, err)
			if record.Status != statusIndeterminate || record.AgentControl != nil {
				t.Fatal("uncertain input execution inferred success")
			}
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], interruptedNodeResult(command.CommandId)) {
				t.Fatal("recovery did not retain explicit uncertainty")
			}
			// Coordinator can accept a delayed receipt without rerunning the intent.
			result := agentResult(command)
			record, err = s.FinishCommand(ctx, "node", "stable", result, now.Add(time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) {
				t.Fatal("delayed receipt was lost")
			}
			record, err = s.FinishCommand(ctx, "node", "stable", pending[0], now.Add(2*time.Second))
			requireOK(t, err)
			if !proto.Equal(recordResult(record), result) {
				t.Fatal("node uncertainty overwrote known receipt")
			}
			if err := j.Acknowledge(ctx, command.CommandId, record.Status); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("ACK used coordinator outcome rather than received status: %v", err)
			}
			requireOK(t, j.Acknowledge(ctx, command.CommandId, pending[0].Status))
			// Agent target strings are not project IDs, even when their bytes match.
			project := worktreeCommand(2, command.AgentControl.Target.PaneId)
			_, claimed, err := j.Claim(ctx, project)
			requireOK(t, err)
			if !claimed {
				t.Fatal("agent uncertainty incorrectly quarantined a project")
			}
			newKey := agentCommand(3, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
			if projectID, revision := protocol.CommandProject(newKey); projectID != "" || revision != "" {
				t.Fatal("agent target acquired an invented project binding")
			}
			rejected, claimed, err := j.Claim(ctx, newKey)
			requireOK(t, err)
			if claimed || rejected.Detail != "lifecycle_unresolved" {
				t.Fatal("new input key escaped retained agent uncertainty")
			}
			blocked := workspaceCommand(4, project.WorktreeCreate.ProjectId)
			got, claimed, err := j.Claim(ctx, blocked)
			requireOK(t, err)
			if claimed || !proto.Equal(got, probeResult(blocked, statusRejected, "project_unresolved")) {
				t.Fatal("existing project quarantine was weakened")
			}
		})
	}
}

func TestAgentAuthorizationRejectionAndCommandBounds(t *testing.T) {
	ctx := context.Background()
	s, j := commandStore(t), openTestNodeJournal(t, testPath(t))
	command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	admitWorkspace(t, s, command, false)
	record, err := s.RejectCommand(ctx, "node", command.CommandId, "authorization_changed", commandTestTime.Add(time.Second))
	requireOK(t, err)
	if record.Status != statusRejected || record.AgentControl != nil || len(record.Audit) != 2 {
		t.Fatal("authorization change did not revoke admission")
	}
	_, dispatched, err := s.DispatchCommand(ctx, "node", command.CommandId, commandTestTime.Add(2*time.Second))
	requireOK(t, err)
	if dispatched {
		t.Fatal("revoked command dispatched")
	}
	for name, change := range map[string]func(*pb.Command){
		"oversized-text": func(c *pb.Command) { c.AgentControl.Text = strings.Repeat("x", 8193) },
		"journal-cap":    func(c *pb.Command) { c.AgentControl.Text = strings.Repeat("x", maxCommandBytes) },
		"wrong-action":   func(c *pb.Command) { c.AgentControl.Action = pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT },
		"unknown":        func(c *pb.Command) { c.AgentControl.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
		"target-unknown": func(c *pb.Command) { c.AgentControl.Target.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01}) },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := agentCommand(2, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
			change(invalid)
			if _, _, err := s.CreateCommand(ctx, invalid, commandTestTime); err == nil {
				t.Fatal("invalid command admitted")
			}
			if _, _, err := j.Claim(ctx, invalid); err == nil {
				t.Fatal("invalid command claimed")
			}
		})
	}
}

func TestAgentPromptExactByteBoundary(t *testing.T) {
	for name, text := range map[string]string{
		"quotes":      strings.Repeat(`"`, 8192),
		"backslashes": strings.Repeat(`\`, 8192),
		"tabs":        "x" + strings.Repeat("\t", 8191),
		"cr-lf":       "xx" + strings.Repeat("\r\n", 4095),
		"mixed":       "xx" + strings.Repeat("\"\\"+"\t\r\n", 1638),
		"utf8":        strings.Repeat("\u00e9", 4096),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			command := agentCommand(1, pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
			command.AgentControl.Text = text
			command.TargetId = strings.Repeat("n", 128)
			command.Actor.ActorId = strings.Repeat("a", 128)
			command.IdempotencyKey = strings.Repeat("k", 128)
			command.AgentControl.Target = &pb.AgentTarget{
				PaneId: strings.Repeat("p", 128), TerminalId: strings.Repeat("t", 128),
				AgentSessionId: strings.Repeat("s", 128),
			}
			if protocol.MaxAgentPromptBytes != 8192 {
				t.Fatal("protocol prompt limit changed")
			}
			if len(text) != protocol.MaxAgentPromptBytes {
				t.Fatal("fixture does not exercise the exact protocol byte limit")
			}
			requireOK(t, protocol.ValidateCommand(command, command.TargetId))
			coordinatorPath, nodePath := testPath(t), testPath(t)
			s := openTestStore(t, coordinatorPath)
			requireOK(t, s.Bind(ctx, "stable", command.TargetId))
			j, err := OpenNodeJournal(ctx, nodePath, command.TargetId)
			requireOK(t, err)
			t.Cleanup(func() { requireOK(t, j.Close()) })
			record, created, err := s.CreateCommand(ctx, command, commandTestTime)
			requireOK(t, err)
			if !created || !proto.Equal(record.Command, command) {
				t.Fatal("boundary prompt admission changed payload")
			}
			_, dispatched, err := s.DispatchCommand(ctx, command.TargetId, command.CommandId, commandTestTime.Add(time.Second))
			requireOK(t, err)
			if !dispatched {
				t.Fatal("boundary prompt was not dispatched")
			}
			_, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if !claimed {
				t.Fatal("boundary prompt intent was not claimed")
			}
			// Include an uncertainty event as well as the final typed receipt.
			requireOK(t, s.RecoverCommands(ctx, commandTestTime.Add(2*time.Second)))
			result := agentResult(command)
			requireOK(t, j.Complete(ctx, result))
			record, err = s.FinishCommand(ctx, command.TargetId, "stable", result, commandTestTime.Add(3*time.Second))
			requireOK(t, err)
			var recordBytes, commandBytes, resultBytes int
			requireOK(t, s.conn.QueryRowContext(ctx, "SELECT length(record) FROM commands").Scan(&recordBytes))
			requireOK(t, j.store.conn.QueryRowContext(ctx, "SELECT length(command), length(result) FROM node_commands").
				Scan(&commandBytes, &resultBytes))
			if recordBytes != proto.Size(record) || commandBytes != proto.Size(command) || resultBytes != proto.Size(result) ||
				recordBytes > maxCommandBytes || commandBytes > maxCommandBytes || resultBytes > maxCommandBytes {
				t.Fatal("stored binary payload size disagrees with protobuf size or exceeds journal cap")
			}
			t.Logf("prompt=8192 bytes; coordinator record=%d; node command=%d; node result=%d; cap=%d",
				recordBytes, commandBytes, resultBytes, maxCommandBytes)
			requireOK(t, s.Close())
			requireOK(t, j.Close())
			s = openTestStore(t, coordinatorPath)
			j, err = OpenNodeJournal(ctx, nodePath, command.TargetId)
			requireOK(t, err)
			stored, err := s.GetCommand(ctx, command.Actor.ActorId, command.CommandId)
			requireOK(t, err)
			if !proto.Equal(stored, record) || stored.Command.AgentControl.Text != text {
				t.Fatal("reopened coordinator changed boundary prompt or metadata")
			}
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 1 || !proto.Equal(pending[0], result) {
				t.Fatal("reopened node lost typed receipt")
			}
			replay, claimed, err := j.Claim(ctx, command)
			requireOK(t, err)
			if claimed || !proto.Equal(replay, result) {
				t.Fatal("reopened node changed boundary prompt identity or resent input")
			}
			tooLarge := proto.Clone(command).(*pb.Command)
			tooLarge.CommandId = fmt.Sprintf("%032x", 2)
			tooLarge.IdempotencyKey = strings.Repeat("z", 128)
			tooLarge.AgentControl.Text += "x"
			if protocol.ValidateCommand(tooLarge, tooLarge.TargetId) == nil {
				t.Fatal("8193-byte prompt unexpectedly passed protocol validation")
			}
			if _, _, err := s.CreateCommand(ctx, tooLarge, commandTestTime); err == nil {
				t.Fatal("8193-byte prompt admitted")
			}
			if _, _, err := j.Claim(ctx, tooLarge); err == nil {
				t.Fatal("8193-byte prompt claimed")
			}
			// Invalid admission must leave both journals usable for valid new work.
			tooLarge.AgentControl.Text = text
			_, created, err = s.CreateCommand(ctx, tooLarge, commandTestTime)
			requireOK(t, err)
			if !created {
				t.Fatal("invalid admission left a coordinator tombstone")
			}
			_, claimed, err = j.Claim(ctx, tooLarge)
			requireOK(t, err)
			if !claimed {
				t.Fatal("invalid admission left a node intent")
			}
		})
	}
}
