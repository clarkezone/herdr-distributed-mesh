package state

import (
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func lifecycleCommand(t *testing.T, id int) *pb.Command {
	t.Helper()
	c := probeCommand(id, "actor", fmt.Sprintf("life%d", id), "node", commandTestTime)
	r, err := protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{NodeInstanceId: "node", IdempotencyKey: c.IdempotencyKey,
		CommandType: protocol.AgentStartCommandType, AgentStart: &pb.AgentStart{ProjectId: "project",
			WorkspaceId: "ws:1", Provider: "copilot", Name: "agent", InitialPrompt: "tiny task"}})
	requireOK(t, err)
	c.CommandType, c.SubmittedRequest, c.Ttl = r.CommandType, r, r.Ttl
	c.ExpiresAt = timestamppb.New(commandTestTime.Add(r.Ttl.AsDuration()))
	c.AgentStart = proto.Clone(r.AgentStart).(*pb.AgentStart)
	c.AgentStart.InitialPrompt, c.AgentStart.BindingRevision = "", "managed:1"
	deadline, err := protocol.LifecycleExecutionDeadline(c.ExpiresAt.AsTime(), c.AgentStart.StartupTimeoutMs)
	requireOK(t, err)
	c.ExecutionExpiresAt = timestamppb.New(deadline)
	requireOK(t, protocol.ValidateCommand(c, "node"))
	return c
}

func lifecycleCheckpoints() []*pb.AgentLifecycleReceipt {
	r := &pb.AgentLifecycleReceipt{Handle: &pb.AgentLifecycleHandle{WorkspaceId: "ws:1",
		Target: &pb.AgentTarget{SessionIncarnation: strings.Repeat("a", 64)}},
		PaneOutcome: "not_attempted", LaunchOutcome: "not_attempted", PromptOutcome: "not_attempted", StopOutcome: "not_attempted"}
	values := []*pb.AgentLifecycleReceipt{proto.Clone(r).(*pb.AgentLifecycleReceipt)}
	for _, stage := range []string{"create_pane", "launch", "prompt"} {
		for _, before := range []bool{true, false} {
			outcome := "confirmed"
			if before {
				outcome = "unknown"
			}
			switch stage {
			case "create_pane":
				r.PaneOutcome = outcome
				if !before {
					r.Handle.TabId, r.Handle.Target.PaneId, r.Handle.Target.TerminalId, r.Handle.Provider = "tab:2", "pane:2", "term:2", "unknown"
				}
			case "launch":
				r.LaunchOutcome = outcome
				if !before {
					r.Handle.Provider, r.ObservedStatus = "copilot", "idle"
				}
			case "prompt":
				r.PromptOutcome = outcome
			}
			r.Stages = append(r.Stages, &pb.AgentLifecycleStage{Stage: stage, Before: before, Outcome: outcome})
			r.Sequence = uint32(len(r.Stages))
			values = append(values, proto.Clone(r).(*pb.AgentLifecycleReceipt))
		}
	}
	return values
}

func TestLifecycleEveryCrashCheckpointRecoversWithoutExecution(t *testing.T) {
	for count := 0; count <= 6; count++ {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			j := openTestNodeJournal(t, path)
			c := lifecycleCommand(t, 1)
			_, claimed, err := j.Claim(ctx, c)
			requireOK(t, err)
			if !claimed {
				t.Fatal("fresh lifecycle was not claimed")
			}
			checkpoints := lifecycleCheckpoints()
			for _, checkpoint := range checkpoints[:count+1] {
				requireOK(t, j.SaveLifecycle(ctx, c.CommandId, checkpoint))
			}
			requireOK(t, j.Close())
			j = openTestNodeJournal(t, path)
			requireOK(t, j.Recover(ctx))
			result, claimed, err := j.Claim(ctx, c)
			requireOK(t, err)
			if claimed || !proto.Equal(result.AgentLifecycle, checkpoints[count]) {
				t.Fatal("recovery re-executed or lost a partial handle")
			}
			want := statusFailed
			switch {
			case count == 0:
				want = statusRejected
			case count == 6:
				want = statusSucceeded
			case count%2 == 1:
				want = statusIndeterminate
			}
			if result.Status != want {
				t.Fatalf("checkpoint %d: got %s, want %s", count, result.Status, want)
			}
			requireOK(t, j.Acknowledge(ctx, c.CommandId, result.Status))
			pending, err := j.PendingResults(ctx)
			requireOK(t, err)
			if len(pending) != 0 {
				t.Fatal("ACK did not clear delivery backlog")
			}
			if want == statusIndeterminate {
				next := lifecycleCommand(t, 2)
				next.Actor.ActorId = "another-actor"
				next.AgentStart.Provider, next.SubmittedRequest.AgentStart.Provider = "claude", "claude"
				rejected, claimed, err := j.Claim(ctx, next)
				requireOK(t, err)
				if claimed || rejected.Detail != "lifecycle_unresolved" {
					t.Fatal("ACK, provider, actor or new key bypassed unresolved workspace")
				}
			}
		})
	}
}

func TestLifecycleLostAckAndLateProgressPreserveNewestReceipt(t *testing.T) {
	ctx := context.Background()
	s := commandStoreAtPath(t, testPath(t))
	c := lifecycleCommand(t, 1)
	admitWorkspace(t, s, c, true)
	checkpoints := lifecycleCheckpoints()
	requireOK(t, s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: checkpoints[3]}))
	requireOK(t, s.LoseNodeCommands(ctx, "node", commandTestTime.Add(2*time.Second)))
	result := &pb.CommandResult{CommandId: c.CommandId, Status: statusSucceeded, Detail: "agent_started", AgentLifecycle: checkpoints[6]}
	for range 2 {
		record, err := s.FinishCommand(ctx, "node", "stable", result, commandTestTime.Add(3*time.Second))
		requireOK(t, err)
		if !proto.Equal(record.AgentLifecycle, checkpoints[6]) {
			t.Fatal("late result lost durable stage evidence")
		}
	}
	requireOK(t, s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: checkpoints[2]}))
	if s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: &pb.AgentLifecycleReceipt{}}) == nil {
		t.Fatal("malformed delayed checkpoint was accepted")
	}
	bad := proto.Clone(checkpoints[2]).(*pb.AgentLifecycleReceipt)
	bad.Handle.Target.TerminalId = "replacement"
	if s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: bad}) == nil {
		t.Fatal("delayed progress rewrote terminal identity")
	}
}

func TestLifecycleStartupBudgetDoesNotExtendDispatchTTL(t *testing.T) {
	ctx := context.Background()
	s := commandStoreAtPath(t, testPath(t))
	running, queued := lifecycleCommand(t, 1), lifecycleCommand(t, 2)
	queued.AgentStart.WorkspaceId, queued.SubmittedRequest.AgentStart.WorkspaceId = "ws:2", "ws:2"
	admitWorkspace(t, s, running, true)
	admitWorkspace(t, s, queued, false)
	requireOK(t, s.ExpireCommands(ctx, commandTestTime.Add(11*time.Second)))
	active, err := s.GetCommand(ctx, "actor", running.CommandId)
	requireOK(t, err)
	expired, err := s.GetCommand(ctx, "actor", queued.CommandId)
	requireOK(t, err)
	if active.Status != statusRunning || expired.Status != statusTimedOut {
		t.Fatal("startup budget changed queued TTL or running startup expired at dispatch TTL")
	}
	requireOK(t, s.ExpireCommands(ctx, running.ExecutionExpiresAt.AsTime()))
	active, err = s.GetCommand(ctx, "actor", running.CommandId)
	requireOK(t, err)
	if active.Status != statusIndeterminate {
		t.Fatal("execution expiry did not preserve uncertainty")
	}
}

func TestLifecycleCoordinatorFencesDefaultAliasesAcrossActors(t *testing.T) {
	ctx := context.Background()
	s := commandStoreAtPath(t, testPath(t))
	c := lifecycleCommand(t, 1)
	admitWorkspace(t, s, c, true)
	requireOK(t, s.SaveCommandProgress(ctx, "node", &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: lifecycleCheckpoints()[3]}))
	alias := lifecycleCommand(t, 2)
	alias.Actor.ActorId = "another"
	alias.AgentStart.SessionName, alias.SubmittedRequest.AgentStart.SessionName = "default", "default"
	alias.AgentStart.SessionIncarnation, alias.SubmittedRequest.AgentStart.SessionIncarnation = strings.Repeat("a", 64), strings.Repeat("a", 64)
	if _, _, err := s.CreateCommand(ctx, alias, commandTestTime); !errors.Is(err, ErrLifecycleUnresolved) {
		t.Fatalf("selector alias escaped quarantine: %v", err)
	}
	alias.AgentStart.SessionIncarnation, alias.SubmittedRequest.AgentStart.SessionIncarnation = strings.Repeat("b", 64), strings.Repeat("b", 64)
	_, _, err := s.CreateCommand(ctx, alias, commandTestTime)
	requireOK(t, err)
}

func TestLifecycleMaximumPromptReceiptAuditFitsJournal(t *testing.T) {
	c := lifecycleCommand(t, 1)
	c.SubmittedRequest.AgentStart.InitialPrompt = strings.Repeat("x", protocol.MaxAgentPromptBytes)
	r := lifecycleCheckpoints()[6]
	for _, target := range []*string{&r.Handle.TabId, &r.Handle.Target.PaneId, &r.Handle.Target.TerminalId, &r.Handle.Target.AgentSessionId} {
		*target = strings.Repeat("i", 128)
	}

	s := commandStoreAtPath(t, testPath(t))
	record, _, err := s.CreateCommand(context.Background(), c, commandTestTime)
	requireOK(t, err)
	record.AgentLifecycle = r
	record.Audit = append(record.Audit, &pb.CommandAudit{Status: statusRunning, Detail: "dispatched", OccurredAt: record.CreatedAt})
	for len(record.Audit) < maxCommandAudit {
		record.Audit = append(record.Audit, &pb.CommandAudit{Status: statusIndeterminate, Detail: "node_disconnected", OccurredAt: record.CreatedAt})
	}
	record.Status, record.Detail = statusIndeterminate, "node_disconnected"
	requireOK(t, validateCommandRecord(record))
	payload, err := marshalCommandMessage(record)
	requireOK(t, err)
	if len(payload) > maxCommandBytes || strings.Count(string(payload), c.SubmittedRequest.AgentStart.InitialPrompt) != 1 {
		t.Fatal("receipt/audit exceeded the blob bound or duplicated prompt")
	}
}

func TestLifecycleCorruptSidecarsFailClosedWithoutRewrite(t *testing.T) {
	for _, fault := range []string{"receipt", "sequence", "scope", "missing-scope", "terminal-result"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			path := testPath(t)
			j := openTestNodeJournal(t, path)
			c := lifecycleCommand(t, 1)
			_, _, err := j.Claim(ctx, c)
			requireOK(t, err)
			for _, checkpoint := range lifecycleCheckpoints()[:4] {
				requireOK(t, j.SaveLifecycle(ctx, c.CommandId, checkpoint))
			}
			statement := ""
			switch fault {
			case "receipt":
				statement = "UPDATE node_lifecycle SET receipt = x'ff'"
			case "sequence":
				statement = "UPDATE node_lifecycle SET sequence = 8"
			case "scope":
				statement = "UPDATE node_command_scopes SET terminal_key = '" + strings.Repeat("a", 64) + "'"
			case "missing-scope":
				statement = "DELETE FROM node_command_scopes"
			case "terminal-result":
				requireOK(t, j.Recover(ctx))
				statement = "UPDATE node_lifecycle SET receipt = x'ff'"
			}
			_, err = j.store.conn.ExecContext(ctx, statement)
			requireOK(t, err)
			requireOK(t, j.Close())
			before, err := os.ReadFile(path)
			requireOK(t, err)
			reopened, err := OpenNodeJournal(ctx, path, "node")
			if reopened != nil {
				requireOK(t, reopened.Close())
			}
			if err == nil {
				t.Fatal("corrupt lifecycle sidecar opened")
			}
			after, err := os.ReadFile(path)
			requireOK(t, err)
			if string(before) != string(after) {
				t.Fatal("failed validation rewrote the corrupt journal")
			}
		})
	}
}
