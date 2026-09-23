package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestInvalidLifecycleEvidenceDoesNotPoisonCoordinator(t *testing.T) {
	for _, kind := range []string{"malformed-progress", "delayed-progress", "forward-progress", "completion"} {
		t.Run(kind, func(t *testing.T) {
			h := newCommandHarness(t, t.TempDir())
			entry := readyAgentEntry(t, h, "node-1", "stable-1")
			entry.lifecycle = true
			c := namedPendingCommand(t)
			ctx := context.Background()
			if _, _, err := h.api.commands.CreateCommand(ctx, c, c.ExpiresAt.AsTime().Add(-c.Ttl.AsDuration())); err != nil {
				t.Fatal(err)
			}
			if _, _, err := h.api.commands.DispatchCommand(ctx, "node-1", c.CommandId, time.Now()); err != nil {
				t.Fatal(err)
			}
			receipt := &pb.AgentLifecycleReceipt{
				Handle: &pb.AgentLifecycleHandle{WorkspaceId: "ws:1", TabId: "tab:1", Provider: "unknown",
					Target: &pb.AgentTarget{PaneId: "pane:1", TerminalId: "term:1",
						SessionName: "main", SessionIncarnation: strings.Repeat("b", 64)}},
				PaneOutcome: "confirmed", LaunchOutcome: "not_attempted", PromptOutcome: "not_attempted", StopOutcome: "not_attempted",
				Sequence: 2, Stages: []*pb.AgentLifecycleStage{
					{Stage: "create_pane", Before: true, Outcome: "unknown"},
					{Stage: "create_pane", Outcome: "confirmed"},
				},
			}
			if err := h.api.commandProgress(entry, &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: receipt}); err != nil {
				t.Fatal(err)
			}
			before, err := h.api.commands.GetOperatorCommand(ctx, c.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			bad := proto.Clone(receipt).(*pb.AgentLifecycleReceipt)
			bad.Handle.Target.TerminalId = "replacement"
			switch kind {
			case "malformed-progress":
				bad = &pb.AgentLifecycleReceipt{}
			case "forward-progress":
				bad.Sequence++
				bad.LaunchOutcome = "unknown"
				bad.Stages = append(bad.Stages, &pb.AgentLifecycleStage{Stage: "launch", Before: true, Outcome: "unknown"})
			}
			if kind == "completion" {
				_, err = h.api.finishCommand(entry, &pb.CommandResult{CommandId: c.CommandId,
					Status: pb.CommandStatus_COMMAND_STATUS_FAILED, Detail: "lifecycle_partial", AgentLifecycle: bad})
			} else {
				err = h.api.commandProgress(entry, &pb.CommandProgress{CommandId: c.CommandId, AgentLifecycle: bad})
			}
			if status.Code(err) != codes.FailedPrecondition || h.api.fleet.storageErr != nil {
				t.Fatalf("invalid node evidence became a fatal persistence failure: %v", err)
			}
			after, err := h.api.commands.GetOperatorCommand(ctx, c.CommandId)
			if err != nil || !proto.Equal(before, after) {
				t.Fatalf("invalid node evidence rewrote the durable checkpoint: %v", err)
			}
			if _, err := h.api.finishCommand(entry, &pb.CommandResult{CommandId: c.CommandId,
				Status: pb.CommandStatus_COMMAND_STATUS_FAILED, Detail: "lifecycle_partial", AgentLifecycle: receipt}); err != nil {
				t.Fatal("valid completion failed after rejected evidence:", err)
			}
		})
	}
}
