package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCanceledWorkspacePollingNeverPoisonsCoordinator(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.workspacePolicy = coordinatorWorkspacePolicy(t)
	readyWorkspaceEntry(t, h)
	actor := metadata.NewIncomingContext(context.Background(), metadata.Pairs("test-peer", "client"))
	record, err := h.api.SubmitCommand(actor, workspaceRequest("poll-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	canceled := 0
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(actor, 50*time.Microsecond)
		// Coarse timers can let every read finish; also guarantee canceled polls.
		if i%2 == 0 {
			cancel()
		}
		_, err := h.api.GetCommand(ctx, &pb.GetCommandRequest{CommandId: record.Command.CommandId})
		cancel()
		switch status.Code(err) {
		case codes.Canceled, codes.DeadlineExceeded:
			canceled++
		case codes.OK:
		default:
			t.Fatalf("poll cancellation surfaced as storage failure: %v", err)
		}
	}
	if canceled < 50 {
		t.Fatal("polling did not exercise cancellation")
	}
	healthy, err := h.api.GetCommand(actor, &pb.GetCommandRequest{CommandId: record.Command.CommandId})
	if err != nil || healthy.Command.CommandId != record.Command.CommandId {
		t.Fatal("coordinator failed after canceled polls", err)
	}
}
