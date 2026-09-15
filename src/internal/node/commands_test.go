package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func probe() *pb.Command {
	return &pb.Command{
		CommandId: protocol.NewCommandID(), IdempotencyKey: protocol.NewCommandID(),
		CommandType: protocol.ProbeCommandType, TargetId: "test-node",
		Actor: &pb.Actor{ActorId: "controller", Role: pb.Role_ROLE_CONTROLLER},
		Ttl:   durationpb.New(10 * time.Second), ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Second)),
	}
}

func testJournal(t *testing.T) *state.NodeJournal {
	t.Helper()
	journal, err := state.OpenNodeJournal(context.Background(), filepath.Join(t.TempDir(), "node.db"), "test-node")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })
	return journal
}

func TestProbeDurableDuplicateAndExpiry(t *testing.T) {
	journal := testJournal(t)
	executions := 0
	handler := &commandHandler{journal: journal, nodeID: "test-node", execute: func() { executions++ }}
	command := probe()
	first, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || first.GetDetail() != "pong" {
		t.Fatalf("first probe: %v %v", first, err)
	}
	command.Ttl = durationpb.New(time.Nanosecond)
	second, err := handler.handle(context.Background(), command, time.Now().Add(-time.Hour))
	if err != nil || !proto.Equal(first, second) || executions != 1 {
		t.Fatalf("duplicate: %v %v executions=%d", second, err, executions)
	}
	for _, absolute := range []bool{true, false} {
		command := probe()
		received := time.Now()
		if absolute {
			command.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
		} else {
			received = received.Add(-time.Minute)
		}
		result, err := handler.handle(context.Background(), command, received)
		if err != nil || result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_TIMED_OUT || result.GetDetail() != "deadline_expired" {
			t.Fatalf("expired probe: %v %v", result, err)
		}
		stored, claimed, err := journal.Claim(context.Background(), command)
		if err != nil || claimed || !proto.Equal(stored, result) {
			t.Fatalf("expiry not durable: %v", err)
		}
	}
	if executions != 1 {
		t.Fatalf("expired probes executed: %d", executions)
	}
	conflict := proto.Clone(command).(*pb.Command)
	conflict.IdempotencyKey = "different"
	if _, err := handler.handle(context.Background(), conflict, time.Now()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("conflicting duplicate accepted: %v", err)
	}
}

func TestProbeRejectsInvalidInputBeforeClaim(t *testing.T) {
	for name, mutate := range map[string]func(*pb.Command){
		"type":    func(c *pb.Command) { c.CommandType = "process.start.v1" },
		"payload": func(c *pb.Command) { c.Payload = &structpb.Struct{} },
		"actor":   func(c *pb.Command) { c.Actor.Role = pb.Role_ROLE_NODE },
		"hop":     func(c *pb.Command) { c.Actor.HopCount = 1 },
		"target":  func(c *pb.Command) { c.TargetId = "other-node" },
		"expiry":  func(c *pb.Command) { c.ExpiresAt = nil },
		"unknown": func(c *pb.Command) { c.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			command := probe()
			mutate(command)
			handler := &commandHandler{nodeID: "test-node"}
			if _, err := handler.handle(context.Background(), command, time.Now()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid probe accepted: %v", err)
			}
		})
	}
}

func TestProbeReplaysKnownResultAfterAbsoluteExpiry(t *testing.T) {
	journal := testJournal(t)
	command := probe()
	command.ExpiresAt = timestamppb.New(time.Now().Add(-time.Hour))
	if _, claimed, err := journal.Claim(context.Background(), command); err != nil || !claimed {
		t.Fatalf("claim: %v", err)
	}
	recorded := &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}
	if err := journal.Complete(context.Background(), recorded); err != nil {
		t.Fatal(err)
	}
	handler := &commandHandler{nodeID: "test-node", journal: journal, execute: func() { t.Fatal("known expired result executed") }}
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || !proto.Equal(result, recorded) {
		t.Fatalf("known result changed after expiry: %v %v", result, err)
	}
}

type failingJournal struct {
	commandJournal
	claimErr    error
	completeErr error
	ackErr      error
}

func (j failingJournal) Claim(context.Context, *pb.Command) (*pb.CommandResult, bool, error) {
	return nil, j.claimErr == nil, j.claimErr
}
func (j failingJournal) Complete(context.Context, *pb.CommandResult) error           { return j.completeErr }
func (j failingJournal) Acknowledge(context.Context, string, pb.CommandStatus) error { return j.ackErr }

func TestProbeStorageFailureNeverReturnsSuccess(t *testing.T) {
	for _, test := range []struct {
		name           string
		journal        commandJournal
		wantExecutions int
	}{
		{"claim", failingJournal{claimErr: errors.New("disk failure")}, 0},
		{"complete", failingJournal{completeErr: errors.New("disk failure")}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			executions := 0
			handler := &commandHandler{nodeID: "test-node", journal: test.journal, execute: func() { executions++ }}
			result, err := handler.handle(context.Background(), probe(), time.Now())
			if result != nil || !isPermanentSessionError(err) || executions != test.wantExecutions {
				t.Fatalf("write failure: result=%v err=%v executions=%d", result, err, executions)
			}
		})
	}
}

func TestProbeCapacityAndAcknowledgements(t *testing.T) {
	handler := &commandHandler{nodeID: "test-node", journal: failingJournal{claimErr: state.ErrCommandCapacity},
		execute: func() { t.Fatal("capacity rejection executed") }}
	command := probe()
	result, err := handler.handle(context.Background(), command, time.Now())
	if err != nil || result.GetDetail() != "journal_full" || result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_REJECTED {
		t.Fatalf("capacity: %v %v", result, err)
	}
	bad := &pb.CommandAck{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED}
	if err := handler.acknowledge(context.Background(), bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad ephemeral ack: %v", err)
	}
	bad.Status = pb.CommandStatus_COMMAND_STATUS_REJECTED
	if err := handler.acknowledge(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	if len(handler.ephemeral) != 0 {
		t.Fatal("ephemeral ack not released")
	}
	for range maxEphemeralResults {
		if result, err := handler.handle(context.Background(), probe(), time.Now()); err != nil || result.GetDetail() != "journal_full" {
			t.Fatalf("bounded rejection tracking: %v %v", result, err)
		}
	}
	if _, err := handler.handle(context.Background(), probe(), time.Now()); status.Code(err) != codes.ResourceExhausted || isPermanentSessionError(err) {
		t.Fatalf("rejection backpressure must reconnect, not fail storage: %v", err)
	}
	for _, journalErr := range []error{state.ErrCommandNotFound, state.ErrCommandConflict, errors.New("disk failure")} {
		handler.journal = failingJournal{ackErr: journalErr}
		err := handler.acknowledge(context.Background(), bad)
		if err == nil || !isPermanentSessionError(err) {
			t.Fatalf("ack error ignored: %v", err)
		}
	}
}

func TestEnabledProbesRequireServerTagBeforeNetwork(t *testing.T) {
	options := Options{CommandJournalPath: filepath.Join(t.TempDir(), "node.db"), InstanceID: "test-node"}
	if _, err := RunSession(context.Background(), nil, options); err == nil {
		t.Fatal("missing server tag accepted")
	}
	if err := Run(context.Background(), options); err == nil {
		t.Fatal("missing server tag accepted by production runtime")
	}
}
