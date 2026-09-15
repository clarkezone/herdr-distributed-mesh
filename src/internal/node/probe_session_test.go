package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type probeStream = grpc.BidiStreamingServer[pb.NodeEnvelope, pb.NodeEnvelope]

type scriptedProbeServer struct {
	pb.UnimplementedNodeControlServer
	capabilities []string
	script       func(probeStream, *pb.Hello) error
}

func (s *scriptedProbeServer) Connect(stream probeStream) error {
	envelope, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := envelope.GetHello()
	if hello == nil {
		return errors.New("missing hello")
	}
	if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_HelloAck{
		HelloAck: &pb.HelloAck{SelectedProtocol: protocol.MinimumVersion, Capabilities: s.capabilities, ServerInstanceId: "test-server"},
	}}); err != nil {
		return err
	}
	return s.script(stream, hello)
}

func sendProbe(stream probeStream, command *pb.Command) error {
	return stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_Command{Command: command}})
}

func receiveResult(stream probeStream, heartbeat func(*pb.Heartbeat), observation func(*pb.HerdrState)) (*pb.CommandResult, error) {
	for {
		message, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if result := message.GetCommandResult(); result != nil {
			return result, nil
		}
		if value := message.GetHeartbeat(); value != nil && heartbeat != nil {
			heartbeat(value)
		}
		if value := message.GetHerdrState(); value != nil && observation != nil {
			observation(value)
		}
	}
}

func TestSessionDuplicateProbeExecutesOnceWithLiveHeartbeatAndObserver(t *testing.T) {
	journal := testJournal(t)
	command := probe()
	executions := atomic.Int32{}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability, protocol.HerdrReadCapability}}
	api.script = func(stream probeStream, hello *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if !slices.Contains(hello.Capabilities, protocol.ProbeCapability) {
			return errors.New("probe not advertised")
		}
		var original *pb.CommandResult
		heartbeats := 0
		observations := 0
		notReady := false
		deadline := time.Now().Add(8 * time.Second)
		for i := 0; i < 2 || heartbeats < 3 || observations == 0; i++ {
			if time.Now().After(deadline) {
				return errors.New("heartbeat or observation starved by commands")
			}
			if err := sendProbe(stream, command); err != nil {
				return err
			}
			result, err := receiveResult(stream, func(h *pb.Heartbeat) {
				heartbeats++
				notReady = notReady || !h.CommandReady
			}, func(*pb.HerdrState) { observations++ })
			if err != nil {
				return err
			}
			if result.Detail != "pong" || notReady {
				return errors.New("wrong result or readiness")
			}
			if original == nil {
				original = result
			} else if !proto.Equal(original, result) {
				return errors.New("duplicate result changed")
			}
			time.Sleep(time.Millisecond)
		}
		return nil
	}
	client := sessionClient(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	_, err := runSessionWithJournal(ctx, client, Options{InstanceID: "test-node", HeartbeatInterval: 10 * time.Millisecond,
		HerdrSocket: filepath.Join(t.TempDir(), "missing.sock")}, journal, func() { executions.Add(1) })
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("script did not complete: %v", err)
	}
	if executions.Load() != 1 {
		t.Fatalf("executions=%d", executions.Load())
	}
	result, claimed, err := journal.Claim(context.Background(), command)
	if err != nil || claimed || result.GetDetail() != "pong" {
		t.Fatalf("result not persisted: %v %v", result, err)
	}
}

func TestSessionRecoversInterruptedClaimAndAcknowledgesAcrossReconnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	command := probe()
	journal, err := state.OpenNodeJournal(context.Background(), path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := journal.Claim(context.Background(), command); err != nil || !claimed {
		t.Fatalf("claim: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	options := Options{InstanceID: "test-node", CommandJournalPath: path, RequiredServerTag: DefaultRequiredServerTag, HeartbeatInterval: 10 * time.Millisecond}
	for _, acknowledge := range []bool{false, true} {
		verified := make(chan error, 1)
		api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability}}
		api.script = func(stream probeStream, _ *pb.Hello) (err error) {
			defer func() { verified <- err }()
			result, err := receiveResult(stream, nil, nil)
			if err != nil {
				return err
			}
			if result.CommandId != command.CommandId || result.Status != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE || result.Detail != "node_restarted" {
				return fmt.Errorf("recovered result: %v", result)
			}
			if !acknowledge {
				return nil
			}
			if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_CommandAck{CommandAck: &pb.CommandAck{
				CommandId: result.CommandId, Status: result.Status,
			}}}); err != nil {
				return err
			}
			// A subsequent duplicate round trip ensures the preceding ack was processed.
			if err := sendProbe(stream, command); err != nil {
				return err
			}
			replayed, err := receiveResult(stream, nil, nil)
			if err != nil {
				return err
			}
			if !proto.Equal(result, replayed) {
				return errors.New("recovered claim executed again")
			}
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, sessionErr := RunSession(ctx, sessionClient(t, api), options)
		cancel()
		select {
		case err := <-verified:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("recovery script did not finish: %v", sessionErr)
		}
	}
	journal, err = state.OpenNodeJournal(context.Background(), path, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	pending, err := journal.PendingResults(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("matching ack not durable: %v %v", pending, err)
	}
	result, claimed, err := journal.Claim(context.Background(), command)
	if err != nil || claimed || result.GetDetail() != "node_restarted" {
		t.Fatalf("tombstone missing: %v %v", result, err)
	}
}

func TestSessionOldServerNeverAdvertisesCommandReadiness(t *testing.T) {
	journal := testJournal(t)
	verified := make(chan error, 1)
	api := &scriptedProbeServer{}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		envelope, err := stream.Recv()
		if err != nil {
			return err
		}
		if h := envelope.GetHeartbeat(); h == nil || h.CommandReady {
			return errors.New("old server advertised readiness")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runSessionWithJournal(ctx, sessionClient(t, api), Options{InstanceID: "test-node"}, journal,
		func() { t.Error("old server executed probe") })
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("old-server script incomplete: %v", err)
	}
}

func TestSessionFailedCompletionNeverSendsSuccess(t *testing.T) {
	journal := failingJournal{commandJournal: testJournal(t), completeErr: errors.New("disk write failed")}
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability}}
	results := make(chan *pb.CommandResult, 1)
	api.script = func(stream probeStream, _ *pb.Hello) error {
		if err := sendProbe(stream, probe()); err != nil {
			return err
		}

		result, err := receiveResult(stream, nil, nil)
		results <- result
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runSessionWithJournal(ctx, sessionClient(t, api), Options{InstanceID: "test-node"}, journal, nil)
	if !isPermanentSessionError(err) {
		t.Fatalf("write failure not fatal: %v", err)
	}
	select {
	case result := <-results:
		if result != nil {
			t.Fatalf("uncommitted result sent: %v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server receive did not cancel after fatal completion failure")
	}
}

func TestSessionPendingReplayDoesNotStarveHeartbeatOrAcknowledgements(t *testing.T) {
	journal := testJournal(t)
	const count = 48
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for range count {
		command := probe()
		if _, claimed, err := journal.Claim(ctx, command); err != nil || !claimed {
			t.Fatalf("seed claim: %v", err)
		}
		if err := journal.Complete(ctx, &pb.CommandResult{CommandId: command.CommandId, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}); err != nil {
			t.Fatal(err)
		}
	}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability}}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		heartbeats := 0
		for range count {
			result, err := receiveResult(stream, func(*pb.Heartbeat) { heartbeats++ }, nil)
			if err != nil {
				return err
			}
			if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_CommandAck{CommandAck: &pb.CommandAck{
				CommandId: result.CommandId, Status: result.Status,
			}}}); err != nil {
				return err
			}
		}
		if heartbeats < 3 {
			return fmt.Errorf("replay starved heartbeats: got %d", heartbeats)
		}
		// This final command is an ordering barrier behind all replay acknowledgements.
		if err := sendProbe(stream, probe()); err != nil {
			return err
		}
		_, err = receiveResult(stream, nil, nil)
		return err
	}
	_, err := runSessionWithJournal(ctx, sessionClient(t, api), Options{InstanceID: "test-node", HeartbeatInterval: 10 * time.Millisecond}, journal, nil)
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("replay script incomplete: %v", err)
	}
	pending, err := journal.PendingResults(context.Background())
	if err != nil || len(pending) != 1 {
		t.Fatalf("replay acknowledgements starved: pending=%d err=%v", len(pending), err)
	}
}
