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
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func workspaceOptions(t *testing.T) Options {
	t.Helper()
	return Options{InstanceID: "test-node", WorkspacePolicy: workspacePolicy(t),
		CommandJournalPath: filepath.Join(t.TempDir(), "node.db"), RequiredServerTag: DefaultRequiredServerTag,
		HerdrSocket: filepath.Join(t.TempDir(), "absent.sock"), HeartbeatInterval: 10 * time.Millisecond,
		VerifyServerPeer: func(_ context.Context, address string) error {
			if address != "bufconn" {
				return fmt.Errorf("expected actual bufconn stream peer, got %q", address)
			}
			return nil
		}}
}

func workspaceCapabilities() []string {
	return []string{protocol.ProbeCapability, protocol.WorkspaceEnsureCapability, protocol.HerdrReadCapability}
}

func TestSessionWorkspaceRefreshesActualPeerBeforeEachEffect(t *testing.T) {
	journal := testJournal(t)
	options := workspaceOptions(t)
	// A configured hostname must never reach WhoIs in place of the stream peer.
	options.ServerAddress = "must-not-identify-this-host:50052"
	first, second := workspaceCommand(), workspaceCommand()
	var claims, verifications, executions atomic.Int32
	var revoked atomic.Bool
	options.VerifyServerPeer = workspacePeerTagVerifier(func(ctx context.Context, address string) (transport.PeerIdentity, error) {
		count := verifications.Add(1)
		deadline, ok := ctx.Deadline()
		if address != "bufconn" || claims.Load() != count || !ok || time.Until(deadline) > workspacePeerVerificationTimeout {
			return transport.PeerIdentity{}, fmt.Errorf("invalid pre-effect refresh: address=%q claims=%d verifications=%d deadline=%v",
				address, claims.Load(), count, deadline)
		}
		if revoked.Load() {
			return transport.PeerIdentity{}, nil
		}
		return transport.PeerIdentity{Tags: []string{DefaultRequiredServerTag}}, nil
	}, options.RequiredServerTag)
	observed := observedJournal{commandJournal: journal, afterClaim: func() { claims.Add(1) }}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if err := sendProbe(stream, first); err != nil {
			return err
		}
		original, err := receiveResult(stream, nil, nil)
		if err != nil || original.GetDetail() != "workspace_created" {
			return fmt.Errorf("authorized workspace: %v %v", original, err)
		}
		revoked.Store(true)
		if err := sendProbe(stream, second); err != nil {
			return err
		}
		rejection, err := receiveResult(stream, nil, nil)
		if err != nil || rejection.GetStatus() != pb.CommandStatus_COMMAND_STATUS_REJECTED || rejection.GetDetail() != "precondition_failed" {
			return fmt.Errorf("revoked peer was not rejected: %v %v", rejection, err)
		}
		stored, claimed, err := journal.Claim(context.Background(), second)
		if err != nil || claimed || !proto.Equal(stored, rejection) {
			return fmt.Errorf("authorization failure sent before durable completion: %v %v", stored, err)
		}
		if err := sendProbe(stream, first); err != nil {
			return err
		}
		replayed, err := receiveResult(stream, nil, nil)
		if err != nil || !proto.Equal(original, replayed) {
			return fmt.Errorf("known result changed after coordinator revocation: %v %v", replayed, err)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := runSessionWithExecutor(ctx, sessionClient(t, api), options, observed, nil,
		func(_ context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			executions.Add(1)
			if verifications.Load() != 1 || claims.Load() != 1 || revoked.Load() {
				return nil, errors.New("executor reached without current coordinator authorization")
			}
			return workspaceSuccess(binding, true), nil
		})
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("peer refresh script incomplete: %v", err)
	}
	if claims.Load() != 2 || verifications.Load() != 2 || executions.Load() != 1 {
		t.Fatalf("unexpected claim/refresh/effect counts: %d/%d/%d", claims.Load(), verifications.Load(), executions.Load())
	}
}

func TestSessionWorkspaceRequiresServerCapability(t *testing.T) {
	api := &sessionServer{capabilities: []string{protocol.ProbeCapability, protocol.HerdrReadCapability},
		messages: make(chan *pb.NodeEnvelope, 8)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registered, err := RunSession(ctx, sessionClient(t, api), workspaceOptions(t))
	if registered || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing workspace capability not permanently rejected: %v", err)
	}
}

func TestSessionUnnegotiatedWorkspaceNeverClaims(t *testing.T) {
	journal := testJournal(t)
	api := &scriptedProbeServer{capabilities: []string{protocol.ProbeCapability}}
	api.script = func(stream probeStream, hello *pb.Hello) error {
		if slices.Contains(hello.Capabilities, protocol.WorkspaceEnsureCapability) {
			return errors.New("absent policy advertised workspace support")
		}
		if err := sendProbe(stream, workspaceCommand()); err != nil {
			return err
		}
		_, err := receiveResult(stream, nil, nil)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runSessionWithJournal(ctx, sessionClient(t, api), Options{InstanceID: "test-node"}, journal, nil)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unnegotiated workspace accepted: %v", err)
	}
	results, err := journal.PendingResults(context.Background())
	if err != nil || len(results) != 0 {
		t.Fatalf("unnegotiated workspace claimed: %v %v", results, err)
	}
}

func TestSessionWorkspaceWorkerKeepsHeartbeatReplayAckAndSnapshotFlowing(t *testing.T) {
	journal := testJournal(t)
	seed := probe()
	if _, claimed, err := journal.Claim(context.Background(), seed); err != nil || !claimed {
		t.Fatalf("seed: %v", err)
	}
	if err := journal.Complete(context.Background(), &pb.CommandResult{CommandId: seed.CommandId,
		Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}); err != nil {
		t.Fatal(err)
	}
	first, second, expired := workspaceCommand(), workspaceCommand(), probe()
	expired.Ttl = durationpb.New(20 * time.Millisecond)
	started := make(chan struct{})
	release := make(chan struct{})
	var active, calls atomic.Int32
	var overlapped atomic.Bool
	execute := func(ctx context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		if active.Add(1) != 1 {
			overlapped.Store(true)
		}
		defer active.Add(-1)
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return workspaceSuccess(binding, false), nil
	}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	api.script = func(stream probeStream, hello *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if !slices.Contains(hello.Capabilities, protocol.WorkspaceEnsureCapability) {
			return errors.New("workspace capability not advertised")
		}
		for _, command := range []*pb.Command{first, second, expired} {
			if err := sendProbe(stream, command); err != nil {
				return err
			}
		}
		select {
		case <-started:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		heartbeats, observations := 0, 0
		replayed := false
		for heartbeats < 8 || observations == 0 || !replayed {
			envelope, err := stream.Recv()
			if err != nil {
				return err
			}
			if heartbeat := envelope.GetHeartbeat(); heartbeat != nil {
				heartbeats++
				if !heartbeat.CommandReady || heartbeat.WorkspaceReady {
					return errors.New("probe readiness changed or unavailable Herdr marked workspace ready")
				}
			}
			if state := envelope.GetHerdrState(); state != nil {
				observations++
			}
			if result := envelope.GetCommandResult(); result != nil {
				if result.CommandId != seed.CommandId {
					return errors.New("blocked executor returned prematurely")
				}
				replayed = true
				if err := stream.Send(&pb.NodeEnvelope{Body: &pb.NodeEnvelope_CommandAck{CommandAck: &pb.CommandAck{
					CommandId: result.CommandId, Status: result.Status,
				}}}); err != nil {
					return err
				}
			}
		}
		pending, err := journal.PendingResults(context.Background())
		if err != nil || len(pending) != 0 {
			return fmt.Errorf("ack starved by mutation: pending=%d err=%v", len(pending), err)
		}
		close(release)
		for _, command := range []*pb.Command{first, second, expired} {
			result, err := receiveResult(stream, nil, nil)
			if err != nil {
				return err
			}
			if result.CommandId != command.CommandId || protocol.ValidateResultForCommand(result, command) != nil {
				return fmt.Errorf("serialized result mismatch: %v", result)
			}
			if command == expired && result.Detail != "deadline_expired" {
				return errors.New("queued command lost arrival TTL")
			}
			if command != expired && result.Detail != "workspace_present" {
				return fmt.Errorf("workspace failed: %v", result)
			}
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := runSessionWithExecutor(ctx, sessionClient(t, api), workspaceOptions(t), journal,
		func() { t.Error("expired queued probe executed") }, execute)
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("session script incomplete: %v", err)
	}
	if calls.Load() != 2 || overlapped.Load() || active.Load() != 0 {
		t.Fatalf("worker not serialized/joined: calls=%d active=%d overlap=%v", calls.Load(), active.Load(), overlapped.Load())
	}
}

func TestSessionWorkspaceCancellationRecoversBeforeReconnectWithoutReexecution(t *testing.T) {
	journal := testJournal(t)
	options := workspaceOptions(t)
	command := workspaceCommand()
	var calls atomic.Int32
	started := make(chan struct{})
	exited := make(chan struct{})
	execute := func(ctx context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		calls.Add(1)
		close(started)
		defer close(exited)
		<-ctx.Done()
		// A success racing stream cancellation must still be recovered uncertain.
		return workspaceSuccess(binding, true), nil
	}
	api := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) error {
		if err := sendProbe(stream, command); err != nil {
			return err
		}
		select {
		case <-started:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := runSessionWithExecutor(ctx, sessionClient(t, api), options, journal, nil, execute); err == nil {
		t.Fatal("closed stream succeeded")
	}
	select {
	case <-exited:
	default:
		t.Fatal("session returned before worker joined")
	}
	options.WorkspacePolicy = nil
	verified := make(chan error, 1)
	reconnect := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	reconnect.script = func(stream probeStream, hello *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if slices.Contains(hello.Capabilities, protocol.WorkspaceEnsureCapability) {
			return errors.New("removed policy still advertised")
		}
		result, err := receiveResult(stream, nil, nil)
		if err != nil {
			return err
		}
		if result.CommandId != command.CommandId || result.Status != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE || result.Detail != "node_restarted" {
			return fmt.Errorf("canceled mutation was not recovered: %v", result)
		}
		if err := sendProbe(stream, command); err != nil {
			return err
		}
		duplicate, err := receiveResult(stream, nil, nil)
		if err != nil || !proto.Equal(result, duplicate) {
			return fmt.Errorf("duplicate recovery changed: %v %v", duplicate, err)
		}
		return nil
	}
	_, err := runSessionWithExecutor(ctx, sessionClient(t, reconnect), options, journal, nil, execute)
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("recovery script incomplete: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("uncertain mutation executed %d times", calls.Load())
	}
}

func TestSessionWorkspaceQueueOverloadCancelsAndJoinsWorker(t *testing.T) {
	journal := testJournal(t)
	started := make(chan struct{})
	var exited atomic.Bool
	command := workspaceCommand()
	api := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) error {
		if err := sendProbe(stream, command); err != nil {
			return err
		}
		select {
		case <-started:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		for range 20 {
			if err := sendProbe(stream, workspaceCommand()); err != nil {
				return err
			}
		}
		_, err := receiveResult(stream, nil, nil)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := runSessionWithExecutor(ctx, sessionClient(t, api), workspaceOptions(t), journal, nil,
		func(ctx context.Context, _ herdr.Config, _ projects.Binding) (*pb.WorkspaceEnsureResult, error) {
			close(started)
			<-ctx.Done()
			exited.Store(true)
			return nil, herdr.ErrWorkspaceIndeterminate
		})
	if status.Code(err) != codes.ResourceExhausted || !exited.Load() {
		t.Fatalf("overload did not restart and join: %v exited=%v", err, exited.Load())
	}
	if err := journal.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, claimed, err := journal.Claim(context.Background(), command)
	if err != nil || claimed || result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE {
		t.Fatalf("overload lost durable intent: %v %v", result, err)
	}
}
