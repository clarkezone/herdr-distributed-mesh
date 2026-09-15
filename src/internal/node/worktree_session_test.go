package node

import (
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func worktreeOptions(t *testing.T) Options {
	t.Helper()
	options := workspaceOptions(t)
	options.WorkspacePolicy = worktreePolicy(t)
	return options
}

func worktreeCapabilities() []string {
	return append(workspaceCapabilities(), protocol.WorktreeCreateCapability)
}

func TestSessionWorktreeAdvertisementAndDefaultDisabled(t *testing.T) {
	for _, name := range []string{"enabled", "no-policy", "default-disabled", "no-journal", "no-socket"} {
		t.Run(name, func(t *testing.T) {
			options := worktreeOptions(t)
			var journal commandJournal = testJournal(t)
			switch name {
			case "no-policy":
				options.WorkspacePolicy = nil
			case "default-disabled":
				options.WorkspacePolicy = workspacePolicy(t)
			case "no-journal":
				journal = nil
			case "no-socket":
				options.HerdrSocket = ""
			}
			verified := make(chan error, 1)
			api := &scriptedProbeServer{capabilities: worktreeCapabilities()}
			api.script = func(stream probeStream, hello *pb.Hello) (err error) {
				defer func() { verified <- err }()
				if got := slices.Contains(hello.Capabilities, protocol.WorktreeCreateCapability); got != (name == "enabled") {
					return fmt.Errorf("worktree advertised=%v case=%s", got, name)
				}
				if name == "no-journal" {
					return nil
				}
				for {
					envelope, err := stream.Recv()
					if err != nil {
						return err
					}
					if heartbeat := envelope.GetHeartbeat(); heartbeat != nil {
						if heartbeat.WorktreeReady {
							return errors.New("missing fresh baseline marked worktree ready")
						}
						return nil
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := runSessionWithJournal(ctx, sessionClient(t, api), options, journal, nil)
			select {
			case scriptErr := <-verified:
				if scriptErr != nil {
					t.Fatal(scriptErr)
				}
			case <-ctx.Done():
				t.Fatalf("advertisement script incomplete: %v", err)
			}
			if name == "no-journal" && status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("policy without journal accepted: %v", err)
			}
		})
	}
}

func TestSessionWorktreeRequiresServerCapability(t *testing.T) {
	api := &sessionServer{capabilities: workspaceCapabilities(), messages: make(chan *pb.NodeEnvelope, 8)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registered, err := RunSession(ctx, sessionClient(t, api), worktreeOptions(t))
	if registered || status.Code(err) != codes.FailedPrecondition || !isPermanentSessionError(err) {
		t.Fatalf("worktree capability silently downgraded: registered=%v err=%v", registered, err)
	}
}

func TestSessionUnnegotiatedWorktreeNeverClaims(t *testing.T) {
	journal := testJournal(t)
	api := &scriptedProbeServer{capabilities: workspaceCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) error {
		if err := sendProbe(stream, worktreeCommand()); err != nil {
			return err
		}
		_, err := receiveResult(stream, nil, nil)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := runSessionWithJournal(ctx, sessionClient(t, api), workspaceOptions(t), journal, nil)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unnegotiated worktree accepted: %v", err)
	}
	results, err := journal.PendingResults(context.Background())
	if err != nil || len(results) != 0 {
		t.Fatalf("unnegotiated worktree claimed: %v %v", results, err)
	}
}

func TestWorktreeReadinessRequiresOptInNegotiationCommandsAndFreshWorkspace(t *testing.T) {
	policy := worktreePolicy(t)
	now := time.Now()
	for _, name := range []string{"ready", "no-policy", "default-disabled", "no-worktree", "no-workspace", "no-commands", "missing", "stale", "future", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			handler := &commandHandler{workspacePolicy: policy, workspaceNegotiated: true, worktreeNegotiated: true}
			local := &pb.HerdrState{Status: "ready", ObservedAt: timestamppb.New(now)}
			commandReady := true
			switch name {
			case "no-policy":
				handler.workspacePolicy = nil
			case "default-disabled":
				handler.workspacePolicy = workspacePolicy(t)
			case "no-worktree":
				handler.worktreeNegotiated = false
			case "no-workspace":
				handler.workspaceNegotiated = false
			case "no-commands":
				commandReady = false
			case "missing":
				local = nil
			case "stale":
				local.ObservedAt = timestamppb.New(now.Add(-30*time.Second - time.Nanosecond))
			case "future":
				local.ObservedAt = timestamppb.New(now.Add(time.Nanosecond))
			case "unavailable":
				local.Status = "unavailable"
			}
			workspace, worktree := handler.mutationReadiness(local, now, commandReady)
			if worktree != (name == "ready") {
				t.Fatalf("worktree readiness=%v", worktree)
			}
			if (name == "no-worktree" || name == "default-disabled" || name == "no-commands") && !workspace {
				t.Fatal("existing workspace readiness changed")
			}
		})
	}
}

func TestSessionWorktreeRefreshesActualPeerAndDurablyRejectsRevocation(t *testing.T) {
	journal := testJournal(t)
	options := worktreeOptions(t)
	options.ServerAddress = "never-resolve-this-configured-host:50052"
	first, second := worktreeCommand(), worktreeCommand()
	var claims, verifications, effects atomic.Int32
	var revoked atomic.Bool
	options.VerifyServerPeer = workspacePeerTagVerifier(func(ctx context.Context, address string) (transport.PeerIdentity, error) {
		count := verifications.Add(1)
		deadline, ok := ctx.Deadline()
		if address != "bufconn" || count != claims.Load() || !ok || time.Until(deadline) > workspacePeerVerificationTimeout {
			return transport.PeerIdentity{}, errors.New("refresh missing actual peer, durable claim, or bounded deadline")
		}
		if revoked.Load() {
			return transport.PeerIdentity{}, nil
		}
		return transport.PeerIdentity{Tags: []string{DefaultRequiredServerTag}}, nil
	}, options.RequiredServerTag)
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: worktreeCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if err := sendProbe(stream, first); err != nil {
			return err
		}
		original, err := receiveResult(stream, nil, nil)
		if err != nil || original.GetDetail() != "worktree_created" {
			return fmt.Errorf("authorized worktree: %v %v", original, err)
		}
		revoked.Store(true)
		if err := sendProbe(stream, second); err != nil {
			return err
		}
		rejection, err := receiveResult(stream, nil, nil)
		if err != nil || rejection.GetDetail() != "precondition_failed" || rejection.GetStatus() != pb.CommandStatus_COMMAND_STATUS_REJECTED {
			return fmt.Errorf("revoked peer not rejected: %v %v", rejection, err)
		}
		stored, claimed, err := journal.Claim(context.Background(), second)
		if err != nil || claimed || !proto.Equal(stored, rejection) {
			return fmt.Errorf("rejection sent before durable completion: %v %v", stored, err)
		}
		if err := sendProbe(stream, first); err != nil {
			return err
		}
		replayed, err := receiveResult(stream, nil, nil)
		if err != nil || !proto.Equal(replayed, original) {
			return fmt.Errorf("revocation changed replay: %v %v", replayed, err)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := runSessionWithMutationExecutors(ctx, sessionClient(t, api), options,
		observedJournal{commandJournal: journal, afterClaim: func() { claims.Add(1) }}, nil, nil,
		func(_ context.Context, _ herdr.Config, _ projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
			effects.Add(1)
			return worktreeSuccess(request), nil
		})
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("peer refresh script incomplete: %v", err)
	}
	if claims.Load() != 2 || verifications.Load() != 2 || effects.Load() != 1 {
		t.Fatalf("claims/verifications/effects=%d/%d/%d", claims.Load(), verifications.Load(), effects.Load())
	}
}

func TestSessionWorktreeWorkerSerializesBothMutationsWithoutStarvingTraffic(t *testing.T) {
	journal := testJournal(t)
	seed := probe()
	if _, claimed, err := journal.Claim(context.Background(), seed); err != nil || !claimed {
		t.Fatalf("seed replay: %v", err)
	}
	if err := journal.Complete(context.Background(), &pb.CommandResult{CommandId: seed.CommandId,
		Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}); err != nil {
		t.Fatal(err)
	}
	first, second, third, expired := worktreeCommand(), workspaceCommand(), worktreeCommand(), worktreeCommand()
	expired.Ttl = durationpb.New(20 * time.Millisecond)
	started, release := make(chan struct{}), make(chan struct{})
	var active, worktreeCalls, workspaceCalls atomic.Int32
	var overlapped atomic.Bool
	enter := func() {
		if active.Add(1) != 1 {
			overlapped.Store(true)
		}
	}
	create := func(ctx context.Context, _ herdr.Config, _ projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
		enter()
		defer active.Add(-1)
		if worktreeCalls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, herdr.ErrWorkspaceIndeterminate
			}
		}
		return worktreeSuccess(request), nil
	}
	ensure := func(_ context.Context, _ herdr.Config, binding projects.Binding) (*pb.WorkspaceEnsureResult, error) {
		enter()
		defer active.Add(-1)
		workspaceCalls.Add(1)
		return workspaceSuccess(binding, false), nil
	}
	verified := make(chan error, 1)
	api := &scriptedProbeServer{capabilities: worktreeCapabilities()}
	api.script = func(stream probeStream, _ *pb.Hello) (err error) {
		defer func() { verified <- err }()
		for _, command := range []*pb.Command{first, second, third, expired} {
			if err := sendProbe(stream, command); err != nil {
				return err
			}
		}
		select {
		case <-started:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		heartbeats, snapshots := 0, 0
		replayed := false
		for heartbeats < 8 || snapshots == 0 || !replayed {
			envelope, err := stream.Recv()
			if err != nil {
				return err
			}
			if heartbeat := envelope.GetHeartbeat(); heartbeat != nil {
				heartbeats++
				if !heartbeat.CommandReady || heartbeat.WorkspaceReady || heartbeat.WorktreeReady {
					return errors.New("incorrect readiness during unavailable Herdr observation")
				}
			}
			if envelope.GetHerdrState() != nil {
				snapshots++
			}
			if result := envelope.GetCommandResult(); result != nil {
				if result.CommandId != seed.CommandId {
					return errors.New("blocked worktree or queued mutation returned prematurely")
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
			return fmt.Errorf("receipt ACK starved by worktree: pending=%d err=%v", len(pending), err)
		}
		close(release)
		for _, command := range []*pb.Command{first, second, third, expired} {
			result, err := receiveResult(stream, nil, nil)
			if err != nil || result.GetCommandId() != command.CommandId {
				return fmt.Errorf("serialized result mismatch: %v %v", result, err)
			}
			if err := protocol.ValidateResultForCommand(result, command); err != nil {
				return err
			}
			want := "worktree_created"
			if command == second {
				want = "workspace_present"
			} else if command == expired {
				want = "deadline_expired"
			}
			if result.Detail != want {
				return fmt.Errorf("result detail=%s want=%s", result.Detail, want)
			}
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := runSessionWithMutationExecutors(ctx, sessionClient(t, api), worktreeOptions(t), journal, nil, ensure, create)
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("serialization script incomplete: %v", err)
	}
	if overlapped.Load() || active.Load() != 0 || worktreeCalls.Load() != 2 || workspaceCalls.Load() != 1 {
		t.Fatalf("worker not serialized/joined: overlap=%v active=%d worktrees=%d workspaces=%d",
			overlapped.Load(), active.Load(), worktreeCalls.Load(), workspaceCalls.Load())
	}
}

func TestSessionWorktreeCancellationJoinsAndRecoversWithoutPolicy(t *testing.T) {
	journal := testJournal(t)
	options := worktreeOptions(t)
	command := worktreeCommand()
	started, exited := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	create := func(ctx context.Context, _ herdr.Config, _ projects.Binding, request *pb.WorktreeCreate) (*pb.WorktreeCreateResult, error) {
		calls.Add(1)
		close(started)
		defer close(exited)
		<-ctx.Done()
		return worktreeSuccess(request), nil
	}
	api := &scriptedProbeServer{capabilities: worktreeCapabilities()}
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
	if _, err := runSessionWithMutationExecutors(ctx, sessionClient(t, api), options, journal, nil, nil, create); err == nil {
		t.Fatal("closed stream succeeded")
	}
	select {
	case <-exited:
	default:
		t.Fatal("session returned before worktree executor joined")
	}
	options.WorkspacePolicy, options.VerifyServerPeer = nil, nil
	verified := make(chan error, 1)
	reconnect := &scriptedProbeServer{capabilities: worktreeCapabilities()}
	reconnect.script = func(stream probeStream, hello *pb.Hello) (err error) {
		defer func() { verified <- err }()
		if slices.Contains(hello.Capabilities, protocol.WorktreeCreateCapability) {
			return errors.New("removed policy advertised worktree")
		}
		result, err := receiveResult(stream, nil, nil)
		if err != nil || result.GetCommandId() != command.CommandId ||
			result.GetStatus() != pb.CommandStatus_COMMAND_STATUS_INDETERMINATE || result.GetDetail() != "node_restarted" {
			return fmt.Errorf("canceled worktree not recovered uncertain: %v %v", result, err)
		}
		if err := sendProbe(stream, command); err != nil {
			return err
		}
		replayed, err := receiveResult(stream, nil, nil)
		if err != nil || !proto.Equal(result, replayed) {
			return fmt.Errorf("recovered worktree replay changed: %v %v", replayed, err)
		}
		return nil
	}
	_, err := runSessionWithMutationExecutors(ctx, sessionClient(t, reconnect), options, journal, nil, nil, create)
	select {
	case scriptErr := <-verified:
		if scriptErr != nil {
			t.Fatal(scriptErr)
		}
	default:
		t.Fatalf("reconnect script incomplete: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled worktree executed %d times", calls.Load())
	}
}
