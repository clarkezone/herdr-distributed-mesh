package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandHarness struct {
	api        *service
	connection *grpc.ClientConn
	stop       func()
}

func commandPeer(ctx context.Context, peer string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "test-peer", peer)
}

func newCommandHarness(t *testing.T, root string, options ...grpc.ServerOption) *commandHarness {
	t.Helper()
	store, views, err := openCoordinatorState(context.Background(), Options{InstanceID: "server-1", DatabasePath: filepath.Join(root, "coordinator", "mesh.db")})
	if err != nil {
		t.Fatal(err)
	}
	api := &service{instanceID: "server-1", requiredClientTag: "tag:client", requiredCommandTag: "tag:client", requiredNodeTag: "tag:node", commands: store, bindNode: durableBinder(store)}
	api.identifyPeer = func(ctx context.Context) (transport.PeerIdentity, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("test-peer")
		if len(values) != 1 {
			return transport.PeerIdentity{}, errors.New("missing test identity")
		}
		switch values[0] {
		case "node":
			return transport.PeerIdentity{StableID: "stable-1", Tags: []string{"tag:node"}}, nil
		case "node2":
			return transport.PeerIdentity{StableID: "stable-2", Tags: []string{"tag:node"}}, nil
		case "client":
			return transport.PeerIdentity{StableID: "client-1", Tags: []string{"tag:client"}}, nil
		case "client2":
			return transport.PeerIdentity{StableID: "client-2", Tags: []string{"tag:client"}}, nil
		default:
			return transport.PeerIdentity{StableID: "denied", Tags: []string{"tag:observer"}}, nil
		}
	}
	api.fleet.storage = store
	api.fleet.commands = store
	api.fleet.fatal = make(chan error, 1)
	if err := api.fleet.restore(views, time.Now()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(append(options, grpc.WaitForHandlers(true))...)
	agentflowv1.RegisterFleetServer(grpcServer, api)
	agentflowv1.RegisterNodeControlServer(grpcServer, api)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveCoordinator(ctx, listener, grpcServer, &api.fleet) }()
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		cancel()
		<-done
		store.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("stop command coordinator: %v", err)
			}
			connection.Close()
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return &commandHarness{api: api, connection: connection, stop: stop}
}

type countedNodeClient struct {
	agentflowv1.NodeControlClient
	count      atomic.Int32
	dropResult atomic.Bool
}
type countedNodeStream struct {
	grpc.BidiStreamingClient[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope]
	owner *countedNodeClient
}

func (c *countedNodeClient) Connect(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope], error) {
	stream, err := c.NodeControlClient.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &countedNodeStream{BidiStreamingClient: stream, owner: c}, nil
}
func (s *countedNodeStream) Recv() (*agentflowv1.NodeEnvelope, error) {
	message, err := s.BidiStreamingClient.Recv()
	if message.GetCommand() != nil {
		s.owner.count.Add(1)
	}
	return message, err
}
func (s *countedNodeStream) Send(message *agentflowv1.NodeEnvelope) error {
	if message.GetCommandResult() != nil && s.owner.dropResult.CompareAndSwap(true, false) {
		return status.Error(codes.Unavailable, "simulated result loss after node journal commit")
	}
	return s.BidiStreamingClient.Send(message)
}

func startJournaledNode(t *testing.T, h *commandHarness, path string, drop bool) (*countedNodeClient, <-chan error, func()) {
	t.Helper()
	client := &countedNodeClient{NodeControlClient: agentflowv1.NewNodeControlClient(h.connection)}
	client.dropResult.Store(drop)
	ctx, cancel := context.WithCancel(commandPeer(context.Background(), "node"))
	done := make(chan error, 1)
	go func() {
		_, err := node.RunSession(ctx, client, node.Options{InstanceID: "node-1", HeartbeatInterval: 50 * time.Millisecond,
			CommandJournalPath: path, RequiredServerTag: node.DefaultRequiredServerTag})
		done <- err
	}()
	t.Cleanup(cancel)
	waitProbeReady(t, h)
	return client, done, cancel
}

func waitProbeReady(t *testing.T, h *commandHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	for {
		list, err := agentflowv1.NewFleetClient(h.connection).ListNodes(ctx, &emptypb.Empty{})
		if err == nil && len(list.Nodes) > 0 && list.Nodes[0].CommandReady {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("node did not become command-ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func submitProbe(t *testing.T, h *commandHarness, key string, ttl time.Duration) *agentflowv1.CommandRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	record, err := agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, &agentflowv1.SubmitCommandRequest{
		NodeInstanceId: "node-1", IdempotencyKey: key, CommandType: protocol.ProbeCommandType, Ttl: durationpb.New(ttl)})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func awaitCommand(t *testing.T, h *commandHarness, id string, want agentflowv1.CommandStatus) *agentflowv1.CommandRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	for {
		record, err := agentflowv1.NewFleetClient(h.connection).GetCommand(ctx, &agentflowv1.GetCommandRequest{CommandId: id})
		if err != nil {
			t.Fatal(err)
		}
		if record.Status == want {
			return record
		}
		select {
		case <-ctx.Done():
			t.Fatalf("command status=%s, want %s", record.Status, want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRealNodeProbeIsAuthorizedJournaledAndIdempotent(t *testing.T) {
	root := t.TempDir()
	h := newCommandHarness(t, filepath.Join(root, "server"))
	client, done, cancel := startJournaledNode(t, h, filepath.Join(root, "node", "journal.db"), false)
	record := submitProbe(t, h, "same-key", 5*time.Second)
	id := record.Command.CommandId
	completed := awaitCommand(t, h, id, agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if completed.Command.Actor.ActorId != "client-1" || completed.Detail != "pong" || len(completed.Audit) < 3 {
		t.Fatal("missing authenticated actor or durable state-transition audit")
	}
	retried := submitProbe(t, h, "same-key", 5*time.Second)
	if retried.Command.CommandId != id || len(retried.Audit) != len(completed.Audit) || client.count.Load() != 1 {
		t.Fatal("idempotent retry created another execution or audit history")
	}
	request := &agentflowv1.SubmitCommandRequest{NodeInstanceId: "node-1", IdempotencyKey: "same-key", CommandType: protocol.ProbeCommandType, Ttl: durationpb.New(6 * time.Second)}
	ctx := commandPeer(context.Background(), "client")
	_, err := agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, request)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting retry accepted: %v", err)
	}
	_, err = agentflowv1.NewFleetClient(h.connection).GetCommand(commandPeer(context.Background(), "client2"), &agentflowv1.GetCommandRequest{CommandId: id})
	if status.Code(err) != codes.NotFound {
		t.Fatal("different actor read private command history")
	}
	_, err = agentflowv1.NewFleetClient(h.connection).SubmitCommand(commandPeer(context.Background(), "node"), request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("node role admitted a command")
	}
	request.IdempotencyKey = "mutation"
	request.CommandType = "workspace.create"
	_, err = agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, request)
	if status.Code(err) != codes.PermissionDenied || client.count.Load() != 1 {
		t.Fatal("mutation reached node")
	}
	request.CommandType = protocol.ProbeCommandType
	request.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1})
	_, err = agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, request)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("unknown/actor-spoof fields accepted")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("node did not stop")
	}
	h.stop()
	reopened := newCommandHarness(t, filepath.Join(root, "server"))
	persisted := awaitCommand(t, reopened, id, agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if len(persisted.Audit) != len(completed.Audit) {
		t.Fatal("restart changed completed audit")
	}
}

func TestCommittedNodeResultReconcilesAfterBothSidesRestart(t *testing.T) {
	root := t.TempDir()
	h := newCommandHarness(t, filepath.Join(root, "server"))
	path := filepath.Join(root, "node", "journal.db")
	first, done, cancel := startJournaledNode(t, h, path, true)
	record := submitProbe(t, h, "lost-result", 10*time.Second)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("simulated result loss did not end session")
	}
	cancel()
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE)
	if first.count.Load() != 1 {
		t.Fatal("probe not dispatched once")
	}
	h.stop()
	reopened := newCommandHarness(t, filepath.Join(root, "server"))
	second, secondDone, secondCancel := startJournaledNode(t, reopened, path, false)
	reconciled := awaitCommand(t, reopened, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED)
	if reconciled.Detail != "pong" || second.count.Load() != 0 {
		t.Fatal("recovery re-executed instead of replaying durable result")
	}
	secondCancel()
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("recovered node did not stop")
	}
}

func startManualProbeNode(t *testing.T, h *commandHarness, ctx context.Context) grpc.BidiStreamingClient[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope] {
	t.Helper()
	stream, err := agentflowv1.NewNodeControlClient(h.connection).Connect(commandPeer(ctx, "node"))
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Capabilities = []string{protocol.ProbeCapability}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_Heartbeat{Heartbeat: &agentflowv1.Heartbeat{
		Sequence: 1, SentAt: timestamppb.Now(), CommandReady: true}}}); err != nil {
		t.Fatal(err)
	}
	waitProbeReady(t, h)
	return stream
}

func TestDispatchedExpiryIsIndeterminateUntilKnownResult(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := startManualProbeNode(t, h, ctx)
	record := submitProbe(t, h, "expiry", time.Second)
	message, err := stream.Recv()
	if err != nil || message.GetCommand() == nil {
		t.Fatal("probe not dispatched", err)
	}
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE)
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: &agentflowv1.CommandResult{
		CommandId: record.Command.CommandId, Status: agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}}}); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || ack.GetCommandAck().GetStatus() != agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatal("late result not committed/acknowledged", err)
	}
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED)
}

func TestCanceledCommandReadDoesNotStopCoordinator(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(), metadata.Pairs("test-peer", "client")))
	cancel()
	_, readErr := h.api.GetCommand(ctx, &agentflowv1.GetCommandRequest{CommandId: protocol.NewCommandID()})
	if status.Code(readErr) != codes.Canceled {
		t.Fatalf("canceled storage read returned %v", readErr)
	}
	h.api.fleet.mu.Lock()
	err := h.api.fleet.storageErr
	h.api.fleet.mu.Unlock()
	if err != nil {
		t.Fatal("caller cancellation poisoned coordinator storage")
	}
}

func TestLegacyNodeCannotClaimProbeReadiness(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	if err := h.api.bindNode("stable-1", "node-1"); err != nil {
		t.Fatal(err)
	}
	entry, err := h.api.fleet.begin("node-1", "stable-1", false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.api.fleet.heartbeat(entry, time.Now(), true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client"), 5*time.Second)
	defer cancel()
	client := agentflowv1.NewFleetClient(h.connection)
	list, err := client.ListNodes(ctx, &emptypb.Empty{})
	if err != nil || len(list.Nodes) != 1 || !list.Nodes[0].Connected || list.Nodes[0].CommandReady {
		t.Fatal("legacy node monitoring/readiness changed", err)
	}
	_, err = client.SubmitCommand(ctx, &agentflowv1.SubmitCommandRequest{
		NodeInstanceId: "node-1", IdempotencyKey: "legacy", CommandType: protocol.ProbeCommandType})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy node admitted probe: %v", err)
	}
}

func TestForeignNodeCannotCompleteProbe(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owner := startManualProbeNode(t, h, ctx)
	record := submitProbe(t, h, "owner-only", 5*time.Second)
	if message, err := owner.Recv(); err != nil || message.GetCommand() == nil {
		t.Fatal("owner did not receive command", err)
	}
	foreign, err := agentflowv1.NewNodeControlClient(h.connection).Connect(commandPeer(ctx, "node2"))
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-2")
	hello.GetHello().Capabilities = []string{protocol.ProbeCapability}
	if err := foreign.Send(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Recv(); err != nil {
		t.Fatal(err)
	}
	result := &agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: &agentflowv1.CommandResult{
		CommandId: record.Command.CommandId, Status: agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED, Detail: "pong"}}}
	if err := foreign.Send(result); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("foreign result not rejected: %v", err)
	}
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_RUNNING)
	if err := owner.Send(result); err != nil {
		t.Fatal(err)
	}
	if ack, err := owner.Recv(); err != nil || ack.GetCommandAck().GetStatus() != agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED {
		t.Fatal("owner result rejected", err)
	}
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_SUCCEEDED)
}

func TestProbeQueueBoundAndActorScopedKeys(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	if err := h.api.bindNode("stable-1", "node-1"); err != nil {
		t.Fatal(err)
	}
	entry, err := h.api.fleet.begin("node-1", "stable-1", false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.api.fleet.mu.Lock()
	entry.probes = true
	h.api.fleet.mu.Unlock()
	if err := h.api.fleet.heartbeat(entry, time.Now(), true); err != nil {
		t.Fatal(err)
	}
	first := submitProbe(t, h, "actor-key", 30*time.Second)
	ctx, cancel := context.WithTimeout(commandPeer(context.Background(), "client2"), 5*time.Second)
	defer cancel()
	second, err := agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, &agentflowv1.SubmitCommandRequest{
		NodeInstanceId: "node-1", IdempotencyKey: "actor-key", CommandType: protocol.ProbeCommandType, Ttl: durationpb.New(30 * time.Second)})
	if err != nil || second.Command.CommandId == first.Command.CommandId || second.Command.Actor.ActorId != "client-2" {
		t.Fatal("different actors shared an idempotency namespace", err)
	}
	for i := 2; i < 16; i++ {
		submitProbe(t, h, fmt.Sprintf("queued-%d", i), 30*time.Second)
	}
	_, err = agentflowv1.NewFleetClient(h.connection).SubmitCommand(ctx, &agentflowv1.SubmitCommandRequest{
		NodeInstanceId: "node-1", IdempotencyKey: "overflow", CommandType: protocol.ProbeCommandType})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("seventeenth queued command admitted: %v", err)
	}
	retried := submitProbe(t, h, "actor-key", 30*time.Second)
	if retried.Command.CommandId != first.Command.CommandId || len(entry.outbound) != 16 {
		t.Fatal("full queue prevented lookup or duplicated admission")
	}
}

func TestCommandTrafficCannotReplaceHeartbeat(t *testing.T) {
	h := newCommandHarness(t, t.TempDir())
	h.api.heartbeatTimeout = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(h.connection).Connect(commandPeer(ctx, "node"))
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Capabilities = []string{protocol.ProbeCapability}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	var sent atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandAck{CommandAck: &agentflowv1.CommandAck{
				CommandId: protocol.NewCommandID(), Status: agentflowv1.CommandStatus_COMMAND_STATUS_ACCEPTED}}})
			if err != nil {
				return
			}
			sent.Add(1)
		}
	}()
	_, err = stream.Recv()
	cancel()
	<-done
	if status.Code(err) != codes.DeadlineExceeded || sent.Load() == 0 || ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("command traffic hid heartbeat deadline: sent=%d, error=%v", sent.Load(), err)
	}
}

type blockedCommandSend struct {
	grpc.ServerStream
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (s *blockedCommandSend) SendMsg(message any) error {
	if envelope, ok := message.(*agentflowv1.NodeEnvelope); ok && envelope.GetCommand() != nil {
		s.once.Do(func() { close(s.started) })
		<-s.Context().Done()
		close(s.stopped)
		return s.Context().Err()
	}
	return s.ServerStream.SendMsg(message)
}

func TestReplacementTerminatesBlockedOldCommandSend(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	h := newCommandHarness(t, t.TempDir(), grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(service, &blockedCommandSend{ServerStream: stream, started: started, stopped: stopped})
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old := startManualProbeNode(t, h, ctx)
	record := submitProbe(t, h, "blocked-send", 5*time.Second)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("old command send did not start")
	}
	startManualProbeNode(t, h, ctx)
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("old command send survived replacement")
	}
	if message, err := old.Recv(); status.Code(err) != codes.Aborted || message.GetCommand() != nil {
		t.Fatalf("superseded stream delivered command: %v, %v", message, err)
	}
	awaitCommand(t, h, record.Command.CommandId, agentflowv1.CommandStatus_COMMAND_STATUS_INDETERMINATE)
}

func TestReplacementWaitsForRegisteredSendsAfterTransportCancellation(t *testing.T) {
	var fleet fleetStore
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old, err := fleet.beginSession(ctx, "node-1", "stable-1", false, time.Now())
	if err != nil || !old.registerSend() {
		t.Fatal("could not register old send", err)
	}
	var once sync.Once
	finish := func() { once.Do(old.sends.Done) }
	t.Cleanup(finish)
	cancel()
	replacementCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := fleet.beginSession(replacementCtx, "node-1", "stable-1", false, time.Now())
		result <- err
	}()
	select {
	case <-old.done:
	case <-replacementCtx.Done():
		t.Fatal("old entry not retired")
	}
	if old.registerSend() {
		old.sends.Done()
		t.Fatal("retired entry accepted another send")
	}
	select {
	case err := <-result:
		t.Fatalf("replacement published before registered send completed: %v", err)
	default:
	}
	fleet.mu.Lock()
	current := fleet.nodes["node-1"]
	fleet.mu.Unlock()
	if current != old {
		t.Fatal("old transport cancellation bypassed send barrier")
	}
	finish()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-replacementCtx.Done():
		t.Fatal("replacement failed to publish after send completion")
	}
}
