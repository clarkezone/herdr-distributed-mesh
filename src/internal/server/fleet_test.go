package server

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func readyState(sequence uint64) *agentflowv1.HerdrState {
	return &agentflowv1.HerdrState{
		Status: "ready", Version: "0.7.5-preview", Protocol: 18,
		Sequence: sequence, ObservedAt: timestamppb.Now(),
		Panes: []*agentflowv1.HerdrEntity{{Id: "pane-1", WorkspaceId: "ws-1", TabId: "tab-1", AgentStatus: "working"}},
	}
}

func listFleet(t *testing.T, fleet *fleetStore, now time.Time) *agentflowv1.NodeList {
	t.Helper()
	list, err := fleet.list(now)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func TestFleetLifecycleAndFencing(t *testing.T) {
	var fleet fleetStore
	now := time.Now()
	old, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := listFleet(t, &fleet, now).Nodes[0]; got.Herdr.Status != "waiting" || !got.Stale {
		t.Fatalf("new node must be waiting/stale: %v", got)
	}
	state := readyState(1)
	if err := fleet.update(old, state, now); err != nil {
		t.Fatal(err)
	}
	state.Panes[0].AgentStatus = "done"
	view := listFleet(t, &fleet, now).Nodes[0]
	if view.Stale || view.Herdr.Panes[0].AgentStatus != "working" {
		t.Fatalf("state must be fresh and cloned: %v", view)
	}
	view.Herdr.Panes[0].AgentStatus = "done"
	if listFleet(t, &fleet, now).Nodes[0].Herdr.Panes[0].AgentStatus != "working" {
		t.Fatal("caller mutated store")
	}
	if !listFleet(t, &fleet, now.Add(herdrStaleAfter+time.Second)).Nodes[0].Stale {
		t.Fatal("snapshot did not become stale")
	}
	if err := fleet.update(old, readyState(1), now); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("replayed sequence accepted: %v", err)
	}
	current, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("old stream not signaled")
	}
	if err := fleet.update(old, readyState(100), now); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded stream wrote state: %v", err)
	}
	if err := fleet.heartbeat(old, now); status.Code(err) != codes.Aborted {
		t.Fatalf("superseded heartbeat accepted: %v", err)
	}
	fleet.end(old)
	if !listFleet(t, &fleet, now).Nodes[0].Connected {
		t.Fatal("old disconnect affected replacement")
	}
	if err := fleet.update(current, readyState(1), now); err != nil {
		t.Fatalf("fresh stream must reset sequence: %v", err)
	}
	fleet.end(current)
	if got := listFleet(t, &fleet, now).Nodes[0]; got.Connected || !got.Stale {
		t.Fatal("disconnected node reported fresh")
	}
	if len(listFleet(t, &fleet, now.Add(offlineRetention+time.Second)).Nodes) != 0 {
		t.Fatal("offline retention not bounded")
	}
}

func TestFleetRejectsInvalidStates(t *testing.T) {
	tests := map[string]func(*agentflowv1.HerdrState){
		"empty sequence": func(s *agentflowv1.HerdrState) { s.Sequence = 0 },
		"missing time":   func(s *agentflowv1.HerdrState) { s.ObservedAt = nil },
		"invalid time":   func(s *agentflowv1.HerdrState) { s.ObservedAt.Seconds = 1 << 60 },
		"raw error":      func(s *agentflowv1.HerdrState) { s.ErrorCode = "local secret" },
		"raw version":    func(s *agentflowv1.HerdrState) { s.Version = "C:\\secret" },
		"invalid status": func(s *agentflowv1.HerdrState) { s.Panes[0].AgentStatus = "invented" },
		"raw id":         func(s *agentflowv1.HerdrState) { s.Panes[0].Id = "C:\\secret" },
		"large id":       func(s *agentflowv1.HerdrState) { s.Panes[0].Id = strings.Repeat("x", 129) },
		"nil entity":     func(s *agentflowv1.HerdrState) { s.Panes[0] = nil },
		"duplicate": func(s *agentflowv1.HerdrState) {
			s.Panes = append(s.Panes, proto.Clone(s.Panes[0]).(*agentflowv1.HerdrEntity))
		},
		"unavailable with data": func(s *agentflowv1.HerdrState) { s.Status = "unavailable"; s.ErrorCode = "offline" },
		"unexpected fields":     func(s *agentflowv1.HerdrState) { s.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := readyState(1)
			mutate(state)
			if err := validateHerdrState(state); err == nil {
				t.Fatal("invalid state accepted")
			}
		})
	}
	unavailable := &agentflowv1.HerdrState{Status: "unavailable", Sequence: 1, ObservedAt: timestamppb.Now(), ErrorCode: "connection_failed"}
	if err := validateHerdrState(unavailable); err != nil {
		t.Fatalf("sanitized failure rejected: %v", err)
	}
}

func TestFleetBounds(t *testing.T) {
	var fleet fleetStore
	now := time.Now()
	for i := 0; i < maxFleetNodes; i++ {
		if _, err := fleet.begin(fmt.Sprint(i), fmt.Sprint(i), false, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fleet.begin("extra", "extra", false, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unbounded node count: %v", err)
	}
	big := readyState(1)
	big.Panes = nil
	for i := 0; i < 4096; i++ {
		big.Panes = append(big.Panes, &agentflowv1.HerdrEntity{
			Id: fmt.Sprintf("%s%d", strings.Repeat("x", 120), i), AgentStatus: "working",
		})
	}
	if err := validateHerdrState(big); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unbounded serialized state: %v", err)
	}
	fleet = fleetStore{}
	medium := readyState(1)
	medium.Panes = nil
	for i := 0; i < 1200; i++ {
		medium.Panes = append(medium.Panes, &agentflowv1.HerdrEntity{
			Id: fmt.Sprintf("%s%d", strings.Repeat("x", 120), i), AgentStatus: "working",
		})
	}
	exhausted := false
	for i := 0; i < 30; i++ {
		entry, _ := fleet.begin(fmt.Sprint(i), fmt.Sprint(i), true, now)
		err := fleet.update(entry, medium, now)
		if status.Code(err) == codes.ResourceExhausted {
			exhausted = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !exhausted {
		t.Fatal("fleet total payload budget not enforced")
	}
}

func startHerdrStream(t *testing.T, ctx context.Context, connection *grpc.ClientConn) grpc.BidiStreamingClient[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope] {
	t.Helper()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Capabilities = []string{protocol.HerdrReadCapability}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestFleetRPCIngestionAndAuthorization(t *testing.T) {
	api := &service{
		requiredClientTag: "tag:client", requiredNodeTag: "tag:node",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1", Tags: []string{"tag:node", "tag:client"}}, nil
		},
	}
	connection := newTestConnection(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := startHerdrStream(t, ctx, connection)
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: readyState(1)}}); err != nil {
		t.Fatal(err)
	}
	stream.CloseSend()
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream failed: %v", err)
	}
	list, err := agentflowv1.NewFleetClient(connection).ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Nodes) != 1 || list.Nodes[0].Herdr.Panes[0].AgentStatus != "working" || list.Nodes[0].Connected || !list.Nodes[0].Stale {
		t.Fatalf("wrong fleet result: %v", list)
	}
	denied := newTestConnection(t, &service{
		requiredClientTag: "tag:client",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "node", Tags: []string{"tag:node"}}, nil
		},
	})
	if _, err := agentflowv1.NewFleetClient(denied).ListNodes(ctx, &emptypb.Empty{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node role could query fleet: %v", err)
	}
}

func TestHerdrTrafficCannotReplaceHeartbeat(t *testing.T) {
	api := &service{heartbeatTimeout: 150 * time.Millisecond,
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1"}, nil
		}}
	connection := newTestConnection(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream := startHerdrStream(t, ctx, connection)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(1); ; seq++ {
			if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: readyState(seq)}}); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Herdr traffic hid missing heartbeat: %v", err)
	}
	cancel()
	<-done
}

func TestNewStreamClosesPreviousStream(t *testing.T) {
	connection := newTestConnection(t, &service{
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1"}, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	old := startHerdrStream(t, ctx, connection)
	current := startHerdrStream(t, ctx, connection)
	if _, err := old.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("old stream not closed: %v", err)
	}
	current.CloseSend()
}

func TestUnnegotiatedHerdrStateIsRejected(t *testing.T) {
	connection := newTestConnection(t, &service{
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "stable-1"}, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(nodeHello("node-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: readyState(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unnegotiated projection accepted: %v", err)
	}
}

func TestUnavailableStateClearsProjection(t *testing.T) {
	var fleet fleetStore
	now := time.Now()
	entry, err := fleet.begin("node-1", "stable-1", true, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.update(entry, readyState(1), now); err != nil {
		t.Fatal(err)
	}
	failure := &agentflowv1.HerdrState{
		Status: "unavailable", Sequence: 2, ObservedAt: timestamppb.Now(), ErrorCode: "disconnected",
	}
	if err := fleet.update(entry, failure, now); err != nil {
		t.Fatal(err)
	}
	view := listFleet(t, &fleet, now).Nodes[0]
	if !view.Stale || view.Herdr.Status != "unavailable" || len(view.Herdr.Panes) != 0 {
		t.Fatal("unavailable status retained success-shaped snapshot")
	}
}
