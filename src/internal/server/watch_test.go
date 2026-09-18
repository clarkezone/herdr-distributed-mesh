package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestFleetSubscriptionRPCRechecksPeerRole(t *testing.T) {
	var revoked atomic.Bool
	api := &service{requiredClientTag: "tag:client", identifyPeer: func(ctx context.Context) (transport.PeerIdentity, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 3*time.Second {
			t.Error("peer lookup was not bounded")
		}
		peer := transport.PeerIdentity{StableID: "client", Tags: []string{"tag:client"}}
		if revoked.Load() {
			peer.Tags = nil
		}
		return peer, nil
	}}
	client := pb.NewFleetClient(newTestConnection(t, api))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.WatchNodes(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, err := stream.Recv(); err != nil || snapshot == nil || len(snapshot.Nodes) != 0 {
		t.Fatalf("initial complete snapshot: %v", err)
	}
	revoked.Store(true)
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revocation was hidden or not rechecked: %v", err)
	}
}

func TestFleetSubscriptionRPCSharesCapacityAndRequiresDeadline(t *testing.T) {
	api := &service{identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
		return transport.PeerIdentity{StableID: "client"}, nil
	}}
	client := pb.NewFleetClient(newTestConnection(t, api))
	stream, err := client.WatchNodes(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unbounded transport accepted: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range fleetwatch.MaxSubscriptions {
		stream, err := client.WatchNodes(ctx, &emptypb.Empty{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	stream, err = client.WatchNodes(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("per-service capacity was not shared: %v", err)
	}
}
