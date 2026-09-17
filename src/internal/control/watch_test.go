package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type watchClientFixture struct {
	pb.FleetClient
	open func(context.Context, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.NodeList], error)
}

func (c watchClientFixture) WatchNodes(ctx context.Context, _ *emptypb.Empty, options ...grpc.CallOption) (grpc.ServerStreamingClient[pb.NodeList], error) {
	return c.open(ctx, options...)
}

type watchStreamFixture struct {
	grpc.ClientStream
	receive func() (*pb.NodeList, error)
}

func (s watchStreamFixture) Recv() (*pb.NodeList, error) { return s.receive() }

func TestFleetWatchReconnectionsReplaceSnapshotsAndReportOneGap(t *testing.T) {
	var out bytes.Buffer
	opens := 0
	client := watchClientFixture{open: func(ctx context.Context, options ...grpc.CallOption) (grpc.ServerStreamingClient[pb.NodeList], error) {
		opens++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > fleetwatch.MaxDuration {
			t.Fatal("stream has no bounded transport deadline")
		}
		matched := false
		for _, option := range options {
			if size, ok := option.(grpc.MaxRecvMsgSizeCallOption); ok && size.MaxRecvMsgSize == fleetwatch.MaxSnapshotBytes {
				matched = true
			}
		}
		if !matched {
			t.Fatal("receive limit does not match server snapshot bound")
		}
		if opens == 2 {
			return nil, status.Error(codes.Unavailable, "still offline")
		}
		first := true
		return watchStreamFixture{receive: func() (*pb.NodeList, error) {
			if first {
				first = false
				id := "old-node"
				if opens == 3 {
					id = "new-node"
				}
				return &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: id}}}, nil
			}
			if opens == 1 {
				return nil, io.EOF
			}
			return nil, status.Error(codes.PermissionDenied, "role removed")
		}}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := watchNodesWithClient(ctx, Options{JSON: true, Output: &out}, client, 100*time.Millisecond, time.Millisecond)
	if status.Code(err) != codes.PermissionDenied || opens != 3 {
		t.Fatalf("terminal failure retried: opens=%d err=%v", opens, err)
	}
	decoder := json.NewDecoder(&out)
	for index, kind := range []string{"snapshot", "gap", "snapshot"} {
		var event fleetWatchEvent
		if err := decoder.Decode(&event); err != nil || event.Type != kind {
			t.Fatalf("event %d: %v %v", index, event, err)
		}
		if index == 2 && (strings.Contains(string(event.Fleet), "old-node") || !strings.Contains(string(event.Fleet), "new-node")) {
			t.Fatal("reconnect merged old inventory instead of replacing it")
		}
	}
	var extra fleetWatchEvent
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatal("unexpected event or malformed trailing data")
	}
}

func TestFleetWatchRenewsBoundedStreamsWithFullSnapshots(t *testing.T) {
	opens := 0
	client := watchClientFixture{open: func(ctx context.Context, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.NodeList], error) {
		opens++
		first := true
		return watchStreamFixture{receive: func() (*pb.NodeList, error) {
			if first {
				first = false
				return &pb.NodeList{}, nil
			}
			if opens == 2 {
				return nil, status.Error(codes.PermissionDenied, "removed")
			}
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var out bytes.Buffer
	err := watchNodesWithClient(ctx, Options{JSON: true, Output: &out}, client, 5*time.Millisecond, time.Millisecond)
	if status.Code(err) != codes.PermissionDenied || opens != 2 || strings.Count(out.String(), `"type":"snapshot"`) != 2 ||
		strings.Contains(out.String(), `"type":"gap"`) {
		t.Fatalf("stream renewal failed: opens=%d output=%s err=%v", opens, out.String(), err)
	}
}

func TestFleetWatchCancellationDoesNotEmitLateSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := watchClientFixture{open: func(context.Context, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.NodeList], error) {
		return watchStreamFixture{receive: func() (*pb.NodeList, error) {
			cancel()
			return &pb.NodeList{}, nil
		}}, nil
	}}
	var out bytes.Buffer
	err := WatchNodes(ctx, Options{FleetClient: client, JSON: true, Output: &out})
	if !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatalf("late snapshot escaped cancellation: %v", err)
	}
	if err := WatchNodes(context.Background(), Options{Output: io.Discard}); err == nil {
		t.Fatal("unbounded controller accepted")
	}
}
