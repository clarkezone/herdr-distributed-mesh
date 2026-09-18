package meshlocal

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type largeSnapshotFleet struct {
	pb.UnimplementedFleetServer
	snapshot *pb.NodeList
	stopped  chan struct{}
}

func (s largeSnapshotFleet) GetServerInfo(context.Context, *emptypb.Empty) (*pb.ServerInfo, error) {
	return &pb.ServerInfo{InstanceId: "verified-coordinator"}, nil
}
func (s largeSnapshotFleet) ListNodes(ctx context.Context, _ *emptypb.Empty) (*pb.NodeList, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, status.Error(codes.InvalidArgument, "caller deadline lost")
	}
	if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("x-identity")) != 0 {
		return nil, status.Error(codes.PermissionDenied, "untrusted identity metadata leaked")
	}
	return s.snapshot, nil
}
func (s largeSnapshotFleet) WatchNodes(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pb.NodeList]) error {
	defer close(s.stopped)
	deadline, ok := stream.Context().Deadline()
	if !ok || time.Until(deadline) > 10*time.Second {
		return status.Error(codes.InvalidArgument, "caller watch deadline lost")
	}
	if err := stream.Send(s.snapshot); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestPrivateProxyAcceptsFullSnapshotsAndRejectsOversize(t *testing.T) {
	for _, size := range []int{maxResponse, maxResponse + 1} {
		snapshot := &pb.NodeList{Nodes: []*pb.NodeView{{Hostname: strings.Repeat("x", size)}}}
		snapshot.Nodes[0].Hostname = snapshot.Nodes[0].Hostname[:size-(proto.Size(snapshot)-size)]
		if proto.Size(snapshot) != size {
			t.Fatal("snapshot fixture missed exact byte boundary")
		}
		upstreamListener := bufconn.Listen(maxMessage)
		remote := grpc.NewServer()
		watchStopped := make(chan struct{})
		pb.RegisterFleetServer(remote, largeSnapshotFleet{snapshot: snapshot, stopped: watchStopped})
		remoteDone := make(chan error, 1)
		go func() { remoteDone <- remote.Serve(upstreamListener) }()
		upstream, err := grpc.NewClient("passthrough:///snapshot-coordinator", grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return upstreamListener.DialContext(ctx) }))
		if err != nil {
			t.Fatal(err)
		}
		dir, err := privateDir(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := listenIPC(dir)
		if err != nil {
			t.Fatal(err)
		}
		proxy, err := newProxy(upstream)
		if err != nil {
			t.Fatal(err)
		}
		proxyDone := make(chan error, 1)
		go func() { proxyDone <- proxy.Serve(listener) }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		connection, err := Dial(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		client := pb.NewFleetClient(connection)
		unary, unaryErr := client.ListNodes(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-identity", "forged")), &emptypb.Empty{})
		watch, watchErr := client.WatchNodes(ctx, &emptypb.Empty{})
		var streamed *pb.NodeList
		if watchErr == nil {
			streamed, watchErr = watch.Recv()
		}
		cancel()
		select {
		case <-watchStopped:
		case <-time.After(2 * time.Second):
			t.Error("caller cancellation did not cancel upstream watch")
		}
		_ = connection.Close()
		proxy.Stop()
		_ = listener.Close()
		<-proxyDone
		_ = upstream.Close()
		remote.Stop()
		_ = upstreamListener.Close()
		<-remoteDone
		if size <= maxResponse {
			if unaryErr != nil || watchErr != nil || proto.Size(unary) != size || proto.Size(streamed) != size {
				t.Fatalf("valid full snapshot rejected: unary=%v stream=%v", unaryErr, watchErr)
			}
		} else if status.Code(unaryErr) != codes.ResourceExhausted || status.Code(watchErr) != codes.ResourceExhausted {
			t.Fatalf("oversized snapshot admitted: unary=%v stream=%v", unaryErr, watchErr)
		}
	}
}
