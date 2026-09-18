package transport

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/fleetwatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type snapshotService struct {
	pb.UnimplementedFleetServer
	snapshot *pb.NodeList
}

func (s snapshotService) ListNodes(context.Context, *emptypb.Empty) (*pb.NodeList, error) {
	return s.snapshot, nil
}
func (s snapshotService) WatchNodes(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pb.NodeList]) error {
	return stream.Send(s.snapshot)
}

func TestGRPCDialReceivesFullBoundedSnapshots(t *testing.T) {
	for _, size := range []int{fleetwatch.MaxSnapshotBytes, fleetwatch.MaxSnapshotBytes + 1} {
		snapshot := &pb.NodeList{Nodes: []*pb.NodeView{{Hostname: strings.Repeat("x", size)}}}
		snapshot.Nodes[0].Hostname = snapshot.Nodes[0].Hostname[:size-(proto.Size(snapshot)-size)]
		if proto.Size(snapshot) != size {
			t.Fatalf("fixture is not exact snapshot boundary: %d != %d", proto.Size(snapshot), size)
		}
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer()
		pb.RegisterFleetServer(server, snapshotService{snapshot: snapshot})
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		connection, err := newGRPCClient("snapshot-test", func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client := pb.NewFleetClient(connection)
		unary, unaryErr := client.ListNodes(ctx, &emptypb.Empty{})
		watch, watchErr := client.WatchNodes(ctx, &emptypb.Empty{})
		var streamed *pb.NodeList
		if watchErr == nil {
			streamed, watchErr = watch.Recv()
		}
		cancel()
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		<-done
		if size <= fleetwatch.MaxSnapshotBytes {
			if unaryErr != nil || watchErr != nil || proto.Size(unary) != size || proto.Size(streamed) != size {
				t.Fatalf("valid snapshot truncated or rejected: unary=%v stream=%v", unaryErr, watchErr)
			}
		} else if status.Code(unaryErr) != codes.ResourceExhausted || status.Code(watchErr) != codes.ResourceExhausted {
			t.Fatalf("oversized snapshot admitted: unary=%v stream=%v", unaryErr, watchErr)
		}
	}
}
