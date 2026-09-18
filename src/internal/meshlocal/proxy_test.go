package meshlocal

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockUpstream struct {
	invoke func(context.Context, string, any, any) error
}

func (u mockUpstream) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	return u.invoke(ctx, method, request, response)
}
func (mockUpstream) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("stream not expected")
}

func proxyConnection(t *testing.T, upstream grpc.ClientConnInterface) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(maxMessage)
	server, err := newProxy(upstream)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///proxy-test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close(); <-done })
	return conn
}

func TestProxyForwardsEveryFleetUnaryAndStripsIdentityMetadata(t *testing.T) {
	calls := make(chan string, 100)
	connection := proxyConnection(t, mockUpstream{invoke: func(ctx context.Context, method string, request, response any) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		if len(md) != 0 {
			t.Errorf("caller metadata escaped local IPC: %v", md)
		}
		calls <- method
		return nil
	}})
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName("agentflow.v1.Fleet")
	if err != nil {
		t.Fatal(err)
	}
	fleet := descriptor.(protoreflect.ServiceDescriptor)
	for i := 0; i < fleet.Methods().Len(); i++ {
		method := fleet.Methods().Get(i)
		if method.IsStreamingServer() {
			continue
		}
		full := "/" + string(fleet.FullName()) + "/" + string(method.Name())
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "forged", "tailscale-user", "admin")), time.Second)
		err := connection.Invoke(ctx, full, dynamicpb.NewMessage(method.Input()), dynamicpb.NewMessage(method.Output()))
		cancel()
		if err != nil {
			t.Fatalf("%s not forwarded: %v", full, err)
		}
		if got := <-calls; got != full {
			t.Fatalf("forwarded %s as %s", full, got)
		}
	}
}

func TestProxyReservesStopAndInterruptCapacityAndBoundsRequests(t *testing.T) {
	entered := make(chan struct{}, 64)
	connection := proxyConnection(t, mockUpstream{invoke: func(ctx context.Context, method string, request, response any) error {
		if strings.HasSuffix(method, "/ListNodes") {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}})
	client := pb.NewFleetClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() { _, _ = client.ListNodes(ctx, &emptypb.Empty{}) })
	}
	for range 64 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("proxy did not admit bounded parallel requests")
		}
	}
	if _, err := client.ListNodes(ctx, &emptypb.Empty{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unbounded ordinary RPC concurrency: %v", err)
	}
	if _, err := client.SubmitCommand(ctx, &pb.SubmitCommandRequest{AgentStop: &pb.AgentStop{}}); err != nil {
		t.Fatalf("ordinary requests blocked reserved stop capacity: %v", err)
	}
	if _, err := client.SubmitCommand(ctx, &pb.SubmitCommandRequest{AgentControl: &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT}}); err != nil {
		t.Fatalf("ordinary requests blocked reserved interrupt capacity: %v", err)
	}
	if _, err := client.SubmitCommand(ctx, &pb.SubmitCommandRequest{AgentControl: &pb.AgentControl{Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT}}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ordinary input consumed reserved urgent capacity: %v", err)
	}
	if _, err := client.SubmitCommand(ctx, &pb.SubmitCommandRequest{IdempotencyKey: strings.Repeat("x", maxMessage+1)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized message accepted: %v", err)
	}
	cancel()
	wg.Wait()
}

func TestProxyPreservesUnaryStatusAndPayload(t *testing.T) {
	connection := proxyConnection(t, mockUpstream{invoke: func(ctx context.Context, method string, request, response any) error {
		if strings.HasSuffix(method, "/GetServerInfo") {
			proto.Merge(response.(proto.Message), &pb.ServerInfo{InstanceId: "verified"})
			return nil
		}
		return status.Error(codes.PermissionDenied, "remote role denied")
	}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := pb.NewFleetClient(connection)
	info, err := client.GetServerInfo(ctx, &emptypb.Empty{})
	if err != nil || info.GetInstanceId() != "verified" {
		t.Fatalf("response payload changed: %+v %v", info, err)
	}
	if _, err := client.ListNodes(ctx, &emptypb.Empty{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("remote authorization error hidden: %v", err)
	}
}
