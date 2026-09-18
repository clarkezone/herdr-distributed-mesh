package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNodeHandshakeAndHeartbeat(t *testing.T) {
	connection := newTestConnection(t, &service{
		instanceID: "server-1",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "peer-1", Name: "node.test"}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(nodeHello("node-1")); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(hello ack) error = %v", err)
	}
	if got := response.GetHelloAck().GetSelectedProtocol(); got != protocol.MaximumVersion {
		t.Fatalf("selected protocol = %d, want %d", got, protocol.MaximumVersion)
	}
	if err := stream.Send(&agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_Heartbeat{
			Heartbeat: &agentflowv1.Heartbeat{
				Sequence: 1,
				SentAt:   timestamppb.Now(),
			},
		},
	}); err != nil {
		t.Fatalf("Send(heartbeat) error = %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
}

func TestNodeHandshakeRejectsIncompatibleProtocol(t *testing.T) {
	connection := newTestConnection(t, &service{
		instanceID: "server-1",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "peer-1"}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	hello := nodeHello("node-1")
	hello.GetHello().Protocol = &agentflowv1.ProtocolRange{Minimum: 2, Maximum: 2}
	if err := stream.Send(hello); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Recv() code = %s, want %s; error = %v", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestGetServerInfoRequiresConfiguredTag(t *testing.T) {
	connection := newTestConnection(t, &service{
		instanceID:        "server-1",
		requiredClientTag: "tag:controller",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "peer-1", Tags: []string{"tag:observer"}}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := agentflowv1.NewFleetClient(connection).GetServerInfo(ctx, &emptypb.Empty{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetServerInfo() code = %s, want %s; error = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestNodeConnectRequiresConfiguredTag(t *testing.T) {
	connection := newTestConnection(t, &service{
		instanceID:      "server-1",
		requiredNodeTag: "tag:node",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "peer-1", Tags: []string{"tag:observer"}}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(nodeHello("node-1")); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Recv() code = %s, want %s; error = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestNodeConnectRejectsIdentityRebinding(t *testing.T) {
	connection := newTestConnection(t, &service{
		instanceID: "server-1",
		identifyPeer: func(context.Context) (transport.PeerIdentity, error) {
			return transport.PeerIdentity{StableID: "peer-1"}, nil
		},
		bindNode: func(stableID, instanceID string) error {
			return state.ErrIdentityConflict
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := agentflowv1.NewNodeControlClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(nodeHello("node-1")); err != nil {
		t.Fatalf("Send(hello) error = %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Recv() code = %s, want %s; error = %v", status.Code(err), codes.PermissionDenied, err)
	}
}

type blockingReceiver struct {
	release <-chan struct{}
}

func (receiver blockingReceiver) Recv() (*agentflowv1.NodeEnvelope, error) {
	<-receiver.release
	return nil, context.Canceled
}

func TestReceiveNodeEnvelopeTimesOut(t *testing.T) {
	release := make(chan struct{})
	_, err := receiveNodeEnvelope(blockingReceiver{release: release}, time.Millisecond)
	close(release)
	if !errors.Is(err, errReceiveTimeout) {
		t.Fatalf("receiveNodeEnvelope() error = %v, want timeout", err)
	}
}

func nodeHello(instanceID string) *agentflowv1.NodeEnvelope {
	return &agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_Hello{
			Hello: &agentflowv1.Hello{
				Protocol:   protocol.SupportedRange(),
				InstanceId: instanceID,
				Role:       agentflowv1.Role_ROLE_NODE,
			},
		},
	}
}

func newTestConnection(t *testing.T, api *service) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true))
	agentflowv1.RegisterNodeControlServer(grpcServer, api)
	agentflowv1.RegisterFleetServer(grpcServer, api)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}
