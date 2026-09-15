package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type sessionServer struct {
	agentflowv1.UnimplementedNodeControlServer
	capabilities []string
	messages     chan *agentflowv1.NodeEnvelope
}

func (s *sessionServer) Connect(stream grpc.BidiStreamingServer[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope]) error {
	hello, err := stream.Recv()
	if err != nil {
		return err
	}
	if hello.GetHello() == nil {
		return errors.New("missing hello")
	}
	if err := stream.Send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HelloAck{
		HelloAck: &agentflowv1.HelloAck{SelectedProtocol: 1, ServerInstanceId: "test-server", Capabilities: s.capabilities},
	}}); err != nil {
		return err
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		select {
		case s.messages <- message:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func sessionClient(t *testing.T, api agentflowv1.NodeControlServer) agentflowv1.NodeControlClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	agentflowv1.RegisterNodeControlServer(server, api)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return agentflowv1.NewNodeControlClient(connection)
}

func TestSessionRejectsMissingHerdrCapability(t *testing.T) {
	api := &sessionServer{messages: make(chan *agentflowv1.NodeEnvelope, 1)}
	client := sessionClient(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registered, err := runSession(ctx, client, Options{InstanceID: "test-node", HerdrSocket: "must-not-be-opened"})
	if registered || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing capability not rejected before local API: registered=%t err=%v", registered, err)
	}
}

func TestSessionTransportOnly(t *testing.T) {
	testSession(t, "")
}

func TestSessionWithLiveHerdr(t *testing.T) {
	socket := os.Getenv("HERDR_MESH_TEST_SOCKET")
	if socket == "" {
		t.Skip("set HERDR_MESH_TEST_SOCKET for read-only live integration")
	}
	testSession(t, socket)
}

func TestUnavailableHerdrPreservesMeshHeartbeat(t *testing.T) {
	api := &sessionServer{capabilities: []string{protocol.HerdrReadCapability}, messages: make(chan *agentflowv1.NodeEnvelope, 16)}
	client := sessionClient(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	done := make(chan error, 1)
	socket := filepath.Join(t.TempDir(), "missing-herdr.sock")
	go func() {
		_, err := runSession(ctx, client, Options{
			InstanceID: "test-node", HeartbeatInterval: 50 * time.Millisecond,
			HerdrSocket: socket,
		})
		done <- err
	}()
	unavailable := false
	for {
		select {
		case message := <-api.messages:
			if state := message.GetHerdrState(); state != nil {
				if state.Status != "unavailable" || state.ErrorCode == "" {
					t.Fatal("missing Herdr must produce explicit unavailable status")
				}
				unavailable = true
			}
			if message.GetHeartbeat() != nil && unavailable {
				cancel()
				select {
				case <-done:
					return
				case <-time.After(2 * time.Second):
					t.Fatal("unavailable observer failed to cancel")
				}
			}
		case err := <-done:
			t.Fatalf("local Herdr failure stopped mesh stream: %v", err)
		case <-ctx.Done():
			t.Fatal("missing unavailable status or subsequent heartbeat")
		}
	}
}

func testSession(t *testing.T, socket string) {
	t.Helper()
	api := &sessionServer{capabilities: []string{protocol.HerdrReadCapability}, messages: make(chan *agentflowv1.NodeEnvelope, 16)}
	client := sessionClient(t, api)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runSession(ctx, client, Options{InstanceID: "test-node", HeartbeatInterval: 20 * time.Millisecond, HerdrSocket: socket})
		done <- err
	}()
	heartbeat, snapshot := false, socket == ""
	for !heartbeat || !snapshot {
		select {
		case message := <-api.messages:
			if h := message.GetHeartbeat(); h != nil {
				if h.CommandReady {
					t.Fatal("read-only node advertised command readiness")
				}
				heartbeat = true
			}
			if s := message.GetHerdrState(); s != nil {
				if socket == "" {
					t.Fatal("transport-only node emitted Herdr state")
				}
				if s.Status == "unavailable" {
					t.Fatalf("live observer unavailable: %s", s.ErrorCode)
				}
				if s.Status != "ready" || s.Sequence == 0 || s.ObservedAt == nil || s.Protocol != 18 {
					t.Fatalf("unexpected live snapshot metadata: status=%s protocol=%d", s.Status, s.Protocol)
				}
				snapshot = true
			}
		case err := <-done:
			t.Fatalf("session stopped early: %v", err)
		case <-ctx.Done():
			t.Fatal("session did not produce heartbeat and snapshot before deadline")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session or observer did not cancel promptly")
	}
}
