package meshlocal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/server"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Only the encrypted network boundary is replaced. Journals, both production
// roles, generic Fleet forwarding, and platform IPC run unchanged.
type localNetwork struct {
	mu       sync.Mutex
	self     transport.SelfStatus
	listener net.Listener
	starts   atomic.Int32
	closes   atomic.Int32
	active   atomic.Int32
	leaked   atomic.Bool
	peerTags []string
}

func (n *localNetwork) SelfStatus() transport.SelfStatus { return n.self }
func (n *localNetwork) Close() error {
	n.closes.Add(1)
	if n.active.Load() != 0 {
		return errors.New("network closed before borrowed roles stopped")
	}
	return nil
}
func (n *localNetwork) Listen(_ string) (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	n.mu.Lock()
	n.listener = listener
	n.mu.Unlock()
	return listener, err
}
func (n *localNetwork) IdentifyPeer(ctx context.Context, _ string) (transport.PeerIdentity, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("x-mesh-identity")) != 0 {
		n.leaked.Store(true)
	}
	return transport.PeerIdentity{StableID: n.self.StableID, Name: n.self.DNSName, Tags: n.self.Tags}, nil
}
func (n *localNetwork) DialGRPC(target string) (*grpc.ClientConn, error) {
	return n.DialGRPCWithPeerTag(target, serverTag)
}
func (n *localNetwork) DialGRPCWithPeerTag(_ string, tag string) (*grpc.ClientConn, error) {
	tags := n.peerTags
	if tags == nil {
		tags = n.self.Tags
	}
	if !slices.Contains(tags, tag) {
		return nil, errors.New("mock peer is missing required role")
	}
	return grpc.NewClient("passthrough:///mock-coordinator", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			n.mu.Lock()
			listener := n.listener
			n.mu.Unlock()
			if listener == nil {
				return nil, errors.New("mock coordinator not yet listening")
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		}))
}

type remoteFleet struct {
	pb.UnimplementedFleetServer
	protocol *pb.ProtocolRange
}

func (f remoteFleet) GetServerInfo(context.Context, *emptypb.Empty) (*pb.ServerInfo, error) {
	return &pb.ServerInfo{InstanceId: "remote-coordinator", Protocol: f.protocol}, nil
}

func (f remoteFleet) ListNodes(context.Context, *emptypb.Empty) (*pb.NodeList, error) {
	return &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: "mock-worker", Connected: true, CommandReady: true}}}, nil
}

func TestManagedWorkerOnlyAndRemoteProtocolGate(t *testing.T) {
	for _, compatible := range []bool{true, false} {
		t.Run(fmt.Sprint(compatible), func(t *testing.T) {
			dir := canonicalTempDir(t)
			if err := Save(dir, Config{Version: 1, Name: "desktop", Server: "remote.tail.test:50052"}); err != nil {
				t.Fatal(err)
			}
			network := &localNetwork{peerTags: []string{serverTag}}
			listener, err := network.Listen(":50052")
			if err != nil {
				t.Fatal(err)
			}
			remote := grpc.NewServer()
			version := protocol.SupportedRange()
			if !compatible {
				version = &pb.ProtocolRange{Minimum: 999, Maximum: 999}
			}
			pb.RegisterFleetServer(remote, remoteFleet{protocol: version})
			remoteDone := make(chan error, 1)
			go func() { remoteDone <- remote.Serve(listener) }()
			defer func() { remote.Stop(); <-remoteDone }()
			deps := network.dependencies(t)
			deps.server = func(context.Context, server.Options, transport.RuntimeNetwork) error {
				t.Error("worker tried to launch a coordinator")
				return errors.New("worker must not run a coordinator")
			}
			var nodeStarted atomic.Bool
			registrationAllowed := make(chan struct{})
			nodeRunning := make(chan struct{})
			deps.node = func(ctx context.Context, options node.Options, borrowed transport.RuntimeNetwork) error {
				nodeStarted.Store(true)
				close(nodeRunning)
				if borrowed != network || options.ServerAddress != "remote.tail.test:50052" || slices.Contains(options.Transport.Tags, serverTag) {
					return errors.New("worker did not preserve remote target or role isolation")
				}
				select {
				case <-registrationAllowed:
					options.OnRegistered("mock-worker")
				case <-ctx.Done():
					return nil
				}
				<-ctx.Done()
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx, dir, io.Discard, deps) }()
			if compatible {
				select {
				case <-nodeRunning:
				case <-ctx.Done():
					t.Fatal("worker process did not start")
				}
				value, err := ReadStatus(dir)
				if err != nil || value.State == "ready" {
					t.Fatalf("runtime ready before worker registration: %+v %v", value, err)
				}
				probe, stopProbe := context.WithTimeout(ctx, 100*time.Millisecond)
				conn, err := dialIPC(probe, dir)
				stopProbe()
				if err == nil {
					_ = conn.Close()
					t.Fatal("IPC published before worker registration")
				}
				close(registrationAllowed)
				waitStatus(t, dir, "ready", done)
				connection, err := Dial(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				_ = connection.Close()
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if !nodeStarted.Load() {
					t.Fatal("worker role was not launched")
				}
			} else {
				if err := <-done; err == nil {
					t.Fatal("incompatible remote protocol accepted")
				}
				if nodeStarted.Load() {
					t.Fatal("worker started before protocol verification")
				}
				connection, err := dialIPC(ctx, dir)
				if err == nil {
					_ = connection.Close()
					t.Fatal("incompatible remote coordinator exposed IPC")
				}
			}
			if network.starts.Load() != 1 || network.closes.Load() != 1 {
				t.Fatalf("worker network was recreated or leaked: %d/%d", network.starts.Load(), network.closes.Load())
			}
		})
	}
}
func (n *localNetwork) dependencies(t *testing.T) runtimeDependencies {
	t.Helper()
	return runtimeDependencies{
		start: func(_ context.Context, config transport.Config) (transport.RuntimeNetwork, error) {
			n.starts.Add(1)
			if config.Hostname != "herdr-mesh-desktop" || config.AuthKeyEnv != "" {
				return nil, errors.New("incorrect managed transport options")
			}
			n.self = transport.SelfStatus{MagicDNSEnabled: true, StableID: "shared-stable", DNSName: "herdr-mesh-desktop.assigned-tail.test.", Tailnet: "example.test", Tags: config.Tags}
			config.UserLog("To authenticate, visit: https://login.tailscale.com/a/private-token")
			return n, nil
		},
		server: func(ctx context.Context, options server.Options, network transport.RuntimeNetwork) error {
			if network != n {
				return errors.New("coordinator did not borrow original network")
			}
			n.active.Add(1)
			defer n.active.Add(-1)
			return server.RunWithNetwork(ctx, options, network)
		},
		node: func(ctx context.Context, options node.Options, network transport.RuntimeNetwork) error {
			if network != n || options.Name != "desktop" {
				return errors.New("worker did not borrow original network or logical name")
			}
			n.active.Add(1)
			defer n.active.Add(-1)
			return node.RunWithNetwork(ctx, options, network)
		},
	}
}

func TestManagedRuntimeSharesOneNetworkAcrossRolesAndParallelClients(t *testing.T) {
	testManagedRuntimeAssignedDNS(t, "herdr-mesh-desktop.assigned-tail.test", canonicalTempDir(t))
}

func TestManagedRuntimePreservesAssignedCollisionSuffix(t *testing.T) {
	testManagedRuntimeAssignedDNS(t, "herdr-mesh-desktop-1.assigned-tail.test", canonicalTempDir(t))
}

func testManagedRuntimeAssignedDNS(t *testing.T, assignedDNS, dir string) {
	t.Helper()
	if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "policy-complete"), []byte(policyCompleteRecord), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var output bytes.Buffer
	network := &localNetwork{}
	deps := network.dependencies(t)
	start := deps.start
	deps.start = func(ctx context.Context, config transport.Config) (transport.RuntimeNetwork, error) {
		n, err := start(ctx, config)
		if err == nil {
			network.self.DNSName = assignedDNS + "."
		}
		return n, err
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx, dir, &output, deps) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("managed runtime did not stop")
		}
	})
	waitStatus(t, dir, "ready", done)
	stateValue, err := ReadStatus(dir)
	if err != nil || stateValue.Server != assignedDNS+":50052" || stateValue.DNSName != assignedDNS || stateValue.AuthURL != "" {
		t.Fatalf("incorrect assigned status: %+v, %v", stateValue, err)
	}
	// Neither another managed owner nor a legacy role can acquire these guards.
	if err := run(ctx, dir, io.Discard, network.dependencies(t)); err == nil {
		t.Fatal("second managed owner acquired the runtime")
	}
	for _, role := range []string{"server", "node"} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			guard, err := state.AcquireRoleState(ctx, filepath.Join(dir, role), role)
			if guard != nil {
				_ = guard.Close()
				t.Fatalf("%s role lost its lifetime guard", role)
			}
			if errors.Is(err, state.ErrLocked) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s guard unavailable: %v", role, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			connection, err := Dial(ctx, dir)
			if err != nil {
				t.Error(err)
				return
			}
			defer connection.Close()
			client := pb.NewFleetClient(connection)
			for range 5 {
				call := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-mesh-identity", "forged-server"))
				if _, err := client.ListNodes(call, &emptypb.Empty{}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	connection, err := Dial(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewFleetClient(connection)
	nodes, err := client.ListNodes(ctx, &emptypb.Empty{})
	if err != nil || len(nodes.GetNodes()) != 1 || nodes.Nodes[0].Hostname != "desktop" {
		t.Fatalf("registered logical node name missing: %+v %v", nodes, err)
	}
	watch, err := client.WatchNodes(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	if err := connection.Invoke(ctx, "/agentflow.v1.NodeControl/Connect", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("local gateway exposed NodeControl: %v", err)
	}
	// Future/generated unary methods must already be forwarded, not unknown.
	_, err = client.ResolveNamedAgent(ctx, &pb.ResolveNamedAgentRequest{})
	if status.Code(err) == codes.Unimplemented {
		t.Fatalf("new Fleet unary method not forwarded: %v", err)
	}
	if running, err := IsRunning(dir); err != nil || !running {
		t.Fatalf("ready runtime ownership: running=%v, err=%v", running, err)
	}
	if err := Shutdown(ctx, dir); err != nil {
		t.Fatalf("cooperative shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("managed shutdown hung with an open watch")
	}
	if network.starts.Load() != 1 || network.closes.Load() != 1 || network.leaked.Load() {
		t.Fatalf("shared network lifecycle/identity violation: starts=%d closes=%d leaked=%t", network.starts.Load(), network.closes.Load(), network.leaked.Load())
	}
	if strings.Contains(output.String(), "private-token") || strings.Contains(output.String(), "https://") {
		t.Fatal("login secret leaked to runtime output")
	}
	if !strings.Contains(output.String(), "coordinator "+assignedDNS+":50052;") {
		t.Fatalf("runtime output did not preserve assigned coordinator DNS: %s", output.String())
	}
	after, err := ReadStatus(dir)
	if err != nil || after.State != "stopped" {
		t.Fatalf("shutdown status %+v, %v", after, err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("stopped runtime ownership: running=%v, err=%v", running, err)
	}
	offline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if conn, err := Dial(offline, dir); err == nil {
		_ = conn.Close()
		t.Fatal("stopped IPC silently reconnected/enrolled")
	}
	if network.starts.Load() != 1 {
		t.Fatal("client dial recreated the network")
	}
	serverID, err := os.ReadFile(filepath.Join(dir, "server", "instance-id"))
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := os.ReadFile(filepath.Join(dir, "node", "instance-id"))
	if err != nil || len(nodeID) == 0 || bytes.Equal(serverID, nodeID) {
		t.Fatalf("role instance identities are not independent: %v", err)
	}
	for _, role := range []string{"server", "node"} {
		marker, err := os.ReadFile(filepath.Join(dir, role, "role-state.lock"))
		if err != nil || !bytes.Contains(marker, []byte("pending")) || bytes.Contains(marker, []byte("\nactive\n")) {
			t.Fatalf("managed %s child activated legacy backup contract: %q %v", role, marker, err)
		}
		if _, err := state.BackupMaintenance(context.Background(), filepath.Join(dir, role), role, filepath.Join(canonicalTempDir(t), "backup")); err == nil {
			t.Fatalf("managed %s child advertised a full identity backup: %v", role, err)
		}
	}
}

func waitStatus(t *testing.T, dir, want string, done chan error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("managed runtime stopped before %s: %v", want, err)
		default:
		}
		value, err := ReadStatus(dir)
		if err == nil && value.State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("managed runtime did not reach %s", want)
}

func TestConfigCreateOnlyAndBoundedPrivateStatus(t *testing.T) {
	dir := canonicalTempDir(t)
	config := Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}
	if err := Save(dir, config); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil || !bytes.Contains(content, []byte(`"schemaVersion":1`)) {
		t.Fatalf("missing managed schema version: %s %v", content, err)
	}
	changed := config
	changed.Name = "replacement"
	if err := Save(dir, changed); !errors.Is(err, os.ErrExist) {
		t.Fatalf("configuration overwritten: %v", err)
	}
	loaded, err := Load(dir)
	if err != nil || loaded != config {
		t.Fatalf("configuration changed: %+v %v", loaded, err)
	}
	var canceled atomic.Bool
	writer := &statusWriter{dir: dir, cancel: func() { canceled.Store(true) }}
	writer.login("visit https://evil.example/a/token")
	if _, err := ReadStatus(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("untrusted browser URL accepted")
	}
	writer.login("visit https://login.tailscale.com/a/secret")
	value, err := ReadStatus(dir)
	if err != nil || value.State != "login_required" || value.AuthURL != "https://login.tailscale.com/a/secret" || canceled.Load() {
		t.Fatalf("private login status missing: %+v %v", value, err)
	}
	writer.login("visit https://controlplane.tailscale.com/a/second-secret")
	value, err = ReadStatus(dir)
	if err != nil || value.AuthURL != "https://controlplane.tailscale.com/a/second-secret" {
		t.Fatalf("official control-plane browser URL rejected: %+v %v", value, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "status.json"), bytes.Repeat([]byte(" "), 16385), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStatus(dir); err == nil {
		t.Fatal("oversized status accepted")
	}
}

func TestManagedDiagnosticsAreBoundedAndDoNotExposeCredentials(t *testing.T) {
	cause := errors.New("login https://login.tailscale.com/a/secret key=tskey-auth-secret " + strings.Repeat("x", 1000))
	err := diagnosticError{cause}
	if len(err.Error()) > 512 || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "tskey") || !errors.Is(err, cause) {
		t.Fatalf("unsafe diagnostic: %s", err.Error())
	}
}

func TestManagedRejectedIdentityNeverPublishesIPC(t *testing.T) {
	for _, test := range []string{"missing-role", "wrong-tailnet", "startup-error", "empty-label", "unqualified-name", "magicdns-disabled"} {
		t.Run(test, func(t *testing.T) {
			dir := canonicalTempDir(t)
			if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "policy-complete"), []byte(policyCompleteRecord), 0600); err != nil {
				t.Fatal(err)
			}

			network := &localNetwork{}
			deps := network.dependencies(t)
			start := deps.start
			deps.start = func(ctx context.Context, config transport.Config) (transport.RuntimeNetwork, error) {
				if test == "startup-error" {
					return nil, errors.New("https://login.tailscale.com/a/secret-startup-diagnostic")
				}
				n, err := start(ctx, config)
				switch test {
				case "missing-role":
					network.self.Tags = []string{nodeTag}
				case "wrong-tailnet":
					network.self.Tailnet = "other.test"
				case "empty-label":
					network.self.DNSName = ".assigned-tail.test"
				case "unqualified-name":
					network.self.DNSName = "herdr-mesh-desktop"
				case "magicdns-disabled":
					network.self.MagicDNSEnabled = false
				}
				return n, err
			}
			if err := run(context.Background(), dir, io.Discard, deps); err == nil {
				t.Fatal("invalid managed identity accepted")
			}
			value, err := ReadStatus(dir)
			if err != nil || value.State != "failed" || strings.Contains(value.Error, "https://") || value.AuthURL != "" {
				t.Fatalf("unsafe failure status: %+v %v", value, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if conn, err := Dial(ctx, dir); err == nil {
				_ = conn.Close()
				t.Fatal("unverified coordinator exposed local IPC")
			}
		})
	}
}
