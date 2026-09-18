package meshlocal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func teardownRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(canonicalTempDir(t), "managed")
	if err := Save(root, Config{Version: 1, Name: "desktop", Coordinator: true, Tailnet: "example.com"}); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestShutdownPinsRuntimeAndPreservesData(t *testing.T) {
	root := teardownRoot(t)
	path := filepath.Join(root, "journal.db")
	if err := os.WriteFile(path, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	guard, err := state.PrepareRoleState(context.Background(), root, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	join, err := startShutdownMonitor(ctx, root, cancel)
	if err != nil {
		t.Fatal(err)
	}
	defer join()
	if err := writePrivateJSON(root, "shutdown-request.json", RuntimeControl{Nonce: "old-runtime-nonce"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("old request targeted a replacement")
	case <-time.After(250 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { <-ctx.Done(); closed <- guard.Close() }()
	request, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := Shutdown(request, root); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "retained" {
		t.Fatal("shutdown changed durable state")
	}
	if _, err := Load(root); err != nil {
		t.Fatal("shutdown removed configuration", err)
	}
}

func TestShutdownLegacyAndTimeoutDoNotDestroyState(t *testing.T) {
	root := teardownRoot(t)
	guard, err := state.PrepareRoleState(context.Background(), root, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if err := Shutdown(context.Background(), root); !errors.Is(err, ErrLegacyShutdown) {
		t.Fatal("legacy runtime not identified", err)
	}
	if err := writePrivateJSON(root, "runtime-control.json", RuntimeControl{Nonce: "new-runtime-nonce-123"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := Shutdown(ctx, root); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unconfirmed shutdown reported success", err)
	}
	if _, err := Load(root); err != nil {
		t.Fatal("failed shutdown destroyed configuration", err)
	}
}

func TestDestroyRecordFencesRuntimeBeforeNetwork(t *testing.T) {
	root := teardownRoot(t)
	record := DestroyState{Identity: ManagedIdentity{DeviceID: "nPinned", DNSName: "desktop.tail.ts.net"}}
	if err := SaveDestroyState(root, record); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), root, nil, runtimeDependencies{
		start: func(context.Context, transport.Config) (transport.RuntimeNetwork, error) {
			t.Fatal("destroying installation enrolled or connected")
			return nil, nil
		},
	})
	if err == nil {
		t.Fatal("destroy fence ignored")
	}
}

func TestManagedPurgeRequiresRemoteReceiptAndExclusiveOwnership(t *testing.T) {
	root := teardownRoot(t)
	record := DestroyState{Identity: ManagedIdentity{DeviceID: "nPinned", DNSName: "desktop.tail.ts.net"}}
	if err := SaveDestroyState(root, record); err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(context.Background(), root); err == nil {
		t.Fatal("purge without confirmed remote deletion")
	}
	record.DeviceRemoved = true
	if err := SaveDestroyState(root, record); err != nil {
		t.Fatal(err)
	}
	guard, err := state.PrepareRoleState(context.Background(), root, "client")
	if err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(context.Background(), root); err == nil {
		t.Fatal("purged a running daemon")
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(filepath.Dir(root), "user-repository")
	if err := os.WriteFile(outside, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("purge left managed state", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal("purge affected sibling repository", err)
	}
}

func TestManagedIdentityIsPinnedAndAvailableOffline(t *testing.T) {
	root := teardownRoot(t)
	identity := ManagedIdentity{DeviceID: "nPinned", DNSName: "desktop.tail.ts.net", Tailnet: "example.com"}
	if err := RetainIdentity(root, identity); err != nil {
		t.Fatal(err)
	}

	value, err := ResolveManagedIdentity(context.Background(), root)
	if err != nil || value != identity {
		t.Fatal("offline identity unavailable", err)
	}
	identity.DeviceID = "nReplacement"
	if err := RetainIdentity(root, identity); err == nil {
		t.Fatal("silently replaced cleanup identity")
	}
}

type legacyIdentityFleet struct {
	remoteFleet
}

func (legacyIdentityFleet) ListNodes(context.Context, *emptypb.Empty) (*pb.NodeList, error) {
	return &pb.NodeList{Nodes: []*pb.NodeView{
		{InstanceId: "another-node", Hostname: "desktop", TailscaleStableId: "nWrong"},
		{InstanceId: "retained-local-instance", Hostname: "renamed", TailscaleStableId: "nCorrect"},
	}}, nil
}

func TestLegacyIdentityDiscoveryUsesExactRetainedInstanceNotName(t *testing.T) {
	root := teardownRoot(t)
	if err := os.Mkdir(filepath.Join(root, "node"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "node", "instance-id")
	if err := os.WriteFile(path, []byte("retained-local-instance\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := state.ProtectPrivatePath(path, false); err != nil {
		t.Fatal(err)
	}
	if err := writeStatus(root, Status{State: "ready", DNSName: "desktop.tail.ts.net"}); err != nil {
		t.Fatal(err)
	}
	listener, err := listenIPC(root)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterFleetServer(server, legacyIdentityFleet{remoteFleet{protocol: protocol.SupportedRange()}})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	identity, err := ResolveManagedIdentity(ctx, root)
	if err != nil || identity.DeviceID != "nCorrect" {
		t.Fatal("legacy discovery selected another computer", identity, err)
	}
	if _, err := os.Stat(filepath.Join(root, "mesh-identity.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read-only identity discovery modified saved identity", err)
	}
}

func TestManagedPurgeRejectsLinkedDescendants(t *testing.T) {
	root := teardownRoot(t)
	identity := ManagedIdentity{DeviceID: "nPinned", DNSName: "desktop.tail.ts.net"}
	if err := SaveDestroyState(root, DestroyState{Identity: identity, DeviceRemoved: true}); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(filepath.Dir(root), "outside")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := PurgeManaged(context.Background(), root); err == nil {
		t.Fatal("purge accepted a linked descendant")
	}
	if _, err := Load(root); err != nil {
		t.Fatal("purge began deleting before validating the complete tree", err)
	}
}

func TestPartialPurgeCanResumeWithoutConfigurationFile(t *testing.T) {
	root := teardownRoot(t)
	config, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	record := DestroyState{Configuration: config, Identity: ManagedIdentity{DeviceID: "nPinned"}, DeviceRemoved: true}
	if err := SaveDestroyState(root, record); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "config.json")); err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(context.Background(), root); err != nil {
		t.Fatal("confirmed partial purge could not resume", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("resumed purge left managed state", err)
	}
}
