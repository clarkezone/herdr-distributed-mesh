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

func TestLocalClientDestroyStopsCooperativelyBeforePurge(t *testing.T) {
	root := filepath.Join(canonicalTempDir(t), "client")
	cfg := Config{Version: 1, Name: "laptop", Server: "controller.tail.ts.net:50052"}
	if err := Save(root, cfg); err != nil {
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
	if err := SaveDestroyState(root, DestroyState{Configuration: cfg, LocalOnly: true}); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { <-ctx.Done(); closed <- guard.Close() }()
	request, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := Shutdown(request, root); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := RecordStopped(request, root); err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(request, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("client state remains: %v", err)
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

func TestLocalClientPurgeStillRequiresStoppedOriginalInstallation(t *testing.T) {
	for _, scenario := range []string{"active", "different-client", "controller"} {
		t.Run(scenario, func(t *testing.T) {
			root := filepath.Join(canonicalTempDir(t), "managed")
			cfg := Config{Version: 1, Name: "laptop", Server: "controller.tail.ts.net:50052"}
			actual := cfg
			if scenario == "controller" {
				actual.Coordinator, actual.Tailnet = true, "example.com"
			}
			if scenario == "different-client" {
				actual.Name = "other"
			}
			if err := Save(root, actual); err != nil {
				t.Fatal(err)
			}
			if err := SaveDestroyState(root, DestroyState{Configuration: cfg, LocalOnly: true}); err != nil {
				t.Fatal(err)
			}
			if scenario == "active" {
				guard, err := state.PrepareRoleState(context.Background(), root, "client")
				if err != nil {
					t.Fatal(err)
				}
				defer guard.Close()
			}
			if err := PurgeManaged(context.Background(), root); err == nil {
				t.Fatal("local purge bypassed ownership or configuration checks")
			}
			if got, err := Load(root); err != nil || got != actual {
				t.Fatalf("refused purge altered configuration: %+v, %v", got, err)
			}
		})
	}
}

func TestLocalDestroyRecordCannotClaimRemoteCleanup(t *testing.T) {
	cfg := Config{Version: 1, Name: "laptop", Server: "controller.tail.ts.net:50052"}
	root := teardownRoot(t)
	if err := SaveDestroyState(root, DestroyState{Configuration: cfg, LocalOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveManagedIdentity(context.Background(), root); err == nil {
		t.Fatal("local-only record supplied an empty remote identity")
	}
	for _, record := range []DestroyState{
		{LocalOnly: true},
		{Configuration: Config{Version: 1, Name: "desktop", Coordinator: true, Tailnet: "example.com"}, LocalOnly: true},
		{Configuration: cfg, LocalOnly: true, RemovePolicy: true},
		{Configuration: cfg, LocalOnly: true, DeviceRemoved: true},
		{Configuration: cfg, LocalOnly: true, PolicyRemoved: true},
		{Configuration: cfg, LocalOnly: true, Identity: ManagedIdentity{DeviceID: "nPinned"}},
	} {
		root := teardownRoot(t)
		if err := SaveDestroyState(root, record); err == nil {
			t.Fatalf("invalid local intent accepted: %+v", record)
		}
		if err := writePrivateJSON(root, "destroy.json", record); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadDestroyState(root); err == nil {
			t.Fatalf("invalid retained local intent accepted: %+v", record)
		}
	}
}

func TestRecordStoppedPreservesIdentityAndCannotOverwriteRunningStatus(t *testing.T) {
	root := teardownRoot(t)
	original := Status{State: "ready", DNSName: "desktop.tail.ts.net", Server: "desktop.tail.ts.net:50052"}
	if err := writeStatus(root, original); err != nil {
		t.Fatal(err)
	}
	guard, err := state.PrepareRoleState(context.Background(), root, "client")
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordStopped(context.Background(), root); err == nil {
		t.Fatal("shutdown status overwrote a running daemon")
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RecordStopped(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	status, err := ReadStatus(root)
	if err != nil || status.State != "stopped" || status.DNSName != original.DNSName || status.Server != original.Server {
		t.Fatal("incorrect stopped status", status, err)
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
