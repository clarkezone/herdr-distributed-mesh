package meshlocal

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"google.golang.org/grpc"
)

func linkedInstallation(t *testing.T) (string, string) {
	t.Helper()
	root := canonicalTempDir(t)
	installation := filepath.Join(root, "release")
	if err := os.Mkdir(installation, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "bin")
	directoryLink(t, alias, installation)
	return alias, installation
}

func directoryLink(t *testing.T, alias, target string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		if output, err := exec.Command(os.Getenv("ComSpec"), "/d", "/c", "mklink", "/J", alias, target).CombinedOutput(); err != nil {
			t.Fatalf("create installation junction: %v: %s", err, output)
		}
	} else if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
}

func TestManagedStateThroughLinkedInstallation(t *testing.T) {
	alias, installation := linkedInstallation(t)
	dir := filepath.Join(alias, "herdr-mesh-state")
	cfg := Config{Version: 1, Name: "test", Coordinator: true, Tailnet: "example.com", HerdrExecutable: "herdr"}
	if err := Save(dir, cfg); err != nil {
		t.Fatalf("initialize through installation link: %v", err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("probe unstarted installation: running=%v, err=%v", running, err)
	}
	for _, selected := range []string{dir, filepath.Join(installation, "herdr-mesh-state")} {
		if got, err := Load(selected); err != nil || got != cfg {
			t.Fatalf("load %s: %+v, %v", selected, got, err)
		}
	}
	if err := Save(dir, cfg); err == nil {
		t.Fatal("existing configuration overwritten")
	}
	if _, err := privateDir(alias, false); err == nil {
		t.Fatal("state root itself may not be a link")
	}
	store, err := state.Open(context.Background(), filepath.Join(dir, "test.db"), "server")
	if err != nil {
		t.Fatalf("open database through installation link: %v", err)
	}
	defer store.Close()
	if other, err := state.Open(context.Background(), filepath.Join(installation, "herdr-mesh-state", "test.db"), "server"); !errors.Is(err, state.ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("physical path bypassed database lock: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	guard, err := state.PrepareRoleState(context.Background(), dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	for _, selected := range []string{dir, filepath.Join(installation, "herdr-mesh-state")} {
		if running, err := IsRunning(selected); err != nil || !running {
			t.Fatalf("probe active installation %s: running=%v, err=%v", selected, running, err)
		}
	}
	if other, err := state.AcquireRoleState(context.Background(), filepath.Join(installation, "herdr-mesh-state"), "client"); !errors.Is(err, state.ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("physical path bypassed runtime lock: %v", err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("probe stopped installation: running=%v, err=%v", running, err)
	}
	if err := RecordStopped(context.Background(), dir); err != nil {
		t.Fatalf("record shutdown through installation link: %v", err)
	}
	if status, err := ReadStatus(dir); err != nil || status.State != "stopped" {
		t.Fatalf("read shutdown status: %+v, %v", status, err)
	}
}

func TestManagedRuntimeThroughLinkedInstallation(t *testing.T) {
	alias, _ := linkedInstallation(t)
	testManagedRuntimeAssignedDNS(t, "herdr-mesh-desktop.assigned-tail.test", filepath.Join(alias, "herdr-mesh-state"))
}

func TestLinkedInstallationKeepsPriorLexicalIPCAndShutdown(t *testing.T) {
	alias, installation := linkedInstallation(t)
	dir := filepath.Join(alias, "herdr-mesh-state")
	if err := Save(dir, Config{Version: 1, Name: "test", Server: "server.tail.ts.net:50052"}); err != nil {
		t.Fatal(err)
	}
	guard, err := state.PrepareRoleState(context.Background(), dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	// dev-d2e07bc selected IPC using the original, absolute lexical spelling.
	listener, err := listenIPC(dir)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterFleetServer(server, remoteFleet{protocol: protocol.SupportedRange()})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); <-done }()
	lifetime, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	join, err := startShutdownMonitor(lifetime, dir, stopRuntime)
	if err != nil {
		t.Fatal(err)
	}
	defer join()
	closed := make(chan error, 1)
	go func() { <-lifetime.Done(); closed <- guard.Close() }()
	if err := writeStatus(dir, Status{State: "ready", DNSName: "worker.tail.ts.net", Server: "server.tail.ts.net:50052"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := Dial(ctx, dir)
	if err != nil {
		t.Fatalf("connect to prior lexical endpoint: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if status, err := ReadStatus(dir); err != nil || status.State != "ready" {
		t.Fatalf("read prior runtime status: %+v, %v", status, err)
	}
	for _, selected := range []string{dir, filepath.Join(installation, "herdr-mesh-state")} {
		if running, err := IsRunning(selected); err != nil || !running {
			t.Fatalf("prior runtime ownership missed through %s: %v, %v", selected, running, err)
		}
	}
	if err := Shutdown(ctx, dir); err != nil {
		t.Fatalf("stop prior runtime through linked installation: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := RecordStopped(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if status, err := ReadStatus(dir); err != nil || status.State != "stopped" {
		t.Fatalf("record prior runtime shutdown: %+v, %v", status, err)
	}
	if _, err := Load(dir); err != nil {
		t.Fatalf("shutdown removed saved configuration: %v", err)
	}
}

func TestLinkedInstallationPolicyArtifactsAndPurge(t *testing.T) {
	alias, installation := linkedInstallation(t)
	dir := filepath.Join(alias, "herdr-mesh-state")
	config := Config{Version: 1, Name: "test", Coordinator: true, Tailnet: "example.com"}
	if err := Save(dir, config); err != nil {
		t.Fatal(err)
	}

	artifact := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(artifact, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := state.ProtectPrivatePath(artifact, false); err != nil {
		t.Fatal(err)
	}
	for _, selected := range []string{dir, filepath.Join(installation, "herdr-mesh-state")} {
		if data, err := ReadPrivateArtifact(selected, filepath.Join(selected, "policy.json"), 1024); err != nil || string(data) != "{}" {
			t.Fatalf("read policy artifact through %s: %q, %v", selected, data, err)
		}
	}
	if _, err := state.InspectMaintenance(context.Background(), dir, "node"); !errors.Is(err, state.ErrMaintenancePath) {
		t.Fatalf("offline maintenance accepted a linked installation ancestor: %v", err)
	}
	record := DestroyState{Configuration: config, Identity: ManagedIdentity{DeviceID: "nPinned"}, DeviceRemoved: true}
	if err := SaveDestroyState(dir, record); err != nil {
		t.Fatal(err)
	}
	if err := PurgeManaged(context.Background(), dir); err != nil {
		t.Fatalf("purge linked installation: %v", err)
	}
	if _, err := os.Stat(filepath.Join(installation, "herdr-mesh-state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("physical state root remains: %v", err)
	}
	if info, err := os.Stat(alias); err != nil || !info.IsDir() {
		t.Fatalf("installation link was removed: %v", err)
	}
}

func TestLinkedInstallationCleanupStillRejectsLinkedDescendants(t *testing.T) {
	alias, installation := linkedInstallation(t)
	dir := filepath.Join(alias, "herdr-mesh-state")
	if err := Save(dir, Config{Version: 1, Name: "test", Server: "server.tail.ts.net:50052"}); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(installation, "user-data")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(outside, "policy.json")
	if err := os.WriteFile(artifact, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := state.ProtectPrivatePath(artifact, false); err != nil {
		t.Fatal(err)
	}
	directoryLink(t, filepath.Join(dir, "linked"), outside)
	if _, err := ReadPrivateArtifact(dir, filepath.Join(dir, "linked", "policy.json"), 1024); err == nil {
		t.Fatal("accepted policy artifact through linked descendant")
	}
	for _, record := range []DestroyState{
		{Identity: ManagedIdentity{DeviceID: "nPinned"}, DeviceRemoved: true},
		{Configuration: Config{Version: 1, Name: "test", Server: "server.tail.ts.net:50052"}, LocalOnly: true},
	} {
		if err := SaveDestroyState(dir, record); err != nil {
			t.Fatal(err)
		}
		if err := PurgeManaged(context.Background(), dir); err == nil {
			t.Fatalf("accepted purge with linked descendant (local-only=%v)", record.LocalOnly)
		}
	}
	if _, err := Load(dir); err != nil {
		t.Fatalf("purge started deleting before validating the tree: %v", err)
	}
	if content, err := os.ReadFile(artifact); err != nil || string(content) != "preserved" {
		t.Fatalf("cleanup changed data outside state: %q, %v", content, err)
	}
}
