package meshlocal

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func linkedInstallation(t *testing.T) (string, string) {
	t.Helper()
	root := canonicalTempDir(t)
	installation := filepath.Join(root, "release")
	if err := os.Mkdir(installation, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "bin")
	if runtime.GOOS == "windows" {
		if output, err := exec.Command(os.Getenv("ComSpec"), "/d", "/c", "mklink", "/J", alias, installation).CombinedOutput(); err != nil {
			t.Fatalf("create installation junction: %v: %s", err, output)
		}
	} else if err := os.Symlink(installation, alias); err != nil {
		t.Fatal(err)
	}
	return alias, installation
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
