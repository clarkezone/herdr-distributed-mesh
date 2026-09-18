package meshlocal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

func TestOwnershipProbeRecognizesStartupWithoutTrustingStatus(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "managed")
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("missing runtime reported live: %t %v", running, err)
	}
	if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("configured runtime reported live: %t %v", running, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "role-state.lock")); !os.IsNotExist(err) {
		t.Fatalf("probe initialized an ownership marker: %v", err)
	}
	guard, err := state.PrepareRoleState(context.Background(), dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if err := writeStatus(dir, Status{State: "login_required", AuthURL: "https://login.tailscale.com/a/private"}); err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err != nil || !running {
		t.Fatalf("login-stage owner not recognized: %t %v", running, err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writeStatus(dir, Status{State: "ready"}); err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("stale status mistaken for live ownership: %t %v", running, err)
	}
	path := filepath.Join(dir, "role-state.lock")
	if err := os.WriteFile(path, []byte("herdr-mesh-role-lock-pen"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err != nil || running {
		t.Fatalf("interrupted stopped marker probe failed: %t %v", running, err)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "herdr-mesh-role-lock-pen" {
		t.Fatalf("read-only probe normalized interrupted state: %q %v", content, err)
	}
	after, err := os.Stat(path)
	if err != nil || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("read-only probe modified state timestamp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "role-state.lock"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if running, err := IsRunning(dir); err == nil || running {
		t.Fatalf("corrupt ownership marker hidden: %t %v", running, err)
	}
}
