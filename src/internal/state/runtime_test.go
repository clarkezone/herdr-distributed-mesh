package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareRoleStateCreatesAndSerializesRuntime(t *testing.T) {
	root := filepath.Join(t.TempDir(), "new", "node")
	guard, err := PrepareRoleState(context.Background(), root, "node")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if _, err := os.Stat(filepath.Join(root, "instance-id")); !os.IsNotExist(err) {
		t.Fatal("preparation accessed instance identity")
	}
	if _, err := PrepareRoleState(context.Background(), root, "node"); !errors.Is(err, ErrLocked) {
		t.Fatalf("concurrent runtime admitted: %v", err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(root, maintenanceRoleLock))
	if err != nil || string(marker) == maintenanceLockVersion+"node\n" {
		t.Fatal("preparation implicitly activated backup contract")
	}
	next, err := PrepareRoleState(context.Background(), root, "node")
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.Activate(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareRoleStateCancellationAndUnsafeRoots(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareRoleState(ctx, root, "server"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("canceled startup created role state")
	}
	volumeRoot := filepath.VolumeName(root) + string(filepath.Separator)
	for _, path := range []string{"", volumeRoot, `\\host\share\state`} {
		if _, err := PrepareRoleState(context.Background(), path, "server"); err == nil {
			t.Fatalf("unsafe role root accepted: %q", path)
		}
	}
	if _, err := PrepareRoleState(context.Background(), root, "unsupported"); err == nil {
		t.Fatal("invalid role accepted")
	}
}

func TestPrepareRoleStateDoesNotFollowDirectoryAliases(t *testing.T) {
	parent := t.TempDir()
	target := t.TempDir()
	link := filepath.Join(parent, "alias")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	if _, err := PrepareRoleState(context.Background(), filepath.Join(link, "new"), "client"); err == nil {
		t.Fatal("role preparation followed a directory alias")
	}
	if _, err := os.Stat(filepath.Join(target, "new")); !os.IsNotExist(err) {
		t.Fatal("rejected alias created state in its target")
	}
}
