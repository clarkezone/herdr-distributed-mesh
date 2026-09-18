package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareRoleStateCreatesAndSerializesRuntime(t *testing.T) {
	root := filepath.Join(maintenanceTempDir(t), "new", "node")
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
	parent := maintenanceTempDir(t)
	t.Chdir(parent)
	root := filepath.Join(parent, "not-created")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareRoleState(ctx, root, "server"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("canceled startup created role state")
	}
	volumeRoot := filepath.VolumeName(root) + string(filepath.Separator)
	for _, path := range []string{
		"", volumeRoot, `\\host\share\state`, "//host/share/state", `\\?\C:\state`, `\\.\C:\state`,
		root + string(filepath.Separator) + ".." + string(filepath.Separator) + "escaped",
		`relative\..\state`, "relative/../state",
	} {
		if guard, err := PrepareRoleState(context.Background(), path, "server"); !errors.Is(err, ErrMaintenancePath) {
			if guard != nil {
				_ = guard.Close()
			}
			t.Fatalf("unsafe role root accepted: %q: %v", path, err)
		}
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("unsafe startup created role state")
	}
	if _, err := PrepareRoleState(context.Background(), root, "unsupported"); err == nil {
		t.Fatal("invalid role accepted")
	}
	entries, err := os.ReadDir(parent)
	requireOK(t, err)
	if len(entries) != 0 {
		t.Fatal("unsafe startup created files or directories")
	}
}

func TestPrepareRoleStateDoesNotFollowDirectoryAliases(t *testing.T) {
	parent := maintenanceTempDir(t)
	target := maintenanceTempDir(t)
	link := filepath.Join(parent, "alias")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	for _, root := range []string{link, filepath.Join(link, "new"), filepath.Join(link, "new", "nested")} {
		if guard, err := PrepareRoleState(context.Background(), root, "client"); !errors.Is(err, ErrMaintenancePath) {
			if guard != nil {
				_ = guard.Close()
			}
			t.Fatalf("role preparation followed a directory alias: %v", err)
		}
	}
	if _, err := os.Stat(filepath.Join(target, "new")); !os.IsNotExist(err) {
		t.Fatal("rejected alias created state in its target")
	}
	if _, err := os.Stat(filepath.Join(target, maintenanceRoleLock)); !os.IsNotExist(err) {
		t.Fatal("rejected alias created a role lock in its target")
	}
}

func TestPrepareRoleStateAcceptsLocalRelativeDirectory(t *testing.T) {
	parent := maintenanceTempDir(t)
	t.Chdir(parent)
	guard, err := PrepareRoleState(context.Background(), filepath.Join("new", "node"), "node")
	requireOK(t, err)
	defer guard.Close()
	if _, err := os.Stat(filepath.Join(parent, "new", "node", maintenanceRoleLock)); err != nil {
		t.Fatalf("relative role directory was not created: %v", err)
	}
}
