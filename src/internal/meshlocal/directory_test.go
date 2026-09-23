package meshlocal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPortableStateDirectoryIsBesideExecutableNotWorkingDirectory(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	dir, err := DefaultDir()
	if err != nil || dir != filepath.Join(filepath.Dir(exe), "herdr-mesh-state") {
		t.Fatalf("wrong portable default: %s, %v", dir, err)
	}
	ctx := context.Background()
	if got, err := StateDir(ctx); err != nil || got != dir {
		t.Fatalf("default resolution: %s, %v", got, err)
	}
	custom := filepath.Join(t.TempDir(), "selected")
	selected, err := WithStateDir(ctx, custom)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := StateDir(selected); err != nil || got != custom {
		t.Fatalf("explicit resolution: %s, %v", got, err)
	}
	if got, _ := StateDir(ctx); got != dir {
		t.Fatal("explicit choice changed unrelated command context")
	}
	if _, err := os.Stat(custom); !os.IsNotExist(err) {
		t.Fatal("state directory selection had filesystem effects")
	}
}

func TestExplicitStateDirectoryRequiresSafeAbsoluteNonRoot(t *testing.T) {
	for _, dir := range []string{"", "relative", filepath.VolumeName(t.TempDir()) + string(filepath.Separator), t.TempDir() + "\n"} {
		if _, err := WithStateDir(context.Background(), dir); err == nil {
			t.Fatalf("invalid directory accepted: %q", dir)
		}
	}
}
