//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package state

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestUnixPrivateDirectoryAndFiles(t *testing.T) {
	path := testPath(t)
	ancestor := filepath.Dir(filepath.Dir(path))
	requireOK(t, os.Chmod(ancestor, 0755))
	initial := openTestStore(t, path)
	requireOK(t, initial.Close())
	requireOK(t, os.Chmod(filepath.Dir(path), 0755))
	for _, file := range []string{path, path + ".lock"} {
		requireOK(t, os.Chmod(file, 0644))
	}
	s := openTestStore(t, path)
	requireOK(t, s.Bind(context.Background(), "stable", "instance"))
	for _, entry := range []struct {
		path string
		mode os.FileMode
	}{
		{ancestor, 0755}, {filepath.Dir(path), 0700},
		{path, 0600}, {path + ".lock", 0600}, {path + "-wal", 0600}, {path + "-shm", 0600},
	} {
		info, err := os.Stat(entry.path)
		requireOK(t, err)
		if got := info.Mode().Perm(); got != entry.mode {
			t.Fatalf("mode %s = %o, want %o", entry.path, got, entry.mode)
		}
	}
}
