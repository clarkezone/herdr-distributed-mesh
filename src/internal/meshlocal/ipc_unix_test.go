//go:build !windows

package meshlocal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnixManagedIPCAndFilesArePrivate(t *testing.T) {
	dir := canonicalTempDir(t)
	if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	if err := writeStatus(dir, Status{State: "starting"}); err != nil {
		t.Fatal(err)
	}
	listener, err := listenIPC(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	socket, err := ipcSocketPath(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, filepath.Join(dir, "config.json"), filepath.Join(dir, "status.json"), socket} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if path == dir {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Fatalf("%s mode = %o, want %o", path, info.Mode().Perm(), want)
		}
	}
}
