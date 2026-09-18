//go:build !windows

package meshlocal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnixManagedIPCAndFilesArePrivate(t *testing.T) {
	dir := t.TempDir()
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
	for _, name := range []string{"", "config.json", "status.json", "fleet.sock"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if name == "" {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Fatalf("%s mode = %o, want %o", name, info.Mode().Perm(), want)
		}
	}
}
