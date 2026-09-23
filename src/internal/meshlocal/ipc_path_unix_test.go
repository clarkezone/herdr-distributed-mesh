//go:build !windows

package meshlocal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestUnixLongCanonicalStateRootUsesPrivateShortSocket(t *testing.T) {
	base := canonicalTempDir(t)
	dir := filepath.Join(base, "managed-"+strings.Repeat("x", 120))
	if err := Save(dir, Config{Version: 1, Name: "desktop", Tailnet: "example.test", Coordinator: true}); err != nil {
		t.Fatal(err)
	}
	if len(filepath.Join(dir, "fleet.sock")) < len(unix.RawSockaddrUnix{}.Path) {
		t.Fatal("fixture does not exceed the platform Unix socket path limit")
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
	if len(socket) >= 104 || strings.HasPrefix(socket, dir+string(filepath.Separator)) {
		t.Fatalf("socket did not use a macOS-safe short path: %q", socket)
	}
	for _, path := range []string{filepath.Dir(socket), socket} {
		var info unix.Stat_t
		if err := unix.Lstat(path, &info); err != nil {
			t.Fatal(err)
		}
		if info.Uid != uint32(os.Geteuid()) || info.Mode&0077 != 0 {
			t.Fatalf("long-path fallback is not private and owner-held: %s", path)
		}
	}
	other := filepath.Join(base, "managed-"+strings.Repeat("y", 120))
	otherSocket, err := ipcSocketPath(other, false)
	if err != nil || otherSocket == socket {
		t.Fatalf("different managed roots share a socket: %q %v", otherSocket, err)
	}
	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			_, err = io.WriteString(connection, "ok")
			_ = connection.Close()
		}
		accepted <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, err := dialIPC(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var response [2]byte
	if _, err := io.ReadFull(connection, response[:]); err != nil || string(response[:]) != "ok" {
		t.Fatalf("long-root socket connection failed: %q %v", response, err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("closed long-root listener left its socket behind: %v", err)
	}
}
