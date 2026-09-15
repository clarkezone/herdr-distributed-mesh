//go:build !windows

package herdr

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestUnixLocalAddress(t *testing.T) {
	for _, path := range []string{"/tmp/herdr.sock", "herdr.sock"} {
		if got, err := localAddress(path); err != nil || got != path {
			t.Errorf("local socket path rejected: %v", err)
		}
	}
	for _, path := range []string{"", "//remote/pipe/herdr", `\\remote\pipe\herdr`, "\x00abstract", "/tmp/a\n"} {
		if _, err := localAddress(path); err == nil {
			t.Error("invalid local path accepted")
		}
	}
}

func TestUnixDialLocal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "herdr.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := dialLocal(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	peer.Close()
	cancel()
	if conn, err := dialLocal(ctx, path); err == nil {
		conn.Close()
		t.Fatal("canceled dial succeeded")
	}
}
