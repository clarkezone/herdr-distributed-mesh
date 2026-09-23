package app

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortableStartAndManagedStateSelection(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing-state")
	for _, command := range []string{"start", "nodes", "status", "dashboard", "mcp"} {
		var out bytes.Buffer
		err := Run(context.Background(), []string{"--state-dir", root, command}, IO{Out: &out, Err: &out})
		if err == nil || !strings.Contains(err.Error(), root) {
			t.Fatalf("%s did not use explicit managed state: %v", command, err)
		}
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("read/start without configuration created state")
	}
	var out bytes.Buffer
	if err := Run(context.Background(), []string{"--state-dir", root, "shutdown"}, IO{Out: &out, Err: &out}); err != nil ||
		!strings.Contains(out.String(), "No managed installation") {
		t.Fatalf("shutdown did not use selected directory: %v %s", err, &out)
	}
	for _, args := range [][]string{
		{"--state-dir", "relative", "start"}, {"start", "extra"}, {"start", "--timeout", "0s"},
		{"--state-dir", root, "server"}, {"--state-dir", root, "dashboard", "--server", "server:50052"},
	} {
		if err := Run(context.Background(), args, IO{Out: io.Discard, Err: io.Discard}); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
}

func TestAdvancedDefaultStateIsPortable(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	network, err := addNetworkFlags(flags, "node", "node", "UNUSED", "")
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(filepath.Dir(exe), "herdr-mesh-state", "advanced", "node"); network.stateDir != want {
		t.Fatalf("got %s want %s", network.stateDir, want)
	}
}
