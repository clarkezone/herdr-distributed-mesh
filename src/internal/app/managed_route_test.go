package app

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func isolatedManagedConfig(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("APPDATA", root)
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	return root
}

func TestManagedHelpDoesNotCreateStateOrRequireEnrollment(t *testing.T) {
	root := isolatedManagedConfig(t)
	t.Setenv("TAILSCALE_API_TOKEN", "")
	t.Setenv("TS_AUTHKEY", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, args := range [][]string{
		{"init", "--help"},
		{"join", "--help"},
		{"nodes", "--help"},
		{"status", "--help"},
		{"doctor", "--help"},
		{"dashboard", "--help"},
		{"mcp", "--help"},
		{"project", "add", "demo", "--help"},
		{"agent", "start", "smoke", "--help"},
		{"agent", "follow", "smoke", "--help"},
		{"agent", "stop", "smoke", "--help"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, diagnostics bytes.Buffer
			err := Run(ctx, args, IO{In: strings.NewReader(""), Out: &out, Err: &diagnostics})
			if !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("help attempted configuration or execution: %v", err)
			}
			if out.Len()+diagnostics.Len() == 0 {
				t.Fatal("help produced no instructions")
			}
		})
	}
	files, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatal("help created configuration or runtime state")
	}
}
