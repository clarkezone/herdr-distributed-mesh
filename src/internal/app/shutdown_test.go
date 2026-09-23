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

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

func TestClientDestroyCLIWithoutInputOrCredentials(t *testing.T) {
	root := filepath.Join(maintenanceAppTempDir(t), "client")
	if err := meshlocal.Save(root, meshlocal.Config{Version: 1, Name: "laptop", Server: "controller.tail.ts.net:50052"}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := Run(context.Background(), []string{"--state-dir", root, "shutdown", "--destroy", "--yes"},
		IO{Out: &output, Err: &output}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("client state was not purged: %v", err)
	}
	if strings.Contains(output.String(), "Tailscale API token (hidden)") ||
		!strings.Contains(output.String(), "device entry may remain") {
		t.Fatalf("unexpected client output: %s", &output)
	}
}

func TestShutdownFlagsAreExplicitAndHelpHasNoEffects(t *testing.T) {
	for _, test := range []struct {
		args []string
		want onboard.ShutdownOptions
	}{
		{nil, onboard.ShutdownOptions{}},
		{[]string{"--dry-run"}, onboard.ShutdownOptions{DryRun: true}},
		{[]string{"--destroy", "--remove-policy", "--yes", "--api-token-env", "PRIVATE_API_TOKEN"},
			onboard.ShutdownOptions{Destroy: true, RemovePolicy: true, Yes: true, TokenEnv: "PRIVATE_API_TOKEN"}},
	} {
		calls := 0
		var out bytes.Buffer
		err := runShutdownWith(context.Background(), test.args, IO{Out: &out, Err: &out},
			func(ctx context.Context, o onboard.ShutdownOptions) error {
				calls++
				if o != test.want {
					t.Fatalf("unexpected shutdown scope: %+v", o)
				}

				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded shutdown")
				}
				return nil
			})
		if err != nil || calls != 1 {
			t.Fatal(err, calls)
		}
	}
	for _, args := range [][]string{{"--help"}, {"--remove-policy"}, {"--yes"}, {"--api-token-env", "TOKEN"}, {"--timeout", "0s"}, {"agent"}, {"--destroy", "--unknown"}} {
		var out bytes.Buffer
		err := runShutdownWith(context.Background(), args, IO{Out: &out, Err: &out}, func(context.Context, onboard.ShutdownOptions) error {
			t.Fatal("help or invalid flags caused effects")
			return nil
		})
		if err == nil || (args[0] == "--help" && !errors.Is(err, flag.ErrHelp)) {
			t.Fatal("invalid/help flags accepted", err)
		}
	}
}
