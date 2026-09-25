package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/setup"
)

func TestSetupCLIFlagsPreviewDefaultAndApply(t *testing.T) {
	for _, apply := range []bool{false, true} {
		var out bytes.Buffer
		args := []string{"tailnet", "-tailnet", "example.com", "-output-directory", t.TempDir(), "-dashboard-port", "8787",
			"-api-token-env", "MY_API_TOKEN", "-tag-owner", "group:operators", "-keys-per-role", "3", "-key-expiry-seconds", "3600", "-json"}
		if apply {
			args = append(args, "-apply")
		}
		called := false
		err := runSetup(context.Background(), args, IO{Out: &out, Err: &out}, func(_ context.Context, o setup.Options) (setup.Report, error) {
			called = true
			if o.Apply != apply || o.DashboardPort == nil || *o.DashboardPort != 8787 || o.ApiTokenEnvironmentVariable != "MY_API_TOKEN" ||
				o.TagOwner != "group:operators" || o.KeysPerRole != 3 || o.KeyExpirySeconds != 3600 {
				t.Fatalf("options=%+v", o)
			}
			mode := "preview"
			if apply {
				mode = "applied"
			}
			return setup.Report{Mode: mode}, nil
		})
		if err != nil || !called || !strings.Contains(out.String(), `"mode"`) {
			t.Fatalf("err=%v output=%s", err, out.String())
		}
	}
}

func TestSetupCLIRejectsInvalidFlagsWithoutEchoingSecrets(t *testing.T) {
	for _, args := range [][]string{
		{}, {"other"}, {"tailnet"}, {"tailnet", "-tailnet", "example.com", "-dashboard-port", "0"},
		{"tailnet", "-tailnet", "example.com", "-dashboard-port", "65536"},
		{"tailnet", "-tailnet", "example.com", "-dashboard-port", "50052"},
		{"tailnet", "-tailnet", "example.com", "-keys-per-role", "0"},
		{"tailnet", "-tailnet", "example.com", "-timeout", "31m"},
		{"tailnet", "-tailnet", "example.com", "-api-token", "tskey-api-secret"},
		{"tailnet", "-tailnet", "example.com", "-timeout", "tskey-api-secret"},
		{"tailnet", "-tailnet", "example.com", "tskey-api-secret"},
		{"tailnet", "-tailnet", "example.com", "-api-token-env", "tskey-api-secret"},
	} {
		var out bytes.Buffer
		err := runSetup(context.Background(), args, IO{Out: &out, Err: &out}, func(context.Context, setup.Options) (setup.Report, error) {
			t.Fatal("invalid flags invoked workflow")
			return setup.Report{}, nil
		})
		if err == nil || strings.Contains(err.Error(), "tskey-") || strings.Contains(out.String(), "tskey-") {
			t.Fatalf("unsafe rejection: %v %s", err, out.String())
		}
	}
}

func TestSetupCLIHelpDoesNotStartWorkflow(t *testing.T) {
	var out bytes.Buffer
	err := runSetup(context.Background(), []string{"tailnet", "-help"}, IO{Out: &out, Err: &out}, func(context.Context, setup.Options) (setup.Report, error) {
		t.Fatal("help invoked workflow")
		return setup.Report{}, nil
	})
	if err != nil || !strings.Contains(out.String(), "PREVIEW") || !strings.Contains(out.String(), "api-token-env") {
		t.Fatalf("help: %v %s", err, out.String())
	}
}
