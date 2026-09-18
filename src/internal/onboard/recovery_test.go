package onboard

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInterruptedStateAndLauncherFailureNeverReplaceIdentity(t *testing.T) {
	for _, scenario := range []string{"orphan-state", "registration", "launch", "lock-never-acquired", "status-read"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			switch scenario {
			case "orphan-state":
				if err := os.MkdirAll(f.dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(f.dir, "identity.json"), []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			case "registration":
				f.d.Register = func(string, string) error { return errors.New("owned entry conflict") }
			case "launch":
				f.d.Start = func(string, string, []string) error { return errors.New("launch refused") }
			case "lock-never-acquired":
				f.d.Running = func(string) (bool, error) { return false, nil }
			case "status-read":
				f.d.Status = nil
				f.d.Running = func(string) (bool, error) { return false, errors.New("private lock unavailable") }
			}
			err := Run(context.Background(), joinOptions(), io.Discard, f.d)
			if err == nil {
				t.Fatal("failure reported success")
			}
			if scenario == "orphan-state" && f.saved != 0 {
				t.Fatal("orphaned identity replaced")
			}
			if scenario == "lock-never-acquired" && !strings.Contains(err.Error(), "did not acquire") {
				t.Fatal(err)
			}
			if f.prompts != 0 || f.policyCalls != 0 {
				t.Fatal("join attempted policy API")
			}
		})
	}
}

func TestDaemonSecretFilterPreservesProviderAuthentication(t *testing.T) {
	t.Setenv("TS_AUTHKEY_CLIENT", "tskey-auth-private")
	t.Setenv("TAILSCALE_API_TOKEN", "tskey-api-private")
	t.Setenv("CUSTOM_TAILNET_CREDENTIAL", "tskey-api-private")
	t.Setenv("OPENAI_API_KEY", "provider-private")
	if err := ClearDaemonSecrets(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TS_AUTHKEY_CLIENT", "TAILSCALE_API_TOKEN", "CUSTOM_TAILNET_CREDENTIAL"} {
		if _, ok := os.LookupEnv(name); ok {
			t.Fatalf("retained %s", name)
		}
	}
	if os.Getenv("OPENAI_API_KEY") != "provider-private" {
		t.Fatal("provider environment removed")
	}
}

func TestConfirmationIsExplicitAndCancellationWins(t *testing.T) {
	for _, value := range []string{"", "n\n", "YES\n", "y\n", "maybe\n"} {
		got, err := confirm(context.Background(), strings.NewReader(value))
		if err != nil || got != (value == "YES\n" || value == "y\n") {
			t.Fatalf("input=%q got=%v err=%v", value, got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := confirm(ctx, strings.NewReader("")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
