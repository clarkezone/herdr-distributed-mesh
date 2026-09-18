package onboard

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/setup"
)

type fixture struct {
	d                                                                                Dependencies
	dir                                                                              string
	cfg                                                                              meshlocal.Config
	saved, starts, registrations, prompts, confirms, policyCalls, verified, browsers int
	token                                                                            []byte
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{dir: filepath.Join(t.TempDir(), "managed"), token: []byte("tskey-api-hidden-test")}
	f.d = Dependencies{
		Dir: func() (string, error) { return f.dir, nil },
		Load: func(string) (meshlocal.Config, error) {
			if f.saved == 0 {
				return meshlocal.Config{}, os.ErrNotExist
			}
			return f.cfg, nil
		},
		Save: func(dir string, cfg meshlocal.Config) error { f.saved++; f.cfg = cfg; return os.MkdirAll(dir, 0700) },
		Prerequisites: func(value string) error {
			if value != "herdr" {
				t.Fatalf("default herdr = %q", value)
			}
			return nil
		},
		Executable: func() (string, error) { return filepath.Join(t.TempDir(), "herdr-mesh.exe"), nil },
		Register:   func(string, string) error { f.registrations++; return nil },
		Start: func(_ string, _ string, env []string) error {
			f.starts++
			if !reflect.DeepEqual(env, []string{"PATH=provider", "ANTHROPIC_API_KEY=provider-secret"}) {
				t.Fatalf("daemon env = %v", env)
			}
			return nil
		},
		Running: func(string) (bool, error) { return f.starts > 0, nil },
		Status: func(string) (meshlocal.Status, error) {
			return meshlocal.Status{State: "ready", DNSName: "herdr-mesh-desktop.actual-tail.ts.net"}, nil
		},
		Verify:  func(context.Context, string) error { f.verified++; return nil },
		Token:   func(context.Context) ([]byte, error) { f.prompts++; return f.token, nil },
		Confirm: func(context.Context) (bool, error) { f.confirms++; return true, nil },
		Browser: func(string) error { f.browsers++; return errors.New("no browser") },
		Wait:    func(context.Context) error { return nil },
		Environment: func() []string {
			return []string{"PATH=provider", "TAILSCALE_API_TOKEN=tskey-api-secret", "TS_AUTHKEY_SERVER=tskey-auth-secret", "CUSTOM=tskey-auth-secret", "ANTHROPIC_API_KEY=provider-secret"}
		},
	}
	f.d.Policy = func(_ context.Context, o setup.Options, token []byte) (setup.Report, error) {
		f.policyCalls++
		if !o.PolicyOnly || o.Tailnet != "example.com" || string(token) != "tskey-api-hidden-test" {
			t.Fatalf("unsafe policy options=%+v token len=%d", o, len(token))
		}
		if err := os.MkdirAll(o.OutputDirectory, 0700); err != nil {
			return setup.Report{}, err
		}
		backup, proposal := filepath.Join(o.OutputDirectory, "backup.json"), filepath.Join(o.OutputDirectory, "proposal.json")
		if err := os.WriteFile(backup, []byte(`{"unrelated":"preserved"}`), 0600); err != nil {
			return setup.Report{}, err
		}
		if err := os.WriteFile(proposal, []byte(`{"unrelated":"preserved","grants":["preview"]}`), 0600); err != nil {
			return setup.Report{}, err
		}
		if o.Apply && len(o.ExpectedPolicySHA256) != 64 {
			t.Fatal("apply not fenced to preview")
		}
		return setup.Report{PolicyBackup: backup, PolicyProposal: proposal, Warnings: []string{"policy_round_trip", "wildcard_allow_preserved"}}, nil
	}
	return f
}

func initOptions() Options {
	return Options{Name: "desktop", Tailnet: "example.com", Coordinator: true}
}
func joinOptions() Options {
	return Options{Name: "laptop", Server: "herdr-mesh-desktop.actual-tail.ts.net"}
}

func TestInitPolicyOnlyThenActualDNSAndExactResume(t *testing.T) {
	f := newFixture(t)
	var out bytes.Buffer
	if err := Run(context.Background(), initOptions(), &out, f.d); err != nil {
		t.Fatal(err)
	}
	if f.saved != 1 || f.starts != 1 || f.prompts != 1 || f.policyCalls != 2 || f.verified != 1 {
		t.Fatalf("%+v", f)
	}
	if strings.Trim(string(f.token), "\x00") != "" {
		t.Fatal("prompt token buffer retained")
	}
	want := "herdr-mesh join --server herdr-mesh-desktop.actual-tail.ts.net --name <choose-name>"
	if !strings.Contains(out.String(), want) || strings.Contains(out.String(), ":50052") || strings.Contains(out.String(), "tskey-api-hidden-test") {
		t.Fatalf("bad output: %s", &out)
	}
	f.d.Running = func(string) (bool, error) { return true, nil }
	if err := Run(context.Background(), initOptions(), &out, f.d); err != nil {
		t.Fatal(err)
	}
	if f.saved != 1 || f.starts != 1 || f.prompts != 1 || f.policyCalls != 2 {
		t.Fatal("resume duplicated setup/identity/daemon")
	}
}

func TestJoinBrowserApprovalProgressThenVerify(t *testing.T) {
	f := newFixture(t)
	states := []meshlocal.Status{
		{State: "needs_login", AuthURL: "https://login.tailscale.com/a/mock"},
		{State: "needs_machine_auth", AuthURL: "https://login.tailscale.com/a/mock", Error: "Device approval is required"},
		{State: "ready", DNSName: "herdr-mesh-laptop.actual-tail.ts.net"},
	}
	i := 0
	f.d.Status = func(string) (meshlocal.Status, error) { v := states[i]; i++; return v, nil }
	var out bytes.Buffer
	if err := Run(context.Background(), joinOptions(), &out, f.d); err != nil {
		t.Fatal(err)
	}
	if f.prompts != 0 || f.policyCalls != 0 || f.browsers != 1 || f.verified != 1 || f.cfg.Server != joinOptions().Server+":50052" {
		t.Fatalf("%+v", f)
	}
	for _, text := range []string{"Browser unavailable?", "Could not open", "Device approval is required", "Ready: herdr-mesh-laptop"} {
		if !strings.Contains(out.String(), text) {
			t.Fatalf("missing %q: %s", text, &out)
		}
	}
}

func TestPolicyRefusalAndUnknownApplyPreserveState(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "refusal", true: "unknown"}[uncertain], func(t *testing.T) {
			f := newFixture(t)
			if uncertain {
				policy := f.d.Policy
				f.d.Policy = func(ctx context.Context, o setup.Options, token []byte) (setup.Report, error) {
					if o.Apply {
						f.policyCalls++
						return setup.Report{}, &setup.Error{Code: "timeout", RemoteEffectsUnknown: true}
					}
					return policy(ctx, o, token)
				}
			} else {
				f.d.Confirm = func(context.Context) (bool, error) { return false, nil }
			}
			err := Run(context.Background(), initOptions(), io.Discard, f.d)
			if err == nil || f.starts != 0 || f.registrations != 0 {
				t.Fatalf("err=%v %+v", err, f)
			}
			if uncertain {
				if _, err := os.Stat(filepath.Join(f.dir, "policy-apply-pending")); err != nil {
					t.Fatal(err)
				}
				calls := f.policyCalls
				if err := Run(context.Background(), initOptions(), io.Discard, f.d); err == nil || !strings.Contains(err.Error(), "unknown") {
					t.Fatal(err)
				}
				if f.policyCalls != calls || f.prompts != 1 {
					t.Fatal("retried unknown mutation")
				}
			}
		})
	}
}

func TestConfigurationConflictAndAdvancedStateRefused(t *testing.T) {
	f := newFixture(t)
	f.saved = 1
	f.cfg = meshlocal.Config{Name: "other"}
	if err := Run(context.Background(), joinOptions(), io.Discard, f.d); err == nil || !strings.Contains(err.Error(), "different managed identity") {
		t.Fatal(err)
	}
	if f.saved != 1 || f.starts != 0 || f.prompts != 0 {
		t.Fatal("conflict had effects")
	}
	f.saved = 0
	if err := os.Mkdir(filepath.Join(filepath.Dir(f.dir), "ctl"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), joinOptions(), io.Discard, f.d); err == nil || !strings.Contains(err.Error(), "advanced mesh state") {
		t.Fatal(err)
	}
	if f.saved != 0 {
		t.Fatal("advanced state migrated")
	}
}

func TestCancellationAndFalseReadiness(t *testing.T) {
	for _, mode := range []string{"canceled", "unknown-dns", "failed", "ipc-failed", "approval-canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "canceled":
				cancel()
			case "unknown-dns":
				f.d.Status = func(string) (meshlocal.Status, error) {
					return meshlocal.Status{State: "ready", DNSName: "invented"}, nil
				}
			case "failed":
				f.d.Status = func(string) (meshlocal.Status, error) {
					return meshlocal.Status{State: "error", Error: "wrong coordinator role"}, nil
				}
			case "ipc-failed":
				f.d.Verify = func(context.Context, string) error { return errors.New("stale status") }
			case "approval-canceled":
				f.d.Status = func(string) (meshlocal.Status, error) { return meshlocal.Status{State: "needs_machine_auth"}, nil }
				f.d.Wait = func(context.Context) error { return context.Canceled }
			}
			var out bytes.Buffer
			if err := Run(ctx, joinOptions(), &out, f.d); err == nil {
				t.Fatal("expected error")
			}
			if strings.Contains(out.String(), "Ready:") {
				t.Fatal("false readiness")
			}
			if mode == "canceled" && f.saved != 0 {
				t.Fatal("canceled run saved state")
			}
		})
	}
}

func TestNamesServerAndRedirectedSecretValidation(t *testing.T) {
	for _, name := range []string{"", "Desktop", "a--b", "../bad", "con", "a-", strings.Repeat("a", 41)} {
		o := joinOptions()
		o.Name = name
		if _, err := o.Normalize(); err == nil {
			t.Fatalf("accepted name %q", name)
		}
	}
	for _, server := range []string{"server", "https://server.tail.ts.net", "server.tail.ts.net:0", "server.tail.ts.net:65536", "127.0.0.1", "evil.ts.net", "server.tail.ts.net&evil"} {
		o := joinOptions()
		o.Server = server
		if _, err := o.Normalize(); err == nil {
			t.Fatalf("accepted server %q", server)
		}
	}
	o := joinOptions()
	o.Server += ":50052"
	if normalized, err := o.Normalize(); err != nil || normalized.Server != joinOptions().Server {
		t.Fatalf("%+v %v", normalized, err)
	}
	if _, err := ReadToken(context.Background(), strings.NewReader("secret\n"), io.Discard); err == nil {
		t.Fatal("redirected token accepted")
	}
	for _, u := range []string{"https://evil.example/a/key", "http://login.tailscale.com/a/key", "https://login.tailscale.com@evil.example/a/key"} {
		if validAuthURL(u) {
			t.Fatalf("unsafe browser URL %s", u)
		}
	}
}
