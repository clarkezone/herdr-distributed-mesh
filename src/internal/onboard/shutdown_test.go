package onboard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
)

type cleanupFixture struct {
	calls                    []string
	deviceError, policyError error
}

func (*cleanupFixture) Close() {}

func (f *cleanupFixture) DeleteDevice(context.Context) error {
	f.calls = append(f.calls, "device")
	return f.deviceError
}
func (f *cleanupFixture) RemovePolicy(context.Context) error {
	f.calls = append(f.calls, "policy")
	return f.policyError
}

func shutdownFixture(t *testing.T) (string, ShutdownDependencies, *cleanupFixture) {
	t.Helper()
	return shutdownFixtureFor(t, meshlocal.Config{Version: 1, Name: "desktop", Coordinator: true, Tailnet: "example.com"})
}

func shutdownFixtureFor(t *testing.T, config meshlocal.Config) (string, ShutdownDependencies, *cleanupFixture) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "managed")
	if err := meshlocal.Save(root, config); err != nil {
		t.Fatal(err)
	}
	f := &cleanupFixture{}
	d := ShutdownDependencies{
		Dir: func() (string, error) { return root, nil },
		Identity: func(context.Context, string) (meshlocal.ManagedIdentity, error) {
			return meshlocal.ManagedIdentity{DeviceID: "nPinned", DNSName: "desktop.tail.ts.net"}, nil
		},
		Stop:    func(context.Context, string) error { f.calls = append(f.calls, "stop"); return nil },
		Confirm: func(context.Context, string) (bool, error) { return true, nil },
		Token:   func(context.Context, string) ([]byte, error) { return []byte("tskey-api-private-test"), nil },
		Remote: func(context.Context, string, meshlocal.Config, meshlocal.ManagedIdentity, bool, []byte) (RemoteCleanup, error) {
			f.calls = append(f.calls, "preflight")
			return f, nil
		},
		Purge: func(ctx context.Context, root string) error {
			f.calls = append(f.calls, "purge")
			return meshlocal.PurgeManaged(ctx, root)
		},
	}
	return root, d, f
}

func clientShutdownFixture(t *testing.T) (string, ShutdownDependencies, *cleanupFixture) {
	t.Helper()
	root, d, f := shutdownFixtureFor(t, meshlocal.Config{Version: 1, Name: "laptop", Server: "controller.tail.ts.net:50052"})
	d.Identity = func(context.Context, string) (meshlocal.ManagedIdentity, error) {
		t.Fatal("client cleanup required a remote identity")
		return meshlocal.ManagedIdentity{}, nil
	}
	d.Token = func(context.Context, string) ([]byte, error) {
		t.Fatal("client cleanup requested an API token")
		return nil, nil
	}
	d.Remote = func(context.Context, string, meshlocal.Config, meshlocal.ManagedIdentity, bool, []byte) (RemoteCleanup, error) {
		t.Fatal("client cleanup contacted the administrative API")
		return nil, nil
	}
	return root, d, f
}

func TestClientDestroyNeedsNoTokenForRunningOrStoppedInstallation(t *testing.T) {
	for _, running := range []bool{true, false} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			root, d, f := clientShutdownFixture(t)
			if running {
				guard, err := state.PrepareRoleState(context.Background(), root, "client")
				if err != nil {
					t.Fatal(err)
				}
				defer guard.Close()
				d.Stop = func(ctx context.Context, dir string) error {
					f.calls = append(f.calls, "stop")
					if err := meshlocal.CheckNotDestroying(dir); err == nil {
						t.Fatal("client stop lacks durable destruction fence")
					}
					return guard.Close()
				}
			}
			var output bytes.Buffer
			if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, Yes: true}, &output, d); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.calls, []string{"stop", "purge"}) {
				t.Fatalf("unexpected client destruction effects: %v", f.calls)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("client state remains: %v", err)
			}
			if !strings.Contains(output.String(), "No API token is required") ||
				!strings.Contains(output.String(), "device entry may remain") ||
				strings.Contains(output.String(), "device absent") {
				t.Fatalf("misleading client cleanup output: %s", &output)
			}
		})
	}
}

func TestClientDestroyDeclineDryRunAndInvalidFlagsHaveNoEffects(t *testing.T) {
	for _, scenario := range []string{"decline", "dry-run", "policy", "token-env"} {
		t.Run(scenario, func(t *testing.T) {
			root, d, f := clientShutdownFixture(t)
			o := ShutdownOptions{Destroy: true}
			switch scenario {
			case "decline":
				d.Confirm = func(context.Context, string) (bool, error) { return false, nil }
			case "dry-run":
				o.DryRun = true
				d.Confirm = func(context.Context, string) (bool, error) {
					t.Fatal("dry run prompted")
					return false, nil
				}
			case "policy":
				o.RemovePolicy = true
			case "token-env":
				o.TokenEnv = "PRIVATE_API_TOKEN"
			}
			err := Shutdown(context.Background(), o, io.Discard, d)
			if (err == nil) != (scenario == "dry-run") || len(f.calls) != 0 {
				t.Fatalf("unexpected effects: %v, %v", f.calls, err)
			}
			if _, err := meshlocal.Load(root); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(root, "destroy.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected destruction fence: %v", err)
			}
		})
	}
}

func TestClientDestroyFailureRetainsTruthfulRecoverableFence(t *testing.T) {
	for _, scenario := range []string{"stop", "partial-purge", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			root, d, f := clientShutdownFixture(t)
			stop, purge := d.Stop, d.Purge
			switch scenario {
			case "stop":
				d.Stop = func(context.Context, string) error { return errors.New("still running") }
			case "partial-purge":
				d.Purge = func(ctx context.Context, root string) error {
					if err := os.Remove(filepath.Join(root, "config.json")); err != nil {
						return err
					}
					return errors.New("interrupted purge")
				}
			case "cancel":
				d.Stop = func(context.Context, string) error { return context.Canceled }
			}
			o := ShutdownOptions{Destroy: true, Yes: true}
			if err := Shutdown(context.Background(), o, io.Discard, d); err == nil {
				t.Fatal("incomplete destruction reported success")
			}
			record, err := meshlocal.ReadDestroyState(root)
			if err != nil || !record.LocalOnly || record.DeviceRemoved || record.PolicyRemoved {
				t.Fatalf("invalid local recovery record: %+v, %v", record, err)
			}
			if err := meshlocal.CheckNotDestroying(root); err == nil {
				t.Fatal("incomplete destruction did not fence restart")
			}
			d.Stop, d.Purge, f.calls = stop, purge, nil
			if err := Shutdown(context.Background(), o, io.Discard, d); err != nil {
				t.Fatalf("tokenless resume failed: %v", err)
			}
		})
	}
}

func TestClientDestroyDoesNotDiscardUnknownLegacyRemoteDeletion(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprint(removed), func(t *testing.T) {
			root, d, f := clientShutdownFixture(t)
			cfg, err := meshlocal.Load(root)
			if err != nil {
				t.Fatal(err)
			}
			record := meshlocal.DestroyState{Configuration: cfg, Identity: meshlocal.ManagedIdentity{DeviceID: "nPinned"}, DeviceRemoved: removed}
			if err := meshlocal.SaveDestroyState(root, record); err != nil {
				t.Fatal(err)
			}
			err = Shutdown(context.Background(), ShutdownOptions{Destroy: true, Yes: true}, io.Discard, d)
			if removed {
				if err != nil {
					t.Fatalf("confirmed legacy cleanup required authorization again: %v", err)
				}
			} else if err == nil || len(f.calls) != 0 {
				t.Fatalf("unknown legacy remote outcome discarded: %v, %v", f.calls, err)
			}
		})
	}
}

func TestShutdownDefaultPreservesEverythingExceptDaemon(t *testing.T) {
	root, d, f := shutdownFixture(t)
	d.Identity = func(context.Context, string) (meshlocal.ManagedIdentity, error) {
		t.Fatal("ordinary shutdown required a remote identity")
		return meshlocal.ManagedIdentity{}, nil
	}
	if err := Shutdown(context.Background(), ShutdownOptions{}, io.Discard, d); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.calls, []string{"stop"}) {
		t.Fatalf("shutdown exceeded scope: %v", f.calls)
	}
	if _, err := meshlocal.Load(root); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDestroyPreflightsBeforeEffectsAndPurgesLast(t *testing.T) {
	root, d, f := shutdownFixture(t)
	if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, RemovePolicy: true, Yes: true}, io.Discard, d); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.calls, []string{"preflight", "stop", "device", "policy", "purge"}) {
		t.Fatal("unsafe teardown order", f.calls)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("managed state not removed", err)
	}
}

func TestShutdownDestroyThroughLinkedInstallation(t *testing.T) {
	root, d, f := shutdownFixture(t)
	alias := filepath.Join(t.TempDir(), "installation")
	if runtime.GOOS == "windows" {
		if output, err := exec.Command(os.Getenv("ComSpec"), "/d", "/c", "mklink", "/J", alias, filepath.Dir(root)).CombinedOutput(); err != nil {
			t.Fatalf("create installation junction: %v: %s", err, output)
		}
	} else if err := os.Symlink(filepath.Dir(root), alias); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(alias, filepath.Base(root))
	d.Dir = func() (string, error) { return dir, nil }
	apply := filepath.Join(dir, "policy-preview-test", "apply")
	if err := os.MkdirAll(apply, 0700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string]string{
		filepath.Join(dir, "policy-complete"):           "completed",
		filepath.Join(apply, "policy-before-test.json"): `{"grants":[]}`,
		filepath.Join(apply, "policy-proposed.json"):    `{"grants":[]}`,
	} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := state.ProtectPrivatePath(path, false); err != nil {
			t.Fatal(err)
		}
	}
	remote := d.Remote
	d.Remote = func(ctx context.Context, path string, cfg meshlocal.Config, identity meshlocal.ManagedIdentity, policy bool, token []byte) (RemoteCleanup, error) {
		if _, _, err := readOwnedPolicy(path); err != nil {
			return nil, err
		}
		return remote(ctx, path, cfg, identity, policy, token)
	}
	if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, RemovePolicy: true, Yes: true}, io.Discard, d); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.calls, []string{"preflight", "stop", "device", "policy", "purge"}) {
		t.Fatal("unsafe teardown order", f.calls)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("physical state remains after linked shutdown: %v", err)
	}
}

func TestShutdownDestroyDeclineDryRunAndAuthorizationHaveNoEffects(t *testing.T) {
	for _, scenario := range []string{"decline", "dry-run", "token", "preflight"} {
		t.Run(scenario, func(t *testing.T) {
			root, d, f := shutdownFixture(t)
			o := ShutdownOptions{Destroy: true, RemovePolicy: true}
			switch scenario {
			case "decline":
				d.Confirm = func(context.Context, string) (bool, error) { return false, nil }
			case "dry-run":
				o.DryRun = true
			case "token":
				d.Token = func(context.Context, string) ([]byte, error) { return nil, errors.New("credential unavailable") }
			case "preflight":
				d.Remote = func(context.Context, string, meshlocal.Config, meshlocal.ManagedIdentity, bool, []byte) (RemoteCleanup, error) {
					return nil, errors.New("remote permission denied")
				}
			}
			err := Shutdown(context.Background(), o, io.Discard, d)
			if (err == nil) != (scenario == "dry-run") || len(f.calls) != 0 {
				t.Fatalf("unexpected effects=%v err=%v", f.calls, err)
			}
			if _, err := meshlocal.Load(root); err != nil {
				t.Fatal("preflight failure lost configuration", err)
			}
			if _, err := os.Stat(filepath.Join(root, "destroy.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("preflight failure fenced live installation", err)
			}
		})
	}
}

func TestShutdownPartialRemoteFailureRetainsRecoveryAndFencesRestart(t *testing.T) {
	root, d, f := shutdownFixture(t)
	o := ShutdownOptions{Destroy: true, RemovePolicy: true, Yes: true}
	f.policyError = errors.New("concurrent policy update")
	if err := Shutdown(context.Background(), o, io.Discard, d); err == nil {
		t.Fatal("partial cleanup reported success")
	}
	record, err := meshlocal.ReadDestroyState(root)
	if err != nil || !record.DeviceRemoved || record.PolicyRemoved {
		t.Fatal("missing truthful recovery receipt", record, err)
	}
	if err := meshlocal.CheckNotDestroying(root); err == nil {
		t.Fatal("partial teardown allowed restart")
	}
	if _, err := meshlocal.Load(root); err != nil {
		t.Fatal("partial cleanup purged configuration", err)
	}
	if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, Yes: true}, io.Discard, d); err == nil {
		t.Fatal("dropped original policy intent to bypass incomplete cleanup")
	}
	f.policyError = nil
	if err := Shutdown(context.Background(), o, io.Discard, d); err != nil {
		t.Fatal("exact resume failed", err)
	}
}

func TestShutdownUnknownDeviceOutcomeCannotPurge(t *testing.T) {
	root, d, f := shutdownFixture(t)
	f.deviceError = errors.New("unknown remote outcome")
	if err := Shutdown(context.Background(), ShutdownOptions{Destroy: true, Yes: true}, io.Discard, d); err == nil {
		t.Fatal("unknown outcome reported success")
	}
	record, err := meshlocal.ReadDestroyState(root)
	if err != nil || record.DeviceRemoved {
		t.Fatal("unknown device deletion marked confirmed")
	}
	if _, err := meshlocal.Load(root); err != nil {
		t.Fatal("unknown cleanup lost local identity")
	}
}
