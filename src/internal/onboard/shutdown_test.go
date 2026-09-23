package onboard

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
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
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "managed")
	if err := meshlocal.Save(root, meshlocal.Config{Version: 1, Name: "desktop", Coordinator: true, Tailnet: "example.com"}); err != nil {
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
