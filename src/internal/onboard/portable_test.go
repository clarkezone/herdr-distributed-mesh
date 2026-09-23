package onboard

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func TestHostnameDefaultAndExplicitOverride(t *testing.T) {
	for _, tc := range []struct{ host, want string }{
		{"ClarkezoneRig", "clarkezonerig"}, {"BUILD.Machine_2", "build-machine-2"},
		{"123-machine", "node-123-machine"}, {"AUX", "node-aux"},
		{strings.Repeat("a", 50), strings.Repeat("a", 40)},
	} {
		got, err := nameFromHostname(tc.host)
		if err != nil || got != tc.want || !portableName.MatchString(got) {
			t.Fatalf("hostname %q: %q, %v", tc.host, got, err)
		}
	}
	if _, err := nameFromHostname("..."); err == nil {
		t.Fatal("empty normalized hostname accepted")
	}
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	want, err := nameFromHostname(host)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []Options{initOptions(), joinOptions()} {
		o.Name = ""
		got, err := o.Normalize()
		if err != nil || got.Name != want {
			t.Fatalf("omitted name: %+v, %v", got, err)
		}
		o.Name = "override"
		got, err = o.Normalize()
		if err != nil || got.Name != "override" {
			t.Fatalf("explicit override: %+v, %v", got, err)
		}
	}
}

func TestStartReusesSavedConfigurationWithoutSetup(t *testing.T) {
	for _, coordinator := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "coordinator"}[coordinator], func(t *testing.T) {
			f := newFixture(t)
			options := joinOptions()
			if coordinator {
				options = initOptions()
			}
			if err := Run(context.Background(), options, io.Discard, f.d); err != nil {
				t.Fatal(err)
			}
			saved, prompts, policyCalls := f.saved, f.prompts, f.policyCalls
			cfg := f.cfg
			f.d.Running = func(string) (bool, error) { return f.starts > 1, nil }
			newExecutable := filepath.Join(t.TempDir(), "renamed-mesh")
			f.d.Executable = func() (string, error) { return newExecutable, nil }
			f.d.Start = func(exe, dir string, _ []string) error {
				if exe != newExecutable || dir != f.dir {
					t.Fatalf("wrong executable/state: %s %s", exe, dir)
				}
				f.starts++
				return nil
			}
			var out bytes.Buffer
			for range 2 {
				if err := Start(context.Background(), &out, f.d); err != nil {
					t.Fatal(err)
				}
			}
			if f.starts != 2 || f.saved != saved || f.prompts != prompts || f.policyCalls != policyCalls || f.cfg != cfg {
				t.Fatal("start repeated setup, rewrote configuration or duplicated daemon")
			}
			if !strings.Contains(out.String(), f.dir) {
				t.Fatal("state location not reported")
			}
		})
	}
}

func TestStartRejectsMissingOrInterruptedStateWithoutEnrollment(t *testing.T) {
	for _, kind := range []string{"missing", "lock", "destroy", "policy", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind != "missing" {
				f.saved, f.cfg = 1, meshlocal.Config{Version: 1, Name: "desktop", Coordinator: true, Tailnet: "example.com", HerdrExecutable: "herdr"}
				if err := os.MkdirAll(f.dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "lock":
				if err := os.WriteFile(filepath.Join(f.dir, "onboarding.lock"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "destroy":
				if err := os.WriteFile(filepath.Join(f.dir, "destroy.json"), []byte("incomplete"), 0600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			if err := Start(ctx, io.Discard, f.d); err == nil {
				t.Fatal("unsafe start accepted")
			}
			if f.starts != 0 || f.prompts != 0 || f.policyCalls != 0 {
				t.Fatal("start enrolled or changed policy")
			}
		})
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("HERDR_MESH_LAUNCH_TEST_CHILD") == "1" && len(os.Args) == 4 && os.Args[1] == "managed-run" {
		dir := os.Args[3]
		cwd, err := os.Getwd()
		if err != nil || cwd != dir || os.Args[2] != "--state-dir" {
			os.Exit(2)
		}
		if os.WriteFile(filepath.Join(dir, "child-ready"), nil, 0600) != nil {
			os.Exit(3)
		}
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if _, err := os.Stat(filepath.Join(dir, "child-release")); err == nil {
				if os.WriteFile(filepath.Join(dir, "child-done"), nil, 0600) != nil {
					os.Exit(4)
				}
				os.Exit(0)
			}
		}
		os.Exit(5)
	}
	os.Exit(m.Run())
}

func TestPortableBackgroundLaunch(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("managed background launch is not supported on this OS")
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	waitFor := func(name string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		t.Fatalf("background child did not create %s", name)
	}
	release := func() { _ = os.WriteFile(filepath.Join(dir, "child-release"), nil, 0600) }
	defer release()
	if err := startDaemon(exe, dir, append(os.Environ(), "HERDR_MESH_LAUNCH_TEST_CHILD=1")); err != nil {
		if runtime.GOOS == "windows" && errors.Is(err, os.ErrPermission) {
			if _, statErr := os.Stat(filepath.Join(dir, "child-ready")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("refused background launch unexpectedly started a child: %v", statErr)
			}
			t.Skipf("host disallows independent Windows process launch: %v", err)
		}
		t.Fatal(err)
	}
	waitFor("child-ready")
	release()
	waitFor("child-done")
	if _, err := os.Stat(filepath.Join(dir, "daemon.log")); err != nil {
		t.Fatal(err)
	}
}
