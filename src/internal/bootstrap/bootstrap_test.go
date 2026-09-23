package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/scripthost"
)

func fixtureOptions() Options {
	return Options{
		SSHHost: "prepared-node", ArchivePath: "trusted.zip", ExpectedSHA256: strings.Repeat("a", 64),
		Architecture: "amd64", InstallDirectory: `C:\Mesh\releases\v1`, StateDirectory: `C:\Mesh\node-state`,
		Server: "coordinator.example:50052", Timeout: time.Minute,
	}
}

func fixtureResult(o Options) *Result {
	args := []string{"node", "-server", o.Server, "-state-dir", o.StateDirectory}
	if o.HerdrExecutable != "" {
		args = append(args, "-herdr-executable", o.HerdrExecutable)
	}
	r := &Result{
		SchemaVersion: 1, Status: "staged-not-started", OperationID: strings.Repeat("1", 32),
		InstallDirectory: o.InstallDirectory, StateDirectory: o.StateDirectory, Executable: o.InstallDirectory + `\herdr-mesh.exe`,
		NodeArguments: args, ArchiveSHA256: o.ExpectedSHA256, BinarySHA256: strings.Repeat("b", 64),
		Architecture: o.Architecture, Startup: "not-attempted", Enrollment: "not-checked", Connectivity: "not-checked", RunnerRequired: true,
	}
	if o.Start {
		r.Status, r.Startup, r.RunnerRequired = "runner-running-mesh-unverified", "requested-once", false
		r.InstallOperationID, r.ExistingTaskName = strings.Repeat("2", 32), o.ExistingTaskName
		r.RunnerState, r.RunnerObservedAtUTC = "Running", time.Now().UTC().Format(time.RFC3339Nano)
	}
	return r
}

func resultBytes(t *testing.T, o Options) []byte {
	t.Helper()
	data, err := json.Marshal(envelope{Version: 1, OK: true, Result: fixtureResult(o)})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func fakeFind(t *testing.T) func(string) (string, error) {
	t.Helper()
	root := t.TempDir()
	return func(name string) (string, error) { return filepath.Join(root, name+".exe"), nil }
}

func assertError(t *testing.T, err error, code, outcome string) {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != code || failure.Outcome != outcome {
		t.Fatalf("expected %s/%s, got %v", code, outcome, err)
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatal("diagnostic leaked input")
	}
}

func TestOptionsBoundaries(t *testing.T) {
	base := fixtureOptions()
	if err := Validate(base); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Options){
		"alias-option":       func(o *Options) { o.SSHHost = "-oProxyCommand=SECRET" },
		"alias-user":         func(o *Options) { o.SSHHost = "user@host" },
		"alias-command":      func(o *Options) { o.SSHHost = "host;SECRET" },
		"alias-newline":      func(o *Options) { o.SSHHost += "\n" },
		"alias-empty":        func(o *Options) { o.SSHHost = "" },
		"archive-empty":      func(o *Options) { o.ArchivePath = "" },
		"archive-bound":      func(o *Options) { o.ArchivePath = strings.Repeat("a", 4097) },
		"archive-null":       func(o *Options) { o.ArchivePath = "SECRET\x00zip" },
		"hash":               func(o *Options) { o.ExpectedSHA256 = "SECRET" },
		"hash-newline":       func(o *Options) { o.ExpectedSHA256 += "\n" },
		"architecture":       func(o *Options) { o.Architecture = "arm64;SECRET" },
		"root":               func(o *Options) { o.InstallDirectory = `C:\` },
		"unc":                func(o *Options) { o.InstallDirectory = `\\server\share\node` },
		"relative":           func(o *Options) { o.InstallDirectory = "node" },
		"traversal":          func(o *Options) { o.InstallDirectory = `C:\node\..\other` },
		"ads":                func(o *Options) { o.InstallDirectory = `C:\node:stream` },
		"device":             func(o *Options) { o.InstallDirectory = `C:\NUL.exe` },
		"expression":         func(o *Options) { o.StateDirectory = `C:\$(SECRET)` },
		"overlap":            func(o *Options) { o.InstallDirectory = o.StateDirectory + `\child` },
		"server":             func(o *Options) { o.Server = "host:22;SECRET" },
		"port":               func(o *Options) { o.Server = "host:65536" },
		"server-newline":     func(o *Options) { o.Server += "\n" },
		"herdr-command":      func(o *Options) { o.HerdrExecutable = `C:\tools\herdr.exe -SECRET` },
		"herdr-wrapper":      func(o *Options) { o.HerdrExecutable = `C:\tools\herdr.ps1` },
		"herdr-relative":     func(o *Options) { o.HerdrExecutable = `herdr.exe` },
		"task-without-start": func(o *Options) { o.ExistingTaskName = `\Mesh\Node` },
		"start-without-task": func(o *Options) { o.Start = true },
		"task-wildcard":      func(o *Options) { o.Start = true; o.ExistingTaskName = `\Mesh\*` },
		"timeout-zero":       func(o *Options) { o.Timeout = 0 },
		"timeout-fractional": func(o *Options) { o.Timeout = 1500 * time.Millisecond },
		"timeout-large":      func(o *Options) { o.Timeout = MaxTimeout + time.Second },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			o := base
			mutate(&o)
			assertError(t, Validate(o), "invalid_options", "not-installed")
		})
	}
}

func TestEmbeddedHostRequest(t *testing.T) {
	t.Setenv("TS_AUTHKEY_NODE", "SECRET-ENROLLMENT-MUST-NOT-BE-SERIALIZED")
	for _, start := range []bool{false, true} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			o := fixtureOptions()
			o.HerdrExecutable = `C:\Herdr tools\herdr.exe`
			o.Start = start
			if start {
				o.ExistingTaskName = `\Mesh\Node`
			}
			calls := 0
			got, err := run(context.Background(), o, fakeFind(t), func(ctx context.Context, req scripthost.Request) (scripthost.Result, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("process has no deadline")
				}
				if req.Timeout != o.Timeout || len(req.Script) < 1000 || len(req.Assets) != 1 ||
					len(req.Assets["bootstrap-node.ps1"]) < 20000 {
					t.Fatal("embedded payload missing or not bounded")
				}
				data, err := json.Marshal(req.Options)
				if err != nil {
					t.Fatal(err)
				}
				if len(data) > 32*1024 || strings.Contains(string(data), "SECRET") {
					t.Fatal("private data leaked into request")
				}
				var values map[string]any
				if err := json.Unmarshal(data, &values); err != nil {
					t.Fatal(err)
				}
				if !filepath.IsAbs(values["ArchivePath"].(string)) || values["TimeoutSeconds"] != float64(60) ||
					values["HerdrExecutable"] != o.HerdrExecutable {
					t.Fatal("structured request changed")
				}
				for _, forbidden := range []string{"Command", "Arguments", "Credential", "Password", "KeyFilePath", "AuthKey", "SSHOptions"} {
					if _, ok := values[forbidden]; ok {
						t.Fatalf("unexpected proxy field %s", forbidden)
					}
				}
				for _, path := range []string{"scripts\\bootstrap-node.ps1", "src\\internal\\bootstrap"} {
					if strings.Contains(string(req.Script), path) {
						t.Fatal("runtime depends on checkout")
					}
				}
				return scripthost.Result{Started: true, Output: resultBytes(t, o)}, nil
			})
			if err != nil || calls != 1 || got.Status != fixtureResult(o).Status {
				t.Fatalf("host result: %v", err)
			}
		})
	}
}

func TestPrerequisitesNoFallback(t *testing.T) {
	for _, missing := range []string{"pwsh", "ssh"} {
		t.Run(missing, func(t *testing.T) {
			find := fakeFind(t)
			_, err := run(context.Background(), fixtureOptions(), func(name string) (string, error) {
				if name == missing {
					return "", errors.New("SECRET")
				}
				return find(name)
			}, func(context.Context, scripthost.Request) (scripthost.Result, error) {
				t.Fatal("process ran despite missing prerequisite")
				return scripthost.Result{}, nil
			})
			assertError(t, err, missing+"_missing", "not-installed")
		})
	}
}

func TestCancellationAndUnknownProcessOutcome(t *testing.T) {
	o := fixtureOptions()
	o.Start, o.ExistingTaskName = true, `\Mesh\Node`
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := run(ctx, o, func(string) (string, error) { t.Fatal("pre-canceled lookup"); return "", nil }, nil)
	assertError(t, err, "canceled", "not-installed")
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if timeout {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
			}
			calls := 0
			_, err := run(ctx, o, fakeFind(t), func(ctx context.Context, _ scripthost.Request) (scripthost.Result, error) {
				calls++
				if !timeout {
					cancel()
				}
				<-ctx.Done()
				return scripthost.Result{Started: true}, errors.New("SECRET")
			})
			code := "canceled"
			target := context.Canceled
			if timeout {
				code, target = "timeout", context.DeadlineExceeded
			}
			assertError(t, err, code, "start-unknown")
			if !errors.Is(err, target) || calls != 1 {
				t.Fatal("context semantics/retry changed")
			}
		})
	}
}

func TestProcessFailuresAreSanitized(t *testing.T) {
	o := fixtureOptions()
	o.Start, o.ExistingTaskName = true, `\Mesh\Node`
	for _, code := range []string{"execution_failed", "output_limit", "cleanup_failed", "temporary_io"} {
		_, err := run(context.Background(), o, fakeFind(t), func(context.Context, scripthost.Request) (scripthost.Result, error) {
			return scripthost.Result{Started: true}, &scripthost.Error{Code: code}
		})
		want := code
		if code == "execution_failed" {
			want = "process_failed"
		}
		if code == "output_limit" {
			want = "output_invalid"
		}
		assertError(t, err, want, "start-unknown")
	}
}

func TestResponseBoundsAndShape(t *testing.T) {
	o := fixtureOptions()
	for name, data := range map[string][]byte{
		"empty": nil, "oversize": []byte(strings.Repeat("SECRET", maxResponse)),
		"raw-stderr":    []byte("SECRET SSH failure"),
		"extra-json":    append(resultBytes(t, o), []byte(` {}`)...),
		"extra-field":   []byte(`{"version":1,"ok":true,"SECRET":"hidden"}`),
		"duplicate":     []byte(`{"version":0,"version":1,"ok":true,"result":null}`),
		"unknown-error": []byte(`{"version":1,"ok":false,"error":{"code":"SECRET","status":"not-installed","phase":"validation"}}`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decode(data, o, true)
			assertError(t, err, "output_invalid", "indeterminate")
		})
	}
	mutations := map[string]func(*Result){
		"claims-ready":   func(r *Result) { r.Status = "mesh-ready" },
		"changed-target": func(r *Result) { r.Executable = `C:\SECRET.exe` },
		"extra-argument": func(r *Result) { r.NodeArguments = append(r.NodeArguments, "SECRET") },
		"wrong-hash":     func(r *Result) { r.ArchiveSHA256 = strings.Repeat("0", 64) },
		"enrolled":       func(r *Result) { r.Enrollment = "ready" },
		"connected":      func(r *Result) { r.Connectivity = "ready" },
		"unknown-runner": func(r *Result) { r.RunnerState = "Running" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := fixtureResult(o)
			mutate(r)
			data, err := json.Marshal(envelope{Version: 1, OK: true, Result: r})
			if err != nil {
				t.Fatal(err)
			}
			_, err = decode(data, o, true)
			assertError(t, err, "output_invalid", "indeterminate")
		})
	}
	o.Start, o.ExistingTaskName = true, `\Mesh\Node`
	data, _ := json.Marshal(envelope{Version: 1, Error: &Error{Code: "BOOTSTRAP_TASK_START", Outcome: "start-unknown", Phase: "start", OperationID: strings.Repeat("3", 32)}})
	_, err := decode(data, o, true)
	assertError(t, err, "BOOTSTRAP_TASK_START", "start-unknown")
}

func TestEmbeddedSSHPolicy(t *testing.T) {
	entry, err := assets.ReadFile("assets/entry.ps1")
	if err != nil {
		t.Fatal(err)
	}
	text := string(entry)
	for _, required := range []string{"BatchMode = 'yes'", "StrictHostKeyChecking = 'yes'", "PasswordAuthentication = 'no'",
		"KbdInteractiveAuthentication = 'no'", "NumberOfPasswordPrompts = '0'", "UpdateHostKeys = 'no'",
		"ConnectionAttempts = '1'", "ForwardAgent = 'no'", "PermitLocalCommand = 'no'", "RemoteCommand = 'none'",
		"ProxyCommand = 'none'", "ControlPath = 'none'", "SSH_ASKPASS_REQUIRE = 'never'",
		"-ConnectingTimeout", "-Subsystem powershell", "-Options $sshOptions"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing policy: %s", required)
		}
	}
	for _, forbidden := range []string{"-KeyFilePath", "-Credential", "-UserName", "Invoke-Expression", "Start-Process", "Register-ScheduledTask"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("unexpected API: %s", forbidden)
		}
	}
	if strings.Count(text, "New-PSSession -HostName") != 1 {
		t.Fatal("SSH connection was retried")
	}
	// Tests inspect embedded bytes, never invoke SSH or scheduler tooling.
	if _, err := os.Stat(filepath.Join("assets", "bootstrap-node.ps1")); err != nil {
		t.Fatal(err)
	}
}
