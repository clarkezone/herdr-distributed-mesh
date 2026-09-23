package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/bootstrap"
)

func bootstrapArgs() []string {
	return []string{"-ssh-host", "prepared-node", "-archive", "trusted.zip", "-sha256", strings.Repeat("a", 64),
		"-architecture", "amd64", "-install-dir", `C:\Mesh\v1`, "-state-dir", `C:\Mesh\state`, "-server", "coordinator:50052"}
}

func TestBootstrapParser(t *testing.T) {
	var output bytes.Buffer
	streams := IO{Out: &output, Err: &output}
	args := append(bootstrapArgs(), "-herdr-executable", `C:\Herdr tools\herdr.exe`, "-start", "-existing-task", `\Mesh\Node`, "-json")
	options, asJSON, err := parseBootstrap(args, streams)
	if err != nil || !asJSON || !options.Start || options.HerdrExecutable != `C:\Herdr tools\herdr.exe` {
		t.Fatalf("parse: %v", err)
	}
	for _, extra := range [][]string{
		{"-password", "SECRET"}, {"-auth-key", "SECRET"}, {"-ssh-options", "SECRET"},
		{"-command", "SECRET"}, {"-timeout", "SECRET"}, {"-herdr-executable", ""},
		{"SECRET"}, {"-ssh-host", "-oSECRET"}, {"-archive", strings.Repeat("SECRET", 3000)},
	} {
		output.Reset()
		_, _, err := parseBootstrap(append(bootstrapArgs(), extra...), streams)
		if err == nil {
			t.Fatalf("accepted invalid flags %v", extra)
		}
		if strings.Contains(err.Error()+output.String(), "SECRET") {
			t.Fatal("parser echoed sensitive input")
		}
	}
	output.Reset()
	_, _, err = parseBootstrap([]string{"-help"}, streams)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(output.String(), "herdr-mesh bootstrap") {
		t.Fatal("help not available")
	}
}

func TestBootstrapRouterShapes(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var out, stderr bytes.Buffer
		calls := 0
		err := runBootstrap(context.Background(), append(bootstrapArgs(), "-json"), IO{Out: &out, Err: &stderr},
			func(_ context.Context, o bootstrap.Options) (*bootstrap.Result, error) {
				calls++
				if o.SSHHost != "prepared-node" {
					t.Fatal("typed options changed")
				}

				if fail {
					return nil, &bootstrap.Error{Code: "BOOTSTRAP_TASK_START", Outcome: "start-unknown", Phase: "start"}
				}
				return &bootstrap.Result{Status: "staged-not-started", Startup: "not-attempted"}, nil
			})
		if calls != 1 || (err != nil) != fail || stderr.Len() != 0 {
			t.Fatalf("router failed: %v", err)
		}
		want := `"status":"staged-not-started"`
		if fail {
			want = `"status":"start-unknown"`
		}
		if !strings.Contains(out.String(), want) {
			t.Fatal("typed status missing")
		}
	}
	if err := RunBootstrap(context.Background(), []string{"-command", "SECRET"}, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
		t.Fatal("public router accepted arbitrary command")
	}
}

func TestBootstrapHelpSucceedsWithoutRuntime(t *testing.T) {
	for _, help := range []string{"-h", "-help", "--help"} {
		t.Run(help, func(t *testing.T) {
			var out, diagnostics bytes.Buffer
			err := runBootstrap(context.Background(), []string{help}, IO{Out: &out, Err: &diagnostics},
				func(context.Context, bootstrap.Options) (*bootstrap.Result, error) {
					t.Fatal("help invoked the bootstrap runtime")
					return nil, nil
				})
			if err != nil || diagnostics.Len() != 0 ||
				!strings.Contains(out.String(), "ssh-host") || !strings.Contains(out.String(), "herdr-executable") {
				t.Fatalf("help did not succeed without runtime: %v", err)
			}

		})
	}
}

func TestBootstrapHumanReceiptAndUnchangedJSON(t *testing.T) {
	for _, staged := range []bool{false, true} {
		result := &bootstrap.Result{
			Status: "runner-running-mesh-unverified", OperationID: "operation-123",
			Startup: "requested-once", Enrollment: "not-checked", Connectivity: "not-checked",
			InstallDirectory: `C:\Mesh\v1`, StateDirectory: `C:\Mesh\state`,
			ExistingTaskName: `\Mesh\Node`, RunnerState: "Running",
		}
		if staged {
			result.Status, result.Startup, result.RunnerRequired = "staged-not-started", "not-attempted", true
			result.ExistingTaskName, result.RunnerState = "", ""
		}
		for _, asJSON := range []bool{false, true} {
			args := bootstrapArgs()
			if asJSON {
				args = append(args, "-json")
			}
			var out, diagnostics bytes.Buffer
			err := runBootstrap(context.Background(), args, IO{Out: &out, Err: &diagnostics},
				func(context.Context, bootstrap.Options) (*bootstrap.Result, error) { return result, nil })
			if err != nil || diagnostics.Len() != 0 {
				t.Fatalf("receipt failed: %v", err)
			}
			if asJSON {
				expected, err := json.Marshal(result)
				if err != nil || out.String() != string(expected)+"\n" {
					t.Fatalf("JSON contract changed: %s", out.String())
				}
				continue
			}
			for _, text := range []string{"Operation ID: operation-123", "Enrollment: not checked", "Connectivity: not checked", "Install directory: C:\\Mesh\\v1", "Next:"} {
				if !strings.Contains(out.String(), text) {
					t.Fatalf("missing %q: %s", text, out.String())
				}
			}
			if staged && !strings.Contains(out.String(), "node not started") ||
				!staged && !strings.Contains(out.String(), "mesh readiness not verified") {
				t.Fatalf("overstated readiness: %s", out.String())
			}
		}
	}
}
