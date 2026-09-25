package scripthost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostChildProcess(t *testing.T) {
	mode := os.Getenv("HERDR_SCRIPT_HOST_TEST_CHILD")
	if mode == "" {
		return
	}
	switch mode {
	case "ok":
		fmt.Print(`{"ok":true}`)
	case "failure":
		fmt.Fprint(os.Stderr, "raw remote payload tskey-"+"api-test-secret")
		os.Exit(3)
	case "flood":
		fmt.Print(strings.Repeat("x", MaxOutputBytes+1))
	case "stderr-flood":
		fmt.Fprint(os.Stderr, strings.Repeat("x", MaxOutputBytes+1))
	case "hold":
		time.Sleep(time.Minute)
	case "tree":
		child := childCommand("hold")
		if err := child.Start(); err != nil {
			os.Exit(4)
		}
		fmt.Println(child.Process.Pid)
		time.Sleep(time.Minute)
	}
	os.Exit(0)
}

func childCommand(mode string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHostChildProcess$")
	cmd.Env = append(os.Environ(), "HERDR_SCRIPT_HOST_TEST_CHILD="+mode)
	return cmd
}

func TestHostStructuredPrivateCleanup(t *testing.T) {
	value := `'; Write-Output "must-not-execute"; #`
	var directory string
	result, err := run(context.Background(), Request{
		Script: []byte("param($OptionsPath)"), Assets: map[string][]byte{"asset.ps1": []byte("asset")},
		Options: map[string]string{"value": value}, Timeout: 10 * time.Second,
	}, func() (string, error) { return os.Executable() }, func(ctx context.Context, cmd *exec.Cmd) (bool, error) {
		directory = cmd.Dir
		if strings.Contains(strings.Join(cmd.Args, " "), value) || cmd.Stdin != nil {
			t.Fatal("options leaked to argv or stdin opened")
		}
		if len(cmd.Args) != 10 || cmd.Args[6] != "-File" || cmd.Args[8] != "-OptionsPath" {
			t.Fatalf("unexpected argv: %v", cmd.Args)
		}
		data, err := os.ReadFile(filepath.Join(cmd.Dir, "options.json"))
		if err != nil {
			t.Fatal(err)
		}
		var options map[string]string
		if json.Unmarshal(data, &options) != nil || options["value"] != value {
			t.Fatal("structured options changed")
		}
		assertPrivate(t, cmd.Dir)
		for _, name := range []string{"host.ps1", "entry.ps1", "options.json", "asset.ps1"} {
			if _, err := os.Stat(filepath.Join(cmd.Dir, name)); err != nil {
				t.Fatal(err)
			}
		}
		fake := childCommand("ok")
		fake.Dir, fake.Stdout, fake.Stderr = cmd.Dir, cmd.Stdout, cmd.Stderr
		return execute(ctx, fake)
	})
	if err != nil || !result.Started || string(result.Output) != `{"ok":true}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary files were not removed")
	}
}

func TestHostFailureBoundsAndSanitization(t *testing.T) {
	for _, test := range []struct {
		mode, code string
		timeout    time.Duration
	}{
		{"failure", "execution_failed", 10 * time.Second},
		{"flood", "output_limit", 10 * time.Second},
		{"stderr-flood", "output_limit", 10 * time.Second},
		{"hold", "timeout", 50 * time.Millisecond},
	} {
		t.Run(test.mode, func(t *testing.T) {
			result, err := run(context.Background(), Request{Script: []byte("param($OptionsPath)"), Options: struct{}{}, Timeout: test.timeout},
				os.Executable, func(ctx context.Context, cmd *exec.Cmd) (bool, error) {
					fake := childCommand(test.mode)
					fake.Stdout, fake.Stderr = cmd.Stdout, cmd.Stderr
					return execute(ctx, fake)
				})
			var hostError *Error
			if !errors.As(err, &hostError) || hostError.Code != test.code || (!result.Started && test.mode != "hold") || result.Output != nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if strings.Contains(err.Error(), "tskey-") || strings.Contains(err.Error(), "payload") {
				t.Fatal("raw error leaked")
			}
		})
	}
}

func TestHostRejectsUnsafeRequestsBeforeProcess(t *testing.T) {
	for _, request := range []Request{
		{Script: []byte("x"), Assets: map[string][]byte{"../escape.ps1": []byte("x")}},
		{Script: []byte("x"), Assets: map[string][]byte{"entry.ps1": []byte("x")}},
		{Script: []byte("x"), Assets: map[string][]byte{"ENTRY.ps1": []byte("x")}},
		{Script: []byte("x"), Assets: map[string][]byte{"NUL.ps1": []byte("x")}},
		{Script: []byte("x"), Options: strings.Repeat("x", maxInputBytes)},
		{Script: []byte("x"), Timeout: MaxTimeout + time.Second},
	} {
		_, err := run(context.Background(), request, func() (string, error) { t.Fatal("runtime discovery for invalid request"); return "", nil }, nil)
		var hostError *Error
		if !errors.As(err, &hostError) || hostError.Code != "invalid_request" {
			t.Fatalf("err=%v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := run(ctx, Request{Script: []byte("x")}, nil, nil)
	if err == nil || result.Started {
		t.Fatal("pre-canceled request started")
	}
	result, err = run(context.Background(), Request{Script: []byte("x")}, func() (string, error) { return "", errors.New("private machine detail") }, nil)
	if err == nil || !strings.Contains(err.Error(), "prerequisite_missing") || result.Started || strings.Contains(err.Error(), "private") {
		t.Fatal("missing runtime must fail safely")
	}
}

func TestHostExactCleanupDoesNotDeleteUnownedFiles(t *testing.T) {
	var extra, root string
	result, err := run(context.Background(), Request{Script: []byte("x")}, os.Executable,
		func(_ context.Context, cmd *exec.Cmd) (bool, error) {
			root = cmd.Dir
			extra = filepath.Join(root, "unowned.txt")
			if err := os.WriteFile(extra, []byte("retain"), 0600); err != nil {
				t.Fatal(err)
			}
			_, _ = io.WriteString(cmd.Stdout, `{"ok":true}`)
			return true, nil
		})
	defer func() { _ = os.Remove(extra); _ = os.Remove(root) }()
	if err == nil || !strings.Contains(err.Error(), "cleanup_failed") || result.Output != nil {
		t.Fatalf("err=%v", err)
	}
	if data, err := os.ReadFile(extra); err != nil || string(data) != "retain" {
		t.Fatal("unowned file deleted")
	}
}

func TestHostPowerShellStructuredSmoke(t *testing.T) {
	for _, name := range []string{"pwsh", "powershell.exe"} {
		t.Run(name, func(t *testing.T) {
			path, err := exec.LookPath(name)
			if err != nil {
				t.Skip("PowerShell variant absent")
			}
			value := `quote '; $env:HERDR_MESH_MUST_NOT_CHANGE = 'wrong` + "\u03bb"
			// This verifies real-runtime interoperability, not cold-start latency.
			// Deadline enforcement is exercised separately with the controlled child.
			result, err := run(context.Background(), Request{
				Script: []byte(`param([string]$OptionsPath)
$value = [IO.File]::ReadAllText($OptionsPath) | ConvertFrom-Json
@{value=$value.value} | ConvertTo-Json -Compress
`), Options: map[string]string{"value": value}, Timeout: DefaultTimeout,
			}, func() (string, error) { return path, nil }, execute)
			if err != nil {
				t.Fatal(err)
			}
			var actual map[string]string
			if json.Unmarshal(result.Output, &actual) != nil || actual["value"] != value {
				t.Fatalf("structured data changed: %q", result.Output)
			}
		})
	}
}
