package setup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/scripthost"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	o := DefaultOptions()
	o.Tailnet = "example.com"
	// macOS temp roots can traverse /var -> /private/var. Production deliberately
	// rejects linked output ancestors, so success fixtures must use physical paths.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.OutputDirectory = filepath.Join(root, "output")
	o.ApiTokenEnvironmentVariable = "HERDR_SETUP_TEST_TOKEN"
	t.Setenv(o.ApiTokenEnvironmentVariable, " tskey-api-test-secret ")
	return o
}

func testReport(o Options) Report {
	report := Report{Mode: "preview", PolicyProposal: filepath.Join(o.OutputDirectory, "policy-proposed.json"),
		PolicyBackup: filepath.Join(o.OutputDirectory, "policy-before-20260917-123456789-0123456789abcdef0123456789abcdef.json"),
		Warnings:     []string{"policy_round_trip", "wildcard_allow_preserved"}}
	if o.Apply {
		report.Mode = "applied"
		report.KeysCreated = 3 * o.KeysPerRole
		report.Warnings = append(report.Warnings, "auth_key_secrets")
	}
	return report
}

func TestSetupTypedEmbeddedPreviewAndApply(t *testing.T) {
	for _, apply := range []bool{false, true} {
		o := testOptions(t)
		o.Apply = apply
		port := 8787
		o.DashboardPort = &port
		report, err := run(context.Background(), o, func(_ context.Context, request scripthost.Request) (scripthost.Result, error) {
			if len(request.Script) == 0 || len(request.Assets["configure-tailnet.ps1"]) == 0 {
				t.Fatal("missing embedded assets")
			}
			data, _ := json.Marshal(request.Options)
			if strings.Contains(string(data), "test-secret") {
				t.Fatal("token value entered structured options")
			}
			var actual Options
			if json.Unmarshal(data, &actual) != nil || actual.Apply != apply || actual.DashboardPort == nil || *actual.DashboardPort != 8787 {
				t.Fatal("typed options changed")
			}
			output, _ := json.Marshal(map[string]any{"ok": true, "report": testReport(o)})
			return scripthost.Result{Started: true, Output: output}, nil
		})
		if err != nil || report.Mode != testReport(o).Mode {
			t.Fatalf("report=%+v err=%v", report, err)
		}
	}
}

func TestSetupSanitizesRemoteOutputAndPreservesUncertainty(t *testing.T) {
	o := testOptions(t)
	o.Apply = true
	for _, test := range []struct {
		output  string
		hostErr error
		started bool
		code    string
	}{
		{`{"ok":false,"error_code":"key_revocation_unconfirmed"}`, nil, true, "key_revocation_unconfirmed"},
		{`{"ok":false,"error_code":"tskey-api-secret"}`, nil, true, "invalid_result"},
		{`{"ok":true,"secret":"tskey-api-secret"}`, nil, true, "invalid_result"},
		{`not-json tskey-api-secret`, nil, true, "invalid_result"},
		{"", errors.New("raw remote body tskey-api-secret"), true, "execution_failed"},
		{"", &scripthost.Error{Code: "timeout"}, true, "timeout"},
		{"", &scripthost.Error{Code: "prerequisite_missing"}, false, "prerequisite_missing"},
	} {
		_, err := run(context.Background(), o, func(context.Context, scripthost.Request) (scripthost.Result, error) {
			return scripthost.Result{Started: test.started, Output: []byte(test.output)}, test.hostErr
		})
		var setupError *Error
		if !errors.As(err, &setupError) || setupError.Code != test.code || setupError.RemoteEffectsUnknown != test.started || strings.Contains(err.Error(), "tskey-") {
			t.Fatalf("err=%v", err)
		}
	}
	o.Apply = false
	_, err := run(context.Background(), o, func(context.Context, scripthost.Request) (scripthost.Result, error) {
		return scripthost.Result{Started: true}, &scripthost.Error{Code: "timeout"}
	})
	var setupError *Error
	if !errors.As(err, &setupError) || setupError.RemoteEffectsUnknown {
		t.Fatal("preview must not imply remote writes")
	}
}

func TestSetupRejectsUnsafeReportFields(t *testing.T) {
	o := testOptions(t)
	for _, change := range []func(*Report){
		func(r *Report) { r.PolicyBackup = filepath.Join(filepath.Dir(o.OutputDirectory), "secret") },
		func(r *Report) { r.PolicyProposal = "remote secret" },
		func(r *Report) { r.Warnings = append(r.Warnings, "raw API body") },
		func(r *Report) { r.KeysCreated = 1 },
		func(r *Report) { r.Mode = "applied" },
	} {
		report := testReport(o)
		change(&report)
		output, _ := json.Marshal(map[string]any{"ok": true, "report": report})
		_, err := run(context.Background(), o, func(context.Context, scripthost.Request) (scripthost.Result, error) {
			return scripthost.Result{Started: true, Output: output}, nil
		})
		if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "API body") {
			t.Fatal("unsafe report accepted or leaked")
		}
	}
}

func TestSetupTokenValidationBeforeHost(t *testing.T) {
	o := testOptions(t)
	for _, token := range []string{"", "tskey-auth-wrong-kind"} {
		t.Setenv(o.ApiTokenEnvironmentVariable, token)
		_, err := run(context.Background(), o, func(context.Context, scripthost.Request) (scripthost.Result, error) {
			t.Fatal("invalid token started host")
			return scripthost.Result{}, nil
		})
		if err == nil || strings.Contains(err.Error(), "wrong-kind") {
			t.Fatal("invalid token error")
		}
	}
}

func TestPolicyOnlyUsesPromptedTokenWithoutKeysOrParentEnvironment(t *testing.T) {
	requirePowerShell(t)
	for _, apply := range []bool{false, true} {
		o := testOptions(t)
		o.PolicyOnly, o.Apply = true, apply
		t.Setenv(o.ApiTokenEnvironmentVariable, "parent-token-is-not-used")
		report, err := runWithToken(context.Background(), o, "tskey-api-prompted-only", mockedSetupHost)
		if err != nil {
			t.Fatal(err)
		}
		if report.KeysCreated != 0 || os.Getenv(o.ApiTokenEnvironmentVariable) != "parent-token-is-not-used" {
			t.Fatal("keys created or parent environment changed")
		}
		for _, warning := range report.Warnings {
			if warning == "auth_key_secrets" || warning == "primary_alias_preserved" {
				t.Fatal("policy-only emitted key warning")
			}
		}
		entries, err := os.ReadDir(o.OutputDirectory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.Contains(entry.Name(), "-key") {
				t.Fatalf("policy-only wrote key artifact %s", entry.Name())
			}
			data, err := os.ReadFile(filepath.Join(o.OutputDirectory, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "tskey-") {
				t.Fatal("token saved")
			}
		}
		var proposal map[string]json.RawMessage
		data, err := os.ReadFile(report.PolicyProposal)
		if err != nil || json.Unmarshal(data, &proposal) != nil || !strings.Contains(string(proposal["acls"]), `"*:*"`) {
			t.Fatalf("unrelated wildcard ACL was lost: %s %v", data, err)
		}
	}
}

func TestPolicyOnlyApplyFencedToReviewedPolicy(t *testing.T) {
	requirePowerShell(t)
	for _, changed := range []bool{false, true} {
		o := testOptions(t)
		o.PolicyOnly, o.Apply = true, true
		policy := `{"acls":[{"action":"accept","src":["*"],"dst":["*:*"]}]}`
		if changed {
			policy = "a different policy was reviewed"
		}
		hash := sha256.Sum256([]byte(policy))
		o.ExpectedPolicySHA256 = hex.EncodeToString(hash[:])
		_, err := run(context.Background(), o, mockedSetupHost)
		if changed {
			var e *Error
			if !errors.As(err, &e) || e.Code != "policy_changed" {
				t.Fatalf("policy race accepted: %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSetupEmbeddedWorkflowWithMockedPowerShellAPI(t *testing.T) {
	testEmbeddedWorkflow(t, func(path string) string { return path })
}

func requirePowerShell(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("pwsh"); err != nil {
		if _, err := exec.LookPath("powershell.exe"); err != nil {
			t.Skip("PowerShell prerequisite absent")
		}
	}
}

func mockedSetupHost(ctx context.Context, request scripthost.Request) (scripthost.Result, error) {
	const mockedEntry = `param([string]$OptionsPath)
		$ErrorActionPreference='Stop'
		$global:sequence=0
		function global:Invoke-WebRequest {
		    param($Method,$Uri,$Headers,[switch]$UseBasicParsing)
		    if ($Method -ne 'Get' -or $Uri -ne 'https://api.tailscale.com/api/v2/tailnet/example.com/acl') { throw 'unexpected mocked read' }
		    return @{Headers=@{ETag='"test-etag"'};Content='{"acls":[{"action":"accept","src":["*"],"dst":["*:*"]}]}'}
		}
		function global:Invoke-RestMethod {
		    param($Method,$Uri,$Headers,$ContentType,$Body)
		    if ($Method -eq 'Post' -and $Uri.EndsWith('/acl')) {
		        if ($Headers['If-Match'] -ne '"test-etag"') { throw 'missing ETag fence' }
		        return
		    }
		    if ($Method -eq 'Post' -and $Uri.EndsWith('/keys')) {
		        $global:sequence++
		        return @{id="fake-$global:sequence";key="tskey-auth-fake-$global:sequence"}
		    }
		    if ($Method -eq 'Delete') { return }
		    throw 'unexpected mocked write'
		}
		& (Join-Path $PSScriptRoot 'actual-entry.ps1') -OptionsPath $OptionsPath
		`
	request.Assets["actual-entry.ps1"] = request.Script
	request.Script = []byte(mockedEntry)
	return scripthost.Run(ctx, request)
}

func testEmbeddedWorkflow(t *testing.T, outputPath func(string) string) {
	t.Helper()
	requirePowerShell(t)
	for _, apply := range []bool{false, true} {
		o := testOptions(t)
		o.OutputDirectory = outputPath(o.OutputDirectory)
		o.Apply = apply
		o.Timeout = 20 * time.Second
		report, err := run(context.Background(), o, mockedSetupHost)
		if err != nil {
			t.Fatal(err)
		}
		if report.Mode != testReport(o).Mode || report.KeysCreated != testReport(o).KeysCreated {
			t.Fatalf("report=%+v", report)
		}
		if _, err := os.Stat(report.PolicyBackup); err != nil {
			t.Fatal(err)
		}
		for _, role := range []string{"server", "node", "client"} {
			_, err := os.Stat(filepath.Join(o.OutputDirectory, role+"-key-1.ps1"))
			if apply && err != nil {
				t.Fatal(err)
			}
			if !apply && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("preview created keys")
			}
		}
	}
}
