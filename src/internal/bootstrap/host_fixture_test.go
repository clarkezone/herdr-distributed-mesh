package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/scripthost"
)

// These local disposable processes are fake transports: no SSH, remoting,
// scheduler, Herdr or node command is present in either fixture script.
func TestPrivateFakeProcessTransport(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows private ACL fixture")
	}
	if _, err := exec.LookPath("pwsh"); err != nil {
		t.Skip("pwsh prerequisite unavailable")
	}
	const script = `param([string]$OptionsPath)
$ErrorActionPreference = 'Stop'
$data = Get-Content -LiteralPath $OptionsPath -Raw | ConvertFrom-Json
$root = Split-Path -Parent $OptionsPath
$acl = Get-Acl -LiteralPath $root
$me = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$private = $acl.AreAccessRulesProtected
foreach ($rule in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
    if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Value -notin @($me, 'S-1-5-18')) { $private = $false }
}
@{ private = $private; root = $root; data_seen = ($data.canary -eq 'PRIVATE_FIXTURE_CANARY');
   argv_private = (-not [Environment]::CommandLine.Contains('PRIVATE_FIXTURE_CANARY'));
   asset_seen = [IO.File]::Exists((Join-Path $root 'bootstrap-node.ps1')) } | ConvertTo-Json -Compress
`
	core, err := assets.ReadFile("assets/bootstrap-node.ps1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := scripthost.Run(context.Background(), scripthost.Request{
		Script: []byte(script), Assets: map[string][]byte{"bootstrap-node.ps1": core},
		Options: map[string]string{"canary": "PRIVATE_FIXTURE_CANARY"}, Timeout: 10 * time.Second,
	})
	if err != nil || !result.Started {
		t.Fatalf("fake process: %v", err)
	}
	var receipt struct {
		Private     bool   `json:"private"`
		Root        string `json:"root"`
		DataSeen    bool   `json:"data_seen"`
		ArgvPrivate bool   `json:"argv_private"`
		AssetSeen   bool   `json:"asset_seen"`
	}
	if err := json.Unmarshal(result.Output, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.Private || !receipt.DataSeen || !receipt.ArgvPrivate || !receipt.AssetSeen {
		t.Fatal("private process transport contract failed")
	}
	if _, err := os.Stat(receipt.Root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("private artifacts were not cleaned up")
	}
}

func TestFakeProcessCancellationAndOutputLimit(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows process-tree fixture")
	}
	if _, err := exec.LookPath("pwsh"); err != nil {
		t.Skip("pwsh prerequisite unavailable")
	}
	for _, tc := range []struct {
		name, script, code string
		timeout            time.Duration
	}{
		{"deadline", `param([string]$OptionsPath) Start-Sleep -Seconds 30`, "timeout", 2 * time.Second},
		{"output", `param([string]$OptionsPath) [Console]::Write(('X' * 300000))`, "output_limit", 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			began := time.Now()
			result, err := scripthost.Run(context.Background(), scripthost.Request{
				Script: []byte(tc.script), Options: struct{}{}, Timeout: tc.timeout,
			})
			var failure *scripthost.Error
			if !result.Started || !errors.As(err, &failure) || failure.Code != tc.code || len(result.Output) != 0 {
				t.Fatalf("fake process did not bound output/lifecycle: %v", err)
			}
			if time.Since(began) > 15*time.Second {
				t.Fatal("fake process was not bounded")
			}
		})
	}
}
