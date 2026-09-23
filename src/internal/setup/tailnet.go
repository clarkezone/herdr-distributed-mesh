package setup

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/scripthost"
)

//go:embed assets/entry.ps1
var entry []byte

//go:embed assets/configure-tailnet.ps1
var script []byte

type Options struct {
	Tailnet                     string        `json:"Tailnet"`
	ApiTokenEnvironmentVariable string        `json:"ApiTokenEnvironmentVariable"`
	TagOwner                    string        `json:"TagOwner"`
	KeyExpirySeconds            int           `json:"KeyExpirySeconds"`
	KeysPerRole                 int           `json:"KeysPerRole"`
	DashboardPort               *int          `json:"DashboardPort,omitempty"`
	OutputDirectory             string        `json:"OutputDirectory"`
	Apply                       bool          `json:"Apply"`
	PolicyOnly                  bool          `json:"PolicyOnly"`
	ExpectedPolicySHA256        string        `json:"ExpectedPolicySHA256,omitempty"`
	Timeout                     time.Duration `json:"-"`
}

func DefaultOptions() Options {
	return Options{ApiTokenEnvironmentVariable: "TAILSCALE_API_TOKEN", TagOwner: "autogroup:admin",
		KeyExpirySeconds: 604800, KeysPerRole: 2, Timeout: scripthost.DefaultTimeout}
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_@.-]{1,253}$`)
var owner = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_@.:+-]{0,253}$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

func (o Options) Normalize() (Options, error) {
	if o.PolicyOnly {
		o.KeysPerRole = 0
	}
	if !identifier.MatchString(o.Tailnet) || strings.HasPrefix(o.Tailnet, "tskey-") ||
		!owner.MatchString(o.TagOwner) || strings.HasPrefix(o.TagOwner, "tskey-") ||
		!environmentName.MatchString(o.ApiTokenEnvironmentVariable) ||
		o.KeyExpirySeconds < 3600 || o.KeyExpirySeconds > 7776000 || (!o.PolicyOnly && (o.KeysPerRole < 1 || o.KeysPerRole > 100)) ||
		(o.ExpectedPolicySHA256 != "" && !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(o.ExpectedPolicySHA256)) ||
		(o.DashboardPort != nil && (*o.DashboardPort < 1 || *o.DashboardPort > 65535)) ||
		o.Timeout <= 0 || o.Timeout > scripthost.MaxTimeout || len(o.OutputDirectory) > 4096 ||
		strings.ContainsAny(o.OutputDirectory, "\x00\r\n") {
		return o, &Error{Code: "invalid_options"}
	}
	if o.OutputDirectory == "" {
		base, err := os.UserConfigDir()
		if runtime.GOOS == "windows" {
			base, err = os.UserCacheDir()
		}
		if err != nil {
			return o, &Error{Code: "output_unavailable"}
		}
		o.OutputDirectory = filepath.Join(base, "herdr-mesh", "tailnet-setup")
	}
	path, err := filepath.Abs(o.OutputDirectory)
	if err != nil || filepath.Dir(path) == path {
		return o, &Error{Code: "output_unavailable"}
	}
	o.OutputDirectory = path
	return o, nil
}

type Report struct {
	Mode           string   `json:"mode"`
	PolicyBackup   string   `json:"policy_backup"`
	PolicyProposal string   `json:"policy_proposal"`
	KeysCreated    int      `json:"keys_created"`
	Warnings       []string `json:"warnings"`
}

type Error struct {
	Code                 string
	RemoteEffectsUnknown bool
}

func (e *Error) Error() string {
	message := "tailnet setup failed: " + e.Code
	switch e.Code {
	case "prerequisite_missing":
		message += "; PowerShell is required (pwsh on PATH, or Windows PowerShell on Windows)"
	case "prerequisite_version":
		message += "; PowerShell 7.3 or newer is required on Linux/macOS"
	case "output_unavailable":
		message += "; cannot create, lock, or secure the policy output directory; use a writable ordinary directory with private permissions"
	case "token_missing", "token_kind", "token_rejected":
		message += "; provide a current tskey-api- access token through the configured environment variable, not an enrollment key"
	case "invalid_options":
		message += "; check flag values and limits with herdr-mesh setup tailnet -help"
	case "api_read_failed":
		message += "; check the tailnet name, network access to the Tailscale API, and API token permissions"
	case "api_update_failed":
		message += "; inspect the current tailnet policy and compare it with the saved proposal before attempting another apply"
	case "key_creation_failed", "key_response_invalid":
		message += "; inspect recently created keys in the Tailscale admin console and revoke unwanted keys before creating replacements"
	case "key_revocation_unconfirmed", "key_cleanup_failed":
		message += "; key revocation was not confirmed; inspect and revoke unwanted keys in the Tailscale admin console"
	case "policy_invalid":
		message += "; review the saved proposal and existing tailnet policy for invalid rules before applying changes"
	case "policy_changed":
		message += "; the policy changed since preview; create a fresh preview in a new output directory and review it before applying"
	case "output_exists":
		message += "; preserve the existing artifacts and choose a new private -output-directory"
	case "etag_missing":
		message += "; the API did not supply a policy version for a safe update; check Tailscale API availability before obtaining a fresh preview"
	case "cleanup_failed":
		message += "; temporary-file cleanup failed; secure and remove leftover setup temporary files after checking whether setup changed remote state"
	case "temporary_io", "local_failure":
		message += "; check free disk space and permissions on private temporary and output directories; preserve existing recovery artifacts"
	case "invalid_request":
		message += "; the embedded setup request was rejected; check setup options with herdr-mesh setup tailnet -help and use a compatible release"
	case "output_limit", "invalid_result":
		message += "; setup returned an unverified receipt; inspect saved artifacts and remote policy/keys before any further apply"
	case "timeout":
		message += "; setup exceeded its deadline; inspect policy, keys, and saved artifacts before deciding whether another run is safe"
	case "canceled":
		message += "; setup was canceled; inspect policy, keys, and saved artifacts before deciding whether another run is safe"
	case "execution_failed":
		message += "; the setup runtime failed; check PowerShell availability and local permissions, then inspect policy and keys before another apply"
	default:
		message += "; inspect saved setup artifacts and the current tailnet policy/keys before further changes"
	}
	if e.RemoteEffectsUnknown {
		message += "; remote effects are unknown: do not blindly retry; inspect policy and newly created keys first"
	}
	return message
}

func Run(ctx context.Context, options Options) (Report, error) {
	return run(ctx, options, scripthost.Run)
}

// RunWithToken supplies a prompted token only to the child environment, never to
// the parent environment, arguments, options file, or report.
func RunWithToken(ctx context.Context, options Options, token []byte) (Report, error) {
	return runWithToken(ctx, options, strings.TrimSpace(string(token)), scripthost.Run)
}

func run(ctx context.Context, options Options, host func(context.Context, scripthost.Request) (scripthost.Result, error)) (Report, error) {
	return runWithToken(ctx, options, strings.TrimSpace(os.Getenv(options.ApiTokenEnvironmentVariable)), host)
}

func runWithToken(ctx context.Context, options Options, token string, host func(context.Context, scripthost.Request) (scripthost.Result, error)) (Report, error) {
	options, err := options.Normalize()
	if err != nil {
		return Report{}, err
	}
	if token == "" {
		return Report{}, &Error{Code: "token_missing"}
	}
	if !strings.HasPrefix(token, "tskey-api-") {
		return Report{}, &Error{Code: "token_kind"}
	}
	result, err := host(ctx, scripthost.Request{Script: entry,
		Assets: map[string][]byte{"configure-tailnet.ps1": script}, Options: options, Timeout: options.Timeout,
		Environment: map[string]string{options.ApiTokenEnvironmentVariable: token}})
	fail := func(code string) (Report, error) {
		return Report{}, &Error{Code: code, RemoteEffectsUnknown: options.Apply && result.Started}
	}
	if err != nil {
		var hostError *scripthost.Error
		if errors.As(err, &hostError) {
			switch hostError.Code {
			case "prerequisite_missing", "invalid_request", "temporary_io", "cleanup_failed", "output_limit", "timeout", "canceled", "execution_failed":
				return fail(hostError.Code)
			}
		}
		return fail("execution_failed")
	}
	var envelope struct {
		OK        bool    `json:"ok"`
		Report    *Report `json:"report,omitempty"`
		ErrorCode string  `json:"error_code,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Output))
	decoder.DisallowUnknownFields()
	if len(result.Output) > scripthost.MaxOutputBytes || decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF {
		return fail("invalid_result")
	}
	if !envelope.OK {
		switch envelope.ErrorCode {
		case "api_read_failed", "api_update_failed", "key_creation_failed", "key_response_invalid",
			"key_revocation_unconfirmed", "policy_invalid", "policy_changed", "output_exists", "output_unavailable", "local_failure", "token_rejected", "etag_missing", "key_cleanup_failed", "prerequisite_version":
			return fail(envelope.ErrorCode)
		default:
			return fail("invalid_result")
		}
	}
	report := envelope.Report
	if report == nil || envelope.ErrorCode != "" || report.PolicyProposal != filepath.Join(options.OutputDirectory, "policy-proposed.json") ||
		filepath.Dir(report.PolicyBackup) != options.OutputDirectory || !regexp.MustCompile(`^policy-before-[0-9]{8}-[0-9]{9}-[a-f0-9]{32}\.json$`).MatchString(filepath.Base(report.PolicyBackup)) {
		return fail("invalid_result")
	}
	expectedMode, expectedKeys := "preview", 0
	if options.Apply {
		expectedMode, expectedKeys = "applied", 3*options.KeysPerRole
	}
	if report.Mode != expectedMode || report.KeysCreated != expectedKeys || len(report.Warnings) > 4 {
		return fail("invalid_result")
	}
	seen := make(map[string]bool)
	for _, warning := range report.Warnings {
		switch warning {
		case "policy_round_trip", "wildcard_allow_preserved", "primary_alias_preserved", "auth_key_secrets":
			if seen[warning] {
				return fail("invalid_result")
			}
			seen[warning] = true
		default:
			return fail("invalid_result")
		}
	}
	if !seen["policy_round_trip"] || (options.Apply && !options.PolicyOnly && !seen["auth_key_secrets"]) ||
		(options.PolicyOnly && (seen["auth_key_secrets"] || seen["primary_alias_preserved"])) {
		return fail("invalid_result")
	}
	return *report, nil
}
