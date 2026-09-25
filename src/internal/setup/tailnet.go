package setup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

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
		KeyExpirySeconds: 604800, KeysPerRole: 2, Timeout: 2 * time.Minute}
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
		o.Timeout <= 0 || o.Timeout > 30*time.Minute || len(o.OutputDirectory) > 4096 ||
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
	HTTPStatus           int
	RemoteEffectsUnknown bool
	PromptedToken        bool
}

func (e *Error) Error() string {
	message := "tailnet setup failed: " + e.Code
	if e.HTTPStatus >= 100 && e.HTTPStatus <= 599 {
		message += "; Tailscale API returned HTTP " + strconv.Itoa(e.HTTPStatus)
	}
	switch e.Code {
	case "output_unavailable":
		message += "; cannot create, lock, or secure the policy output directory; use a writable ordinary directory with private permissions"
	case "token_missing", "token_kind", "token_rejected":
		if e.PromptedToken {
			switch e.Code {
			case "token_missing":
				message += "; the hidden prompt was empty; paste a complete tskey-api- access token"
			case "token_kind":
				message += "; the value read from the hidden prompt does not begin with tskey-api-; paste a complete API access token, not an enrollment key"
			case "token_rejected":
				message += "; Tailscale rejected the prompted token; check that the tskey-api- access token is current and belongs to this tailnet"
			}
		} else {
			message += "; provide a current tskey-api- access token through the configured environment variable, not an enrollment key"
		}
	case "invalid_options":
		message += "; check flag values and limits with herdr-mesh setup tailnet -help"
	case "api_read_failed":
		message += "; check the tailnet name, network access to the Tailscale API, and API token permissions"
	case "api_update_failed":
		message += "; inspect the current tailnet policy and compare it with the saved proposal before attempting another apply"
	case "key_creation_failed", "key_response_invalid":
		message += "; inspect recently created keys in the Tailscale admin console and revoke unwanted keys before creating replacements"
	case "key_revocation_unconfirmed":
		message += "; key revocation was not confirmed; inspect and revoke unwanted keys in the Tailscale admin console"
	case "key_cleanup_failed":
		message += "; inspect and remove newly created local key files after confirming their remote keys were revoked"
	case "policy_invalid":
		message += "; review the saved proposal and existing tailnet policy for invalid rules before applying changes"
	case "policy_changed":
		message += "; the policy changed since preview; create a fresh preview in a new output directory and review it before applying"
	case "output_exists":
		message += "; preserve the existing artifacts and choose a new private -output-directory"
	case "etag_missing":
		message += "; the API did not supply a policy version for a safe update; check Tailscale API availability before obtaining a fresh preview"
	case "cleanup_failed":
		message += "; setup lock cleanup failed; inspect the private output directory before retrying"
	case "local_failure":
		message += "; check free disk space and permissions in the private output directory; preserve existing recovery artifacts"
	case "timeout":
		message += "; setup exceeded its deadline; inspect policy, keys, and saved artifacts before deciding whether another run is safe"
	case "canceled":
		message += "; setup was canceled; inspect policy, keys, and saved artifacts before deciding whether another run is safe"
	default:
		message += "; inspect saved setup artifacts and the current tailnet policy/keys before further changes"
	}
	if e.RemoteEffectsUnknown {
		message += "; remote effects are unknown: do not blindly retry; inspect policy and newly created keys first"
	}
	return message
}

func Run(ctx context.Context, options Options) (Report, error) {
	return runNative(ctx, options, strings.TrimSpace(os.Getenv(options.ApiTokenEnvironmentVariable)), nil)
}

// RunWithToken keeps the prompted token in memory and sends it only in the API
// Authorization header. It is never placed in the parent environment or files.
func RunWithToken(ctx context.Context, options Options, token []byte) (Report, error) {
	report, err := runNative(ctx, options, normalizePromptedToken(token), nil)
	var setupError *Error
	if errors.As(err, &setupError) {
		setupError.PromptedToken = true
	}
	return report, err
}

func normalizePromptedToken(token []byte) string {
	value := strings.TrimSpace(string(token))
	// Some terminals wrap pasted text in bracketed-paste control sequences.
	// term.ReadPassword returns these bytes as part of the hidden input.
	const pasteStart, pasteEnd = "\x1b[200~", "\x1b[201~"
	if strings.HasPrefix(value, pasteStart) && strings.HasSuffix(value, pasteEnd) {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, pasteStart), pasteEnd))
	}
	return value
}
