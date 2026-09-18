// Package onboard implements the guided, single-identity desktop workflow.
package onboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/setup"
)

type Options struct {
	Name, Tailnet, Server, HerdrExecutable string
	Coordinator                            bool
}

// Dependencies keeps network, interactive, process and registration effects
// injectable; tests never enroll devices or change login entries.
type Dependencies struct {
	Dir           func() (string, error)
	Load          func(string) (meshlocal.Config, error)
	Save          func(string, meshlocal.Config) error
	Status        func(string) (meshlocal.Status, error)
	Running       func(string) (bool, error)
	Verify        func(context.Context, string) error
	Prerequisites func(string) error
	Executable    func() (string, error)
	Register      func(string, string) error
	Start         func(string, string, []string) error
	Browser       func(string) error
	Token         func(context.Context) ([]byte, error)
	Confirm       func(context.Context) (bool, error)
	Policy        func(context.Context, setup.Options, []byte) (setup.Report, error)
	Wait          func(context.Context) error
	Environment   func() []string
}

var portableName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func (o Options) Normalize() (Options, error) {
	if len(o.Name) > 40 || !portableName.MatchString(o.Name) ||
		regexp.MustCompile(`^(con|prn|aux|nul|com[0-9]|lpt[0-9])$`).MatchString(o.Name) {
		return o, errors.New("--name (this computer's mesh node label) must be 1..40 lowercase letters, digits or single hyphens, start with a letter, and not be a reserved Windows name")
	}
	if o.HerdrExecutable == "" {
		o.HerdrExecutable = "herdr"
	}
	if strings.ContainsAny(o.HerdrExecutable, "\x00\r\n") {
		return o, errors.New("invalid Herdr executable")
	}
	if o.Coordinator {
		if o.Server != "" || len(o.Tailnet) > 253 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9@.-]*$`).MatchString(o.Tailnet) || strings.HasPrefix(o.Tailnet, "tskey-") {
			return o, errors.New("init requires --tailnet with the actual Tailscale tailnet name")
		}
	} else {
		if o.Tailnet != "" {
			return o, errors.New("join does not accept a tailnet; use the coordinator's actual full MagicDNS name")
		}
		host := strings.ToLower(strings.TrimSuffix(o.Server, "."))
		port := "50052"
		if h, p, err := net.SplitHostPort(host); err == nil {
			number, err := strconv.Atoi(p)
			if err != nil || number < 1 || number > 65535 {
				return o, errors.New("coordinator port must be between 1 and 65535")
			}
			host, port = strings.TrimSuffix(h, "."), strconv.Itoa(number)
		}
		if !ValidDNS(host) {
			return o, errors.New("--server must be the actual full coordinator MagicDNS name ending in .ts.net, as printed by init (not a URL or an invented short name)")
		}
		o.Server = host
		if port != "50052" {
			o.Server = net.JoinHostPort(host, port)
		}
	}
	return o, nil
}

func ValidDNS(host string) bool {
	if len(host) > 253 || !strings.HasSuffix(host, ".ts.net") || len(strings.Split(host, ".")) < 4 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if !dnsLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func Run(ctx context.Context, options Options, output io.Writer, d Dependencies) (result error) {
	o, err := options.Normalize()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Prerequisites(o.HerdrExecutable); err != nil {
		return err
	}
	dir, err := d.Dir()
	if err != nil {
		return err
	}
	exe, err := d.Executable()
	if err != nil {
		return err
	}
	if !filepath.IsAbs(exe) {
		return errors.New("managed startup requires an absolute executable path")
	}
	server := ""
	if !o.Coordinator {
		server = o.Server
		if _, _, err := net.SplitHostPort(server); err != nil {
			server = net.JoinHostPort(server, "50052")
		}
	}
	cfg := meshlocal.Config{Version: 1, Name: o.Name, Tailnet: o.Tailnet, Server: server, HerdrExecutable: o.HerdrExecutable, Coordinator: o.Coordinator}
	existing, err := d.Load(dir)
	if err == nil {
		if existing != cfg {
			return fmt.Errorf("this computer already has a different managed identity in %s; reuse its exact init/join options, or explicitly stop and archive it before choosing another identity", dir)
		}
		fmt.Fprintln(output, "Reusing this computer's existing managed identity.")
	} else {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cannot inspect managed configuration; refusing replacement: %w", err)
		}
		if err := checkAdvanced(filepath.Dir(dir)); err != nil {
			return err
		}
		if entries, readErr := os.ReadDir(dir); readErr == nil && len(entries) != 0 {
			return fmt.Errorf("managed state at %s exists without a readable configuration; inspect and explicitly recover/archive it before onboarding (no identity was replaced)", dir)
		} else if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		if err := d.Save(dir, cfg); err != nil {
			return fmt.Errorf("cannot create managed identity (existing state is never overwritten): %w", err)
		}
	}
	lock := filepath.Join(dir, "onboarding.lock")
	f, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("onboarding is already running or was interrupted; confirm no init/join is active before removing %s", lock)
	}
	if err := f.Close(); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.Remove(lock)) }()
	if err := meshlocal.CheckNotDestroying(dir); err != nil {
		return err
	}
	if o.Coordinator {
		if err := configurePolicy(ctx, dir, o.Tailnet, output, d); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Register(exe, dir); err != nil {
		return fmt.Errorf("managed Windows sign-in registration: %w", err)
	}
	fmt.Fprintln(output, "Per-user Windows sign-in startup configured (not a boot service).")
	running, err := d.Running(dir)
	if err != nil {
		return fmt.Errorf("cannot inspect existing managed daemon; refusing duplicate launch: %w", err)
	}
	if !running {
		fmt.Fprintln(output, "Starting the shared managed daemon; browser enrollment may be required.")
		if err := d.Start(exe, dir, DaemonEnvironment(d.Environment())); err != nil {
			return fmt.Errorf("could not start managed daemon; saved identity is preserved: %w", err)
		}
		for attempt := 0; ; attempt++ {
			running, err = d.Running(dir)
			if err != nil {
				return fmt.Errorf("cannot verify background daemon startup: %w", err)
			}
			if running {
				break
			}
			if attempt == 15 {
				return fmt.Errorf("background daemon did not acquire its private runtime lock; inspect %s and rerun the same command (saved identity retained)", filepath.Join(dir, "daemon.log"))
			}
			if err := d.Wait(ctx); err != nil {
				return fmt.Errorf("startup verification interrupted; rerun the same command to resume: %w", err)
			}
		}
	}
	return waitReady(ctx, dir, o.Coordinator, output, d)
}

func checkAdvanced(root string) error {
	// The advanced commands own these directories. Never infer permission to
	// migrate or concurrently enroll a second identity from their presence.
	for _, name := range []string{"server", "node", "client", "ctl", "mcp", "dashboard", "server-state", "node-state", "client-state"} {
		path := filepath.Join(root, name)
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("advanced mesh state exists at %s; stop the advanced processes and explicitly archive their state before guided onboarding (nothing was migrated)", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func configurePolicy(ctx context.Context, dir, tailnet string, output io.Writer, d Dependencies) error {
	done, pending := filepath.Join(dir, "policy-complete"), filepath.Join(dir, "policy-apply-pending")
	if _, err := os.Lstat(done); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := os.Lstat(pending); err == nil {
		return fmt.Errorf("previous policy application is pending or its remote effects are unknown; inspect the Tailscale policy and private policy backups in %s before retrying. After explicitly reconciling the policy, remove only %s and rerun init for a fresh preview; no automatic mutation retry was performed", dir, pending)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fmt.Fprintln(output, "Prerequisites: Herdr, Git, and your chosen provider CLI must already be installed and authenticated. Nothing is installed globally.")
	fmt.Fprintln(output, "Policy access requires a Tailscale API access token (tskey-api-) authorized to read/write this tailnet policy. Browser device sign-in does not grant policy API access. The token is used only for this preview/application and is not saved.")
	token, err := d.Token(ctx)
	defer clear(token)
	if err != nil {
		return err
	}
	base := setup.DefaultOptions()
	base.Tailnet, base.PolicyOnly = tailnet, true
	previewDir, err := os.MkdirTemp(dir, "policy-preview-")
	if err != nil {
		return err
	}
	// The setup host creates and protects its own output directory.
	base.OutputDirectory = filepath.Join(previewDir, "artifacts")
	fmt.Fprintln(output, "Reading policy and preparing an additive preview (no enrollment keys)...")
	preview, err := d.Policy(ctx, base, token)
	if err != nil {
		return err
	}
	before, err := os.ReadFile(preview.PolicyBackup)
	if err != nil {
		return err
	}
	proposal, err := os.ReadFile(preview.PolicyProposal)
	if err != nil {
		return err
	}
	if len(before) > 1048576 || len(proposal) > 2*1048576 {
		return errors.New("policy preview exceeds the supported size")
	}
	fmt.Fprintln(output, "Proposed merged policy (existing unrelated policy is preserved; JSON serialization removes comments/formatting):")
	fmt.Fprintln(output, string(proposal))
	for _, warning := range preview.Warnings {
		if warning == "wildcard_allow_preserved" {
			fmt.Fprintln(output, "WARNING: existing wildcard access is preserved; these role grants do not establish network isolation.")
		}
	}
	fmt.Fprintln(output, "Apply this policy? [y/N]")
	confirmed, err := d.Confirm(ctx)
	if err != nil {
		return err
	}
	if !confirmed {
		return errors.New("policy application declined; no remote changes or enrollment keys were created; rerun the same init command when ready")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	hash := sha256.Sum256(before)
	base.ExpectedPolicySHA256 = hex.EncodeToString(hash[:])
	base.OutputDirectory = filepath.Join(previewDir, "apply")
	base.Apply = true
	if err := createMarker(pending, []byte("Policy application may have remote effects. Inspect before retrying.\n")); err != nil {
		return err
	}
	fmt.Fprintln(output, "Applying the reviewed policy with concurrency protection; creating zero device keys...")
	if _, err := d.Policy(ctx, base, token); err != nil {
		return fmt.Errorf("policy application did not complete; pending state retained at %s; inspect and reconcile remote policy before removing this marker and rerunning init: %w", pending, err)
	}
	if err := createMarker(done, []byte("Policy-only setup completed; no device keys created.\n")); err != nil {
		return err
	}
	if err := os.Remove(pending); err != nil {
		return err
	}
	return nil
}

func createMarker(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	return errors.Join(writeErr, f.Sync(), f.Close())
}

func waitReady(ctx context.Context, dir string, coordinator bool, output io.Writer, d Dependencies) error {
	last, opened := "", ""
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("onboarding wait stopped; the saved identity and background daemon are preserved. Rerun the same command to inspect/resume: %w", err)
		}
		running, err := d.Running(dir)
		if err != nil {
			return fmt.Errorf("cannot verify managed daemon liveness: %w", err)
		}
		if !running {
			return fmt.Errorf("managed daemon stopped before readiness; inspect %s and rerun the same command after resolving the failure (saved identity retained)", filepath.Join(dir, "daemon.log"))
		}
		status, err := d.Status(dir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cannot read private daemon status: %w", err)
		}
		if err == nil {
			progress := status.State + "\n" + status.Error
			if progress != last {
				fmt.Fprintf(output, "Managed daemon: %s\n", status.State)
				if status.Error != "" {
					fmt.Fprintln(output, status.Error)
				}
				last = progress
			}
			if status.AuthURL != "" && status.AuthURL != opened {
				if !validAuthURL(status.AuthURL) {
					return errors.New("daemon supplied an unrecognized browser sign-in URL; refusing to open it")
				}
				fmt.Fprintln(output, "Complete Tailscale browser sign-in. If your tailnet requires device approval, an administrator must approve this computer before it can become ready.")
				fmt.Fprintf(output, "Browser unavailable? Open this URL yourself: %s\n", status.AuthURL)
				if err := d.Browser(status.AuthURL); err != nil {
					fmt.Fprintln(output, "Could not open the browser automatically; use the URL above.")
				}
				opened = status.AuthURL
			}
			switch status.State {
			case "ready":
				if !ValidDNS(strings.TrimSuffix(status.DNSName, ".")) {
					return errors.New("daemon reported ready without an actual full MagicDNS name; no join endpoint can be advertised")
				}
				endpoint := strings.TrimSuffix(status.DNSName, ".")
				if coordinator && status.Server != "" {
					host, port, err := net.SplitHostPort(status.Server)
					number, portErr := strconv.Atoi(port)
					if err != nil || portErr != nil || number < 1 || number > 65535 ||
						!strings.EqualFold(strings.TrimSuffix(host, "."), endpoint) {
						return errors.New("coordinator address does not match its actual MagicDNS identity; refusing to advertise an unverified endpoint")
					}
					if number != 50052 {
						endpoint = net.JoinHostPort(endpoint, strconv.Itoa(number))
					}
				}
				if err := d.Verify(ctx, dir); err != nil {
					return fmt.Errorf("daemon readiness could not be verified through private IPC; rerun the same command to resume: %w", err)
				}
				fmt.Fprintf(output, "Ready: %s\n", status.DNSName)
				if coordinator {
					fmt.Fprintf(output, "On another computer run:\nherdr-mesh join --server %s --name <choose-node-name>\n", endpoint)
				}
				return nil
			case "error", "failed", "stopped":
				return errors.New("managed daemon is not ready; inspect the reported error and rerun the same command after resolving it (saved identity retained)")
			}
		}
		if err := d.Wait(ctx); err != nil {
			return fmt.Errorf("onboarding wait stopped; rerun the same command to resume with the saved identity: %w", err)
		}
	}
}

func DaemonEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, value, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, "TS_AUTHKEY") || strings.HasPrefix(upper, "TS_AUTH_KEY") ||
			strings.HasPrefix(upper, "TS_API_") || strings.HasPrefix(upper, "TS_OAUTH_") || upper == "TS_CLIENT_SECRET" ||
			strings.HasPrefix(upper, "TAILSCALE_") || strings.HasPrefix(upper, "HERDR_MESH_API_TOKEN") ||
			strings.HasPrefix(strings.TrimSpace(value), "tskey-") {
			continue
		}

		result = append(result, entry)
	}
	return result
}

// ClearDaemonSecrets also covers Windows sign-in launches, which inherit the
// login environment rather than the initial onboarding process environment.
func ClearDaemonSecrets() error {
	keep := make(map[string]bool)
	for _, entry := range DaemonEnvironment(os.Environ()) {
		name, _, _ := strings.Cut(entry, "=")
		keep[name] = true
	}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !keep[name] {
			if err := os.Unsetenv(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func wait(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
