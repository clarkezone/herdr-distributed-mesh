package onboard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

type ShutdownOptions struct {
	Destroy, RemovePolicy, DryRun, Yes bool
	TokenEnv                           string
}

type RemoteCleanup interface {
	DeleteDevice(context.Context) error
	RemovePolicy(context.Context) error
	Close()
}

type ShutdownDependencies struct {
	Dir        func() (string, error)
	Stop       func(context.Context, string) error
	Unregister func(string) error
	Identity   func(context.Context, string) (meshlocal.ManagedIdentity, error)
	Confirm    func(context.Context, string) (bool, error)
	Token      func(context.Context, string) ([]byte, error)
	Remote     func(context.Context, string, meshlocal.Config, meshlocal.ManagedIdentity, bool, []byte) (RemoteCleanup, error)
	Purge      func(context.Context, string) error
}

func Shutdown(ctx context.Context, o ShutdownOptions, output io.Writer, d ShutdownDependencies) (result error) {
	if o.RemovePolicy && !o.Destroy {
		return errors.New("--remove-policy requires --destroy; ordinary shutdown never changes Tailscale or local data")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := d.Dir()
	if err != nil {
		return err
	}
	cfg, err := meshlocal.LoadForCleanup(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if _, statErr := os.Lstat(dir); errors.Is(statErr, os.ErrNotExist) {
				_, err = fmt.Fprintln(output, "No managed installation exists on this computer. Nothing was changed.")
				return err
			}
		}
		return fmt.Errorf("cannot inspect managed installation; refusing cleanup: %w", err)
	}
	if o.RemovePolicy && !cfg.Coordinator {
		return errors.New("--remove-policy must run on the managed coordinator that owns the policy receipt")
	}
	fmt.Fprintf(output, "Target: mesh node %q at %s\n", cfg.Name, dir)
	if cfg.Coordinator {
		fmt.Fprintln(output, "This is the coordinator: other nodes lose mesh control until a coordinator is available. They are not deleted.")
	}
	if !o.Destroy {
		fmt.Fprintln(output, "Shutdown only: preserve databases, Tailscale enrollment, startup entry, Herdr sessions, agents and repositories.")
		if o.DryRun {
			return nil
		}
		if err := d.Stop(ctx, dir); err != nil {
			return err
		}
		fmt.Fprintln(output, "Managed daemon stopped. State and enrollment retained; Windows sign-in startup remains enabled if previously configured.")
		return nil
	}
	fmt.Fprintln(output, "DESTROY: stop the managed daemon, remove its sign-in entry and exact Tailscale device, and delete all managed databases, journals, configuration and tsnet state.")
	fmt.Fprintln(output, "Herdr sessions, provider processes, checkouts, Git worktrees and other computers are NOT destroyed.")
	if o.RemovePolicy {
		fmt.Fprintln(output, "Also remove provably owned policy additions, only when no other device depends on them. Unrelated policy is preserved.")
	} else {
		fmt.Fprintln(output, "Tailnet policy is retained; use --remove-policy only when retiring the coordinator's shared mesh policy.")
	}
	identity, err := d.Identity(ctx, dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(output, "Pinned Tailscale device: %s (%s)\n", identity.DeviceID, identity.DNSName)
	if o.DryRun {
		fmt.Fprintln(output, "Dry run: no startup, process, remote policy/device or local data changes. Remote authorization/conflicts are checked before actual teardown.")
		return nil
	}
	if !o.Yes {
		fmt.Fprintf(output, "Type the mesh node label %q to confirm irreversible destruction: ", cfg.Name)
		ok, err := d.Confirm(ctx, cfg.Name)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("destruction declined; nothing was changed")
		}
	}
	lock := filepath.Join(dir, "onboarding.lock")
	file, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("init/join/shutdown is already active or was interrupted; inspect the retained onboarding.lock before retrying")
	}
	if err := file.Close(); err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(lock); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	current, err := meshlocal.LoadForCleanup(dir)
	if err != nil || current != cfg {
		return errors.New("managed configuration changed before teardown; nothing was destroyed")
	}
	token, err := d.Token(ctx, o.TokenEnv)
	defer clear(token)
	if err != nil {
		return fmt.Errorf("remote cleanup authorization unavailable; no teardown performed: %w", err)
	}
	// Authenticate and resolve every requested remote change before disabling
	// startup or erasing the identity needed to reconcile a partial teardown.
	remote, err := d.Remote(ctx, dir, cfg, identity, o.RemovePolicy, token)
	if err != nil {
		return fmt.Errorf("remote cleanup preflight failed; no teardown performed: %w", err)
	}
	defer remote.Close()
	record, err := meshlocal.ReadDestroyState(dir)
	if errors.Is(err, os.ErrNotExist) {
		record = meshlocal.DestroyState{Configuration: cfg, Identity: identity, RemovePolicy: o.RemovePolicy}
	} else if err != nil {
		return err
	}
	if record.Identity.DeviceID != identity.DeviceID {
		return errors.New("destroy device changed; refusing replacement")
	}
	if record.RemovePolicy != o.RemovePolicy {
		return errors.New("resume destruction with the original --remove-policy choice; do not bypass incomplete remote cleanup")
	}
	if err := meshlocal.SaveDestroyState(dir, record); err != nil {
		return err
	}
	if err := d.Unregister(dir); err != nil {
		return fmt.Errorf("remove managed startup: %w; destroy intent retained, state not purged", err)
	}
	if err := d.Stop(ctx, dir); err != nil {
		return fmt.Errorf("stop managed daemon: %w; destroy intent retained, state not purged", err)
	}
	if err := remote.DeleteDevice(ctx); err != nil {
		return fmt.Errorf("device removal not confirmed: %w; local state retained, rerun the same destroy command", err)
	}
	record.DeviceRemoved = true
	if err := meshlocal.SaveDestroyState(dir, record); err != nil {
		return err
	}
	if o.RemovePolicy {
		if err := remote.RemovePolicy(ctx); err != nil {
			return fmt.Errorf("policy removal not confirmed: %w; local recovery state retained, rerun the same destroy command", err)
		}
		record.PolicyRemoved = true
		if err := meshlocal.SaveDestroyState(dir, record); err != nil {
			return err
		}
	}
	if err := d.Purge(ctx, dir); err != nil {
		return fmt.Errorf("remote cleanup completed but local purge is incomplete: %w", err)
	}
	fmt.Fprintln(output, "Managed installation destroyed: daemon stopped, startup removed, exact Tailscale device absent, and managed local state deleted. You can run init/join from clean state.")
	return nil
}
