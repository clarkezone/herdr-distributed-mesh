package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

type managedAccessError struct {
	message string
	cause   error
}

func (e *managedAccessError) Error() string { return e.message }
func (e *managedAccessError) Unwrap() error { return e.cause }

func explainManagedConnection(ctx context.Context, cause error) error {
	if errors.Is(cause, context.Canceled) {
		return cause
	}
	dir, err := meshlocal.StateDir(ctx)
	if err != nil {
		return errors.Join(cause, err)
	}
	message := ""
	if _, err := meshlocal.Load(dir); errors.Is(err, os.ErrNotExist) {
		message = fmt.Sprintf("no mesh is configured in %s. Run herdr-mesh init --tailnet <tailnet> to create a mesh, or herdr-mesh join --server <controller-address> to join one", dir)
	} else if err != nil {
		message = fmt.Sprintf("cannot read mesh configuration in %s; check that the directory exists and is accessible: %v", dir, err)
	} else {
		running, probeErr := meshlocal.IsRunning(dir)
		switch {
		case probeErr != nil:
			message = fmt.Sprintf("cannot check whether mesh is running; check access to %s. Details: %v", dir, probeErr)
		case !running:
			message = "mesh is stopped. Run herdr-mesh start to resume it"
		default:
			local, statusErr := meshlocal.ReadStatus(dir)
			switch {
			case statusErr != nil && !errors.Is(statusErr, os.ErrNotExist):
				message = fmt.Sprintf("mesh is running but its status cannot be read: %v", statusErr)
			case local.State == "login_required":
				message = "mesh is waiting for Tailscale sign-in. Run herdr-mesh start to reopen the sign-in page"
			case local.State == "starting" || errors.Is(statusErr, os.ErrNotExist):
				message = "mesh is still starting. Wait a moment, then run herdr-mesh status"
			default:
				message = "mesh is running but is not responding. Run herdr-mesh doctor and inspect the log"
			}
		}
	}
	message += fmt.Sprintf("\nState directory: %s\nLog: %s", dir, filepath.Join(dir, "daemon.log"))
	return &managedAccessError{message: message, cause: cause}
}

func printLocalStatus(output io.Writer, dir string, local meshlocal.Status) error {
	if _, err := fmt.Fprintf(output, "Mesh: %s\nState directory: %s\n", onboard.StatusLabel(local.State), dir); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"Address", local.DNSName}, {"Controller", local.Server}, {"Details", local.Error},
	} {
		if field.value != "" {
			if _, err := fmt.Fprintf(output, "%s: %s\n", field.name, field.value); err != nil {
				return err
			}
		}
	}
	if local.State == "login_required" && onboard.ValidAuthURL(local.AuthURL) {
		_, err := fmt.Fprintf(output, "Complete Tailscale sign-in: %s\n", local.AuthURL)
		return err
	}
	return nil
}

func printControllerHelp(ctx context.Context, output io.Writer) error {
	dir, err := meshlocal.StateDir(ctx)
	if err != nil {
		return err
	}
	return printControllerInstructions(dir, output)
}

func printControllerInstructions(dir string, output io.Writer) error {
	cfg, err := meshlocal.Load(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read this installation's configuration for join instructions: %w", err)
	}
	if !cfg.Coordinator {
		return nil
	}
	local, err := meshlocal.ReadStatus(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot read controller status for join instructions: %w", err)
	}
	if local.DNSName == "" {
		_, err := fmt.Fprintln(output, "\nThis controller has not reported its address yet. Run herdr-mesh status to check setup progress.")
		return err
	}
	if err := onboard.PrintJoinInstructions(output, local); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, "The controller must be running when another computer joins.")
	return err
}
