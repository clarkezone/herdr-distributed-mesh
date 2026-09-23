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

// Start resumes saved configuration without enrollment setup or policy changes.
func Start(ctx context.Context, output io.Writer, d Dependencies) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := d.Dir()
	if err != nil {
		return err
	}
	cfg, err := d.Load(dir)
	if err != nil {
		return fmt.Errorf("load saved mesh at %s; run init or join first (no enrollment attempted): %w", dir, err)
	}
	if err := d.Prerequisites(cfg.HerdrExecutable); err != nil {
		return err
	}
	exe, err := d.Executable()
	if err != nil {
		return err
	}
	if !filepath.IsAbs(exe) {
		return errors.New("managed startup requires an absolute executable path")
	}
	lock := filepath.Join(dir, "onboarding.lock")
	file, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("init/join/start/shutdown is active or was interrupted; inspect %s before retrying: %w", lock, err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.Remove(lock)) }()
	if err := meshlocal.CheckNotDestroying(dir); err != nil {
		return err
	}
	if cfg.Coordinator {
		if _, err := os.Stat(filepath.Join(dir, "policy-complete")); err != nil {
			return fmt.Errorf("coordinator policy setup is incomplete; finish the original init command before start: %w", err)
		}
	}
	fmt.Fprintln(output, "Starting from saved mesh configuration.")
	return launchAndWait(ctx, exe, dir, cfg.Coordinator, output, d)
}
