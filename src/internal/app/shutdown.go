package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

func runShutdown(ctx context.Context, args []string, streams IO) error {
	return runShutdownWith(ctx, args, streams, func(ctx context.Context, options onboard.ShutdownOptions) error {
		deps := onboard.DefaultShutdownDependencies(streams.In, streams.Out)
		deps.Dir = func() (string, error) { return meshlocal.StateDir(ctx) }
		return onboard.Shutdown(ctx, options, streams.Out, deps)
	})
}

func runShutdownWith(ctx context.Context, args []string, streams IO, run func(context.Context, onboard.ShutdownOptions) error) error {
	flags := flag.NewFlagSet("shutdown", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	var options onboard.ShutdownOptions
	flags.BoolVar(&options.Destroy, "destroy", false, "also unregister this computer's Tailscale device and permanently delete managed local state")
	flags.BoolVar(&options.RemovePolicy, "remove-policy", false, "with --destroy, remove only provably owned and unused coordinator policy additions")
	flags.BoolVar(&options.DryRun, "dry-run", false, "show the exact local target and requested actions without changing anything")
	flags.BoolVar(&options.Yes, "yes", false, "explicitly skip typed destruction confirmation (automation only)")
	flags.StringVar(&options.TokenEnv, "api-token-env", "", "optional environment variable holding a Tailscale API token; otherwise prompt privately")
	timeout := flags.Duration("timeout", 2*time.Minute, "bounded shutdown/cleanup budget")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), `Usage: herdr-mesh shutdown [options]

Stop this computer's managed mesh daemon, NOT a Herdr agent.
Default: retain all state and enrollment; resume with herdr-mesh start.
--destroy: permanently remove this computer's managed installation.
--remove-policy: additionally retire its owned shared policy (coordinator only).
Herdr sessions, provider agents, repositories and other computers are never deleted.

Examples:
  herdr-mesh shutdown
  herdr-mesh shutdown --destroy --remove-policy --dry-run
  herdr-mesh shutdown --destroy --remove-policy

Destruction requires typing the local mesh node label unless --yes is supplied.
A Tailscale API token is required for destruction, not for ordinary shutdown.
Options:`)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 || *timeout > 10*time.Minute {
		return errors.New("shutdown accepts no positional arguments; timeout must be in (0,10m]")
	}
	if !options.Destroy && (options.RemovePolicy || options.Yes || options.TokenEnv != "") {
		return errors.New("--remove-policy, --yes and --api-token-env require --destroy")
	}
	op, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	return run(op, options)
}
