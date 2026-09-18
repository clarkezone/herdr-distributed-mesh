package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

func runOnboarding(ctx context.Context, command string, args []string, streams IO) error {
	return runOnboardingWith(ctx, command, args, streams, onboard.Run)
}

func runOnboardingWith(ctx context.Context, command string, args []string, streams IO,
	run func(context.Context, onboard.Options, io.Writer, onboard.Dependencies) error) error {
	if command != "init" && command != "join" {
		return errors.New("guided onboarding requires init or join")
	}
	options := onboard.Options{Coordinator: command == "init", HerdrExecutable: "herdr"}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.Name, "name", "", "unique portable name for this computer (required)")
	flags.StringVar(&options.HerdrExecutable, "herdr", "herdr", "existing Herdr executable (default: herdr from PATH)")
	if options.Coordinator {
		flags.StringVar(&options.Tailnet, "tailnet", "", "Tailscale tailnet (required); policy API token is prompted once, hidden")
	} else {
		flags.StringVar(&options.Server, "server", "", "actual full coordinator MagicDNS name printed by init (required); port 50052 is implicit")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(streams.Out, "herdr-mesh %s: one shared managed daemon and identity per computer.\nWindows per-user sign-in startup; Herdr, Git and provider CLI must already be installed.\n", command)
			flags.SetOutput(streams.Out)
			flags.PrintDefaults()
			return flag.ErrHelp
		}
		return fmt.Errorf("invalid %s flags; use herdr-mesh %s --help", command, command)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s does not accept positional arguments", command)
	}
	var err error
	options, err = options.Normalize()
	if err != nil {
		return err
	}
	return run(ctx, options, streams.Out, onboard.DefaultDependencies(streams.In, streams.Out))
}

func runManagedDaemon(ctx context.Context, args []string, streams IO) error {
	return runManagedDaemonWith(ctx, args, streams, func(ctx context.Context, dir string, output io.Writer) error {
		if err := onboard.ClearDaemonSecrets(); err != nil {
			return err
		}
		return meshlocal.Run(ctx, dir, output)
	})
}

func runManagedDaemonWith(ctx context.Context, args []string, streams IO, run func(context.Context, string, io.Writer) error) error {
	flags := flag.NewFlagSet("managed-run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dir := flags.String("state-dir", "", "private managed root (internal launcher only)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(streams.Out)
			flags.PrintDefaults()
			return flag.ErrHelp
		}
		return errors.New("invalid internal managed-run flags")
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*dir) {
		return errors.New("managed-run requires one absolute --state-dir and no positional arguments")
	}
	return run(ctx, *dir, streams.Err)
}
