package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/bootstrap"
)

// RunBootstrap is the public bootstrap router; args excludes "bootstrap".
// It does not initialize a mesh identity or tsnet.
func RunBootstrap(ctx context.Context, args []string, streams IO) error {
	return runBootstrap(ctx, args, streams, bootstrap.Run)
}

func runBootstrap(ctx context.Context, args []string, streams IO,
	run func(context.Context, bootstrap.Options) (*bootstrap.Result, error)) error {
	options, asJSON, err := parseBootstrap(args, streams)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	result, err := run(ctx, options)
	if err != nil {
		var failure *bootstrap.Error
		if asJSON && errors.As(err, &failure) {
			if writeErr := json.NewEncoder(streams.Out).Encode(failure); writeErr != nil {
				return writeErr
			}
		}
		return err
	}
	if result == nil {
		return errors.New("bootstrap returned no receipt")
	}
	if asJSON {
		return json.NewEncoder(streams.Out).Encode(result)
	}
	status := strings.ReplaceAll(result.Status, "-", " ")
	if result.Status == "staged-not-started" {
		status = "staged; node not started"
	} else if result.Status == "runner-running-mesh-unverified" {
		status = "runner is running; mesh readiness not verified"
	}
	var receipt strings.Builder
	fmt.Fprintf(&receipt, "Bootstrap: %s\nOperation ID: %s\nStartup: %s\nEnrollment: %s\nConnectivity: %s\n",
		status, result.OperationID, strings.ReplaceAll(result.Startup, "-", " "),
		strings.ReplaceAll(result.Enrollment, "-", " "), strings.ReplaceAll(result.Connectivity, "-", " "))
	for _, field := range []struct{ label, value string }{
		{"Install directory", result.InstallDirectory}, {"State directory", result.StateDirectory},
		{"Executable", result.Executable}, {"Scheduled Task", result.ExistingTaskName},
		{"Runner state", result.RunnerState}, {"Runner observed at (UTC)", result.RunnerObservedAtUTC},
	} {
		if field.value != "" {
			fmt.Fprintf(&receipt, "%s: %s\n", field.label, field.value)
		}
	}
	if result.Status == "staged-not-started" {
		fmt.Fprintln(&receipt, "Next: prepare an exact-match Scheduled Task runner, then repeat the same install inputs with -start -existing-task <task-name>. Bootstrap does not create the runner.")
	} else {
		fmt.Fprintln(&receipt, "Next: inspect the node with herdr-mesh ctl nodes -server <coordinator:port>. A running runner does not verify enrollment, connectivity, or execution readiness.")
	}
	_, err = fmt.Fprint(streams.Out, receipt.String())
	return err
}

func parseBootstrap(args []string, streams IO) (bootstrap.Options, bool, error) {
	var o bootstrap.Options
	invalid := errors.New("invalid bootstrap options; use herdr-mesh bootstrap -help (no credentials or arbitrary arguments are accepted)")
	if len(args) > 40 {
		return o, false, invalid
	}
	size := 0
	for _, arg := range args {
		size += len(arg)
		if strings.ContainsRune(arg, 0) || size > 16*1024 {
			return o, false, invalid
		}
	}
	flags := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	// flag.Parse errors echo supplied values; only fixed help and sanitized errors
	// may reach operator output.
	flags.SetOutput(io.Discard)
	flags.StringVar(&o.SSHHost, "ssh-host", "", "explicit existing literal Host alias in the operator's .ssh\\config")
	flags.StringVar(&o.ArchivePath, "archive", "", "trusted local Windows release ZIP")
	flags.StringVar(&o.ExpectedSHA256, "sha256", "", "mandatory expected archive SHA256 from a trusted channel")
	flags.StringVar(&o.Architecture, "architecture", "", "native target architecture: amd64 or arm64")
	flags.StringVar(&o.InstallDirectory, "install-dir", "", "new absolute private target install directory, or matching install for -start")
	flags.StringVar(&o.StateDirectory, "state-dir", "", "existing separate private target node state directory")
	flags.StringVar(&o.Server, "server", "", "coordinator DNS/IPv4 name and port")
	flags.StringVar(&o.HerdrExecutable, "herdr-executable", "", "optional existing absolute target Herdr .exe; configures headless mesh")
	flags.BoolVar(&o.Start, "start", false, "opt in to the existing validated Scheduled Task runner")
	flags.StringVar(&o.ExistingTaskName, "existing-task", "", "explicit full existing task name, required with -start")
	flags.DurationVar(&o.Timeout, "timeout", bootstrap.DefaultTimeout, "overall deadline: whole seconds, 1s through 30m")
	asJSON := flags.Bool("json", false, "write a typed receipt or sanitized failure object")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(streams.Out)
			fmt.Fprintln(streams.Out, "Usage: herdr-mesh bootstrap -ssh-host <alias> -archive <zip> -sha256 <hash> -architecture <amd64|arm64> -install-dir <path> -state-dir <path> -server <host:port> [options]")
			flags.PrintDefaults()
			return o, false, flag.ErrHelp
		}
		return o, false, invalid
	}
	if flags.NArg() != 0 {
		return o, false, invalid
	}
	emptyHerdr := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "herdr-executable" && o.HerdrExecutable == "" {
			emptyHerdr = true
		}
	})
	if emptyHerdr {
		return o, false, invalid
	}
	if err := bootstrap.Validate(o); err != nil {
		return o, false, err
	}
	return o, *asJSON, nil
}
