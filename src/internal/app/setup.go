package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/setup"
)

// RunSetup handles args after "setup"; it does not initialize tsnet or enroll.
func RunSetup(ctx context.Context, args []string, streams IO) error {
	return runSetup(ctx, args, streams, setup.Run)
}

func runSetup(ctx context.Context, args []string, streams IO, run func(context.Context, setup.Options) (setup.Report, error)) error {
	if len(args) == 0 || args[0] != "tailnet" {
		return errors.New("setup requires tailnet")
	}
	options := setup.DefaultOptions()
	flags := flag.NewFlagSet("setup tailnet", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.Tailnet, "tailnet", "", "Tailscale tailnet name (required)")
	flags.StringVar(&options.ApiTokenEnvironmentVariable, "api-token-env", options.ApiTokenEnvironmentVariable, "environment variable NAME containing a tskey-api- token; never a token value")
	flags.StringVar(&options.TagOwner, "tag-owner", options.TagOwner, "owner added to each role tag without removing existing owners")
	flags.IntVar(&options.KeyExpirySeconds, "key-expiry-seconds", options.KeyExpirySeconds, "one-off enrollment key lifetime, 3600..7776000 seconds")
	flags.IntVar(&options.KeysPerRole, "keys-per-role", options.KeysPerRole, "one-off keys per server/node/client role, 1..100")
	port := flags.Int("dashboard-port", 0, "optional dashboard TCP grant from all Tailscale policy * sources to mesh server, 1..65535 except 50052")
	flags.StringVar(&options.OutputDirectory, "output-directory", "", "private local artifacts directory; existing files are never overwritten")
	flags.BoolVar(&options.Apply, "apply", false, "explicitly update remote policy and create keys; default is PREVIEW")
	flags.DurationVar(&options.Timeout, "timeout", options.Timeout, "bounded API operation deadline, at most 30m")
	asJSON := flags.Bool("json", false, "emit a safe structured report without keys or remote payloads")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(streams.Out)
			flags.PrintDefaults()
			return nil
		}
		return errors.New("invalid setup tailnet flags; use setup tailnet -help")
	}
	if flags.NArg() != 0 {
		return errors.New("setup tailnet does not accept positional arguments")
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "dashboard-port" {
			options.DashboardPort = port
		}
	})
	options, err := options.Normalize()
	if err != nil {
		return err
	}
	report, err := run(ctx, options)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(streams.Out).Encode(report)
	}
	if _, err := fmt.Fprintf(streams.Out, "tailnet setup: %s; keys_created=%d\nPolicy backup: %s\nPolicy proposal: %s\n",
		report.Mode, report.KeysCreated, report.PolicyBackup, report.PolicyProposal); err != nil {
		return err
	}
	for _, warning := range report.Warnings {
		var message string
		switch warning {
		case "policy_round_trip":
			message = "Applying JSON serialization removes HuJSON comments/formatting; preserve the untouched private backup."
		case "wildcard_allow_preserved":
			message = "Existing wildcard rules are preserved: role grants do NOT establish network isolation."
		case "primary_alias_preserved":
			message = "Existing primary key aliases were preserved; use this run's numbered key files."
		case "auth_key_secrets":
			message = "Numbered server/node/client key files are secrets; revoke unused keys and remove consumed files after enrollment."
		}
		if _, err := fmt.Fprintln(streams.Out, "WARNING: "+message); err != nil {
			return err
		}
	}
	return nil
}
