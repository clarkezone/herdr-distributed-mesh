package app

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/deinitnet"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/onboard"
)

// runPolicy recovers a retired mesh whose local state is already gone. It
// requires no remaining Herdr mesh role devices and never discovers a device
// deletion target.
func runPolicy(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 || args[0] != "cleanup" {
		return errors.New("usage: herdr-mesh policy cleanup --tailnet <tailnet> [--api-token-env <name>]")
	}
	flags := flag.NewFlagSet("policy cleanup", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	tailnet := flags.String("tailnet", "", "tailnet whose retired Herdr mesh policy should be removed")
	tokenEnv := flags.String("api-token-env", "", "environment variable containing a Tailscale API token; otherwise prompt privately")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *tailnet == "" {
		return errors.New("policy cleanup requires --tailnet and no positional arguments")
	}
	op, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	fmt.Fprintln(streams.Out, "This removes Herdr mesh policy entries if no device still uses a Herdr mesh role. Unrelated devices and policy are preserved.")
	token, err := onboard.CleanupToken(op, *tokenEnv, streams.In, streams.Out)
	if err != nil {
		return err
	}
	defer clear(token)
	client, err := deinitnet.New(token, deinitnet.Options{})
	if err != nil {
		return err
	}
	defer client.Close()
	plan, err := client.PrepareMeshPolicyRemoval(op, *tailnet)
	if err != nil {
		return err
	}
	if err := client.CheckPolicyDependencies(op, plan, ""); err != nil {
		return err
	}
	if !plan.HasChanges() {
		fmt.Fprintln(streams.Out, "Herdr mesh policy entries are already absent.")
		return nil
	}
	fmt.Fprintf(streams.Out, "Tailnet: %s\nHerdr mesh grants and tag owners will be removed. Type the tailnet name to confirm: ", *tailnet)
	line, err := bufio.NewReader(io.LimitReader(streams.In, 512)).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if strings.TrimSpace(line) != *tailnet {
		return errors.New("policy cleanup declined; nothing was changed")
	}
	result, err := client.ApplyPolicy(op, plan)
	if err != nil {
		return fmt.Errorf("policy cleanup not confirmed; inspect the remote policy before retrying: %w", err)
	}
	if result.Reconciled {
		fmt.Fprintln(streams.Out, "Herdr mesh policy removal confirmed by readback.")
	} else {
		fmt.Fprintln(streams.Out, "Herdr mesh policy removed.")
	}
	return nil
}
