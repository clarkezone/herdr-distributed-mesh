package app

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
)

func runAgentInventory(ctx context.Context, args []string, streams IO) error {
	flags := flag.NewFlagSet("agents", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	server := flags.String("server", "", "coordinator MagicDNS name or tailnet IP with port")
	timeout := flags.Duration("timeout", 20*time.Second, "overall inventory timeout")
	jsonOutput := flags.Bool("json", false, "write redacted inventory with independent source freshness")
	filter := control.InventoryFilter{}
	flags.StringVar(&filter.NodeID, "node", "", "filter by exact mesh node instance ID")
	flags.StringVar(&filter.ProjectID, "project", "", "filter by exact registered project ID")
	flags.StringVar(&filter.WorkspaceID, "workspace", "", "filter by exact workspace ID within the selected nodes/sessions")
	flags.StringVar(&filter.Provider, "provider", "", "filter by exact observed provider ID")
	flags.StringVar(&filter.Readiness, "ready", "any", "filter observed readiness: any, true, false, unknown; stale state remains stale")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *server == "" || *timeout <= 0 || flags.NArg() != 0 {
		return errors.New("agents requires -server, a positive -timeout, and no positional arguments")
	}
	operation, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	return control.AgentInventory(operation, control.Options{
		Output: streams.Out, JSON: *jsonOutput, ServerAddress: *server, Transport: network.config(),
	}, filter)
}
