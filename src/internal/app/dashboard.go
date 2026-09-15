package app

import (
	"context"
	"errors"
	"flag"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/dashboard"
)

func runDashboard(ctx context.Context, args []string, streams IO) error {
	flags := flag.NewFlagSet("dashboard", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-dashboard", "dashboard", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	serverAddress := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	listenAddress := flags.String("listen", "127.0.0.1:8787", "local browser address; must be a literal loopback IP and port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("dashboard does not accept positional arguments")
	}
	return dashboard.Run(ctx, dashboard.Options{
		ServerAddress: *serverAddress,
		ListenAddress: *listenAddress,
		Transport:     network.config(),
		Output:        streams.Out,
	})
}
