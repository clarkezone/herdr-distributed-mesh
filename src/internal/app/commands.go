package app

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func runCommandQuery(ctx context.Context, command string, args []string, streams IO) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	server := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	requiredServerTag := flags.String("required-server-tag", "tag:herdr-mesh-server", "Tailscale tag required on the actual coordinator connection")
	jsonOutput := flags.Bool("json", false, "write machine-readable command record")
	timeout := flags.Duration("timeout", 60*time.Second, "overall connection and operation timeout")
	var nodeID, key, id string
	ttl := protocol.DefaultCommandTTL
	if command == "ping" {
		flags.StringVar(&nodeID, "node", "", "target mesh node instance ID")
		flags.StringVar(&key, "idempotency-key", "", "actor-scoped request key (not an enrollment secret); reuse it for the same operation")
		flags.DurationVar(&ttl, "ttl", ttl, "probe execution deadline after admission; at most 30s")
	} else {
		flags.StringVar(&id, "id", "", "command ID returned by a previous ping")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *server == "" {
		return errors.New("-server is required")
	}
	if *requiredServerTag == "" {
		return errors.New("-required-server-tag must not be empty")
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if command == "ping" {
		if nodeID == "" {
			return errors.New("-node is required")
		}
		if ttl <= 0 || ttl > protocol.MaxCommandTTL {
			return errors.New("-ttl must be positive and at most 30s")
		}
		if key == "" {
			key = protocol.NewCommandID()
		}
	} else if !protocol.ValidCommandID(id) {
		return errors.New("-id requires a valid command ID")
	}
	op, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	options := control.Options{JSON: *jsonOutput, Output: streams.Out, ServerAddress: *server, RequiredServerTag: *requiredServerTag, Transport: network.config()}
	if command == "ping" {
		return control.Ping(op, options, nodeID, key, ttl)
	}
	return control.CommandStatus(op, options, id)
}
