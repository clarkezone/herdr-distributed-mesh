package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/identity"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/server"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
)

const defaultPort = "50052"

type IO struct {
	Out io.Writer
	Err io.Writer
}

type networkFlags struct {
	authKeyEnv string
	debug      bool
	hostname   string
	stateDir   string
	tags       string
}

func Run(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 {
		printUsage(streams.Err)
		return flag.ErrHelp
	}

	switch args[0] {
	case "server":
		return runServer(ctx, args[1:], streams)
	case "node":
		return runNode(ctx, args[1:], streams)
	case "ctl":
		return runControl(ctx, args[1:], streams)
	case "doctor":
		return runDoctor(ctx, args[1:], streams)
	case "dashboard":
		return runDashboard(ctx, args[1:], streams)
	case "version":
		fmt.Fprintln(streams.Out, buildinfo.Version)
		return nil
	case "help", "-h", "--help":
		printUsage(streams.Out)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runServer(ctx context.Context, args []string, streams IO) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-server", "server", "TS_AUTHKEY_SERVER", "tag:herdr-mesh-server")
	listen := flags.String("listen", ":"+defaultPort, "tsnet TCP listen address")
	requiredClientTag := flags.String("required-client-tag", "tag:herdr-mesh-client", "Tailscale tag required for fleet read requests")
	requiredNodeTag := flags.String("required-node-tag", "tag:herdr-mesh-node", "Tailscale tag required for node streams")
	if err := flags.Parse(args); err != nil {
		return err
	}

	instanceID, err := identity.LoadOrCreate(network.stateDir)
	if err != nil {
		return err
	}
	return server.Run(ctx, server.Options{
		BindingPath:       filepath.Join(network.stateDir, "node-bindings.jsonl"),
		DatabasePath:      filepath.Join(network.stateDir, "coordinator", "mesh.db"),
		InstanceID:        instanceID,
		ListenAddress:     *listen,
		RequiredClientTag: *requiredClientTag,
		RequiredNodeTag:   *requiredNodeTag,
		Transport:         network.config(),
	})
}

func runNode(ctx context.Context, args []string, streams IO) error {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, defaultNodeHostname(), "node", "TS_AUTHKEY_NODE", "tag:herdr-mesh-node")
	serverAddress := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	heartbeat := flags.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	herdrSocket := flags.String("herdr-socket", "", "opt-in read-only Herdr socket marker path (Windows named pipe); empty disables integration")
	reconnectDelay := flags.Duration("reconnect-delay", 2*time.Second, "delay before reopening a failed stream")
	reconnectMaximum := flags.Duration("reconnect-max-delay", time.Minute, "maximum delay before reopening a failed stream")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *serverAddress == "" {
		return errors.New("-server is required")
	}
	if *heartbeat <= 0 {
		return errors.New("-heartbeat must be greater than zero")
	}
	if *reconnectDelay <= 0 {
		return errors.New("-reconnect-delay must be greater than zero")
	}
	if *reconnectMaximum < *reconnectDelay {
		return errors.New("-reconnect-max-delay must be greater than or equal to -reconnect-delay")
	}

	instanceID, err := identity.LoadOrCreate(network.stateDir)
	if err != nil {
		return err
	}
	return node.Run(ctx, node.Options{
		HeartbeatInterval: *heartbeat,
		HerdrSocket:       *herdrSocket,
		InstanceID:        instanceID,
		ReconnectDelay:    *reconnectDelay,
		ReconnectMaximum:  *reconnectMaximum,
		ServerAddress:     *serverAddress,
		Transport:         network.config(),
	})
}

func runControl(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 {
		return errors.New("ctl requires a command; currently supported: server-info, nodes")
	}
	switch args[0] {
	case "server-info":
		return runFleetQuery(ctx, args[1:], streams, false, false)
	case "nodes":
		return runFleetQuery(ctx, args[1:], streams, false, true)
	default:
		return fmt.Errorf("unknown ctl command %q", args[0])
	}
}

func runDoctor(ctx context.Context, args []string, streams IO) error {
	fmt.Fprintln(streams.Out, "checking tsnet enrollment, server reachability, and protocol compatibility")
	if err := runFleetQuery(ctx, args, streams, true, false); err != nil {
		return fmt.Errorf("doctor failed: %w", err)
	}
	fmt.Fprintln(streams.Out, "doctor passed")
	return nil
}

func runFleetQuery(ctx context.Context, args []string, streams IO, doctor, nodes bool) error {
	command := "server-info"
	if nodes {
		command = "nodes"
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	role := "ctl"
	if doctor {
		role = "doctor"
	}
	network := addNetworkFlags(flags, "herdr-mesh-"+role, role, "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	serverAddress := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	jsonOutput := flags.Bool("json", false, "write machine-readable JSON")
	timeout := flags.Duration("timeout", 20*time.Second, "overall operation timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *serverAddress == "" {
		return errors.New("-server is required")
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be greater than zero")
	}

	operationContext, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	options := control.Options{
		Diagnose:      doctor,
		JSON:          *jsonOutput,
		Output:        streams.Out,
		ServerAddress: *serverAddress,
		Transport:     network.config(),
	}
	if nodes {
		return control.Nodes(operationContext, options)
	}
	return control.ServerInfo(operationContext, options)
}

func addNetworkFlags(flags *flag.FlagSet, hostname, stateName, authKeyEnv, defaultTags string) *networkFlags {
	network := &networkFlags{}
	flags.StringVar(&network.authKeyEnv, "auth-key-env", authKeyEnv, "environment variable containing a Tailscale auth key")
	flags.BoolVar(&network.debug, "debug", false, "enable verbose tsnet logs")
	flags.StringVar(&network.hostname, "hostname", hostname, "Tailscale hostname for this embedded node")
	flags.StringVar(&network.stateDir, "state-dir", filepath.Join(configRoot(), stateName), "directory for local identity and tsnet state")
	flags.StringVar(&network.tags, "tags", defaultTags, "comma-separated Tailscale tags requested during enrollment")
	return network
}

func (flags *networkFlags) config() transport.Config {
	return transport.Config{
		AuthKeyEnv: flags.authKeyEnv,
		Debug:      flags.debug,
		Hostname:   flags.hostname,
		StateDir:   filepath.Join(flags.stateDir, "tsnet"),
		Tags:       splitList(flags.tags),
	}
}

func configRoot() string {
	root, err := os.UserConfigDir()
	if err != nil {
		return ".herdr-mesh"
	}
	return filepath.Join(root, "herdr-mesh")
}

var invalidHostnameCharacter = regexp.MustCompile(`[^a-z0-9-]+`)

func defaultNodeHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "herdr-mesh-node"
	}
	hostname = strings.ToLower(hostname)
	hostname = invalidHostnameCharacter.ReplaceAllString(hostname, "-")
	hostname = strings.Trim(hostname, "-")
	if hostname == "" {
		return "herdr-mesh-node"
	}
	return "herdr-mesh-node-" + hostname
}

func splitList(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, `Herdr distributed mesh

Usage:
  herdr-mesh server [flags]
  herdr-mesh node -server <host:port> [flags]
  herdr-mesh ctl server-info -server <host:port> [flags]
  herdr-mesh ctl nodes -server <host:port> [-json] [flags]
  herdr-mesh doctor -server <host:port> [flags]
  herdr-mesh dashboard -server <host:port> [-listen 127.0.0.1:8787] [flags]
  herdr-mesh version`)
}
