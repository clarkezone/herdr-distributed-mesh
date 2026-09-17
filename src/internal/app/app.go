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
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/server"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
)

const defaultPort = "50052"

type IO struct {
	In  io.Reader
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
	case "mcp":
		return runMCP(ctx, args[1:], streams)
	case "maintenance":
		return RunMaintenance(ctx, args[1:], streams)
	case "setup":
		return RunSetup(ctx, args[1:], streams)
	case "bootstrap":
		return RunBootstrap(ctx, args[1:], streams)
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
	dashboardListen := flags.String("dashboard-listen", "", "optional tsnet-only dashboard port, such as :8787")
	dashboardOrigin := flags.String("dashboard-origin", "", "exact HTTP(S) dashboard origin; HTTPS uses the full coordinator Tailscale DNS name")
	requiredClientTag := flags.String("required-client-tag", "tag:herdr-mesh-client", "Tailscale tag required for fleet read requests")
	requiredCommandTag := flags.String("required-command-tag", "tag:herdr-mesh-client", "Tailscale tag required for commands, history, and project configuration")
	requiredNodeTag := flags.String("required-node-tag", "tag:herdr-mesh-node", "Tailscale tag required for node streams")
	flags.String("workspace-policy", "", "deprecated: remove this flag and use ctl project register")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*requiredCommandTag) == "" {
		return errors.New("-required-command-tag must not be empty")
	}
	legacyPolicyFlag := false
	flags.Visit(func(value *flag.Flag) {
		legacyPolicyFlag = legacyPolicyFlag || value.Name == "workspace-policy"
	})
	if legacyPolicyFlag {
		return errors.New("server -workspace-policy is deprecated; remove it and use ctl project register (legacy node files may be supplied to node -workspace-policy for migration)")
	}

	return server.Run(ctx, server.Options{
		BindingPath:            filepath.Join(network.stateDir, "node-bindings.jsonl"),
		DatabasePath:           filepath.Join(network.stateDir, "coordinator", "mesh.db"),
		ListenAddress:          *listen,
		RequiredClientTag:      *requiredClientTag,
		RequiredCommandTag:     *requiredCommandTag,
		RequiredNodeTag:        *requiredNodeTag,
		Transport:              network.config(),
		DashboardListenAddress: *dashboardListen,
		DashboardOrigin:        *dashboardOrigin,
		Output:                 streams.Out,
	})
}

func runNode(ctx context.Context, args []string, streams IO) error {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, defaultNodeHostname(), "node", "TS_AUTHKEY_NODE", "tag:herdr-mesh-node")
	serverAddress := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	heartbeat := flags.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	herdrSocket := flags.String("herdr-socket", "", "local Herdr socket marker path; enables observation, managed projects, and authenticated existing-agent control")
	herdrExecutable := flags.String("herdr-executable", "", "explicit Herdr executable enabling named headless sessions; no default socket is required")
	enableProbes := flags.Bool("enable-probes", false, "opt in to durably journaled read-only node ping commands; no Herdr mutations")
	workspacePolicyPath := flags.String("workspace-policy", "", "deprecated: legacy local project file for one-time central adoption; use ctl project register for changes")
	requiredServerTag := flags.String("required-server-tag", "tag:herdr-mesh-server", "Tailscale tag required on the coordinator when commands are enabled")
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
	journalPath := ""
	if *workspacePolicyPath != "" && *herdrSocket == "" && *herdrExecutable == "" {
		return errors.New("-herdr-socket or -herdr-executable is required with workspace policy")
	}
	if *enableProbes || *workspacePolicyPath != "" || *herdrSocket != "" || *herdrExecutable != "" {
		if strings.TrimSpace(*requiredServerTag) == "" {
			return errors.New("-required-server-tag must not be empty when commands are enabled")
		}
		journalPath = filepath.Join(network.stateDir, "commands", "journal.db")
	}

	if *workspacePolicyPath != "" {
		fmt.Fprintln(streams.Err, "node -workspace-policy is deprecated migration input, not ongoing authority; inspect adoption with ctl projects and use ctl project register for changes")
	}
	return node.Run(ctx, node.Options{
		HeartbeatInterval:  *heartbeat,
		HerdrSocket:        *herdrSocket,
		HerdrExecutable:    *herdrExecutable,
		CommandJournalPath: journalPath,
		RequiredServerTag:  *requiredServerTag,
		ReconnectDelay:     *reconnectDelay,
		ReconnectMaximum:   *reconnectMaximum,
		ServerAddress:      *serverAddress,
		Transport:          network.config(),
		LegacyPolicyPath:   *workspacePolicyPath,
		EnableAgentControl: *herdrSocket != "" || *herdrExecutable != "",
	})
}

func runControl(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 {
		return errors.New("ctl requires a command; currently supported: server-info, nodes, agents, sessions, session, projects, project, ping, ensure-workspace, create-worktree, command, agent")
	}
	switch args[0] {
	case "server-info":
		return runFleetQuery(ctx, args[1:], streams, false, false)
	case "nodes":
		return runFleetQuery(ctx, args[1:], streams, false, true)
	case "sessions", "session":
		return runSessionQuery(ctx, args[0], args[1:], streams)
	case "ping", "ensure-workspace", "create-worktree", "command":
		return runCommandQuery(ctx, args[0], args[1:], streams)
	case "agent":
		return runAgent(ctx, args[1:], streams)
	case "agents":
		return runAgentInventory(ctx, args[1:], streams)
	case "project", "projects":
		return runProjectQuery(ctx, args[0], args[1:], streams)
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
	watch := false
	if nodes {
		flags.BoolVar(&watch, "watch", false, "subscribe to replacement fleet snapshots; reconnects report gaps and begin with a full snapshot")
	}
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
		if watch {
			return control.WatchNodes(operationContext, options)
		}
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
		AuthKeyEnv:   flags.authKeyEnv,
		Debug:        flags.debug,
		Hostname:     flags.hostname,
		RoleStateDir: flags.stateDir,
		StateDir:     filepath.Join(flags.stateDir, "tsnet"),
		Tags:         splitList(flags.tags),
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
  herdr-mesh server [-dashboard-listen :8787 -dashboard-origin <origin>] [flags]
  herdr-mesh node -server <host:port> [-herdr-socket <marker>] [-herdr-executable <executable>] [flags]
  herdr-mesh ctl server-info -server <host:port> [flags]
  herdr-mesh ctl nodes -server <host:port> [-watch] [-json] [flags]
  herdr-mesh ctl agents -server <host:port> [-node <id>] [-project <id>] [-workspace <id>] [-provider <id>] [-ready <any|true|false|unknown>] [-json] [flags]
  herdr-mesh ctl sessions -server <host:port> -node <instance-id> [-json] [flags]
  herdr-mesh ctl session ensure -server <host:port> -node <instance-id> -name <name> [-key <retry-key>] [-ttl <duration>] [flags]
  herdr-mesh ctl ping -server <host:port> -node <instance-id> [-idempotency-key <retry-key>] [flags]
  herdr-mesh ctl project register -server <host:port> -node <instance-id> -project <id> -path <node-checkout> [-worktree-root <node-root>] [flags]
  herdr-mesh ctl projects -server <host:port> [-node <instance-id>] [-json] [flags]
  herdr-mesh ctl project get -server <host:port> -node <instance-id> -project <id> [-json] [flags]
  herdr-mesh ctl ensure-workspace -server <host:port> -node <instance-id> -project <id> [-binding-revision <revision>] [-session <name>] [-session-incarnation <64hex>] [flags]
  herdr-mesh ctl create-worktree -server <host:port> -node <instance-id> -project <id> -name <name> [-binding-revision <revision>] [-branch <branch>] [-base-commit <sha>] [-session <name>] [-session-incarnation <64hex>] [flags]
  herdr-mesh ctl command -server <host:port> -id <command-id> [flags]
  herdr-mesh ctl agent <get|read|wait|prompt|input|interrupt> -server <host:port> -node <instance-id> -agent <pane-id> [-session <name>] [-session-incarnation <64hex>] [flags]
  herdr-mesh ctl agent start -server <host:port> -node <instance-id> -project <id> -workspace <id> -provider <provider> -name <name> [flags]
  herdr-mesh ctl agent stop -server <host:port> -node <instance-id> -agent <pane-id> -terminal <id> -workspace <id> -tab <id> -provider <provider> -session-incarnation <64hex> [flags]
  herdr-mesh ctl agent read -server <host:port> -node <instance-id> -agent <pane-id> [-session <name>] [-session-incarnation <64hex>] -follow [-poll-interval <duration>] [flags]
  herdr-mesh doctor -server <host:port> [flags]
  herdr-mesh mcp -server <host:port> [flags]
  herdr-mesh maintenance <inspect|backup|verify-backup> -role <server|node> -state-dir <path> [flags]
  herdr-mesh setup tailnet -tailnet <name> [-apply] [flags]
  herdr-mesh bootstrap [flags]
  herdr-mesh dashboard -server <host:port> [-listen 127.0.0.1:8787] [flags]
  herdr-mesh version`)
}
