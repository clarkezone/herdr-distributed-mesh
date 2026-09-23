package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func readAgentPrompt(prompt, path string, streams IO) (string, error) {
	if path == "" {
		return prompt, nil
	}
	if prompt != "" {
		return "", errors.New("use -prompt or -prompt-file, not both")
	}
	input := streams.In
	if input == nil {
		input = os.Stdin
	}
	if path != "-" {
		file, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("open prompt file: %w", err)
		}
		defer file.Close()
		input = file
	}
	data, err := io.ReadAll(io.LimitReader(input, protocol.MaxAgentPromptBytes+1))
	if err != nil {
		return "", fmt.Errorf("read prompt: %w", err)
	}
	return string(data), nil
}

func runAgentLifecycle(ctx context.Context, verb string, args []string, streams IO) error {
	flags := flag.NewFlagSet("agent "+verb, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network, err := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	if err != nil {
		return err
	}
	server := flags.String("server", "", "coordinator MagicDNS name or tailnet IP with port")
	tag := flags.String("required-server-tag", "tag:herdr-mesh-server", "expected coordinator tag")
	node := flags.String("node", "", "target mesh node instance ID")
	key := flags.String("idempotency-key", "", "preserve this key and original request; unknown effects must not be retried with a new key")
	workspace := flags.String("workspace", "", "explicit existing workspace ID")
	provider := flags.String("provider", "", "supported provider; no arbitrary provider arguments")
	session := flags.String("session", "", "named Herdr session; omission selects only the node's configured default")
	incarnation := flags.String("session-incarnation", "", "expected native incarnation; required for stop even with configured default")
	jsonOutput := flags.Bool("json", false, "write typed command receipt including partial handles")
	ttl := flags.Duration("ttl", protocol.DefaultCommandTTL, "dispatch TTL, positive and at most 30s; independent of startup")
	timeout := flags.Duration("timeout", 2*time.Minute, "client connection/wait budget; expiration does not restart or cancel the provider")
	var project, revision, name, prompt, promptFile, pane, terminal, tab, providerSession string
	startup := time.Duration(protocol.DefaultAgentStartupTimeoutMs) * time.Millisecond
	if verb == "start" {
		flags.StringVar(&project, "project", "", "registered managed project ID")
		flags.StringVar(&revision, "binding-revision", "", "expected project revision; omitted resolves current generation")
		flags.StringVar(&name, "name", "", "name for the dedicated no-focus agent pane")
		flags.DurationVar(&startup, "startup-timeout", startup, "bounded readiness budget, 3001ms..5m")
		flags.StringVar(&prompt, "prompt", "", "optional initial prompt; acknowledgement is not task success")
		flags.StringVar(&promptFile, "prompt-file", "", "client-local UTF-8 prompt file or - for stdin")
	} else {
		flags.StringVar(&pane, "agent", "", "exact agent pane from the preserved handle")
		flags.StringVar(&terminal, "terminal", "", "exact terminal from the preserved handle")
		flags.StringVar(&tab, "tab", "", "exact tab from the preserved handle")
		flags.StringVar(&providerSession, "agent-session", "", "optional expected provider session ID")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *server == "" || *node == "" || *workspace == "" || *provider == "" ||
		*tag == "" || *timeout <= 0 || *ttl <= 0 || *ttl > protocol.MaxCommandTTL {
		return errors.New("supply -server, -node, -workspace, -provider, a nonempty -required-server-tag, positive -timeout, and -ttl greater than 0 and at most 30s; positional arguments are not accepted; use herdr-mesh ctl agent start -help or herdr-mesh ctl agent stop -help")
	}
	if *key != "" && !protocol.ValidIdempotencyKey(*key) {
		return errors.New("invalid idempotency key (-idempotency-key); use 1..128 ASCII letters, digits, underscores, colons, or hyphens; preserve the original key for retries")
	}
	op, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	options := control.Options{Output: streams.Out, JSON: *jsonOutput, ServerAddress: *server,
		RequiredServerTag: *tag, Transport: network.config()}
	if verb == "stop" {
		stop := &pb.AgentStop{WorkspaceId: *workspace, TabId: tab, Provider: *provider,
			Target: &pb.AgentTarget{PaneId: pane, TerminalId: terminal, AgentSessionId: providerSession,
				SessionName: *session, SessionIncarnation: *incarnation}}
		if err := protocol.ValidateAgentStop(stop); err != nil {
			return err
		}
		return control.StopAgent(op, options, *node, *key, stop, *ttl)
	}
	if startup < 3001*time.Millisecond || startup > 5*time.Minute || startup%time.Millisecond != 0 {
		return errors.New("-startup-timeout must be 3001ms..5m in whole milliseconds")
	}
	prompt, err = readAgentPrompt(prompt, promptFile, streams)
	if err != nil {
		return err
	}
	return control.StartAgent(op, options, *node, *key, &pb.AgentStart{ProjectId: project, BindingRevision: revision,
		WorkspaceId: *workspace, Name: name, Provider: *provider, SessionName: *session, SessionIncarnation: *incarnation,
		StartupTimeoutMs: uint32(startup / time.Millisecond), InitialPrompt: prompt}, *ttl)
}
