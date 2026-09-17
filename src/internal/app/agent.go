package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

type agentKeys []string

func (v *agentKeys) String() string { return fmt.Sprint([]string(*v)) }
func (v *agentKeys) Set(value string) error {
	if len(*v) >= 8 || !protocol.ValidAgentKey(value) {
		return errors.New("use at most eight supported -key values")
	}
	*v = append(*v, value)
	return nil
}

func runAgent(ctx context.Context, args []string, streams IO) error {
	if len(args) == 0 {
		return errors.New("agent requires get, read, wait, prompt, input, interrupt, start, or stop")
	}
	verb := args[0]
	if verb == "start" || verb == "stop" {
		return runAgentLifecycle(ctx, verb, args[1:], streams)
	}
	kind := pb.AgentQueryKind_AGENT_QUERY_KIND_GET
	action := pb.AgentControlAction_AGENT_CONTROL_ACTION_UNSPECIFIED
	switch verb {
	case "get":
	case "read":
		kind = pb.AgentQueryKind_AGENT_QUERY_KIND_READ
	case "wait":
		kind = pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT
	case "prompt":
		action = pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT
	case "input":
		action = pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT
	case "interrupt":
		action = pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT
	default:
		return fmt.Errorf("unknown agent operation %q", verb)
	}
	flags := flag.NewFlagSet("agent "+verb, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	server := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	tag := flags.String("required-server-tag", "tag:herdr-mesh-server", "required tag on the actual coordinator connection")
	nodeID := flags.String("node", "", "target mesh node instance ID")
	paneID := flags.String("agent", "", "agent pane ID, for example w1:p1")
	terminalID := flags.String("terminal", "", "expected terminal ID; optional for discovery, preserve for exact retries")
	sessionID := flags.String("agent-session", "", "optional expected provider session ID; not a named Herdr session")
	sessionName := flags.String("session", "", "named Herdr session; omission selects only the configured default")
	sessionIncarnation := flags.String("session-incarnation", "", "expected 64-hex Herdr session incarnation; preserve for exact retries")
	jsonOutput := flags.Bool("json", false, "write one typed JSON result")
	timeout := flags.Duration("timeout", time.Minute, "overall connection and operation timeout")
	var lines uint
	var follow bool
	pollInterval := time.Second
	var until, prompt, promptFile, key string
	var keys agentKeys
	waitTimeout := 30 * time.Second
	ttl := protocol.DefaultCommandTTL
	if verb == "read" {
		flags.UintVar(&lines, "lines", 100, "recent output lines, at most 1000")
		flags.BoolVar(&follow, "follow", false, "poll bounded snapshots until timeout; JSON output is JSONL with explicit gaps")
		flags.DurationVar(&pollInterval, "poll-interval", time.Second, "follow polling interval, 250ms..30s")
	}
	if verb == "wait" {
		flags.StringVar(&until, "until", "", "comma-separated observed states; default idle,done,blocked")
		flags.DurationVar(&waitTimeout, "wait-timeout", 30*time.Second, "observation timeout, at most five minutes; does not cancel the task")
	}
	if action != pb.AgentControlAction_AGENT_CONTROL_ACTION_UNSPECIFIED {
		flags.StringVar(&key, "idempotency-key", "", "request key; reuse with the original target to inspect/retry the same input")
		flags.DurationVar(&ttl, "ttl", ttl, "input-delivery deadline, not task runtime; at most 30s")
	}
	if verb == "prompt" {
		flags.StringVar(&prompt, "prompt", "", "prompt text; alternatively use -prompt-file")
		flags.StringVar(&promptFile, "prompt-file", "", "client-local UTF-8 prompt file, or - for stdin")
	}
	if verb == "input" {
		flags.Var(&keys, "key", "explicit key; repeat up to eight times (e.g. down, enter)")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *server == "" || *nodeID == "" || *paneID == "" {
		return errors.New("-server, -node and -agent are required")
	}
	if *tag == "" || *timeout <= 0 {
		return errors.New("expected server tag and positive timeout are required")
	}
	if ttl <= 0 || ttl > protocol.MaxCommandTTL {
		return errors.New("-ttl must be positive and at most 30s")
	}
	if waitTimeout < time.Millisecond || waitTimeout > protocol.MaxAgentQueryTimeout {
		return errors.New("-wait-timeout must be 1ms..5m")
	}
	if verb == "read" && (lines == 0 || lines > 1000) {
		return errors.New("-lines must be 1..1000")
	}
	if verb == "read" && (pollInterval < 250*time.Millisecond || pollInterval > 30*time.Second) {
		return errors.New("-poll-interval must be 250ms..30s")
	}
	target := &pb.AgentTarget{PaneId: *paneID, TerminalId: *terminalID, AgentSessionId: *sessionID, SessionName: *sessionName, SessionIncarnation: *sessionIncarnation}
	if err := protocol.ValidateAgentTarget(target, false); err != nil {
		return err
	}
	if verb == "prompt" {
		var err error
		prompt, err = readAgentPrompt(prompt, promptFile, streams)
		if err != nil {
			return err
		}
	}
	var command *pb.AgentControl
	if action != pb.AgentControlAction_AGENT_CONTROL_ACTION_UNSPECIFIED {
		command = &pb.AgentControl{Action: action, Target: target, Text: prompt, Keys: keys}
		if err := protocol.ValidateAgentControlInput(action, prompt, keys); err != nil {
			return err
		}
	}
	query := &pb.AgentQueryRequest{NodeInstanceId: *nodeID, Kind: kind, Target: target, Lines: uint32(lines),
		TimeoutMs: uint32(waitTimeout / time.Millisecond)}
	if verb == "wait" {
		query.Until = splitList(until)
	}
	op, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	options := control.Options{Output: streams.Out, JSON: *jsonOutput, ServerAddress: *server, RequiredServerTag: *tag, Transport: network.config()}
	if follow {
		return control.FollowAgent(op, options, query, pollInterval)
	}
	return control.Agent(op, options, query, command, key, ttl)
}
