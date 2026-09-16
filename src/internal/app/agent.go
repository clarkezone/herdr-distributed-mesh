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
		return errors.New("agent requires get, read, wait, prompt, input, or interrupt")
	}
	verb := args[0]
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
	jsonOutput := flags.Bool("json", false, "write one typed JSON result")
	timeout := flags.Duration("timeout", time.Minute, "overall connection and operation timeout")
	var lines uint
	var until, prompt, promptFile, key string
	var keys agentKeys
	waitTimeout := 30 * time.Second
	ttl := protocol.DefaultCommandTTL
	if verb == "read" {
		flags.UintVar(&lines, "lines", 100, "recent output lines, at most 1000")
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
	target := &pb.AgentTarget{PaneId: *paneID, TerminalId: *terminalID, AgentSessionId: *sessionID}
	if err := protocol.ValidateAgentTarget(target, false); err != nil {
		return err
	}
	if verb == "prompt" {
		if promptFile != "" && prompt != "" {
			return errors.New("use -prompt or -prompt-file, not both")
		}
		if promptFile != "" {
			input := streams.In
			if input == nil {
				input = os.Stdin
			}
			if promptFile != "-" {
				file, err := os.Open(promptFile)
				if err != nil {
					return fmt.Errorf("open prompt file: %w", err)
				}
				defer file.Close()
				input = file
			}
			data, err := io.ReadAll(io.LimitReader(input, protocol.MaxAgentPromptBytes+1))
			if err != nil {
				return fmt.Errorf("read prompt: %w", err)
			}
			prompt = string(data)
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
	return control.Agent(op, control.Options{Output: streams.Out, JSON: *jsonOutput, ServerAddress: *server, RequiredServerTag: *tag, Transport: network.config()}, query, command, key, ttl)
}
