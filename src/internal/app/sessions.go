package app

import (
	"context"
	"errors"
	"flag"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/types/known/durationpb"
)

type sessionQuery struct {
	options control.Options
	timeout time.Duration
	nodeID  string
	request *pb.SubmitCommandRequest
}

func runSessionQuery(ctx context.Context, command string, args []string, streams IO) error {
	query, err := parseSessionQuery(command, args, streams)
	if err != nil {
		return err
	}
	op, cancel := context.WithTimeout(ctx, query.timeout)
	defer cancel()
	if query.request == nil {
		return control.Sessions(op, query.options, query.nodeID)
	}
	return control.EnsureSession(op, query.options, query.nodeID, query.request.IdempotencyKey,
		query.request.SessionEnsure.Name, query.request.Ttl.AsDuration())
}

func parseSessionQuery(command string, args []string, streams IO) (*sessionQuery, error) {
	ensure := command == "session"
	if ensure {
		if len(args) == 0 || args[0] != "ensure" {
			return nil, errors.New("session requires ensure; use herdr-mesh ctl session ensure -help for flags, or ctl sessions to list sessions")
		}
		args = args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network, err := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	if err != nil {
		return nil, err
	}
	server := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	tag := flags.String("required-server-tag", "tag:herdr-mesh-server", "required tag on the actual coordinator connection")
	nodeID := flags.String("node", "", "target mesh node instance ID")
	jsonOutput := flags.Bool("json", false, "write a typed session list or durable command record")
	timeout := flags.Duration("timeout", time.Minute, "overall connection and operation timeout")
	var name, key string
	ttl := protocol.DefaultCommandTTL
	if ensure {
		flags.StringVar(&name, "name", "", "portable Herdr session name; preserve exact letter case")
		flags.StringVar(&key, "key", "", "actor-scoped request key; preserve name, node, and TTL for exact retries")
		flags.StringVar(&key, "idempotency-key", "", "alias for -key")
		flags.DurationVar(&ttl, "ttl", ttl, "startup deadline after admission; at most 30s")
	}
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("session commands accept flags, not positional arguments; add -help to see the required flags")
	}
	if strings.TrimSpace(*server) == "" || strings.TrimSpace(*nodeID) == "" {
		return nil, errors.New("-server and -node are required")
	}
	if strings.TrimSpace(*tag) == "" || *timeout <= 0 {
		return nil, errors.New("expected server tag (-required-server-tag) must not be empty and -timeout must be positive (for example 1m)")
	}
	query := &sessionQuery{nodeID: *nodeID, timeout: *timeout, options: control.Options{
		Output: streams.Out, RetryOutput: streams.Err, JSON: *jsonOutput, ServerAddress: *server, RequiredServerTag: *tag, Transport: network.config(),
	}}
	if ensure {
		if ttl <= 0 || ttl > protocol.MaxCommandTTL {
			return nil, errors.New("-ttl must be positive and at most 30s")
		}
		if key == "" {
			key = protocol.NewCommandID()
		}
		var err error
		query.request, err = protocol.NormalizeCommandRequest(&pb.SubmitCommandRequest{
			NodeInstanceId: *nodeID, IdempotencyKey: key, CommandType: protocol.SessionEnsureCommandType,
			SessionEnsure: &pb.SessionEnsure{Name: name}, Ttl: durationpb.New(ttl),
		})
		if err != nil {
			return nil, err
		}
	}
	return query, nil
}
