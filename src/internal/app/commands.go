package app

import (
	"context"
	"errors"
	"flag"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func runCommandQuery(ctx context.Context, command string, args []string, streams IO) error {
	query, err := parseCommandQuery(command, args, streams)
	if err != nil {
		return err
	}
	op, cancel := context.WithTimeout(ctx, query.timeout)
	defer cancel()
	if command == "ping" {
		return control.Ping(op, query.options, query.nodeID, query.key, query.ttl)
	}
	if command == "ensure-workspace" {
		return control.EnsureWorkspaceInSession(op, query.options, query.nodeID, query.key, &pb.WorkspaceEnsure{
			ProjectId: query.worktree.ProjectId, BindingRevision: query.worktree.BindingRevision,
			SessionName: query.worktree.SessionName, SessionIncarnation: query.worktree.SessionIncarnation,
		}, query.ttl)
	}
	if command == "create-worktree" {
		return control.CreateWorktree(op, query.options, query.nodeID, query.key, query.worktree, query.ttl)
	}
	return control.CommandStatus(op, query.options, query.id)
}

type commandQuery struct {
	options         control.Options
	timeout, ttl    time.Duration
	nodeID, key, id string
	worktree        *pb.WorktreeCreate
}

func parseCommandQuery(command string, args []string, streams IO) (*commandQuery, error) {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network, err := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	if err != nil {
		return nil, err
	}
	server := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	requiredServerTag := flags.String("required-server-tag", "tag:herdr-mesh-server", "Tailscale tag required on the actual coordinator connection")
	jsonOutput := flags.Bool("json", false, "write machine-readable command record")
	timeout := flags.Duration("timeout", 60*time.Second, "overall connection and operation timeout")
	var nodeID, key, id, projectID, revision string
	var name, branch, baseCommit string
	var sessionName, sessionIncarnation string
	ttl := protocol.DefaultCommandTTL
	if command == "create-worktree" {
		ttl = protocol.MaxCommandTTL
	}
	submit := command == "ping" || command == "ensure-workspace" || command == "create-worktree"
	if submit {
		flags.StringVar(&nodeID, "node", "", "target mesh node instance ID")
		flags.StringVar(&key, "idempotency-key", "", "actor-scoped request key (not an enrollment secret); reuse it for the same operation")
		flags.DurationVar(&ttl, "ttl", ttl, "execution deadline after admission; at most 30s")
		if command == "ensure-workspace" || command == "create-worktree" {
			flags.StringVar(&projectID, "project", "", "stable configured project ID, never a checkout path")
			flags.StringVar(&revision, "binding-revision", "", "optional exact binding revision override; otherwise derived by the coordinator")
			flags.StringVar(&sessionName, "session", "", "named Herdr session; omission selects only the configured default")
			flags.StringVar(&sessionIncarnation, "session-incarnation", "", "expected 64-hex Herdr session incarnation; preserve for exact retries")
		}
		if command == "create-worktree" {
			flags.StringVar(&name, "name", "", "portable lowercase destination name within the node's configured worktree root")
			flags.StringVar(&branch, "branch", "", "new portable lowercase single-component Git branch; defaults to name")
			flags.StringVar(&baseCommit, "base-commit", "", "optional full lowercase commit ID; omission resolves exact HEAD on the node")
		}
	} else {
		flags.StringVar(&id, "id", "", "command ID returned by a previous operation")
	}
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("unexpected positional arguments")
	}
	if *server == "" {
		return nil, errors.New("-server is required")
	}
	if *requiredServerTag == "" {
		return nil, errors.New("-required-server-tag must not be empty")
	}
	if *timeout <= 0 {
		return nil, errors.New("-timeout must be positive")
	}
	if err := protocol.ValidateSessionSelector(sessionName, sessionIncarnation, false); err != nil {
		return nil, err
	}
	if submit {
		if nodeID == "" {
			return nil, errors.New("-node is required")
		}
		if ttl <= 0 || ttl > protocol.MaxCommandTTL {
			return nil, errors.New("-ttl must be positive and at most 30s")
		}
		if key == "" {
			key = protocol.NewCommandID()
		}
		if (command == "ensure-workspace" || command == "create-worktree") && projectID == "" {
			return nil, errors.New("-project is required")
		}
		if command == "create-worktree" && name == "" {
			return nil, errors.New("-name is required")
		}
		if command == "create-worktree" && branch == "" {
			branch = name
		}
	} else if !protocol.ValidCommandID(id) {
		return nil, errors.New("-id requires a valid command ID")
	}
	options := control.Options{JSON: *jsonOutput, Output: streams.Out, ServerAddress: *server, RequiredServerTag: *requiredServerTag, Transport: network.config()}
	return &commandQuery{
		options: options, timeout: *timeout, ttl: ttl, nodeID: nodeID, key: key, id: id,
		worktree: &pb.WorktreeCreate{
			ProjectId: projectID, BindingRevision: revision, Name: name, Branch: branch, BaseCommit: baseCommit,
			SessionName: sessionName, SessionIncarnation: sessionIncarnation,
		},
	}, nil
}
