package app

import (
	"context"
	"errors"
	"flag"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/control"
)

type projectQuery struct {
	command string
	options control.Options
	timeout time.Duration
	request *pb.RegisterProjectRequest
}

func runProjectQuery(ctx context.Context, command string, args []string, streams IO) error {
	query, err := parseProjectQuery(command, args, streams)
	if err != nil {
		return err
	}
	op, cancel := context.WithTimeout(ctx, query.timeout)
	defer cancel()
	switch query.command {
	case "register":
		return control.RegisterProject(op, query.options, query.request)
	case "get":
		return control.GetProject(op, query.options, query.request.NodeInstanceId, query.request.ProjectId)
	default:
		return control.Projects(op, query.options, query.request.NodeInstanceId)
	}
}

func parseProjectQuery(command string, args []string, streams IO) (*projectQuery, error) {
	if command == "project" {
		if len(args) == 0 || (args[0] != "register" && args[0] != "get") {
			return nil, errors.New("ctl project requires register or get")
		}
		command, args = args[0], args[1:]
	}
	flagName := "project " + command
	if command == "projects" {
		flagName = command
	}
	flags := flag.NewFlagSet(flagName, flag.ContinueOnError)
	flags.SetOutput(streams.Err)
	network := addNetworkFlags(flags, "herdr-mesh-ctl", "ctl", "TS_AUTHKEY_CLIENT", "tag:herdr-mesh-client")
	server := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	serverTag := flags.String("required-server-tag", "tag:herdr-mesh-server", "Tailscale tag required on the actual coordinator connection")
	jsonOutput := flags.Bool("json", false, "write project configuration JSON, including node-local paths")
	timeout := flags.Duration("timeout", 20*time.Second, "overall operation timeout")
	request := &pb.RegisterProjectRequest{}
	flags.StringVar(&request.NodeInstanceId, "node", "", "mesh node instance ID; optional filter for projects")
	if command != "projects" {
		flags.StringVar(&request.ProjectId, "project", "", "stable logical project ID")
	}
	if command == "register" {
		flags.StringVar(&request.CheckoutPath, "path", "", "existing absolute Git checkout path on the target node, not this client")
		flags.StringVar(&request.WorktreeRoot, "worktree-root", "", "optional existing absolute worktree output root on the target node")
	}
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("unexpected positional arguments")
	}
	if strings.TrimSpace(*server) == "" {
		return nil, errors.New("-server is required")
	}
	if strings.TrimSpace(*serverTag) == "" {
		return nil, errors.New("-required-server-tag must not be empty")
	}
	if *timeout <= 0 {
		return nil, errors.New("-timeout must be positive")
	}
	if command != "projects" && (strings.TrimSpace(request.NodeInstanceId) == "" || strings.TrimSpace(request.ProjectId) == "") {
		return nil, errors.New("-node and -project are required")
	}
	if command == "register" && strings.TrimSpace(request.CheckoutPath) == "" {
		return nil, errors.New("-path is required")
	}
	return &projectQuery{
		command: command, timeout: *timeout, request: request,
		options: control.Options{JSON: *jsonOutput, Output: streams.Out, ServerAddress: *server, RequiredServerTag: *serverTag, Transport: network.config()},
	}, nil
}
