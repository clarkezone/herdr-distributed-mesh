package control

import (
	"context"
	"fmt"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Nodes(ctx context.Context, options Options) error {
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, _ transport.SelfStatus) error {
		list, err := retryUnavailable(ctx, func() (*agentflowv1.NodeList, error) {
			return client.ListNodes(ctx, &emptypb.Empty{})
		})
		if err != nil {
			return fmt.Errorf("list nodes: %w", err)
		}
		return writeNodes(options, list)
	})
}

func writeNodes(options Options, list *agentflowv1.NodeList) error {
	if options.JSON {
		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(list)
		if err != nil {
			return fmt.Errorf("encode nodes: %w", err)
		}
		_, err = fmt.Fprintln(options.Output, string(data))
		return err
	}
	if len(list.Nodes) == 0 {
		_, err := fmt.Fprintln(options.Output, "No nodes registered. Check that an execution node is running and connected to this coordinator.")
		return err
	}
	for _, node := range list.Nodes {
		state := node.GetHerdr()
		var view humanView
		view.field("Node", node.Hostname)
		view.field("Node ID", node.InstanceId)
		connection := "connected to coordinator"
		if !node.Connected {
			connection = "disconnected; check that the node is running and has tailnet access"
		}
		view.field("Connection", connection)
		herdr := readable(state.GetStatus())
		if herdr == "" {
			herdr = "not reported"
		}
		view.field("Default-session Herdr", herdr)
		view.freshness(node.Stale || !node.Connected)
		view.field("Default-session inventory", fmt.Sprintf("workspaces: %d; tabs: %d; panes: %d; agents: %d",
			len(state.GetWorkspaces()), len(state.GetTabs()), len(state.GetPanes()), len(state.GetAgents())))
		view.field("Herdr issue", HumanDetail(state.GetErrorCode()))
		view.field("Named sessions", fmt.Sprint(len(node.Sessions)))
		manager := "ready"
		if !node.SessionsReady {
			manager = "unavailable; check the node's managed-session configuration"
		}
		view.field("Session manager", manager)
		view.field("Session manager issue", HumanDetail(node.SessionsErrorCode))
		if !node.Connected || node.Stale || state.GetStatus() != "ready" || !node.SessionsReady {
			view.field("Next", "inspect this node's sessions before starting work; connection alone does not confirm execution readiness")
		}
		if _, err := fmt.Fprintln(options.Output, view.String()); err != nil {
			return err
		}
	}
	return nil
}
