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
		_, err := fmt.Fprintln(options.Output, "no nodes registered")
		return err
	}
	for _, node := range list.Nodes {
		state := node.GetHerdr()
		if _, err := fmt.Fprintf(options.Output, "node=%s connected=%t herdr=%s stale=%t workspaces=%d tabs=%d panes=%d agents=%d error=%s\n",
			node.InstanceId, node.Connected, state.GetStatus(), node.Stale,
			len(state.GetWorkspaces()), len(state.GetTabs()), len(state.GetPanes()), len(state.GetAgents()), state.GetErrorCode()); err != nil {
			return err
		}
	}
	return nil
}
