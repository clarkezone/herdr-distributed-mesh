package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Options struct {
	Diagnose      bool
	JSON          bool
	Output        io.Writer
	ServerAddress string
	Transport     transport.Config
}

func ServerInfo(ctx context.Context, options Options) error {
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, localStatus transport.SelfStatus) error {
		return writeServerInfo(ctx, options, client, localStatus)
	})
}

func withFleet(ctx context.Context, options Options, query func(agentflowv1.FleetClient, transport.SelfStatus) error) error {
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer network.Close()

	localStatus := network.SelfStatus()
	if options.Diagnose {
		if err := localStatus.Validate(options.Transport.Tags, time.Now()); err != nil {
			return fmt.Errorf("validate local tsnet identity: %w", err)
		}
	}
	connection, err := network.DialGRPC(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()

	return query(agentflowv1.NewFleetClient(connection), localStatus)
}

func writeServerInfo(ctx context.Context, options Options, client agentflowv1.FleetClient, localStatus transport.SelfStatus) error {
	info, err := getServerInfo(ctx, client)
	if err != nil {
		return fmt.Errorf("get server info: %w", err)
	}
	selectedProtocol, err := protocol.Negotiate(info.Protocol)
	if err != nil {
		return fmt.Errorf("server protocol is incompatible: %w", err)
	}
	if options.JSON {
		result := map[string]any{"server": map[string]any{
			"capabilities":           info.Capabilities,
			"implementation_version": info.ImplementationVersion,
			"instance_id":            info.InstanceId,
			"protocol_maximum":       info.Protocol.GetMaximum(),
			"protocol_minimum":       info.Protocol.GetMinimum(),
			"protocol_selected":      selectedProtocol,
		}}
		if options.Diagnose {
			result["local_tsnet"] = localStatus
		}
		return json.NewEncoder(options.Output).Encode(result)
	}
	if options.Diagnose {
		keyExpiry := "none"
		if localStatus.KeyExpiry != nil {
			keyExpiry = localStatus.KeyExpiry.Format(time.RFC3339)
		}
		fmt.Fprintf(
			options.Output,
			"local tsnet stable_id=%s dns=%s ips=%v requested_tags=%v assigned_tags=%v key_expiry=%s state_dir=%s health=%v\n",
			localStatus.StableID,
			localStatus.DNSName,
			localStatus.IPs,
			options.Transport.Tags,
			localStatus.Tags,
			keyExpiry,
			localStatus.StateDir,
			localStatus.Health,
		)
	}
	fmt.Fprintf(
		options.Output,
		"server instance=%s version=%s protocol=%d-%d selected=%d capabilities=%v\n",
		info.InstanceId,
		info.ImplementationVersion,
		info.Protocol.GetMinimum(),
		info.Protocol.GetMaximum(),
		selectedProtocol,
		info.Capabilities,
	)
	return nil
}

type serverInfoClient interface {
	GetServerInfo(context.Context, *emptypb.Empty, ...grpc.CallOption) (*agentflowv1.ServerInfo, error)
}

func getServerInfo(ctx context.Context, client serverInfoClient) (*agentflowv1.ServerInfo, error) {
	return retryUnavailable(ctx, func() (*agentflowv1.ServerInfo, error) {
		return client.GetServerInfo(ctx, &emptypb.Empty{})
	})
}

func retryUnavailable[T any](ctx context.Context, call func() (*T, error)) (*T, error) {
	var lastErr error
	for {
		info, err := call()
		if err == nil {
			return info, nil
		}
		if status.Code(err) != codes.Unavailable {
			return nil, err
		}
		lastErr = err
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, lastErr
		case <-timer.C:
		}
	}
}
