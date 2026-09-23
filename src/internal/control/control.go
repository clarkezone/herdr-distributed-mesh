package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Options struct {
	FleetClient agentflowv1.FleetClient
	Diagnose    bool
	JSON        bool
	Output      io.Writer
	// RetryOutput optionally records CLI recovery identity before dispatch.
	// Library/MCP callers leave it nil; command JSON is written only to Output.
	RetryOutput       io.Writer
	ServerAddress     string
	RequiredServerTag string
	Transport         transport.Config
}

// WithFleet owns one transport connection for a long-lived controller such as
// MCP. Its operations reuse the ordinary CLI services and validation paths.
func WithFleet(ctx context.Context, options Options, use func(agentflowv1.FleetClient) error) error {
	if use == nil {
		return errors.New("fleet callback is required")
	}
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, _ transport.SelfStatus) error {
		return use(client)
	})
}

func ServerInfo(ctx context.Context, options Options) error {
	return withFleet(ctx, options, func(client agentflowv1.FleetClient, localStatus transport.SelfStatus) error {
		return writeServerInfo(ctx, options, client, localStatus)
	})
}

func withFleet(ctx context.Context, options Options, query func(agentflowv1.FleetClient, transport.SelfStatus) error) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.FleetClient != nil {
		if options.Diagnose {
			return errors.New("transport diagnostics require an owned transport")
		}
		return query(options.FleetClient, transport.SelfStatus{})
	}
	guard, err := state.PrepareRoleState(ctx, options.Transport.RoleStateDir, "client")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, guard.Close()) }()
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, network.Close()) }()
	if err := guard.Activate(); err != nil {
		return err
	}

	localStatus := network.SelfStatus()
	if options.Diagnose {
		if err := localStatus.Validate(options.Transport.Tags, time.Now()); err != nil {
			return fmt.Errorf("validate local tsnet identity: %w", err)
		}
	}
	var connection *grpc.ClientConn
	if options.RequiredServerTag != "" {
		connection, err = network.DialGRPCWithPeerTag(options.ServerAddress, options.RequiredServerTag)
	} else {
		connection, err = network.DialGRPC(options.ServerAddress)
	}
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
	var view humanView
	view.field("Coordinator", "reachable; compatible with this client")
	view.field("Address", options.ServerAddress)
	view.field("Version", info.ImplementationVersion)
	view.field("Coordinator ID", info.InstanceId)
	view.field("Execution readiness", "not checked here; inspect nodes and sessions before starting work")
	if options.Diagnose {
		view.field("Protocol", fmt.Sprintf("compatible; using version %d (coordinator supports %d-%d)",
			selectedProtocol, info.Protocol.GetMinimum(), info.Protocol.GetMaximum()))
		view.field("Capability codes", strings.Join(info.Capabilities, ", "))
		view.field("Local transport", "embedded Tailscale client")
		view.field("Local Tailscale ID", localStatus.StableID)
		view.field("Local DNS name", localStatus.DNSName)
		view.field("Local IP addresses", strings.Join(localStatus.IPs, ", "))
		view.field("Requested role tags", strings.Join(options.Transport.Tags, ", "))
		view.field("Assigned role tags", strings.Join(localStatus.Tags, ", "))
		keyExpiry := "none reported"
		if localStatus.KeyExpiry != nil {
			keyExpiry = localStatus.KeyExpiry.Format(time.RFC3339)
		}
		view.field("Enrollment key expiry", keyExpiry)
		view.field("Local transport state directory", localStatus.StateDir)
		health := "no issues reported"
		if len(localStatus.Health) > 0 {
			health = strings.Join(localStatus.Health, "; ")
		}
		view.field("Local transport health", health)
	}
	_, err = fmt.Fprint(options.Output, view.String())
	return err
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
