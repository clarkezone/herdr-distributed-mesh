package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var capabilities = []string{"node.heartbeat.v1"}

type Options struct {
	HeartbeatInterval time.Duration
	InstanceID        string
	ReconnectDelay    time.Duration
	ServerAddress     string
	Transport         transport.Config
}

func Run(ctx context.Context, options Options) error {
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer network.Close()

	connection, err := network.DialGRPC(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()

	client := agentflowv1.NewNodeControlClient(connection)
	for {
		err := runSession(ctx, client, options)
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("node session ended: %v; reconnecting in %s", err, options.ReconnectDelay)
		timer := time.NewTimer(options.ReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func runSession(ctx context.Context, client agentflowv1.NodeControlClient, options Options) error {
	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Connect(sessionContext)
	if err != nil {
		return fmt.Errorf("open node control stream: %w", err)
	}
	if err := stream.Send(&agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_Hello{
			Hello: &agentflowv1.Hello{
				Protocol:              protocol.SupportedRange(),
				ImplementationVersion: buildinfo.Version,
				InstanceId:            options.InstanceID,
				Role:                  agentflowv1.Role_ROLE_NODE,
				Capabilities:          slices.Clone(capabilities),
			},
		},
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive hello acknowledgement: %w", err)
	}
	ack := response.GetHelloAck()
	if ack == nil {
		return errors.New("server did not respond with hello acknowledgement")
	}
	if ack.SelectedProtocol < protocol.MinimumVersion || ack.SelectedProtocol > protocol.MaximumVersion {
		return fmt.Errorf("server selected unsupported protocol %d", ack.SelectedProtocol)
	}
	log.Printf(
		"node registered instance_id=%s server_instance_id=%s protocol=%d",
		options.InstanceID,
		ack.ServerInstanceId,
		ack.SelectedProtocol,
	)

	receiveErr := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				receiveErr <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(options.HeartbeatInterval)
	defer ticker.Stop()
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			_ = stream.CloseSend()
			return nil
		case err := <-receiveErr:
			if errors.Is(err, io.EOF) {
				return errors.New("server closed the node control stream")
			}
			return fmt.Errorf("receive server message: %w", err)
		case timestamp := <-ticker.C:
			sequence++
			if err := stream.Send(&agentflowv1.NodeEnvelope{
				Body: &agentflowv1.NodeEnvelope_Heartbeat{
					Heartbeat: &agentflowv1.Heartbeat{
						Sequence:     sequence,
						SentAt:       timestamppb.New(timestamp),
						CommandReady: false,
					},
				},
			}); err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
		}
	}
}
