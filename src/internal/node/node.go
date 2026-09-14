package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"slices"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var capabilities = []string{"node.heartbeat.v1"}

type Options struct {
	HeartbeatInterval time.Duration
	InstanceID        string
	ReconnectDelay    time.Duration
	ReconnectMaximum  time.Duration
	ServerAddress     string
	Transport         transport.Config
}

func Run(ctx context.Context, options Options) error {
	if options.ReconnectMaximum <= 0 {
		options.ReconnectMaximum = time.Minute
	}
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer network.Close()
	self := network.SelfStatus()
	if err := self.Validate(options.Transport.Tags, time.Now()); err != nil {
		return fmt.Errorf("validate node tsnet identity: %w", err)
	}
	log.Printf(
		"node tsnet identity stable_id=%s dns=%s tags=%v key_expiry=%s",
		self.StableID,
		self.DNSName,
		self.Tags,
		formatExpiry(self.KeyExpiry),
	)

	connection, err := network.DialGRPC(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()

	client := agentflowv1.NewNodeControlClient(connection)
	attempt := 0
	for {
		registered, err := runSession(ctx, client, options)
		if ctx.Err() != nil {
			return nil
		}
		if isPermanentSessionError(err) {
			return fmt.Errorf("node session rejected permanently: %w", err)
		}
		if registered {
			attempt = 0
		}
		delay := reconnectDelay(options.ReconnectDelay, options.ReconnectMaximum, attempt, rand.Float64())
		log.Printf("node session ended: %v; reconnecting in %s", err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			attempt++
		}
	}
}

func isPermanentSessionError(err error) bool {
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

func runSession(ctx context.Context, client agentflowv1.NodeControlClient, options Options) (bool, error) {
	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Connect(sessionContext)
	if err != nil {
		return false, fmt.Errorf("open node control stream: %w", err)
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
		return false, fmt.Errorf("send hello: %w", err)
	}

	response, err := stream.Recv()
	if err != nil {
		return false, fmt.Errorf("receive hello acknowledgement: %w", err)
	}
	ack := response.GetHelloAck()
	if ack == nil {
		return false, errors.New("server did not respond with hello acknowledgement")
	}
	if ack.SelectedProtocol < protocol.MinimumVersion || ack.SelectedProtocol > protocol.MaximumVersion {
		return false, fmt.Errorf("server selected unsupported protocol %d", ack.SelectedProtocol)
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
			return true, nil
		case err := <-receiveErr:
			if errors.Is(err, io.EOF) {
				return true, errors.New("server closed the node control stream")
			}
			return true, fmt.Errorf("receive server message: %w", err)
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
				return true, fmt.Errorf("send heartbeat: %w", err)
			}
		}
	}
}

func reconnectDelay(base, maximum time.Duration, attempt int, jitter float64) time.Duration {
	delay := base
	for range attempt {
		if delay >= maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	factor := 0.8 + 0.4*jitter
	jittered := time.Duration(float64(delay) * factor)
	if jittered > maximum {
		return maximum
	}
	return jittered
}

func formatExpiry(expiry *time.Time) string {
	if expiry == nil {
		return "none"
	}
	return expiry.Format(time.RFC3339)
}
