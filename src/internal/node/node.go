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
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var capabilities = []string{"node.heartbeat.v1"}

type Options struct {
	HeartbeatInterval time.Duration
	HerdrSocket       string
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
	// Cancel the entire stream on a blocked send; only this goroutine sends.
	send := func(envelope *agentflowv1.NodeEnvelope) error {
		timer := time.AfterFunc(10*time.Second, cancel)
		defer timer.Stop()
		return stream.Send(envelope)
	}
	advertised := slices.Clone(capabilities)
	if options.HerdrSocket != "" {
		advertised = append(advertised, protocol.HerdrReadCapability)
	}
	if err := send(&agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_Hello{
			Hello: &agentflowv1.Hello{
				Protocol:              protocol.SupportedRange(),
				ImplementationVersion: buildinfo.Version,
				InstanceId:            options.InstanceID,
				Role:                  agentflowv1.Role_ROLE_NODE,
				Capabilities:          advertised,
			},
		},
	}); err != nil {
		return false, fmt.Errorf("send hello: %w", err)
	}

	helloTimer := time.AfterFunc(10*time.Second, cancel)
	response, err := stream.Recv()
	helloTimer.Stop()
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
	if options.HerdrSocket != "" && !slices.Contains(ack.Capabilities, protocol.HerdrReadCapability) {
		return false, status.Error(codes.FailedPrecondition, "server does not support read-only Herdr state; upgrade server first")
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

	updates := make(chan *agentflowv1.HerdrState, 1)
	observerErr := make(chan error, 1)
	if options.HerdrSocket != "" {
		done := make(chan struct{})
		go func() {
			defer close(done)
			observerErr <- herdr.Observe(sessionContext, herdr.Config{SocketPath: options.HerdrSocket},
				func(state *agentflowv1.HerdrState) error {
					select {
					case updates <- state:
						return nil
					case <-sessionContext.Done():
						return sessionContext.Err()
					}
				})
		}()
		defer func() { cancel(); <-done }()
	}

	ticker := time.NewTicker(options.HeartbeatInterval)
	defer ticker.Stop()
	var sequence uint64
	sendHeartbeat := func(timestamp time.Time) error {
		sequence++
		return send(&agentflowv1.NodeEnvelope{
			Body: &agentflowv1.NodeEnvelope_Heartbeat{
				Heartbeat: &agentflowv1.Heartbeat{
					Sequence: sequence, SentAt: timestamppb.New(timestamp), CommandReady: false,
				},
			},
		})
	}
	for {
		select {
		case <-sessionContext.Done():
			_ = stream.CloseSend()
			return true, sessionContext.Err()
		case err := <-observerErr:
			if err == nil {
				err = errors.New("Herdr observer stopped unexpectedly")
			}
			return true, fmt.Errorf("observe Herdr: %w", err)
		case state := <-updates:
			// Give a due heartbeat priority even during continuous refresh traffic.
			select {
			case timestamp := <-ticker.C:
				if err := sendHeartbeat(timestamp); err != nil {
					return true, fmt.Errorf("send heartbeat: %w", err)
				}
			default:
			}
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: state}}); err != nil {
				return true, fmt.Errorf("send Herdr state: %w", err)
			}
		case err := <-receiveErr:
			if errors.Is(err, io.EOF) {
				return true, errors.New("server closed the node control stream")
			}
			return true, fmt.Errorf("receive server message: %w", err)
		case timestamp := <-ticker.C:
			if err := sendHeartbeat(timestamp); err != nil {
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
