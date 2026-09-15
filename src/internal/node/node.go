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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var capabilities = []string{"node.heartbeat.v1"}

type Options struct {
	CommandJournalPath string
	RequiredServerTag  string
	HeartbeatInterval  time.Duration
	HerdrSocket        string
	InstanceID         string
	ReconnectDelay     time.Duration
	ReconnectMaximum   time.Duration
	ServerAddress      string
	Transport          transport.Config
}

func Run(ctx context.Context, options Options) (err error) {
	if options.ReconnectMaximum <= 0 {
		options.ReconnectMaximum = time.Minute
	}
	journal, err := openJournal(ctx, options)
	if err != nil {
		return err
	}
	var commands commandJournal
	if journal != nil {
		commands = journal
		defer func() {
			if closeErr := journal.Close(); closeErr != nil {
				err = errors.Join(err, storageError("close", closeErr))
			}
		}()
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

	dial := network.DialGRPC
	if commands != nil {
		dial = func(target string) (*grpc.ClientConn, error) {
			return network.DialGRPCWithPeerTag(target, options.RequiredServerTag)
		}
	}
	connection, err := dial(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()

	client := agentflowv1.NewNodeControlClient(connection)
	attempt := 0
	for {
		registered, err := runSessionWithJournal(ctx, client, options, commands, nil)
		var journalFailure *journalError
		if errors.As(err, &journalFailure) {
			return err
		}
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
	var journalFailure *journalError
	if errors.As(err, &journalFailure) {
		return true
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

func runSession(ctx context.Context, client agentflowv1.NodeControlClient, options Options) (bool, error) {
	return RunSession(ctx, client, options)
}

func runSessionWithJournal(ctx context.Context, client agentflowv1.NodeControlClient, options Options, journal commandJournal, execute func()) (bool, error) {
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
	if journal != nil {
		advertised = append(advertised, protocol.ProbeCapability)
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
	commandReady := journal != nil && slices.Contains(ack.Capabilities, protocol.ProbeCapability)
	handler := &commandHandler{journal: journal, nodeID: options.InstanceID, execute: execute}
	var pending []*agentflowv1.CommandResult
	if commandReady {
		operation, stop := context.WithTimeout(sessionContext, journalOperationTimeout)
		pending, err = journal.PendingResults(operation)
		stop()
		if err != nil {
			return true, storageError("pending results", err)
		}
	}
	log.Printf(
		"node registered instance_id=%s server_instance_id=%s protocol=%d",
		options.InstanceID,
		ack.ServerInstanceId,
		ack.SelectedProtocol,
	)

	type incomingMessage struct {
		envelope   *agentflowv1.NodeEnvelope
		receivedAt time.Time
		err        error
	}
	incoming := make(chan incomingMessage, 16)
	receiveDone := make(chan struct{})
	go func() {
		defer close(receiveDone)
		for {
			envelope, err := stream.Recv()
			select {
			case incoming <- incomingMessage{envelope: envelope, receivedAt: time.Now(), err: err}:
			case <-sessionContext.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-receiveDone; _ = stream.CloseSend() }()

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

	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	replayTicker := time.NewTicker(5 * time.Millisecond)
	defer replayTicker.Stop()
	var replay <-chan time.Time
	if len(pending) > 0 {
		replay = replayTicker.C
	}
	var sequence uint64
	sendHeartbeat := func(timestamp time.Time) error {
		sequence++
		return send(&agentflowv1.NodeEnvelope{
			Body: &agentflowv1.NodeEnvelope_Heartbeat{
				Heartbeat: &agentflowv1.Heartbeat{
					Sequence: sequence, SentAt: timestamppb.New(timestamp), CommandReady: commandReady,
				},
			},
		})
	}
	if err := sendHeartbeat(time.Now()); err != nil {
		return true, fmt.Errorf("send initial heartbeat: %w", err)
	}
	handleIncoming := func(message incomingMessage) error {
		if message.err != nil {
			if errors.Is(message.err, io.EOF) {
				return errors.New("server closed the node control stream")
			}
			return fmt.Errorf("receive server message: %w", message.err)
		}
		envelope := message.envelope
		if envelope == nil {
			return status.Error(codes.InvalidArgument, "empty server envelope")
		}
		if envelope.GetCommand() == nil && envelope.GetCommandAck() == nil {
			return nil
		}
		if !commandReady {
			return status.Error(codes.FailedPrecondition, "server sent command traffic without negotiated probe readiness")
		}
		if len(envelope.ProtoReflect().GetUnknown()) != 0 {
			return status.Error(codes.InvalidArgument, "unknown command envelope fields")
		}
		if command := envelope.GetCommand(); command != nil {
			result, err := handler.handle(sessionContext, command, message.receivedAt)
			if err != nil {
				return err
			}
			return send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: result}})
		}
		return handler.acknowledge(sessionContext, envelope.GetCommandAck())
	}
	for {
		// Prioritize due heartbeats before each bounded unit of command/replay work.
		select {
		case timestamp := <-ticker.C:
			if err := sendHeartbeat(timestamp); err != nil {
				return true, fmt.Errorf("send heartbeat: %w", err)
			}
		default:
		}
		select {
		case <-sessionContext.Done():
			return true, sessionContext.Err()
		case err := <-observerErr:
			if err == nil {
				err = errors.New("Herdr observer stopped unexpectedly")
			}
			return true, fmt.Errorf("observe Herdr: %w", err)
		case state := <-updates:
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: state}}); err != nil {
				return true, fmt.Errorf("send Herdr state: %w", err)
			}
		case message := <-incoming:
			if err := handleIncoming(message); err != nil {
				return true, err
			}
		case <-replay:
			// Drain one incoming acknowledgement before replay, never the whole queue.
			select {
			case message := <-incoming:
				if err := handleIncoming(message); err != nil {
					return true, err
				}
			default:
			}
			result := pending[0]
			pending[0] = nil
			pending = pending[1:]
			if err := protocol.ValidateProbeResult(result); err != nil {
				return true, storageError("pending result", err)
			}
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: result}}); err != nil {
				return true, fmt.Errorf("replay command result: %w", err)
			}
			if len(pending) == 0 {
				replay = nil
			}
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
