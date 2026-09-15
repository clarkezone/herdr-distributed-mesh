package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Options struct {
	BindingPath        string
	DatabasePath       string
	RequiredClientTag  string
	RequiredCommandTag string
	InstanceID         string
	ListenAddress      string
	RequiredNodeTag    string
	Transport          transport.Config
}

type service struct {
	agentflowv1.UnimplementedNodeControlServer
	agentflowv1.UnimplementedFleetServer

	instanceID         string
	identifyPeer       func(context.Context) (transport.PeerIdentity, error)
	requiredClientTag  string
	requiredCommandTag string
	requiredNodeTag    string
	bindNode           func(string, string) error
	helloTimeout       time.Duration
	heartbeatTimeout   time.Duration
	fleet              fleetStore
	commands           *state.Store
}

func Run(ctx context.Context, options Options) (result error) {
	store, restored, err := openCoordinatorState(ctx, options)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, store.Close()) }()
	api := &service{
		instanceID:         options.InstanceID,
		requiredClientTag:  options.RequiredClientTag,
		requiredCommandTag: options.RequiredCommandTag,
		requiredNodeTag:    options.RequiredNodeTag,
		bindNode:           durableBinder(store),
		helloTimeout:       10 * time.Second,
		heartbeatTimeout:   time.Minute,
		commands:           store,
	}
	api.fleet.storage = store
	api.fleet.commands = store
	api.fleet.fatal = make(chan error, 1)
	if err := api.fleet.restore(restored, time.Now()); err != nil {
		return fmt.Errorf("restore durable fleet: %w", err)
	}
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer network.Close()
	self := network.SelfStatus()
	if err := self.Validate(options.Transport.Tags, time.Now()); err != nil {
		return fmt.Errorf("validate server tsnet identity: %w", err)
	}
	log.Printf(
		"server tsnet identity stable_id=%s dns=%s tags=%v key_expiry=%s",
		self.StableID,
		self.DNSName,
		self.Tags,
		formatExpiry(self.KeyExpiry),
	)

	listener, err := network.Listen(options.ListenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()

	grpcServer := grpc.NewServer(
		grpc.WaitForHandlers(true),
		grpc.MaxRecvMsgSize(1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	api.identifyPeer = func(ctx context.Context) (transport.PeerIdentity, error) {
		grpcPeer, ok := peer.FromContext(ctx)
		if !ok || grpcPeer.Addr == nil {
			return transport.PeerIdentity{}, errors.New("gRPC peer address is unavailable")
		}
		return network.IdentifyPeer(ctx, grpcPeer.Addr.String())
	}
	agentflowv1.RegisterNodeControlServer(grpcServer, api)
	agentflowv1.RegisterFleetServer(grpcServer, api)

	log.Printf("mesh server ready instance_id=%s address=%s", options.InstanceID, listener.Addr())
	return serveCoordinator(ctx, listener, grpcServer, &api.fleet)
}

func serveCoordinator(ctx context.Context, listener net.Listener, grpcServer *grpc.Server, fleet *fleetStore) error {
	defer grpcServer.Stop()
	expiry := time.NewTicker(500 * time.Millisecond)
	defer expiry.Stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("stopping mesh server")
			grpcServer.Stop()
			err := <-serveErr
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				return fmt.Errorf("serve gRPC: %w", err)
			}
			fleet.mu.Lock()
			persistenceErr := fleet.storageErr
			fleet.mu.Unlock()
			if persistenceErr != nil {
				return fmt.Errorf("coordinator persistence failed during shutdown: %w", persistenceErr)
			}
			return nil
		case err := <-fleet.fatal:
			grpcServer.Stop()
			<-serveErr
			return fmt.Errorf("coordinator stopped after persistence failure: %w", err)
		case err := <-serveErr:
			if err != nil {
				return fmt.Errorf("serve gRPC: %w", err)
			}
			return nil
		case now := <-expiry.C:
			if err := fleet.expireCommands(now); err != nil {
				grpcServer.Stop()
				<-serveErr
				return err
			}
		}
	}
}

func (service *service) Connect(stream grpc.BidiStreamingServer[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope]) (result error) {
	identity, err := service.authorizePeer(stream.Context(), service.requiredNodeTag)
	if err != nil {
		return err
	}

	first, err := receiveNodeEnvelope(stream, service.timeoutOrDefault(service.helloTimeout, 10*time.Second))
	if err != nil {
		if errors.Is(err, errReceiveTimeout) {
			return status.Errorf(codes.DeadlineExceeded, "receive hello: %v", err)
		}
		return status.Errorf(codes.InvalidArgument, "receive hello: %v", err)
	}
	hello := first.GetHello()
	if err := protocol.ValidateNodeHello(hello); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid hello: %v", err)
	}
	selectedProtocol, _ := protocol.Negotiate(hello.Protocol)
	if service.bindNode != nil {
		if err := service.bindNode(identity.StableID, hello.InstanceId); err != nil {
			if errors.Is(err, state.ErrIdentityConflict) {
				return status.Error(codes.PermissionDenied, "mesh identity conflicts with an existing Tailscale binding")
			}
			return service.fleet.fail(err)
		}
	}
	herdrEnabled := slices.Contains(hello.Capabilities, protocol.HerdrReadCapability)
	entry, err := service.fleet.beginSession(stream.Context(), hello.InstanceId, identity.StableID, herdrEnabled, time.Now())
	if err != nil {
		return err
	}
	defer func() {
		if err := service.fleet.end(entry); err != nil && result == nil {
			result = err
		}
	}()
	service.fleet.mu.Lock()
	entry.probes = service.commands != nil && slices.Contains(hello.Capabilities, protocol.ProbeCapability)
	service.fleet.mu.Unlock()
	if err := sendNodeEnvelope(stream, &agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_HelloAck{
			HelloAck: &agentflowv1.HelloAck{
				SelectedProtocol: selectedProtocol,
				ServerInstanceId: service.instanceID,
				Capabilities:     service.capabilities(),
			},
		},
	}, entry); err != nil {
		return status.Errorf(codes.Unavailable, "send hello acknowledgement: %v", err)
	}

	log.Printf(
		"node connected instance_id=%s peer_id=%s peer_name=%s protocol=%d capabilities=%v",
		hello.InstanceId,
		identity.StableID,
		identity.Name,
		selectedProtocol,
		hello.Capabilities,
	)
	defer log.Printf("node disconnected instance_id=%s peer_id=%s", hello.InstanceId, identity.StableID)

	heartbeatTimeout := service.timeoutOrDefault(service.heartbeatTimeout, time.Minute)
	heartbeatDeadline := time.Now().Add(heartbeatTimeout)
	heartbeatTimer := time.NewTimer(heartbeatTimeout)
	defer heartbeatTimer.Stop()
	sessionContext, cancel := context.WithCancel(stream.Context())
	defer cancel()
	received := make(chan receiveResult, 1)
	go func() {
		for {
			envelope, err := stream.Recv()
			select {
			case received <- receiveResult{envelope: envelope, err: err}:
			case <-sessionContext.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var heartbeatSequence uint64
	for {
		if !time.Now().Before(heartbeatDeadline) {
			return status.Error(codes.DeadlineExceeded, "heartbeat deadline exceeded")
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-entry.done:
			return status.Error(codes.Aborted, "node stream superseded")
		case <-heartbeatTimer.C:
			return status.Error(codes.DeadlineExceeded, "heartbeat deadline exceeded")
		case pending := <-entry.outbound:
			if !time.Now().Before(heartbeatDeadline) {
				return status.Error(codes.DeadlineExceeded, "heartbeat deadline exceeded")
			}
			outbound, err := service.prepareCommand(entry, pending.GetCommand())
			if err != nil {
				return err
			}
			if outbound != nil {
				if err := sendNodeEnvelope(stream, &agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_Command{Command: outbound}}, entry); err != nil {
					return err
				}
			}
		case result := <-received:
			if !time.Now().Before(heartbeatDeadline) {
				return status.Error(codes.DeadlineExceeded, "heartbeat deadline exceeded")
			}
			envelope, err := result.envelope, result.err
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				if errors.Is(err, errReceiveTimeout) {
					return status.Errorf(codes.DeadlineExceeded, "receive heartbeat: %v", err)
				}
				return err
			}
			heartbeat := envelope.GetHeartbeat()
			if heartbeat == nil {
				if state := envelope.GetHerdrState(); state != nil && herdrEnabled {
					if err := service.fleet.update(entry, state, time.Now()); err != nil {
						return err
					}
					continue
				}
				if envelope.GetCommandAck() != nil && entry.probes {
					ack := envelope.GetCommandAck()
					if !protocol.ValidCommandID(ack.CommandId) || ack.Status != agentflowv1.CommandStatus_COMMAND_STATUS_ACCEPTED ||
						(ack.Detail != "" && ack.Detail != "accepted") || len(ack.ProtoReflect().GetUnknown()) != 0 {
						return status.Error(codes.InvalidArgument, "invalid command acknowledgement")
					}
					continue
				}
				if envelope.GetCommandResult() != nil && entry.probes {
					ack, err := service.finishCommand(entry, envelope.GetCommandResult())
					if err != nil {
						return err
					}
					if err := sendNodeEnvelope(stream, &agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandAck{CommandAck: ack}}, entry); err != nil {
						return err
					}
					continue
				}
				return status.Error(codes.InvalidArgument, "expected heartbeat or negotiated read-only Herdr state")
			}
			if heartbeat.Sequence <= heartbeatSequence || heartbeat.SentAt == nil || heartbeat.SentAt.CheckValid() != nil {
				return status.Error(codes.InvalidArgument, "invalid heartbeat sequence or timestamp")
			}
			heartbeatSequence = heartbeat.Sequence
			heartbeatDeadline = time.Now().Add(heartbeatTimeout)
			heartbeatTimer.Reset(heartbeatTimeout)
			if err := service.fleet.heartbeat(entry, time.Now(), heartbeat.CommandReady); err != nil {
				return err
			}
			log.Printf(
				"node heartbeat instance_id=%s sequence=%d command_ready=%t",
				hello.InstanceId,
				heartbeat.Sequence,
				heartbeat.CommandReady,
			)
		}
	}
}

func sendNodeEnvelope(stream grpc.BidiStreamingServer[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope], envelope *agentflowv1.NodeEnvelope, entry *fleetEntry) error {
	if !entry.registerSend() {
		return status.Error(codes.Aborted, "node stream superseded")
	}
	done := make(chan error, 1)
	go func() {
		defer entry.sends.Done()
		done <- stream.Send(envelope)
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-entry.done:
		return status.Error(codes.Aborted, "node stream superseded")
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "node send deadline exceeded")
	}
}

type nodeEnvelopeReceiver interface {
	Recv() (*agentflowv1.NodeEnvelope, error)
}

type receiveResult struct {
	envelope *agentflowv1.NodeEnvelope
	err      error
}

var errReceiveTimeout = errors.New("receive timed out")

func receiveNodeEnvelope(receiver nodeEnvelopeReceiver, timeout time.Duration) (*agentflowv1.NodeEnvelope, error) {
	return receiveNodeEnvelopeUntil(receiver, timeout, nil)
}

func receiveNodeEnvelopeUntil(receiver nodeEnvelopeReceiver, timeout time.Duration, superseded <-chan struct{}) (*agentflowv1.NodeEnvelope, error) {
	result := make(chan receiveResult, 1)
	go func() {
		envelope, err := receiver.Recv()
		result <- receiveResult{envelope: envelope, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case value := <-result:
		return value.envelope, value.err
	case <-timer.C:
		return nil, fmt.Errorf("%w after %s", errReceiveTimeout, timeout)
	case <-superseded:
		return nil, status.Error(codes.Aborted, "node stream superseded")
	}
}

func (service *service) timeoutOrDefault(value, defaultValue time.Duration) time.Duration {
	if value <= 0 {
		return defaultValue
	}
	return value
}

func formatExpiry(expiry *time.Time) string {
	if expiry == nil {
		return "none"
	}
	return expiry.Format(time.RFC3339)
}

func (service *service) GetServerInfo(ctx context.Context, _ *emptypb.Empty) (*agentflowv1.ServerInfo, error) {
	if _, err := service.authorizePeer(ctx, service.requiredClientTag); err != nil {
		return nil, err
	}
	return &agentflowv1.ServerInfo{
		InstanceId:            service.instanceID,
		ImplementationVersion: buildinfo.Version,
		Protocol:              protocol.SupportedRange(),
		Capabilities:          service.capabilities(),
	}, nil
}

func (service *service) capabilities() []string {
	values := slices.Clone(protocol.ServerCapabilities)
	if service.commands != nil {
		values = append(values, protocol.ProbeCapability)
	}
	return values
}

func (service *service) authorizePeer(ctx context.Context, requiredTag string) (transport.PeerIdentity, error) {
	identity, err := service.identifyPeer(ctx)
	if err != nil {
		return transport.PeerIdentity{}, status.Errorf(codes.PermissionDenied, "identify Tailscale peer: %v", err)
	}
	if identity.StableID == "" {
		return transport.PeerIdentity{}, status.Error(codes.PermissionDenied, "Tailscale peer has no stable identity")
	}
	if requiredTag != "" && !slices.Contains(identity.Tags, requiredTag) {
		return transport.PeerIdentity{}, status.Errorf(codes.PermissionDenied, "peer is missing required tag %q", requiredTag)
	}
	return identity, nil
}
