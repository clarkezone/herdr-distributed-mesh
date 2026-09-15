package server

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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Options struct {
	BindingPath       string
	RequiredClientTag string
	InstanceID        string
	ListenAddress     string
	RequiredNodeTag   string
	Transport         transport.Config
}

type service struct {
	agentflowv1.UnimplementedNodeControlServer
	agentflowv1.UnimplementedFleetServer

	instanceID        string
	identifyPeer      func(context.Context) (transport.PeerIdentity, error)
	requiredClientTag string
	requiredNodeTag   string
	bindNode          func(string, string) error
	helloTimeout      time.Duration
	heartbeatTimeout  time.Duration
	fleet             fleetStore
}

func Run(ctx context.Context, options Options) error {
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

	bindings, err := newBindingStore(options.BindingPath)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
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
	api := &service{
		instanceID:        options.InstanceID,
		requiredClientTag: options.RequiredClientTag,
		requiredNodeTag:   options.RequiredNodeTag,
		bindNode:          bindings.Bind,
		helloTimeout:      10 * time.Second,
		heartbeatTimeout:  time.Minute,
	}
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
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Printf("stopping mesh server")
		grpcServer.Stop()
		err := <-serveErr
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC: %w", err)
		}
		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve gRPC: %w", err)
		}
		return nil
	}
}

func (service *service) Connect(stream grpc.BidiStreamingServer[agentflowv1.NodeEnvelope, agentflowv1.NodeEnvelope]) error {
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
			return status.Errorf(codes.PermissionDenied, "bind mesh identity to Tailscale peer: %v", err)
		}
	}
	herdrEnabled := slices.Contains(hello.Capabilities, protocol.HerdrReadCapability)
	entry, err := service.fleet.begin(hello.InstanceId, identity.StableID, herdrEnabled, time.Now())
	if err != nil {
		return err
	}
	defer service.fleet.end(entry)
	if err := stream.Send(&agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_HelloAck{
			HelloAck: &agentflowv1.HelloAck{
				SelectedProtocol: selectedProtocol,
				ServerInstanceId: service.instanceID,
				Capabilities:     slices.Clone(protocol.ServerCapabilities),
			},
		},
	}); err != nil {
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

	heartbeatDeadline := time.Now().Add(service.timeoutOrDefault(service.heartbeatTimeout, time.Minute))
	var heartbeatSequence uint64
	for {
		if !time.Now().Before(heartbeatDeadline) {
			return status.Error(codes.DeadlineExceeded, "heartbeat deadline exceeded")
		}
		envelope, err := receiveNodeEnvelopeUntil(
			stream,
			time.Until(heartbeatDeadline), entry.done,
		)
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
			return status.Error(codes.InvalidArgument, "expected heartbeat or negotiated read-only Herdr state")
		}
		if heartbeat.Sequence <= heartbeatSequence || heartbeat.SentAt == nil || heartbeat.SentAt.CheckValid() != nil {
			return status.Error(codes.InvalidArgument, "invalid heartbeat sequence or timestamp")
		}
		heartbeatSequence = heartbeat.Sequence
		heartbeatDeadline = time.Now().Add(service.timeoutOrDefault(service.heartbeatTimeout, time.Minute))
		if err := service.fleet.heartbeat(entry, time.Now()); err != nil {
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
		Capabilities:          slices.Clone(protocol.ServerCapabilities),
	}, nil
}

func (service *service) authorizePeer(ctx context.Context, requiredTag string) (transport.PeerIdentity, error) {
	identity, err := service.identifyPeer(ctx)
	if err != nil {
		return transport.PeerIdentity{}, status.Errorf(codes.PermissionDenied, "identify Tailscale peer: %v", err)
	}
	if requiredTag != "" && !slices.Contains(identity.Tags, requiredTag) {
		return transport.PeerIdentity{}, status.Errorf(codes.PermissionDenied, "peer is missing required tag %q", requiredTag)
	}
	return identity, nil
}
