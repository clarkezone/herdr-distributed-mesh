package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Options struct {
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
}

func Run(ctx context.Context, options Options) error {
	network, err := transport.Start(ctx, options.Transport)
	if err != nil {
		return err
	}
	defer network.Close()

	listener, err := network.Listen(options.ListenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	api := &service{
		instanceID:        options.InstanceID,
		requiredClientTag: options.RequiredClientTag,
		requiredNodeTag:   options.RequiredNodeTag,
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

	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "receive hello: %v", err)
	}
	hello := first.GetHello()
	if err := protocol.ValidateNodeHello(hello); err != nil {
		return status.Errorf(codes.FailedPrecondition, "invalid hello: %v", err)
	}
	selectedProtocol, _ := protocol.Negotiate(hello.Protocol)
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

	for {
		envelope, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		heartbeat := envelope.GetHeartbeat()
		if heartbeat == nil {
			return status.Error(codes.InvalidArgument, "only heartbeat messages are accepted after hello")
		}
		log.Printf(
			"node heartbeat instance_id=%s sequence=%d command_ready=%t",
			hello.InstanceId,
			heartbeat.Sequence,
			heartbeat.CommandReady,
		)
	}
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
