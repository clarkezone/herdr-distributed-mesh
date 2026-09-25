package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/dashboard"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func validateDashboardOptions(options Options) error {
	if options.DashboardListenAddress == "" && options.DashboardOrigin == "" {
		return nil
	}
	if options.DashboardListenAddress == "" || options.DashboardOrigin == "" || options.Output == nil {
		return errors.New("hosted dashboard requires listen address, origin, and output")
	}
	host, port, err := net.SplitHostPort(options.DashboardListenAddress)
	if err != nil || host != "" {
		return errors.New("dashboard listen address must be a tsnet port such as :8787")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return errors.New("dashboard listen port must be 1..65535")
	}
	if _, rpcPort, err := net.SplitHostPort(options.ListenAddress); err == nil {
		rpcNumber, _ := strconv.Atoi(rpcPort)
		if number == rpcNumber {
			return errors.New("dashboard and gRPC require distinct ports")
		}
	}
	if err := dashboard.ValidateHostedOrigin(options.DashboardOrigin); err != nil {
		return err
	}
	u, err := url.Parse(options.DashboardOrigin)
	if err != nil {
		return err
	}
	originPort := u.Port()
	if originPort == "" {
		originPort = "80"
		if u.Scheme == "https" {
			originPort = "443"
		}
	}
	originNumber, err := strconv.Atoi(originPort)
	if err != nil || originNumber != number {
		return errors.New("dashboard origin port must match its tsnet listener")
	}
	return nil
}

type hostedFleetReader struct{ fleet *fleetStore }

func (r hostedFleetReader) ListNodes(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*pb.NodeList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.fleet.list(time.Now())
}

func dashboardPeerAuthorizer(identify func(context.Context, string) (transport.PeerIdentity, error)) dashboard.PeerAuthorizer {
	return func(ctx context.Context, address string) error {
		if identify == nil {
			return errors.New("dashboard peer authentication is not configured")
		}
		identity, err := identify(ctx, address)
		if err != nil {
			return err
		}
		if identity.StableID == "" {
			return errors.New("dashboard peer has no Tailscale identity")
		}
		return nil
	}
}

func serveHostedCoordinator(ctx context.Context, options Options, network *transport.Network, rpcListener net.Listener, rpcServer *grpc.Server, api *service) error {
	u, err := url.Parse(options.DashboardOrigin)
	if err != nil {
		return err
	}
	listen := network.Listen
	if u.Scheme == "https" {
		if u.Hostname() != strings.TrimSuffix(network.SelfStatus().DNSName, ".") {
			return errors.New("HTTPS dashboard origin must use the coordinator's full Tailscale DNS name")
		}
		listen = network.ListenTLS
	}
	listener, err := listen(options.DashboardListenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()
	return runPairedServices(ctx,
		func(ctx context.Context) error { return serveCoordinator(ctx, rpcListener, rpcServer, &api.fleet) },
		func(ctx context.Context) error {
			return dashboard.ServeHosted(ctx, listener, hostedFleetReader{&api.fleet}, dashboard.HostedOptions{
				Origin: options.DashboardOrigin, Output: options.Output,
				AuthorizePeer: dashboardPeerAuthorizer(network.IdentifyPeer),
			})
		})
}

func runPairedServices(ctx context.Context, rpc, web func(context.Context) error) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		name string
		err  error
	}
	done := make(chan outcome, 2)
	go func() { done <- outcome{"gRPC", rpc(child)} }()
	go func() { done <- outcome{"dashboard", web(child)} }()
	first := <-done
	if first.err == nil && ctx.Err() == nil {
		first.err = fmt.Errorf("%s service stopped unexpectedly", first.name)
	}
	cancel()
	second := <-done
	return errors.Join(first.err, second.err)
}
