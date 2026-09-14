package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

type Config struct {
	AuthKeyEnv string
	Debug      bool
	Hostname   string
	StateDir   string
	Tags       []string
}

type PeerIdentity struct {
	StableID string
	Name     string
	Tags     []string
}

type SelfStatus struct {
	BackendState string     `json:"backend_state"`
	DNSName      string     `json:"dns_name"`
	Expired      bool       `json:"expired"`
	Health       []string   `json:"health"`
	HostName     string     `json:"host_name"`
	IPs          []string   `json:"ips"`
	KeyExpiry    *time.Time `json:"key_expiry,omitempty"`
	OS           string     `json:"os"`
	StableID     string     `json:"stable_id"`
	StateDir     string     `json:"state_dir"`
	Tags         []string   `json:"tags"`
}

type Network struct {
	magicDNSSuffix string
	self           SelfStatus
	server         *tsnet.Server
}

func Start(ctx context.Context, config Config) (*Network, error) {
	if strings.TrimSpace(config.Hostname) == "" {
		return nil, errors.New("tsnet hostname is required")
	}
	if strings.TrimSpace(config.StateDir) == "" {
		return nil, errors.New("tsnet state directory is required")
	}
	if err := os.MkdirAll(config.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create tsnet state directory: %w", err)
	}

	server := &tsnet.Server{
		AuthKey:       os.Getenv(config.AuthKeyEnv),
		Dir:           config.StateDir,
		Hostname:      config.Hostname,
		AdvertiseTags: config.Tags,
		UserLogf:      log.Printf,
	}
	if config.Debug {
		server.Logf = log.Printf
	}

	log.Printf(
		"starting tsnet hostname=%s state_dir=%s auth_key_present=%t",
		config.Hostname,
		config.StateDir,
		server.AuthKey != "",
	)
	status, err := server.Up(ctx)
	if err != nil {
		_ = server.Close()
		return nil, fmt.Errorf("bring up tsnet node: %w", err)
	}
	ipv4, ipv6 := server.TailscaleIPs()
	log.Printf("tsnet ready hostname=%s ipv4=%s ipv6=%s", config.Hostname, ipv4, ipv6)
	magicDNSSuffix := ""
	if status.CurrentTailnet != nil && status.CurrentTailnet.MagicDNSEnabled {
		magicDNSSuffix = status.CurrentTailnet.MagicDNSSuffix
	}
	return &Network{
		magicDNSSuffix: magicDNSSuffix,
		self:           makeSelfStatus(status, config.StateDir),
		server:         server,
	}, nil
}

func (network *Network) Close() error {
	return network.server.Close()
}

func (network *Network) Listen(address string) (net.Listener, error) {
	listener, err := network.server.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on tsnet address %q: %w", address, err)
	}
	return listener, nil
}

func (network *Network) DialGRPC(target string) (*grpc.ClientConn, error) {
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("gRPC target is required")
	}
	target = normalizeMagicDNSTarget(target, network.magicDNSSuffix)
	return grpc.NewClient(
		"passthrough:///"+target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return network.server.Dial(ctx, "tcp", address)
		}),
	)
}

func normalizeMagicDNSTarget(target, suffix string) string {
	host, port, err := net.SplitHostPort(target)
	if err != nil || suffix == "" {
		return target
	}
	trimmedHost := strings.TrimSuffix(host, ".")
	dnsSuffix := strings.TrimPrefix(strings.TrimSuffix(suffix, "."), ".")
	fullSuffix := "." + dnsSuffix
	if !strings.HasSuffix(strings.ToLower(trimmedHost), strings.ToLower(fullSuffix)) {
		return target
	}
	shortHost := trimmedHost[:len(trimmedHost)-len(fullSuffix)]
	if shortHost == "" {
		return target
	}
	return net.JoinHostPort(shortHost, port)
}

func (network *Network) IdentifyPeer(ctx context.Context, remoteAddress string) (PeerIdentity, error) {
	client, err := network.server.LocalClient()
	if err != nil {
		return PeerIdentity{}, fmt.Errorf("create Tailscale local client: %w", err)
	}
	response, err := client.WhoIs(ctx, remoteAddress)
	if err != nil {
		return PeerIdentity{}, fmt.Errorf("identify Tailscale peer %q: %w", remoteAddress, err)
	}
	if response.Node == nil {
		return PeerIdentity{}, errors.New("Tailscale WhoIs response did not contain a node")
	}
	return PeerIdentity{
		StableID: string(response.Node.StableID),
		Name:     strings.TrimSuffix(response.Node.Name, "."),
		Tags:     slices.Clone(response.Node.Tags),
	}, nil
}

func (network *Network) SelfStatus() SelfStatus {
	status := network.self
	status.Health = slices.Clone(status.Health)
	status.IPs = slices.Clone(status.IPs)
	status.Tags = slices.Clone(status.Tags)
	if status.KeyExpiry != nil {
		expiry := *status.KeyExpiry
		status.KeyExpiry = &expiry
	}
	return status
}

func (status SelfStatus) Validate(requiredTags []string, now time.Time) error {
	for _, requiredTag := range requiredTags {
		if !slices.Contains(status.Tags, requiredTag) {
			return fmt.Errorf("requested Tailscale tag %q is not assigned; assigned tags=%v", requiredTag, status.Tags)
		}
	}
	if status.Expired {
		return errors.New("Tailscale node key is expired")
	}
	if status.KeyExpiry != nil && !status.KeyExpiry.After(now) {
		return fmt.Errorf("Tailscale node key expired at %s", status.KeyExpiry.Format(time.RFC3339))
	}
	return nil
}

func makeSelfStatus(status *ipnstate.Status, stateDir string) SelfStatus {
	result := SelfStatus{StateDir: stateDir}
	if status == nil {
		return result
	}
	result.BackendState = status.BackendState
	result.Health = slices.Clone(status.Health)
	result.IPs = stringifyAddresses(status.TailscaleIPs)
	if status.Self == nil {
		return result
	}

	result.DNSName = strings.TrimSuffix(status.Self.DNSName, ".")
	result.Expired = status.Self.Expired
	result.HostName = status.Self.HostName
	result.OS = status.Self.OS
	result.StableID = string(status.Self.ID)
	result.IPs = stringifyAddresses(status.Self.TailscaleIPs)
	if status.Self.Tags != nil {
		result.Tags = slices.Clone(status.Self.Tags.AsSlice())
	}
	if status.Self.KeyExpiry != nil {
		expiry := *status.Self.KeyExpiry
		result.KeyExpiry = &expiry
	}
	return result
}

func stringifyAddresses(addresses []netip.Addr) []string {
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, address.String())
	}
	return result
}
