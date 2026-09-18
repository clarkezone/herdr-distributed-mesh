package meshlocal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/node"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/server"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	serverTag = "tag:herdr-mesh-server"
	nodeTag   = "tag:herdr-mesh-node"
	clientTag = "tag:herdr-mesh-client"
)

type runtimeDependencies struct {
	start  func(context.Context, transport.Config) (transport.RuntimeNetwork, error)
	server func(context.Context, server.Options, transport.RuntimeNetwork) error
	node   func(context.Context, node.Options, transport.RuntimeNetwork) error
}

func Run(ctx context.Context, dir string, output io.Writer) error {
	err := run(ctx, dir, output, runtimeDependencies{
		start: func(ctx context.Context, config transport.Config) (transport.RuntimeNetwork, error) {
			return transport.Start(ctx, config)
		},
		server: server.RunWithNetwork,
		node:   node.RunWithNetwork,
	})
	if err != nil {
		return diagnosticError{err}
	}
	return nil
}

var diagnosticSecrets = regexp.MustCompile(`https?://[^\s"'<>]+|tskey-[A-Za-z0-9_-]+`)

type diagnosticError struct{ cause error }

func (e diagnosticError) Unwrap() error { return e.cause }
func (e diagnosticError) Error() string {
	text := diagnosticSecrets.ReplaceAllString(e.cause.Error(), "[redacted]")
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 512 {
		text = text[:509] + "..."
	}
	return text
}

type statusWriter struct {
	mu     sync.Mutex
	dir    string
	status Status
	err    error
	cancel context.CancelFunc
}

func (s *statusWriter) update(update func(*Status)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	update(&s.status)
	s.err = writeStatus(s.dir, s.status)
	if s.err != nil {
		s.cancel()
	}
	return s.err
}

func (s *statusWriter) login(message string) {
	for _, word := range strings.Fields(message) {
		word = strings.Trim(word, `"'<>(),`)
		parsed, err := url.Parse(word)
		// tsnet's official interactive login URL is the only secret accepted.
		if err == nil && parsed.Scheme == "https" && (parsed.Host == "login.tailscale.com" || parsed.Host == "controlplane.tailscale.com") &&
			parsed.User == nil && strings.HasPrefix(parsed.Path, "/a/") && len(word) <= 2048 {
			_ = s.update(func(status *Status) { status.State, status.AuthURL = "login_required", word })
		}
	}
}

func run(ctx context.Context, dir string, output io.Writer, deps runtimeDependencies) (result error) {
	config, err := Load(dir)
	if err != nil {
		return err
	}
	root, err := privateDir(dir, false)
	if err != nil {
		return err
	}
	// One root guard owns the identity; role-specific guards remain independent.
	guard, err := state.PrepareRoleState(ctx, root, "client")
	if err != nil {
		return fmt.Errorf("acquire managed runtime ownership: %w", err)
	}
	defer func() { result = errors.Join(result, guard.Close()) }()
	if err := guard.DisableFullRoleBackup(); err != nil {
		return err
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := &statusWriter{dir: root, cancel: cancel}
	if err := writer.update(func(status *Status) { status.State = "starting" }); err != nil {
		return err
	}
	defer func() {
		writer.mu.Lock()
		defer writer.mu.Unlock()
		result = errors.Join(result, writer.err)
		final := Status{State: "stopped", DNSName: writer.status.DNSName, Server: writer.status.Server}
		if result != nil {
			final.State = "failed"
			final.Error = diagnosticError{result}.Error()
		}
		result = errors.Join(result, writeStatus(root, final))
	}()
	if config.Coordinator {
		if err := requirePolicyComplete(root); err != nil {
			return fmt.Errorf("coordinator policy setup is not complete; resume init before starting the managed runtime: %w", err)
		}
	}
	tags := []string{nodeTag, clientTag}
	if config.Coordinator {
		tags = append(tags, serverTag)
	}
	transportConfig := transport.Config{
		Hostname: "herdr-mesh-" + config.Name, RoleStateDir: root,
		StateDir: filepath.Join(root, "tsnet"), Tags: tags, UserLog: writer.login,
	}
	if _, err := privateDir(transportConfig.StateDir, true); err != nil {
		return err
	}
	network, err := deps.start(child, transportConfig)
	if err != nil {
		return fmt.Errorf("start managed network: %w", err)
	}
	defer func() { result = errors.Join(result, network.Close()) }()
	self := network.SelfStatus()
	if !self.MagicDNSEnabled {
		return errors.New("MagicDNS is disabled or unavailable; enable MagicDNS for this tailnet and retry (no join endpoint can be advertised)")
	}
	if self.StableID == "" || self.DNSName == "" || len(self.DNSName) > 253 {
		return errors.New("managed network has no assigned stable identity or DNS name")
	}
	dnsName := strings.TrimSuffix(self.DNSName, ".")
	label, suffix, qualified := strings.Cut(dnsName, ".")
	if !qualified || label == "" || suffix == "" {
		return errors.New("managed network has no assigned full DNS name")
	}
	if err := self.Validate(tags, time.Now()); err != nil {
		return fmt.Errorf("validate assigned managed roles: %w", err)
	}
	tailnet := strings.TrimSuffix(config.Tailnet, ".")
	if tailnet != "" && !strings.EqualFold(tailnet, strings.TrimSuffix(self.Tailnet, ".")) && !strings.EqualFold(tailnet, strings.TrimSuffix(self.DNSSuffix, ".")) {
		return errors.New("assigned network does not match the configured tailnet")
	}
	target := config.Server
	if config.Coordinator {
		port := "50052"
		if target != "" {
			_, port, _ = net.SplitHostPort(target)
		}
		target = net.JoinHostPort(dnsName, port)
	}
	if err := writer.update(func(status *Status) {
		status.State, status.DNSName, status.Server, status.AuthURL = "starting", dnsName, target, ""
	}); err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(child)
	// Every launched role is joined before its borrowed network is closed.
	defer func() { cancel(); result = errors.Join(result, group.Wait()) }()
	if config.Coordinator {
		_, port, _ := net.SplitHostPort(target)
		role := transportConfig
		role.RoleStateDir = filepath.Join(root, "server")
		options := server.Options{Transport: role, ListenAddress: ":" + port,
			DatabasePath:      filepath.Join(role.RoleStateDir, "coordinator.db"),
			RequiredClientTag: clientTag, RequiredCommandTag: clientTag, RequiredNodeTag: nodeTag, Output: output}
		group.Go(func() error {
			err := deps.server(groupCtx, options, network)
			if err == nil && groupCtx.Err() == nil {
				return errors.New("managed coordinator stopped unexpectedly")
			}
			return err
		})
	}
	upstream, err := network.DialGRPCWithPeerTag(target, serverTag)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, upstream.Close()) }()
	ready, stopReady := context.WithTimeout(groupCtx, 30*time.Second)
	info, err := pb.NewFleetClient(upstream).GetServerInfo(ready, &emptypb.Empty{}, grpc.WaitForReady(true))
	stopReady()
	if err != nil {
		return fmt.Errorf("verify managed coordinator: %w", err)
	}
	if info.GetInstanceId() == "" {
		return errors.New("managed coordinator has no instance identity")
	}
	if _, err := protocol.Negotiate(info.GetProtocol()); err != nil {
		return fmt.Errorf("verify managed protocol: %w", err)
	}
	role := transportConfig
	role.RoleStateDir = filepath.Join(root, "node")
	registered := make(chan string, 1)
	nodeOptions := node.Options{Name: config.Name, Transport: role, ServerAddress: target,
		RequiredServerTag: serverTag, CommandJournalPath: filepath.Join(role.RoleStateDir, "commands.db"),
		HerdrExecutable: config.HerdrExecutable, EnableAgentControl: config.HerdrExecutable != "",
		HeartbeatInterval: 10 * time.Second, ReconnectDelay: time.Second, ReconnectMaximum: time.Minute,
		OnRegistered: func(instanceID string) {
			select {
			case registered <- instanceID:
			default:
			}
		}}
	group.Go(func() error {
		err := deps.node(groupCtx, nodeOptions, network)
		if err == nil && groupCtx.Err() == nil {
			return errors.New("managed worker stopped unexpectedly")
		}
		return err
	})
	registration, stopRegistration := context.WithTimeout(groupCtx, 30*time.Second)
	err = waitWorkerRegistration(registration, upstream, registered)
	stopRegistration()
	if err != nil {
		return fmt.Errorf("verify managed worker registration: %w", err)
	}
	proxy, err := newProxy(upstream)
	if err != nil {
		return err
	}
	listener, err := listenIPC(root)
	if err != nil {
		return fmt.Errorf("listen on private managed IPC: %w", err)
	}
	defer listener.Close()
	defer proxy.Stop()
	group.Go(func() error {
		err := proxy.Serve(limitedListener{Listener: listener, gate: make(chan struct{}, 64)})
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve managed IPC: %w", err)
		}
		if groupCtx.Err() == nil {
			return errors.New("managed IPC stopped unexpectedly")
		}
		return nil
	})
	group.Go(func() error { <-groupCtx.Done(); proxy.Stop(); return nil })
	if err := writer.update(func(status *Status) { status.State = "ready" }); err != nil {
		return err
	}
	if output != nil {
		if _, err := fmt.Fprintf(output, "Managed mesh ready; coordinator %s; local clients share one authenticated connection.\n", target); err != nil {
			return err
		}
	}
	<-groupCtx.Done()
	return nil
}

func waitWorkerRegistration(ctx context.Context, connection grpc.ClientConnInterface, registered <-chan string) error {
	var instanceID string
	select {
	case instanceID = <-registered:
	case <-ctx.Done():
		return ctx.Err()
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	client := pb.NewFleetClient(connection)
	for {
		nodes, err := client.ListNodes(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		for _, view := range nodes.GetNodes() {
			if view.GetInstanceId() == instanceID && view.GetConnected() && view.GetCommandReady() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
