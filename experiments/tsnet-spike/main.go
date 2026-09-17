package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"tailscale.com/tsnet"
)

const defaultPort = "50051"

type commonOptions struct {
	authKeyEnv string
	debug      bool
	hostname   string
	stateDir   string
	tags       string
}

type pingServer struct{}

func (pingServer) Ping(stream pingServicePingServer) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		message, err := decodeMessage(request)
		if err != nil {
			return fmt.Errorf("decode ping: %w", err)
		}
		log.Printf("received ping id=%s text=%q", message.ID, message.Text)

		response, err := encodeMessage(pingMessage{
			ID:     message.ID,
			Kind:   "pong",
			SentAt: time.Now().UTC(),
			Text:   message.Text,
		})
		if err != nil {
			return fmt.Errorf("encode pong: %w", err)
		}
		if err := stream.Send(response); err != nil {
			return fmt.Errorf("send pong: %w", err)
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var err error
	switch os.Args[1] {
	case "server":
		err = runServerCommand(ctx, os.Args[2:])
	case "client":
		err = runClientCommand(ctx, os.Args[2:], os.Stdin, os.Stdout)
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func runServerCommand(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	options := addCommonFlags(flags, "herdr-mesh-spike-server", "server", "TS_AUTHKEY_SERVER")
	listenAddress := flags.String("listen", ":"+defaultPort, "tsnet TCP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}

	tailnet, err := startTSNet(ctx, options)
	if err != nil {
		return err
	}
	defer tailnet.Close()

	listener, err := tailnet.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen on tsnet address %q: %w", *listenAddress, err)
	}
	defer listener.Close()

	server := grpc.NewServer()
	registerPingServiceServer(server, pingServer{})
	log.Printf("gRPC server ready hostname=%s address=%s", options.hostname, listener.Addr())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Printf("stopping gRPC server")
		server.Stop()
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

func runClientCommand(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	options := addCommonFlags(flags, "herdr-mesh-spike-client", "client", "TS_AUTHKEY_CLIENT")
	serverAddress := flags.String("server", "", "server MagicDNS name or tailnet IP with port")
	retryDelay := flags.Duration("retry-delay", 2*time.Second, "delay before reopening a failed stream")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*serverAddress) == "" {
		return errors.New("-server is required, for example herdr-mesh-spike-server.example.ts.net:50051")
	}

	tailnet, err := startTSNet(ctx, options)
	if err != nil {
		return err
	}
	defer tailnet.Close()

	target := "passthrough:///" + *serverAddress
	connection, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dialContext context.Context, address string) (net.Conn, error) {
			log.Printf("dialing server address=%s", address)
			return tailnet.Dial(dialContext, "tcp", address)
		}),
	)
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer connection.Close()

	client := newPingServiceClient(connection)
	session := pingSession{client: client}
	defer session.close()
	scanner := bufio.NewScanner(input)
	fmt.Fprintln(output, "Enter text to ping. Press Ctrl+C or Ctrl+Z then Enter to exit.")

	var sequence uint64
	for {
		fmt.Fprint(output, "> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read input: %w", err)
			}
			return nil
		}

		sequence++
		message := pingMessage{
			ID:     fmt.Sprintf("%s-%d-%d", options.hostname, time.Now().UnixNano(), sequence),
			Kind:   "ping",
			SentAt: time.Now().UTC(),
			Text:   scanner.Text(),
		}
		if err := session.exchangeWithRetry(ctx, message, *retryDelay, output); err != nil {
			return err
		}
	}
}

type pingSession struct {
	client pingServiceClient
	stream pingServicePingClient
}

func (session *pingSession) exchangeWithRetry(
	ctx context.Context,
	message pingMessage,
	retryDelay time.Duration,
	output io.Writer,
) error {
	for attempt := 1; ; attempt++ {
		if session.stream == nil {
			stream, err := session.client.Ping(ctx)
			if err != nil {
				if err := waitToRetry(ctx, attempt, retryDelay, err); err != nil {
					return err
				}
				continue
			}
			session.stream = stream
			log.Printf("gRPC ping stream opened")
		}

		err := func() error {
			request, encodeErr := encodeMessage(message)
			if encodeErr != nil {
				return fmt.Errorf("encode ping: %w", encodeErr)
			}
			if err := session.stream.Send(request); err != nil {
				return fmt.Errorf("send ping: %w", err)
			}
			response, err := receivePong(session.stream, message.ID)
			if err != nil {
				return err
			}
			fmt.Fprintf(output, "pong id=%s text=%q at=%s\n", response.ID, response.Text, response.SentAt.Format(time.RFC3339Nano))
			return nil
		}()
		if err == nil {
			return nil
		}

		session.close()
		if err := waitToRetry(ctx, attempt, retryDelay, err); err != nil {
			return err
		}
	}
}

func (session *pingSession) close() {
	if session.stream == nil {
		return
	}
	_ = session.stream.CloseSend()
	session.stream = nil
	log.Printf("gRPC ping stream closed")
}

func waitToRetry(ctx context.Context, attempt int, retryDelay time.Duration, cause error) error {
	log.Printf("ping attempt=%d failed: %v; retrying in %s", attempt, cause, retryDelay)
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func receivePong(stream pingServicePingClient, requestID string) (pingMessage, error) {
	response, err := stream.Recv()
	if err != nil {
		return pingMessage{}, fmt.Errorf("receive pong: %w", err)
	}
	message, err := decodeMessage(response)
	if err != nil {
		return pingMessage{}, fmt.Errorf("decode pong: %w", err)
	}
	if message.Kind != "pong" {
		return pingMessage{}, fmt.Errorf("unexpected message kind %q", message.Kind)
	}
	if message.ID != requestID {
		return pingMessage{}, fmt.Errorf("pong id %q does not match ping id %q", message.ID, requestID)
	}
	return message, nil
}

func addCommonFlags(flags *flag.FlagSet, defaultHostname, stateName, defaultAuthKeyEnv string) commonOptions {
	defaultStateDir := filepath.Join(defaultConfigDir(), stateName)
	var options commonOptions
	flags.StringVar(&options.authKeyEnv, "auth-key-env", defaultAuthKeyEnv, "environment variable containing a Tailscale auth key")
	flags.BoolVar(&options.debug, "debug", false, "enable verbose tsnet logs")
	flags.StringVar(&options.hostname, "hostname", defaultHostname, "Tailscale hostname for this embedded node")
	flags.StringVar(&options.stateDir, "state-dir", defaultStateDir, "directory for persistent tsnet identity")
	flags.StringVar(&options.tags, "tags", "", "comma-separated Tailscale tags to advertise")
	return options
}

func defaultConfigDir() string {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return ".tsnet-spike"
	}
	return filepath.Join(configDir, "herdr-distributed-mesh", "tsnet-spike")
}

func startTSNet(ctx context.Context, options commonOptions) (*tsnet.Server, error) {
	if err := os.MkdirAll(options.stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create state directory %q: %w", options.stateDir, err)
	}

	server := &tsnet.Server{
		AuthKey:       os.Getenv(options.authKeyEnv),
		Dir:           options.stateDir,
		Hostname:      options.hostname,
		AdvertiseTags: splitTags(options.tags),
		UserLogf:      log.Printf,
	}
	if options.debug {
		server.Logf = log.Printf
	}

	log.Printf("starting tsnet hostname=%s state_dir=%s auth_key_present=%t", options.hostname, options.stateDir, server.AuthKey != "")
	if _, err := server.Up(ctx); err != nil {
		server.Close()
		return nil, fmt.Errorf("bring up tsnet node: %w", err)
	}

	ipv4, ipv6 := server.TailscaleIPs()
	log.Printf("tsnet ready hostname=%s ipv4=%s ipv6=%s", options.hostname, ipv4, ipv6)
	return server, nil
}

func splitTags(value string) []string {
	var tags []string
	for _, tag := range strings.Split(value, ",") {
		if tag = strings.TrimSpace(tag); tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Windows tsnet/gRPC feasibility spike

Usage:
  spike server [flags]
  spike client -server <magic-dns-name-or-tailnet-ip:port> [flags]

Run "spike server -h" or "spike client -h" for mode-specific flags.`)
}
