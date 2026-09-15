package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

//go:embed web
var assets embed.FS

const (
	requestTimeout   = 5 * time.Second
	maxResponseBytes = 8 * 1024 * 1024
)

type Options struct {
	ListenAddress string
	ServerAddress string
	Transport     transport.Config
	Output        io.Writer
}

type fleetClient interface {
	ListNodes(context.Context, *emptypb.Empty, ...grpc.CallOption) (*agentflowv1.NodeList, error)
}

func ValidateListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("-listen must be a loopback IP and port, such as 127.0.0.1:8787")
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsLoopback() || ip.Zone() != "" {
		return errors.New("-listen must use a literal loopback IP; remote dashboard access is not supported")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return errors.New("-listen requires a port from 0 through 65535")
	}
	return nil
}

func Run(ctx context.Context, options Options) error {
	if strings.TrimSpace(options.ServerAddress) == "" {
		return errors.New("-server is required")
	}
	if err := ValidateListenAddress(options.ListenAddress); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", options.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for dashboard: %w", err)
	}
	defer listener.Close()

	startup, cancel := context.WithTimeout(ctx, time.Minute)
	network, err := transport.Start(startup, options.Transport)
	cancel()
	if err != nil {
		return err
	}
	defer network.Close()
	if err := network.SelfStatus().Validate(options.Transport.Tags, time.Now()); err != nil {
		return fmt.Errorf("validate dashboard tsnet identity: %w", err)
	}
	connection, err := network.DialGRPC(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()
	return serve(ctx, listener, agentflowv1.NewFleetClient(connection), options.Output)
}

func serve(ctx context.Context, listener net.Listener, client fleetClient, output io.Writer) error {
	files, err := fs.Sub(assets, "web")
	if err != nil {
		return fmt.Errorf("open dashboard assets: %w", err)
	}
	host := listener.Addr().String()
	server := &http.Server{
		Handler:           newHandler(client, host, files),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	if _, err := fmt.Fprintf(output, "dashboard ready: http://%s (read-only; keep this process running)\n", host); err != nil {
		return fmt.Errorf("write dashboard address: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			err = errors.Join(err, server.Close())
		}
		serveErr := <-done
		if !errors.Is(serveErr, http.ErrServerClosed) {
			err = errors.Join(err, serveErr)
		}
		return err
	case err := <-done:
		return fmt.Errorf("serve dashboard: %w", err)
	}
}

type handler struct {
	client  fleetClient
	host    string
	files   fs.FS
	slots   chan struct{}
	timeout time.Duration
}

func newHandler(client fleetClient, host string, files fs.FS) *handler {
	return &handler{client: client, host: host, files: files, slots: make(chan struct{}, 4), timeout: requestTimeout}
}

var assetName = regexp.MustCompile(`^[A-Za-z0-9_-]+\.(css|js|mjs)$`)

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

	// Exact Host + Origin checks prevent an unrelated website from using this
	// trusted-local gateway through DNS rebinding or cross-origin requests.
	if r.Host != h.host || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != "http://"+h.host) ||
		r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		writeError(w, http.StatusForbidden, "authorization_denied", "This dashboard accepts same-origin loopback requests only.")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "read_only", "The dashboard is read-only.")
		return
	}
	if r.ContentLength != 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request bodies are not accepted.")
		return
	}
	if r.URL.Path == "/favicon.ico" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path == "/api/nodes" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, "read_only", "Use GET to read fleet state.")
			return
		}
		// A custom header forces cross-origin browser requests through a denied
		// preflight. It is not a credential or a local-process isolation boundary.
		if r.Header.Get("X-Herdr-Dashboard") != "1" {
			writeError(w, http.StatusForbidden, "authorization_denied", "Open the dashboard page to read fleet state.")
			return
		}
		h.nodes(w, r)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/") {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	if r.URL.Path == "/" {
		name = "index.html"
	} else if !assetName.MatchString(name) {
		http.NotFound(w, r)
		return
	}
	data, err := fs.ReadFile(h.files, name)
	if errors.Is(err, fs.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		log.Printf("dashboard asset unavailable")
		writeError(w, http.StatusInternalServerError, "internal_error", "Dashboard assets could not be loaded.")
		return
	}
	switch {
	case name == "index.html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case strings.HasSuffix(name, ".css"):
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method != http.MethodHead {
		if _, err := w.Write(data); err != nil {
			log.Printf("dashboard response write failed")
		}
	}
}

func (h *handler) nodes(w http.ResponseWriter, r *http.Request) {
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		writeError(w, http.StatusTooManyRequests, "busy", "Too many dashboard requests; retry shortly.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	list, err := h.client.ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		httpCode, category, message := queryError(err)
		log.Printf("dashboard query failed: %s", category)
		writeError(w, httpCode, category, message)
		return
	}
	if list == nil {
		writeError(w, http.StatusBadGateway, "invalid_response", "The mesh server returned an invalid response.")
		return
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(list)
	if err != nil {
		log.Printf("dashboard response encoding failed")
		writeError(w, http.StatusBadGateway, "invalid_response", "The mesh server returned an invalid response.")
		return
	}
	if len(data) > maxResponseBytes {
		writeError(w, http.StatusBadGateway, "invalid_response", "Fleet state exceeds the dashboard response limit.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(data); err != nil {
		log.Printf("dashboard response write failed")
	}
}

func queryError(err error) (int, string, string) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return http.StatusGatewayTimeout, "request_timeout", "The mesh server did not respond in time. Retrying automatically."
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated:
		return http.StatusForbidden, "authorization_denied", "The dashboard identity is not authorized to read this fleet."
	case codes.DeadlineExceeded, codes.Canceled:
		return http.StatusGatewayTimeout, "request_timeout", "The mesh server did not respond in time. Retrying automatically."
	case codes.Unimplemented, codes.FailedPrecondition:
		return http.StatusBadGateway, "incompatible_server", "The mesh server does not support this read-only API. Upgrade the server."
	default:
		return http.StatusServiceUnavailable, "server_unavailable", "The mesh server is unavailable. Retrying automatically."
	}
}

func writeError(w http.ResponseWriter, code int, category, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	body := struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{}
	body.Error.Code, body.Error.Message = category, message
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("dashboard error response write failed")
	}
}
