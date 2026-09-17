package dashboard

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type FleetReader = fleetClient

// PeerAuthorizer authenticates the connection's actual remote address.
// Hosted callers must use the owning tsnet network, never forwarded headers.
type PeerAuthorizer func(context.Context, string) error

type HostedOptions struct {
	Origin        string
	AuthorizePeer PeerAuthorizer
	Output        io.Writer
}

func ValidateHostedOrigin(origin string) error {
	u, err := url.Parse(origin)
	if err != nil || len(origin) > 512 || strings.ContainsAny(origin, " \t\r\n") ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.Opaque != "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery ||
		u.Fragment != "" || strings.Contains(origin, "#") || strings.HasSuffix(u.Host, ":") {
		return errors.New("dashboard origin requires an explicit HTTP(S) authority without credentials, path, query or fragment")
	}
	if port := u.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return errors.New("dashboard origin requires a port from 1 through 65535")
		}
	}
	return nil
}

// NewHostedHandler shares the loopback dashboard's assets and read-only API.
// Every resource requires peer authentication, including static assets.
func NewHostedHandler(client FleetReader, origin string, authorize PeerAuthorizer) (http.Handler, error) {
	if client == nil || authorize == nil {
		return nil, errors.New("hosted dashboard requires a fleet reader and peer authentication")
	}
	if err := ValidateHostedOrigin(origin); err != nil {
		return nil, err
	}
	files, err := fs.Sub(assets, "web")
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	h := newHandler(client, u.Host, files)
	h.origin = origin
	h.authorize = authorize
	h.authSlots = make(chan struct{}, 16)
	return h, nil
}

// ServeHosted owns the supplied listener until shutdown. The coordinator must
// supply a tsnet listener (TLS when using an HTTPS origin), not a host listener.
func ServeHosted(ctx context.Context, listener net.Listener, client FleetReader, options HostedOptions) error {
	if listener == nil || options.Output == nil {
		return errors.New("hosted dashboard requires a listener and output")
	}
	defer listener.Close()
	handler, err := NewHostedHandler(client, options.Origin, options.AuthorizePeer)
	if err != nil {
		return err
	}
	return serveHandler(ctx, listener, handler, options.Output, options.Origin)
}

func (h *handler) authorizeRequest(w http.ResponseWriter, r *http.Request) bool {
	if h.authorize == nil {
		return true
	}
	select {
	case h.authSlots <- struct{}{}:
		defer func() { <-h.authSlots }()
	default:
		writeError(w, http.StatusTooManyRequests, "busy", "Too many dashboard requests; retry shortly.")
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	err := h.authorize(ctx, r.RemoteAddr)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		log.Printf("dashboard peer identification unavailable")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "Dashboard peer identification is unavailable.")
		return false
	}
	log.Printf("dashboard peer denied")
	writeError(w, http.StatusForbidden, "authorization_denied", "This dashboard requires a trusted controller peer.")
	return false
}
