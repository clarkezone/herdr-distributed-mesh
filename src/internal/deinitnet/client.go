// Package deinitnet performs bounded, explicitly requested Tailscale teardown.
//
// The caller owns confirmation, durable pending markers, local state, and the
// provenance of the retained device ID and successful init policy artifacts.
// Device deletion never discovers its target. Policy cleanup lists devices only
// to refuse removal of shared role rules. No local files or mutation retries
// are performed.
// Keep a pending marker until a mutation succeeds or is explicitly reconciled.
//
// The API contract is documented at https://tailscale.com/api (the official
// schema is https://api.tailscale.com/api/v2?outputOpenapiSchema=true).
// Device endpoints accept the stable nodeId (preferred) or the legacy numeric
// id, not a hostname, node public key, or machine public key.
package deinitnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"
)

const (
	apiURL = "https://api.tailscale.com/api/v2"
	// MaxPolicyBytes bounds each input artifact and policy API response.
	MaxPolicyBytes  = 2 << 20
	maxDeviceBytes  = 64 << 10
	defaultTimeout  = 15 * time.Second
	maximumTimeout  = 30 * time.Second
	maxResponseHead = 32 << 10
)

var (
	ErrDeviceAbsent        = errors.New("Tailscale managed device is absent")
	ErrOutcomeUnknown      = errors.New("Tailscale mutation outcome is unknown; retain pending state and reconcile before another mutation")
	ErrPolicyConflict      = errors.New("Tailscale policy differs from the retained snapshots; automatic cleanup refused")
	ErrInvalidPolicy       = errors.New("Tailscale policy artifacts are invalid or do not describe supported init additions")
	ErrPlanUsed            = errors.New("Tailscale policy plan has already been attempted; reconcile before preparing another plan")
	ErrClosed              = errors.New("Tailscale teardown client is closed")
	ErrPolicyInUse         = errors.New("Tailscale mesh roles are still used by other devices; policy cleanup refused")
	ErrAmbiguousMeshPolicy = errors.New("Herdr mesh policy is mixed with unrelated access or contains unsupported references; review it manually")
)

var deviceIDPattern = regexp.MustCompile(`^(?:[0-9]{1,32}|n[A-Za-z0-9]{1,127})$`)

// Options permits an injectable transport, not an alternate API origin.
// An injected transport is trusted and must honor request contexts, avoid
// retries, and never log credentials or request/response bodies.
type Options struct {
	Transport http.RoundTripper
	// Timeout bounds each HTTP exchange, including reading its response body.
	// Zero selects 15 seconds; the maximum is 30 seconds. An operation performs
	// at most three exchanges. A caller context can impose a shorter total bound.
	Timeout time.Duration
}

// Client keeps a private copy of the API token in memory until Close.
// No credentials, policies, raw HTTP errors, or response bodies enter errors.
type Client struct {
	mu     sync.RWMutex
	token  []byte
	http   *http.Client
	closed bool
}

// New constructs a client restricted to https://api.tailscale.com. It performs
// no network requests. The caller remains responsible for clearing its token.
func New(token []byte, options Options) (*Client, error) {
	if len(token) < len("tskey-api-")+1 || len(token) > 4096 ||
		!bytes.HasPrefix(token, []byte("tskey-api-")) {
		return nil, errors.New("a Tailscale API access token is required")
	}
	for _, b := range token[len("tskey-api-"):] {
		if !(b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' || b == '_') {
			return nil, errors.New("invalid Tailscale API access token")
		}
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	if timeout < 0 || timeout > maximumTimeout {
		return nil, errors.New("Tailscale request timeout must be positive and at most 30 seconds")
	}
	transport := options.Transport
	if transport == nil {
		// Do not inherit an application-modified http.DefaultTransport or a
		// proxy from the environment for this credential-bearing client.
		transport = &http.Transport{
			TLSHandshakeTimeout:    timeout,
			ResponseHeaderTimeout:  timeout,
			MaxResponseHeaderBytes: maxResponseHead,
			IdleConnTimeout:        30 * time.Second,
			MaxIdleConnsPerHost:    1,
		}
	}
	return &Client{
		token: bytes.Clone(token),
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (*Client) String() string   { return "deinitnet.Client{redacted}" }
func (*Client) GoString() string { return "deinitnet.Client{redacted}" }

// Close clears the retained token and closes idle connections. Previously
// dispatched requests may still hold temporary HTTP header copies; callers
// should finish or cancel in-flight operations before closing the client.
func (c *Client) Close() {
	c.mu.Lock()
	clear(c.token)
	c.token = nil
	c.closed = true
	c.mu.Unlock()
	c.http.CloseIdleConnections()
}

// HTTPError exposes only an operation and status code, never remote text.
type HTTPError struct {
	Operation  string
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("Tailscale %s failed (HTTP %d)", e.Operation, e.StatusCode)
}

// Device deliberately excludes hostnames: identity must come from retained
// managed state, not name-based discovery. NodeID is the stable node ID.
type Device struct {
	ID     string `json:"id"`
	NodeID string `json:"nodeId"`
}

// GetDevice fetches only the exact retained ID. Only a 404 from this endpoint
// yields ErrDeviceAbsent; other errors, including authorization failures, do not.
func (c *Client) GetDevice(ctx context.Context, retainedID string) (Device, error) {
	if !deviceIDPattern.MatchString(retainedID) {
		return Device{}, errors.New("invalid retained Tailscale device ID")
	}
	r, err := c.request(ctx, http.MethodGet, "/device/"+retainedID, nil, "", maxDeviceBytes, "get device")
	if err != nil {
		return Device{}, err
	}
	if r.status == http.StatusNotFound {
		return Device{}, ErrDeviceAbsent
	}
	if r.status != http.StatusOK {
		return Device{}, &HTTPError{"get device", r.status}
	}
	var device Device
	if json.Unmarshal(r.body, &device) != nil ||
		(device.ID != retainedID && device.NodeID != retainedID) {
		return Device{}, errors.New("Tailscale device response did not verify the retained identity")
	}
	return device, nil
}

// DeleteResult is returned only with a nil error when absence is confirmed.
type DeleteResult struct {
	AlreadyAbsent bool
	Reconciled    bool
}

// DeleteDevice verifies and deletes only retainedID. Confirmation belongs to
// the caller. A failed/uncertain DELETE is never retried: at most one exact GET
// reconciles absence. If that cannot establish absence, ErrOutcomeUnknown
// requires retaining pending state. A later GetDevice can reconcile it.
func (c *Client) DeleteDevice(ctx context.Context, retainedID string) (DeleteResult, error) {
	_, err := c.GetDevice(ctx, retainedID)
	if errors.Is(err, ErrDeviceAbsent) {
		return DeleteResult{AlreadyAbsent: true}, nil
	}
	if err != nil {
		return DeleteResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeleteResult{}, err
	}
	r, err := c.request(ctx, http.MethodDelete, "/device/"+retainedID, nil, "", maxDeviceBytes, "delete device")
	if err == nil && (r.status == http.StatusOK || r.status == http.StatusNoContent) {
		return DeleteResult{}, nil
	}
	if err == nil && definiteRejection(r.status) && r.status != http.StatusNotFound {
		return DeleteResult{}, &HTTPError{"delete device", r.status}
	}
	if errors.Is(err, ErrClosed) {
		return DeleteResult{}, err
	}
	// DELETE 404 is not a documented success response. Confirm with exact GET.
	if _, checkErr := c.GetDevice(ctx, retainedID); errors.Is(checkErr, ErrDeviceAbsent) {
		return DeleteResult{Reconciled: true}, nil
	}
	return DeleteResult{}, ErrOutcomeUnknown
}

func definiteRejection(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusPaymentRequired,
		http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed,
		http.StatusConflict, http.StatusLengthRequired, http.StatusPreconditionFailed,
		http.StatusRequestEntityTooLarge, http.StatusRequestURITooLong,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity, http.StatusTooManyRequests:
		return true
	default:
		// Timeouts and nonstandard statuses (for example a proxy's 499) do
		// not establish whether the server completed a mutation.
		return false
	}
}

type response struct {
	status int
	etag   string
	body   []byte
}

func (c *Client) request(ctx context.Context, method, path string, body []byte, etag string, limit int64, operation string) (response, error) {
	if err := ctx.Err(); err != nil {
		return response{}, err
	}
	// Wrapping the reader suppresses GetBody: net/http must not replay a
	// policy POST. No retry or idempotency-key middleware is installed.
	var reader io.Reader
	if body != nil {
		reader = struct{ io.Reader }{bytes.NewReader(body)}
	}
	req, err := http.NewRequestWithContext(ctx, method, apiURL+path, reader)
	if err != nil {
		return response{}, errors.New("cannot construct Tailscale request")
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return response{}, ErrClosed
	}
	req.Header.Set("Authorization", "Bearer "+string(c.token))
	c.mu.RUnlock()
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	res, err := c.http.Do(req)
	if err != nil {
		// Transport errors can contain URLs, credentials, or raw remote text.
		return response{}, errors.New("Tailscale " + operation + " transport failed")
	}
	defer res.Body.Close()
	result := response{status: res.StatusCode, etag: res.Header.Get("ETag")}
	// Error payloads are untrusted and unnecessary, even for a 404.
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
		return result, nil
	}
	result.body, err = io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return response{}, errors.New("Tailscale " + operation + " response could not be read")
	}
	if int64(len(result.body)) > limit {
		return response{}, errors.New("Tailscale " + operation + " response exceeds the supported size")
	}
	return result, nil
}
