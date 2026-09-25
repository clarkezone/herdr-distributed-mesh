package deinitnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testToken  = "tskey-" + "api-TEST-DO-NOT-USE"
	testID     = "n292kg92CNTRL"
	deviceJSON = `{"id":"92960230385","nodeId":"n292kg92CNTRL","hostname":"not-used"}`
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type exchange struct {
	method string
	path   string
	status int
	body   string
	etag   string
	err    error
	check  func(*testing.T, *http.Request)
}

func scriptedClient(t *testing.T, exchanges ...exchange) *Client {
	t.Helper()
	index := 0
	var mu sync.Mutex
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(exchanges) {
			t.Errorf("unexpected extra request: %s %s", r.Method, r.URL.Path)
			return nil, errors.New("unexpected request")
		}
		e := exchanges[index]
		index++
		if r.Method != e.method || r.URL.Path != "/api/v2"+e.path {
			t.Errorf("request %d: got %s %s; want %s /api/v2%s", index, r.Method, r.URL.Path, e.method, e.path)
		}
		if r.URL.Scheme != "https" || r.URL.Host != "api.tailscale.com" || r.URL.RawQuery != "" {
			t.Errorf("unsafe destination: %s", r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("missing bearer authentication")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Error("missing policy JSON negotiation")
		}
		if e.check != nil {
			e.check(t, r)
		}
		if e.err != nil {
			return nil, e.err
		}
		header := make(http.Header)
		if e.etag != "" {
			header.Set("ETag", e.etag)
		}
		return &http.Response{StatusCode: e.status, Header: header, Body: io.NopCloser(strings.NewReader(e.body)), Request: r}, nil
	})
	client, err := New([]byte(testToken), Options{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		mu.Lock()
		defer mu.Unlock()
		if index != len(exchanges) {
			t.Errorf("performed %d requests; want %d", index, len(exchanges))
		}
	})
	return client
}

func getDevice(status int, body string) exchange {
	return exchange{method: http.MethodGet, path: "/device/" + testID, status: status, body: body}
}

func deleteDevice(status int) exchange {
	return exchange{method: http.MethodDelete, path: "/device/" + testID, status: status}
}

func TestGetDeviceExactIdentity(t *testing.T) {
	for _, id := range []string{testID, "92960230385"} {
		t.Run(id, func(t *testing.T) {
			c := scriptedClient(t, exchange{method: http.MethodGet, path: "/device/" + id, status: 200, body: deviceJSON})
			device, err := c.GetDevice(t.Context(), id)
			if err != nil || device.NodeID != testID || device.ID != "92960230385" {
				t.Fatalf("GetDevice = %+v, %v", device, err)
			}
		})
	}
}

func TestRejectNonRetainedIdentifiersBeforeNetwork(t *testing.T) {
	c := scriptedClient(t)
	for _, id := range []string{"", "host.tailnet.ts.net", "server", "nodekey:abc", "mkey:abc", "../devices", testID + "?fields=all", "n/other", "123\n", strings.Repeat("1", 129)} {
		t.Run(id, func(t *testing.T) {
			if _, err := c.DeleteDevice(t.Context(), id); err == nil {
				t.Fatal("accepted invalid device ID")
			}
		})
	}
}

func TestGetDeviceRejectsWrongOrMalformedIdentity(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"id":"111","nodeId":"nOTHER"}`, `{"nodeId":1}`, deviceJSON + `{}`, `SECRET-not-json`} {
		t.Run(body, func(t *testing.T) {
			c := scriptedClient(t, getDevice(200, body))
			if _, err := c.DeleteDevice(t.Context(), testID); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("expected sanitized identity error: %v", err)
			}
		})
	}
}

func TestOnlyDeviceGET404IsAbsent(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 408, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c := scriptedClient(t, getDevice(status, "SECRET-"+testToken))
			_, err := c.GetDevice(t.Context(), testID)
			if errors.Is(err, ErrDeviceAbsent) != (status == 404) {
				t.Fatalf("status %d: %v", status, err)
			}
			if err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), testToken) {
				t.Fatalf("unsanitized/missing error: %v", err)
			}
		})
	}
}

func TestDeleteDevice(t *testing.T) {
	tests := []struct {
		name       string
		exchanges  []exchange
		want       DeleteResult
		wantErr    error
		wantStatus int
	}{
		{"already absent", []exchange{getDevice(404, "")}, DeleteResult{AlreadyAbsent: true}, nil, 0},
		{"success", []exchange{getDevice(200, deviceJSON), deleteDevice(200)}, DeleteResult{}, nil, 0},
		{"no content", []exchange{getDevice(200, deviceJSON), deleteDevice(204)}, DeleteResult{}, nil, 0},
		{"forbidden", []exchange{getDevice(200, deviceJSON), deleteDevice(403)}, DeleteResult{}, nil, 403},
		{"delete 404 confirmed", []exchange{getDevice(200, deviceJSON), deleteDevice(404), getDevice(404, "")}, DeleteResult{Reconciled: true}, nil, 0},
		{"delete 404 not proof", []exchange{getDevice(200, deviceJSON), deleteDevice(404), getDevice(200, deviceJSON)}, DeleteResult{}, ErrOutcomeUnknown, 0},
		{"server error confirmed", []exchange{getDevice(200, deviceJSON), deleteDevice(500), getDevice(404, "")}, DeleteResult{Reconciled: true}, nil, 0},
		{"server error present", []exchange{getDevice(200, deviceJSON), deleteDevice(500), getDevice(200, deviceJSON)}, DeleteResult{}, ErrOutcomeUnknown, 0},
		{"server error cannot read", []exchange{getDevice(200, deviceJSON), deleteDevice(504), getDevice(403, "")}, DeleteResult{}, ErrOutcomeUnknown, 0},
		{"timeout response", []exchange{getDevice(200, deviceJSON), deleteDevice(408), getDevice(404, "")}, DeleteResult{Reconciled: true}, nil, 0},
		{"unknown client status", []exchange{getDevice(200, deviceJSON), deleteDevice(499), getDevice(404, "")}, DeleteResult{Reconciled: true}, nil, 0},
		{"accepted not finished", []exchange{getDevice(200, deviceJSON), deleteDevice(202), getDevice(200, deviceJSON)}, DeleteResult{}, ErrOutcomeUnknown, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := scriptedClient(t, tt.exchanges...)
			got, err := c.DeleteDevice(t.Context(), testID)
			if tt.wantStatus != 0 {
				var status *HTTPError
				if !errors.As(err, &status) || status.StatusCode != tt.wantStatus {
					t.Fatalf("expected HTTP %d: %v", tt.wantStatus, err)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v; want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("result = %+v; want %+v", got, tt.want)
			}
		})
	}
}

func TestDeleteLostResponseDoesNotRetry(t *testing.T) {
	for _, absent := range []bool{true, false} {
		t.Run(fmt.Sprint(absent), func(t *testing.T) {
			mutation := deleteDevice(0)
			mutation.err = errors.New("SECRET " + testToken)
			reconcile := getDevice(200, deviceJSON)
			if absent {
				reconcile = getDevice(404, "")
			}
			c := scriptedClient(t, getDevice(200, deviceJSON), mutation, reconcile)
			got, err := c.DeleteDevice(t.Context(), testID)
			if absent {
				if err != nil || !got.Reconciled {
					t.Fatalf("expected reconciliation: %+v %v", got, err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("expected unknown: %v", err)
			}
		})
	}
}

func TestRequestBoundsAndSanitization(t *testing.T) {
	c := scriptedClient(t, getDevice(200, strings.Repeat("x", maxDeviceBytes+1)))
	if _, err := c.GetDevice(t.Context(), testID); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("expected bounded device response: %v", err)
	}
	c = scriptedClient(t, exchange{method: http.MethodGet, path: "/device/" + testID, err: errors.New("SECRET " + testToken)})
	if _, err := c.GetDevice(t.Context(), testID); err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), testToken) {
		t.Fatalf("expected sanitized transport error: %v", err)
	}
}

func TestDeviceResponseExactSizeBoundary(t *testing.T) {
	body := deviceJSON + strings.Repeat(" ", maxDeviceBytes-len(deviceJSON))
	c := scriptedClient(t, getDevice(200, body))
	if _, err := c.GetDevice(t.Context(), testID); err != nil {
		t.Fatalf("exactly maximum-sized response rejected: %v", err)
	}
}

type failingBody struct{ closed bool }

func (*failingBody) Read([]byte) (int, error) { return 0, errors.New("SECRET " + testToken) }
func (b *failingBody) Close() error {
	b.closed = true
	return nil
}

func TestResponseReadErrorsAreSanitizedAndClosed(t *testing.T) {
	body := &failingBody{}
	c, err := New([]byte(testToken), Options{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: r}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.GetDevice(t.Context(), testID); err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), testToken) {
		t.Fatalf("read error was not sanitized: %v", err)
	}
	if !body.closed {
		t.Fatal("response body was not closed")
	}
}

func TestDeleteCancellationDuringMutationIsUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	mutation := deleteDevice(0)
	mutation.check = func(*testing.T, *http.Request) { cancel() }
	mutation.err = context.Canceled
	c := scriptedClient(t, getDevice(200, deviceJSON), mutation, getDevice(404, ""))
	if _, err := c.DeleteDevice(ctx, testID); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("canceled mutation did not retain unknown outcome: %v", err)
	}
	if _, err := c.GetDevice(t.Context(), testID); !errors.Is(err, ErrDeviceAbsent) {
		t.Fatalf("fresh context could not reconcile absence: %v", err)
	}
}

func TestRedirectNeverFollowed(t *testing.T) {
	for _, target := range []string{"https://evil.invalid/steal", apiURL + "/device/nOTHER"} {
		t.Run(target, func(t *testing.T) {
			calls := 0
			c, err := New([]byte(testToken), Options{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{
					StatusCode: 307, Header: http.Header{"Location": []string{target}},
					Body: io.NopCloser(strings.NewReader("")), Request: r,
				}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_, err = c.GetDevice(t.Context(), testID)
			if err == nil || calls != 1 || strings.Contains(err.Error(), target) {
				t.Fatalf("redirect handling: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestNewCloseAndSafeFormatting(t *testing.T) {
	for _, token := range []string{"", "tskey-" + "auth-test", "tskey-" + "client-test", "tskey-api-", "tskey-api-x\r\nx: y", "tskey-api-x space"} {
		if c, err := New([]byte(token), Options{}); c != nil || err == nil {
			t.Fatal("accepted invalid token")
		}
	}
	for _, timeout := range []time.Duration{-1, maximumTimeout + 1} {
		if c, err := New([]byte(testToken), Options{Timeout: timeout}); c != nil || err == nil {
			t.Fatal("accepted unbounded timeout")
		}
	}
	token := []byte(testToken)
	c, err := New(token, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if c.http.Timeout != defaultTimeout {
		t.Fatal("missing bounded default")
	}
	transport, ok := c.http.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.MaxResponseHeaderBytes != maxResponseHead {
		t.Fatal("unexpected default transport")
	}
	clear(token)
	if string(c.token) != testToken {
		t.Fatal("client did not clone token")
	}
	if text := fmt.Sprintf("%v %+v %#v", c, c, c); strings.Contains(text, testToken) {
		t.Fatal("client formatting disclosed token")
	}
	saved := c.token
	c.Close()
	c.Close()
	for _, b := range saved {
		if b != 0 {
			t.Fatal("Close did not clear retained token")
		}
	}
	if _, err := c.GetDevice(t.Context(), testID); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed client: %v", err)
	}
}

func TestCancellationBeforeMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	e := getDevice(200, deviceJSON)
	e.check = func(*testing.T, *http.Request) { cancel() }
	c := scriptedClient(t, e)
	if _, err := c.DeleteDevice(ctx, testID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled before deletion: %v", err)
	}
	c = scriptedClient(t)
	if _, err := c.GetDevice(ctx, testID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled before request: %v", err)
	}
}

func TestTimeoutIncludesResponseBody(t *testing.T) {
	// The only socket in package tests targets this loopback server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	base := &http.Transport{}
	defer base.CloseIdleConnections()
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.URL.Scheme, copy.URL.Host = target.Scheme, target.Host
		return base.RoundTrip(copy)
	})
	c, err := New([]byte(testToken), Options{Transport: transport, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	if _, err := c.GetDevice(t.Context(), testID); err == nil {
		t.Fatal("expected body timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout was not bounded: %s", elapsed)
	}
}
