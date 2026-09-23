package dashboard

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestHostedOriginValidation(t *testing.T) {
	for _, origin := range []string{"http://mesh.example:8787", "https://mesh.example", "http://[::1]:8787"} {
		if err := ValidateHostedOrigin(origin); err != nil {
			t.Fatalf("valid origin rejected: %v", err)
		}
	}
	for _, origin := range []string{
		"", "mesh.example", "file://mesh.example", "http://", "http://user:password@mesh.example",
		"http://mesh.example/", "http://mesh.example?q=1", "http://mesh.example?",
		"http://mesh.example#fragment", "http://mesh.example#", "http://mesh.example:",
		"http://mesh.example:0", "http://mesh.example:65536", "http://mesh.example:bad",
		"http://mesh.example\r\nInjected: yes",
	} {
		if err := ValidateHostedOrigin(origin); err == nil {
			t.Fatalf("invalid origin accepted: %q", origin)
		}
	}
}

func TestHostedResourcesRequireConnectionPeer(t *testing.T) {
	var queries atomic.Int32
	reader := fakeClient{func(context.Context) (*pb.NodeList, error) {
		queries.Add(1)
		return &pb.NodeList{}, nil
	}}
	if _, err := NewHostedHandler(reader, "https://mesh.example", nil); err == nil {
		t.Fatal("hosted handler accepted missing peer authentication")
	}
	for _, allowed := range []bool{false, true} {
		h, err := NewHostedHandler(reader, "https://mesh.example", func(ctx context.Context, address string) error {
			if address != "100.64.0.7:1234" {
				t.Errorf("authenticated header instead of socket peer: %q", address)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Error("peer identification has no deadline")
			}
			if !allowed {
				return errors.New("PRIVATE identity details")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/", "/app.js", "/model.mjs", "/favicon.ico", "/api/nodes"} {
			r := httptest.NewRequest("GET", "https://mesh.example"+path, nil)
			r.RemoteAddr = "100.64.0.7:1234"
			r.Header.Set("Origin", "https://mesh.example")
			r.Header.Set("X-Herdr-Dashboard", "1")
			r.Header.Set("X-Forwarded-For", "100.64.0.8")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if !allowed && w.Code != http.StatusForbidden {
				t.Fatalf("unauthenticated resource exposed: %s: %d", path, w.Code)
			}
			if allowed && w.Code != http.StatusOK && w.Code != http.StatusNoContent {
				t.Fatalf("trusted resource denied: %s: %d", path, w.Code)
			}
			if strings.Contains(w.Body.String(), "PRIVATE") {
				t.Fatal("peer error details leaked")
			}
		}
	}
	if queries.Load() != 1 {
		t.Fatalf("untrusted peer queried inventory: %d queries", queries.Load())
	}
}

func TestHostedOriginStillRejectsCrossOrigin(t *testing.T) {
	h, err := NewHostedHandler(fakeClient{}, "https://mesh.example", func(context.Context, string) error {
		t.Fatal("invalid origin reached peer lookup")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Host = "other.example" },
		func(r *http.Request) { r.Header.Set("Origin", "http://mesh.example") },
		func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
	} {
		r := httptest.NewRequest("GET", "https://mesh.example/", nil)
		mutate(r)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Fatalf("cross-origin request accepted: %d", w.Code)
		}
	}
}

func TestHostedPeerLookupIsBounded(t *testing.T) {
	value, err := NewHostedHandler(fakeClient{}, "http://mesh.example", func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	h := value.(*handler)
	h.timeout = time.Millisecond
	r := httptest.NewRequest("GET", "http://mesh.example/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("lookup timeout not surfaced: %d", w.Code)
	}
	for range cap(h.authSlots) {
		h.authSlots <- struct{}{}
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("lookup overload not bounded: %d", w.Code)
	}
}

func TestServeHostedReusesInventoryAndShutsDown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	origin := "http://" + listener.Addr().String()
	go func() {
		done <- ServeHosted(ctx, listener, fakeClient{func(context.Context) (*pb.NodeList, error) {
			return &pb.NodeList{}, nil
		}}, HostedOptions{Origin: origin, Output: io.Discard, AuthorizePeer: func(context.Context, string) error { return nil }})
	}()
	r, err := http.NewRequest("GET", origin+"/api/nodes", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Herdr-Dashboard", "1")
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("hosted inventory unavailable: %d", response.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("hosted dashboard did not shut down")
	}
}
