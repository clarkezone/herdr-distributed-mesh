package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeClient struct {
	list func(context.Context) (*agentflowv1.NodeList, error)
}

func (f fakeClient) ListNodes(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*agentflowv1.NodeList, error) {
	return f.list(ctx)
}

func testHandler(client fleetClient) *handler {
	return newHandler(client, "127.0.0.1:8787", fstest.MapFS{
		"index.html":     &fstest.MapFile{Data: []byte("<!doctype html><title>Herdr Mesh</title>")},
		"app.js":         &fstest.MapFile{Data: []byte("export const ready = true;")},
		"model.mjs":      &fstest.MapFile{Data: []byte("export const ready = true;")},
		"styles.css":     &fstest.MapFile{Data: []byte("body { color: white; }")},
		"model.test.mjs": &fstest.MapFile{Data: []byte("not an asset")},
	})
}

func apiRequest() *http.Request {
	r := httptest.NewRequest("GET", "http://127.0.0.1:8787/api/nodes", nil)
	r.Header.Set("X-Herdr-Dashboard", "1")
	return r
}

func TestValidateListenAddress(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8787", "[::1]:8787", "127.0.0.1:0", "127.0.0.2:1234"} {
		if err := ValidateListenAddress(address); err != nil {
			t.Fatalf("%s rejected: %v", address, err)
		}
	}
	for _, address := range []string{"", ":8787", "0.0.0.0:8787", "[::]:8787", "localhost:8787", "100.1.2.3:8787",
		"example.com:8787", "127.0.0.1", "127.0.0.1:http", "127.0.0.1:-1", "127.0.0.1:65536", "[::1%eth0]:8787"} {
		if err := ValidateListenAddress(address); err == nil {
			t.Fatalf("unsafe address %s accepted", address)
		}
	}
}

func TestOriginAndMethodBoundary(t *testing.T) {
	var calls atomic.Int32
	h := testHandler(fakeClient{func(context.Context) (*agentflowv1.NodeList, error) {
		calls.Add(1)
		return &agentflowv1.NodeList{}, nil
	}})
	tests := []struct {
		name   string
		modify func(*http.Request)
		code   int
	}{
		{"host rebinding", func(r *http.Request) { r.Host = "attacker.example:8787" }, 403},
		{"wrong port", func(r *http.Request) { r.Host = "127.0.0.1:8788" }, 403},
		{"cross origin", func(r *http.Request) { r.Header.Set("Origin", "https://attacker.example") }, 403},
		{"null origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, 403},
		{"cross site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, 403},
		{"missing header", func(r *http.Request) { r.Header.Del("X-Herdr-Dashboard") }, 403},
		{"preflight", func(r *http.Request) { r.Method = "OPTIONS" }, 405},
		{"mutation", func(r *http.Request) { r.Method = "POST" }, 405},
		{"head API", func(r *http.Request) { r.Method = "HEAD" }, 405},
		{"body", func(r *http.Request) { r.ContentLength = 1 }, 400},
		{"chunked body", func(r *http.Request) { r.ContentLength = -1 }, 400},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := apiRequest()
			test.modify(r)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.code {
				t.Fatalf("status=%d, want %d", w.Code, test.code)
			}
			if w.Header().Get("Access-Control-Allow-Origin") != "" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("unsafe cache or CORS policy")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("rejected requests reached fleet RPC")
	}
	r := apiRequest()
	r.Header.Set("Origin", "http://127.0.0.1:8787")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"nodes":[]}` {
		t.Fatalf("valid same-origin request failed: %d %s", w.Code, w.Body.String())
	}
}

func TestAssetBoundaryAndHeaders(t *testing.T) {
	h := testHandler(nil)
	icon := httptest.NewRecorder()
	h.ServeHTTP(icon, httptest.NewRequest("GET", "http://127.0.0.1:8787/favicon.ico", nil))
	if icon.Code != http.StatusNoContent {
		t.Fatal("default browser favicon request should not fail")
	}
	for _, item := range []struct{ path, contentType string }{
		{"/", "text/html; charset=utf-8"}, {"/app.js", "text/javascript; charset=utf-8"},
		{"/model.mjs", "text/javascript; charset=utf-8"}, {"/styles.css", "text/css; charset=utf-8"},
	} {
		for _, method := range []string{"GET", "HEAD"} {
			r := httptest.NewRequest(method, "http://127.0.0.1:8787"+item.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || w.Header().Get("Content-Type") != item.contentType {
				t.Fatalf("asset %s %s: code=%d type=%s", method, item.path, w.Code, w.Header().Get("Content-Type"))
			}
			if method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("HEAD wrote body")
			}
			if !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") ||
				w.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("missing browser protection headers")
			}
		}
	}
	for _, path := range []string{"/web/", "/../dashboard.go", "/model.test.mjs", "/api/commands", "/index.html", "/missing.js"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1:8787"+path, nil))
		if w.Code != 404 {
			t.Fatalf("unexpected exposed path %s: %d", path, w.Code)
		}
	}
}

func TestQueryErrorsNeverReturnOldStateOrRawDetails(t *testing.T) {
	for _, test := range []struct {
		err      error
		code     int
		category string
	}{
		{status.Error(codes.PermissionDenied, "secret policy"), 403, "authorization_denied"},
		{status.Error(codes.Unauthenticated, "secret policy"), 403, "authorization_denied"},
		{status.Error(codes.Unavailable, "secret endpoint"), 503, "server_unavailable"},
		{status.Error(codes.DeadlineExceeded, "secret"), 504, "request_timeout"},
		{context.DeadlineExceeded, 504, "request_timeout"},
		{status.Error(codes.Unimplemented, "secret"), 502, "incompatible_server"},
		{errors.New("secret"), 503, "server_unavailable"},
	} {
		h := testHandler(fakeClient{func(context.Context) (*agentflowv1.NodeList, error) {
			return nil, test.err
		}})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiRequest())
		if w.Code != test.code || !strings.Contains(w.Body.String(), test.category) || strings.Contains(w.Body.String(), "secret") ||
			strings.Contains(w.Body.String(), `"nodes"`) {
			t.Fatalf("wrong failure response: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestRequestBoundsAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	h := testHandler(fakeClient{func(ctx context.Context) (*agentflowv1.NodeList, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return &agentflowv1.NodeList{}, nil
		}
	}})
	done := make(chan struct{}, 4)
	for range 4 {
		go func() { h.ServeHTTP(httptest.NewRecorder(), apiRequest()); done <- struct{}{} }()
	}
	for range 4 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("request did not enter")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, apiRequest())
	if w.Code != 429 {
		t.Fatalf("concurrency limit ignored: %d", w.Code)
	}
	close(release)
	for range 4 {
		<-done
	}

	h = testHandler(fakeClient{func(ctx context.Context) (*agentflowv1.NodeList, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	h.timeout = time.Millisecond
	w = httptest.NewRecorder()
	h.ServeHTTP(w, apiRequest())
	if w.Code != 504 {
		t.Fatalf("deadline not enforced: %d", w.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	h.ServeHTTP(w, apiRequest().WithContext(ctx))
	if w.Code != 504 {
		t.Fatal("caller cancellation not propagated")
	}
}

func TestInvalidAndOversizedResponse(t *testing.T) {
	for _, list := range []*agentflowv1.NodeList{
		nil,
		{Nodes: []*agentflowv1.NodeView{{InstanceId: strings.Repeat("x", maxResponseBytes)}}},
	} {
		h := testHandler(fakeClient{func(context.Context) (*agentflowv1.NodeList, error) { return list, nil }})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiRequest())
		if w.Code != 502 || strings.Contains(w.Body.String(), `"nodes"`) {
			t.Fatal("invalid/oversized response was not rejected")
		}
	}
}

func TestResponseSizeBoundary(t *testing.T) {
	list := &agentflowv1.NodeList{Nodes: []*agentflowv1.NodeView{{InstanceId: ""}}}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []int{0, 1} {
		list.Nodes[0].InstanceId = strings.Repeat("x", maxResponseBytes-len(encoded)+extra)
		h := testHandler(fakeClient{func(context.Context) (*agentflowv1.NodeList, error) { return list, nil }})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiRequest())
		if extra == 0 && (w.Code != 200 || w.Body.Len() != maxResponseBytes) {
			t.Fatalf("exact size boundary rejected: code=%d bytes=%d", w.Code, w.Body.Len())
		}
		if extra == 1 && w.Code != 502 {
			t.Fatal("one byte beyond limit accepted")
		}
	}
}

type testFleetService struct {
	agentflowv1.UnimplementedFleetServer
}

func (testFleetService) ListNodes(context.Context, *emptypb.Empty) (*agentflowv1.NodeList, error) {
	return &agentflowv1.NodeList{Nodes: []*agentflowv1.NodeView{{
		InstanceId: "test-node", Connected: true, Stale: false,
		Herdr: &agentflowv1.HerdrState{Status: "ready", Sequence: 42, Agents: []*agentflowv1.HerdrEntity{{Id: "w1:p1", AgentStatus: "working"}}},
	}}}, nil
}

func TestHTTPToGRPCProjectionAndShutdown(t *testing.T) {
	rpcListener := bufconn.Listen(1024 * 1024)
	rpcServer := grpc.NewServer()
	agentflowv1.RegisterFleetServer(rpcServer, testFleetService{})
	go rpcServer.Serve(rpcListener)
	defer rpcServer.Stop()
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return rpcListener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, listener, agentflowv1.NewFleetClient(connection), io.Discard) }()
	request, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+"/api/nodes", nil)
	request.Header.Set("X-Herdr-Dashboard", "1")
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Nodes []struct {
			InstanceID string                            `json:"instance_id"`
			Herdr      struct{ Status, Sequence string } `json:"herdr"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || len(result.Nodes) != 1 || result.Nodes[0].InstanceID != "test-node" ||
		result.Nodes[0].Herdr.Sequence != "42" || result.Nodes[0].Herdr.Status != "ready" {
		t.Fatalf("wrong projection: %+v", result)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dashboard did not shut down")
	}
}
