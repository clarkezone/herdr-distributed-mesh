package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

var stopEmission = errors.New("stop emission")

type testRequest struct {
	ID     string
	Method string
	Params map[string]json.RawMessage
}

type step struct {
	method string
	serve  func(net.Conn, testRequest)
}

type pipeScript struct {
	t       *testing.T
	mu      sync.Mutex
	steps   []step
	calls   int
	peers   []net.Conn
	workers sync.WaitGroup
}

func newScript(t *testing.T, steps ...step) *pipeScript {
	t.Helper()
	s := &pipeScript{t: t, steps: steps}
	t.Cleanup(func() {
		s.mu.Lock()
		for _, peer := range s.peers {
			peer.Close()
		}
		s.mu.Unlock()
		s.workers.Wait()
	})
	return s
}

func testConfig() Config {
	path := "/tmp/herdr-observer-test.sock"
	if runtime.GOOS == "windows" {
		path = `C:\test\herdr.sock`
	}
	return Config{
		SocketPath: path, RefreshInterval: time.Hour,
		RetryDelay: time.Millisecond, RequestTimeout: 2 * time.Second,
	}
}

func (s *pipeScript) dial(ctx context.Context, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	expected, _ := localAddress(testConfig().SocketPath)
	if address != expected {
		s.t.Errorf("incorrect mapped test address")
	}
	if _, ok := ctx.Deadline(); !ok {
		s.t.Error("dial has no deadline")
	}
	s.mu.Lock()
	index := s.calls
	s.calls++
	if index >= len(s.steps) {
		s.mu.Unlock()
		s.t.Errorf("unexpected dial %d", index+1)
		return nil, errors.New("unexpected dial")
	}
	next := s.steps[index]
	client, peer := net.Pipe()
	s.peers = append(s.peers, peer)
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		defer peer.Close()
		peer.SetDeadline(time.Now().Add(5 * time.Second))
		var raw object
		if err := readTestFrame(peer, &raw); err != nil {
			s.t.Errorf("reading test request: %v", err)
			return
		}
		var request testRequest
		if len(raw) != 3 || required(raw, "id", &request.ID) != nil ||
			required(raw, "method", &request.Method) != nil || required(raw, "params", &request.Params) != nil {
			s.t.Error("request is not exactly {id:string,method:string,params:object}")
			return
		}
		if request.ID != fmt.Sprintf("herdr-%d", index+1) || request.Method != next.method {
			s.t.Errorf("request %d: got id=%q method=%q; expected method=%q", index+1, request.ID, request.Method, next.method)
			return
		}
		if request.Method == "events.subscribe" {
			var subscriptions []subscription
			if len(request.Params) != 1 || required(request.Params, "subscriptions", &subscriptions) != nil {
				s.t.Error("invalid subscription params")
				return
			}
			want := []subscription{
				{"workspace.created"}, {"workspace.updated"}, {"workspace.closed"}, {"workspace.renamed"}, {"workspace.focused"},
				{"tab.created"}, {"tab.closed"}, {"tab.renamed"}, {"tab.focused"},
				{"pane.created"}, {"pane.updated"}, {"pane.closed"}, {"pane.focused"}, {"pane.agent_detected"}, {"layout.updated"},
			}
			if !reflect.DeepEqual(subscriptions, want) {
				s.t.Errorf("incorrect read-only subscriptions: %v", subscriptions)
				return
			}
		} else if len(request.Params) != 0 {
			s.t.Error("ping/snapshot params must be an empty object")
			return
		}
		next.serve(peer, request)
	}()
	return client, nil
}

func (s *pipeScript) finished() {
	s.t.Helper()
	done := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.t.Fatal("observer left connections or readers running")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls != len(s.steps) {
		s.t.Errorf("got %d connections; want %d", s.calls, len(s.steps))
	}
}

func reply(conn net.Conn, request testRequest, result any) error {
	return json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "result": result})
}

func expectClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	var data [1]byte
	n, err := conn.Read(data[:])
	if n != 0 || err == nil {
		t.Error("more than one request on a connection")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Error("connection was not closed by observer")
	}
}

func pongStep(t *testing.T) step {
	return step{"ping", func(conn net.Conn, request testRequest) {
		if err := reply(conn, request, map[string]any{"type": "pong", "version": "0.7.5-preview", "protocol": 18}); err != nil {
			t.Errorf("pong: %v", err)
		}
		expectClosed(t, conn)
	}}
}

func subscriptionStep(t *testing.T) step {
	return step{"events.subscribe", func(conn net.Conn, request testRequest) {
		if err := reply(conn, request, map[string]any{"type": "subscription_started"}); err != nil {
			t.Errorf("ack: %v", err)
		}
		expectClosed(t, conn)
	}}
}

func snapshotJSON(id string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"version":"0.7.5-preview","protocol":18,
		"workspaces":[{"workspace_id":%q,"focused":true,"agent_status":"working","title":"PRIVATE TITLE","cwd":"PRIVATE CWD"}],
		"tabs":[],"panes":[],"agents":[],"layouts":[],"focused_workspace_id":%q
	}`, id, id))
}

func snapshotStep(t *testing.T, id string) step {
	return step{"session.snapshot", func(conn net.Conn, request testRequest) {
		if err := reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": snapshotJSON(id)}); err != nil {
			t.Errorf("snapshot: %v", err)
		}
		expectClosed(t, conn)
	}}
}

func checkState(t *testing.T, state *agentflowv1.HerdrState, sequence uint64, status string) {
	t.Helper()
	if state.Sequence != sequence || state.Status != status || state.ObservedAt == nil || state.ObservedAt.CheckValid() != nil {
		t.Errorf("invalid state metadata: sequence=%d status=%q", state.Sequence, state.Status)
	}
	if state.ObservedAt != nil && time.Since(state.ObservedAt.AsTime()).Abs() > 5*time.Second {
		t.Error("timestamp is not current")
	}
	if status == "unavailable" {
		if state.ErrorCode == "" || state.Version != "" || state.Protocol != 0 ||
			len(state.Workspaces)+len(state.Tabs)+len(state.Panes)+len(state.Agents) != 0 {
			t.Error("unavailable state contains prior data or lacks an error category")
		}
	} else if state.ErrorCode != "" {
		t.Error("ready state has an error")
	}
}

func TestObserveReadyReadOnlyAndEmitError(t *testing.T) {
	s := newScript(t, pongStep(t), subscriptionStep(t), snapshotStep(t, "ws:1"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		calls++
		checkState(t, state, 1, "ready")
		if len(state.Workspaces) != 1 || state.Workspaces[0].Id != "ws:1" {
			t.Error("missing snapshot entities")
		}
		return stopEmission
	}, s.dial)
	if err != stopEmission || calls != 1 {
		t.Fatalf("emit error not terminal: %v, calls=%d", err, calls)
	}
	s.finished()
}

func TestObserveProjectedSizeLimitBeforeEmit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		size      int
		oversized bool
	}{
		{"fits-with-metadata", maxStateBytes - 32, false},
		{"metadata-crosses-limit", maxStateBytes, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := projectedStateWithSize(t, tc.size)
			entries := make([]map[string]any, len(state.Workspaces))
			for i, entity := range state.Workspaces {
				entries[i] = map[string]any{
					"workspace_id": entity.Id, "focused": entity.Focused, "agent_status": entity.AgentStatus,
				}
			}
			raw, err := json.Marshal(map[string]any{
				"version": state.Version, "protocol": state.Protocol, "workspaces": entries,
				"tabs": []any{}, "panes": []any{}, "agents": []any{}, "layouts": []any{},
			})
			if err != nil || len(raw) >= maxFrameBytes {
				t.Fatal("invalid size-limit snapshot fixture")
			}
			projected, err := sanitizeSnapshot(raw)
			if err != nil || proto.Size(projected) != tc.size {
				t.Fatal("fixture must be valid before publication adds metadata")
			}
			steps := []step{pongStep(t), subscriptionStep(t), {"session.snapshot", func(conn net.Conn, request testRequest) {
				reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": json.RawMessage(raw)})
				expectClosed(t, conn)
			}}}
			if tc.oversized {
				steps = append(steps, pongStep(t), subscriptionStep(t), snapshotStep(t, "recovered"))
			}
			s := newScript(t, steps...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			err = observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
				calls++
				wire, err := proto.Marshal(state)
				if err != nil || len(wire) > maxStateBytes {
					t.Error("observer emitted a state the server would reject for size")
				}
				if tc.oversized && calls == 1 {
					checkState(t, state, 1, "unavailable")
					if state.ErrorCode != "state_too_large" {
						t.Errorf("got category %q", state.ErrorCode)
					}
					return nil
				}
				checkState(t, state, uint64(calls), "ready")
				return stopEmission
			}, s.dial)
			wantCalls := 1
			if tc.oversized {
				wantCalls = 2
			}
			if err != stopEmission || calls != wantCalls {
				t.Errorf("size-limit/recovery failed: err=%v calls=%d", err, calls)
			}
			s.finished()
		})
	}
}

func TestUnsupportedProtocolsInvalidateAndRetry(t *testing.T) {
	for _, protocol := range []uint32{17, 19} {
		for _, phase := range []string{"ping", "snapshot", "periodic"} {
			t.Run(fmt.Sprintf("%s-%d", phase, protocol), func(t *testing.T) {
				var steps []step
				if phase == "ping" {
					steps = []step{{"ping", func(conn net.Conn, request testRequest) {
						reply(conn, request, map[string]any{"type": "pong", "version": "0.7.5-preview", "protocol": protocol})
						expectClosed(t, conn)
					}}}
				} else {
					steps = []step{pongStep(t), subscriptionStep(t)}
					if phase == "periodic" {
						steps = append(steps, snapshotStep(t, "previous"))
					}
					steps = append(steps, step{"session.snapshot", func(conn net.Conn, request testRequest) {
						var snapshot object
						json.Unmarshal(snapshotJSON("incompatible"), &snapshot)
						snapshot["protocol"], _ = json.Marshal(protocol)
						reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": snapshot})
						expectClosed(t, conn)
					}})
				}
				steps = append(steps, pongStep(t), subscriptionStep(t), snapshotStep(t, "recovered"))
				s := newScript(t, steps...)
				config := testConfig()
				if phase == "periodic" {
					config.RefreshInterval = 20 * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				calls := 0
				failureSequence := 1
				if phase == "periodic" {
					failureSequence = 2
				}
				err := observe(ctx, config, func(state *agentflowv1.HerdrState) error {
					calls++
					if calls == failureSequence {
						checkState(t, state, uint64(calls), "unavailable")
						if state.ErrorCode != "unsupported_protocol" {
							t.Errorf("unexpected category %q", state.ErrorCode)
						}
						return nil
					}
					checkState(t, state, uint64(calls), "ready")
					if state.Protocol != 18 {
						t.Error("observer transmitted unsupported protocol")
					}
					if calls > failureSequence {
						return stopEmission
					}
					return nil
				}, s.dial)
				if err != stopEmission || calls != failureSequence+1 {
					t.Errorf("protocol rejection/recovery failed: err=%v calls=%d", err, calls)
				}
				s.finished()
			})
		}
	}
}

func TestInstalledProtocol18SnakeCaseEventReconciles(t *testing.T) {
	// Synthetic values with the field names/types observed in installed Herdr
	// 0.7.5-preview. No live session values are retained in this fixture.
	const event = `{"event":"pane_updated","data":{"type":"pane_updated","pane":{"pane_id":"pane:1","workspace_id":"ws:1","tab_id":"tab:1","focused":true,"agent_status":"blocked","terminal_title":"PRIVATE"}}}`
	const snapshot = `{
		"version":"0.7.5-preview","protocol":18,
		"focused_workspace_id":"ws:1","focused_tab_id":"tab:1","focused_pane_id":"pane:1",
		"workspaces":[{"workspace_id":"ws:1","focused":true,"agent_status":"working","active_tab_id":"tab:1","label":"Demo","number":1,"pane_count":1,"tab_count":1}],
		"tabs":[{"tab_id":"tab:1","workspace_id":"ws:1","focused":true,"agent_status":"working","label":"Tests","number":1,"pane_count":1}],
		"panes":[{"pane_id":"pane:1","workspace_id":"ws:1","tab_id":"tab:1","focused":true,"agent_status":"working","agent":"copilot","agent_session":{"kind":"path","value":"PRIVATE","agent":"copilot","source":"fixture"},"cwd":"/src/demo","revision":1,"scroll":{},"terminal_id":"terminal:1","terminal_title":"PRIVATE","terminal_title_stripped":"PRIVATE"}],
		"agents":[{"pane_id":"pane:1","workspace_id":"ws:1","tab_id":"tab:1","focused":true,"agent_status":"working","agent":"copilot","agent_session":{"kind":"path","value":"PRIVATE","agent":"copilot","source":"fixture"},"cwd":"/src/demo","revision":1,"state_change_seq":1,"terminal_id":"terminal:1","terminal_title":"PRIVATE","terminal_title_stripped":"PRIVATE"}],
		"layouts":[{"workspace_id":"ws:1","tab_id":"tab:1","focused_pane_id":"pane:1","area":{},"panes":[],"splits":[],"zoomed":false}]
	}`
	steps := []step{
		{"ping", func(conn net.Conn, request testRequest) {
			reply(conn, request, map[string]any{"type": "pong", "version": "0.7.5-preview", "protocol": 18, "capabilities": map[string]any{}})
			expectClosed(t, conn)
		}},
		{"events.subscribe", func(conn net.Conn, request testRequest) {
			// The event can arrive immediately with the acknowledgment and be
			// buffered before the first snapshot is requested.
			fmt.Fprintf(conn, "{\"id\":%q,\"result\":{\"type\":\"subscription_started\"}}\n%s\n", request.ID, event)
			expectClosed(t, conn)
		}},
	}
	for _, status := range []string{"working", "done"} {
		steps = append(steps, step{"session.snapshot", func(conn net.Conn, request testRequest) {
			raw := strings.ReplaceAll(snapshot, `"agent_status":"working"`, `"agent_status":"`+status+`"`)
			reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": json.RawMessage(raw)})
			expectClosed(t, conn)
		}})
	}
	s := newScript(t, steps...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		calls++
		checkState(t, state, uint64(calls), "ready")
		if state.Status != "ready" {
			return stopEmission
		}
		wantStatus := "working"
		if calls == 2 {
			wantStatus = "done"
		}
		for _, entities := range [][]*agentflowv1.HerdrEntity{state.Workspaces, state.Tabs, state.Panes, state.Agents} {
			if len(entities) != 1 || entities[0].AgentStatus != wantStatus {
				t.Error("event did not cause authoritative snapshot reconciliation")
			}
		}
		wire, err := proto.Marshal(state)
		if err != nil || strings.Contains(string(wire), "PRIVATE") {
			t.Error("installed-shape fixture failed redaction")
		}
		if state.Workspaces[0].DisplayName != "Demo" || state.Tabs[0].DisplayName != "Tests" ||
			state.Panes[0].Directory != "/src/demo" || state.Agents[0].Directory != "/src/demo" {
			t.Error("installed-shape configured labels and directories were lost")
		}
		if calls == 2 {
			return stopEmission
		}
		return nil
	}, s.dial)
	if err != stopEmission || calls != 2 {
		t.Errorf("installed event shape rejected: err=%v calls=%d", err, calls)
	}
	s.finished()
}

func TestEventsDrainDuringSnapshotAndTriggerFreshSnapshot(t *testing.T) {
	flood := make(chan struct{})
	drained := make(chan struct{})
	var starts []time.Time
	s := newScript(t, pongStep(t),
		step{"events.subscribe", func(conn net.Conn, request testRequest) {
			if err := reply(conn, request, map[string]any{"type": "subscription_started"}); err != nil {
				t.Error(err)
				return
			}
			select {
			case <-flood:
			case <-time.After(2 * time.Second):
				t.Error("snapshot did not start after ack")
				return
			}
			for i := 0; i < 1000; i++ {
				// Payloads can describe old topology and must never be applied.
				if _, err := io.WriteString(conn, "{\"event\":\"workspace_closed\",\"data\":{\"workspace_id\":\"fresh\",\"title\":\"PRIVATE\"}}\n"); err != nil {
					t.Errorf("event reader stopped draining: %v", err)
					return
				}
			}
			close(drained)
			expectClosed(t, conn)
		}},
		step{"session.snapshot", func(conn net.Conn, request testRequest) {
			starts = append(starts, time.Now())
			close(flood)
			select {
			case <-drained:
			case <-time.After(time.Second):
				t.Error("events blocked while snapshot RPC pending")
				return
			}
			reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": snapshotJSON("old")})
			expectClosed(t, conn)
		}},
		step{"session.snapshot", func(conn net.Conn, request testRequest) {
			starts = append(starts, time.Now())
			reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": snapshotJSON("fresh")})
			expectClosed(t, conn)
		}},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ids []string
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		checkState(t, state, uint64(len(ids)+1), "ready")
		if len(state.Workspaces) != 1 {
			return errors.New("unexpected topology")
		}
		ids = append(ids, state.Workspaces[0].Id)
		if len(ids) == 2 {
			return stopEmission
		}
		return nil
	}, s.dial)
	if err != stopEmission || !reflect.DeepEqual(ids, []string{"old", "fresh"}) {
		t.Fatalf("reconciliation did not replace snapshot: %v, %v", err, ids)
	}
	s.finished()
	if starts[1].Sub(starts[0]) < minEventRefresh-20*time.Millisecond {
		t.Error("event refresh was not rate limited")
	}
}

func TestSubscriptionEOFDuringSnapshotReconnectsWithoutReady(t *testing.T) {
	subConn := make(chan net.Conn, 1)
	s := newScript(t, pongStep(t),
		step{"events.subscribe", func(conn net.Conn, request testRequest) {
			reply(conn, request, map[string]any{"type": "subscription_started"})
			subConn <- conn
			expectClosed(t, conn)
		}},
		step{"session.snapshot", func(conn net.Conn, request testRequest) {
			(<-subConn).Close()
			// EOF must cancel this pending snapshot, not wait for its timeout
			// or publish the previous successfully decoded topology.
			expectClosed(t, conn)
		}},
		pongStep(t), subscriptionStep(t), snapshotStep(t, "reconnected"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	started := time.Now()
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		calls++
		if calls == 1 {
			checkState(t, state, 1, "unavailable")
			if state.ErrorCode != "connection_closed" || time.Since(started) >= time.Second {
				t.Error("subscription EOF did not promptly invalidate pending snapshot")
			}
			return nil
		}
		checkState(t, state, 2, "ready")
		return stopEmission
	}, s.dial)
	if err != stopEmission || calls != 2 {
		t.Fatalf("failed to reconnect: %v, calls=%d", err, calls)
	}
	s.finished()
}

type gatedSnapshotConn struct {
	net.Conn
	ctx   context.Context
	ready chan struct{}
	once  sync.Once
}

func (c *gatedSnapshotConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	if n > 0 {
		c.once.Do(func() {
			close(c.ready)
			// Return already-received success bytes only after the subscription
			// reader has noticed EOF and canceled this request's context.
			<-c.ctx.Done()
		})
	}
	return n, err
}

func TestSubscriptionFailureSuppressesAlreadyReceivedSnapshotSuccess(t *testing.T) {
	received := make(chan struct{})
	s := newScript(t, pongStep(t),
		step{"events.subscribe", func(conn net.Conn, request testRequest) {
			reply(conn, request, map[string]any{"type": "subscription_started"})
			select {
			case <-received:
			case <-time.After(2 * time.Second):
				t.Error("snapshot response was not received")
			}
		}},
		snapshotStep(t, "must-not-publish"),
	)
	dials := 0
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		conn, err := s.dial(ctx, address)
		dials++
		if err == nil && dials == 3 {
			return &gatedSnapshotConn{Conn: conn, ctx: ctx, ready: received}, nil
		}
		return conn, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		checkState(t, state, 1, "unavailable")
		if state.ErrorCode != "connection_closed" {
			t.Errorf("unexpected failure category %q", state.ErrorCode)
		}
		return stopEmission
	}, dial)
	if err != stopEmission {
		t.Errorf("got %v", err)
	}
	s.finished()
}

func TestStreamErrorsInvalidateReadyState(t *testing.T) {
	for _, tc := range []struct {
		name, frame, code string
	}{
		{"api-error", "{\"id\":\"herdr-2\",\"error\":{\"message\":\"PRIVATE\"}}\n", "invalid_response"},
		{"malformed", "{PRIVATE\n", "malformed_json"},
		{"oversized", strings.Repeat(" ", maxFrameBytes+1) + "\n", "frame_too_large"},
		{"unexpected-event", "{\"event\":\"pane_agent_status_changed\",\"data\":{}}\n", "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalidate := make(chan struct{})
			s := newScript(t, pongStep(t),
				step{"events.subscribe", func(conn net.Conn, request testRequest) {
					reply(conn, request, map[string]any{"type": "subscription_started"})
					select {
					case <-invalidate:
					case <-time.After(2 * time.Second):
						t.Error("initial state not published")
						return
					}
					io.WriteString(conn, tc.frame)
					expectClosed(t, conn)
				}}, snapshotStep(t, "previous"))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
				calls++
				if calls == 1 {
					checkState(t, state, 1, "ready")
					close(invalidate)
					return nil
				}
				checkState(t, state, 2, "unavailable")
				if state.ErrorCode != tc.code {
					t.Errorf("got %q; want %q", state.ErrorCode, tc.code)
				}
				return stopEmission
			}, s.dial)
			if err != stopEmission || calls != 2 {
				t.Errorf("stream failure not emitted: %v", err)
			}
			s.finished()
		})
	}
}

func TestPeriodicSnapshotFailureClearsPriorState(t *testing.T) {
	for _, tc := range []struct {
		name, payload, code string
	}{
		{"api", `"error":{"message":"PRIVATE PATH"}`, "api_error"},
		{"invalid-snapshot", `"result":{"type":"session_snapshot","snapshot":{"version":"0.7.5-preview","protocol":18}}`, "invalid_snapshot"},
		{"wrong-type", `"result":{"type":"pong","version":"0.7.5-preview","protocol":18}`, "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, pongStep(t), subscriptionStep(t), snapshotStep(t, "previous"),
				step{"session.snapshot", func(conn net.Conn, request testRequest) {
					fmt.Fprintf(conn, "{\"id\":%q,%s}\n", request.ID, tc.payload)
					expectClosed(t, conn)
				}})
			config := testConfig()
			config.RefreshInterval = 20 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			err := observe(ctx, config, func(state *agentflowv1.HerdrState) error {
				calls++
				if calls == 1 {
					checkState(t, state, 1, "ready")
					return nil
				}
				checkState(t, state, 2, "unavailable")
				if state.ErrorCode != tc.code {
					t.Errorf("got %q; want %q", state.ErrorCode, tc.code)
				}
				return stopEmission
			}, s.dial)
			if err != stopEmission || calls != 2 {
				t.Errorf("snapshot failure not emitted: %v", err)
			}
			s.finished()
		})
	}
}

func TestEOFClearsPublishedEntities(t *testing.T) {
	subConn := make(chan net.Conn, 1)
	s := newScript(t, pongStep(t), step{"events.subscribe", func(conn net.Conn, request testRequest) {
		reply(conn, request, map[string]any{"type": "subscription_started"})
		subConn <- conn
		expectClosed(t, conn)
	}}, snapshotStep(t, "previous"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
		calls++
		if calls == 1 {
			checkState(t, state, 1, "ready")
			(<-subConn).Close()
			return nil
		}
		checkState(t, state, 2, "unavailable")
		return stopEmission
	}, s.dial)
	if err != stopEmission || calls != 2 {
		t.Fatalf("EOF/emit error: %v, calls=%d", err, calls)
	}
	s.finished()
}

func TestPeriodicSnapshotsKeepIdleSubscriptionAlive(t *testing.T) {
	steps := []step{pongStep(t), subscriptionStep(t)}
	for i := 0; i < 7; i++ {
		steps = append(steps, snapshotStep(t, fmt.Sprintf("ws:%d", i)))
	}
	s := newScript(t, steps...)
	config := testConfig()
	config.RefreshInterval = 40 * time.Millisecond
	config.RequestTimeout = 120 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := observe(ctx, config, func(state *agentflowv1.HerdrState) error {
		calls++
		checkState(t, state, uint64(calls), "ready")
		if calls == 7 {
			return stopEmission
		}
		return nil
	}, s.dial)
	if err != stopEmission || calls != 7 {
		t.Fatalf("periodic reconciliation failed: %v, calls=%d", err, calls)
	}
	s.finished()
}

func TestCancellationClosesEveryOperation(t *testing.T) {
	for _, phase := range []string{"ping", "events.subscribe", "session.snapshot", "idle"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			block := step{phase, func(conn net.Conn, _ testRequest) {
				close(entered)
				expectClosed(t, conn)
			}}
			var steps []step
			switch phase {
			case "ping":
				steps = []step{block}
			case "events.subscribe":
				steps = []step{pongStep(t), block}
			case "session.snapshot":
				steps = []step{pongStep(t), subscriptionStep(t), block}
			case "idle":
				steps = []step{pongStep(t), subscriptionStep(t), snapshotStep(t, "ws:1")}
			}
			s := newScript(t, steps...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				result <- observe(ctx, testConfig(), func(*agentflowv1.HerdrState) error {
					if phase != "idle" {
						t.Error("published state before cancellation")
					}
					close(entered)
					return nil
				}, s.dial)
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("operation did not start")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("got %v; want cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation survived cancellation")
			}
			s.finished()
		})
	}
}

func TestCancellationDuringDialAndRetry(t *testing.T) {
	for _, phase := range []string{"dial", "retry"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := testConfig()
			config.RetryDelay = time.Hour
			entered := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				result <- observe(ctx, config, func(state *agentflowv1.HerdrState) error {
					checkState(t, state, 1, "unavailable")
					close(entered)
					return nil
				}, func(dialCtx context.Context, _ string) (net.Conn, error) {
					if phase == "dial" {
						close(entered)
						<-dialCtx.Done()
						return nil, dialCtx.Err()
					}
					return nil, errors.New("PRIVATE local path")
				})
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("phase did not start")
			}
			cancel()
			select {
			case err := <-result:
				if err != context.Canceled {
					t.Errorf("got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not interrupt dial/retry")
			}
		})
	}
}

func TestRequestDeadlines(t *testing.T) {
	for _, phase := range []string{"dial", "ping", "events.subscribe", "session.snapshot", "write"} {
		t.Run(phase, func(t *testing.T) {
			config := testConfig()
			config.RequestTimeout = 40 * time.Millisecond
			var dial dialFunc
			var script *pipeScript
			var peer net.Conn
			switch phase {
			case "dial":
				dial = func(ctx context.Context, _ string) (net.Conn, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
			case "write":
				dial = func(context.Context, string) (net.Conn, error) {
					client, server := net.Pipe()
					peer = server
					return client, nil
				}
			default:
				block := step{phase, func(conn net.Conn, _ testRequest) { expectClosed(t, conn) }}
				steps := []step{block}
				if phase == "events.subscribe" {
					steps = []step{pongStep(t), block}
				} else if phase == "session.snapshot" {
					steps = []step{pongStep(t), subscriptionStep(t), block}
				}
				script = newScript(t, steps...)
				dial = script.dial
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := observe(ctx, config, func(state *agentflowv1.HerdrState) error {
				checkState(t, state, 1, "unavailable")
				if state.ErrorCode != "request_timeout" {
					t.Errorf("got category %q", state.ErrorCode)
				}
				return stopEmission
			}, dial)
			if err != stopEmission {
				t.Errorf("got %v", err)
			}
			if script != nil {
				script.finished()
			}
			if peer != nil {
				defer peer.Close()
				expectClosed(t, peer)
			}
		})
	}
}

func TestBadRepliesEmitSanitizedUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, frame, code string
	}{
		{"api", `{"id":"herdr-1","error":{"message":"PRIVATE TOKEN AND PATH","code":42}}` + "\n", "api_error"},
		{"id", `{"id":"wrong","result":{"type":"pong"}}` + "\n", "invalid_response"},
		{"type", `{"id":"herdr-1","result":{"type":"session_snapshot"}}` + "\n", "invalid_response"},
		{"event", `{"event":"workspace_created","data":{}}` + "\n", "invalid_response"},
		{"mixed", `{"id":"herdr-1","result":{"type":"pong"},"error":{}}` + "\n", "invalid_response"},
		{"malformed", "{PRIVATE\n", "malformed_json"},
		{"oversized", strings.Repeat(" ", maxFrameBytes+1) + "\n", "frame_too_large"},
		{"version", `{"id":"herdr-1","result":{"type":"pong","version":"PRIVATE PATH","protocol":18}}` + "\n", "invalid_response"},
		{"zero-protocol", `{"id":"herdr-1","result":{"type":"pong","version":"0.7.5-preview","protocol":0}}` + "\n", "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newScript(t, step{"ping", func(conn net.Conn, _ testRequest) {
				io.WriteString(conn, tc.frame)
				expectClosed(t, conn)
			}})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := observe(ctx, testConfig(), func(state *agentflowv1.HerdrState) error {
				checkState(t, state, 1, "unavailable")
				if state.ErrorCode != tc.code {
					t.Errorf("got %q; want %q", state.ErrorCode, tc.code)
				}
				return stopEmission
			}, s.dial)
			if err != stopEmission {
				t.Errorf("got %v", err)
			}
			s.finished()
		})
	}
}

func TestConfigurationDoesNotDial(t *testing.T) {
	dial := func(context.Context, string) (net.Conn, error) {
		t.Fatal("invalid config dialed")
		return nil, nil
	}
	for _, config := range []Config{
		{}, {SocketPath: `\\remote\pipe\herdr`}, {SocketPath: "//remote/pipe/herdr"},
		{SocketPath: testConfig().SocketPath, RefreshInterval: -1},
		{SocketPath: testConfig().SocketPath, RetryDelay: -1},
		{SocketPath: testConfig().SocketPath, RequestTimeout: -1},
	} {
		if err := observe(context.Background(), config, func(*agentflowv1.HerdrState) error { return nil }, dial); err == nil {
			t.Error("invalid config accepted")
		}
	}
	if err := observe(context.Background(), testConfig(), nil, dial); err == nil {
		t.Error("nil emitter accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := observe(ctx, testConfig(), func(*agentflowv1.HerdrState) error { return nil }, dial); err != context.Canceled {
		t.Errorf("pre-canceled observation returned %v", err)
	}
}

func TestDefaultRequestTimeoutAndSanitizedDialFailure(t *testing.T) {
	config := Config{SocketPath: testConfig().SocketPath}
	err := observe(context.Background(), config, func(state *agentflowv1.HerdrState) error {
		checkState(t, state, 1, "unavailable")
		if state.ErrorCode != "connection_failed" {
			t.Errorf("got category %q", state.ErrorCode)
		}
		return stopEmission
	}, func(ctx context.Context, _ string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 3*time.Second || time.Until(deadline) < 2*time.Second {
			t.Error("incorrect default request deadline")
		}
		return nil, errors.New("PRIVATE PATH")
	})
	if err != stopEmission {
		t.Errorf("got %v", err)
	}
}
