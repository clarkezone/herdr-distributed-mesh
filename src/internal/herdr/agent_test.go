package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func agentFixture() map[string]any {
	return map[string]any{
		"pane_id": "pane:1", "terminal_id": "terminal:1", "workspace_id": "ws:1", "tab_id": "tab:1",
		"agent": "copilot", "agent_status": "idle", "focused": false, "interactive_ready": true,
		"launch_pending": false, "revision": 9, "state_change_seq": 4,
		"agent_session": map[string]any{
			"kind": "id", "value": "12345678-1234-1234-1234-123456789abc", "agent": "copilot", "source": "PRIVATE-source",
		},
		"name": "Review helper", "title": "PRIVATE-title", "cwd": `C:\src\demo`,
		"terminal_title": "PRIVATE-terminal", "tokens": map[string]any{"secret": "PRIVATE-token"},
	}
}

func agentExpectedTarget() *pb.AgentTarget {
	return &pb.AgentTarget{PaneId: "pane:1", TerminalId: "terminal:1"}
}

func agentQueryFixture(kind pb.AgentQueryKind) *pb.AgentQueryRequest {
	return &pb.AgentQueryRequest{
		NodeInstanceId: "node-1", Kind: kind, Target: agentExpectedTarget(), TimeoutMs: 3000,
	}
}

func agentControlFixture(action pb.AgentControlAction) *pb.AgentControl {
	command := &pb.AgentControl{Action: action, Target: agentExpectedTarget()}
	if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
		command.Text = "Please summarize."
	}
	if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT {
		command.Keys = []string{"enter"}
	}
	return command
}

func agentReply(method, kind string, fields map[string]any, want map[string]any) step {
	return step{method, func(conn net.Conn, request testRequest) {
		var actual map[string]any
		raw, _ := json.Marshal(request.Params)
		json.Unmarshal(raw, &actual)
		expectedRaw, _ := json.Marshal(want)
		var expected map[string]any
		json.Unmarshal(expectedRaw, &expected)
		if !reflect.DeepEqual(actual, expected) {
			// Fail with a malformed response without logging terminal input.
			reply(conn, request, map[string]any{"type": "test_parameter_mismatch"})
			return
		}
		value := map[string]any{"type": kind}
		for key, field := range fields {
			value[key] = field
		}
		reply(conn, request, value)
	}}
}

func agentGetStep(info map[string]any) step {
	return agentReply("agent.get", "agent_info", map[string]any{"agent": info}, map[string]any{"target": "pane:1"})
}

func agentPongStep() step {
	return agentReply("ping", "pong", map[string]any{"version": "0.7.5-preview", "protocol": 18}, map[string]any{})
}

func agentReadFixture(text string) map[string]any {
	return map[string]any{
		"pane_id": "pane:1", "workspace_id": "ws:1", "tab_id": "tab:1", "source": "recent_unwrapped",
		"format": "text", "text": text, "revision": 10, "truncated": false,
	}
}

func agentReadStep(read map[string]any, lines uint32) step {
	return agentReply("agent.read", "pane_read", map[string]any{"read": read}, map[string]any{
		"target": "pane:1", "source": "recent_unwrapped", "lines": lines, "strip_ansi": true, "format": "text",
	})
}

func agentDial(t *testing.T, steps ...step) dialFunc {
	t.Helper()
	var mu sync.Mutex
	var workers sync.WaitGroup
	var peers []net.Conn
	calls := 0
	t.Cleanup(func() {
		mu.Lock()
		for _, peer := range peers {
			peer.Close()
		}
		mu.Unlock()
		workers.Wait()
		if calls != len(steps) {
			t.Errorf("got %d calls, want %d", calls, len(steps))
		}
	})
	return func(ctx context.Context, address string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, _ := localAddress(testConfig().SocketPath)
		if address != expected {
			t.Error("wrong local address")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded dial")
		}
		mu.Lock()
		index := calls
		calls++
		mu.Unlock()
		if index >= len(steps) {
			t.Error("unexpected operation or duplicate effect")
			return nil, errors.New("PRIVATE-dial")
		}
		client, peer := net.Pipe()
		mu.Lock()
		peers = append(peers, peer)
		mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			var request testRequest
			if err := readTestFrame(peer, &request); err != nil {
				t.Errorf("request decode failed: %v", err)
				return
			}
			if request.ID != fmt.Sprintf("herdr-%d", index+1) || request.Method != steps[index].method {
				t.Error("wrong method or request ID")
				return
			}
			steps[index].serve(peer, request)
		}()
		return client, nil
	}
}

func TestAgentGetActualShapeAndAllowlist(t *testing.T) {
	query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)
	query.Target.TerminalId = ""
	original := proto.Clone(query)
	result, err := queryAgent(context.Background(), testConfig(), query, agentDial(t, agentPongStep(), agentGetStep(agentFixture())))
	if err != nil {
		t.Fatal(err)
	}
	if result.Agent.Provider != "copilot" || result.Agent.Target.TerminalId != "terminal:1" ||
		result.Agent.Target.AgentSessionId != "12345678-1234-1234-1234-123456789abc" ||
		!result.Agent.InteractiveReady || result.Agent.Revision != 9 || result.Agent.StateChangeSeq != 4 ||
		result.Agent.DisplayName != "Review helper" || result.Agent.Directory != `C:\src\demo` {
		t.Fatal("approved agent metadata was not preserved")
	}
	raw, err := protojson.Marshal(result)
	if err != nil || strings.Contains(string(raw), "PRIVATE") || result.Text != "" || result.QueryId != "" || result.ErrorCode != "" {
		t.Fatal("query forwarded private metadata or assigned caller-owned fields")
	}
	if !proto.Equal(original, query) {
		t.Fatal("query normalization mutated caller input")
	}
	checkAgentQueryProtocol(t, result, query)
}

func checkAgentQueryProtocol(t *testing.T, result *pb.AgentQueryResult, query *pb.AgentQueryRequest) {
	t.Helper()
	if result.QueryId != "" {
		t.Fatal("adapter assigned the runtime-owned query ID")
	}
	wire := proto.Clone(result).(*pb.AgentQueryResult)
	wire.QueryId = "0123456789abcdef0123456789abcdef"
	if err := protocol.ValidateAgentQueryResult(wire, query); err != nil {
		t.Fatalf("query incompatible with protocol: %v", err)
	}
}

func TestAgentOptionalMetadata(t *testing.T) {
	for _, mode := range []string{"absent", "null_session", "path_session", "absent_provider", "null_provider"} {
		t.Run(mode, func(t *testing.T) {
			info := agentFixture()
			delete(info, "interactive_ready")
			delete(info, "launch_pending")
			delete(info, "state_change_seq")
			delete(info, "agent_session")
			switch mode {
			case "null_session":
				info["agent_session"] = nil
			case "path_session":
				info["agent_session"] = map[string]any{"kind": "path", "value": `C:\PRIVATE-session.json`, "agent": "copilot", "source": "PRIVATE"}
			case "absent_provider":
				delete(info, "agent")
			case "null_provider":
				info["agent"] = nil
			}
			data, _ := json.Marshal(info)
			view, err := parseAgentInfo(data)
			if err != nil {
				t.Fatal(err)
			}
			if view.InteractiveReady || view.LaunchPending || view.StateChangeSeq != 0 || view.Target.AgentSessionId != "" {
				t.Fatal("optional defaults or path omission incorrect")
			}
		})
	}
}

func TestAgentRejectsMalformedMetadata(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing_terminal", func(v map[string]any) { delete(v, "terminal_id") }},
		{"missing_revision", func(v map[string]any) { delete(v, "revision") }},
		{"missing_status", func(v map[string]any) { delete(v, "agent_status") }},
		{"bad_status", func(v map[string]any) { v["agent_status"] = "PRIVATE-status" }},
		{"bad_provider", func(v map[string]any) { v["agent"] = `C:\PRIVATE` }},
		{"null_ready", func(v map[string]any) { v["interactive_ready"] = nil }},
		{"bad_ready", func(v map[string]any) { v["interactive_ready"] = "true" }},
		{"negative_seq", func(v map[string]any) { v["state_change_seq"] = -1 }},
		{"bad_session_type", func(v map[string]any) { v["agent_session"] = "PRIVATE" }},
		{"path_as_id", func(v map[string]any) {
			v["agent_session"].(map[string]any)["value"] = `C:\PRIVATE`
		}},
		{"uri_as_id", func(v map[string]any) { v["agent_session"].(map[string]any)["value"] = "urn:PRIVATE" }},
		{"long_session", func(v map[string]any) { v["agent_session"].(map[string]any)["value"] = strings.Repeat("x", 129) }},
		{"session_provider", func(v map[string]any) { v["agent_session"].(map[string]any)["agent"] = "other" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			info := agentFixture()
			test.mutate(info)
			result, err := queryAgent(context.Background(), testConfig(), agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_GET),
				agentDial(t, agentPongStep(), agentGetStep(info)))
			if result != nil || !errors.Is(err, ErrAgentUnavailable) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("malformed metadata not safely rejected: %v", err)
			}
		})
	}
}

func TestAgentIdentityCheckedBeforeEffect(t *testing.T) {
	for _, changed := range []string{"pane_id", "terminal_id", "session_id"} {
		t.Run(changed, func(t *testing.T) {
			info := agentFixture()
			command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
			if changed == "session_id" {
				command.Target.AgentSessionId = "expected-session"
			} else {
				info[changed] = "different"
			}
			result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t, agentPongStep(), agentGetStep(info)))
			if result != nil || !errors.Is(err, ErrAgentChanged) {
				t.Fatalf("replacement not rejected before effect: %v", err)
			}
		})
	}
}

func TestAgentPromptPreconditions(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		ready  bool
		launch bool
		want   error
	}{
		{"working", "working", true, false, ErrAgentBusy},
		{"blocked", "blocked", true, false, ErrAgentBlocked},
		{"unknown", "unknown", true, false, ErrAgentPrecondition},
		{"not_ready", "idle", false, false, ErrAgentPrecondition},
		{"launching", "idle", true, true, ErrAgentPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := agentFixture()
			info["agent_status"], info["interactive_ready"], info["launch_pending"] = test.status, test.ready, test.launch
			result, err := controlAgent(context.Background(), testConfig(), agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
				agentDial(t, agentPongStep(), agentGetStep(info)))
			if result != nil || !errors.Is(err, test.want) {
				t.Fatalf("unsafe prompt precondition: %v", err)
			}
		})
	}
}

func TestAgentPromptAcknowledgesDeliveryWithoutWait(t *testing.T) {
	ack := agentFixture()
	ack["agent_status"], ack["state_change_seq"] = "working", 5
	command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
		agentPongStep(), agentGetStep(agentFixture()),
		agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": ack},
			map[string]any{"target": "pane:1", "text": command.Text})))
	if err != nil || result == nil || result.ObservedStatus != "working" || result.StateChangeSeq != 5 {
		t.Fatalf("prompt acknowledgement incorrect: %v", err)
	}
	if !proto.Equal(result.Target, command.Target) || result.Target == command.Target {
		t.Fatal("acknowledgement must preserve the original target without aliasing input")
	}
}

func TestAgentInputPermitsBlockedAndVerifiesAfterAck(t *testing.T) {
	info := agentFixture()
	info["agent_status"] = "blocked"
	command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
	result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
		agentPongStep(), agentGetStep(info),
		agentReply("agent.send_keys", "ok", nil, map[string]any{"target": "pane:1", "keys": command.Keys}),
		agentGetStep(info)))
	if err != nil || result == nil || result.ObservedStatus != "blocked" {
		t.Fatalf("blocked agent input failed: %v", err)
	}
}

func TestAgentInterruptUnsupportedProviders(t *testing.T) {
	for _, provider := range []string{"claude", "codex", "Copilot", "unknown"} {
		t.Run(provider, func(t *testing.T) {
			info := agentFixture()
			info["agent"] = provider
			delete(info, "agent_session")
			result, err := controlAgent(context.Background(), testConfig(), agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT),
				agentDial(t, agentPongStep(), agentGetStep(info)))
			if result != nil || !errors.Is(err, ErrAgentUnsupported) {
				t.Fatalf("unverified interrupt enabled: %v", err)
			}
		})
	}
}

func TestAgentCopilotInterruptDeliversTwoEscInOneCall(t *testing.T) {
	for _, status := range []string{"working", "idle"} {
		t.Run(status, func(t *testing.T) {
			before, after := agentFixture(), agentFixture()
			before["agent_status"] = "working"
			after["agent_status"], after["state_change_seq"] = status, 7
			command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT)
			result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
				agentPongStep(), agentGetStep(before),
				agentReply("agent.send_keys", "ok", nil, map[string]any{"target": "pane:1", "keys": []string{"esc", "esc"}}),
				agentGetStep(after)))
			if err != nil || result == nil || result.ObservedStatus != status || result.StateChangeSeq != 7 ||
				!proto.Equal(result.Target, command.Target) || len(command.Keys) != 0 {
				t.Fatalf("interrupt did not preserve delivery-only observation: %v", err)
			}
		})
	}
}

func TestAgentEffectFailuresNeverRetry(t *testing.T) {
	for _, action := range []pb.AgentControlAction{
		pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT,
		pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT,
	} {
		for _, failure := range []string{"lost_reply", "server_error", "wrong_type", "malformed", "cancel", "deadline"} {
			t.Run(fmt.Sprintf("%d_%s", action, failure), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				method := "agent.prompt"
				if action != pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
					method = "agent.send_keys"
				}
				config := testConfig()
				config.RequestTimeout = 300 * time.Millisecond
				effect := step{method, func(conn net.Conn, request testRequest) {
					switch failure {
					case "server_error":
						json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "error": map[string]any{"code": "PRIVATE", "message": `C:\PRIVATE`}})
					case "wrong_type":
						reply(conn, request, map[string]any{"type": "PRIVATE"})
					case "malformed":
						io.WriteString(conn, "{PRIVATE}\n")
					case "cancel":
						cancel()
						expectClosed(t, conn)
					case "deadline":
						expectClosed(t, conn)
					}
				}}
				result, err := controlAgent(ctx, config, agentControlFixture(action), agentDial(t, agentPongStep(), agentGetStep(agentFixture()), effect))
				if result != nil || !errors.Is(err, ErrAgentIndeterminate) || strings.Contains(err.Error(), "PRIVATE") {
					t.Fatalf("effect failure not quarantined: %v", err)
				}
				if failure == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation identity lost")
				}
			})
		}
	}
}

func TestAgentPromptAckReplacementIndeterminate(t *testing.T) {
	info := agentFixture()
	info["terminal_id"] = "terminal:replaced"
	command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
	result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
		agentPongStep(), agentGetStep(agentFixture()),
		agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": info}, map[string]any{"target": "pane:1", "text": command.Text})))
	if result != nil || !errors.Is(err, ErrAgentIndeterminate) {
		t.Fatalf("replaced prompt acknowledgement trusted: %v", err)
	}
}

func TestAgentInputPostAckFailureIndeterminate(t *testing.T) {
	for _, mode := range []string{"replacement", "lost_reply"} {
		t.Run(mode, func(t *testing.T) {
			info := agentFixture()
			info["terminal_id"] = "terminal:replacement"
			after := agentGetStep(info)
			if mode == "lost_reply" {
				after = step{"agent.get", func(net.Conn, testRequest) {}}
			}
			command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
			result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
				agentPongStep(), agentGetStep(agentFixture()),
				agentReply("agent.send_keys", "ok", nil, map[string]any{"target": "pane:1", "keys": command.Keys}), after))
			if result != nil || !errors.Is(err, ErrAgentIndeterminate) {
				t.Fatalf("post-input uncertainty not quarantined: %v", err)
			}
		})
	}
}

func TestAgentReadBoundsAndControlStripping(t *testing.T) {
	for _, test := range []struct {
		name      string
		text      string
		lines     uint32
		serverCut bool
		want      string
		cut       bool
	}{
		{"plain", "hello\nworld\n", 100, false, "hello\nworld\n", false},
		{"controls", "\x1b[31mred\x1b[0m\tok\r\n\x00\x7f\u0085\x1b]0;PRIVATE\x07safe\u009b32mgreen\u009b0m", 100, false, "red\tok\nsafegreen", false},
		{"direction_controls", "a\u202eb\u2066c\u2069", 100, false, "abc", false},
		{"byte_limit", strings.Repeat("a", maxAgentTextBytes) + "b", 100, false, strings.Repeat("a", maxAgentTextBytes), true},
		{"utf8_limit", strings.Repeat("a", maxAgentTextBytes-1) + "\u20ac", 100, false, strings.Repeat("a", maxAgentTextBytes-1), true},
		{"exact_limit", strings.Repeat("a", maxAgentTextBytes), 100, false, strings.Repeat("a", maxAgentTextBytes), false},
		{"line_limit", "one\ntwo\nthree", 2, false, "one\ntwo\n", true},
		{"server_truncated", "hello", 100, true, "hello", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := agentReadFixture(test.text)
			read["truncated"] = test.serverCut
			query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_READ)
			query.Lines = test.lines
			result, err := queryAgent(context.Background(), testConfig(), query, agentDial(t,
				agentPongStep(), agentGetStep(agentFixture()), agentReadStep(read, test.lines), agentGetStep(agentFixture())))
			if err != nil || result == nil {
				t.Fatalf("read failed: %v", err)
			}
			if result.Text != test.want || result.Truncated != test.cut || !utf8.ValidString(result.Text) || len(result.Text) > maxAgentTextBytes {
				t.Fatal("read bounds, UTF8 or control filtering incorrect")
			}
			checkAgentQueryProtocol(t, result, query)
		})
	}
}

func TestAgentReadReplacementDiscardsOutput(t *testing.T) {
	for _, field := range []string{"terminal_id", "pane_id", "workspace_id", "tab_id", "agent", "agent_session"} {
		t.Run(field, func(t *testing.T) {
			info := agentFixture()
			query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_READ)
			if field == "agent_session" {
				query.Target.AgentSessionId = "12345678-1234-1234-1234-123456789abc"
				delete(info, field)
			} else {
				info[field] = "replacement"
				if field == "agent" {
					delete(info, "agent_session")
				}
			}
			result, err := queryAgent(context.Background(), testConfig(), query, agentDial(t,
				agentPongStep(), agentGetStep(agentFixture()), agentReadStep(agentReadFixture("PRIVATE-output"), 100), agentGetStep(info)))
			if result != nil || !errors.Is(err, ErrAgentChanged) {
				t.Fatalf("output survived replacement: %v", err)
			}
		})
	}
}

func TestAgentReadRejectsInvalidShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(map[string]any)
		want   error
	}{
		{"missing_text", func(v map[string]any) { delete(v, "text") }, ErrAgentUnavailable},
		{"missing_truncated", func(v map[string]any) { delete(v, "truncated") }, ErrAgentUnavailable},
		{"missing_revision", func(v map[string]any) { delete(v, "revision") }, ErrAgentUnavailable},
		{"wrong_source", func(v map[string]any) { v["source"] = "visible" }, ErrAgentUnavailable},
		{"ansi", func(v map[string]any) { v["format"] = "ansi" }, ErrAgentUnavailable},
		{"wrong_pane", func(v map[string]any) { v["pane_id"] = "pane:other" }, ErrAgentChanged},
		{"wrong_workspace", func(v map[string]any) { v["workspace_id"] = "ws:other" }, ErrAgentChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := agentReadFixture("PRIVATE")
			test.change(read)
			result, err := queryAgent(context.Background(), testConfig(), agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_READ), agentDial(t,
				agentPongStep(), agentGetStep(agentFixture()), agentReadStep(read, 100)))
			if result != nil || !errors.Is(err, test.want) {
				t.Fatalf("invalid read accepted: %v", err)
			}
		})
	}
}

func TestAgentReadRejectsInvalidFramesBeforeTruncating(t *testing.T) {
	for _, mode := range []string{"oversized", "invalid_utf8", "duplicate_key", "partial", "wrong_id", "flat_read"} {
		t.Run(mode, func(t *testing.T) {
			bad := step{"agent.read", func(conn net.Conn, request testRequest) {
				switch mode {
				case "oversized":
					io.WriteString(conn, `{"id":"`+request.ID+`","result":{"type":"pane_read","text":"`+strings.Repeat("x", maxFrameBytes)+`"}}`+"\n")
				case "invalid_utf8":
					conn.Write([]byte("{\"text\":\"\xff\"}\n"))
				case "duplicate_key":
					io.WriteString(conn, `{"id":"`+request.ID+`","result":{"type":"pane_read","type":"pane_read"}}`+"\n")
				case "partial":
					io.WriteString(conn, `{"id":"`+request.ID+`"`)
				case "wrong_id":
					reply(conn, testRequest{ID: "different"}, map[string]any{"type": "pane_read"})
				case "flat_read":
					read := agentReadFixture("PRIVATE")
					read["type"] = "pane_read"
					reply(conn, request, read)
				}
			}}
			result, err := queryAgent(context.Background(), testConfig(), agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_READ),
				agentDial(t, agentPongStep(), agentGetStep(agentFixture()), bad))
			if result != nil || !errors.Is(err, ErrAgentUnavailable) {
				t.Fatalf("invalid frame became truncated success: %v", err)
			}
		})
	}
}

func TestAgentWaitUsesQueryBudgetAndReturnsFreshObservation(t *testing.T) {
	query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT)
	query.TimeoutMs = 1000
	config := testConfig()
	config.RequestTimeout = 20 * time.Millisecond
	final := agentFixture()
	final["agent_status"], final["state_change_seq"] = "blocked", 6
	waitStep := step{"agent.wait", func(conn net.Conn, request testRequest) {
		var target string
		var until []string
		var timeout uint32
		if len(request.Params) != 3 || required(request.Params, "target", &target) != nil || target != "pane:1" ||
			required(request.Params, "until", &until) != nil || !reflect.DeepEqual(until, []string{"idle", "done", "blocked"}) ||
			required(request.Params, "timeout_ms", &timeout) != nil || timeout <= 20 || timeout > query.TimeoutMs {
			t.Error("WAIT did not receive its normalized separate timeout")
		}
		time.Sleep(60 * time.Millisecond)
		reply(conn, request, map[string]any{"type": "agent_info", "agent": final})
	}}
	result, err := queryAgent(context.Background(), config, query, agentDial(t,
		agentPongStep(), agentGetStep(agentFixture()), waitStep, agentGetStep(final)))
	if err != nil || result == nil || result.Agent.Status != "blocked" || result.Agent.StateChangeSeq != 6 || result.Text != "" {
		t.Fatalf("WAIT incorrectly bounded or semantic result invented: %v", err)
	}
	checkAgentQueryProtocol(t, result, query)
}

func TestAgentControlSessionIdentityOnlyWhenRequested(t *testing.T) {
	for _, action := range []pb.AgentControlAction{
		pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT,
		pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT,
	} {
		for _, mode := range []string{"appeared", "changed_unpinned", "disappeared_unpinned", "pinned_equal", "pinned_changed"} {
			t.Run(fmt.Sprintf("%d_%s", action, mode), func(t *testing.T) {
				before, after := agentFixture(), agentFixture()
				command := agentControlFixture(action)
				switch mode {
				case "appeared":
					delete(before, "agent_session")
				case "changed_unpinned", "pinned_changed":
					after["agent_session"].(map[string]any)["value"] = "new-session"
				case "disappeared_unpinned":
					delete(after, "agent_session")
				}
				if strings.HasPrefix(mode, "pinned_") {
					command.Target.AgentSessionId = "12345678-1234-1234-1234-123456789abc"
				}
				steps := []step{agentPongStep(), agentGetStep(before)}
				if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT {
					steps = append(steps, agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": after},
						map[string]any{"target": "pane:1", "text": command.Text}))
				} else {
					keys := command.Keys
					if action == pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
						keys = []string{"esc", "esc"}
					}
					steps = append(steps, agentReply("agent.send_keys", "ok", nil,
						map[string]any{"target": "pane:1", "keys": keys}), agentGetStep(after))
				}
				result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t, steps...))
				if mode == "pinned_changed" {
					if result != nil || !errors.Is(err, ErrAgentIndeterminate) {
						t.Fatalf("pinned session change not quarantined: %v", err)
					}
					return
				}
				if err != nil || result == nil || !proto.Equal(result.Target, command.Target) || result.Target == command.Target {
					t.Fatalf("control target enriched or unpinned session rejected: %v", err)
				}
				id := "0123456789abcdef0123456789abcdef"
				wire := &pb.CommandResult{
					CommandId: id, Status: pb.CommandStatus_COMMAND_STATUS_SUCCEEDED,
					Detail: protocol.AgentControlSuccessDetail(action), AgentControl: result,
				}
				original := &pb.Command{CommandId: id, CommandType: protocol.AgentControlCommandType, AgentControl: command}
				if err := protocol.ValidateResultForCommand(wire, original); err != nil {
					t.Fatalf("control acknowledgement incompatible with protocol: %v", err)
				}
			})
		}
	}
}

func TestAgentQuerySessionIdentityOnlyWhenRequested(t *testing.T) {
	for _, kind := range []pb.AgentQueryKind{pb.AgentQueryKind_AGENT_QUERY_KIND_READ, pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT} {
		for _, mode := range []string{"appeared", "changed_unpinned", "disappeared_unpinned", "pinned_equal", "pinned_changed"} {
			t.Run(fmt.Sprintf("%d_%s", kind, mode), func(t *testing.T) {
				before, after := agentFixture(), agentFixture()
				query := agentQueryFixture(kind)
				switch mode {
				case "appeared":
					delete(before, "agent_session")
				case "changed_unpinned", "pinned_changed":
					after["agent_session"].(map[string]any)["value"] = "new-session"
				case "disappeared_unpinned":
					delete(after, "agent_session")
				}
				if strings.HasPrefix(mode, "pinned_") {
					query.Target.AgentSessionId = "12345678-1234-1234-1234-123456789abc"
				}
				steps := []step{agentPongStep(), agentGetStep(before)}
				if kind == pb.AgentQueryKind_AGENT_QUERY_KIND_READ {
					steps = append(steps, agentReadStep(agentReadFixture("output"), 100), agentGetStep(after))
				} else {
					steps = append(steps, step{"agent.wait", func(conn net.Conn, request testRequest) {
						reply(conn, request, map[string]any{"type": "agent_info", "agent": after})
					}})
					if mode != "pinned_changed" {
						steps = append(steps, agentGetStep(after))
					}
				}
				result, err := queryAgent(context.Background(), testConfig(), query, agentDial(t, steps...))
				if mode == "pinned_changed" {
					if result != nil || !errors.Is(err, ErrAgentChanged) {
						t.Fatalf("pinned query accepted changed session: %v", err)
					}
					return
				}
				if err != nil || result == nil {
					t.Fatalf("unpinned session update rejected: %v", err)
				}
				checkAgentQueryProtocol(t, result, query)
			})
		}
	}
}

func TestAgentWaitValidatesMatchAndIdentity(t *testing.T) {
	for _, mode := range []string{"unmatched", "replacement", "generic_ok", "wrong_agent_wait_type", "missing_agent", "after_changed", "after_working"} {
		t.Run(mode, func(t *testing.T) {
			query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT)
			observed := agentFixture()
			observed["agent_status"] = "done"
			after := agentFixture()
			after["agent_status"] = "working"
			if mode == "unmatched" {
				observed["agent_status"] = "working"
			}
			if mode == "replacement" {
				observed["terminal_id"] = "terminal:replacement"
			}
			if mode == "after_changed" {
				after["terminal_id"] = "terminal:replacement"
			}
			waitStep := step{"agent.wait", func(conn net.Conn, request testRequest) {
				result := map[string]any{"type": "agent_info", "agent": observed}
				if mode == "generic_ok" {
					result["type"] = "ok"
				}
				if mode == "wrong_agent_wait_type" {
					result["type"] = "agent_wait"
				}
				if mode == "missing_agent" {
					delete(result, "agent")
				}
				reply(conn, request, result)
			}}
			steps := []step{agentPongStep(), agentGetStep(agentFixture()), waitStep}
			if mode == "after_changed" || mode == "after_working" {
				steps = append(steps, agentGetStep(after))
			}
			result, err := queryAgent(context.Background(), testConfig(), query, agentDial(t, steps...))
			if mode == "after_working" {
				if err != nil || result == nil || result.Agent.Status != "done" {
					t.Fatalf("WAIT discarded its matched observation: %v", err)
				}
				return
			}
			want := ErrAgentUnavailable
			if mode == "replacement" || mode == "after_changed" {
				want = ErrAgentChanged
			}
			if result != nil || !errors.Is(err, want) {
				t.Fatalf("invalid WAIT observation accepted: %v", err)
			}
		})
	}
}

func TestAgentWaitCancellationAndTimeout(t *testing.T) {
	for _, mode := range []string{"cancel", "query_timeout", "parent_deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "parent_deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 70*time.Millisecond)
			}
			defer cancel()
			query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_WAIT)
			if mode == "query_timeout" {
				query.TimeoutMs = 70
			}
			waitStep := step{"agent.wait", func(conn net.Conn, request testRequest) {
				if mode == "cancel" {
					cancel()
				}
				expectClosed(t, conn)
			}}
			started := time.Now()
			result, err := queryAgent(ctx, testConfig(), query, agentDial(t, agentPongStep(), agentGetStep(agentFixture()), waitStep))
			if result != nil || !errors.Is(err, ErrAgentUnavailable) || time.Since(started) > time.Second {
				t.Fatalf("WAIT not promptly bounded: %v", err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("WAIT lost context cancellation")
			}
			if mode != "cancel" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("WAIT lost deadline")
			}
		})
	}
}

func TestAgentPreflightDoesNotDialInvalidOrCanceled(t *testing.T) {
	for _, mode := range []string{"nil_query", "nil_control", "missing_terminal", "too_many_lines", "too_long_timeout", "unknown_fields", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_READ)
			switch mode {
			case "nil_query":
				query = nil
			case "missing_terminal":
				query.Target.TerminalId = ""
			case "too_many_lines":
				query.Lines = 1001
			case "too_long_timeout":
				query.TimeoutMs = 300001
			case "unknown_fields":
				query.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "cancel":
				cancel()
			}
			var err error
			if mode == "nil_control" {
				_, err = controlAgent(ctx, testConfig(), nil, agentDial(t))
			} else {
				_, err = queryAgent(ctx, testConfig(), query, agentDial(t))
			}
			want := ErrAgentPrecondition
			if mode == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("invalid preflight: %v", err)
			}
		})
	}
}

func TestAgentUnsupportedProtocolAndSanitizedDialFailure(t *testing.T) {
	t.Run("protocol", func(t *testing.T) {
		pong := agentReply("ping", "pong", map[string]any{"version": "0.7.5", "protocol": 19}, map[string]any{})
		_, err := controlAgent(context.Background(), testConfig(), agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
			agentDial(t, pong))
		if !errors.Is(err, ErrAgentUnsupported) {
			t.Fatalf("unsupported protocol accepted: %v", err)
		}
	})
	t.Run("dial", func(t *testing.T) {
		dial := func(context.Context, string) (net.Conn, error) {
			return nil, errors.New(`PRIVATE C:\socket`)
		}
		_, err := queryAgent(context.Background(), testConfig(), agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_GET), dial)
		if !errors.Is(err, ErrAgentUnavailable) || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("dial failure not sanitized: %v", err)
		}
	})
}
