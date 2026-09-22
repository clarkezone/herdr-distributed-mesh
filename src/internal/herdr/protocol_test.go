package herdr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSnapshotRedactionAllowlist(t *testing.T) {
	raw := json.RawMessage(`{
		"version":"0.7.5-preview","protocol":18,
		"focused_workspace_id":"workspace:1","focused_tab_id":"tab-1","focused_pane_id":null,
		"title":"PRIVATE","token":"PRIVATE",
		"workspaces":[{"workspace_id":"workspace:1","focused":true,"agent_status":"idle","title":"PRIVATE","cwd":"PRIVATE","metadata":{"token":"PRIVATE"}}],
		"tabs":[{"tab_id":"tab-1","workspace_id":"workspace:1","focused":false,"agent_status":"blocked","label":"Build tools"}],
		"panes":[{"pane_id":"pane_1","workspace_id":"workspace:1","tab_id":"tab-1","focused":true,"agent_status":"done","terminal_text":"PRIVATE"}],
		"agents":[{"pane_id":"pane_1","workspace_id":"workspace:1","tab_id":"tab-1","focused":false,"agent_status":"PRIVATE_NEW_ENUM","token":"PRIVATE"}],
		"layouts":[{"workspace_id":"workspace:1","tree":{"secret":"PRIVATE"}}]
	}`)
	got, err := sanitizeSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := &agentflowv1.HerdrState{
		Status: "ready", Version: "0.7.5-preview", Protocol: 18,
		Workspaces: []*agentflowv1.HerdrEntity{{Id: "workspace:1", Focused: true, AgentStatus: "idle"}},
		Tabs:       []*agentflowv1.HerdrEntity{{Id: "tab-1", WorkspaceId: "workspace:1", AgentStatus: "blocked", DisplayName: "Build tools"}},
		Panes:      []*agentflowv1.HerdrEntity{{Id: "pane_1", WorkspaceId: "workspace:1", TabId: "tab-1", Focused: true, AgentStatus: "done"}},
		Agents:     []*agentflowv1.HerdrEntity{{Id: "pane_1", WorkspaceId: "workspace:1", TabId: "tab-1", AgentStatus: "unknown"}},
	}
	if !proto.Equal(got, want) {
		t.Error("snapshot differs from exact protobuf allowlist")
	}
	encoded, err := protojson.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"PRIVATE", "title", "cwd", "metadata", "token", "terminal", "layout", "label"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("output contains forbidden field/value %q", forbidden)
		}
	}
}

func TestSnapshotRejectsInvalidFieldsAndMissingCollections(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"version": "0.7.5-preview", "protocol": 18,
			"workspaces": []any{map[string]any{"workspace_id": "ws:1", "focused": true, "agent_status": "working"}},
			"tabs":       []any{}, "panes": []any{}, "agents": []any{}, "layouts": []any{},
		}
	}
	check := func(t *testing.T, value map[string]any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		state, err := sanitizeSnapshot(raw)
		if err == nil || state != nil || category(err) != "invalid_snapshot" {
			t.Errorf("invalid snapshot accepted or wrong error category: %v", err)
		}
	}
	for _, name := range []string{"workspaces", "tabs", "panes", "agents", "layouts", "version", "protocol"} {
		t.Run("missing-"+name, func(t *testing.T) {
			value := base()
			delete(value, name)
			check(t, value)
		})
		t.Run("null-"+name, func(t *testing.T) {
			value := base()
			value[name] = nil
			check(t, value)
		})
	}
	for _, collection := range []string{"workspaces", "tabs", "panes", "agents", "layouts"} {
		for _, bad := range []any{"not-an-array", map[string]any{}, []any{nil}, []any{"not-an-object"}} {
			t.Run("collection-"+collection, func(t *testing.T) {
				value := base()
				value[collection] = bad
				check(t, value)
			})
		}
	}
	for _, tc := range []struct {
		field string
		bad   any
	}{
		{"version", "PRIVATE PATH"}, {"version", strings.Repeat("1", 129) + ".0.0"},
		{"version", "0.7.5-preview\nPRIVATE"}, {"version", true},
		{"protocol", 0}, {"protocol", -1}, {"protocol", 1.5}, {"protocol", uint64(1) << 32}, {"protocol", "18"},
		{"focused_workspace_id", "PRIVATE / PATH"}, {"focused_tab_id", 12}, {"focused_pane_id", ""},
	} {
		t.Run(tc.field, func(t *testing.T) {
			value := base()
			value[tc.field] = tc.bad
			check(t, value)
		})
	}
	for _, collection := range []string{"workspaces", "tabs", "panes", "agents"} {
		idFields := []string{"workspace_id"}
		if collection != "workspaces" {
			idFields = append(idFields, "tab_id")
		}
		if collection == "panes" || collection == "agents" {
			idFields = append(idFields, "pane_id")
		}
		for _, field := range append(idFields, "focused", "agent_status") {
			for _, bad := range []any{nil, 7, []any{}} {
				t.Run(collection+"-"+field, func(t *testing.T) {
					value := base()
					entry := map[string]any{"workspace_id": "ws:1", "tab_id": "tab:1", "pane_id": "pane:1", "focused": true, "agent_status": "idle"}
					entry[field] = bad
					value[collection] = []any{entry}
					check(t, value)
					delete(entry, field)
					check(t, value)
				})
			}
		}
		for _, field := range idFields {
			for _, bad := range []string{"", "a b", "a/b", `a\b`, "a.b", "caf\u00e9", "a\n", strings.Repeat("a", 129)} {
				t.Run(collection+"-id", func(t *testing.T) {
					value := base()
					entry := map[string]any{"workspace_id": "ws:1", "tab_id": "tab:1", "pane_id": "pane:1", "focused": true, "agent_status": "idle"}
					entry[field] = bad
					value[collection] = []any{entry}
					check(t, value)
				})
			}
		}
	}
}

func TestSnapshotIDsMustBeUniqueWithinEachGroup(t *testing.T) {
	entry := map[string]any{
		"workspace_id": "shared", "tab_id": "shared", "pane_id": "shared",
		"focused": false, "agent_status": "idle",
	}
	snapshot := map[string]any{
		"version": "0.7.5-preview", "protocol": 18,
		"workspaces": []any{entry}, "tabs": []any{entry},
		"panes": []any{entry}, "agents": []any{entry}, "layouts": []any{},
	}
	raw, _ := json.Marshal(snapshot)
	if _, err := sanitizeSnapshot(raw); err != nil {
		t.Fatalf("IDs shared across groups must be accepted: %v", err)
	}
	for _, group := range []string{"workspaces", "tabs", "panes", "agents"} {
		t.Run(group, func(t *testing.T) {
			snapshot[group] = []any{entry, entry}
			defer func() { snapshot[group] = []any{entry} }()
			raw, _ := json.Marshal(snapshot)
			state, err := sanitizeSnapshot(raw)
			if state != nil || err == nil || category(err) != "invalid_snapshot" {
				t.Errorf("duplicate ID accepted: %v", err)
			}
		})
	}
}

func TestSnapshotOnlySupportsVerifiedProtocols(t *testing.T) {
	for _, protocol := range []uint32{1, 17, 18, 19, 20, 21, 22, 23, ^uint32(0)} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			var snapshot object
			json.Unmarshal(snapshotJSON("ws:1"), &snapshot)
			snapshot["protocol"], _ = json.Marshal(protocol)
			raw, _ := json.Marshal(snapshot)
			state, err := sanitizeSnapshot(raw)
			if protocol == 18 || protocol == 20 || protocol == 22 {
				if err != nil || state == nil || state.Protocol != protocol {
					t.Errorf("supported snapshot rejected: %v", err)
				}
			} else if state != nil || err == nil || category(err) != "unsupported_protocol" {
				t.Errorf("unsupported snapshot accepted or wrong category: %v", err)
			}
		})
	}
}

// Build an exact-sized projection using only valid, unique, bounded IDs.
func projectedStateWithSize(t *testing.T, target int) *agentflowv1.HerdrState {
	t.Helper()
	state := &agentflowv1.HerdrState{Status: "ready", Version: "0.7.5-preview", Protocol: 18}
	for i := 0; i < 2048; i++ {
		state.Workspaces = append(state.Workspaces, &agentflowv1.HerdrEntity{
			Id: fmt.Sprintf("ws:%04d:", i) + strings.Repeat("a", 120), Focused: true, AgentStatus: "working",
		})
	}
	size := proto.Size(state)
	for _, entity := range state.Workspaces {
		contribution := &agentflowv1.HerdrState{Workspaces: []*agentflowv1.HerdrEntity{entity}}
		before := proto.Size(contribution)
		for len(entity.Id) > 8 && size > target {
			oldID := entity.Id
			entity.Id = oldID[:len(oldID)-1]
			after := proto.Size(contribution)
			if size-(before-after) < target {
				entity.Id = oldID
				break
			}
			size -= before - after
			before = after
		}
	}
	if size != target || proto.Size(state) != target {
		t.Fatalf("could not construct exact-sized fixture: got %d; want %d", proto.Size(state), target)
	}
	return state
}

func TestProjectedStateSerializedSizeBoundaryIncludesMetadata(t *testing.T) {
	metadata := &agentflowv1.HerdrState{
		Sequence: ^uint64(0), ObservedAt: timestamppb.New(time.Unix(1800000000, 999999999)),
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("limit%+d", delta), func(t *testing.T) {
			target := maxStateBytes + delta
			state := projectedStateWithSize(t, target-proto.Size(metadata))
			state.Sequence = metadata.Sequence
			state.ObservedAt = metadata.ObservedAt
			wire, err := proto.Marshal(state)
			if err != nil || len(wire) != target {
				t.Fatalf("incorrect wire-size boundary: size=%d, err=%v", len(wire), err)
			}
			err = checkStateSize(state)
			if delta <= 0 {
				if err != nil {
					t.Errorf("permitted serialized size rejected: %v", err)
				}
			} else if err == nil || category(err) != "state_too_large" {
				t.Errorf("oversized serialized state accepted: %v", err)
			}
		})
	}
}

func TestSnapshotBoundsAndStatusEnums(t *testing.T) {
	for _, status := range []string{"idle", "working", "blocked", "done", "unknown", "new-status", ""} {
		entry := object{
			"workspace_id": json.RawMessage(`"` + strings.Repeat("A", 128) + `"`),
			"focused":      json.RawMessage(`false`),
			"agent_status": json.RawMessage(`"` + status + `"`),
		}
		got, err := sanitizeEntity(entry, "workspace_id")
		if err != nil {
			t.Fatal(err)
		}
		want := status
		if status == "new-status" || status == "" {
			want = "unknown"
		}
		if got.AgentStatus != want {
			t.Errorf("status %q mapped to %q", status, got.AgentStatus)
		}
	}
	for _, count := range []int{maxEntities - 1, maxEntities} {
		layouts := make([]object, count)
		for i := range layouts {
			layouts[i] = object{}
		}
		var snapshot object
		json.Unmarshal(snapshotJSON("ws:1"), &snapshot)
		snapshot["layouts"], _ = json.Marshal(layouts)
		raw, _ := json.Marshal(snapshot)
		state, err := sanitizeSnapshot(raw)
		if count == maxEntities-1 {
			if err != nil || state == nil {
				t.Errorf("exact total entity limit rejected: %v", err)
			}
		} else if state != nil || category(err) != "too_many_entities" {
			t.Error("total entity limit (including omitted layouts) not enforced")
		}
	}
}

func TestFrameBoundsAndMalformedJSON(t *testing.T) {
	exact := `{"x":"` + strings.Repeat("a", maxFrameBytes-len("{\"x\":\"\"}\n")) + "\"}\n"
	for _, tc := range []struct {
		name, input, code string
	}{
		{"exact-limit", exact, ""},
		{"over-limit", " " + exact, "frame_too_large"},
		{"unterminated", `{"x":1}`, "malformed_json"},
		{"empty", "", "connection_closed"},
		{"blank", "\n", "malformed_json"},
		{"two-values", "{}{}\n", "malformed_json"},
		{"duplicate", "{\"id\":1,\"id\":2}\n", "malformed_json"},
		{"nested-duplicate", "{\"data\":{\"id\":1,\"id\":2}}\n", "malformed_json"},
		{"deep", strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66) + "\n", "malformed_json"},
		{"invalid", "{\"x\":}\n", "malformed_json"},
		{"invalid-utf8", "{\"x\":\"\xff\"}\n", "malformed_json"},
		{"array", "[]\n", "invalid_response"},
		{"null", "null\n", "invalid_response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := frameReader{bufio.NewReader(strings.NewReader(tc.input))}
			_, err := reader.object()
			if tc.code == "" {
				if err != nil {
					t.Error(err)
				}
			} else if err == nil || category(err) != tc.code {
				t.Errorf("got %v; want %s", err, tc.code)
			}
		})
	}
}

func TestEventValidation(t *testing.T) {
	for _, input := range []string{
		`{"id":"herdr-1","result":{"type":"subscription_started"}}`,
		`{"event":"pane_agent_status_changed","data":{}}`,
		`{"event":"pane_output_matched","data":{}}`,
		`{"event":"workspace_created"}`,
		`{"event":"workspace_created","data":null}`,
		`{"event":"workspace_created","data":[]}`,
		`{"event":"workspace_created","data":{},"result":{}}`,
		`{"event":"workspace.created","data":{}}`,
	} {
		var event object
		json.Unmarshal([]byte(input), &event)
		if err := validateEvent(event); err == nil || category(err) != "invalid_response" {
			t.Error("unexpected event envelope accepted")
		}
	}
	for _, sub := range topologySubscriptions() {
		raw, _ := json.Marshal(map[string]any{"event": strings.ReplaceAll(sub.Type, ".", "_"), "data": map[string]any{"title": "PRIVATE"}})
		var event object
		json.Unmarshal(raw, &event)
		if err := validateEvent(event); err != nil {
			t.Errorf("stable event rejected: %v", err)
		}
	}
}
