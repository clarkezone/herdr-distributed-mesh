package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func namedSnapshot(name string) json.RawMessage {
	data, _ := json.Marshal(map[string]any{
		"version": "0.9.0", "protocol": 18, "layouts": []any{},
		"workspaces": []any{map[string]any{
			"workspace_id": "w1", "focused": true, "agent_status": "working", "label": name,
			"worktree": map[string]any{"checkout_path": `C:\src\demo`, "repo_root": "PRIVATE-root", "repo_key": "PRIVATE-key"},
		}},
		"tabs": []any{map[string]any{"tab_id": "t1", "workspace_id": "w1", "focused": true, "agent_status": "working", "label": "Tests"}},
		"panes": []any{map[string]any{
			"pane_id": "p1", "workspace_id": "w1", "tab_id": "t1", "focused": true, "agent_status": "working",
			"label": "Build shell", "cwd": `C:\src\demo`, "foreground_cwd": `C:\src\demo\tests`, "title": "PRIVATE-title",
		}},
		"agents": []any{map[string]any{
			"pane_id": "p1", "workspace_id": "w1", "tab_id": "t1", "focused": true, "agent_status": "working",
			"name": "Reviewer", "agent": "copilot", "cwd": `C:\src\demo`, "foreground_cwd": `C:\src\demo\tests`,
			"terminal_title": "PRIVATE-title", "tokens": "PRIVATE-token", "prompt": "PRIVATE-prompt",
			"agent_session": map[string]any{"kind": "path", "value": "PRIVATE-session-file", "agent": "copilot", "source": "PRIVATE-source"},
		}},
	})
	return data
}

func TestSnapshotDisplayMetadataAllowlist(t *testing.T) {
	state, err := sanitizeSnapshot(namedSnapshot("API \u754c"))
	if err != nil {
		t.Fatal(err)
	}
	if state.Workspaces[0].DisplayName != "API \u754c" || state.Workspaces[0].Directory != `C:\src\demo` ||
		state.Tabs[0].DisplayName != "Tests" || state.Tabs[0].Directory != "" ||
		state.Panes[0].DisplayName != "Build shell" || state.Panes[0].Directory != `C:\src\demo\tests` ||
		state.Agents[0].DisplayName != "Reviewer" || state.Agents[0].Directory != `C:\src\demo\tests` {
		t.Fatal("native configured labels or reported directories were lost")
	}
	data, err := protojson.Marshal(state)
	if err != nil || strings.Contains(string(data), "PRIVATE") {
		t.Fatal("display projection leaked non-display metadata", err)
	}
}

func TestDisplayMetadataOptionalFieldsAndBounds(t *testing.T) {
	for _, test := range []struct {
		kind, input, name, directory string
		valid                        bool
	}{
		{"agents", `{}`, "", "", true},
		{"agents", `{"name":null,"cwd":null,"foreground_cwd":null}`, "", "", true},
		{"agents", `{"name":"","title":"PRIVATE","terminal_title":"PRIVATE","cwd":"/src"}`, "", "/src", true},
		{"agents", `{"name":"Review","foreground_cwd":"","cwd":"/src"}`, "Review", "/src", true},
		{"panes", `{"label":"Shell","foreground_cwd":"/src/sub","cwd":"/src"}`, "Shell", "/src/sub", true},
		{"tabs", `{"label":"Tests","cwd":"PRIVATE"}`, "Tests", "", true},
		{"workspaces", `{"label":"Plain shell","cwd":"PRIVATE","worktree":null}`, "Plain shell", "", true},
		{"workspaces", `{"label":"Repo","worktree":{"repo_root":"PRIVATE"}}`, "Repo", "", true},
		{"agents", `{"name":42}`, "", "", false},
		{"panes", `{"label":[]}`, "", "", false},
		{"agents", `{"cwd":false}`, "", "", false},
		{"agents", `{"foreground_cwd":{},"cwd":"/src"}`, "", "", false},
		{"workspaces", `{"worktree":{"checkout_path":42}}`, "", "", false},
		{"workspaces", `{"worktree":"PRIVATE"}`, "", "", false},
		{"agents", `{"name":"bad\nname"}`, "", "", false},
		{"agents", `{"cwd":"/bad\u001bpath"}`, "", "", false},
	} {
		var entry object
		if err := json.Unmarshal([]byte(test.input), &entry); err != nil {
			t.Fatal(err)
		}
		name, directory, err := displayMetadata(entry, test.kind)
		if (err == nil) != test.valid || (test.valid && (name != test.name || directory != test.directory)) {
			t.Fatalf("unexpected %s projection: %q %q %v", test.kind, name, directory, err)
		}
	}
	for _, field := range []string{"name", "cwd"} {
		limit := protocol.MaxDisplayNameBytes
		if field == "cwd" {
			limit = protocol.MaxDirectoryBytes
		}
		for _, size := range []int{limit, limit + 1} {
			value, _ := json.Marshal(strings.Repeat("x", size))
			_, _, err := displayMetadata(object{field: value}, "agents")
			if (err == nil) != (size == limit) {
				t.Fatalf("%s byte boundary not enforced", field)
			}
		}
	}
}

func TestAgentDisplayChangesDoNotRetarget(t *testing.T) {
	info := agentFixture()
	data, _ := json.Marshal(info)
	before, err := parseAgentInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	info["name"], info["foreground_cwd"] = "Renamed", "/new/directory"
	data, _ = json.Marshal(info)
	after, err := parseAgentInfo(data)
	if err != nil || !sameAgent(before, after, before.Target) || !proto.Equal(before.Target, after.Target) ||
		after.DisplayName != "Renamed" || after.Directory != "/new/directory" {
		t.Fatal("display metadata changed targeting or did not refresh", err)
	}
	info["name"] = "bad\nname"
	data, _ = json.Marshal(info)
	if _, err := parseAgentInfo(data); !errors.Is(err, ErrAgentUnavailable) {
		t.Fatal("malformed display metadata bypassed agent query validation")
	}
}

func TestObserveRenameRefreshesDisplayWithoutChangingIdentity(t *testing.T) {
	published := make(chan struct{})
	snapshot := func(name string) step {
		return step{"session.snapshot", func(conn net.Conn, request testRequest) {
			if err := reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": namedSnapshot(name)}); err != nil {
				t.Error(err)
			}
			expectClosed(t, conn)
		}}
	}
	script := newScript(t, pongStep(t), step{"events.subscribe", func(conn net.Conn, request testRequest) {
		if err := reply(conn, request, map[string]any{"type": "subscription_started"}); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-published:
		case <-time.After(5 * time.Second):
			t.Error("initial snapshot was not published")
			return
		}
		if _, err := io.WriteString(conn, "{\"event\":\"workspace_renamed\",\"data\":{\"label\":\"PRIVATE-event\"}}\n"); err != nil {
			t.Error(err)
			return
		}
		expectClosed(t, conn)
	}}, snapshot("Original"), snapshot("Renamed"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var names []string
	err := observe(ctx, testConfig(), func(state *pb.HerdrState) error {
		if state.Status != "ready" || len(state.Workspaces) != 1 || state.Workspaces[0].Id != "w1" {
			t.Fatal("rename changed inventory identity or availability")
		}
		names = append(names, state.Workspaces[0].DisplayName)
		if len(names) == 1 {
			close(published)
			return nil
		}
		return stopEmission
	}, script.dial)
	if err != stopEmission || len(names) != 2 || names[0] != "Original" || names[1] != "Renamed" {
		t.Fatalf("rename did not refresh from authoritative snapshot: %v %v", names, err)
	}
	script.finished()
}
