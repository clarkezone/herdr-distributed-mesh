package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func workspaceBinding(t *testing.T) projects.Binding {
	t.Helper()
	root := t.TempDir()
	checkout := filepath.Join(root, "PRIVATE-checkout")
	if err := os.MkdirAll(filepath.Join(checkout, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"projects": []any{map[string]any{
		"project_id": "AgentFlow", "node_id": "node-1", "binding_revision": "r1",
		"actor_ids": []string{"actor-1"}, "path": checkout,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "policy.json")
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(config, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := policy.Resolve("node-1", "AgentFlow", "r1", "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func workspaceInfo(id, checkout string) map[string]any {
	value := map[string]any{
		"workspace_id": id, "number": 1, "label": "PRIVATE-label",
		"focused": false, "pane_count": 1, "tab_count": 1,
		"active_tab_id": "tab:1", "agent_status": "idle",
	}
	if checkout != "" {
		value["worktree"] = map[string]any{
			"checkout_path": checkout, "repo_root": "PRIVATE-repository",
			"repo_key": "PRIVATE-key", "repo_name": "PRIVATE-name", "is_linked_worktree": false,
		}
	}
	return value
}

func workspaceReply(method, kind string, fields map[string]any) step {
	return step{method, func(conn net.Conn, request testRequest) {
		result := map[string]any{"type": kind}
		for key, value := range fields {
			result[key] = value
		}
		reply(conn, request, result)
	}}
}

func workspacePong() step {
	return workspaceReply("ping", "pong", map[string]any{"protocol": 18, "version": "0.7.5-preview"})
}

func workspaceList(values ...any) step {
	if values == nil {
		values = []any{}
	}
	return workspaceReply("workspace.list", "workspace_list", map[string]any{"workspaces": values})
}

func workspaceCreate(value any) step {
	return workspaceReply("worktree.open", "worktree_opened", map[string]any{
		"already_open": false,
		"workspace":    value, "tab": map[string]any{"private": "never returned"},
		"root_pane": map[string]any{"private": "never returned"},
	})
}

// This fake only permits the three ensure methods and checks the complete
// checkout-open parameter allowlist. It never dials a real Herdr instance.
func workspaceDial(t *testing.T, binding projects.Binding, steps ...step) dialFunc {
	t.Helper()
	var mu sync.Mutex
	var workers sync.WaitGroup
	calls := 0
	t.Cleanup(func() {
		workers.Wait()
		mu.Lock()
		defer mu.Unlock()
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
			t.Error("wrong IPC address")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("request not bounded")
		}
		mu.Lock()
		index := calls
		calls++
		mu.Unlock()
		if index >= len(steps) {
			t.Error("unexpected request or retry")
			return nil, errors.New("PRIVATE-unexpected-dial")
		}
		client, peer := net.Pipe()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			var request testRequest
			if err := readTestFrame(peer, &request); err != nil {
				t.Errorf("read request: %v", err)
				return
			}
			if request.ID != fmt.Sprintf("herdr-%d", index+1) || request.Method != steps[index].method {
				t.Errorf("unexpected request %q %q", request.ID, request.Method)
				return
			}
			switch request.Method {
			case "ping", "workspace.list":
				if len(request.Params) != 0 {
					t.Error("read request parameters must be empty")
				}
			case "worktree.open":
				var cwd, path string
				var focus bool
				if len(request.Params) != 3 || required(request.Params, "cwd", &cwd) != nil ||
					required(request.Params, "path", &path) != nil ||
					required(request.Params, "focus", &focus) != nil || cwd != binding.Path || path != binding.Path || focus {
					t.Error("open must send exactly canonical cwd/path and focus:false")
				}
			default:
				t.Error("forbidden mutation")
				return
			}
			steps[index].serve(peer, request)
		}()
		return client, nil
	}
}

func assertWorkspaceError(t *testing.T, result *pb.WorkspaceEnsureResult, err, want error) {
	t.Helper()
	if result != nil || !errors.Is(err, want) || err.Error() != want.Error() || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("result=%v error=%v, want sanitized %v", result, err, want)
	}
}

func TestEnsureWorkspaceExistingAndCreate(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			b := workspaceBinding(t)
			unrelated := workspaceBinding(t)
			// Labels/repository names are not checkout identity.
			other := workspaceInfo("ws:other", unrelated.Path)
			other["label"] = b.ProjectID
			steps := []step{workspacePong()}
			if existing {
				steps = append(steps, workspaceList(other, workspaceInfo("ws:bound", b.Path)))
			} else {
				steps = append(steps, workspaceList(other, workspaceInfo("ws:no-worktree", "")),
					workspaceCreate(workspaceInfo("ws:bound", b.Path)))
			}
			result, err := ensureWorkspace(context.Background(), testConfig(), b, workspaceDial(t, b, steps...))
			want := &pb.WorkspaceEnsureResult{
				ProjectId: "AgentFlow", BindingRevision: "r1", WorkspaceId: "ws:bound", Created: !existing,
			}
			if err != nil || !proto.Equal(result, want) {
				t.Fatalf("got %v, %v", result, err)
			}
			encoded, err := protojson.Marshal(result)
			if err != nil || strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), "path") {
				t.Fatalf("result contains private fields: %s, %v", encoded, err)
			}
		})
	}
}

func TestEnsureWorkspaceAmbiguous(t *testing.T) {
	b := workspaceBinding(t)
	dial := workspaceDial(t, b, workspacePong(), workspaceList(workspaceInfo("ws:1", b.Path), workspaceInfo("ws:2", b.Path)))
	result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
	assertWorkspaceError(t, result, err, ErrWorkspaceAmbiguous)
}

func TestEnsureWorkspaceNativeConcurrentReuseIsNotCreated(t *testing.T) {
	b := workspaceBinding(t)
	dial := workspaceDial(t, b, workspacePong(), workspaceList(),
		workspaceReply("worktree.open", "worktree_opened", map[string]any{
			"workspace": workspaceInfo("ws:bound", b.Path), "already_open": true,
		}))
	result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
	if err != nil || result.GetCreated() || result.GetWorkspaceId() != "ws:bound" {
		t.Fatalf("native reuse was reported as creation: %v, %v", result, err)
	}
}

func TestEnsureWorkspaceMalformedListNeverCreates(t *testing.T) {
	b := workspaceBinding(t)
	tests := map[string]any{
		"null list": nil, "wrong list type": map[string]any{}, "null entry": []any{nil},
		"unsafe ID":         []any{workspaceInfo("PRIVATE/path", b.Path)},
		"relative checkout": []any{workspaceInfo("ws:1", "PRIVATE-relative")},
		"missing checkout":  []any{map[string]any{}},
		"duplicate IDs":     []any{workspaceInfo("ws:1", b.Path), workspaceInfo("ws:1", b.Path)},
	}
	for _, field := range []string{"workspace_id", "number", "label", "focused", "pane_count", "tab_count", "active_tab_id", "agent_status"} {
		value := workspaceInfo("ws:1", b.Path)
		delete(value, field)
		tests["missing "+field] = []any{value}
		value = workspaceInfo("ws:1", b.Path)
		value[field] = nil
		tests["null "+field] = []any{value}
	}
	for name, worktree := range map[string]any{
		"empty": map[string]any{}, "wrong type": []any{},
		"null checkout":      map[string]any{"checkout_path": nil},
		"nonstring checkout": map[string]any{"checkout_path": 42},
	} {
		value := workspaceInfo("ws:1", b.Path)
		value["worktree"] = worktree
		tests["worktree "+name] = []any{value}
	}
	tooMany := make([]any, maxEntities+1)
	for i := range tooMany {
		tooMany[i] = workspaceInfo(fmt.Sprintf("ws:%d", i), "")
	}
	tests["too many workspaces"] = tooMany
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			dial := workspaceDial(t, b, workspacePong(),
				workspaceReply("workspace.list", "workspace_list", map[string]any{"workspaces": values}))
			result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
			assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
		})
	}
	t.Run("missing list", func(t *testing.T) {
		dial := workspaceDial(t, b, workspacePong(), workspaceReply("workspace.list", "workspace_list", nil))
		result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
	})
	t.Run("full list validated after match", func(t *testing.T) {
		dial := workspaceDial(t, b, workspacePong(), workspaceList(workspaceInfo("ws:1", b.Path), workspaceInfo("PRIVATE/path", "")))
		result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
	})
}

func TestEnsureWorkspaceListFrameValidation(t *testing.T) {
	b := workspaceBinding(t)
	for name, frame := range map[string]string{
		"duplicate key":    `{"id":"herdr-2","result":{"type":"workspace_list","workspaces":[],"workspaces":[]}}` + "\n",
		"wrong id":         `{"id":"PRIVATE-wrong","result":{"type":"workspace_list","workspaces":[]}}` + "\n",
		"wrong type":       `{"id":"herdr-2","result":{"type":"PRIVATE-wrong","workspaces":[]}}` + "\n",
		"trailing garbage": `{"id":"herdr-2","result":{"type":"workspace_list","workspaces":[]}}{}` + "\n",
		"oversized frame": `{"id":"herdr-2","result":{"type":"workspace_list","workspaces":[],"private":"` +
			strings.Repeat("x", maxFrameBytes) + "\"}}\n",
		"lost list": "",
		"API error": `{"id":"herdr-2","error":{"message":"PRIVATE-secret-path"}}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			dial := workspaceDial(t, b, workspacePong(), step{"workspace.list", func(conn net.Conn, request testRequest) {
				io.WriteString(conn, frame)
			}})
			result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
			assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
		})
	}
}

func TestEnsureWorkspaceCreateUnknownNeverRetries(t *testing.T) {
	b := workspaceBinding(t)
	other := workspaceBinding(t)
	for name, create := range map[string]step{
		"lost reply": {"worktree.open", func(net.Conn, testRequest) {}},
		"API error": {"worktree.open", func(conn net.Conn, request testRequest) {
			json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "error": map[string]any{"message": "PRIVATE-error"}})
		}},
		"malformed JSON": {"worktree.open", func(conn net.Conn, request testRequest) {
			io.WriteString(conn, "PRIVATE-not-json\n")
		}},
		"duplicate keys": {"worktree.open", func(conn net.Conn, request testRequest) {
			io.WriteString(conn, `{"id":"herdr-3","result":{"type":"worktree_opened","workspace":null,"workspace":{}}}`+"\n")
		}},
		"wrong response ID": {"worktree.open", func(conn net.Conn, request testRequest) {
			request.ID = "wrong"
			reply(conn, request, map[string]any{"type": "worktree_opened", "workspace": workspaceInfo("ws:1", b.Path)})
		}},
		"wrong type":          workspaceReply("worktree.open", "workspace_list", map[string]any{"workspaces": []any{}}),
		"missing workspace":   workspaceReply("worktree.open", "worktree_opened", nil),
		"missing reuse flag":  workspaceReply("worktree.open", "worktree_opened", map[string]any{"workspace": workspaceInfo("ws:1", b.Path)}),
		"null workspace":      workspaceCreate(nil),
		"unsafe ID":           workspaceCreate(workspaceInfo("PRIVATE/path", b.Path)),
		"mismatched checkout": workspaceCreate(workspaceInfo("ws:1", other.Path)),
		"relative checkout":   workspaceCreate(workspaceInfo("ws:1", "PRIVATE-relative")),
		"malformed workspace": workspaceCreate(map[string]any{"workspace_id": "ws:1"}),
	} {
		t.Run(name, func(t *testing.T) {
			dial := workspaceDial(t, b, workspacePong(), workspaceList(), create)
			result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
			assertWorkspaceError(t, result, err, ErrWorkspaceIndeterminate)
		})
	}
	t.Run("create dial failure", func(t *testing.T) {
		base := workspaceDial(t, b, workspacePong(), workspaceList())
		calls := 0
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			calls++
			if calls == 3 {
				return nil, errors.New("PRIVATE-dial-error")
			}
			return base(ctx, address)
		}
		result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceIndeterminate)
		if calls != 3 {
			t.Fatalf("create retried: %d calls", calls)
		}
	})
	t.Run("reused workspace identity", func(t *testing.T) {
		dial := workspaceDial(t, b, workspacePong(), workspaceList(workspaceInfo("ws:1", other.Path)),
			workspaceCreate(workspaceInfo("ws:1", b.Path)))
		result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceIndeterminate)
	})
}

func TestEnsureWorkspaceUnconfirmedCreatedCheckoutIsIndeterminate(t *testing.T) {
	b, other := workspaceBinding(t), workspaceBinding(t)
	for _, test := range []struct {
		name     string
		worktree any
		absent   bool
	}{
		{name: "missing worktree", absent: true},
		{name: "null worktree"},
		{name: "missing checkout", worktree: map[string]any{}},
		{name: "null checkout", worktree: map[string]any{"checkout_path": nil}},
		{name: "empty checkout", worktree: map[string]any{"checkout_path": ""}},
		{name: "mismatched checkout", worktree: map[string]any{"checkout_path": other.Path}},
	} {
		t.Run(test.name, func(t *testing.T) {
			created := workspaceInfo("ws:1", "")
			if !test.absent {
				created["worktree"] = test.worktree
			}
			dial := workspaceDial(t, b, workspacePong(), workspaceList(), workspaceCreate(created))
			result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
			assertWorkspaceError(t, result, err, ErrWorkspaceIndeterminate)
		})
	}
}

func TestEnsureWorkspaceProtocolAndConfig(t *testing.T) {
	b := workspaceBinding(t)
	for name, fields := range map[string]map[string]any{
		"unknown protocol": {"protocol": 19, "version": "0.7.5"},
		"missing protocol": {"version": "0.7.5"},
		"missing version":  {"protocol": 18},
		"invalid version":  {"protocol": 18, "version": "PRIVATE-raw"},
	} {
		t.Run(name, func(t *testing.T) {
			dial := workspaceDial(t, b, workspaceReply("ping", "pong", fields))
			result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
			assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
		})
	}
	for _, field := range []string{"socket", "timeout", "refresh", "retry"} {
		config := testConfig()
		switch field {
		case "socket":
			config.SocketPath = "PRIVATE-invalid\x00"
		case "timeout":
			config.RequestTimeout = -time.Second
		case "refresh":
			config.RefreshInterval = -time.Second
		case "retry":
			config.RetryDelay = -time.Second
		}
		result, err := ensureWorkspace(context.Background(), config, b, workspaceDial(t, b))
		assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
	}
	t.Run("default timeout", func(t *testing.T) {
		config := testConfig()
		config.RequestTimeout = 0
		base := workspaceDial(t, b, workspacePong(), workspaceList(workspaceInfo("ws:1", b.Path)))
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 3*time.Second || time.Until(deadline) < 2500*time.Millisecond {
				t.Error("default request timeout is not 3 seconds")
			}
			return base(ctx, address)
		}
		result, err := ensureWorkspace(context.Background(), config, b, dial)
		if err != nil || result.Created {
			t.Fatalf("default timeout: %v, %v", result, err)
		}
	})
}

func TestEnsureWorkspaceCancellation(t *testing.T) {
	b := workspaceBinding(t)
	t.Run("before read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := ensureWorkspace(ctx, testConfig(), b, workspaceDial(t, b))
		assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
	})
	for _, duringCreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("during create=%v", duringCreate), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			block := func(conn net.Conn, request testRequest) {
				cancel()
				expectClosed(t, conn)
			}
			steps := []step{workspacePong()}
			want := ErrWorkspaceUnavailable
			if duringCreate {
				steps = append(steps, workspaceList(), step{"worktree.open", block})
				want = ErrWorkspaceIndeterminate
			} else {
				steps = append(steps, step{"workspace.list", block})
			}
			result, err := ensureWorkspace(ctx, testConfig(), b, workspaceDial(t, b, steps...))
			assertWorkspaceError(t, result, err, want)
		})
	}
	t.Run("canceled after list", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dial := workspaceDial(t, b, workspacePong(), step{"workspace.list", func(conn net.Conn, request testRequest) {
			cancel()
			reply(conn, request, map[string]any{"type": "workspace_list", "workspaces": []any{}})
		}})
		result, err := ensureWorkspace(ctx, testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceUnavailable)
	})
	t.Run("TTL bounds create", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		dial := workspaceDial(t, b, workspacePong(), workspaceList(), step{"worktree.open", func(conn net.Conn, request testRequest) {
			expectClosed(t, conn)
		}})
		start := time.Now()
		result, err := ensureWorkspace(ctx, testConfig(), b, dial)
		assertWorkspaceError(t, result, err, ErrWorkspaceIndeterminate)
		if time.Since(start) > time.Second {
			t.Fatal("command exceeded TTL")
		}
	})
}

func TestEnsureWorkspacePathChanges(t *testing.T) {
	replace := func(t *testing.T, path string) {
		t.Helper()
		if err := os.Rename(path, path+"-original"); err != nil {
			t.Error(err)
			return
		}
		if err := os.MkdirAll(filepath.Join(path, ".git"), 0700); err != nil {
			t.Error(err)
		}
	}
	t.Run("before ping", func(t *testing.T) {
		b := workspaceBinding(t)
		replace(t, b.Path)
		result, err := ensureWorkspace(context.Background(), testConfig(), b, workspaceDial(t, b))
		assertWorkspaceError(t, result, err, ErrWorkspacePrecondition)
	})
	t.Run("forged binding", func(t *testing.T) {
		b := projects.Binding{Path: t.TempDir()}
		result, err := ensureWorkspace(context.Background(), testConfig(), b, workspaceDial(t, b))
		assertWorkspaceError(t, result, err, ErrWorkspacePrecondition)
	})
	t.Run("canonical symlink retarget before ping", func(t *testing.T) {
		b, other := workspaceBinding(t), workspaceBinding(t)
		if err := os.Rename(b.Path, b.Path+"-original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other.Path, b.Path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		result, err := ensureWorkspace(context.Background(), testConfig(), b, workspaceDial(t, b))
		assertWorkspaceError(t, result, err, ErrWorkspacePrecondition)
	})
	for _, duringCreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("during create=%v", duringCreate), func(t *testing.T) {
			b := workspaceBinding(t)
			steps := []step{workspacePong()}
			want := ErrWorkspacePrecondition
			if duringCreate {
				steps = append(steps, workspaceList(), step{"worktree.open", func(conn net.Conn, request testRequest) {
					replace(t, b.Path)
					reply(conn, request, map[string]any{"type": "worktree_opened", "already_open": false, "workspace": workspaceInfo("ws:1", b.Path)})
				}})
				want = ErrWorkspaceIndeterminate
			} else {
				steps = append(steps, step{"workspace.list", func(conn net.Conn, request testRequest) {
					replace(t, b.Path)
					reply(conn, request, map[string]any{"type": "workspace_list", "workspaces": []any{}})
				}})
			}
			result, err := ensureWorkspace(context.Background(), testConfig(), b, workspaceDial(t, b, steps...))
			assertWorkspaceError(t, result, err, want)
		})
	}
}

func TestEnsureWorkspaceCanonicalAlias(t *testing.T) {
	b := workspaceBinding(t)
	link := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(b.Path, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	dial := workspaceDial(t, b, workspacePong(), workspaceList(workspaceInfo("ws:1", link)))
	result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
	if err != nil || result.Created || result.WorkspaceId != "ws:1" {
		t.Fatalf("canonical alias not matched: %v, %v", result, err)
	}
}

func TestEnsureWorkspaceMaximumList(t *testing.T) {
	b := workspaceBinding(t)
	values := make([]any, maxEntities)
	for i := range values {
		values[i] = workspaceInfo(fmt.Sprintf("ws:%d", i), "")
	}
	values[len(values)-1] = workspaceInfo("ws:bound", b.Path)
	dial := workspaceDial(t, b, workspacePong(), workspaceList(values...))
	result, err := ensureWorkspace(context.Background(), testConfig(), b, dial)
	if err != nil || result.WorkspaceId != "ws:bound" || result.Created {
		t.Fatalf("4096-workspace boundary: %v, %v", result, err)
	}
}
