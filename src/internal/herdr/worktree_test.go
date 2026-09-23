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

type worktreeFixture struct {
	binding projects.Binding
	request *pb.WorktreeCreate
	head    string
}

func testWorktreeGit(t *testing.T, cwd string, args ...string) string {
	t.Helper()
	git, err := newWorktreeGit(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	output, code, err := git.run(context.Background(), cwd, args...)
	if err != nil || code != 0 {
		t.Fatalf("isolated git %v failed: exit=%d, error=%v", args, code, err)
	}
	return output
}

func newWorktreeFixture(t *testing.T) worktreeFixture {
	t.Helper()
	return newWorktreeFixtureFormat(t, "sha1")
}

func newWorktreeFixtureFormat(t *testing.T, format string) worktreeFixture {
	t.Helper()
	parent := t.TempDir()
	checkout, root := filepath.Join(parent, "PRIVATE-source"), filepath.Join(parent, "PRIVATE-worktrees")
	for _, path := range []string{checkout, root} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	testWorktreeGit(t, checkout, "init", "--quiet", "--initial-branch=source", "--object-format="+format)
	commit := func(message string) {
		testWorktreeGit(t, checkout, "-c", "user.name=Worktree Test", "-c", "user.email=test@example.invalid",
			"-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", message)
	}
	commit("base")
	base := testWorktreeGit(t, checkout, "rev-parse", "HEAD")
	commit("source")
	head := testWorktreeGit(t, checkout, "rev-parse", "HEAD")
	data, err := json.Marshal(map[string]any{"projects": []any{map[string]any{
		"project_id": "AgentFlow", "node_id": "node-1", "binding_revision": "r1",
		"actor_ids": []string{"actor-1"}, "path": checkout, "allow_worktrees": true, "worktree_root": root,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "policy.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := projects.Load(path, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := policy.Resolve("node-1", "AgentFlow", "r1", "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	return worktreeFixture{binding, &pb.WorktreeCreate{
		ProjectId: "AgentFlow", BindingRevision: "r1", Name: "new-tree", Branch: "new-branch", BaseCommit: base,
	}, head}
}

func (f worktreeFixture) destination() string {
	return filepath.Join(f.binding.WorktreeRoot, f.request.Name)
}

func (f worktreeFixture) list(trees ...any) map[string]any {
	if trees == nil {
		trees = []any{}
	}
	return map[string]any{
		"source": map[string]any{
			"source_checkout_path": f.binding.Path, "repo_root": f.binding.Path,
			"repo_key": "PRIVATE-key", "repo_name": "PRIVATE-name", "source_workspace_id": "ws:source",
		},
		"worktrees": trees,
	}
}

func (f worktreeFixture) listStep() step {
	return workspaceReply("worktree.list", "worktree_list", f.list())
}

func treeInfo(path, branch string) map[string]any {
	return map[string]any{
		"path": path, "branch": branch, "label": "PRIVATE-label", "is_bare": false,
		"is_detached": false, "is_prunable": false, "is_linked_worktree": true,
	}
}

func (f worktreeFixture) created() map[string]any {
	return map[string]any{
		"type": "worktree_created", "workspace": workspaceInfo("ws:new", f.destination()),
		"worktree": treeInfo(f.destination(), f.request.Branch), "tab": map[string]any{},
		"root_pane": map[string]any{},
	}
}

func (f worktreeFixture) createStep(t *testing.T) step {
	return step{"worktree.create", func(conn net.Conn, request testRequest) {
		testWorktreeGit(t, f.binding.Path, "worktree", "add", "--quiet", "-b",
			f.request.Branch, f.destination(), f.request.BaseCommit)
		reply(conn, request, f.created())
	}}
}

// The fake accepts only ping/list/create, checking the entire parameter set.
// All simulated Git effects are confined to this fixture's isolated temp repo.
func worktreeDial(t *testing.T, f worktreeFixture, steps ...step) dialFunc {
	t.Helper()
	var mu sync.Mutex
	var workers sync.WaitGroup
	calls := 0
	t.Cleanup(func() {
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
			t.Error("wrong IPC address")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded request")
		}
		mu.Lock()
		index := calls
		calls++
		mu.Unlock()
		if index >= len(steps) {
			t.Error("unexpected IPC or retry")
			return nil, errors.New("PRIVATE-unexpected-dial")
		}
		client, peer := net.Pipe()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(15 * time.Second))
			var request testRequest
			if err := readTestFrame(peer, &request); err != nil {
				t.Errorf("decode request: %v", err)
				return
			}
			if request.ID != fmt.Sprintf("herdr-%d", index+1) || request.Method != steps[index].method {
				t.Error("wrong request ID or unexpected RPC")
				return
			}
			switch request.Method {
			case "ping":
				if len(request.Params) != 0 {
					t.Error("ping parameters")
				}
			case "worktree.list":
				var cwd string
				if len(request.Params) != 1 || required(request.Params, "cwd", &cwd) != nil || cwd != f.binding.Path {
					t.Error("list must send only canonical cwd")
				}
			case "worktree.create":
				var cwd, path, branch, base string
				var focus bool
				if len(request.Params) != 5 || required(request.Params, "cwd", &cwd) != nil ||
					required(request.Params, "path", &path) != nil || required(request.Params, "branch", &branch) != nil ||
					required(request.Params, "base", &base) != nil || required(request.Params, "focus", &focus) != nil ||
					cwd != f.binding.Path || path != f.destination() || branch != f.request.Branch ||
					base != f.request.BaseCommit || focus {
					t.Error("create must send exactly cwd/path/branch/base/focus:false")
				}
			default:
				t.Error("forbidden RPC")
				return
			}
			steps[index].serve(peer, request)
		}()
		return client, nil
	}
}

func assertWorktreeError(t *testing.T, result *pb.WorktreeCreateResult, err, want error) {
	t.Helper()
	if result != nil || !errors.Is(err, want) || err.Error() != want.Error() || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("result=%v, error=%v; want sanitized %v", result, err, want)
	}
}

func TestCreateWorktreeSuccess(t *testing.T) {
	for _, prefix := range []string{"", "refs/heads/"} {
		t.Run("branch="+prefix, func(t *testing.T) {
			f := newWorktreeFixture(t)
			create := f.createStep(t)
			original := create.serve
			create.serve = func(conn net.Conn, request testRequest) {
				if prefix == "" {
					original(conn, request)
					return
				}
				testWorktreeGit(t, f.binding.Path, "worktree", "add", "--quiet", "-b",
					f.request.Branch, f.destination(), f.request.BaseCommit)
				result := f.created()
				result["worktree"].(map[string]any)["branch"] = prefix + f.request.Branch
				reply(conn, request, result)
			}
			sourceTree := treeInfo(f.binding.Path, "source")
			sourceTree["is_linked_worktree"] = false
			staleTree := treeInfo(filepath.Join(f.binding.WorktreeRoot, "old-missing"), "old")
			staleTree["is_prunable"] = true
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), workspaceReply("worktree.list", "worktree_list", f.list(sourceTree, staleTree)), create))
			want := &pb.WorktreeCreateResult{
				ProjectId: f.request.ProjectId, BindingRevision: f.request.BindingRevision, WorkspaceId: "ws:new",
				Name: f.request.Name, Branch: f.request.Branch, BaseCommit: f.request.BaseCommit,
			}
			if err != nil || !proto.Equal(result, want) {
				t.Fatalf("result=%v, error=%v", result, err)
			}
			encoded, err := protojson.Marshal(result)
			if err != nil || strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), "path") {
				t.Fatalf("private result: %s, %v", encoded, err)
			}
			if testWorktreeGit(t, f.binding.Path, "rev-parse", "HEAD") != f.head ||
				testWorktreeGit(t, f.destination(), "rev-parse", "HEAD") != f.request.BaseCommit ||
				testWorktreeGit(t, f.destination(), "symbolic-ref", "HEAD") != "refs/heads/"+f.request.Branch {
				t.Fatal("Git postconditions not preserved")
			}
		})
	}
}

func TestCreateWorktreeRejectsBeforeIPC(t *testing.T) {
	f := newWorktreeFixture(t)
	for name, change := range map[string]func(*projects.Binding, **pb.WorktreeCreate){
		"nil request":       func(_ *projects.Binding, r **pb.WorktreeCreate) { *r = nil },
		"project mismatch":  func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).ProjectId = "Other" },
		"revision mismatch": func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).BindingRevision = "r2" },
		"ref base":          func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).BaseCommit = "HEAD" },
		"expression base":   func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).BaseCommit += "^" },
		"uppercase base":    func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).BaseCommit = strings.Repeat("A", 40) },
		"escape":            func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).Name = "../escape" },
		"reserved":          func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).Name = "con" },
		"unsafe branch":     func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).Branch = "-b" },
		"unknown fields":    func(_ *projects.Binding, r **pb.WorktreeCreate) { (*r).ProtoReflect().SetUnknown([]byte{0x30, 1}) },
		"no opt in":         func(b *projects.Binding, _ **pb.WorktreeCreate) { b.AllowWorktrees = false },
		"forged root":       func(b *projects.Binding, _ **pb.WorktreeCreate) { b.WorktreeRoot = t.TempDir() },
		"forged binding": func(b *projects.Binding, _ **pb.WorktreeCreate) {
			*b = projects.Binding{ProjectID: b.ProjectID, NodeID: b.NodeID, Revision: b.Revision,
				Path: b.Path, AllowWorktrees: true, WorktreeRoot: b.WorktreeRoot}
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := f.binding
			r := proto.Clone(f.request).(*pb.WorktreeCreate)
			change(&b, &r)
			result, err := createWorktree(context.Background(), testConfig(), b, r, worktreeDial(t, f))
			assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
		})
	}

	for _, kind := range []string{"file", "directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			r := proto.Clone(f.request).(*pb.WorktreeCreate)
			r.Name = kind
			path := filepath.Join(f.binding.WorktreeRoot, kind)
			var err error
			switch kind {
			case "file":
				err = os.WriteFile(path, nil, 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(f.binding.Path, path)
				if err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := createWorktree(context.Background(), testConfig(), f.binding, r, worktreeDial(t, f))
			assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
		})
	}
}

func TestCreateWorktreeSHA256Success(t *testing.T) {
	f := newWorktreeFixtureFormat(t, "sha256")
	if len(f.request.BaseCommit) != 64 {
		t.Fatal("fixture is not SHA256")
	}
	result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
		worktreeDial(t, f, workspacePong(), f.listStep(), f.createStep(t)))
	if err != nil || result == nil || result.BaseCommit != f.request.BaseCommit ||
		testWorktreeGit(t, f.destination(), "rev-parse", "HEAD") != f.request.BaseCommit {
		t.Fatalf("SHA256 result=%v, error=%v", result, err)
	}
}

func TestCreateWorktreeListValidation(t *testing.T) {
	f := newWorktreeFixture(t)
	tests := map[string]func(map[string]any){
		"missing source": func(v map[string]any) { delete(v, "source") },
		"null trees":     func(v map[string]any) { v["worktrees"] = nil },
		"null entry":     func(v map[string]any) { v["worktrees"] = []any{nil} },
		"missing trees":  func(v map[string]any) { delete(v, "worktrees") },
		"too many trees": func(v map[string]any) { v["worktrees"] = make([]any, maxEntities+1) },
		"duplicate tree": func(v map[string]any) {
			tree := treeInfo(f.binding.Path, "source")
			v["worktrees"] = []any{tree, tree}
		},
		"unsafe source ID": func(v map[string]any) { v["source"].(map[string]any)["source_workspace_id"] = "PRIVATE/path" },
	}
	for _, field := range []string{"repo_root", "source_checkout_path", "repo_key", "repo_name"} {
		tests["missing source "+field] = func(v map[string]any) { delete(v["source"].(map[string]any), field) }
	}
	for _, field := range []string{"path", "label", "is_bare", "is_detached", "is_prunable", "is_linked_worktree"} {
		tests["missing tree "+field] = func(v map[string]any) {
			tree := treeInfo(f.binding.Path, "source")
			delete(tree, field)
			v["worktrees"] = []any{tree}
		}
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			list := f.list()
			change(list)
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), workspaceReply("worktree.list", "worktree_list", list)))
			assertWorktreeError(t, result, err, ErrWorkspaceUnavailable)
		})
	}
	for _, branch := range []string{f.request.Branch, "refs/heads/" + f.request.Branch} {
		list := f.list(treeInfo(f.binding.Path, branch))
		result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
			worktreeDial(t, f, workspacePong(), workspaceReply("worktree.list", "worktree_list", list)))
		assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
	}
	for name, list := range map[string]map[string]any{
		"target listed": f.list(treeInfo(f.destination(), "other")),
		"wrong source": {
			"source": map[string]any{"source_checkout_path": t.TempDir(), "repo_root": f.binding.Path,
				"repo_key": "private", "repo_name": "private"}, "worktrees": []any{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), workspaceReply("worktree.list", "worktree_list", list)))
			assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
		})
	}
	t.Run("alias target listed", func(t *testing.T) {
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(f.binding.WorktreeRoot, alias); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		list := f.list(treeInfo(filepath.Join(alias, f.request.Name), "other"))
		result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
			worktreeDial(t, f, workspacePong(), workspaceReply("worktree.list", "worktree_list", list)))
		assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
	})
}

func TestCreateWorktreeGitPreconditions(t *testing.T) {
	f := newWorktreeFixture(t)
	config := testConfig()
	config.RequestTimeout = 10 * time.Second
	for name, base := range map[string]string{
		"missing SHA1": strings.Repeat("0", 40), "missing SHA256": strings.Repeat("0", 64),
		"tree not commit": testWorktreeGit(t, f.binding.Path, "rev-parse", "HEAD^{tree}"),
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(f.request).(*pb.WorktreeCreate)
			r.BaseCommit = base
			result, err := createWorktree(context.Background(), config, f.binding, r,
				worktreeDial(t, f, workspacePong(), f.listStep()))
			assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
		})
	}
	t.Run("unattached branch exists", func(t *testing.T) {
		testWorktreeGit(t, f.binding.Path, "branch", f.request.Branch, f.request.BaseCommit)
		result, err := createWorktree(context.Background(), config, f.binding, f.request,
			worktreeDial(t, f, workspacePong(), f.listStep()))
		assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
	})
	t.Run("fake Git marker", func(t *testing.T) {
		f := newWorktreeFixture(t)
		if err := os.Rename(filepath.Join(f.binding.Path, ".git"), filepath.Join(f.binding.Path, "git-original")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(f.binding.Path, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
		result, err := createWorktree(context.Background(), config, f.binding, f.request,
			worktreeDial(t, f, workspacePong(), f.listStep()))
		assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
	})
}

func TestCreateWorktreeUnknownNeverRetries(t *testing.T) {
	f := newWorktreeFixture(t)
	for name, create := range map[string]step{
		"lost reply": {"worktree.create", func(net.Conn, testRequest) {}},
		"api error": {"worktree.create", func(conn net.Conn, request testRequest) {
			json.NewEncoder(conn).Encode(map[string]any{"id": request.ID, "error": map[string]any{"message": "PRIVATE-error"}})
		}},
		"malformed": {"worktree.create", func(conn net.Conn, _ testRequest) { io.WriteString(conn, "PRIVATE-invalid\n") }},
		"duplicate keys": {"worktree.create", func(conn net.Conn, _ testRequest) {
			io.WriteString(conn, `{"id":"herdr-3","result":{"type":"worktree_created","worktree":null,"worktree":{}}}`+"\n")
		}},
		"wrong ID": {"worktree.create", func(conn net.Conn, request testRequest) {
			request.ID = "wrong"
			reply(conn, request, f.created())
		}},
		"wrong result type": workspaceReply("worktree.create", "workspace_created", nil),
	} {
		t.Run(name, func(t *testing.T) {
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), f.listStep(), create))
			assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
		})
	}
	for name, change := range map[string]func(map[string]any){
		"missing workspace": func(v map[string]any) { delete(v, "workspace") },
		"missing worktree":  func(v map[string]any) { delete(v, "worktree") },
		"unsafe workspace":  func(v map[string]any) { v["workspace"].(map[string]any)["workspace_id"] = "PRIVATE/path" },
		"missing checkout":  func(v map[string]any) { delete(v["workspace"].(map[string]any), "worktree") },
		"wrong branch":      func(v map[string]any) { v["worktree"].(map[string]any)["branch"] = "other" },
		"wrong ref prefix":  func(v map[string]any) { v["worktree"].(map[string]any)["branch"] = "refs/remotes/" + f.request.Branch },
		"wrong path":        func(v map[string]any) { v["worktree"].(map[string]any)["path"] = f.binding.Path },
		"bare":              func(v map[string]any) { v["worktree"].(map[string]any)["is_bare"] = true },
		"detached":          func(v map[string]any) { v["worktree"].(map[string]any)["is_detached"] = true },
		"prunable":          func(v map[string]any) { v["worktree"].(map[string]any)["is_prunable"] = true },
		"not linked":        func(v map[string]any) { v["worktree"].(map[string]any)["is_linked_worktree"] = false },
		"no actual create":  func(map[string]any) {},
	} {
		t.Run(name, func(t *testing.T) {
			created := f.created()
			change(created)
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), f.listStep(), workspaceReply("worktree.create", "worktree_created", created)))
			assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
		})
	}
	t.Run("create dial error", func(t *testing.T) {
		base := worktreeDial(t, f, workspacePong(), f.listStep())
		calls := 0
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			calls++
			if calls == 3 {
				return nil, errors.New("PRIVATE-dial-failure")
			}
			return base(ctx, address)
		}
		result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request, dial)
		assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
		if calls != 3 {
			t.Fatal("retried")
		}
	})
}

func TestCreateWorktreeActualEffectsRemainUnknown(t *testing.T) {
	for _, kind := range []string{"lost reply", "partial branch", "missing metadata", "wrong HEAD", "wrong branch", "different repository", "source HEAD changed", "root changed"} {
		t.Run(kind, func(t *testing.T) {
			f := newWorktreeFixture(t)
			create := step{"worktree.create", func(conn net.Conn, request testRequest) {
				if kind == "partial branch" {
					testWorktreeGit(t, f.binding.Path, "branch", f.request.Branch, f.request.BaseCommit)
					return
				}
				base, branch := f.request.BaseCommit, f.request.Branch
				if kind == "wrong HEAD" {
					base = f.head
				}
				if kind == "wrong branch" {
					branch = "wrong"
				}
				source := f.binding.Path
				if kind == "different repository" {
					source = filepath.Join(t.TempDir(), "clone")
					testWorktreeGit(t, f.binding.Path, "clone", "--quiet", "--no-local", f.binding.Path, source)
				}
				testWorktreeGit(t, source, "worktree", "add", "--quiet", "-b", branch, f.destination(), base)
				if kind == "lost reply" {
					return
				}
				if kind == "source HEAD changed" {
					testWorktreeGit(t, f.binding.Path, "checkout", "--quiet", "--detach", f.request.BaseCommit)
				}
				if kind == "root changed" {
					if err := os.Rename(f.binding.WorktreeRoot, f.binding.WorktreeRoot+"-original"); err != nil {
						t.Error(err)
						return
					}
					if err := os.Mkdir(f.binding.WorktreeRoot, 0700); err != nil {
						t.Error(err)
						return
					}
				}
				created := f.created()
				if kind == "missing metadata" {
					delete(created, "worktree")
				}
				reply(conn, request, created)
			}}
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), f.listStep(), create))
			assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
			if kind == "partial branch" {
				if testWorktreeGit(t, f.binding.Path, "rev-parse", "refs/heads/"+f.request.Branch) != f.request.BaseCommit {
					t.Fatal("partial branch deleted")
				}
				if _, err := os.Lstat(f.destination()); !os.IsNotExist(err) {
					t.Fatal("unexpected destination")
				}
				return
			}
			path := f.destination()
			if kind == "root changed" {
				path = filepath.Join(f.binding.WorktreeRoot+"-original", f.request.Name)
			}
			if _, err := os.Stat(filepath.Join(path, ".git")); err != nil {
				t.Fatal("partial effect rolled back or deleted")
			}
		})
	}
}

func TestCreateWorktreeRechecksBeforeEffect(t *testing.T) {
	for _, kind := range []string{"root", "checkout", "target"} {
		t.Run(kind, func(t *testing.T) {
			f := newWorktreeFixture(t)
			list := step{"worktree.list", func(conn net.Conn, request testRequest) {
				path := f.binding.WorktreeRoot
				if kind == "checkout" {
					path = f.binding.Path
				}
				if kind == "target" {
					path = f.destination()
				} else if err := os.Rename(path, path+"-original"); err != nil {
					t.Error(err)
					return
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Error(err)
					return
				}
				reply(conn, request, map[string]any{"type": "worktree_list", "source": f.list()["source"], "worktrees": []any{}})
			}}
			result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
				worktreeDial(t, f, workspacePong(), list))
			assertWorktreeError(t, result, err, ErrWorkspacePrecondition)
		})
	}
}

func TestCreateWorktreeCancellationAndDeadlines(t *testing.T) {
	f := newWorktreeFixture(t)
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := createWorktree(ctx, testConfig(), f.binding, f.request, worktreeDial(t, f))
		assertWorktreeError(t, result, err, ErrWorkspaceUnavailable)
	})
	for _, method := range []string{"worktree.list", "worktree.create"} {
		t.Run("cancel during "+method, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			steps := []step{workspacePong()}
			want := ErrWorkspaceUnavailable
			if method == "worktree.create" {
				steps = append(steps, f.listStep())
				want = ErrWorkspaceIndeterminate
			}
			steps = append(steps, step{method, func(conn net.Conn, _ testRequest) {
				cancel()
				expectClosed(t, conn)
			}})
			result, err := createWorktree(ctx, testConfig(), f.binding, f.request, worktreeDial(t, f, steps...))
			assertWorktreeError(t, result, err, want)
		})
	}
	t.Run("reads bounded create gets remaining TTL", func(t *testing.T) {
		config := testConfig()
		config.RequestTimeout = 0
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		callerDeadline, _ := ctx.Deadline()
		base := worktreeDial(t, f, workspacePong(), f.listStep(), step{"worktree.create", func(net.Conn, testRequest) {}})
		calls := 0
		dial := func(ctx context.Context, address string) (net.Conn, error) {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok || deadline.After(callerDeadline) {
				t.Error("deadline extended")
			}
			if calls < 3 && time.Until(deadline) > 3*time.Second {
				t.Error("read timeout not bounded")
			}
			if calls == 3 && !deadline.Equal(callerDeadline) {
				t.Error("create did not receive remaining caller TTL")
			}
			return base(ctx, address)
		}
		result, err := createWorktree(ctx, config, f.binding, f.request, dial)
		assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
	})
	t.Run("postcheck canceled", func(t *testing.T) {
		f := newWorktreeFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		create := step{"worktree.create", func(conn net.Conn, request testRequest) {
			testWorktreeGit(t, f.binding.Path, "worktree", "add", "--quiet", "-b", f.request.Branch, f.destination(), f.request.BaseCommit)
			reply(conn, request, f.created())
			cancel()
		}}
		result, err := createWorktree(ctx, testConfig(), f.binding, f.request,
			worktreeDial(t, f, workspacePong(), f.listStep(), create))
		assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
	})
	t.Run("create deadline expires", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		result, err := createWorktree(ctx, testConfig(), f.binding, f.request,
			worktreeDial(t, f, workspacePong(), f.listStep(), step{"worktree.create", func(conn net.Conn, _ testRequest) {
				expectClosed(t, conn)
			}}))
		assertWorktreeError(t, result, err, ErrWorkspaceIndeterminate)
		if time.Since(start) > 4*time.Second {
			t.Fatal("create exceeded caller TTL")
		}
	})
	t.Run("read deadline expires", func(t *testing.T) {
		config := testConfig()
		config.RequestTimeout = 30 * time.Millisecond
		result, err := createWorktree(context.Background(), config, f.binding, f.request,
			worktreeDial(t, f, step{"ping", func(conn net.Conn, _ testRequest) { expectClosed(t, conn) }}))
		assertWorktreeError(t, result, err, ErrWorkspaceUnavailable)
	})
}

func TestCreateWorktreeProtocolAndGitBounds(t *testing.T) {
	f := newWorktreeFixture(t)
	for _, kind := range []string{"protocol", "version", "unexpected RPC result"} {
		pong := map[string]any{"protocol": 18, "version": "0.7.5"}
		resultType := "pong"
		switch kind {
		case "protocol":
			pong["protocol"] = 19
		case "version":
			pong["version"] = "PRIVATE-invalid"
		default:
			resultType = "worktree_created"
		}
		result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
			worktreeDial(t, f, workspaceReply("ping", resultType, pong)))
		assertWorktreeError(t, result, err, ErrWorkspaceUnavailable)
	}
	t.Run("bounded output", func(t *testing.T) {
		var output worktreeGitOutput
		if n, err := output.Write(make([]byte, maxWorktreeGitOutput)); n != maxWorktreeGitOutput || err != nil {
			t.Fatal("exact limit rejected")
		}
		if _, err := output.Write([]byte{1}); err == nil || !output.overflow || output.Len() != maxWorktreeGitOutput {
			t.Fatal("unbounded output")
		}
	})
	t.Run("bounded actual Git output", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "oversized.gitconfig")
		if err := os.WriteFile(path, []byte("[test]\n"+strings.Repeat("large = PRIVATE-value\n", 6000)), 0600); err != nil {
			t.Fatal(err)
		}
		git, err := newWorktreeGit(3 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		output, _, err := git.run(context.Background(), f.binding.Path, "config", "--file", path, "--get-all", "test.large")
		if output != "" || !errors.Is(err, ErrWorkspaceUnavailable) {
			t.Fatalf("Git output limit bypassed: bytes=%d, error=%v", len(output), err)
		}
	})
	t.Run("Git read deadline", func(t *testing.T) {
		git, err := newWorktreeGit(time.Nanosecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := git.run(context.Background(), f.binding.Path, "rev-parse", "HEAD"); !errors.Is(err, ErrWorkspaceUnavailable) {
			t.Fatal("expired Git read accepted")
		}
	})
	t.Run("Git honors canceled context", func(t *testing.T) {
		git, err := newWorktreeGit(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := git.run(ctx, f.binding.Path, "rev-parse", "HEAD"); !errors.Is(err, ErrWorkspaceUnavailable) {
			t.Fatal("canceled Git read accepted")
		}
	})
	t.Run("Git ignores inherited routing", func(t *testing.T) {
		t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "PRIVATE-invalid"))
		t.Setenv("GIT_WORK_TREE", t.TempDir())
		if head := testWorktreeGit(t, f.binding.Path, "rev-parse", "HEAD"); head != f.head {
			t.Fatal("inherited environment redirected repository")
		}
	})
}
