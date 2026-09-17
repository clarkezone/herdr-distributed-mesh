package projects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func managedCheckout(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkout")
	cmd := exec.Command("git", "init", "--quiet", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	return path
}

func TestManagedApplyRootsAndResolve(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(fmt.Sprint(override), func(t *testing.T) {
			m := NewManaged("node-1")
			path := managedCheckout(t)
			root := ""
			want := path + "-worktrees"
			if override {
				want = filepath.Join(t.TempDir(), "custom")
				root = want
			}
			b, err := m.Apply(context.Background(), "project", "server:opaque_2", path, root)
			if err != nil {
				t.Fatal(err)
			}
			if b.WorktreeRoot != want || !b.AllowWorktrees || b.ValidateWorktreeRoot() != nil || !m.HasWorktrees() {
				t.Fatalf("invalid binding: %+v", b)
			}
			for _, actor := range []string{"", "ungranted", "not/an/id"} {
				got, err := m.Resolve("node-1", "project", "server:opaque_2", actor)
				if err != nil || got.Path != b.Path {
					t.Fatalf("actor affected resolution: %v", err)
				}
			}
			for _, args := range [][3]string{
				{"node-2", "project", b.Revision}, {"node-1", "other", b.Revision}, {"node-1", "project", "old"},
			} {
				if _, err := m.Resolve(args[0], args[1], args[2], ""); !errors.Is(err, ErrDenied) {
					t.Fatalf("mismatched binding allowed: %v", err)
				}
			}
			if _, err := os.Stat(filepath.Join(path, ".git")); err != nil {
				t.Fatal("checkout changed")
			}
		})
	}
}

func TestManagedRejectsInvalidGitCheckout(t *testing.T) {
	path := managedCheckout(t)
	subdir := filepath.Join(path, "subdir")
	if err := os.Mkdir(subdir, 0700); err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(t.TempDir(), "bare")
	if out, err := exec.Command("git", "init", "--bare", "--quiet", bare).CombinedOutput(); err != nil {
		t.Fatalf("git init bare: %v: %s", err, out)
	}
	for _, invalid := range []string{gitCheckout(t), t.TempDir(), subdir, bare, "relative", filepath.Join(t.TempDir(), "absent")} {
		root := filepath.Join(t.TempDir(), "root")
		_, err := NewManaged("node").Apply(context.Background(), "project", "r1", invalid, root)
		if err != ErrInvalidPath {
			t.Fatalf("accepted invalid checkout %q: %v", invalid, err)
		}
		if _, err := os.Lstat(root); !os.IsNotExist(err) {
			t.Fatal("invalid apply created root")
		}
	}
	t.Setenv("GIT_DIR", filepath.Join(path, ".git"))
	t.Setenv("GIT_WORK_TREE", path)
	if _, err := NewManaged("node").Apply(context.Background(), "project", "r1", gitCheckout(t), ""); err != ErrInvalidPath {
		t.Fatalf("inherited Git environment bypass: %v", err)
	}
}

func TestManagedRootAdoptionAndRepeatedApply(t *testing.T) {
	path := managedCheckout(t)
	root := t.TempDir()
	m := NewManaged("node")
	b, err := m.Apply(context.Background(), "project", "r1", path, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "owned-worktree"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []string{"r1", "r2", "r2"} {
		next, err := m.Apply(context.Background(), "project", revision, path, root)
		if err != nil || next.ValidateWorktreeRoot() != nil || b.ValidateWorktreeRoot() != nil {
			t.Fatalf("repeat/frozen binding failed: %v", err)
		}
		if next.pin == b.pin || next.worktreePin == b.worktreePin {
			t.Fatal("reused mutable pin")
		}
	}
	if _, err := NewManaged("node").Apply(context.Background(), "project", "r1", path, root); err != ErrInvalidPath {
		t.Fatalf("unrelated nonempty root adopted: %v", err)
	}
	if contents, err := os.ReadFile(filepath.Join(root, "owned-worktree")); err != nil || string(contents) != "preserve" {
		t.Fatal("root contents changed")
	}
}

func TestManagedLinkedGitWorktree(t *testing.T) {
	checkout := managedCheckout(t)
	cmd := exec.Command("git", "-C", checkout, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
		"-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "initial")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v: %s", err, out)
	}
	linked := filepath.Join(t.TempDir(), "linked")
	if out, err := exec.Command("git", "-C", checkout, "worktree", "add", "--quiet", "--detach", linked).CombinedOutput(); err != nil {
		t.Fatalf("git worktree: %v: %s", err, out)
	}
	m := NewManaged("node")
	b, err := m.Apply(context.Background(), "project", "r1", linked, "")
	if err != nil || b.ValidateWorktreeRoot() != nil {
		t.Fatalf("real linked checkout rejected: %v", err)
	}
	b.Revision, b.Path, b.WorktreeRoot = "forged", checkout, checkout
	if b.ValidatePath() == nil || b.ValidateWorktreeRoot() == nil {
		t.Fatal("exported fields bypassed pins")
	}
	current, err := m.Resolve("node", "project", "r1", "")
	if err != nil || current.Path != linked || current.ValidateWorktreeRoot() != nil {
		t.Fatal("returned binding mutation affected cache")
	}
}

func TestManagedReconfigurationFreezesCopies(t *testing.T) {
	m := NewManaged("node")
	path := managedCheckout(t)
	dirty := filepath.Join(path, "untracked.txt")
	if err := os.WriteFile(dirty, []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	old, err := m.Apply(context.Background(), "project", "r1", path, "")
	if err != nil {
		t.Fatal(err)
	}
	newPath := managedCheckout(t)
	if _, err := m.Apply(context.Background(), "project", "r1", newPath, ""); err != ErrInvalidPolicy {
		t.Fatalf("revision was repurposed: %v", err)
	}
	newBinding, err := m.Apply(context.Background(), "project", "r2", newPath, "")
	if err != nil {
		t.Fatal(err)
	}
	if old.Revision != "r1" || old.Path != path || old.ValidateWorktreeRoot() != nil ||
		newBinding.Path != newPath || newBinding.ValidateWorktreeRoot() != nil {
		t.Fatal("reconfiguration invalidated frozen binding")
	}
	if _, err := m.Resolve("node", "project", "r1", ""); err != ErrDenied {
		t.Fatal("old revision still current")
	}
	if contents, err := os.ReadFile(dirty); err != nil || string(contents) != "dirty" {
		t.Fatal("dirty checkout changed")
	}
	if _, err := m.Apply(context.Background(), "project", "r3", t.TempDir(), ""); err == nil {
		t.Fatal("invalid update succeeded")
	}
	if b, err := m.Resolve("node", "project", "r2", ""); err != nil || b.Path != newPath {
		t.Fatal("failed update changed current binding")
	}
}

func TestManagedOverlaps(t *testing.T) {
	path := managedCheckout(t)
	child := filepath.Join(path, "child")
	for _, root := range []string{path, filepath.Dir(path), child} {
		if _, err := NewManaged("node").Apply(context.Background(), "project", "r1", path, root); err != ErrInvalidPolicy {
			t.Fatalf("checkout/root overlap accepted: %v", err)
		}
	}
	if _, err := os.Stat(child); !os.IsNotExist(err) {
		t.Fatal("overlapping root was created")
	}
	m := NewManaged("node")
	first, err := m.Apply(context.Background(), "first", "r1", path, "")
	if err != nil {
		t.Fatal(err)
	}
	other := managedCheckout(t)
	for _, pair := range [][2]string{
		{path, t.TempDir()},
		{other, first.WorktreeRoot},
		{other, filepath.Join(first.WorktreeRoot, "nested")},
		{other, filepath.Dir(first.WorktreeRoot)},
		{other, filepath.Join(path, "nested")},
	} {
		if _, err := m.Apply(context.Background(), "second", "r1", pair[0], pair[1]); err != ErrInvalidPolicy {
			t.Fatalf("cross-project overlap accepted %v: %v", pair, err)
		}
	}
	nestedCheckout := filepath.Join(first.WorktreeRoot, "nested-checkout")
	if out, err := exec.Command("git", "init", "--quiet", nestedCheckout).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	if _, err := m.Apply(context.Background(), "second", "r1", nestedCheckout, t.TempDir()); err != ErrInvalidPolicy {
		t.Fatalf("checkout inside root accepted: %v", err)
	}
}

func TestManagedRootSafety(t *testing.T) {
	path := managedCheckout(t)
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"relative", file, filepath.Join(t.TempDir(), "missing", "root")} {
		if _, err := NewManaged("node").Apply(context.Background(), "project", "r1", path, root); err != ErrInvalidPath {
			t.Fatalf("unsafe root accepted: %v", err)
		}
	}
	t.Run("symlink", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(t.TempDir(), root); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := NewManaged("node").Apply(context.Background(), "project", "r1", path, root); err != ErrInvalidPath {
			t.Fatalf("root symlink adopted: %v", err)
		}
	})
	t.Run("replacement", func(t *testing.T) {
		m := NewManaged("node")
		b, err := m.Apply(context.Background(), "project", "r1", path, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(b.WorktreeRoot, b.WorktreeRoot+"-old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(b.WorktreeRoot, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(b.WorktreeRoot, "unrelated"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Apply(context.Background(), "project", "r2", path, ""); err != ErrInvalidPath {
			t.Fatalf("replacement inherited ownership: %v", err)
		}
		if b.ValidateWorktreeRoot() != ErrInvalidPath {
			t.Fatal("old copy accepted replacement")
		}
	})
}

func TestManagedLegacyMigration(t *testing.T) {
	path, root := managedCheckout(t), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "legacy-worktree"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root))), "node-1")
	if err != nil {
		t.Fatal(err)
	}
	export := p.LegacyBindings()
	if len(export) != 1 {
		t.Fatal("missing export")
	}
	m := NewManaged("node-1")
	b, err := m.AdoptLegacy(context.Background(), export[0])
	if err != nil || b.ValidateWorktreeRoot() != nil {
		t.Fatalf("legacy adoption failed: %v", err)
	}
	if _, err := m.Resolve("node-1", "AgentFlow", "r1", "not-granted"); err != nil {
		t.Fatal("legacy grants leaked")
	}
	export[0].Revision = "mutated"
	if _, err := NewManaged("node-1").AdoptLegacy(context.Background(), export[0]); err != ErrInvalidPath {
		t.Fatal("forged legacy pin adopted")
	}
	if _, err := p.Resolve("node-1", "AgentFlow", "r1", "actor-1"); err != nil {
		t.Fatal("export mutated legacy policy")
	}
	if _, err := NewManaged("node-1").AdoptLegacy(context.Background(), Binding{
		NodeID: "node-1", ProjectID: "AgentFlow", Revision: "r1", Path: path, WorktreeRoot: root, AllowWorktrees: true,
	}); err != ErrInvalidPath {
		t.Fatal("unpinned fields adopted")
	}
	workspace := loadBinding(t, managedCheckout(t))
	if b, err := NewManaged("node-1").AdoptLegacy(context.Background(), workspace); err != nil || !b.AllowWorktrees {
		t.Fatalf("workspace-only migration failed: %v", err)
	}
}

func TestManagedValidationAndBounds(t *testing.T) {
	var nilManaged *Managed
	var nilPolicy *Policy
	if nilManaged.HasWorktrees() || NewManaged("node").HasWorktrees() || nilPolicy.LegacyBindings() != nil {
		t.Fatal("empty state enabled")
	}
	if _, err := nilManaged.Resolve("node", "project", "r1", ""); err != ErrDenied {
		t.Fatal("nil manager resolved")
	}
	for _, ids := range [][3]string{{"", "project", "r1"}, {"node", "", "r1"}, {"node", "project", ""}, {"node", "a/b", "r1"}, {"node", "p", strings.Repeat("r", 129)}} {
		if _, err := NewManaged(ids[0]).Apply(context.Background(), ids[1], ids[2], "", ""); err != ErrInvalidPolicy {
			t.Fatalf("invalid IDs accepted: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewManaged("node").Apply(ctx, "project", "r1", "", ""); err != ErrInvalidPath {
		t.Fatal("cancellation ignored")
	}
	m := NewManaged("node")
	for i := 0; i < maxBindings; i++ {
		m.bindings[fmt.Sprint(i)] = Binding{}
	}
	if _, err := m.Apply(context.Background(), "new", "r1", "", ""); err != ErrInvalidPolicy {
		t.Fatal("binding limit ignored")
	}
}

func TestManagedConcurrentReadersAndUpdates(t *testing.T) {
	m := NewManaged("node")
	path := managedCheckout(t)
	if _, err := m.Apply(context.Background(), "project", "r1", path, ""); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				b, err := m.Resolve("node", "project", "r1", "")
				if err != nil || b.ValidateWorktreeRoot() != nil || !m.HasWorktrees() {
					t.Error("concurrent resolution failed")
				}
			}
		}()
	}
	for i := 0; i < 3; i++ {
		if _, err := m.Apply(context.Background(), "project", "r1", path, ""); err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
}

func TestManagedGitOutputBound(t *testing.T) {
	var output boundedGitOutput
	if _, err := io.Copy(&output, strings.NewReader(strings.Repeat("a", 32771))); err != ErrInvalidPath {
		t.Fatalf("unbounded Git output: %v", err)
	}
	if output.buffer.Len() > 32770 {
		t.Fatal("Git output exceeded limit")
	}
}
