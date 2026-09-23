package projects

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func worktreeEntry(path, root string) map[string]any {
	entry := policyEntry(path)
	entry["allow_worktrees"] = true
	entry["worktree_root"] = root
	return entry
}

func worktreeBinding(t *testing.T, path, root string) Binding {
	t.Helper()
	p, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root))), "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasWorktrees() {
		t.Fatal("opt-in not reported")
	}
	b, err := p.Resolve("node-1", "AgentFlow", "r1", "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWorktreePolicyExplicitOptIn(t *testing.T) {
	var nilPolicy *Policy
	if nilPolicy.HasWorktrees() {
		t.Fatal("nil policy enabled")
	}
	for _, node := range []string{"", "node-1"} {
		entry := policyEntry("")
		if node != "" {
			entry["path"] = gitCheckout(t)
		}
		for _, explicitFalse := range []bool{false, true} {
			if explicitFalse {
				entry["allow_worktrees"] = false
			}
			p, err := Load(writePolicy(t, policyJSON(t, entry)), node)
			if err != nil || p.HasWorktrees() {
				t.Fatalf("old/false policy changed: %v", err)
			}
			b, _ := p.Resolve("node-1", "AgentFlow", "r1", "actor-1")
			if b.AllowWorktrees || b.WorktreeRoot != "" || b.ValidateWorktreeRoot() == nil {
				t.Fatal("unopted binding allowed")
			}
			b.AllowWorktrees, b.WorktreeRoot = true, t.TempDir()
			if _, err := b.WorktreeDestination("new"); !errors.Is(err, ErrInvalidPath) {
				t.Fatal("mutable exported fields forged authorization")
			}
		}
	}
	entry := policyEntry("")
	entry["allow_worktrees"] = true
	p, err := Load(writePolicy(t, policyJSON(t, entry)), "")
	if err != nil || !p.HasWorktrees() {
		t.Fatalf("server opt-in: %v", err)
	}
	b, _ := p.Resolve("node-1", "AgentFlow", "r1", "actor-1")
	if b.ValidateWorktreeRoot() == nil {
		t.Fatal("server allowed local execution")
	}
	for _, root := range []any{"", t.TempDir(), nil} {
		entry["worktree_root"] = root
		if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("server accepted root: %v", err)
		}
	}
}

func TestWorktreePolicyStrictFields(t *testing.T) {
	path, root := gitCheckout(t), t.TempDir()
	for name, change := range map[string]func(map[string]any){
		"null allow":        func(e map[string]any) { e["allow_worktrees"] = nil },
		"string allow":      func(e map[string]any) { e["allow_worktrees"] = "true" },
		"number allow":      func(e map[string]any) { e["allow_worktrees"] = 1 },
		"null root":         func(e map[string]any) { e["worktree_root"] = nil },
		"number root":       func(e map[string]any) { e["worktree_root"] = 1 },
		"false latent root": func(e map[string]any) { e["allow_worktrees"] = false },
		"old latent root":   func(e map[string]any) { delete(e, "allow_worktrees") },
	} {
		t.Run(name, func(t *testing.T) {
			entry := worktreeEntry(path, root)
			change(entry)
			if _, err := Load(writePolicy(t, policyJSON(t, entry)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("invalid field accepted: %v", err)
			}
		})
	}
	data := string(policyJSON(t, worktreeEntry(path, root)))
	data = strings.Replace(data, `"allow_worktrees":true`, `"allow_worktrees":false,"allow_worktrees":true`, 1)
	if _, err := Load(writePolicy(t, []byte(data)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatal("duplicate opt-in accepted")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"", "relative", filepath.Join(t.TempDir(), "absent"), file,
		filepath.VolumeName(path) + string(os.PathSeparator), `\\host\share\root`} {
		if _, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root))), "node-1"); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("invalid root accepted: %v", err)
		}
	}
}

func TestWorktreeDestinationAbsentPortableLeaf(t *testing.T) {
	b := worktreeBinding(t, gitCheckout(t), t.TempDir())
	for _, name := range []string{"new", "a", strings.Repeat("a", 64), "new_1-2"} {
		dest, err := b.WorktreeDestination(name)
		if err != nil || dest != filepath.Join(b.WorktreeRoot, name) {
			t.Fatalf("destination: %q, %v", dest, err)
		}
		if _, err := os.Lstat(dest); !os.IsNotExist(err) {
			t.Fatal("destination reserved")
		}
	}
	for _, name := range []string{"", ".", "..", "../escape", `..\escape`, "a/b", `a\b`, "UPPER",
		"con", "aux", "nul", "prn", "com0", "com1", "lpt9", "a.", "a ", "a:b", "café", strings.Repeat("a", 65)} {
		if dest, err := b.WorktreeDestination(name); dest != "" || !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("unsafe leaf accepted: %q", name)
		}
	}
	for _, kind := range []string{"file", "directory", "symlink", "dangling"} {
		t.Run(kind, func(t *testing.T) {
			dest := filepath.Join(b.WorktreeRoot, kind)
			var err error
			switch kind {
			case "file":
				err = os.WriteFile(dest, nil, 0600)
			case "directory":
				err = os.Mkdir(dest, 0700)
			default:
				target := t.TempDir()
				if kind == "dangling" {
					target = filepath.Join(target, "absent")
				}
				err = os.Symlink(target, dest)
				if err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.WorktreeDestination(kind); !errors.Is(err, ErrInvalidPath) {
				t.Fatal("existing target accepted")
			}
		})
	}
}

func TestWorktreePolicyOverlaps(t *testing.T) {
	path := gitCheckout(t)
	child := filepath.Join(path, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{path, filepath.Dir(path), child} {
		if _, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root))), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("checkout overlap accepted: %v", err)
		}
	}
	for _, reversed := range []bool{false, true} {
		root := t.TempDir()
		nested := filepath.Join(root, "nested")
		if err := os.MkdirAll(filepath.Join(nested, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
		for _, other := range []map[string]any{
			policyEntry(nested),
			worktreeEntry(gitCheckout(t), root),
			worktreeEntry(gitCheckout(t), nested),
		} {
			other["project_id"] = "Other"
			entries := []map[string]any{worktreeEntry(path, root), other}
			if reversed {
				entries[0], entries[1] = entries[1], entries[0]
			}
			if _, err := Load(writePolicy(t, policyJSON(t, entries...)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("cross-binding overlap accepted: %v", err)
			}
		}
	}
	t.Run("aliases", func(t *testing.T) {
		root := t.TempDir()
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(root, alias); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		other := worktreeEntry(gitCheckout(t), alias)
		other["project_id"] = "Other"
		if _, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root), other)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatal("aliased roots accepted")
		}
	})
	if runtime.GOOS == "windows" {
		root := t.TempDir()
		other := worktreeEntry(gitCheckout(t), strings.ToUpper(root))
		other["project_id"] = "Other"
		if _, err := Load(writePolicy(t, policyJSON(t, worktreeEntry(path, root), other)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatal("case-aliased roots accepted")
		}
	}
}

func TestWorktreeRootPins(t *testing.T) {
	b := worktreeBinding(t, gitCheckout(t), t.TempDir())
	for _, change := range []func(*Binding){
		func(b *Binding) { b.AllowWorktrees = false },
		func(b *Binding) { b.WorktreeRoot = t.TempDir() },
		func(b *Binding) { b.Path = gitCheckout(t) },
		func(b *Binding) { b.ProjectID = "Other" },
		func(b *Binding) { b.NodeID = "Other" },
		func(b *Binding) { b.Revision = "r2" },
	} {
		copy := b
		change(&copy)
		if copy.ValidateWorktreeRoot() == nil {
			t.Fatal("modified binding accepted")
		}
	}
	for _, target := range []string{"root", "checkout"} {
		t.Run(target+" replacement", func(t *testing.T) {
			b := worktreeBinding(t, gitCheckout(t), t.TempDir())
			path := b.WorktreeRoot
			if target == "checkout" {
				path = b.Path
			}
			if err := os.Rename(path, path+"-old"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				// Restore only the specific empty replacement created by this test.
				if target == "checkout" {
					os.Remove(filepath.Join(path, ".git"))
				}
				os.Remove(path)
				os.Rename(path+"-old", path)
			})
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			if target == "checkout" {
				if err := os.Mkdir(filepath.Join(path, ".git"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if b.ValidateWorktreeRoot() == nil {
				t.Fatal("replacement accepted")
			}
		})
	}
	t.Run("configured alias retarget", func(t *testing.T) {
		root, other := t.TempDir(), t.TempDir()
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(root, alias); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		b := worktreeBinding(t, gitCheckout(t), alias)
		if err := os.Remove(alias); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other, alias); err != nil {
			t.Fatal(err)
		}
		if b.ValidateWorktreeRoot() == nil {
			t.Fatal("retargeted alias accepted")
		}
	})
}
