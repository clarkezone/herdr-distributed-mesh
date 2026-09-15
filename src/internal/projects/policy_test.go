package projects

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writePolicy(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func policyJSON(t *testing.T, entries ...map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"projects": entries})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func policyEntry(path string) map[string]any {
	entry := map[string]any{
		"project_id": "AgentFlow", "node_id": "node-1", "binding_revision": "r1",
		"actor_ids": []string{"actor-1", "actor-2"},
	}
	if path != "" {
		entry["path"] = path
	}
	return entry
}

func gitCheckout(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private-checkout")
	if err := os.MkdirAll(filepath.Join(path, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadBinding(t *testing.T, path string) Binding {
	t.Helper()
	p, err := Load(writePolicy(t, policyJSON(t, policyEntry(path))), "node-1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Resolve("node-1", "AgentFlow", "r1", "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestResolveExactAuthorization(t *testing.T) {
	for _, nodePolicy := range []bool{false, true} {
		t.Run(map[bool]string{false: "coordinator", true: "node"}[nodePolicy], func(t *testing.T) {
			node, path := "", ""
			if nodePolicy {
				node, path = "node-1", gitCheckout(t)
			}
			p, err := Load(writePolicy(t, policyJSON(t, policyEntry(path))), node)
			if err != nil {
				t.Fatal(err)
			}
			for _, actor := range []string{"actor-1", "actor-2"} {
				b, err := p.Resolve("node-1", "AgentFlow", "r1", actor)
				if err != nil || b.ProjectID != "AgentFlow" || b.NodeID != "node-1" || b.Revision != "r1" {
					t.Fatalf("binding: %+v, %v", b, err)
				}
				if nodePolicy {
					if b.ValidatePath() != nil || !filepath.IsAbs(b.Path) {
						t.Fatal("node binding was not pinned")
					}
					b.Path = gitCheckout(t)
					if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
						t.Fatal("modified binding accepted")
					}
				} else if b.Path != "" || !errors.Is(b.ValidatePath(), ErrInvalidPath) {
					t.Fatal("coordinator binding permits local execution")
				}
			}
			for _, args := range [][4]string{
				{"node-2", "AgentFlow", "r1", "actor-1"},
				{"node-1", "agentflow", "r1", "actor-1"},
				{"node-1", "AgentFlow", "r2", "actor-1"},
				{"node-1", "AgentFlow", "r1", "Actor-1"},
				{"node-1", "AgentFlow", "r1", "*"},
				{"", "", "", ""},
			} {
				b, err := p.Resolve(args[0], args[1], args[2], args[3])
				if !errors.Is(err, ErrDenied) || b.ProjectID != "" || b.Path != "" {
					t.Fatalf("unauthorized resolve: %+v, %v", b, err)
				}
			}
			var nilPolicy *Policy
			if _, err := nilPolicy.Resolve("node-1", "AgentFlow", "r1", "actor-1"); !errors.Is(err, ErrDenied) {
				t.Fatal("nil policy allows resolution")
			}
		})
	}
}

func TestPolicyStrictJSON(t *testing.T) {
	valid := string(policyJSON(t, policyEntry("")))
	tests := map[string]string{
		"empty":                 "",
		"null":                  "null",
		"array":                 "[]",
		"empty policy":          `{"projects":[]}`,
		"null projects":         `{"projects":null}`,
		"null binding":          `{"projects":[null]}`,
		"missing projects":      `{}`,
		"wrong root case":       strings.Replace(valid, "projects", "Projects", 1),
		"unknown root field":    strings.Replace(valid, `{"projects":`, `{"secret":"private","projects":`, 1),
		"duplicate root":        `{"projects":[],"projects":[]}`,
		"duplicate escaped key": `{"projects":[],"\u0070rojects":[]}`,
		"duplicate binding key": strings.Replace(valid, `"project_id":"AgentFlow"`, `"project_id":"AgentFlow","project_id":"AgentFlow"`, 1),
		"unknown binding field": strings.Replace(valid, `"project_id":`, `"secret":"private","project_id":`, 1),
		"wrong binding case":    strings.Replace(valid, "project_id", "Project_ID", 1),
		"null identifier":       strings.Replace(valid, `"AgentFlow"`, "null", 1),
		"number identifier":     strings.Replace(valid, `"AgentFlow"`, "12", 1),
		"unsafe identifier":     strings.Replace(valid, `"AgentFlow"`, `"../private"`, 1),
		"dot identifier":        strings.Replace(valid, `"AgentFlow"`, `"Agent.Flow"`, 1),
		"unicode identifier":    strings.Replace(valid, `"AgentFlow"`, `"\u00e9"`, 1),
		"oversized identifier":  strings.Replace(valid, "AgentFlow", strings.Repeat("a", 129), 1),
		"empty identifier":      strings.Replace(valid, `"AgentFlow"`, `""`, 1),
		"trailing value":        valid + `{}`,
		"trailing garbage":      valid + "garbage",
		"invalid utf8":          strings.Replace(valid, "AgentFlow", "\xff", 1),
		"unbounded nesting":     `{"projects":` + strings.Repeat("[", 20) + "0" + strings.Repeat("]", 20) + "}",
		"duplicate actors":      strings.Replace(valid, `"actor-2"`, `"actor-1"`, 1),
		"null actor":            strings.Replace(valid, `"actor-2"`, "null", 1),
		"wildcard actor":        strings.Replace(valid, `"actor-2"`, `"*"`, 1),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writePolicy(t, []byte(data)), ""); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("got %v", err)
			}
		})
	}
	for _, field := range []string{"project_id", "node_id", "binding_revision", "actor_ids"} {
		t.Run("missing "+field, func(t *testing.T) {
			entry := policyEntry("")
			delete(entry, field)
			if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatal(err)
			}
		})
	}
	for _, actors := range []any{nil, []string{}, []string{""}, "actor-1", []int{1}} {
		entry := policyEntry("")
		entry["actor_ids"] = actors
		if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("invalid actors accepted: %v", actors)
		}
	}
}

func TestPolicyBounds(t *testing.T) {
	valid := policyJSON(t, policyEntry(""))
	exact := append(append([]byte{}, valid...), []byte(strings.Repeat(" ", maxPolicyBytes-len(valid)))...)
	if _, err := Load(writePolicy(t, exact), ""); err != nil {
		t.Fatalf("64 KiB valid policy: %v", err)
	}
	if _, err := Load(writePolicy(t, append(exact, ' ')), ""); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("oversized policy: %v", err)
	}
	entries := make([]map[string]any, maxBindings)
	for i := range entries {
		entries[i] = policyEntry("")
		entries[i]["node_id"] = "node-" + string(rune('A'+i/26)) + string(rune('A'+i%26))
	}
	if _, err := Load(writePolicy(t, policyJSON(t, entries...)), ""); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, policyEntry(""))
	if _, err := Load(writePolicy(t, policyJSON(t, entries...)), ""); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("too many bindings: %v", err)
	}
	actors := make([]string, maxActors)
	for i := range actors {
		actors[i] = "actor-" + string(rune('A'+i/26)) + string(rune('A'+i%26))
	}
	entry := policyEntry("")
	entry["actor_ids"] = actors
	if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); err != nil {
		t.Fatal(err)
	}
	entry["actor_ids"] = append(actors, "one-more")
	if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("too many actors: %v", err)
	}
	entry = policyEntry("")
	entry["project_id"] = strings.Repeat("a", 128)
	if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); err != nil {
		t.Fatalf("128-byte identifier: %v", err)
	}
}

func TestPolicyPathRequirementsAndRedaction(t *testing.T) {
	git := gitCheckout(t)
	for name, entry := range map[string]map[string]any{
		"missing":     policyEntry(""),
		"relative":    policyEntry("private-checkout"),
		"nonexistent": policyEntry(filepath.Join(t.TempDir(), "private-missing")),
		"not git":     policyEntry(t.TempDir()),
		"remote UNC":  policyEntry(`\\private-host\share\checkout`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writePolicy(t, policyJSON(t, entry)), "node-1")
			if !errors.Is(err, ErrInvalidPath) || strings.Contains(err.Error(), "private") {
				t.Fatalf("path validation: %v", err)
			}
		})
	}
	file := filepath.Join(t.TempDir(), "private-file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(writePolicy(t, policyJSON(t, policyEntry(file))), "node-1"); !errors.Is(err, ErrInvalidPath) {
		t.Fatal("regular file checkout accepted")
	}
	for _, node := range []string{"", "node-other", "*"} {
		if _, err := Load(writePolicy(t, policyJSON(t, policyEntry(git))), node); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("wrong node/coordinator accepted: %v", err)
		}
	}
	entry := policyEntry("")
	entry["path"] = ""
	if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); err != nil {
		t.Fatalf("empty coordinator path: %v", err)
	}
	entry["path"] = nil
	if _, err := Load(writePolicy(t, policyJSON(t, entry)), ""); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("null path: %v", err)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "private-missing"), ""); !errors.Is(err, ErrPolicyRead) {
		t.Fatalf("read failure: %v", err)
	}
	if _, err := Load(t.TempDir(), ""); !errors.Is(err, ErrPolicyRead) {
		t.Fatalf("directory policy: %v", err)
	}
}

func TestPolicyDuplicateBindingsAndAliases(t *testing.T) {
	one := policyEntry("")
	two := policyEntry("")
	two["binding_revision"] = "r2"
	if _, err := Load(writePolicy(t, policyJSON(t, one, two)), ""); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatal("duplicate project/node pair accepted")
	}
	git := gitCheckout(t)
	one, two = policyEntry(git), policyEntry(filepath.Join(git, "."))
	two["project_id"] = "Other"
	if _, err := Load(writePolicy(t, policyJSON(t, one, two)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatal("duplicate canonical checkout accepted")
	}
	if runtime.GOOS == "windows" {
		two["path"] = strings.ToUpper(git)
		if _, err := Load(writePolicy(t, policyJSON(t, one, two)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatal("case alias accepted")
		}
	}
	link := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(git, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	two["path"] = link
	if _, err := Load(writePolicy(t, policyJSON(t, one, two)), "node-1"); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatal("symlink alias accepted")
	}
}

func TestValidatePathRejectsChanges(t *testing.T) {
	t.Run("forged", func(t *testing.T) {
		b := Binding{ProjectID: "AgentFlow", NodeID: "node-1", Revision: "r1", Path: gitCheckout(t)}
		if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
			t.Fatal("forged binding accepted")
		}
	})
	t.Run("replacement", func(t *testing.T) {
		path := gitCheckout(t)
		b := loadBinding(t, path)
		if err := os.Rename(path, path+"-original"); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(path, ".git"), 0700); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
			t.Fatal("directory replacement accepted")
		}
	})
	t.Run("git marker removed", func(t *testing.T) {
		path := gitCheckout(t)
		b := loadBinding(t, path)
		if err := os.Remove(filepath.Join(path, ".git")); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
			t.Fatal("checkout without marker accepted")
		}
	})
	t.Run("git file", func(t *testing.T) {
		path := t.TempDir()
		if err := os.WriteFile(filepath.Join(path, ".git"), []byte("gitdir: local-admin-metadata\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if loadBinding(t, path).ValidatePath() != nil {
			t.Fatal("Git file marker rejected")
		}
	})
	t.Run("symlink retarget", func(t *testing.T) {
		first, second := gitCheckout(t), gitCheckout(t)
		link := filepath.Join(t.TempDir(), "configured-link")
		if err := os.Symlink(first, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		b := loadBinding(t, link)
		canonical, _ := filepath.EvalSymlinks(first)
		if b.Path != canonical {
			t.Fatal("binding did not expose canonical target")
		}
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(second, link); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
			t.Fatal("retargeted configured symlink accepted")
		}
	})
	t.Run("canonical destination retarget", func(t *testing.T) {
		path, other := gitCheckout(t), gitCheckout(t)
		b := loadBinding(t, path)
		if err := os.Rename(path, path+"-original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other, path); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if !errors.Is(b.ValidatePath(), ErrInvalidPath) {
			t.Fatal("retargeted canonical destination accepted")
		}
	})
}
