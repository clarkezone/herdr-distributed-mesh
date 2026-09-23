package projects

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestProjectForRepositoryMatchesIdentityNotNames(t *testing.T) {
	first, second := managedCheckout(t), managedCheckout(t)
	m := NewManaged("node")
	if _, err := m.Apply(context.Background(), "one", "r1", first, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(context.Background(), "two", "r1", second, ""); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(first, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ path, want string }{
		{first, "one"}, {second, "two"}, {child, ""}, {managedCheckout(t), ""},
	} {
		got, err := m.ProjectForRepository(test.path)
		if err != nil || got != test.want {
			t.Fatalf("project = %q, %v; want %q", got, err, test.want)
		}
	}
}

func TestProjectForRepositoryRejectsReplacedBinding(t *testing.T) {
	path := managedCheckout(t)
	m := NewManaged("node")
	if _, err := m.Apply(context.Background(), "project", "r1", path, ""); err != nil {
		t.Fatal(err)
	}
	moved := path + "-moved"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := m.ProjectForRepository(path); err != nil || got != "" {
		t.Fatalf("replacement inherited project: %q, %v", got, err)
	}
	if got, err := m.ProjectForRepository(moved); got != "" || !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("invalid binding still mapped original: %q, %v", got, err)
	}
}

func TestProjectForRepositoryInvalidAndEmptyRegistry(t *testing.T) {
	m := NewManaged("node")
	for _, path := range []string{"", "relative", `\\host\share`, filepath.Join(t.TempDir(), "bad\x00")} {
		if got, err := m.ProjectForRepository(path); got != "" || !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("invalid path accepted: %q, %v", got, err)
		}
	}
	for _, registry := range []*Managed{m, nil} {
		if got, err := registry.ProjectForRepository(t.TempDir()); got != "" || err != nil {
			t.Fatalf("empty registry: %q, %v", got, err)
		}
	}
}

func TestProjectForRepositoryRejectsAmbiguousPins(t *testing.T) {
	path := managedCheckout(t)
	first, second := NewManaged("node"), NewManaged("node")
	if _, err := first.Apply(context.Background(), "one", "r1", path, ""); err != nil {
		t.Fatal(err)
	}
	b, err := second.Apply(context.Background(), "two", "r1", path, filepath.Join(t.TempDir(), "trees"))
	if err != nil {
		t.Fatal(err)
	}
	first.bindings["two"] = b
	if got, err := first.ProjectForRepository(path); got != "" || !errors.Is(err, ErrAmbiguousRepository) {
		t.Fatalf("ambiguous identity assigned a project: %q, %v", got, err)
	}
}

func TestProjectForRepositoryMatchesAlias(t *testing.T) {
	path := managedCheckout(t)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	m := NewManaged("node")
	if _, err := m.Apply(context.Background(), "project", "r1", path, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := m.ProjectForRepository(alias); got != "project" || err != nil {
		t.Fatalf("alias failed identity match: %q, %v", got, err)
	}
}
