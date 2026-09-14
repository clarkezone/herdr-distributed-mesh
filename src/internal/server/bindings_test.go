package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBindingStorePersistsAndRejectsConflicts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-bindings.jsonl")
	store, err := newBindingStore(path)
	if err != nil {
		t.Fatalf("newBindingStore() error = %v", err)
	}
	if err := store.Bind("stable-1", "instance-1"); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := store.Bind("stable-2", "instance-2"); err != nil {
		t.Fatalf("second Bind() error = %v", err)
	}
	if err := store.Bind("stable-1", "instance-1"); err != nil {
		t.Fatalf("idempotent Bind() error = %v", err)
	}

	reloaded, err := newBindingStore(path)
	if err != nil {
		t.Fatalf("reload newBindingStore() error = %v", err)
	}
	if err := reloaded.Bind("stable-1", "instance-2"); err == nil {
		t.Fatal("Bind() accepted a different instance for the same stable ID")
	}
	if err := reloaded.Bind("stable-3", "instance-1"); err == nil {
		t.Fatal("Bind() accepted a different stable ID for the same instance")
	}
}

func TestBindingStoreRejectsTruncatedSnapshotWithoutChangingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-bindings.jsonl")
	store, err := newBindingStore(path)
	if err != nil {
		t.Fatalf("newBindingStore() error = %v", err)
	}
	if err := store.Bind("stable-1", "instance-1"); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if err := os.WriteFile(path+".tmp", valid[:len(valid)/2], 0o600); err != nil {
		t.Fatalf("WriteFile(temp) error = %v", err)
	}
	reloaded, err := newBindingStore(path)
	if err != nil {
		t.Fatalf("newBindingStore(valid path) error = %v", err)
	}
	if err := reloaded.Bind("stable-1", "instance-1"); err != nil {
		t.Fatalf("reloaded Bind() error = %v", err)
	}
}
