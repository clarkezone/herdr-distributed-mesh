package identity

import (
	"sync"
	"testing"
)

func TestLoadOrCreatePersistsIdentity(t *testing.T) {
	stateDir := t.TempDir()

	first, err := LoadOrCreate(stateDir)
	if err != nil {
		t.Fatalf("LoadOrCreate() first error = %v", err)
	}
	second, err := LoadOrCreate(stateDir)
	if err != nil {
		t.Fatalf("LoadOrCreate() second error = %v", err)
	}
	if first == "" {
		t.Fatal("LoadOrCreate() returned an empty identity")
	}
	if first != second {
		t.Fatalf("LoadOrCreate() identities differ: %q != %q", first, second)
	}
}

func TestLoadOrCreateConcurrentCallersShareIdentity(t *testing.T) {
	stateDir := t.TempDir()
	const callerCount = 16

	identities := make(chan string, callerCount)
	errors := make(chan error, callerCount)
	var callers sync.WaitGroup
	for range callerCount {
		callers.Add(1)
		go func() {
			defer callers.Done()
			id, err := LoadOrCreate(stateDir)
			if err != nil {
				errors <- err
				return
			}
			identities <- id
		}()
	}
	callers.Wait()
	close(identities)
	close(errors)

	for err := range errors {
		t.Errorf("LoadOrCreate() error = %v", err)
	}
	var expected string
	for id := range identities {
		if expected == "" {
			expected = id
		}
		if id != expected {
			t.Errorf("LoadOrCreate() identity = %q, want %q", id, expected)
		}
	}
}
