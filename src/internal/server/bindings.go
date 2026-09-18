package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type nodeBinding struct {
	StableID   string `json:"stable_id"`
	InstanceID string `json:"instance_id"`
}

type bindingStore struct {
	mu         sync.Mutex
	path       string
	byInstance map[string]string
	byStableID map[string]string
}

func newBindingStore(path string) (*bindingStore, error) {
	store := &bindingStore{
		path:       path,
		byInstance: map[string]string{},
		byStableID: map[string]string{},
	}
	if path == "" {
		return store, nil
	}
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read node identity bindings: %w", err)
	}

	trimmed := bytes.TrimSpace(content)
	if len(trimmed) == 0 {
		return store, nil
	}
	if trimmed[0] == '[' {
		var bindings []nodeBinding
		if err := json.Unmarshal(trimmed, &bindings); err != nil {
			return nil, fmt.Errorf("decode node identity bindings: %w", err)
		}
		for _, binding := range bindings {
			if err := store.record(binding); err != nil {
				return nil, fmt.Errorf("load node identity binding: %w", err)
			}
		}
		return store, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		var binding nodeBinding
		if err := json.Unmarshal(scanner.Bytes(), &binding); err != nil {
			return nil, fmt.Errorf("decode node identity binding: %w", err)
		}
		if err := store.record(binding); err != nil {
			return nil, fmt.Errorf("load node identity binding: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read node identity bindings: %w", err)
	}
	return store, nil
}

func (store *bindingStore) Bind(stableID, instanceID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	binding := nodeBinding{StableID: stableID, InstanceID: instanceID}
	if existing, ok := store.byStableID[stableID]; ok {
		if existing != instanceID {
			return fmt.Errorf("Tailscale stable ID %q is already bound to mesh instance %q", stableID, existing)
		}
		return nil
	}
	if existing, ok := store.byInstance[instanceID]; ok {
		return fmt.Errorf("mesh instance %q is already bound to Tailscale stable ID %q", instanceID, existing)
	}
	if err := store.persistWith(binding); err != nil {
		return err
	}
	return store.record(binding)
}

func (store *bindingStore) persistWith(binding nodeBinding) error {
	if store.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		return fmt.Errorf("create node identity binding directory: %w", err)
	}
	bindings := make([]nodeBinding, 0, len(store.byStableID)+1)
	for stableID, instanceID := range store.byStableID {
		bindings = append(bindings, nodeBinding{StableID: stableID, InstanceID: instanceID})
	}
	bindings = append(bindings, binding)
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].StableID < bindings[j].StableID
	})
	encoded, err := json.MarshalIndent(bindings, "", "  ")
	if err != nil {
		return fmt.Errorf("encode node identity bindings: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(store.path), "node-bindings-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary node identity bindings: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect temporary node identity bindings: %w", err)
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary node identity bindings: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary node identity bindings: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary node identity bindings: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("publish node identity bindings: %w", err)
	}
	return nil
}

func (store *bindingStore) record(binding nodeBinding) error {
	if binding.StableID == "" || binding.InstanceID == "" {
		return errors.New("node identity binding contains an empty identifier")
	}
	if existing, ok := store.byStableID[binding.StableID]; ok && existing != binding.InstanceID {
		return fmt.Errorf("Tailscale stable ID %q has conflicting mesh instances", binding.StableID)
	}
	if existing, ok := store.byInstance[binding.InstanceID]; ok && existing != binding.StableID {
		return fmt.Errorf("mesh instance %q has conflicting Tailscale stable IDs", binding.InstanceID)
	}
	store.byStableID[binding.StableID] = binding.InstanceID
	store.byInstance[binding.InstanceID] = binding.StableID
	return nil
}
