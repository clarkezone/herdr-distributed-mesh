package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const fileName = "instance-id"

func LoadOrCreate(stateDir string) (string, error) {
	if strings.TrimSpace(stateDir) == "" {
		return "", errors.New("state directory is required")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return "", fmt.Errorf("create state directory: %w", err)
	}

	path := filepath.Join(stateDir, fileName)
	if id, err := read(path); err == nil {
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate instance ID: %w", err)
	}
	id := hex.EncodeToString(random)

	temporary, err := os.CreateTemp(stateDir, fileName+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create temporary instance ID: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return "", fmt.Errorf("set temporary instance ID permissions: %w", err)
	}
	if _, err := temporary.WriteString(id + "\n"); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write temporary instance ID: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", fmt.Errorf("sync temporary instance ID: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close temporary instance ID: %w", err)
	}

	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return read(path)
		}
		return "", fmt.Errorf("publish instance ID: %w", err)
	}
	return id, nil
}

func read(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read instance ID: %w", err)
	}
	id := strings.TrimSpace(string(content))
	if id == "" {
		return "", fmt.Errorf("instance ID file %q is empty", path)
	}
	return id, nil
}
