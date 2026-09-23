package herdr

import (
	"bytes"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
)

func displayMetadata(entry object, kind string) (string, string, error) {
	nameField := "label"
	if kind == "agents" {
		nameField = "name"
	}
	name, err := displayString(entry, nameField)
	if err != nil {
		return "", "", err
	}
	var directory string
	switch kind {
	case "workspaces":
		if raw, ok := entry["worktree"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			var worktree object
			if decodeRequired(raw, &worktree) != nil {
				return "", "", apiError("invalid_snapshot")
			}
			directory, err = displayString(worktree, "checkout_path")
		}
	case "panes", "agents":
		directory, err = displayString(entry, "foreground_cwd")
		if err == nil && directory == "" {
			directory, err = displayString(entry, "cwd")
		}
	}
	if err != nil || !protocol.ValidDisplayMetadata(name, directory) {
		return "", "", apiError("invalid_snapshot")
	}
	return name, directory, nil
}

func displayString(entry object, key string) (string, error) {
	raw, exists := entry[key]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var value string
	if decodeRequired(raw, &value) != nil {
		return "", apiError("invalid_snapshot")
	}
	return value, nil
}
