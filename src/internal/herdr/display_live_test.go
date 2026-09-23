package herdr

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestLiveDisplayMetadata(t *testing.T) {
	if os.Getenv("HERDR_MESH_DISPLAY_LIVE") != "1" {
		t.Skip("set HERDR_MESH_DISPLAY_LIVE=1 to read the existing native session without mutations")
	}
	executable, err := exec.LookPath("herdr")
	if err != nil {
		t.Fatal("native Herdr is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "api", "snapshot")
	command.Stderr = io.Discard
	data, err := command.Output()
	if err != nil {
		t.Fatal("read-only native snapshot failed", err)
	}
	var envelope struct {
		Result struct {
			Snapshot json.RawMessage `json:"snapshot"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Result.Snapshot) == 0 {
		t.Fatal("native snapshot envelope is invalid")
	}
	state, err := sanitizeSnapshot(envelope.Result.Snapshot)
	if err != nil {
		t.Fatal("live native display projection failed", err)
	}
	var native struct {
		Workspaces []struct {
			ID    string `json:"workspace_id"`
			Label string `json:"label"`
		} `json:"workspaces"`
		Agents []struct {
			ID         string `json:"pane_id"`
			Name       string `json:"name"`
			Cwd        string `json:"cwd"`
			Foreground string `json:"foreground_cwd"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(envelope.Result.Snapshot, &native); err != nil {
		t.Fatal("native display fields are invalid")
	}
	if len(native.Workspaces) != len(state.Workspaces) || len(native.Agents) != len(state.Agents) {
		t.Fatal("live inventory changed during projection")
	}
	for i, item := range native.Workspaces {
		if state.Workspaces[i].Id != item.ID || state.Workspaces[i].DisplayName != item.Label {
			t.Fatal("live workspace configured name was not preserved")
		}
	}
	for i, item := range native.Agents {
		directory := item.Foreground
		if directory == "" {
			directory = item.Cwd
		}
		if state.Agents[i].Id != item.ID || state.Agents[i].DisplayName != item.Name || state.Agents[i].Directory != directory {
			t.Fatal("live agent display metadata was not preserved")
		}
	}
	t.Logf("Verified display metadata for %d workspaces, %d tabs, %d panes, %d agents; no sessions modified",
		len(state.Workspaces), len(state.Tabs), len(state.Panes), len(state.Agents))
}
