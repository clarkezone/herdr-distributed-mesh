package scripthost

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostSecretEnvironmentIsChildOnly(t *testing.T) {
	t.Setenv("HERDR_SCRIPT_SECRET", "original")
	secret := "tskey-" + "api-process-only"
	_, err := run(context.Background(), Request{
		Script: []byte("param($OptionsPath)"), Options: struct{}{},
		Environment: map[string]string{"HERDR_SCRIPT_SECRET": secret},
	}, os.Executable, func(_ context.Context, cmd *exec.Cmd) (bool, error) {
		if strings.Contains(strings.Join(cmd.Args, " "), secret) {
			t.Fatal("secret in argv")
		}
		count := 0
		for _, entry := range cmd.Env {
			if strings.HasPrefix(entry, "HERDR_SCRIPT_SECRET=") {
				count++
				if entry != "HERDR_SCRIPT_SECRET="+secret {
					t.Fatal("wrong environment override")
				}
			}
		}
		if count != 1 {
			t.Fatal("missing/duplicate token environment")
		}
		entries, err := os.ReadDir(cmd.Dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			data, err := os.ReadFile(filepath.Join(cmd.Dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), secret) {
				t.Fatal("secret on disk")
			}
		}
		if os.Getenv("HERDR_SCRIPT_SECRET") != "original" {
			t.Fatal("parent environment changed")
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
