package app

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestWorktreeFlagsRejectUnsafeInputsBeforeEnrollment(t *testing.T) {
	basic := []string{"ctl", "create-worktree", "-server", "server:50052", "-node", "node-1", "-project", "AgentFlow", "-binding-revision", "r2"}
	for _, suffix := range [][]string{
		{},
		{"-name", "task", "-branch", "task"},
		{"-name", "../escape", "-branch", "task", "-base-commit", strings.Repeat("a", 40)},
		{"-name", "task", "-branch", "--danger", "-base-commit", strings.Repeat("a", 40)},
		{"-name", "task", "-branch", "task", "-base-commit", "HEAD"},
		{"-name", "task", "-branch", "task", "-base-commit", strings.Repeat("a", 40), "-ttl", "31s"},
	} {
		args := append(append([]string{}, basic...), suffix...)
		if err := Run(context.Background(), args, IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}); err == nil {
			t.Fatalf("unsafe flags accepted: %v", args)
		}
	}
}
