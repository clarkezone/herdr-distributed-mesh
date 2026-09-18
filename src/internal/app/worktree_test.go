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

func TestWorkspaceCommandOptionalBindingsAndOverrides(t *testing.T) {
	streams := IO{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}
	for _, command := range []string{"ensure-workspace", "create-worktree"} {
		basic := []string{"-server", "server:50052", "-node", "node-1", "-project", "AgentFlow", "-idempotency-key", "retry-1"}
		if command == "create-worktree" {
			basic = append(basic, "-name", "task")
		}
		query, err := parseCommandQuery(command, basic, streams)
		if err != nil {
			t.Fatal(err)
		}
		if query.worktree.BindingRevision != "" || query.worktree.BaseCommit != "" || query.key != "retry-1" {
			t.Fatal("omitted values or retry identity changed")
		}
		if command == "create-worktree" && query.worktree.Branch != "task" {
			t.Fatal("branch did not default to name")
		}
		args := append(append([]string{}, basic...), "-binding-revision", "r2")
		if command == "create-worktree" {
			args = append(args, "-branch", "custom", "-base-commit", strings.Repeat("a", 40))
		}
		query, err = parseCommandQuery(command, args, streams)
		if err != nil || query.worktree.BindingRevision != "r2" || query.key != "retry-1" {
			t.Fatalf("explicit retry override lost: %+v %v", query, err)
		}
		if command == "create-worktree" && (query.worktree.Branch != "custom" || query.worktree.BaseCommit != strings.Repeat("a", 40)) {
			t.Fatal("exact worktree overrides lost")
		}
	}
}
