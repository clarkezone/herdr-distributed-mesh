package herdr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const maxWorktreeGitOutput = 64 * 1024

type worktreeGit struct {
	executable string
	timeout    time.Duration
}

type worktreeGitState struct {
	common, head, branch string
}

func newWorktreeGit(timeout time.Duration) (worktreeGit, error) {
	path, err := exec.LookPath("git")
	if err != nil || !filepath.IsAbs(path) {
		return worktreeGit{}, ErrWorkspaceUnavailable
	}
	return worktreeGit{path, timeout}, nil
}

type worktreeGitOutput struct {
	buffer   bytes.Buffer
	overflow bool
}

func (b *worktreeGitOutput) Len() int       { return b.buffer.Len() }
func (b *worktreeGitOutput) String() string { return b.buffer.String() }

func (b *worktreeGitOutput) Write(p []byte) (int, error) {
	if len(p) > maxWorktreeGitOutput-b.Len() {
		b.overflow = true
		return 0, io.ErrShortWrite
	}
	return b.buffer.Write(p)
}

// Only fixed read commands call run. Inherited Git routing/config injections
// are removed so -C always names the authorized repository. No shell, hooks,
// optional locks, credentials, or interactive prompts are needed for these reads.
func (g worktreeGit) run(ctx context.Context, cwd string, args ...string) (string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	command := exec.CommandContext(ctx, g.executable, append([]string{"--no-optional-locks", "-C", cwd}, args...)...)
	command.WaitDelay = 100 * time.Millisecond
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(strings.ToUpper(key), "GIT_") && !strings.EqualFold(key, "GCM_INTERACTIVE") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "GIT_TERMINAL_PROMPT=0", "GCM_INTERACTIVE=Never",
		"GIT_OPTIONAL_LOCKS=0", "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1",
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	var stdout, stderr worktreeGitOutput
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if ctx.Err() != nil || stdout.overflow || stderr.overflow {
		return "", -1, ErrWorkspaceUnavailable
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", exit.ExitCode(), nil
		}
		return "", -1, ErrWorkspaceUnavailable
	}
	return strings.TrimSuffix(strings.TrimSuffix(stdout.String(), "\n"), "\r"), 0, nil
}

func (g worktreeGit) read(ctx context.Context, cwd string, args ...string) (string, error) {
	output, code, err := g.run(ctx, cwd, args...)
	if err != nil {
		return "", err
	}
	if code != 0 || output == "" {
		return "", ErrWorkspacePrecondition
	}
	return output, nil
}

func (g worktreeGit) state(ctx context.Context, cwd string) (worktreeGitState, error) {
	var state worktreeGitState
	top, err := g.read(ctx, cwd, "rev-parse", "--show-toplevel")
	if err != nil {
		return state, err
	}
	if match, err := workspaceMatches(top, cwd); err != nil || !match {
		return state, ErrWorkspacePrecondition
	}
	state.common, err = g.read(ctx, cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil || !validWorkspacePath(state.common) {
		if err != nil {
			return state, err
		}
		return state, ErrWorkspacePrecondition
	}
	state.head, err = g.read(ctx, cwd, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		return state, err
	}
	var code int
	state.branch, code, err = g.run(ctx, cwd, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		return state, err
	}
	if code != 0 && code != 1 {
		return state, ErrWorkspacePrecondition
	}
	return state, nil
}

func (g worktreeGit) preconditions(ctx context.Context, cwd, branch, base string) (worktreeGitState, error) {
	state, err := g.state(ctx, cwd)
	if err != nil {
		return state, err
	}
	commit, err := g.read(ctx, cwd, "rev-parse", "--verify", "--end-of-options", base+"^{commit}")
	if err != nil {
		return state, err
	}
	if commit != base {
		return state, ErrWorkspacePrecondition
	}
	_, code, err := g.run(ctx, cwd, "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	if err != nil {
		return state, err
	}
	// --quiet --verify returns 1 only for absence; other failures are not an
	// authorization to create. Check all local branches, not just attached ones.
	if code != 1 {
		return state, ErrWorkspacePrecondition
	}
	return state, nil
}

func (g worktreeGit) postconditions(ctx context.Context, cwd, destination, branch, base string, before worktreeGitState) error {
	created, err := g.state(ctx, destination)
	if err != nil || created.head != base || created.branch != "refs/heads/"+branch {
		return ErrWorkspaceIndeterminate
	}
	after, err := g.state(ctx, cwd)
	if err != nil || after.head != before.head || after.branch != before.branch {
		return ErrWorkspaceIndeterminate
	}
	for _, common := range []string{created.common, before.common} {
		if match, err := workspaceMatches(common, after.common); err != nil || !match {
			return ErrWorkspaceIndeterminate
		}
	}
	return nil
}
