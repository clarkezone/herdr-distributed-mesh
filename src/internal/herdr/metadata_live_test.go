package herdr

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
)

// This opt-in probe creates only scratch Git workspaces in an owned headless
// session. It never launches a provider, attaches a frontend, or alters a
// preexisting session. Native stop/delete confirms cleanup before returning.
func TestLiveProjectObservation(t *testing.T) {
	if os.Getenv("HERDR_MESH_OBSERVATION_LIVE") != "1" {
		t.Skip("set HERDR_MESH_OBSERVATION_LIVE=1 for isolated native project/worktree observation")
	}
	executable, err := exec.LookPath("herdr")
	if err != nil {
		t.Fatal("native Herdr is required")
	}
	root := t.TempDir()
	checkout := filepath.Join(root, "checkout")
	trees := filepath.Join(root, "trees")
	hooks := filepath.Join(root, "empty-hooks")
	for _, path := range []string{checkout, trees, hooks} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	t.Cleanup(cancel)
	for _, args := range [][]string{
		{"-c", "init.templateDir=" + hooks, "init", "--quiet", checkout},
	} {
		cmd := exec.CommandContext(ctx, "git", args...)
		configureLifecycleLiveCommand(cmd)
		if err := cmd.Run(); err != nil {
			t.Fatalf("scratch Git setup failed: %v", err)
		}
	}
	registry := projects.NewManaged("observation-node")
	binding, err := registry.Apply(ctx, "observation-project", "r1", checkout, trees)
	if err != nil {
		t.Fatal(err)
	}
	gitConfig := exec.CommandContext(ctx, "git", "-C", checkout, "config", "--local", "core.hooksPath", hooks)
	configureLifecycleLiveCommand(gitConfig)
	if err := gitConfig.Run(); err != nil {
		t.Fatal("could not isolate scratch repository hooks")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "mesh-observation-" + hex.EncodeToString(nonce[:])
	configPath := filepath.Join(root, "herdr.toml")
	if err := os.WriteFile(configPath, []byte("[general]\n[update]\nversion_check=false\nmanifest_check=false\n[session]\nresume_agents_on_restore=false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := lifecycleLiveEnvironment(configPath)
	unused, err := lifecycleLiveStatus(ctx, executable, env, name)
	if err != nil || unused.Running || unused.Session != name || unused.Socket == "" {
		t.Fatal("could not establish unused session identity")
	}
	sessionDir := filepath.Dir(unused.Socket)
	if _, err := os.Stat(sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusing to reuse a preexisting session directory")
	}
	server := exec.CommandContext(ctx, executable, "--session", name, "server")
	server.Env, server.Stdout, server.Stderr = env, io.Discard, io.Discard
	configureLifecycleLiveCommand(server)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Wait() }()
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if _, err := lifecycleLiveNative(cleanupCtx, executable, env, "session", "stop", name, "--json"); err != nil {
			t.Errorf("owned observation session stop failed: %v", err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owned server did not exit after native stop")
			if err := server.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("owned server termination failed: %v", err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("owned server exit remains unconfirmed")
				return
			}
		}
		stopped, err := lifecycleLiveStatus(cleanupCtx, executable, env, name)
		if err != nil || stopped.Running {
			t.Error("owned session stop remains unconfirmed")
			return
		}
		if _, err := lifecycleLiveNative(cleanupCtx, executable, env, "session", "delete", name, "--json"); err != nil {
			t.Error("owned session deletion failed")
			return
		}
		if _, err := os.Stat(sessionDir); !errors.Is(err, os.ErrNotExist) {
			t.Error("owned session directory remains")
		}
		t.Log("owned headless session stop/delete confirmed; no provider launched")
	})
	config := Config{SocketPath: unused.Socket, RequestTimeout: 3 * time.Second, ProjectResolver: registry}
	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	var o *observer
	for {
		o, err = agentObserver(readyCtx, config, dialLocal)
		if err == nil {
			break
		}
		if lifecycleLivePause(readyCtx, 200*time.Millisecond) != nil {
			t.Fatal("isolated headless API did not become ready")
		}
	}
	created, err := EnsureWorkspace(ctx, config, binding)
	if err != nil || !created.GetCreated() {
		t.Fatalf("native workspace ensure did not confirm creation: %v", err)
	}
	reused, err := EnsureWorkspace(ctx, config, binding)
	if err != nil || reused.GetCreated() || reused.GetWorkspaceId() != created.GetWorkspaceId() {
		t.Fatalf("native workspace ensure did not reuse the same checkout: %v", err)
	}
	commit := exec.CommandContext(ctx, "git", "-C", checkout, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
		"-c", "commit.gpgSign=false", "commit", "--quiet", "--allow-empty", "-m", "observation fixture")
	configureLifecycleLiveCommand(commit)
	if err := commit.Run(); err != nil {
		t.Fatal("scratch initial commit failed")
	}
	linked, err := CreateWorktree(ctx, config, binding, &pb.WorktreeCreate{
		ProjectId: binding.ProjectID, BindingRevision: binding.Revision, Name: "linked", Branch: "observation"})
	if err != nil {
		t.Fatalf("native linked-worktree creation was not confirmed; not retried: %v", err)
	}
	wanted := map[string]bool{created.WorkspaceId: true, linked.WorkspaceId: true}
	state, err := o.snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, workspace := range state.Workspaces {
		if wanted[workspace.Id] {
			if workspace.ProjectId != "observation-project" {
				t.Fatal("native main/linked repository root did not resolve the registered checkout")
			}
			matched++
		}
	}
	if matched != 2 {
		t.Fatal("main and linked-worktree workspaces were not both observed")
	}
	for _, pane := range state.Panes {
		if wanted[pane.WorkspaceId] && pane.ProjectId != "observation-project" {
			t.Fatal("project association did not reach the native workspace pane")
		}
	}
	t.Log("native main and linked worktree both project to the registered main checkout")
}
