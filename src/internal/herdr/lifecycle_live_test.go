package herdr

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

// TestLiveAgentLifecycle is deliberately opt-in and performs exactly one
// provider launch and at most one prompt submission per invocation:
//
// PowerShell:
//
//	$env:HERDR_MESH_LIFECYCLE_LIVE='1'
//	go test ./src/internal/herdr -run '^TestLiveAgentLifecycle$' -count=1 -v -timeout=5m
//
// Requires native protocol-18 Herdr, Git and preauthenticated Copilot. Never
// installs, authenticates, attaches a frontend, or retries a mutation. A random
// named session and scratch Git checkout isolate all effects. The private,
// fsynced stage journal is removed with the test's temporary directory.
// Startup dialogs and provider permissions are never automatically approved.
// HERDR_MESH_LIFECYCLE_DENY_COST=1 permits only the explicit No (Esc) choice
// for the observed user:copilot-cli-cost dialog, at most twice with a verified
// non-blocked transition between appearances. It also requires EVIDENCE_DIR.
//
// Launch and prompt are intentionally separate: native agent.start can report
// premature ready/idle. This probe observes a settled provider screen before
// submitting, then requires an actual fresh response, not echo or idle state.
// A test-only, read-only pre-launch hook also waits for the cold shell screen
// to settle, eliminating startup timing as a confounder without claiming this
// is authoritative shell-readiness evidence.
// It does not certify immediate InitialPrompt delivery or graceful provider exit.
func TestLiveAgentLifecycle(t *testing.T) {
	if os.Getenv("HERDR_MESH_LIFECYCLE_LIVE") != "1" {
		t.Skip("set HERDR_MESH_LIFECYCLE_LIVE=1 to spend one tiny Copilot prompt in an isolated headless session")
	}
	if os.Getenv("HERDR_MESH_LIFECYCLE_UI_DIAGNOSTIC") == "1" || os.Getenv("HERDR_MESH_LIFECYCLE_DENY_COST") == "1" {
		dir := os.Getenv("HERDR_MESH_LIFECYCLE_EVIDENCE_DIR")
		info, err := os.Stat(dir)
		if !validWorkspacePath(dir) || err != nil || !info.IsDir() {
			t.Fatal("UI diagnostic requires an existing private absolute evidence directory before launching")
		}
	}
	executable, err := exec.LookPath("herdr")
	if err != nil {
		t.Fatal("native Herdr is required")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "mesh-lifecycle-" + hex.EncodeToString(nonce[:])
	marker, prompt := lifecycleLivePrompt(nonce)
	root := t.TempDir()
	checkout := filepath.Join(root, "checkout")
	if err := os.Mkdir(checkout, 0700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "herdr.toml")
	configText := "[general]\n[update]\nversion_check=false\nmanifest_check=false\n[session]\nresume_agents_on_restore=false\n"
	if err := os.WriteFile(configPath, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}
	env := lifecycleLiveEnvironment(configPath)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	git := exec.CommandContext(ctx, "git", "init", "--quiet", checkout)
	configureLifecycleLiveCommand(git)
	if err := git.Run(); err != nil {
		t.Fatal("scratch Git initialization failed")
	}
	status, err := lifecycleLiveStatus(ctx, executable, env, name)
	if err != nil || status.Running || status.Session != name || status.Socket == "" {
		t.Fatal("could not establish unused named-session identity")
	}
	sessionDir := filepath.Dir(status.Socket)
	if _, err := os.Stat(sessionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusing to use or clean a preexisting session directory")
	}
	server := exec.CommandContext(ctx, executable, "--session", name, "server")
	server.Env = env
	server.Stdout, server.Stderr = io.Discard, io.Discard
	configureLifecycleLiveCommand(server)
	if err := server.Start(); err != nil {
		t.Fatal("isolated headless server launch failed")
	}
	t.Logf("owned headless session=%s server_pid=%d", name, server.Process.Pid)
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Wait() }()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cleanupCancel()
		if _, err := lifecycleLiveNative(cleanupCtx, executable, env, "session", "stop", name, "--json"); err != nil {
			t.Errorf("native stop failed for owned session %s (PID %d)", name, server.Process.Pid)
		}
		select {
		case <-serverDone:
		case <-time.After(5 * time.Second):
			t.Errorf("native stop did not reap owned PID %d; terminating only that child", server.Process.Pid)
			if err := server.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("owned child termination failed: %v", err)
			}
			select {
			case <-serverDone:
			case <-time.After(5 * time.Second):
				t.Error("owned child did not exit; session cleanup is incomplete")
				return
			}
		}
		stopped, err := lifecycleLiveStatus(cleanupCtx, executable, env, name)
		if err != nil || stopped.Running {
			t.Error("owned session stop was not confirmed")
			return
		}
		if _, err := lifecycleLiveNative(cleanupCtx, executable, env, "session", "delete", name, "--json"); err != nil {
			t.Error("native delete failed for owned stopped session")
			return
		}
		if _, err := os.Stat(sessionDir); !errors.Is(err, os.ErrNotExist) {
			t.Error("owned session directory remains after native delete")
			return
		}
		// Windows can release PTY cwd handles shortly after server exit.
		for deadline := time.Now().Add(10 * time.Second); ; {
			if err := os.RemoveAll(checkout); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Error("owned scratch checkout remains locked after native cleanup")
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("native stop/delete confirmed for session=%s PID=%d", name, server.Process.Pid)
	})
	config := Config{SocketPath: status.Socket, RequestTimeout: 3 * time.Second}
	var o *observer
	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	for {
		o, err = agentObserver(readyCtx, config, dialLocal)
		if err == nil {
			break
		}
		if lifecycleLivePause(readyCtx, 200*time.Millisecond) != nil {
			t.Fatal("isolated headless API did not become ready")
		}
	}
	created, err := o.agentRequest(ctx, "workspace.create", "workspace_created", struct {
		Cwd   string `json:"cwd"`
		Focus bool   `json:"focus"`
	}{checkout, false})
	if err != nil {
		t.Fatal("scratch workspace creation uncertain; no retry")
	}
	workspaceID, _, err := workspaceIdentity(created["workspace"])
	if err != nil {
		t.Fatal("invalid scratch workspace identity")
	}
	original, err := parseAgentInfo(created["root_pane"])
	if err != nil || original.WorkspaceId != workspaceID {
		t.Fatal("invalid scratch root-pane identity")
	}
	originalHandle := lifecycleHandle(original)
	journal, err := os.OpenFile(filepath.Join(root, "stages.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var events []LifecycleEvent
	attempted := map[LifecycleStage]bool{}
	var launchExpected LifecycleHandle
	var nativeStarted *pb.AgentView
	hook := func(ctx context.Context, event LifecycleEvent) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if event.Before {
			if attempted[event.Stage] {
				return errors.New("duplicate mutation intent")
			}
			attempted[event.Stage] = true
		}
		if err := json.NewEncoder(journal).Encode(event); err != nil {
			return err
		}
		if err := journal.Sync(); err != nil {
			return err
		}
		events = append(events, event)
		if event.Before && event.Stage == LifecycleLaunch {
			launchExpected = event.Result.Handle
			if err := lifecycleLiveShellSettle(ctx, o, event.Result.Handle); err != nil {
				return err
			}
			t.Log("cold shell screen settled before the single agent.start dispatch")
		}
		return nil
	}
	// The transparent dial wrapper diagnoses native rejection versus invalid
	// success shape without printing native messages, paths, or terminal text.
	liveDial := func(ctx context.Context, address string) (net.Conn, error) {
		conn, err := dialLocal(ctx, address)
		if err != nil {
			return nil, err
		}
		return &lifecycleLiveConn{Conn: conn, observe: func(envelope object) {
			var nativeError object
			if required(envelope, "error", &nativeError) == nil {
				var message string
				_ = required(nativeError, "message", &message)
				message = strings.ToLower(message)
				t.Logf("native agent.start rejected: mentions_shell=%t mentions_ready=%t mentions_executable=%t mentions_timeout=%t",
					strings.Contains(message, "shell"), strings.Contains(message, "ready"),
					strings.Contains(message, "executable"), strings.Contains(message, "timeout"))
			} else {
				var result object
				if required(envelope, "result", &result) == nil {
					agent, err := parseAgentInfo(result["agent"])
					var argv []string
					hasArgv := required(result, "argv", &argv) == nil && len(argv) > 0
					t.Logf("native agent.start response: parsed_agent=%t nonempty_argv=%t", err == nil && agent != nil, hasArgv)
					if err == nil && agent != nil {
						nativeStarted = agent
						t.Logf("native launch identity: workspace_match=%t tab_match=%t pane_match=%t terminal_match=%t provider_match=%t session_match=%t",
							agent.WorkspaceId == launchExpected.WorkspaceID, agent.TabId == launchExpected.TabID,
							agent.Target.PaneId == launchExpected.PaneID, agent.Target.TerminalId == launchExpected.TerminalID,
							agent.Provider == "copilot",
							launchExpected.AgentSessionID == "" || agent.Target.AgentSessionId == launchExpected.AgentSessionID)
						t.Logf("native launch readiness: interactive_ready=%t launch_pending=%t", agent.InteractiveReady, agent.LaunchPending)
					}
				}
			}
		}}, nil
	}
	launched, err := startAgent(ctx, config, StartAgentRequest{
		WorkspaceID: workspaceID, Cwd: checkout, Name: "nonce-probe", Provider: "copilot",
		StartupTimeout: 60 * time.Second,
	}, hook, liveDial)
	t.Logf("launch pane=%s launch=%s prompt=%s status=%s", launched.Pane, launched.Launch, launched.Prompt, launched.ObservedStatus)
	if os.Getenv("HERDR_MESH_LIFECYCLE_UI_DIAGNOSTIC") == "1" {
		if launched.Pane != LifecycleConfirmed || launched.Handle.TerminalID == "" {
			t.Fatal("UI diagnostic has no verified created pane; no retry")
		}
		artifact, captureErr := lifecycleCaptureStartupUI(ctx, o, launched.Handle,
			os.Getenv("HERDR_MESH_LIFECYCLE_EVIDENCE_DIR"), name)
		t.Logf("redacted startup evidence saved before cleanup: %s", filepath.Base(artifact))
		if captureErr != nil {
			t.Fatalf("UI capture did not obtain complete question/options: %v", captureErr)
		}
		t.Log("UI-only diagnostic finished; no task prompt or approval input sent")
		return
	}
	if os.Getenv("HERDR_MESH_LIFECYCLE_DIAGNOSTIC") == "1" {
		if nativeStarted == nil {
			t.Fatal("diagnostic did not obtain a parsed native launch acknowledgement; no retry")
		}
		fresh, err := o.getAgent(ctx, nativeStarted.Target)
		if err != nil || !lifecycleMatches(lifecycleHandle(nativeStarted), fresh, true) {
			t.Fatalf("diagnostic fresh get disagrees with acknowledged provider identity: %v", err)
		}
		t.Logf("diagnostic identities: shell_terminal=%s acknowledged_terminal=%s fresh_terminal=%s provider=%s",
			launchExpected.TerminalID, nativeStarted.Target.TerminalId, fresh.Target.TerminalId, fresh.Provider)
		t.Logf("diagnostic stages: create=%s launch=%s; fresh returned identity verified; no prompt submitted",
			launched.Pane, launched.Launch)
		return
	}
	startOutcome := launched.Launch
	continueBlocked := os.Getenv("HERDR_MESH_LIFECYCLE_DENY_COST") == "1" && errors.Is(err, ErrAgentBlocked) &&
		launched.Pane == LifecycleConfirmed && launched.Handle.TerminalID != ""
	if (err != nil && !continueBlocked) || (launched.Launch != LifecycleConfirmed && !continueBlocked) || launched.Handle.PaneID == originalHandle.PaneID ||
		launched.Handle.TerminalID == originalHandle.TerminalID {
		t.Fatalf("StartAgent acceptance failed: %v; partial stages remain quarantined, no restart", err)
	}
	if err := lifecycleLivePreserved(ctx, o, originalHandle, 2); err != nil {
		t.Fatal(err)
	}
	stopAttempted := false
	defer func() {
		if !stopAttempted {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer stopCancel()
			stopped, err := StopAgent(stopCtx, config, StopAgentRequest{Handle: launched.Handle}, hook)
			if err != nil || stopped.Stop != LifecycleConfirmed {
				t.Errorf("failure-path verified-pane cleanup uncertain: %v", err)
			} else {
				t.Log("failure-path StopAgent confirmed before owned session cleanup")
			}
		}
	}()
	var onObservation func(context.Context, *pb.AgentView) error
	var costProbe *lifecycleCostProbe
	if os.Getenv("HERDR_MESH_LIFECYCLE_DENY_COST") == "1" {
		costProbe = &lifecycleCostProbe{
			handle: launched.Handle,
			read: func(ctx context.Context, handle LifecycleHandle) (string, string, error) {
				return lifecycleStartupScreen(ctx, o, handle, "visible")
			},
			send: func(ctx context.Context, command *pb.AgentControl) (*pb.AgentControlResult, error) {
				return ControlAgent(ctx, config, command)
			},
			record: func(attempt int, before bool, outcome LifecycleOutcome) error {
				if err := json.NewEncoder(journal).Encode(struct {
					Action  string
					Attempt int
					Before  bool
					Outcome LifecycleOutcome
				}{"deny_optional_user:copilot-cli-cost", attempt, before, outcome}); err != nil {
					return err
				}
				if err := journal.Sync(); err != nil {
					return err
				}
				if !before {
					t.Logf("explicit cost-tool denial attempt=%d outcome=%s; sent one Esc, never approval", attempt, outcome)
				}
				return nil
			},
		}
		onObservation = func(ctx context.Context, agent *pb.AgentView) error {
			err := costProbe.observe(ctx, agent)
			if costProbe.handle.Provider == "copilot" {
				launched.Handle = costProbe.handle
			}
			if errors.Is(err, errLifecycleDifferentDialog) {
				artifact, captureErr := lifecycleCaptureStartupUI(ctx, o, launched.Handle,
					os.Getenv("HERDR_MESH_LIFECYCLE_EVIDENCE_DIR"), name+"-different-dialog")
				t.Logf("different dialog left unapproved; redacted evidence=%s capture_error=%v", filepath.Base(artifact), captureErr)
			}
			return err
		}
	}
	// Native readiness is not enough on this installation. Wait at least ten
	// seconds and require three seconds of ready observations with output.
	// Provider animations need not stop; only the fresh nonce proves a response.
	settleCtx, settleCancel := context.WithTimeout(ctx, 35*time.Second)
	defer settleCancel()
	if err := lifecycleLiveSettle(settleCtx, config, launched.Handle, marker, onObservation); err != nil {
		t.Fatalf("provider readiness compatibility gap: %v; no prompt submitted", err)
	}
	t.Log("provider screen settled after launch; submitting exactly one fragmented-nonce prompt")
	if costProbe != nil {
		t.Logf("same pinned agent ready/non-blocked after %d explicit cost denials; original launch outcome=%s; StartAgent was not repeated",
			costProbe.attempts, startOutcome)
	}
	err = lifecycleEffect(ctx, hook, LifecyclePrompt, &launched, func() error {
		result, err := ControlAgent(ctx, config, &pb.AgentControl{
			Target: lifecycleTarget(launched.Handle), Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT, Text: prompt,
		})
		if err != nil {
			return err
		}
		launched.Prompt, launched.ObservedStatus = LifecycleConfirmed, result.ObservedStatus
		return nil
	})
	if err != nil {
		t.Fatalf("single prompt outcome=%s error=%v; no resend", launched.Prompt, err)
	}
	responseCtx, responseCancel := context.WithTimeout(ctx, 90*time.Second)
	defer responseCancel()
	var observations int
	var lastStatus string
	var lastBytes int
	matched := false
	for {
		read, err := lifecycleLiveRead(responseCtx, config, launched.Handle)
		if err != nil {
			t.Fatalf("response observation failed: %v; no resend", err)
		}
		observations++
		lastStatus, lastBytes = read.Agent.Status, len(read.Text)
		if onObservation != nil {
			if err := onObservation(responseCtx, read.Agent); err != nil {
				t.Fatalf("response blocked by unresolved interaction: %v; no prompt resend", err)
			}
		}
		if strings.Contains(read.Text, marker) {
			matched = true
			break
		}
		if lifecycleLivePause(responseCtx, time.Second) != nil {
			break
		}
	}
	if !matched {
		t.Errorf("provider response compatibility gap: no fresh marker after %d bounded reads (status=%s bytes=%d); echo/idle is not success",
			observations, lastStatus, lastBytes)
	} else {
		t.Logf("fresh non-echo response verified: marker=%s length=%d reads=%d", marker, len(marker), observations)
	}
	// Stop still runs if the response timed out; task observation never retries
	// the prompt and does not silently substitute a session-wide stop.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer stopCancel()
	stopAttempted = true
	stopped, err := StopAgent(stopCtx, config, StopAgentRequest{Handle: launched.Handle}, hook)
	if err != nil || stopped.Stop != LifecycleConfirmed {
		t.Fatalf("verified-pane StopAgent failed: outcome=%s err=%v", stopped.Stop, err)
	}
	if err := lifecycleLivePreserved(stopCtx, o, originalHandle, 1); err != nil {
		t.Fatal(err)
	}
	t.Log("StopAgent confirmed: original pane/terminal/workspace retained; selected agent pane absent")
	git = exec.CommandContext(stopCtx, "git", "-C", checkout, "status", "--porcelain", "--untracked-files=all")
	configureLifecycleLiveCommand(git)
	changes, err := git.Output()
	if err != nil || len(changes) != 0 {
		t.Error("scratch checkout cleanliness was not confirmed")
	}
	if len(events) != 8 || len(attempted) != 4 {
		t.Fatalf("unexpected mutation history: %d events, %d intents", len(events), len(attempted))
	}
	for _, event := range events {
		want := LifecycleConfirmed
		if event.Before {
			want = LifecycleUnknown
		} else if event.Stage == LifecycleLaunch {
			want = startOutcome
		}
		if *lifecycleStageOutcome(&event.Result, event.Stage) != want {
			t.Error("live stage journal did not confirm each single mutation")
		}
	}
}

func lifecycleLivePrompt(nonce [16]byte) (string, string) {
	value := hex.EncodeToString(nonce[:])
	marker := "M_" + value
	prompt := fmt.Sprintf("Reply with exactly one line by joining these three fragments, in order, with no spaces:\n"+
		"prefix: M_\nfirst: %s\nsecond: %s\n"+
		"Do not use tools, run commands, edit files, or add any explanation.", value[:16], value[16:])
	return marker, prompt
}

func lifecycleLiveEnvironment(configPath string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "HERDR_") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "HERDR_CONFIG_PATH="+configPath)
}

type lifecycleLiveOutput struct{ buffer bytes.Buffer }

func (b *lifecycleLiveOutput) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > maxFrameBytes {
		return 0, errors.New("native probe output exceeded bound")
	}
	return b.buffer.Write(p)
}

func lifecycleLiveNative(ctx context.Context, executable string, env []string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = env
	configureLifecycleLiveCommand(cmd)
	var output lifecycleLiveOutput
	cmd.Stdout, cmd.Stderr = &output, io.Discard
	if err := cmd.Run(); err != nil {
		return nil, errors.New("native probe command failed")
	}
	return output.buffer.Bytes(), nil
}

type lifecycleLiveServerStatus struct {
	Running bool   `json:"running"`
	Session string `json:"session"`
	Socket  string `json:"socket"`
}

func lifecycleLiveStatus(ctx context.Context, executable string, env []string, name string) (lifecycleLiveServerStatus, error) {
	var status lifecycleLiveServerStatus
	data, err := lifecycleLiveNative(ctx, executable, env, "--session", name, "status", "server", "--json")
	if err == nil {
		err = json.Unmarshal(data, &status)
	}
	return status, err
}

func lifecycleLiveRead(ctx context.Context, config Config, handle LifecycleHandle) (*pb.AgentQueryResult, error) {
	return QueryAgent(ctx, config, &pb.AgentQueryRequest{
		NodeInstanceId: "live-probe", Target: lifecycleTarget(handle),
		Kind: pb.AgentQueryKind_AGENT_QUERY_KIND_READ, TimeoutMs: 3000, Lines: 100,
	})
}

func lifecycleLiveSettle(ctx context.Context, config Config, handle LifecycleHandle, marker string, onObservation func(context.Context, *pb.AgentView) error) error {
	start := time.Now()
	stableSince := start
	var lastStatus string
	var lastReady, lastPending bool
	var lastBytes int
	for {
		read, err := lifecycleLiveRead(ctx, config, handle)
		if err != nil {
			return err
		}
		if strings.Contains(read.Text, marker) {
			return errors.New("fresh marker unexpectedly present before submission")
		}
		lastStatus, lastReady, lastPending, lastBytes = read.Agent.Status, read.Agent.InteractiveReady, read.Agent.LaunchPending, len(read.Text)
		if onObservation != nil {
			if err := onObservation(ctx, read.Agent); err != nil {
				return err
			}
		} else if read.Agent.Status == "blocked" {
			return fmt.Errorf("%w: startup interaction unclassified; no input sent", ErrAgentBlocked)
		}
		if !read.Agent.InteractiveReady || read.Agent.LaunchPending || strings.TrimSpace(read.Text) == "" ||
			(read.Agent.Status != "idle" && read.Agent.Status != "done") {
			stableSince = time.Now()
		}
		if time.Since(start) >= 10*time.Second && time.Since(stableSince) >= 3*time.Second {
			return nil
		}
		if lifecycleLivePause(ctx, time.Second) != nil {
			return fmt.Errorf("provider did not settle within 35 seconds: status=%s ready=%t pending=%t bytes=%d",
				lastStatus, lastReady, lastPending, lastBytes)
		}
	}
}

func lifecycleLivePause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func lifecycleLivePreserved(ctx context.Context, o *observer, original LifecycleHandle, count int) error {
	panes, err := o.lifecyclePanes(ctx)
	if err != nil || len(panes) != count {
		return errors.New("unexpected isolated-session pane count")
	}
	found := false
	for _, pane := range panes {
		if lifecycleMatches(original, pane, true) {
			found = true
		}
	}
	if !found {
		return errors.New("original workspace pane was replaced or removed")
	}
	return o.lifecycleWorkspace(ctx, original.WorkspaceID)
}

func TestLifecycleLiveNonceCannotMatchPromptEcho(t *testing.T) {
	var nonce [16]byte
	for i := range nonce {
		nonce[i] = byte(i)
	}
	marker, prompt := lifecycleLivePrompt(nonce)
	if len(marker) != 34 || !strings.HasPrefix(marker, "M_") || strings.Contains(prompt, marker) {
		t.Fatal("probe marker must be short and absent from its submitted prompt")
	}
	if strings.Contains(strings.ReplaceAll(prompt, "\n", ""), marker) {
		t.Fatal("unwrapping prompt echo can fabricate the fresh marker")
	}
}

func lifecycleLiveShellSettle(ctx context.Context, o *observer, handle LifecycleHandle) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	start, stableSince := time.Now(), time.Now()
	lastText := ""
	for {
		pane, err := o.lifecyclePane(ctx, handle)
		if err != nil || pane.Provider != "unknown" {
			return errors.New("cold-shell identity or occupant changed")
		}
		response, err := o.agentRequest(ctx, "pane.read", "pane_read", struct {
			PaneID    string `json:"pane_id"`
			Source    string `json:"source"`
			Lines     uint32 `json:"lines"`
			StripANSI bool   `json:"strip_ansi"`
			Format    string `json:"format"`
		}{handle.PaneID, "recent_unwrapped", 20, true, "text"})
		if err != nil {
			return err
		}
		text, _, err := parseAgentRead(response, pane, 20)
		if err != nil {
			return err
		}
		if text != lastText {
			stableSince = time.Now()
		}
		lastText = text
		if strings.TrimSpace(text) != "" && time.Since(start) >= 5*time.Second && time.Since(stableSince) >= 2*time.Second {
			return nil
		}
		if lifecycleLivePause(ctx, 500*time.Millisecond) != nil {
			return errors.New("cold shell screen did not settle")
		}
	}
}

type lifecycleLiveConn struct {
	net.Conn
	request, response []byte
	observe           func(object)
	reported          bool
}

func (c *lifecycleLiveConn) Write(p []byte) (int, error) {
	if len(c.request)+len(p) <= maxFrameBytes {
		c.request = append(c.request, p...)
	}
	return c.Conn.Write(p)
}

func (c *lifecycleLiveConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if !c.reported && len(c.response)+n <= maxFrameBytes {
		c.response = append(c.response, p[:n]...)
		if end := bytes.IndexByte(c.response, '\n'); end >= 0 {
			c.reported = true
			var request struct {
				Method string `json:"method"`
			}
			var response object
			if json.Unmarshal(c.request, &request) == nil && request.Method == "agent.start" &&
				json.Unmarshal(c.response[:end], &response) == nil {
				c.observe(response)
			}
			c.request, c.response = nil, nil
		}
	}
	return n, err
}
