package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func nativePong(version int) step {
	release := "0.7.5-preview"
	if version == 20 {
		release = "0.8.2"
	} else if version == 22 {
		release = "0.9.1"
	}
	fields := map[string]any{"version": release, "protocol": version}
	if version == 22 {
		fields["capabilities"] = map[string]any{"endpoint_protocol_generation": 1, "health_check": true, "surface_interest": true}
	}
	return agentReply("ping", "pong", fields, map[string]any{})
}

func nativeSteps(version int, steps ...step) []step {
	for i := range steps {
		if steps[i].method == "ping" {
			steps[i] = nativePong(version)
		}
	}
	return steps
}

func TestVerifiedNativeContracts(t *testing.T) {
	for _, version := range []int{18, 20, 22} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			t.Run("observation", func(t *testing.T) {
				var snapshot map[string]any
				if err := json.Unmarshal(snapshotJSON("ws:1"), &snapshot); err != nil {
					t.Fatal(err)
				}
				snapshot["protocol"] = version
				if version == 20 {
					snapshot["version"] = "0.8.2"
				} else if version == 22 {
					snapshot["version"] = "0.9.1"
				}
				s := newScript(t, nativePong(version), subscriptionStep(t),
					step{"session.snapshot", func(conn net.Conn, request testRequest) {
						if err := reply(conn, request, map[string]any{"type": "session_snapshot", "snapshot": snapshot}); err != nil {
							t.Error(err)
						}
						expectClosed(t, conn)
					}})
				err := observe(context.Background(), testConfig(), func(state *pb.HerdrState) error {
					if state.Status != "ready" || state.Protocol != uint32(version) || len(state.Workspaces) != 1 {
						t.Errorf("protocol %d failed observation: %v", version, state)
					}
					return stopEmission
				}, s.dial)
				if !errors.Is(err, stopEmission) {
					t.Fatal(err)
				}
				s.finished()
			})
			t.Run("workspace-open", func(t *testing.T) {
				b := workspaceBinding(t)
				result, err := ensureWorkspace(context.Background(), testConfig(), b,
					workspaceDial(t, b, nativePong(version), workspaceList(), workspaceCreate(workspaceInfo("ws:1", b.Path))))
				if err != nil || !result.GetCreated() || result.GetWorkspaceId() != "ws:1" {
					t.Fatalf("workspace result=%v err=%v", result, err)
				}
			})
			t.Run("worktree-create", func(t *testing.T) {
				f := newWorktreeFixture(t)
				result, err := createWorktree(context.Background(), testConfig(), f.binding, f.request,
					worktreeDial(t, f, nativePong(version), f.listStep(), f.createStep(t)))
				if err != nil || result.GetWorkspaceId() != "ws:new" ||
					testWorktreeGit(t, f.destination(), "rev-parse", "HEAD") != f.request.BaseCommit ||
					testWorktreeGit(t, f.binding.Path, "rev-parse", "HEAD") != f.head {
					t.Fatalf("worktree result=%v err=%v", result, err)
				}
			})
			t.Run("agent-get", func(t *testing.T) {
				query := agentQueryFixture(pb.AgentQueryKind_AGENT_QUERY_KIND_GET)
				result, err := queryAgent(context.Background(), testConfig(), query,
					agentDial(t, nativePong(version), agentGetStep(agentFixture())))
				if err != nil || result.Agent.Target.TerminalId != "terminal:1" || result.Agent.DisplayName != "Review helper" {
					t.Fatalf("agent result=%v err=%v", result, err)
				}
			})
			t.Run("agent-input", func(t *testing.T) {
				command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT)
				result, err := controlAgent(context.Background(), testConfig(), command, agentDial(t,
					nativePong(version), agentGetStep(agentFixture()),
					agentReply("agent.send_keys", "ok", nil, map[string]any{"target": "pane:1", "keys": command.Keys}),
					agentGetStep(agentFixture())))
				if err != nil || result == nil || result.Target.TerminalId != "terminal:1" {
					t.Fatalf("input result=%v err=%v", result, err)
				}
			})
			t.Run("start-pending-then-ready", func(t *testing.T) {
				request := lifecycleRequest(t)
				pending := agentFixture()
				pending["launch_pending"], pending["interactive_ready"], pending["agent_session"] = true, false, nil
				steps := append(lifecyclePrepareSteps(request), lifecycleStartStep(request, pending),
					agentGetStep(pending), agentGetStep(agentFixture()))
				result, err := startAgent(context.Background(), testConfig(), request, lifecycleDiscard,
					agentDial(t, nativeSteps(version, steps...)...))
				if err != nil || result.Launch != LifecycleConfirmed || result.Handle.TerminalID != "terminal:1" {
					t.Fatalf("start result=%+v err=%v", result, err)
				}
			})
			t.Run("stop", func(t *testing.T) {
				steps := append(lifecycleStopSteps(), lifecycleCloseStep(), lifecycleListStep(lifecycleOtherPane()), lifecycleWorkspaceStep())
				result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleDiscard,
					agentDial(t, nativeSteps(version, steps...)...))
				if err != nil || result.Stop != LifecycleConfirmed {
					t.Fatalf("stop result=%+v err=%v", result, err)
				}
			})
			t.Run("replacement-cannot-receive-input", func(t *testing.T) {
				info := agentFixture()
				info["terminal_id"] = "replacement"
				result, err := controlAgent(context.Background(), testConfig(),
					agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
					agentDial(t, nativePong(version), agentGetStep(info)))
				if result != nil || !errors.Is(err, ErrAgentChanged) {
					t.Fatalf("replacement result=%v err=%v", result, err)
				}
			})
			t.Run("rejected-or-lost-prompt-never-retries", func(t *testing.T) {
				for _, last := range []step{lifecycleAPIError("agent.prompt", "agent_blocked"), lifecycleLostResponse("agent.prompt")} {
					result, err := controlAgent(context.Background(), testConfig(),
						agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
						agentDial(t, nativePong(version), agentGetStep(agentFixture()), last))
					if result != nil || !errors.Is(err, ErrAgentIndeterminate) {
						t.Fatalf("uncertain delivery result=%v err=%v", result, err)
					}
				}
			})
			t.Run("delayed-prompt-acknowledgement", func(t *testing.T) {
				config := testConfig()
				config.RequestTimeout = time.Second
				command := agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT)
				delayed := agentReply("agent.prompt", "agent_prompted", map[string]any{"agent": agentFixture()},
					map[string]any{"target": "pane:1", "text": command.Text})
				acknowledge := delayed.serve
				delayed.serve = func(conn net.Conn, request testRequest) {
					time.Sleep(20 * time.Millisecond)
					acknowledge(conn, request)
				}
				result, err := controlAgent(context.Background(), config, command,
					agentDial(t, nativePong(version), agentGetStep(agentFixture()), delayed))
				if err != nil || result == nil {
					t.Fatalf("delayed acknowledgement rejected: %v", err)
				}
			})
			t.Run("canceled-submitted-prompt-is-not-retried", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				submitted := step{"agent.prompt", func(conn net.Conn, request testRequest) {
					cancel()
					expectClosed(t, conn)
				}}
				result, err := controlAgent(ctx, testConfig(), agentControlFixture(pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT),
					agentDial(t, nativePong(version), agentGetStep(agentFixture()), submitted))
				if result != nil || !errors.Is(err, ErrAgentIndeterminate) {
					t.Fatalf("submitted prompt must remain uncertain: %v %v", result, err)
				}
			})
			t.Run("close-refusal-does-not-escalate", func(t *testing.T) {
				steps := append(lifecycleStopSteps(), lifecycleAPIError("pane.close", "confirmation_required"))
				result, err := stopAgent(context.Background(), testConfig(), lifecycleStopRequest(), lifecycleDiscard,
					agentDial(t, nativeSteps(version, steps...)...))
				if !errors.Is(err, ErrAgentIndeterminate) || result.Stop != LifecycleUnknown {
					t.Fatalf("close refusal must not close a group or retry: %+v %v", result, err)
				}
			})
		})
	}
}
