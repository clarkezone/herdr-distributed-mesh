package herdr

import (
	"context"
	"errors"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestWorkspaceChecksSessionImmediatelyBeforeCreate(t *testing.T) {
	binding := workspaceBinding(t)
	changed := errors.New("native session replaced")
	config := testConfig()
	checks := 0
	config.CheckSession = func(context.Context) error { checks++; return changed }
	value, err := ensureWorkspace(context.Background(), config, binding,
		workspaceDial(t, binding, workspacePong(), workspaceList()))
	if !errors.Is(err, changed) || value != nil || checks != 1 {
		t.Fatalf("replacement must prevent workspace.create: %v %v checks=%d", value, err, checks)
	}
}

func TestAgentChecksSessionAfterDiscoveryBeforeEachEffect(t *testing.T) {
	for _, action := range []pb.AgentControlAction{
		pb.AgentControlAction_AGENT_CONTROL_ACTION_PROMPT,
		pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT,
		pb.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT,
	} {
		t.Run(action.String(), func(t *testing.T) {
			changed := errors.New("native session replaced")
			config := testConfig()
			checks := 0
			config.CheckSession = func(context.Context) error { checks++; return changed }
			value, err := controlAgent(context.Background(), config, agentControlFixture(action),
				agentDial(t, agentPongStep(), agentGetStep(agentFixture())))
			if !errors.Is(err, changed) || value != nil || checks != 1 {
				t.Fatalf("replacement must prevent input IPC: %v %v checks=%d", value, err, checks)
			}
		})
	}
}
