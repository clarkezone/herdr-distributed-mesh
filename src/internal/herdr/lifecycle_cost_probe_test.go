package herdr

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

var errLifecycleDifferentDialog = errors.New("different startup dialog: no input authorized")

// This is an opt-in test driver, not StartAgent policy. Unknown delivery or
// persistence failure permanently fences this driver; it never resends Esc.
type lifecycleCostProbe struct {
	handle   LifecycleHandle
	attempts int
	canDeny  bool
	failure  error
	read     func(context.Context, LifecycleHandle) (string, string, error)
	send     func(context.Context, *pb.AgentControl) (*pb.AgentControlResult, error)
	record   func(int, bool, LifecycleOutcome) error
}

func (p *lifecycleCostProbe) observe(ctx context.Context, agent *pb.AgentView) error {
	if p.failure != nil {
		return p.failure
	}
	if !lifecycleMatches(p.handle, agent, false) || agent.Provider != "copilot" {
		return ErrAgentChanged
	}
	p.handle = lifecycleHandle(agent)
	if lifecycleCostUnblocked(agent.Status) {
		p.canDeny = true
	}
	if agent.Status != "blocked" {
		return nil
	}
	screen, currentStatus, err := p.read(ctx, p.handle)
	if err != nil {
		return err
	}
	if currentStatus != "blocked" {
		if lifecycleCostUnblocked(currentStatus) {
			p.canDeny = true
		}
		return nil
	}
	if !lifecycleExactCostDialog(screen) {
		return errLifecycleDifferentDialog
	}
	if p.attempts != 0 && !p.canDeny {
		return nil
	}
	if p.attempts >= 2 {
		return errors.New("cost permission dialog recurred after two explicit denials; no more input")
	}
	p.attempts++
	p.canDeny = false
	if err := p.record(p.attempts, true, LifecycleUnknown); err != nil {
		p.failure = ErrLifecyclePersistence
		return p.failure
	}
	result, sendErr := p.send(ctx, &pb.AgentControl{
		Target: lifecycleTarget(p.handle), Action: pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT,
		Keys: []string{"esc"},
	})
	outcome := LifecycleUnknown
	if sendErr == nil && result != nil {
		outcome = LifecycleConfirmed
	}
	recordErr := p.record(p.attempts, false, outcome)
	if recordErr != nil {
		p.failure = errors.Join(ErrLifecyclePersistence, sendErr)
		return p.failure
	}
	if sendErr != nil || result == nil {
		p.failure = ErrAgentIndeterminate
		return p.failure
	}
	p.canDeny = lifecycleCostUnblocked(result.ObservedStatus)
	return nil
}

func lifecycleCostUnblocked(status string) bool {
	return status == "idle" || status == "done" || status == "working"
}

var lifecycleExtraCostOption = regexp.MustCompile(`(?:^|\s)[4-9][.)]\s`)

func lifecycleExactCostDialog(text string) bool {
	text = strings.ReplaceAll(text, "\u2502", " ")
	text = strings.Join(strings.Fields(text), " ")
	return strings.Count(text, "1. Yes") == 1 &&
		strings.Count(text, `2. Yes, and always allow "user:copilot-cli-cost"`) == 1 &&
		strings.Count(text, "3. No (Esc)") == 1 &&
		strings.Count(text, "always allow") == 1 && !lifecycleExtraCostOption.MatchString(text)
}

const lifecycleCostDialogFixture = "1. Yes\n2. Yes, and always allow \"user:copilot-cli-cost\"\n3. No (Esc)"

func TestLifecycleCostDialogRequiresExactObservedDenial(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{lifecycleCostDialogFixture, true},
		{"\u2502 > 1. Yes \u2502\n\u2502 2. Yes, and always allow \"user:copilot-cli-cost\" \u2502\n\u2502 3. No (Esc) \u2502", true},
		{strings.ReplaceAll(lifecycleCostDialogFixture, "copilot-cli-cost", "other-tool"), false},
		{strings.ReplaceAll(lifecycleCostDialogFixture, "No (Esc)", "Approve (Esc)"), false},
		{lifecycleCostDialogFixture + "\n4. Another choice", false},
		{lifecycleCostDialogFixture + "\n2. Yes, and always allow \"another-tool\"", false},
		{"Do you trust this folder?\n1. Yes\n2. No (Esc)", false},
	} {
		if lifecycleExactCostDialog(tc.text) != tc.want {
			t.Fatal("cost denial guard accepted a different or ambiguous dialog")
		}
	}
}

func lifecycleCostProbeFixture(t *testing.T) (*lifecycleCostProbe, *pb.AgentView, *int) {
	t.Helper()
	agent := &pb.AgentView{Target: agentExpectedTarget(), WorkspaceId: "ws:1", TabId: "tab:1", Provider: "copilot", Status: "blocked"}
	sends := 0
	probe := &lifecycleCostProbe{
		handle: lifecycleHandle(agent),
		read: func(context.Context, LifecycleHandle) (string, string, error) {
			return lifecycleCostDialogFixture, "blocked", nil
		},
		send: func(_ context.Context, command *pb.AgentControl) (*pb.AgentControlResult, error) {
			sends++
			if command.Action != pb.AgentControlAction_AGENT_CONTROL_ACTION_INPUT || command.Text != "" ||
				!reflect.DeepEqual(command.Keys, []string{"esc"}) || command.Target.TerminalId != "terminal:1" {
				t.Fatal("driver sent something other than one pinned Esc")
			}
			return &pb.AgentControlResult{ObservedStatus: "blocked"}, nil
		},
		record: func(int, bool, LifecycleOutcome) error { return nil },
	}
	return probe, agent, &sends
}

func TestLifecycleCostDenialRequiresRecurrenceAndStopsAtTwo(t *testing.T) {
	probe, agent, sends := lifecycleCostProbeFixture(t)
	ctx := context.Background()
	if err := probe.observe(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := probe.observe(ctx, agent); err != nil || *sends != 1 {
		t.Fatal("a still-visible dialog caused duplicate input")
	}
	agent.Status = "idle"
	if err := probe.observe(ctx, agent); err != nil {
		t.Fatal(err)
	}
	agent.Status = "blocked"
	if err := probe.observe(ctx, agent); err != nil || *sends != 2 {
		t.Fatal("second distinct appearance was not denied once")
	}
	agent.Status = "idle"
	if err := probe.observe(ctx, agent); err != nil {
		t.Fatal(err)
	}
	agent.Status = "blocked"
	if err := probe.observe(ctx, agent); err == nil || *sends != 2 {
		t.Fatal("driver exceeded two denials")
	}
}

func TestLifecycleCostDenialFencesUncertaintyAndPersistenceFailure(t *testing.T) {
	for _, fail := range []string{"before", "after", "send"} {
		t.Run(fail, func(t *testing.T) {
			probe, agent, sends := lifecycleCostProbeFixture(t)
			probe.record = func(_ int, before bool, _ LifecycleOutcome) error {
				if (fail == "before" && before) || (fail == "after" && !before) {
					return errors.New("storage failure")
				}
				return nil
			}
			if fail == "send" {
				probe.send = func(context.Context, *pb.AgentControl) (*pb.AgentControlResult, error) {
					*sends++
					return nil, ErrAgentIndeterminate
				}
			}
			if err := probe.observe(context.Background(), agent); err == nil {
				t.Fatal("failure was not surfaced")
			}
			previous := *sends
			agent.Status = "idle"
			_ = probe.observe(context.Background(), agent)
			agent.Status = "blocked"
			if err := probe.observe(context.Background(), agent); err == nil || *sends != previous {
				t.Fatal("uncertain denial was retried")
			}
		})
	}
}

func TestLifecycleCostDenialRejectsOtherDialogOrReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		probe, agent, sends := lifecycleCostProbeFixture(t)
		if replacement {
			agent.Target.TerminalId = "replacement"
		} else {
			probe.read = func(context.Context, LifecycleHandle) (string, string, error) {
				return "1. Yes\n2. Yes, and always allow \"other\"\n3. No (Esc)", "blocked", nil
			}
		}
		if err := probe.observe(context.Background(), agent); err == nil || *sends != 0 {
			t.Fatal("unrecognized dialog or replacement received input")
		}
	}
}
