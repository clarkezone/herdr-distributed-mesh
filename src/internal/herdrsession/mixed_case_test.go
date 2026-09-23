package herdrsession

import (
	"context"
	"fmt"
	"testing"
)

type mixedCaseRunner struct {
	fakeRunner
	namedRunning bool
}

func (f *mixedCaseRunner) run(ctx context.Context, args ...string) ([]byte, error) {
	if args[0] == "session" {
		return []byte(`{"sessions":[{"default":true,"name":"default","running":true},
			{"default":false,"name":"QEI","running":false}]}`), nil
	}
	f.running = args[0] != "--session" || f.namedRunning
	return f.fakeRunner.run(ctx, args...)
}

func TestMixedCaseSessionDoesNotDiscardDefaultInventory(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			f := &mixedCaseRunner{fakeRunner: fakeRunner{protocol: 22}, namedRunning: running}
			m, err := newManager(Config{}, f, func(string) (string, error) { return "incarnation", nil })
			if err != nil {
				t.Fatal(err)
			}
			sessions, err := m.List(context.Background())
			if err != nil || len(sessions) != 2 {
				t.Fatalf("valid mixed-case session discarded inventory: %+v, %v", sessions, err)
			}
			wantNamedStatus := "stopped"
			if running {
				wantNamedStatus = "ready"
			}
			if sessions[0].Name != "QEI" || sessions[0].Default || sessions[0].Status != wantNamedStatus ||
				sessions[1].Name != "default" || !sessions[1].Default || sessions[1].Status != "ready" {
				t.Fatalf("wrong names or statuses: %+v", sessions)
			}
			if f.starts != 0 || fmt.Sprint(f.commands) != "[[status server --json] [--session QEI status server --json]]" {
				t.Fatalf("discovery started a session or changed its name: %v, starts=%d", f.commands, f.starts)
			}
		})
	}
}
