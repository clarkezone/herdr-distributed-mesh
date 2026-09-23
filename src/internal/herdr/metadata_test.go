package herdr

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEntityObservationPreservesReportedReadiness(t *testing.T) {
	for _, raw := range []string{`{}`, `{"agent":null,"agent_session":null}`} {
		var info object
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			t.Fatal(err)
		}
		got, err := readEntityObservation(info)
		if err != nil || got != (entityObservation{}) {
			t.Fatalf("unreported metadata fabricated: %+v, %v", got, err)
		}
	}
	for _, ready := range []string{"false", "true"} {
		var info object
		raw := `{"agent":"copilot","terminal_id":"terminal:1","interactive_ready":` + ready +
			`,"agent_session":{"kind":"id","value":"provider-session-1","agent":"copilot","source":"native"}}`
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			t.Fatal(err)
		}
		got, err := readEntityObservation(info)
		if err != nil || got.provider != "copilot" || got.terminalID != "terminal:1" ||
			got.agentSessionID != "provider-session-1" || got.interactiveReady == nil ||
			*got.interactiveReady != (ready == "true") {
			t.Fatalf("reported metadata lost: %+v, %v", got, err)
		}
	}
}

func TestEntityObservationDropsPrivateSessionPaths(t *testing.T) {
	var info object
	if err := json.Unmarshal([]byte(`{"agent":"copilot","cwd":"PRIVATE",
		"agent_session":{"kind":"path","value":"C:\\PRIVATE\\session.json","agent":"copilot","source":"PRIVATE"}}`), &info); err != nil {
		t.Fatal(err)
	}
	got, err := readEntityObservation(info)
	if err != nil || got.agentSessionID != "" || got.provider != "copilot" {
		t.Fatalf("private session reference leaked: %+v, %v", got, err)
	}
}

func TestEntityObservationRejectsMalformedMetadata(t *testing.T) {
	for _, raw := range []string{
		`{"terminal_id":null}`, `{"terminal_id":""}`, `{"terminal_id":"C:\\PRIVATE"}`,
		`{"agent":""}`, `{"agent":"C:\\PRIVATE"}`, `{"agent":17}`,
		`{"interactive_ready":null}`, `{"interactive_ready":"false"}`,
		`{"agent_session":[]}`,
		`{"agent":"copilot","agent_session":{"kind":"id","value":"id","agent":"claude","source":"native"}}`,
		`{"agent":"copilot","agent_session":{"kind":"id","value":"file:private","agent":"copilot","source":"native"}}`,
		`{"agent":"copilot","agent_session":{"kind":"id","value":"C:\\PRIVATE","agent":"copilot","source":"native"}}`,
		`{"agent":"copilot","agent_session":{"kind":"other","value":"id","agent":"copilot","source":"native"}}`,
		`{"agent":"` + strings.Repeat("x", 129) + `"}`,
	} {
		var info object
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			t.Fatal(err)
		}
		if got, err := readEntityObservation(info); err == nil || got != (entityObservation{}) {
			t.Fatalf("invalid metadata accepted: %+v, %v", got, err)
		}
	}
}
