package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
)

func TestNodesJSONShape(t *testing.T) {
	var out bytes.Buffer
	if err := writeNodes(Options{JSON: true, Output: &out}, &agentflowv1.NodeList{}); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Nodes == nil || len(result.Nodes) != 0 {
		t.Fatalf("expected single JSON object with nodes array, got %s (%v)", out.String(), err)
	}
	out.Reset()
	list := &agentflowv1.NodeList{Nodes: []*agentflowv1.NodeView{{
		InstanceId: "node-1", Stale: true, Herdr: &agentflowv1.HerdrState{Status: "unavailable", ErrorCode: "offline"},
	}}}
	if err := writeNodes(Options{JSON: true, Output: &out}, list); err != nil {
		t.Fatal(err)
	}
	var full map[string][]map[string]any
	if err := json.Unmarshal(out.Bytes(), &full); err != nil {
		t.Fatal(err)
	}
	if full["nodes"][0]["connected"] != false || full["nodes"][0]["stale"] != true {
		t.Fatal("disconnected/stale flags missing")
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("output failed") }

func TestNodesSurfacesOutputFailure(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		if err := writeNodes(Options{Output: failedWriter{}, JSON: jsonOutput}, &agentflowv1.NodeList{}); err == nil {
			t.Fatal("output failure hidden")
		}
	}
}
