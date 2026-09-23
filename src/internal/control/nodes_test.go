package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

func TestNodesHumanAvailabilityAndUnchangedJSON(t *testing.T) {
	list := &agentflowv1.NodeList{Nodes: []*agentflowv1.NodeView{
		{InstanceId: "node-1", Hostname: "build-machine", Connected: true, SessionsReady: true,
			Herdr: &agentflowv1.HerdrState{Status: "ready", Agents: []*agentflowv1.HerdrEntity{{Id: "agent-1"}}}},
		{InstanceId: "node-2", Stale: true, Herdr: &agentflowv1.HerdrState{Status: "unavailable", ErrorCode: "herdr_unavailable"},
			SessionsErrorCode: "session_manager_unavailable"},
	}}
	before := proto.Clone(list)
	var out bytes.Buffer
	if err := writeNodes(Options{Output: &out}, list); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Node: build-machine", "Node ID: node-1", "Node ID: node-2",
		"Connection: connected to coordinator", "Connection: disconnected", "Default-session Herdr: ready",
		"Default-session inventory: workspaces: 0; tabs: 0; panes: 0; agents: 1",
		"stale; not current readiness", "Session manager: unavailable", "inspect this node's sessions before starting work"} {
		if !strings.Contains(out.String(), text) {
			t.Fatalf("missing %q: %s", text, &out)
		}
	}
	out.Reset()
	if err := writeNodes(Options{Output: &out, JSON: true}, list); err != nil {
		t.Fatal(err)
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if json.Unmarshal(data, &want) != nil || json.Unmarshal(out.Bytes(), &got) != nil || !reflect.DeepEqual(want, got) || !proto.Equal(before, list) {
		t.Fatalf("JSON contract or source data changed: %s", &out)
	}
	if err := writeNodes(Options{Output: failedWriter{}}, list); err == nil {
		t.Fatal("nonempty output failure hidden")
	}
}
