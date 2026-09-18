package dashboard

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGeneratedSessionJSONMatchesDashboardModel(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for the generated-JSON/dashboard contract check")
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	stamp := timestamppb.New(now)
	session := func(name, incarnation string, received *timestamppb.Timestamp) *pb.SessionView {
		return &pb.SessionView{Name: name, Incarnation: incarnation, Status: "ready", HerdrReceivedAt: received,
			Herdr: &pb.HerdrState{Status: "ready", Version: "0.9.0", Protocol: 18, Sequence: 1, ObservedAt: stamp,
				Workspaces: []*pb.HerdrEntity{{Id: "w1", AgentStatus: "working", DisplayName: "API project", Directory: `C:\src\demo`}},
				Tabs:       []*pb.HerdrEntity{{Id: "t1", WorkspaceId: "w1", AgentStatus: "working", DisplayName: "Tests"}},
				Panes:      []*pb.HerdrEntity{{Id: "p1", WorkspaceId: "w1", TabId: "t1", AgentStatus: "working", DisplayName: "Shell"}},
				Agents:     []*pb.HerdrEntity{{Id: "p1", WorkspaceId: "w1", TabId: "t1", AgentStatus: "working", DisplayName: "Reviewer", Directory: `C:\src\demo\tests`}}}}
	}
	list := &pb.NodeList{Nodes: []*pb.NodeView{{InstanceId: "node", TailscaleStableId: "peer", Hostname: "laptop",
		Connected: true, LastSeen: stamp, Stale: true, Herdr: &pb.HerdrState{Status: "unavailable", ErrorCode: "connection_failed"},
		SessionsReady: true, SessionsReceivedAt: stamp,
		Sessions: []*pb.SessionView{session("first", strings.Repeat("a", 64), stamp), session("second", strings.Repeat("b", 64), nil)},
	}}}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	module, err := filepath.Abs(filepath.Join("web", "model.mjs"))
	if err != nil {
		t.Fatal(err)
	}
	const script = `
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
const model = await import(pathToFileURL(process.argv[1]));
const now = Number(process.argv[2]);
const snapshot = model.parseSnapshot(JSON.parse(readFileSync(0, "utf8")));
const state = model.acceptSnapshot(model.initialState(), snapshot, now);
const rows = model.projectAgents(snapshot.nodes);
assert.equal(snapshot.nodes.length, 1);
assert.equal(rows.length, 2);
assert.deepEqual(rows.map(row => row.node.session_name), ["first", "second"]);
assert.equal(rows[0].agent.display_name, "Reviewer");
assert.equal(rows[0].agent.directory, "C:\\src\\demo\\tests");
assert.equal(rows[0].node.hostname, "laptop");
assert.equal(model.projectAgents(snapshot.nodes, "API project").length, 2);
assert.equal(model.projectAgents(snapshot.nodes, "C:\\src\\demo").length, 2);
assert.equal(model.summary(state, now).live.workspaces, 1);
assert.equal(model.summary(state, now).known.workspaces, 2);
const missing = model.sessionContexts(snapshot.nodes[0]).find(row => row.session_name === "second");
assert.equal(missing.herdr_received_at, null);
assert.equal(model.nodeFreshness(missing, state, now).live, false);
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, "--input-type=module", "-e", script, module, strconv.FormatInt(now.UnixMilli(), 10))
	command.Stdin = bytes.NewReader(data)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated session JSON is incompatible with dashboard: %v\n%s", err, output)
	}
}
