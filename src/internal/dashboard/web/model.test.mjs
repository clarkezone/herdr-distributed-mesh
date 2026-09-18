import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";
import * as model from "./model.mjs";
import {
  STALE_AFTER_MS, ERROR_MESSAGES, DashboardError, agentStatus, parseSnapshot,
  initialState, acceptSnapshot, rejectSnapshot, freshness, nodeFreshness, summary,
  projectAgents, projectWorkspaces, projectNodes, sessionContexts, topology, entityLabel, entityWorkspace,
  entityTab, entityDirectory, agentDisplayName, workspaceDirectories, countLabel, ageLabel, fetchSnapshot, createPoller,
} from "./model.mjs";

const now = Date.parse("2026-09-15T17:00:00Z");
const iso = (offset = 0) => new Date(now + offset).toISOString();
const entity = (id, extra = {}) => ({ id, workspace_id: "ws-1", tab_id: "tab-1", agent_status: "unknown", focused: false, ...extra });
function node(id = "node-1", extra = {}) {
  return {
    instance_id: id, tailscale_stable_id: "stable-1", connected: true, stale: false,
    last_seen: iso(), herdr_received_at: iso(),
    herdr: {
      status: "ready", version: "1.0", protocol: 1, sequence: "9007199254740993", observed_at: iso(),
      error_code: "", workspaces: [entity("ws-1")], tabs: [entity("tab-1")],
      panes: [entity("pane-1", { focused: true })],
      agents: [entity("agent-working", { agent_status: "working", focused: true }),
        entity("agent-blocked", { agent_status: "blocked" }), entity("agent-done", { agent_status: "done" })],
    },
    ...extra,
  };
}
const snapshot = (nodes = [node()]) => parseSnapshot({ nodes });
const success = (nodes = [node()], fetchedAt = now) => acceptSnapshot(initialState(), snapshot(nodes), fetchedAt);

test("empty fleet is a successful snapshot, not loading or an error", () => {
  const state = success([]);
  assert.equal(freshness(state, now), "live");
  assert.equal(summary(state, now).total, 0);
  assert.deepEqual(projectAgents(state.snapshot.nodes), []);
});

test("loading, initial error, retained stale, and recovery are distinct", () => {
  const initial = initialState();
  assert.equal(freshness(initial, now), "loading");
  const failed = rejectSnapshot(initial, new DashboardError("authorization_denied"));
  assert.equal(freshness(failed, now), "error");
  assert.equal(failed.snapshot, null);
  const known = success();
  const stale = rejectSnapshot(known, new DashboardError("server_unavailable"));
  assert.equal(freshness(stale, now), "stale");
  assert.equal(stale.snapshot, known.snapshot);
  assert.equal(nodeFreshness(stale.snapshot.nodes[0], stale, now).live, false);
  const recovered = acceptSnapshot(stale, snapshot([]), now + 5_000);
  assert.equal(recovered.error, null);
  assert.equal(freshness(recovered, now + 5_000), "live");
  assert.equal(recovered.snapshot.nodes.length, 0);
});

test("local clock expires a snapshot at exactly 30 seconds even without polling", () => {
  const state = success();
  assert.equal(freshness(state, now + STALE_AFTER_MS - 1), "live");
  assert.equal(freshness(state, now + STALE_AFTER_MS), "stale");
  assert.equal(nodeFreshness(state.snapshot.nodes[0], state, now + STALE_AFTER_MS).live, false);
  assert.equal(freshness(state, now - 1), "stale");
});

test("node readiness requires connectivity, ready state, receive time, and no stale flag", () => {
  const cases = [
    node("disconnected", { connected: false }),
    node("flagged", { stale: true }),
    node("old", { herdr_received_at: iso(-30_000) }),
    node("missing", { herdr_received_at: null }),
    node("clock-ahead", { herdr_received_at: iso(30_000) }),
    ...["disabled", "waiting", "unavailable"].map((status) => {
      const value = node(status);
      value.herdr.status = status;
      return value;
    }),
  ];
  for (const value of cases) {
    const state = success([value]);
    assert.equal(nodeFreshness(state.snapshot.nodes[0], state, now).live, false, value.instance_id);
  }
  const recent = success([node("recent", { herdr_received_at: iso(-29_999) })]);
  assert.equal(nodeFreshness(recent.snapshot.nodes[0], recent, now).live, true);
});

test("fleet pulse counts inventory only on fresh nodes, independently of filters", () => {
  const state = success([node("fresh"), node("stale", { stale: true }), node("offline", { connected: false })]);
  const result = summary(state, now);
  assert.deepEqual(result.live, { connected: 2, fresh: 1, workspaces: 1, working: 1, blocked: 1, done: 1 });
  assert.deepEqual(result.known, { connected: 2, workspaces: 3, working: 3, blocked: 3, done: 3 });
  projectAgents(state.snapshot.nodes, "nothing", "blocked");
  assert.deepEqual(summary(state, now), result);
  const stale = summary(rejectSnapshot(state, new DashboardError("busy")), now);
  assert.equal(stale.live.working, 0);
  assert.equal(stale.live.connected, 0);
  assert.equal(stale.known.working, 3);
});

test("unknown agent strings normalize to a fixed legitimate status", () => {
  for (const raw of ["unknown", "new-future-status", "<img src=x onerror=alert(1)>", "__proto__", ""]) {
    assert.equal(agentStatus(raw), "unknown");
    const value = node();
    value.herdr.agents[0].agent_status = raw;
    const parsed = snapshot([value]);
    assert.equal(projectAgents(parsed.nodes, "", "unknown").length, 1);
  }
});

test("validation rejects malformed success bodies instead of inventing an empty fleet", () => {
  for (const value of [null, [], {}, { nodes: null }, { nodes: [null] }, { nodes: [{}] }]) {
    assert.throws(() => parseSnapshot(value), { code: "invalid_response" });
  }
  for (const change of [
    (n) => { delete n.connected; },
    (n) => { n.last_seen = "yesterday"; },
    (n) => { n.herdr_received_at = undefined; },
    (n) => { n.herdr.agents = null; },
    (n) => { n.herdr.protocol = "1"; },
    (n) => { n.herdr.status = "unexpected"; },
    (n) => { n.herdr.agents[0].focused = "true"; },
    (n) => { n.herdr.agents[0].id = {}; },
  ]) {
    const value = node();
    change(value);
    assert.throws(() => snapshot([value]), { code: "invalid_response" });
  }
  assert.throws(() => snapshot([node(), node()]), { code: "invalid_response" });
});

test("sorting is deterministic, inputs are not mutated, and sequence stays a string", () => {
  const input = { nodes: [node("z"), node("a")] };
  const before = structuredClone(input);
  const parsed = parseSnapshot(input);
  assert.deepEqual(input, before);
  assert.deepEqual(parsed.nodes.map((n) => n.instance_id), ["a", "z"]);
  assert.deepEqual(parsed.nodes[0].herdr.agents.map((a) => a.id), ["agent-blocked", "agent-done", "agent-working"]);
  assert.equal(parsed.nodes[0].herdr.sequence, "9007199254740993");
});

test("agent filtering combines case-insensitive identifier search and status", () => {
  const nodes = snapshot([node("node-A"), node("node-B")]).nodes;
  assert.equal(projectAgents(nodes, "  NODE-a  ", "blocked").length, 1);
  assert.equal(projectAgents(nodes, "agent-working").length, 2);
  assert.equal(projectAgents(nodes, "WS-1", "done").length, 2);
  assert.equal(projectAgents(nodes, "tab-1", "working").length, 2);
  assert.equal(projectAgents(nodes, "pane-1").length, 0, "a pane without an agent is not an agent result");
  assert.equal(projectAgents(nodes, "no-match").length, 0);
});

test("topology groups known entities and preserves orphan references", () => {
  const value = node();
  value.herdr.panes.push(entity("orphan-pane", { workspace_id: "missing-ws", tab_id: "missing-tab" }));
  value.herdr.agents.push(entity("unassigned", { workspace_id: "", tab_id: "" }));
  const groups = topology(snapshot([value]).nodes[0]);
  assert.deepEqual(groups.map((g) => g.id), ["", "missing-ws", "ws-1"]);
  assert.equal(groups[0].agents[0].id, "unassigned");
  assert.equal(groups[1].entity, null);
  assert.equal(groups[1].tabs[0].entity, null);
  assert.equal(groups[1].tabs[0].panes[0].id, "orphan-pane");
  assert.equal(groups[2].tabs[0].agents.length, 3);
  assert.equal(groups[2].tabs[0].panes[0].focused, true);
});

test("workspace filters select whole matching groups with sibling context", () => {
  const nodes = snapshot().nodes;
  for (const query of ["node-1", "ws-1", "tab-1", "pane-1", "agent-blocked"]) {
    const groups = projectWorkspaces(nodes, query, "blocked");
    assert.equal(groups.length, 1, query);
    assert.equal(groups[0].workspace.tabs[0].agents.length, 3);
  }
  assert.equal(projectWorkspaces(nodes, "", "idle").length, 0);
  assert.equal(projectWorkspaces(nodes, "no-match").length, 0);
});

test("node filters search descendant identifiers and statuses", () => {
  const nodes = snapshot([node("node-1"), node("node-2")]).nodes;
  assert.equal(projectNodes(nodes, "node-2", "working").length, 1);
  assert.equal(projectNodes(nodes, "pane-1", "blocked").length, 2);
  assert.equal(projectNodes(nodes, "", "unknown").length, 0);
});

function paneAgentNode(id = "node-1") {
  const value = node(id);
  value.herdr.workspaces = [entity("w1")];
  value.herdr.tabs = [entity("w1:t1", { workspace_id: "w1" })];
  value.herdr.panes = [entity("w1:p1", { workspace_id: "w1", tab_id: "w1:t1", focused: true })];
  value.herdr.agents = [entity("w1:p1", {
    workspace_id: "w1", tab_id: "w1:t1", agent_status: "working", focused: true,
  })];
  return value;
}

function multiSessionNode() {
  const value = paneAgentNode();
  value.herdr = { ...value.herdr, status: "waiting", workspaces: [], tabs: [], panes: [], agents: [] };
  value.stale = true;
  value.herdr_received_at = null;
  value.sessions_ready = true;
  value.sessions_received_at = iso();
  value.sessions = ["dev-a", "dev-b"].map((name) => ({ name, incarnation: `${name}-incarnation`,
    status: "ready", error_code: "", herdr: paneAgentNode().herdr }));
  return value;
}

test("absent and empty names, directories, and hostname preserve legacy IDs and unknown metadata", () => {
  for (const supplied of [false, true]) {
    const value = paneAgentNode();
    if (supplied) value.hostname = "";
    for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
      if (supplied) Object.assign(value.herdr[kind][0], { display_name: "", directory: "" });
      Object.assign(value.herdr[kind][0], { title: "Do not use terminal titles", prompt: "Do not use prompts" });
    }
    const parsed = snapshot([value]).nodes[0];
    assert.equal(parsed.hostname, "");
    for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
      const item = parsed.herdr[kind][0];
      assert.equal(item.display_name, "");
      assert.equal(item.directory, "");
      assert.deepEqual(entityLabel(item), { primary: item.id, secondary: "" });
      assert.deepEqual(entityDirectory(parsed, item), { directory: "", source: "unknown" });
      assert.equal(item.title, undefined);
      assert.equal(item.prompt, undefined);
    }
    assert.equal(projectAgents([parsed])[0].pane.id, "w1:p1");
  }
});

test("optional display metadata rejects malformed types, all C0/C1 controls, and lone surrogates", () => {
  const invalidValues = [null, undefined, 17, false, {}, [], "\ud800", "\udfff",
    ...Array.from({ length: 32 }, (_, i) => `prefix${String.fromCharCode(i)}suffix`),
    ...Array.from({ length: 33 }, (_, i) => `prefix${String.fromCharCode(127 + i)}suffix`)];
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
    for (const field of ["display_name", "directory"]) {
      for (const invalid of invalidValues) {
        const value = paneAgentNode();
        value.herdr[kind][0][field] = invalid;
        assert.throws(() => snapshot([value]), { code: "invalid_response" }, `${kind}.${field}: ${JSON.stringify(invalid)}`);
      }
    }
  }
  for (const hostname of invalidValues) {
    assert.throws(() => snapshot([node("node-1", { hostname })]), { code: "invalid_response" });
  }
  const sessions = multiSessionNode();
  sessions.sessions[1].herdr.tabs[0].directory = "bad\npath";
  assert.throws(() => snapshot([sessions]), { code: "invalid_response" });
});

test("name and directory limits measure UTF-8 bytes, with exact boundaries accepted", () => {
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
    for (const [field, limit] of [["display_name", 256], ["directory", 4096]]) {
      for (const unit of ["a", "\u00e9", "\u754c", "\ud83d\ude80"]) {
        const width = Buffer.byteLength(unit, "utf8");
        const boundary = unit.repeat(Math.floor(limit / width)) + "a".repeat(limit % width);
        assert.equal(Buffer.byteLength(boundary, "utf8"), limit);
        const value = paneAgentNode();
        value.herdr[kind][0][field] = boundary;
        assert.equal(snapshot([value]).nodes[0].herdr[kind][0][field], boundary);
        value.herdr[kind][0][field] += "a";
        assert.throws(() => snapshot([value]), { code: "invalid_response" }, `${kind}.${field}`);
      }
    }
  }
  assert.equal(snapshot([node("n", { hostname: "a".repeat(256) })]).nodes[0].hostname.length, 256);
  assert.throws(() => snapshot([node("n", { hostname: "a".repeat(257) })]), { code: "invalid_response" });
});

function namedNode(id = "node-1") {
  const value = paneAgentNode(id);
  value.hostname = "Build machine";
  const names = { workspaces: "Workspace Atlas", tabs: "Review tab", panes: "Shell pane", agents: "Coding agent" };
  for (const [kind, display_name] of Object.entries(names)) {
    Object.assign(value.herdr[kind][0], { display_name, directory: `C:\\Repos\\${kind}\\project` });
  }
  return value;
}

test("display labels prefer configured literal Unicode names with IDs secondary, never inferred titles", () => {
  assert.deepEqual(entityLabel({ id: "p1", display_name: "\u958b\u767a \ud83d\ude80 <img src=x>" }),
    { primary: "\u958b\u767a \ud83d\ude80 <img src=x>", secondary: "p1" });
  assert.deepEqual(entityLabel({ id: "p1", title: "Ignored", prompt: "Ignored" }), { primary: "p1", secondary: "" });
  assert.deepEqual(entityLabel(null, "orphan"), { primary: "orphan", secondary: "" });
  assert.deepEqual(entityLabel(null, "", "Missing"), { primary: "Missing", secondary: "" });
  assert.deepEqual(entityLabel(undefined), { primary: "Not supplied", secondary: "" });
});

test("agent display names prefer their own name then an exactly scoped pane label, never provider or terminal text", () => {
  const pane = entity("p1", { display_name: "Scoped pane" });
  const agent = entity("p1", { display_name: "Own name", provider: "copilot", title: "Terminal", prompt: "Prompt" });
  assert.equal(agentDisplayName(agent, pane), "Own name");
  for (const display_name of ["", undefined]) {
    agent.display_name = display_name;
    assert.equal(agentDisplayName(agent, pane), "Scoped pane");
    assert.equal(agentDisplayName(agent, null), "");
    assert.equal(agentDisplayName(agent, { ...pane, display_name: "", title: "Terminal", prompt: "Prompt" }), "");
    for (const field of ["id", "workspace_id", "tab_id"]) {
      assert.equal(agentDisplayName(agent, { ...pane, [field]: "other" }), "", field);
    }
  }
});

test("pane fallback labels are searchable without borrowing from other nodes, sessions, workspaces, or tabs", () => {
  const value = multiSessionNode();
  for (const session of value.sessions) {
    session.herdr = namedNode().herdr;
    session.herdr.agents[0].display_name = "";
  }
  value.sessions[0].herdr.panes = [];
  const otherNode = namedNode("node-2");
  otherNode.herdr.agents[0].display_name = "";
  otherNode.herdr.panes[0].display_name = "Other machine pane";
  const nodes = snapshot([value, otherNode]).nodes;
  assert.deepEqual(projectAgents(nodes).map(({ agent, pane }) => agentDisplayName(agent, pane)),
    ["", "Shell pane", "Other machine pane"]);
  assert.deepEqual(projectAgents(nodes, " SHELL PANE ").map(({ node }) => [node.instance_id, node.session_name]),
    [["node-1", "dev-b"]]);
  assert.equal(projectAgents(nodes, "Shell pane", "blocked").length, 0);
  assert.equal(projectAgents(nodes, "Shell pane", "all", { session: "dev-a" }).length, 0);
  assert.equal(projectAgents(nodes, "Shell pane", "all", { provider: "copilot" }).length, 0);
  for (const project of [projectWorkspaces, projectNodes]) assert.equal(project(nodes, "Shell pane").length, 1);
  for (const field of ["workspace_id", "tab_id"]) {
    const mismatched = namedNode();
    mismatched.herdr.agents[0].display_name = "";
    mismatched.herdr.panes[0][field] = "other";
    const parsed = snapshot([mismatched]).nodes;
    assert.equal(projectAgents(parsed, "Shell pane").length, 0, field);
    assert.equal(agentDisplayName(projectAgents(parsed)[0].agent, projectAgents(parsed)[0].pane), "");
  }
});

test("name and path search finds agents by their metadata, node name, and containing workspace or tab", () => {
  const nodes = snapshot([namedNode()]).nodes;
  for (const query of ["coding AGENT", "Workspace ATLAS", "Review TAB", "BUILD MACHINE",
    "c:\\repos\\agents", "C:\\Repos\\workspaces", "C:\\Repos\\tabs", "w1:p1", "node-1"]) {
    assert.equal(projectAgents(nodes, `  ${query}  `, "working").length, 1, query);
    assert.equal(projectAgents(nodes, query, "blocked").length, 0, query);
    assert.equal(projectWorkspaces(nodes, query).length, 1, query);
    assert.equal(projectNodes(nodes, query).length, 1, query);
  }
  for (const query of ["Shell pane", "C:\\Repos\\panes"]) {
    assert.equal(projectWorkspaces(nodes, query).length, 1);
    assert.equal(projectNodes(nodes, query).length, 1);
  }
  assert.equal(projectAgents(nodes, "absent label").length, 0);
  const empty = namedNode();
  empty.herdr.agents = [];
  empty.herdr.panes = [];
  for (const query of ["Review tab", "C:\\Repos\\tabs", "Workspace Atlas", "C:\\Repos\\workspaces"]) {
    assert.equal(projectWorkspaces(snapshot([empty]).nodes, query).length, 1, "empty named containers remain searchable");
    assert.equal(projectNodes(snapshot([empty]).nodes, query).length, 1);
  }
  empty.herdr.tabs = [];
  empty.herdr.workspaces = [];
  assert.equal(projectNodes(snapshot([empty]).nodes, "Build machine").length, 1);
});

test("directories distinguish reported values, scoped workspace fallback, and unknown orphan data", () => {
  const value = namedNode();
  value.herdr.workspaces.push(entity("w2", { display_name: "Workspace Atlas", directory: "C:\\Other" }));
  value.herdr.tabs.push(entity("w1:t1", { workspace_id: "w2", display_name: "Other tab", directory: "C:\\OtherTab" }));
  value.herdr.agents.push(entity("orphan", { workspace_id: "missing", tab_id: "w1:t1" }));
  const parsed = snapshot([value]).nodes[0];
  const agent = parsed.herdr.agents.find((item) => item.id === "w1:p1");
  assert.deepEqual(entityDirectory(parsed, agent), { directory: "C:\\Repos\\agents\\project", source: "reported" });
  agent.directory = "";
  assert.deepEqual(entityDirectory(parsed, agent), { directory: "C:\\Repos\\workspaces\\project", source: "workspace" });
  assert.equal(entityWorkspace(parsed, agent).id, "w1");
  assert.equal(entityTab(parsed, agent).display_name, "Review tab");
  assert.equal(projectAgents([parsed], "Other tab").length, 0);
  assert.equal(projectAgents([parsed], "C:\\Other").length, 0);
  const orphan = parsed.herdr.agents.find((item) => item.id === "orphan");
  assert.equal(entityWorkspace(parsed, orphan), null);
  assert.equal(entityTab(parsed, orphan), null);
  assert.deepEqual(entityDirectory(parsed, orphan), { directory: "", source: "unknown" });
  orphan.directory = "C:\\Orphan";
  assert.deepEqual(entityDirectory(parsed, orphan), { directory: "C:\\Orphan", source: "reported" });
  assert.equal(projectAgents([parsed], "C:\\Orphan")[0].agent.id, "orphan");
  assert.equal(projectWorkspaces([parsed], "C:\\Orphan")[0].workspace.entity, null);
  parsed.herdr.workspaces.find((item) => item.id === "w1").directory = "";
  assert.deepEqual(entityDirectory(parsed, agent), { directory: "", source: "unknown" }, "tab/pane paths are not agent cwd fallbacks");
  parsed.herdr.workspaces.push(entity("", { directory: "C:\\Unassigned" }));
  assert.deepEqual(entityDirectory(parsed, { workspace_id: "" }), { directory: "", source: "unknown" });
  assert.deepEqual(entityDirectory(parsed, null), { directory: "", source: "unknown" });
});

function containedDirectoryNode() {
  const value = namedNode();
  delete value.herdr.workspaces[0].directory;
  value.herdr.panes[0].display_name = "";
  value.herdr.agents[0].display_name = "";
  value.herdr.panes[0].directory = "C:\\Shared";
  value.herdr.agents[0].directory = "C:\\Shared";
  value.herdr.panes.push(entity("p2", { workspace_id: "w1", tab_id: "", directory: "C:\\Unassigned" }),
    entity("p3", { workspace_id: "w1", tab_id: "w1:t1", directory: "" }),
    entity("p4", { workspace_id: "w2", directory: "C:\\OtherWorkspace" }));
  value.herdr.agents.push(entity("orphan", { workspace_id: "w1", tab_id: "missing-tab", directory: "C:\\AgentOnly" }),
    entity("missing-ws", { workspace_id: "missing-ws", directory: "C:\\MissingWorkspace" }));
  value.herdr.workspaces.push(entity("w2", { display_name: "Workspace Atlas" }), entity("empty-workspace"));
  return value;
}

test("workspace reported directories are distinct scoped pane and agent paths, including unassigned and orphan references", () => {
  const parsed = snapshot([containedDirectoryNode()]).nodes[0];
  const groups = topology(parsed);
  const workspace = groups.find((item) => item.id === "w1");
  const before = structuredClone(workspace);
  assert.deepEqual(workspaceDirectories(workspace), ["C:\\AgentOnly", "C:\\Shared", "C:\\Unassigned"]);
  assert.deepEqual(workspace, before, "directory aggregation must not mutate inventory or invent a workspace path");
  assert.equal(workspace.entity.directory, "");
  assert.deepEqual(workspaceDirectories(groups.find((item) => item.id === "w2")), ["C:\\OtherWorkspace"]);
  assert.deepEqual(workspaceDirectories(groups.find((item) => item.id === "missing-ws")), ["C:\\MissingWorkspace"]);
  assert.deepEqual(workspaceDirectories(groups.find((item) => item.id === "empty-workspace")), []);
  for (const query of ["C:\\AgentOnly", "C:\\Shared", "C:\\Unassigned"]) {
    assert.deepEqual(projectWorkspaces([parsed], query).map(({ workspace }) => workspace.id), ["w1"]);
  }
});

test("workspace directory aggregation cannot cross sessions or nodes reusing workspace IDs and labels", () => {
  const value = multiSessionNode();
  for (const [index, session] of value.sessions.entries()) {
    session.herdr = namedNode().herdr;
    session.herdr.workspaces[0].directory = "";
    session.herdr.agents[0].directory = "";
    session.herdr.panes[0].directory = `C:\\Session${index}`;
  }
  const other = namedNode("other-node");
  other.herdr.workspaces[0].directory = "";
  other.herdr.agents[0].directory = "";
  other.herdr.panes[0].directory = "C:\\OtherNode";
  const workspaces = projectWorkspaces(snapshot([value, other]).nodes);
  assert.deepEqual(workspaces.map(({ workspace }) => workspaceDirectories(workspace)),
    [["C:\\Session0"], ["C:\\Session1"], ["C:\\OtherNode"]]);
});

test("metadata lookup and inherited search never borrow across nodes or sessions with duplicate names and IDs", () => {
  const first = multiSessionNode();
  first.sessions[0].herdr = namedNode().herdr;
  first.sessions[1].herdr = namedNode().herdr;
  first.sessions[0].herdr.workspaces[0].directory = "C:\\OnlySessionA";
  first.sessions[1].herdr.workspaces = [];
  first.sessions[1].herdr.tabs = [];
  for (const session of first.sessions) session.herdr.agents[0].directory = "";
  const second = namedNode("node-2");
  second.herdr.workspaces[0].directory = "C:\\OnlyNode2";
  second.herdr.agents[0].directory = "";
  const nodes = snapshot([first, second]).nodes;
  const rows = projectAgents(nodes);
  assert.deepEqual(rows.map(({ node, agent }) => entityDirectory(node, agent)), [
    { directory: "C:\\OnlySessionA", source: "workspace" },
    { directory: "", source: "unknown" },
    { directory: "C:\\OnlyNode2", source: "workspace" },
  ]);
  assert.equal(entityTab(rows[1].node, rows[1].agent), null);
  assert.deepEqual(projectAgents(nodes, "C:\\OnlySessionA").map(({ node }) => node.session_name), ["dev-a"]);
  assert.deepEqual(projectAgents(nodes, "C:\\OnlyNode2").map(({ node }) => node.instance_id), ["node-2"]);
  assert.equal(projectAgents(nodes, "Review tab").length, 2);
  assert.equal(projectAgents(nodes, "Coding agent").length, 3, "duplicate labels do not merge agents");
});

test("duplicate configured names and renames preserve ID-based topology, order, and pane associations", () => {
  const value = namedNode();
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
    const original = value.herdr[kind][0];
    value.herdr[kind].push({ ...original, id: kind === "workspaces" ? "w2" : kind === "tabs" ? "w2:t1" : "w2:p1",
      workspace_id: "w2", tab_id: "w2:t1" });
  }
  const before = snapshot([value]).nodes;
  const structure = (nodes) => projectAgents(nodes).map(({ node, agent, pane }) =>
    [node.instance_id, node.session_name, node.session_incarnation, agent.workspace_id, agent.tab_id, agent.id, pane.id]);
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) value.herdr[kind][0].display_name = "Renamed";
  const after = snapshot([value]).nodes;
  assert.deepEqual(structure(after), structure(before));
  assert.deepEqual(topology(after[0]).map((ws) => [ws.id, ws.tabs[0].id, ws.tabs[0].paneGroups[0].id]),
    [["w1", "w1:t1", "w1:p1"], ["w2", "w2:t1", "w2:p1"]]);
  assert.equal(projectAgents(after, "Coding agent")[0].agent.workspace_id, "w2");
  assert.equal(projectAgents(after, "Renamed")[0].agent.workspace_id, "w1");
  assert.equal(projectAgents(after).length, 2);
});

test("named sessions isolate reused pane IDs without requiring a ready default socket", () => {
  const value = multiSessionNode();
  value.sessions[0].herdr.panes = [];
  const state = success([value]);
  const rows = projectAgents(state.snapshot.nodes);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].node.session_name, "dev-a");
  assert.equal(rows[0].pane, null);
  assert.equal(rows[1].node.session_name, "dev-b");
  assert.equal(rows[1].pane.id, "w1:p1");
  assert.equal(nodeFreshness(rows[0].node, state, now).live, true);
  assert.equal(nodeFreshness(state.snapshot.nodes[0], state, now).live, true);
  assert.equal(summary(state, now).live.connected, 1);
  assert.equal(summary(state, now).live.fresh, 1);
  assert.equal(summary(state, now).live.workspaces, 2);
  assert.equal(projectWorkspaces(state.snapshot.nodes).length, 2);
  assert.equal(projectNodes(state.snapshot.nodes, "dev-b").length, 1);
});

test("literal default never replaces an unidentified configured endpoint", () => {
  const value = paneAgentNode();
  value.sessions_ready = true;
  value.sessions_received_at = iso();
  value.sessions = [{ name: "default", incarnation: "default-incarnation", status: "ready", error_code: "", herdr: value.herdr }];
  const state = success([value]);
  const contexts = sessionContexts(state.snapshot.nodes[0]);
  assert.equal(contexts.length, 2);
  assert.equal(contexts[0].session_name, "");
  assert.equal(contexts[0].session_label, "Configured default");
  assert.equal(contexts[1].session_name, "default");
  assert.equal(nodeFreshness(contexts[0], state, now).live, true);
  assert.equal(projectAgents(state.snapshot.nodes).length, 2);
  assert.equal(projectAgents(state.snapshot.nodes, "", "all", { session: "default" }).length, 1);
  assert.equal(summary(state, now).known.workspaces, 2);
});

test("session freshness uses its own observation stream and never revives stopped retained data", () => {
  const value = multiSessionNode();
  value.sessions[0].herdr_received_at = iso(-30_000);
  value.sessions[1].status = "stopped";
  const state = success([value]);
  assert.equal(summary(state, now).live.workspaces, 0);
  assert.equal(summary(state, now).known.workspaces, 2);
  assert.equal(projectAgents(state.snapshot.nodes).length, 2, "retained inventory remains visible");
  for (const row of projectAgents(state.snapshot.nodes)) assert.equal(nodeFreshness(row.node, state, now).live, false);
});

test("an explicitly missing child timestamp cannot borrow aggregate freshness", () => {
  const value = multiSessionNode();
  value.sessions[0].herdr_received_at = null;
  value.sessions[0].stale = false;
  const state = success([value]);
  const child = sessionContexts(state.snapshot.nodes[0]).find((item) => item.session_name === "dev-a");
  assert.equal(child.herdr_received_at, null);
  assert.equal(nodeFreshness(child, state, now).live, false);
  assert.equal(summary(state, now).live.workspaces, 1);
  assert.equal(summary(state, now).known.workspaces, 2);
});

test("project provider readiness and session filters compose on the same agent", () => {
  const value = multiSessionNode();
  for (const session of value.sessions) {
    session.herdr.workspaces[0].project_id = "example-app";
    session.herdr.agents[0].provider = session.name === "dev-a" ? "copilot" : "claude";
    session.herdr.agents[0].interactive_ready = session.name === "dev-a";
  }
  const nodes = snapshot([value]).nodes;
  const filters = { project: "example-app", provider: "COPILOT", readiness: "ready", session: "dev-a" };
  assert.equal(projectAgents(nodes, "", "working", filters).length, 1);
  assert.equal(projectWorkspaces(nodes, "", "working", filters).length, 1);
  assert.equal(projectNodes(nodes, "", "working", filters).length, 1);
  assert.equal(projectAgents(nodes, "", "working", { ...filters, session: "dev-b" }).length, 0);
  assert.equal(projectAgents(nodes, "", "working", { ...filters, readiness: "not-ready" }).length, 0);
  assert.equal(projectAgents(nodes, "claude").length, 1);
  assert.equal(projectAgents(nodes, "example-app").length, 2);
  assert.equal(projectWorkspaces(nodes, "example-app").length, 2);
});

test("legacy provider/readiness remain unknown instead of fabricating readiness or a project", () => {
  const nodes = snapshot([paneAgentNode()]).nodes;
  assert.equal(projectAgents(nodes, "", "all", { readiness: "unknown", provider: "unknown" }).length, 1);
  assert.equal(projectAgents(nodes, "", "all", { readiness: "ready" }).length, 0);
  assert.equal(projectAgents(nodes, "", "all", { project: "example-app" }).length, 0);
});

test("session parsing rejects duplicate identities, malformed metadata and excessive sessions", () => {
  for (const change of [
    (n) => { n.sessions[1].name = n.sessions[0].name; },
    (n) => { n.sessions[0].status = "invented"; },
    (n) => { n.sessions[0].incarnation = {}; },
    (n) => { n.sessions_received_at = "yesterday"; },
    (n) => { n.sessions[0].herdr.agents[0].provider = "<script>"; },
    (n) => { n.sessions[0].herdr.agents[0].interactive_ready = "yes"; },
    (n) => { n.sessions = Array.from({ length: 65 }, (_, i) => ({ ...n.sessions[0], name: `dev-${i}` })); },
  ]) {
    const value = multiSessionNode();
    change(value);
    assert.throws(() => snapshot([value]), { code: "invalid_response" });
  }
});

test("not-yet-observed sessions preserve lifecycle status without inventing inventory", () => {
  const value = multiSessionNode();
  value.sessions[0].status = "starting";
  value.sessions[0].herdr = null;
  const state = success([value]);
  const pending = sessionContexts(state.snapshot.nodes[0]).find((item) => item.session_name === "dev-a");
  assert.equal(pending.session_status, "starting");
  assert.equal(pending.herdr.status, "waiting");
  assert.equal(nodeFreshness(pending, state, now).live, false);
  assert.equal(projectAgents(state.snapshot.nodes).length, 1);
});

test("real-shaped agent IDs resolve to panes in the table projection and nested topology", () => {
  const nodes = snapshot([paneAgentNode()]).nodes;
  const rows = projectAgents(nodes, "w1:p1", "working");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].agent.id, "w1:p1");
  assert.equal(rows[0].pane, nodes[0].herdr.panes[0]);
  const workspace = projectWorkspaces(nodes, "w1:p1", "working")[0].workspace;
  const panes = workspace.tabs[0].paneGroups;
  assert.equal(panes.length, 1, "matched agents must not create duplicate pane groups");
  assert.equal(panes[0].id, "w1:p1");
  assert.equal(panes[0].entity, rows[0].pane);
  assert.deepEqual(panes[0].agents, [rows[0].agent]);
  assert.equal(panes[0].agents[0].focused, true);
});

test("missing pane targets retain the referenced ID and orphan agent without inventing a pane entity", () => {
  const value = paneAgentNode();
  value.herdr.agents.push(entity("w1:p404", { workspace_id: "w1", tab_id: "w1:t1", agent_status: "blocked" }));
  const nodes = snapshot([value]).nodes;
  const rows = projectAgents(nodes, "w1:p404", "blocked");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].agent.id, "w1:p404");
  assert.equal(rows[0].pane, null);
  const workspace = projectWorkspaces(nodes, "w1:p404", "blocked")[0].workspace;
  const panes = workspace.tabs[0].paneGroups;
  assert.deepEqual(panes.map((pane) => pane.id), ["w1:p1", "w1:p404"]);
  assert.equal(panes[1].entity, null);
  assert.deepEqual(panes[1].agents, [rows[0].agent]);
  assert.equal(nodes[0].herdr.panes.length, 1, "synthetic references must not change reported inventory");
  assert.equal(summary(success([value]), now).live.blocked, 1);
});

test("pane matching never crosses node, workspace, or tab boundaries", () => {
  for (const field of ["workspace_id", "tab_id"]) {
    const value = paneAgentNode();
    value.herdr.panes[0][field] = "other";
    const nodes = snapshot([value]).nodes;
    assert.equal(projectAgents(nodes)[0].pane, null, field);
    const groups = topology(nodes[0]).flatMap((ws) => [ws, ...ws.tabs])
      .flatMap((group) => group.paneGroups);
    const orphan = groups.find((pane) => pane.agents.length);
    assert.equal(orphan.id, "w1:p1");
    assert.equal(orphan.entity, null, field);
  }
  const missing = paneAgentNode("node-A");
  missing.herdr.panes = [];
  const owner = paneAgentNode("node-B");
  const rows = projectAgents(snapshot([missing, owner]).nodes);
  assert.equal(rows[0].pane, null);
  assert.equal(rows[1].pane.id, "w1:p1");
  assert.equal(topology(rows[0].node)[0].tabs[0].paneGroups[0].entity, null);
});

test("unassigned orphan agents retain their pane reference in workspace topology", () => {
  const value = paneAgentNode();
  value.herdr.agents[0].workspace_id = "";
  value.herdr.agents[0].tab_id = "";
  const group = topology(snapshot([value]).nodes[0])[0];
  assert.equal(group.id, "");
  assert.equal(group.paneGroups[0].id, "w1:p1");
  assert.equal(group.paneGroups[0].entity, null);
  assert.equal(group.paneGroups[0].agents[0].id, "w1:p1");
});

test("inventory counts use singular for one and plural for zero or multiple entities", () => {
  for (const kind of ["agent", "workspace", "node", "tab", "pane"]) {
    assert.equal(countLabel(0, kind), `0 ${kind}s`);
    assert.equal(countLabel(1, kind), `1 ${kind}`);
    assert.equal(countLabel(2, kind), `2 ${kind}s`);
  }
});

test("age labels handle missing times, subsecond boundaries, and future clocks", () => {
  assert.equal(ageLabel(null, now), "No timestamp");
  assert.equal(ageLabel(iso(), now), "Just now");
  assert.equal(ageLabel(iso(-29_999), now), "29s ago");
  assert.equal(ageLabel(iso(-61_000), now), "1m ago");
  assert.equal(ageLabel(iso(-3_600_000), now), "1h ago");
  assert.equal(ageLabel(iso(31_000), now), "Clock ahead");
});

test("request is same-origin, uncached, authenticated by the dashboard header", async () => {
  let captured;
  const result = await fetchSnapshot({ fetchImpl: async (url, options) => {
    captured = { url, options };
    return { ok: true, json: async () => ({ nodes: [] }) };
  } });
  assert.deepEqual(result, { nodes: [] });
  assert.equal(captured.url, "/api/nodes");
  assert.deepEqual(captured.options.headers, { "X-Herdr-Dashboard": "1" });
  assert.equal(captured.options.credentials, "same-origin");
  assert.equal(captured.options.mode, "same-origin");
  assert.equal(captured.options.cache, "no-store");
  assert.equal(captured.options.signal.aborted, false);
});

test("all API error categories use fixed local copy, never the server message", async () => {
  for (const code of [...Object.keys(ERROR_MESSAGES), "<script>", "__proto__"]) {
    const expected = Object.hasOwn(ERROR_MESSAGES, code) ? code : "internal_error";
    await assert.rejects(fetchSnapshot({ fetchImpl: async () => ({
      ok: false, json: async () => ({ error: { code, message: "PRIVATE UNTRUSTED SERVER TEXT" }, nodes: [node()] }),
    }) }), (error) => error.code === expected && error.message === ERROR_MESSAGES[expected]);
  }
});

test("invalid JSON, malformed success, and transport failure are explicit errors", async () => {
  await assert.rejects(fetchSnapshot({ fetchImpl: async () => ({ ok: true, json: async () => { throw new SyntaxError(); } }) }), { code: "invalid_response" });
  await assert.rejects(fetchSnapshot({ fetchImpl: async () => ({ ok: true, json: async () => ({}) }) }), { code: "invalid_response" });
  await assert.rejects(fetchSnapshot({ fetchImpl: async () => { throw new TypeError("network"); } }), { code: "server_unavailable" });
});

test("fetch aborts after exactly 10 seconds and clears its timeout", async () => {
  let expire;
  let cleared = false;
  let signal;
  const request = fetchSnapshot({
    setTimer(callback, delay) { assert.equal(delay, 10_000); expire = callback; return 7; },
    clearTimer(id) { assert.equal(id, 7); cleared = true; },
    fetchImpl: async (_url, options) => {
      signal = options.signal;
      return new Promise((_resolve, reject) => signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError"))));
    },
  });
  expire();
  await assert.rejects(request, { code: "request_timeout" });
  assert.equal(signal.aborted, true);
  assert.equal(cleared, true);
});

test("timeout also covers a response body that stalls", async () => {
  let expire;
  let bodyStarted;
  const started = new Promise((resolve) => { bodyStarted = resolve; });
  const request = fetchSnapshot({
    setTimer(callback) { expire = callback; return 1; }, clearTimer() {},
    fetchImpl: async (_url, { signal }) => ({ ok: true, json: () => {
      bodyStarted();
      return new Promise((_resolve, reject) => signal.addEventListener("abort", () => reject(new Error("aborted"))));
    } }),
  });
  await started;
  expire();
  await assert.rejects(request, { code: "request_timeout" });
});

test("manual, auto, and resume refreshes share one in-flight request and schedule after completion", async () => {
  let resolve;
  let loads = 0;
  let starts = 0;
  const scheduled = [];
  const poller = createPoller({
    load: () => { loads++; return new Promise((done) => { resolve = done; }); },
    onStart: () => { starts++; }, onSuccess() {}, onError: assert.fail,
    setTimer(callback, delay) { scheduled.push({ callback, delay }); return scheduled.length; }, clearTimer() {},
  });
  const first = poller.refresh();
  assert.equal(poller.refresh(), first);
  assert.equal(poller.setAutomatic(true), first);
  await Promise.resolve();
  assert.equal(loads, 1);
  assert.equal(starts, 1);
  assert.equal(scheduled.length, 0);
  resolve(snapshot([]));
  await first;
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0].delay, 3_000);
  poller.stop();
});

test("poller recovers from failure; pausing does not interrupt manual refresh", async () => {
  let loads = 0;
  const events = [];
  const scheduled = new Map();
  let id = 0;
  const poller = createPoller({
    load: async () => { if (++loads === 1) throw new DashboardError("busy"); return snapshot([]); },
    onStart() {}, onSuccess: () => events.push("success"), onError: (error) => events.push(error.code),
    setTimer(callback) { scheduled.set(++id, callback); return id; },
    clearTimer(key) { scheduled.delete(key); },
  });
  await poller.refresh();
  assert.deepEqual(events, ["busy"]);
  const next = [...scheduled.values()][0];
  await next();
  assert.deepEqual(events, ["busy", "success"]);
  poller.setAutomatic(false);
  assert.equal(scheduled.size, 0);
  await poller.refresh();
  assert.deepEqual(events, ["busy", "success", "success"]);
  assert.equal(scheduled.size, 0);
  poller.stop();
  await poller.refresh();
  assert.equal(loads, 3);
});

async function renderHarness() {
  class Element {
    constructor(tagName) {
      this.tagName = tagName.toUpperCase();
      this.children = [];
      this.dataset = {};
      this.className = "";
    }
    append(...children) { this.children.push(...children); }
    set textContent(value) { this.children = [{ textContent: String(value) }]; }
    get textContent() { return this.children.map((child) => child.textContent).join(""); }
  }
  const app = await readFile(new URL("./app.js", import.meta.url), "utf8");
  const context = vm.createContext({ ...model, document: {
    createElement: (tag) => new Element(tag),
    createTextNode: (textContent) => ({ textContent }),
  } });
  vm.runInContext(app.slice(app.indexOf("const $"), app.indexOf("const poller =")), context);
  return vm.runInContext("({ agentRow, workspaceCard, nodeCard, setState(value) { state = value; } })", context);
}
const descendants = (element) => [element, ...(element.children ?? []).flatMap(descendants)];
const tableCell = (row, label) => row.children.find((cell) => cell.dataset.label === label);

test("browser rows and topology render names first, secondary IDs, literal Unicode, and explicit directory sources", async () => {
  const ui = await renderHarness();
  const value = namedNode();
  const literal = "\u958b\u767a \ud83d\ude80 <img src=x onerror=alert(1)>";
  value.herdr.agents[0].display_name = literal;
  value.herdr.agents[0].directory = "";
  const state = success([value]);
  ui.setState(state);
  const row = ui.agentRow(projectAgents(state.snapshot.nodes)[0], now);
  const agentIdentity = row.children[0].children[0].children[0];
  assert.equal(agentIdentity.children[0].textContent, literal);
  assert.equal(agentIdentity.children[1].textContent, "ID: w1:p1");
  assert.equal(tableCell(row, "Directory").textContent, "Workspace directory (fallback)C:\\Repos\\workspaces\\project");
  assert.equal(tableCell(row, "Directory").children[0].dataset.directorySource, "workspace");
  for (const label of ["Build machine", "Workspace Atlas", "Review tab", "Shell pane"]) assert.ok(row.textContent.includes(label));
  assert.equal(row.children.length, 12);
  const card = ui.workspaceCard(projectWorkspaces(state.snapshot.nodes)[0], now);
  for (const label of ["Workspace Atlas", "Review tab", "Shell pane", literal, "ID: w1", "ID: w1:t1", "ID: w1:p1",
    "Reported directory", "Workspace directory (fallback)", "C:\\Repos\\panes\\project"]) {
    assert.ok(card.textContent.includes(label), label);
  }
  const nodeCard = ui.nodeCard(state.snapshot.nodes[0], now);
  assert.ok(nodeCard.textContent.includes("Build machineID: node-1"));
  const names = descendants(card).filter((item) => item.className === "entity-name");
  assert.ok(names.some((item) => item.tagName === "BDI" && item.textContent === literal));
  assert.ok(descendants(card).every((item) => item.tagName !== "IMG"));
  assert.ok(descendants(card).filter((item) => item.className === "directory-path").every((item) => item.tagName === "BDI"));
  const keys = (element) => descendants(element).filter((item) => item.dataset?.key).map((item) => item.dataset.key);
  value.hostname = "Renamed node";
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) value.herdr[kind][0].display_name = "Renamed";
  const renamed = success([value]);
  ui.setState(renamed);
  assert.deepEqual(keys(ui.agentRow(projectAgents(renamed.snapshot.nodes)[0], now)), keys(row));
  assert.deepEqual(keys(ui.workspaceCard(projectWorkspaces(renamed.snapshot.nodes)[0], now)), keys(card));
  assert.deepEqual(keys(ui.nodeCard(renamed.snapshot.nodes[0], now)), keys(nodeCard));
});

test("browser orphan references and absent directories stay explicit without fabricated names or cwd", async () => {
  const ui = await renderHarness();
  const value = namedNode();
  value.herdr.workspaces = [];
  value.herdr.tabs = [];
  value.herdr.panes = [];
  value.herdr.agents[0].display_name = "";
  value.herdr.agents[0].directory = "";
  const state = success([value]);
  ui.setState(state);
  const row = ui.agentRow(projectAgents(state.snapshot.nodes)[0], now);
  assert.equal(row.children[0].children[0].children[0].textContent, "w1:p1");
  assert.equal(tableCell(row, "Directory").textContent, "Directory not reported");
  for (const kind of ["workspace", "tab", "pane"]) assert.ok(row.textContent.includes(`Unresolved ${kind}`));
  const card = ui.workspaceCard(projectWorkspaces(state.snapshot.nodes)[0], now);
  assert.ok(card.textContent.includes("Unresolved workspace reference"));
  assert.ok(card.textContent.includes("Unresolved tab reference"));
  assert.ok(card.textContent.includes("Unresolved pane reference"));
  assert.ok(card.textContent.includes("Directory not reported"));
  assert.ok(!card.textContent.includes("Workspace directory (fallback)"));
  value.herdr.agents[0].directory = "C:\\ReportedAgent";
  const reported = success([value]);
  ui.setState(reported);
  const reportedRow = ui.agentRow(projectAgents(reported.snapshot.nodes)[0], now);
  assert.equal(tableCell(reportedRow, "Directory").textContent, "Reported directoryC:\\ReportedAgent");
  assert.equal(tableCell(reportedRow, "Directory").children[0].dataset.directorySource, "reported");
});

test("browser agent table and topology use scoped pane labels as fallback while retaining IDs and directory provenance", async () => {
  const ui = await renderHarness();
  const value = namedNode();
  const literal = "\u958b\u767a <img src=x>";
  value.herdr.panes[0].display_name = literal;
  value.herdr.agents[0].directory = "";
  value.herdr.workspaces[0].directory = "";
  let previousKey;
  for (const ownName of [undefined, "", "Own name"]) {
    if (ownName === undefined) delete value.herdr.agents[0].display_name;
    else value.herdr.agents[0].display_name = ownName;
    const state = success([value]);
    ui.setState(state);
    const row = ui.agentRow(projectAgents(state.snapshot.nodes)[0], now);
    const label = row.children[0].children[0].children[0];
    assert.equal(label.children[0].textContent, ownName || literal);
    assert.equal(label.children[0].className, "entity-name");
    assert.equal(label.children[1].textContent, "ID: w1:p1");
    assert.equal(tableCell(row, "Directory").textContent, "Directory not reported", "pane name fallback does not copy its cwd");
    if (previousKey) assert.equal(row.dataset.key, previousKey);
    previousKey = row.dataset.key;
    const card = ui.workspaceCard(projectWorkspaces(state.snapshot.nodes)[0], now);
    const agent = descendants(card).find((item) => item.dataset?.key &&
      JSON.parse(item.dataset.key)[0] === "Agent");
    assert.equal(agent.children[1].children[0].textContent, ownName || literal);
    assert.equal(agent.children[1].children[1].textContent, "ID: w1:p1");
    assert.ok(descendants(card).every((item) => item.tagName !== "IMG"));
  }
});

test("workspace cards expose contained reported directories outside collapsed tabs without guessing a workspace cwd", async () => {
  const ui = await renderHarness();
  const value = containedDirectoryNode();
  const state = success([value]);
  ui.setState(state);
  const groups = projectWorkspaces(state.snapshot.nodes);
  const card = ui.workspaceCard(groups.find(({ workspace }) => workspace.id === "w1"), now);
  const summary = card.children[1];
  assert.equal(summary.className, "workspace-directories");
  assert.ok(summary.textContent.includes("Directory not reported"));
  assert.ok(summary.textContent.includes("Reported directories"));
  assert.ok(summary.textContent.includes("Reported by contained panes and agents; not a workspace directory."));
  assert.ok(!summary.textContent.includes("Workspace directory (fallback)"));
  const paths = descendants(summary).filter((item) => item.className === "directory-path");
  assert.deepEqual(paths.map((item) => item.textContent), ["C:\\AgentOnly", "C:\\Shared", "C:\\Unassigned"]);
  assert.ok(descendants(summary).every((item) => item.tagName !== "DETAILS"));
  const orphan = ui.workspaceCard(groups.find(({ workspace }) => workspace.id === "missing-ws"), now);
  assert.ok(orphan.children[1].textContent.includes("C:\\MissingWorkspace"));
  const empty = ui.workspaceCard(groups.find(({ workspace }) => workspace.id === "empty-workspace"), now);
  assert.equal(empty.children[1].textContent, "Directory not reported");
  value.herdr.workspaces[0].directory = "C:\\ExplicitCheckout";
  const reported = success([value]);
  ui.setState(reported);
  const direct = ui.workspaceCard(projectWorkspaces(reported.snapshot.nodes).find(({ workspace }) => workspace.id === "w1"), now);
  assert.equal(direct.children[1].textContent, "Reported directoryC:\\ExplicitCheckout");
  assert.equal(direct.dataset.key, card.dataset.key);
});

test("unnamed agents prominently show workspace and tab labels and their reported directory beside stable IDs", async () => {
  const ui = await renderHarness();
  const value = containedDirectoryNode();
  const state = success([value]);
  ui.setState(state);
  const row = ui.agentRow(projectAgents(state.snapshot.nodes).find(({ agent }) => agent.id === "w1:p1"), now);
  assert.deepEqual(row.children.slice(0, 4).map((cell) => cell.dataset.label), ["Agent / status", "Workspace", "Tab", "Directory"]);
  assert.equal(row.children[0].children[0].children[0].textContent, "w1:p1");
  assert.equal(row.children[1].children[0].children[0].children[0].textContent, "Workspace Atlas");
  assert.equal(row.children[2].children[0].children[0].children[0].textContent, "Review tab");
  assert.equal(row.children[3].textContent, "Reported directoryC:\\Shared");
  assert.equal(row.children[3].children[0].dataset.directorySource, "reported");
  const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
  assert.deepEqual([...html.matchAll(/<th scope="col">([^<]+)<\/th>/g)].map((match) => match[1]),
    row.children.map((cell) => cell.dataset.label));
  value.herdr.agents[0].display_name = "Configured agent";
  value.herdr.panes[0].display_name = "Configured pane";
  const named = success([value]);
  ui.setState(named);
  const namedRow = ui.agentRow(projectAgents(named.snapshot.nodes).find(({ agent }) => agent.id === "w1:p1"), now);
  assert.equal(namedRow.children[0].children[0].children[0].children[0].textContent, "Configured agent");
  assert.equal(namedRow.dataset.key, row.dataset.key);
});

test("static assets have no inline execution, styles, HTML injection, or external dependencies", async () => {
  const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
  const app = await readFile(new URL("./app.js", import.meta.url), "utf8");
  const css = await readFile(new URL("./styles.css", import.meta.url), "utf8");
  assert.match(html, /<script type="module" src="\/app.js"><\/script>/);
  assert.doesNotMatch(html, /<style\b|\sstyle=|\son\w+=/i);
  assert.doesNotMatch(app, /innerHTML|outerHTML|insertAdjacentHTML|\beval\s*\(/);
  assert.doesNotMatch(html + app + css, /https?:\/\/|@import|url\(/);
  assert.match(html, /aria-live="polite"/);
  assert.match(css, /:focus-visible/);
  assert.match(css, /max-width: 600px/);
  assert.match(css, /\.directory-path\s*\{[^}]*overflow-wrap: anywhere/);
  assert.match(html, /Search names, directories, or IDs/);
  assert.match(html, /<th scope="col">Directory<\/th>/);
});
