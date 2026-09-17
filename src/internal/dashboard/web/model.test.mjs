import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  STALE_AFTER_MS, ERROR_MESSAGES, DashboardError, agentStatus, parseSnapshot,
  initialState, acceptSnapshot, rejectSnapshot, freshness, nodeFreshness, summary,
  projectAgents, projectWorkspaces, projectNodes, sessionContexts, topology, countLabel, ageLabel, fetchSnapshot, createPoller,
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
});
