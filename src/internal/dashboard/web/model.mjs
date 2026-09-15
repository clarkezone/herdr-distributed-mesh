export const STALE_AFTER_MS = 30_000;
export const AGENT_STATUSES = Object.freeze(["working", "blocked", "done", "idle", "unknown"]);
const HERDR_STATUSES = ["disabled", "waiting", "ready", "unavailable"];
export const ERROR_MESSAGES = Object.freeze({
  authorization_denied: "Dashboard access was denied. Check the dashboard client's authorization.",
  server_unavailable: "The mesh server is unavailable. Waiting to reconnect.",
  request_timeout: "The mesh request timed out. Try refreshing or wait for the next poll.",
  incompatible_server: "The mesh server is not compatible with this dashboard.",
  invalid_response: "The server returned an invalid dashboard response.",
  busy: "The mesh server is busy. A later refresh may succeed.",
  internal_error: "The dashboard could not retrieve mesh status.",
});

export class DashboardError extends Error {
  constructor(code) {
    const safeCode = Object.hasOwn(ERROR_MESSAGES, code) ? code : "internal_error";
    super(ERROR_MESSAGES[safeCode]);
    this.code = safeCode;
  }
}

const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const timestamp = (value) => value === null || (typeof value === "string" && /^\d{4}-\d\d-\d\dT/.test(value) && Number.isFinite(Date.parse(value)));
const strings = (value, keys) => keys.every((key) => typeof value[key] === "string");
const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const ordered = (items) => [...items].sort((a, b) => order(a.id, b.id) || order(a.workspace_id, b.workspace_id) || order(a.tab_id, b.tab_id));

export function agentStatus(value) {
  return AGENT_STATUSES.includes(value) ? value : "unknown";
}

export function parseSnapshot(body) {
  const invalid = () => { throw new DashboardError("invalid_response"); };
  if (!record(body) || !Array.isArray(body.nodes)) invalid();
  const ids = new Set();
  const nodes = body.nodes.map((node) => {
    if (!record(node) || !strings(node, ["instance_id", "tailscale_stable_id"]) ||
      typeof node.connected !== "boolean" || typeof node.stale !== "boolean" ||
      !timestamp(node.last_seen) || !timestamp(node.herdr_received_at) || !record(node.herdr) ||
      ids.has(node.instance_id)) invalid();
    ids.add(node.instance_id);
    const h = node.herdr;
    if (!HERDR_STATUSES.includes(h.status) || !strings(h, ["version", "sequence", "error_code"]) ||
      !Number.isInteger(h.protocol) || h.protocol < 0 || !timestamp(h.observed_at)) invalid();
    const herdr = { ...h };
    for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
      if (!Array.isArray(h[kind])) invalid();
      herdr[kind] = ordered(h[kind].map((entity) => {
        if (!record(entity) || !strings(entity, ["id", "workspace_id", "tab_id", "agent_status"]) ||
          typeof entity.focused !== "boolean") invalid();
        return { id: entity.id, workspace_id: entity.workspace_id, tab_id: entity.tab_id,
          focused: entity.focused, agent_status: agentStatus(entity.agent_status) };
      }));
    }
    return { ...node, herdr };
  }).sort((a, b) => order(a.instance_id, b.instance_id));
  return { nodes };
}

export function initialState() {
  return { snapshot: null, fetchedAt: null, error: null, refreshing: false };
}

export function acceptSnapshot(state, snapshot, now) {
  return { ...state, snapshot, fetchedAt: now, error: null, refreshing: false };
}

export function rejectSnapshot(state, error) {
  return { ...state, error: error instanceof DashboardError ? error : new DashboardError("internal_error"), refreshing: false };
}

export function freshness(state, now) {
  if (!state.snapshot) return state.error ? "error" : "loading";
  if (state.error || state.fetchedAt === null || now - state.fetchedAt >= STALE_AFTER_MS || now < state.fetchedAt) return "stale";
  return "live";
}

export function nodeFreshness(node, state, now) {
  if (freshness(state, now) !== "live") return { live: false, reason: "Not live · retained snapshot" };
  if (!node.connected) return { live: false, reason: "Not live · node disconnected" };
  if (node.herdr.status !== "ready") return { live: false, reason: `Not live · Herdr ${node.herdr.status}` };
  if (node.stale) return { live: false, reason: "Stale · reported by server" };
  const received = Date.parse(node.herdr_received_at);
  if (!Number.isFinite(received)) return { live: false, reason: "Stale · no snapshot timestamp" };
  if (now - received >= STALE_AFTER_MS) return { live: false, reason: "Stale · snapshot over 30s old" };
  if (received - now >= STALE_AFTER_MS) return { live: false, reason: "Not live · clock mismatch" };
  return { live: true, reason: "Fresh snapshot" };
}

function totals(nodes) {
  const result = { connected: 0, workspaces: 0, working: 0, blocked: 0, done: 0 };
  for (const node of nodes) {
    if (node.connected) result.connected++;
    result.workspaces += node.herdr.workspaces.length;
    for (const agent of node.herdr.agents) {
      if (["working", "blocked", "done"].includes(agent.agent_status)) result[agent.agent_status]++;
    }
  }
  return result;
}

export function summary(state, now) {
  const nodes = state.snapshot?.nodes ?? [];
  const freshNodes = nodes.filter((node) => nodeFreshness(node, state, now).live);
  const live = totals(freshNodes);
  live.connected = freshness(state, now) === "live" ? nodes.filter((node) => node.connected).length : 0;
  live.fresh = freshNodes.length;
  return { live, known: totals(nodes), total: nodes.length };
}

const includes = (value, query) => value.toLowerCase().includes(query);
function entityMatch(node, entity, query) {
  return [node.instance_id, entity.id, entity.workspace_id, entity.tab_id].some((value) => includes(value, query));
}

const paneKey = (entity) => JSON.stringify([entity.workspace_id, entity.tab_id, entity.id]);

export function projectAgents(nodes, search = "", status = "all") {
  const query = search.trim().toLowerCase();
  return nodes.flatMap((node) => {
    const panes = new Map(node.herdr.panes.map((pane) => [paneKey(pane), pane]));
    return node.herdr.agents
      .filter((agent) => (status === "all" || agent.agent_status === status) && entityMatch(node, agent, query))
      .map((agent) => ({ node, agent, pane: panes.get(paneKey(agent)) ?? null }));
  });
}

function paneGroups(group) {
  const panes = new Map(group.panes.map((pane) =>
    [paneKey(pane), { id: pane.id, entity: pane, agents: [] }]));
  for (const agent of group.agents) {
    const key = paneKey(agent);
    if (!panes.has(key)) panes.set(key, { id: agent.id, entity: null, agents: [] });
    panes.get(key).agents.push(agent);
  }
  return [...panes.values()].sort((a, b) => order(a.id, b.id));
}

// Keep unresolved references visible rather than inventing an association or dropping inventory.
export function topology(node) {
  const workspaces = new Map();
  const workspace = (id) => {
    if (!workspaces.has(id)) workspaces.set(id, { id, entity: null, tabs: new Map(), agents: [], panes: [] });
    return workspaces.get(id);
  };
  const tab = (ws, id) => {
    if (!ws.tabs.has(id)) ws.tabs.set(id, { id, entity: null, agents: [], panes: [] });
    return ws.tabs.get(id);
  };
  for (const entity of node.herdr.workspaces) workspace(entity.id).entity = entity;
  for (const entity of node.herdr.tabs) tab(workspace(entity.workspace_id), entity.id).entity = entity;
  for (const kind of ["panes", "agents"]) {
    for (const entity of node.herdr[kind]) {
      const ws = workspace(entity.workspace_id);
      (entity.tab_id ? tab(ws, entity.tab_id) : ws)[kind].push(entity);
    }
  }
  return [...workspaces.values()].sort((a, b) => order(a.id, b.id))
    .map((ws) => ({ ...ws, paneGroups: paneGroups(ws),
      tabs: [...ws.tabs.values()].sort((a, b) => order(a.id, b.id))
        .map((tab) => ({ ...tab, paneGroups: paneGroups(tab) })) }));
}

export function projectWorkspaces(nodes, search = "", status = "all") {
  const query = search.trim().toLowerCase();
  return nodes.flatMap((node) => topology(node).filter((ws) => {
    const entities = [...ws.agents, ...ws.panes, ...ws.tabs.flatMap((tab) => [...tab.agents, ...tab.panes])];
    const agents = [...ws.agents, ...ws.tabs.flatMap((tab) => tab.agents)];
    const matchesQuery = [node.instance_id, ws.id, ...ws.tabs.map((tab) => tab.id)].some((id) => includes(id, query)) ||
      entities.some((entity) => entityMatch(node, entity, query));
    return matchesQuery && (status === "all" || agents.some((agent) => agent.agent_status === status));
  }).map((workspace) => ({ node, workspace })));
}

export function projectNodes(nodes, search = "", status = "all") {
  const query = search.trim().toLowerCase();
  return nodes.filter((node) =>
    (includes(node.instance_id, query) || ["workspaces", "tabs", "panes", "agents"]
      .some((kind) => node.herdr[kind].some((entity) => entityMatch(node, entity, query)))) &&
    (status === "all" || node.herdr.agents.some((agent) => agent.agent_status === status)));
}

export function countLabel(count, singular) {
  return `${count} ${singular}${count === 1 ? "" : "s"}`;
}

export function ageLabel(value, now) {
  const time = typeof value === "number" ? value : Date.parse(value);
  if (!Number.isFinite(time)) return "No timestamp";
  const seconds = Math.floor((now - time) / 1000);
  if (seconds < -5) return "Clock ahead";
  if (seconds < 1) return "Just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export async function fetchSnapshot({ fetchImpl = globalThis.fetch, timeoutMs = 10_000,
  setTimer = setTimeout, clearTimer = clearTimeout } = {}) {
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimer(() => { timedOut = true; controller.abort(); }, timeoutMs);
  try {
    const response = await fetchImpl("/api/nodes", {
      headers: { "X-Herdr-Dashboard": "1" }, credentials: "same-origin",
      mode: "same-origin", cache: "no-store", signal: controller.signal,
    });
    let body;
    try { body = await response.json(); }
    catch { throw new DashboardError(timedOut ? "request_timeout" : "invalid_response"); }
    if (!response.ok) throw new DashboardError(record(body?.error) ? body.error.code : "internal_error");
    return parseSnapshot(body);
  } catch (error) {
    if (timedOut) throw new DashboardError("request_timeout");
    if (error instanceof DashboardError) throw error;
    throw new DashboardError("server_unavailable");
  } finally {
    clearTimer(timer);
  }
}

// Schedule from completion, not an interval: slow requests never overlap.
export function createPoller({ load, onStart, onSuccess, onError, intervalMs = 3_000,
  setTimer = setTimeout, clearTimer = clearTimeout }) {
  let automatic = true;
  let stopped = false;
  let timer = null;
  let inFlight = null;
  const cancelTimer = () => { if (timer !== null) clearTimer(timer); timer = null; };
  function refresh() {
    if (stopped) return Promise.resolve();
    cancelTimer();
    if (inFlight) return inFlight;
    onStart();
    inFlight = Promise.resolve().then(load).then(onSuccess, onError).finally(() => {
      inFlight = null;
      if (automatic && !stopped) timer = setTimer(refresh, intervalMs);
    });
    return inFlight;
  }
  return {
    refresh,
    setAutomatic(value) { automatic = value; cancelTimer(); if (automatic) return refresh(); },
    stop() { stopped = true; cancelTimer(); },
  };
}
