export const STALE_AFTER_MS = 30_000;
export const AGENT_STATUSES = Object.freeze(["working", "blocked", "done", "idle", "unknown"]);
const HERDR_STATUSES = ["disabled", "waiting", "ready", "unavailable"];
const SESSION_STATUSES = ["ready", "stopped", "starting", "unavailable", "unsupported", "unknown"];
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
const utf8 = new TextEncoder();

function optionalText(value, key, limit, invalid) {
  if (!Object.hasOwn(value, key)) return "";
  const text = value[key];
  if (typeof text !== "string" || /[\u0000-\u001f\u007f-\u009f\uD800-\uDFFF]/u.test(text) ||
    utf8.encode(text).length > limit) invalid();
  return text;
}

export function agentStatus(value) {
  return AGENT_STATUSES.includes(value) ? value : "unknown";
}

function parseHerdr(h, invalid) {
  if (!record(h) || !HERDR_STATUSES.includes(h.status) || !strings(h, ["version", "sequence", "error_code"]) ||
    !Number.isInteger(h.protocol) || h.protocol < 0 || !timestamp(h.observed_at)) invalid();
  const herdr = { ...h };
  for (const kind of ["workspaces", "tabs", "panes", "agents"]) {
    if (!Array.isArray(h[kind])) invalid();
    herdr[kind] = ordered(h[kind].map((entity) => {
      if (!record(entity) || !strings(entity, ["id", "workspace_id", "tab_id", "agent_status"]) ||
        typeof entity.focused !== "boolean") invalid();
      for (const key of ["project_id", "provider"]) {
        if (entity[key] !== undefined && (typeof entity[key] !== "string" ||
          (entity[key] !== "" && !/^[A-Za-z0-9:_-]{1,128}$/.test(entity[key])))) invalid();
      }
      if (entity.interactive_ready !== undefined && entity.interactive_ready !== null &&
        typeof entity.interactive_ready !== "boolean") invalid();
      return { id: entity.id, workspace_id: entity.workspace_id, tab_id: entity.tab_id,
        display_name: optionalText(entity, "display_name", 256, invalid),
        directory: optionalText(entity, "directory", 4096, invalid),
        focused: entity.focused, agent_status: agentStatus(entity.agent_status),
        project_id: entity.project_id ?? "", provider: entity.provider ?? "",
        interactive_ready: entity.interactive_ready ?? null };
    }));
  }
  return herdr;
}

function waitingHerdr() {
  return { status: "waiting", version: "", sequence: "0", error_code: "", protocol: 0,
    observed_at: null, workspaces: [], tabs: [], panes: [], agents: [] };
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
    const herdr = parseHerdr(node.herdr, invalid);
    if (node.sessions !== undefined && (!Array.isArray(node.sessions) || node.sessions.length > 64)) invalid();
    if (node.sessions_received_at !== undefined && !timestamp(node.sessions_received_at)) invalid();
    if (node.sessions_ready !== undefined && typeof node.sessions_ready !== "boolean") invalid();
    const names = new Set();
    const sessions = (node.sessions ?? []).map((session) => {
      if (!record(session) || !strings(session, ["name", "incarnation", "status", "error_code"]) ||
        !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(session.name) || names.has(session.name) ||
        !SESSION_STATUSES.includes(session.status) ||
        (session.incarnation !== "" && !/^[A-Za-z0-9:_-]{1,128}$/.test(session.incarnation))) invalid();
      if (session.herdr_received_at !== undefined && !timestamp(session.herdr_received_at)) invalid();
      if (session.stale !== undefined && typeof session.stale !== "boolean") invalid();
      names.add(session.name);
      return { name: session.name, incarnation: session.incarnation, status: session.status,
        error_code: session.error_code,
        herdr: session.herdr == null ? waitingHerdr() : parseHerdr(session.herdr, invalid),
        herdr_received_at: session.herdr_received_at !== undefined
          ? session.herdr_received_at : node.sessions_received_at ?? null,
        stale: session.stale ?? node.sessions_ready === false };
    }).sort((a, b) => order(a.name, b.name));
    return { ...node, hostname: optionalText(node, "hostname", 256, invalid),
      sessions_error_code: optionalText(node, "sessions_error_code", 128, invalid), herdr, sessions };
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
  if (!node.session_context && !node.session_name && hasSessionDiscovery(node)) {
    if (node.sessions_error_code) return { live: false, reason: "Not live · Herdr session discovery failed" };
    const states = sessionContexts(node).map((session) => nodeFreshness(session, state, now));
    if (states.length) return states.find((value) => value.live) ?? states[0];
    if (node.sessions_ready === false) return { live: false, reason: "Not live · Herdr session manager unavailable" };
    const received = Date.parse(node.sessions_received_at);
    if (!Number.isFinite(received)) return { live: false, reason: "Not live · waiting for Herdr session discovery" };
    if (now - received >= STALE_AFTER_MS) return { live: false, reason: "Stale · Herdr session discovery over 30s old" };
    if (received - now >= STALE_AFTER_MS) return { live: false, reason: "Not live · clock mismatch" };
    return { live: false, reason: "Not live · no Herdr sessions reported" };
  }
  if (node.session_status && node.session_status !== "ready") return { live: false, reason: `Not live · session ${node.session_status}` };
  if (node.herdr.status !== "ready") return { live: false, reason: `Not live · Herdr ${node.herdr.status}` };
  if (node.stale) return { live: false, reason: "Stale · reported by server" };
  const received = Date.parse(node.herdr_received_at);
  if (!Number.isFinite(received)) return { live: false, reason: "Stale · no snapshot timestamp" };
  if (now - received >= STALE_AFTER_MS) return { live: false, reason: "Stale · snapshot over 30s old" };
  if (received - now >= STALE_AFTER_MS) return { live: false, reason: "Not live · clock mismatch" };
  return { live: true, reason: "Fresh snapshot" };
}

export function hasSessionDiscovery(node) {
  return Boolean(node.sessions_ready || node.sessions_received_at || node.sessions_error_code || node.sessions?.length);
}

// Physical nodes remain unique. These views isolate overlapping entity IDs and
// use session inventory freshness rather than the default socket's readiness.
export function sessionContexts(node) {
  if (node.session_context || node.session_name) return [node];
  const sessions = node.sessions ?? [];
  const contexts = sessions.map((session) => ({ ...node, session_context: true, session_name: session.name, session_label: session.name,
    session_incarnation: session.incarnation, session_status: session.status,
    session_error_code: session.error_code, herdr: session.herdr,
    herdr_received_at: session.herdr_received_at, stale: session.stale }));
  if (node.herdr.status !== "disabled" || !hasSessionDiscovery(node)) {
    contexts.unshift({ ...node, session_context: true, session_name: "", session_label: "Configured default", session_incarnation: "",
      session_status: node.herdr.status === "ready" ? "ready" : "unknown", session_error_code: node.herdr.error_code });
  }
  return contexts;
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
  const contexts = nodes.flatMap(sessionContexts);
  const freshNodes = contexts.filter((node) => nodeFreshness(node, state, now).live);
  const live = totals(freshNodes);
  live.connected = freshness(state, now) === "live" ? nodes.filter((node) => node.connected).length : 0;
  live.fresh = new Set(freshNodes.map((node) => node.instance_id)).size;
  const known = totals(contexts);
  known.connected = nodes.filter((node) => node.connected).length;
  return { live, known, total: nodes.length };
}

const includes = (value, query) => value.toLowerCase().includes(query);
export function entityLabel(entity, id = entity?.id ?? "", fallback = "Not supplied") {
  return { primary: entity?.display_name || id || fallback, secondary: entity?.display_name ? id : "" };
}

export function entityWorkspace(node, entity) {
  return entity?.workspace_id ? node.herdr.workspaces.find((ws) => ws.id === entity.workspace_id) ?? null : null;
}

export function entityTab(node, entity) {
  return entity?.workspace_id && entity.tab_id ? node.herdr.tabs.find((tab) =>
    tab.workspace_id === entity.workspace_id && tab.id === entity.tab_id) ?? null : null;
}

export function entityDirectory(node, entity) {
  if (entity?.directory) return { directory: entity.directory, source: "reported" };
  const directory = entityWorkspace(node, entity)?.directory;
  return directory ? { directory, source: "workspace" } : { directory: "", source: "unknown" };
}

export function entityProject(node, entity) {
  return entity.project_id || node.herdr.workspaces.find((ws) => ws.id === entity.workspace_id)?.project_id || "";
}
function entityMatch(node, entity, query) {
  const parents = [entityWorkspace(node, entity), entityTab(node, entity)];
  return [node.instance_id, node.hostname ?? "", node.session_label ?? node.session_name ?? "Configured default",
    entity.id, entity.workspace_id, entity.tab_id, entityProject(node, entity), entity.provider ?? "",
    ...[entity, ...parents].flatMap((item) => [item?.display_name ?? "", item?.directory ?? ""])]
    .some((value) => includes(value, query));
}

const paneKey = (entity) => JSON.stringify([entity.workspace_id, entity.tab_id, entity.id]);

export function agentDisplayName(agent, pane) {
  return agent.display_name || (pane && paneKey(pane) === paneKey(agent) ? pane.display_name : "") || "";
}

function matchesFilters(node, entity, filters) {
  const project = entityProject(node, entity);
  const readiness = entity.interactive_ready === null || entity.interactive_ready === undefined ? "unknown" :
    entity.interactive_ready ? "ready" : "not-ready";
  return (!filters.session?.trim() || node.session_name === filters.session.trim()) &&
    (!filters.project?.trim() || project === filters.project.trim()) &&
    (!filters.provider?.trim() || (entity.provider || "unknown").toLowerCase() === filters.provider.trim().toLowerCase()) &&
    (!filters.readiness || filters.readiness === "all" || readiness === filters.readiness);
}

export function projectAgents(nodes, search = "", status = "all", filters = {}) {
  const query = search.trim().toLowerCase();
  return nodes.flatMap(sessionContexts).flatMap((node) => {
    const panes = new Map(node.herdr.panes.map((pane) => [paneKey(pane), pane]));
    return node.herdr.agents
      .map((agent) => ({ node, agent, pane: panes.get(paneKey(agent)) ?? null }))
      .filter(({ agent, pane }) => (status === "all" || agent.agent_status === status) &&
        (entityMatch(node, agent, query) || includes(agentDisplayName(agent, pane), query)) &&
        matchesFilters(node, agent, filters));
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

export function workspaceDirectories(workspace) {
  const entities = [...workspace.panes, ...workspace.agents,
    ...workspace.tabs.flatMap((tab) => [...tab.panes, ...tab.agents])];
  return [...new Set(entities.map((entity) => entity.directory).filter(Boolean))].sort(order);
}

export function projectWorkspaces(nodes, search = "", status = "all", filters = {}) {
  const query = search.trim().toLowerCase();
  return nodes.flatMap(sessionContexts).flatMap((node) => topology(node).filter((ws) => {
    const entities = [...ws.agents, ...ws.panes, ...ws.tabs.flatMap((tab) => [...tab.agents, ...tab.panes])];
    const agents = [...ws.agents, ...ws.tabs.flatMap((tab) => tab.agents)];
    const matchesQuery = [node.instance_id, node.hostname ?? "", node.session_label ?? node.session_name ?? "", ws.id, ws.entity?.project_id ?? "",
      ws.entity?.display_name ?? "", ws.entity?.directory ?? "",
      ...ws.tabs.flatMap((tab) => [tab.id, tab.entity?.display_name ?? "", tab.entity?.directory ?? ""])]
      .some((value) => includes(value, query)) ||
      entities.some((entity) => entityMatch(node, entity, query));
    const agentFilter = status !== "all" || filters.provider?.trim() || (filters.readiness && filters.readiness !== "all");
    return matchesQuery && (agentFilter ?
      agents.some((agent) => (status === "all" || agent.agent_status === status) && matchesFilters(node, agent, filters)) :
      matchesFilters(node, { project_id: ws.entity?.project_id, workspace_id: ws.id }, filters));
  }).map((workspace) => ({ node, workspace })));
}

export function projectNodes(nodes, search = "", status = "all", filters = {}) {
  const query = search.trim().toLowerCase();
  return nodes.filter((node) => {
    const contexts = sessionContexts(node);
    return (contexts.length ? contexts : [node]).some((session) => {
      if (filters.session?.trim() && session.session_name !== filters.session.trim()) return false;
      const queryMatch = includes(node.instance_id, query) || includes(node.hostname ?? "", query) ||
        includes(session.session_label ?? session.session_name ?? "", query) ||
        ["workspaces", "tabs", "panes", "agents"].some((kind) => session.herdr[kind].some((entity) => entityMatch(session, entity, query)));
      if (!queryMatch) return false;
      if (status !== "all" || filters.provider?.trim() || (filters.readiness && filters.readiness !== "all")) {
        return session.herdr.agents.some((agent) => (status === "all" || agent.agent_status === status) && matchesFilters(session, agent, filters));
      }
      return !filters.project?.trim() || session.herdr.workspaces.some((ws) =>
        matchesFilters(session, { ...ws, workspace_id: ws.id }, filters));
    });
  });
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
