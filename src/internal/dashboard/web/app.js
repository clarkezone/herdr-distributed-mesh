import {
  initialState, acceptSnapshot, rejectSnapshot, freshness, nodeFreshness, summary,
  projectAgents, projectWorkspaces, projectNodes, sessionContexts, entityProject, entityLabel, entityWorkspace,
  entityTab, entityDirectory, agentDisplayName, workspaceDirectories, countLabel, ageLabel, fetchSnapshot, createPoller,
} from "/model.mjs";

const $ = (id) => document.getElementById(id);
let state = initialState();
let currentView = "agents";
const filters = { search: "", status: "all", session: "", project: "", provider: "", readiness: "all" };
const badgeClasses = new Set(["working", "blocked", "done", "idle", "unknown",
  "ready", "waiting", "disabled", "unavailable", "stopped", "starting", "unsupported", "connected", "disconnected", "fresh", "stale"]);

function el(tag, className = "", text) {
  const result = document.createElement(tag);
  if (className) result.className = className;
  if (text !== undefined) result.textContent = text;
  return result;
}
function text(id, value) {
  if ($(id).textContent !== value) $(id).textContent = value;
}
function keyed(element, key) {
  element.dataset.key = JSON.stringify(key);
  return element;
}
function identifier(value, fallback = "Not supplied") {
  return el("span", "identifier", value || fallback);
}
function identity(entity, id = entity?.id ?? "", fallback = "Not supplied") {
  const label = entityLabel(entity, id, fallback);
  const result = el("span", "entity-identity");
  const primary = el("bdi", entity?.display_name ? "entity-name" : "identifier", label.primary);
  result.append(primary);
  if (label.secondary) result.append(el("bdi", "identifier secondary-id", `ID: ${label.secondary}`));
  return result;
}
function nodeIdentity(node) {
  return identity({ display_name: node.hostname }, node.instance_id);
}
function agentIdentity(agent, pane) {
  return identity({ ...agent, display_name: agentDisplayName(agent, pane) });
}
function directory(node, entity) {
  const value = entityDirectory(node, entity);
  const result = el("span", "entity-directory");
  result.dataset.directorySource = value.source;
  const label = value.source === "workspace" ? "Workspace directory (fallback)" : "Reported directory";
  result.append(el("span", "muted", value.source === "unknown" ? "Directory not reported" : label));
  if (value.directory) result.append(el("bdi", "directory-path", value.directory));
  return result;
}
function workspaceDirectorySummary(node, workspace) {
  const result = el("div", "workspace-directories");
  result.append(directory(node, workspace.entity));
  const reported = workspace.entity?.directory ? [] : workspaceDirectories(workspace);
  if (reported.length) {
    result.append(el("h4", "", "Reported directories"),
      el("p", "muted", "Reported by contained panes and agents; not a workspace directory."));
    const list = el("ul", "directory-list");
    for (const path of reported) {
      const item = keyed(el("li"), ["directory", path]);
      item.append(el("bdi", "directory-path", path));
      list.append(item);
    }
    result.append(list);
  }
  return result;
}
function entityReference(entity, id, kind) {
  const result = el("span", "entity-identity");
  result.append(identity(entity, id, `${kind} not supplied`));
  if (!entity && id) result.append(badge(`Unresolved ${kind.toLowerCase()} · missing from snapshot`, "stale"));
  return result;
}
function badge(label, status) {
  return el("span", `badge status-${badgeClasses.has(status) ? status : "unknown"}`, label);
}
function age(value, now) {
  const result = el("span", "muted", ageLabel(value, now));
  if (value !== null && value !== undefined) result.title = typeof value === "number" ? new Date(value).toISOString() : value;
  return result;
}
function focusBadge(entity) {
  return entity.focused ? badge("Focused", "unknown") : el("span", "muted", "Not focused");
}

// Patch in place so polling preserves keyboard focus, expanded groups, and scroll.
function patch(oldNode, newNode) {
  if (oldNode.nodeType !== newNode.nodeType || oldNode.nodeName !== newNode.nodeName) {
    oldNode.replaceWith(newNode);
    return;
  }
  if (oldNode.nodeType === Node.TEXT_NODE) {
    if (oldNode.nodeValue !== newNode.nodeValue) oldNode.nodeValue = newNode.nodeValue;
    return;
  }
  for (const attribute of [...oldNode.attributes]) {
    if (oldNode.tagName === "DETAILS" && attribute.name === "open") continue;
    if (!newNode.hasAttribute(attribute.name)) oldNode.removeAttribute(attribute.name);
  }
  for (const attribute of [...newNode.attributes]) {
    if (oldNode.tagName === "DETAILS" && attribute.name === "open") continue;
    if (oldNode.getAttribute(attribute.name) !== attribute.value) oldNode.setAttribute(attribute.name, attribute.value);
  }
  reconcile(oldNode, [...newNode.childNodes]);
}
function reconcile(parent, desired) {
  let cursor = parent.firstChild;
  const keyedChildren = new Map([...parent.children].filter((child) => child.dataset.key)
    .map((child) => [child.dataset.key, child]));
  for (const next of desired) {
    const key = next.nodeType === Node.ELEMENT_NODE ? next.dataset.key : undefined;
    let existing = key ? keyedChildren.get(key) : cursor;
    if (!key && existing?.nodeType === Node.ELEMENT_NODE && existing.dataset.key) existing = null;
    if (existing) {
      if (existing !== cursor) parent.insertBefore(existing, cursor);
      const following = existing.nextSibling;
      patch(existing, next);
      cursor = following;
    } else {
      parent.insertBefore(next, cursor);
    }
  }
  while (cursor) {
    const next = cursor.nextSibling;
    cursor.remove();
    cursor = next;
  }
}

function freshnessBadge(node, now) {
  const fresh = nodeFreshness(node, state, now);
  return badge(fresh.reason, fresh.live ? "fresh" : "stale");
}
function freshnessCell(node, now) {
  const result = el("div", "freshness-cell");
  result.append(freshnessBadge(node, now), age(node.herdr_received_at, now));
  return result;
}

function renderSummary(now) {
  const counts = summary(state, now);
  const live = freshness(state, now) === "live";
  const available = state.snapshot !== null;
  text("summary-scope", live ? "Unfiltered fleet · inventory counts include fresh nodes only" :
    available ? "Last known data · live counts unavailable" : "Live counts unavailable");
  $("summary").dataset.stale = String(!live);
  const specs = [
    ["connected", "Connected nodes", `${counts.total} registered`, `${counts.known.connected} last known connected`],
    ["fresh", "Fresh Herdr nodes", "Ready · received within 30s", "Freshness cannot be confirmed"],
    ["workspaces", "Workspaces", "On fresh nodes", `${counts.known.workspaces} last known`],
    ["working", "Agents working", "In progress · fresh nodes", `${counts.known.working} last known`],
    ["blocked", "Agents blocked", "Needs attention · fresh nodes", `${counts.known.blocked} last known`],
    ["done", "Agents done", "Observed state · not task success", `${counts.known.done} last known`],
  ];
  reconcile($("summary"), specs.map(([key, label, note, retained]) => {
    const card = keyed(el("div", `metric metric-${key}`), [key]);
    card.dataset.metric = key;
    card.append(el("p", "metric-label", label), el("span", "metric-value", live ? String(counts.live[key]) : "—"),
      el("p", "metric-note", live ? note : available ? retained : "Awaiting first snapshot"));
    return card;
  }));
}

function renderConnection(now) {
  const phase = freshness(state, now);
  $("connection-banner").dataset.state = phase;
  const automatic = $("auto-refresh").checked;
  const retry = automatic ? "Automatic refresh will retry." : "Auto-refresh is paused. Use Refresh now to retry.";
  if (phase === "loading") {
    text("connection-status", "Connecting to the mesh…");
    text("connection-detail", "Fetching the first snapshot. No live data yet.");
  } else if (phase === "error") {
    text("connection-status", "Unable to load fleet · no live data");
    text("connection-detail", `${state.error.message} ${retry}`);
  } else if (phase === "stale") {
    text("connection-status", "Not live · showing last known data");
    text("connection-detail", state.error ? `${state.error.message} ${retry}` :
      `The last successful refresh is over 30 seconds old, or the local clock changed. ${retry}`);
  } else {
    text("connection-status", "Live · mesh reachable");
    const stale = state.snapshot.nodes.filter((node) => !nodeFreshness(node, state, now).live).length;
    text("connection-detail", `${stale ? `${stale} node${stale === 1 ? "" : "s"} without fresh Herdr data. ` : "All available Herdr snapshots are fresh. "}${automatic ? "Polling every 3 seconds after each response." : "Auto-refresh paused; data expires after 30 seconds."}`);
  }
  text("last-updated", state.fetchedAt === null ? "No successful refresh yet" : `Last success · ${ageLabel(state.fetchedAt, now)}`);
  text("refresh-button", state.refreshing ? "Refreshing…" : "Refresh now");
  $("refresh-button").setAttribute("aria-disabled", String(state.refreshing));
}

const tableLabels = ["Agent / status", "Workspace", "Tab", "Directory", "Node", "Session", "Project", "Provider", "Readiness", "Pane", "Focus", "Data freshness"];
function agentRow({ node, agent, pane }, now) {
  const row = keyed(el("tr"), [node.instance_id, node.session_name, node.session_incarnation, agent.workspace_id, agent.tab_id, agent.id]);
  row.dataset.stale = String(!nodeFreshness(node, state, now).live);
  row.dataset.agentStatus = agent.agent_status;
  const agentCell = el("div", "agent-cell");
  agentCell.append(agentIdentity(agent, pane), badge(agent.agent_status, agent.agent_status));
  const paneReference = el("div", "freshness-cell");
  paneReference.dataset.paneState = pane ? "resolved" : "missing";
  paneReference.append(entityReference(pane, agent.id, "Pane"));
  if (!pane && !agent.id) paneReference.append(badge("Unresolved pane · missing from snapshot", "stale"));
  const project = entityProject(node, agent);
  const readiness = agent.interactive_ready === null ? "Not reported" : agent.interactive_ready ? "Ready" : "Not ready";
  const session = identifier(node.session_label ?? node.session_name);
  session.title = node.session_incarnation ? `Incarnation: ${node.session_incarnation}` : "Legacy default socket; incarnation not reported";
  const values = [agentCell,
    entityReference(entityWorkspace(node, agent), agent.workspace_id, "Workspace"),
    entityReference(entityTab(node, agent), agent.tab_id, "Tab"),
    directory(node, agent), nodeIdentity(node), session, identifier(project, "Unmapped"),
    identifier(agent.provider, "Unknown"), badge(readiness, agent.interactive_ready ? "ready" : "unknown"),
    paneReference, focusBadge(agent), freshnessCell(node, now)];
  values.forEach((value, i) => {
    const cell = el("td");
    cell.dataset.label = tableLabels[i];
    cell.append(value);
    row.append(cell);
  });
  return row;
}

function entityRow(node, entity, kind, pane = null) {
  const row = keyed(el("li", "tree-row"), [kind, entity.workspace_id, entity.tab_id, entity.id]);
  row.append(el("span", "tree-label", kind), kind === "Agent" ? agentIdentity(entity, pane) : identity(entity), directory(node, entity));
  if (kind === "Agent") row.append(badge(entity.agent_status, entity.agent_status));
  if (entity.focused) row.append(focusBadge(entity));
  return row;
}
function treeChildren(node, group, workspaceId) {
  return group.paneGroups.map((pane) => {
    const item = keyed(el("li"), ["pane", pane.id]);
    item.dataset.paneState = pane.entity ? "resolved" : "missing";
    const heading = el("div", "tree-row");
    heading.append(el("span", "tree-label", pane.entity ? "Pane" : "Unresolved pane reference"),
      identity(pane.entity, pane.id, "Empty pane reference"),
      directory(node, pane.entity ?? { workspace_id: workspaceId }));
    if (!pane.entity) heading.append(badge("Missing from snapshot", "stale"));
    if (pane.entity?.focused) heading.append(focusBadge(pane.entity));
    item.append(heading);
    if (pane.agents.length) {
      const agents = el("ul", "tree");
      agents.append(...pane.agents.map((agent) => entityRow(node, agent, "Agent", pane.entity)));
      item.append(agents);
    }
    return item;
  });
}
function workspaceCard({ node, workspace: ws }, now) {
  const card = keyed(el("article", "workspace-card"), [node.instance_id, node.session_name, node.session_incarnation, ws.id]);
  card.dataset.stale = String(!nodeFreshness(node, state, now).live);
  const header = el("div", "card-heading");
  const heading = el("h3");
  heading.append(el("span", "eyebrow", ws.entity ? "Workspace" : "Unresolved workspace reference"),
    identity(ws.entity, ws.id, "Workspace not supplied"));
  const badges = el("div", "badges");
  if (ws.entity?.focused) badges.append(focusBadge(ws.entity));
  badges.append(freshnessBadge(node, now));
  header.append(heading, badges);
  const origin = el("div", "tree-row");
  origin.append(el("span", "tree-label", "Node"), nodeIdentity(node),
    el("span", "tree-label", "Session"), identifier(node.session_label ?? node.session_name),
    el("span", "tree-label", "Project"), identifier(ws.entity?.project_id, "Unmapped"), age(node.herdr_received_at, now));
  const tree = el("ul", "tree");
  tree.append(...ws.tabs.map((tab) => {
    const item = keyed(el("li"), ["tab", tab.id]);
    const details = el("details");
    details.open = true;
    const title = el("summary");
    title.append(el("span", "tree-label", tab.entity ? "Tab" : "Unresolved tab reference"),
      identity(tab.entity, tab.id), directory(node, tab.entity ?? { workspace_id: ws.id }));
    if (tab.entity?.focused) title.append(document.createTextNode(" · Focused"));
    const children = el("ul", "tree");
    children.append(...treeChildren(node, tab, ws.id));
    if (!children.children.length) children.append(el("li", "tree-empty", "No panes or agents reported in this tab."));
    details.append(title, children);
    item.append(details);
    return item;
  }));
  if (ws.panes.length || ws.agents.length) {
    const unassigned = keyed(el("li"), ["unassigned"]);
    unassigned.append(el("span", "tree-label", "Tab not supplied"));
    const children = el("ul", "tree");
    children.append(...treeChildren(node, ws, ws.id));
    unassigned.append(children);
    tree.append(unassigned);
  }
  if (!tree.children.length) tree.append(el("li", "tree-empty", "No tabs, panes, or agents reported in this workspace."));
  card.append(header, workspaceDirectorySummary(node, ws), origin, tree);
  return card;
}

function nodeCard(node, now) {
  const card = keyed(el("article", "node-card"), [node.instance_id]);
  card.dataset.stale = String(!nodeFreshness(node, state, now).live);
  const header = el("div", "card-heading");
  const heading = el("h3");
  heading.append(el("span", "eyebrow", "Node"), nodeIdentity(node));
  const badges = el("div", "badges");
  const globallyLive = freshness(state, now) === "live";
  badges.append(badge(`${globallyLive ? "" : "Last known: "}${node.connected ? "connected" : "disconnected"}`,
    globallyLive ? node.connected ? "connected" : "disconnected" : "stale"));
  badges.append(badge(`Default Herdr ${node.herdr.status}`, globallyLive ? node.herdr.status : "stale"));
  header.append(heading, badges);
  const metadata = el("dl", "node-metadata");
  const fields = [
    ["Stable ID", identifier(node.tailscale_stable_id)],
    ["Last seen", age(node.last_seen, now)],
    ["Snapshot received", age(node.herdr_received_at, now)],
    ["Herdr observed", age(node.herdr.observed_at, now)],
    ["Version / protocol", identifier(`${node.herdr.version || "Not supplied"} / ${node.herdr.protocol}`)],
    ["Sequence", identifier(node.herdr.sequence)],
    ["Herdr error code", identifier(node.herdr.error_code, "None reported")],
  ];
  for (const [label, value] of fields) {
    const detail = el("dd");
    detail.append(value);
    metadata.append(el("dt", "", label), detail);
  }
  const contexts = sessionContexts(node);
  const counts = { workspaces: 0, tabs: 0, panes: 0, agents: 0 };
  const sessions = el("ul", "tree");
  for (const session of contexts) {
    for (const kind of Object.keys(counts)) counts[kind] += session.herdr[kind].length;
    const row = keyed(el("li", "tree-row"), [session.session_name, session.session_incarnation]);
    row.append(el("span", "tree-label", "Session"), identifier(session.session_label ?? session.session_name),
      badge(session.session_status, session.session_status), freshnessBadge(session, now));
    if (session.session_error_code) row.append(identifier(session.session_error_code));
    sessions.append(row);
  }
  card.append(header, freshnessBadge(node, now), metadata,
    sessions, el("p", "node-counts", `Inventory including retained sessions · ${countLabel(counts.workspaces, "workspace")} · ${countLabel(counts.tabs, "tab")} · ${countLabel(counts.panes, "pane")} · ${countLabel(counts.agents, "agent")}`));
  return card;
}

function renderInventory(now) {
  const nodes = state.snapshot?.nodes ?? [];
  const projections = {
    agents: () => projectAgents(nodes, filters.search, filters.status, filters),
    workspaces: () => projectWorkspaces(nodes, filters.search, filters.status, filters),
    nodes: () => projectNodes(nodes, filters.search, filters.status, filters),
  };
  const rows = projections[currentView]();
  const phase = freshness(state, now);
  const descriptions = {
    agents: "Configured names lead; unnamed agents use their resolved pane label. IDs remain visible and are matched only within the same node, session incarnation, workspace, and tab. Workspace directory fallbacks are not reported agent working directories. Project/provider/readiness are never inferred from names.",
    workspaces: "Workspace → tab → pane → agent names and IDs. Without a workspace directory, cards list distinct directories reported by contained panes and agents, not a guessed workspace path. Missing references stay visible. Filters select whole workspaces with sibling context. Expand or collapse tabs with the keyboard.",
    nodes: "Managed node names lead; instance IDs remain visible. Connectivity and Herdr readiness are separate. Search includes names, directories, identifiers, and descendant inventory.",
  };
  text("view-description", descriptions[currentView]);
  text("result-count", state.snapshot ? `${countLabel(rows.length, currentView.slice(0, -1))} shown${phase === "stale" ? " · not live" : ""}` : "No snapshot yet");
  for (const view of ["agents", "workspaces", "nodes"]) {
    $(`view-${view}`).setAttribute("aria-pressed", String(view === currentView));
    $(`${view}-view`).hidden = view !== currentView || rows.length === 0;
  }
  $("empty-state").hidden = rows.length > 0;
  if (!rows.length) {
    let title;
    let detail;
    if (!state.snapshot) {
      title = phase === "error" ? "Fleet data unavailable" : "Loading your fleet";
      detail = phase === "error" ? `${state.error.message} No successful snapshot has been received.` : "The first snapshot will appear here. This is not an empty-fleet result.";
    } else if (!nodes.length) {
      title = phase === "stale" ? "Last snapshot had no nodes · not live" : "No nodes registered";
      detail = phase === "stale" ? "The current fleet is unknown. Refresh to retrieve an up-to-date snapshot." : "The mesh server is reachable, but its fleet is empty. Registered nodes will appear automatically.";
    } else if (filters.search.trim() || filters.status !== "all" || filters.session.trim() ||
      filters.project.trim() || filters.provider.trim() || filters.readiness !== "all") {
      title = "No matching inventory";
      detail = "Try another name, directory, or identifier, or clear the filters. Fleet pulse above always describes the unfiltered fleet.";
    } else {
      title = `No ${currentView} reported`;
      detail = "Inspect Nodes for connectivity and Herdr readiness. Inventory appears when a node reports it.";
    }
    text("empty-title", title);
    text("empty-description", detail);
  }
  if (currentView === "agents") reconcile($("agents-body"), rows.map((row) => agentRow(row, now)));
  if (currentView === "workspaces") reconcile($("workspace-list"), rows.map((row) => workspaceCard(row, now)));
  if (currentView === "nodes") reconcile($("node-list"), rows.map((row) => nodeCard(row, now)));
}

function render() {
  const now = Date.now();
  renderConnection(now);
  renderSummary(now);
  renderInventory(now);
}

const poller = createPoller({
  load: fetchSnapshot,
  onStart() { state = { ...state, refreshing: true }; render(); },
  onSuccess(snapshot) { state = acceptSnapshot(state, snapshot, Date.now()); render(); },
  onError(error) { state = rejectSnapshot(state, error); render(); },
});
$("refresh-button").addEventListener("click", () => { void poller.refresh(); });
$("auto-refresh").addEventListener("change", (event) => {
  void poller.setAutomatic(event.target.checked);
  render();
});
$("search-input").addEventListener("input", (event) => { filters.search = event.target.value; render(); });
$("status-filter").addEventListener("change", (event) => { filters.status = event.target.value; render(); });
for (const key of ["session", "project", "provider"]) {
  $(`${key}-filter`).addEventListener("input", (event) => { filters[key] = event.target.value; render(); });
}
$("readiness-filter").addEventListener("change", (event) => { filters.readiness = event.target.value; render(); });
$("clear-filters").addEventListener("click", () => {
  filters.search = "";
  filters.status = "all";
  $("search-input").value = "";
  $("status-filter").value = "all";
  for (const key of ["session", "project", "provider"]) {
    filters[key] = "";
    $(`${key}-filter`).value = "";
  }
  filters.readiness = "all";
  $("readiness-filter").value = "all";
  render();
  $("search-input").focus();
});
for (const button of document.querySelectorAll("[data-view]")) {
  button.addEventListener("click", () => { currentView = button.dataset.view; render(); });
}
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") { render(); void poller.refresh(); }
});
window.addEventListener("pageshow", (event) => {
  if (event.persisted) { render(); void poller.refresh(); }
});
window.addEventListener("online", () => { void poller.refresh(); });
setInterval(render, 1_000);
render();
void poller.refresh();
