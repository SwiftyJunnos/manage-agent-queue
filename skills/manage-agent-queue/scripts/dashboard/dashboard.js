"use strict";

const base = location.pathname.endsWith("/")
  ? location.pathname
  : `${location.pathname}/`;
const endpoints = {
  revision: "api/revision",
  snapshot: "api/snapshot",
  events: "api/events?after=",
  health: "api/health",
};
const state = {
  revision: null,
  eventSequence: 0,
  taskFingerprints: new Map(),
  stopped: false,
  polling: false,
  interval: 2,
  retryDelay: 2,
  timer: null,
  lastSuccess: null,
};
const byId = (id) => document.getElementById(id);
const element = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = String(text);
  return node;
};
const api = async (path) => {
  const response = await fetch(`${base}${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
};
const setConnection = (label, className) => {
  const node = byId("connection");
  node.textContent = label;
  node.className = `connection ${className}`;
};
const fingerprint = (task) => JSON.stringify([
  task.state,
  task.assignee,
  task.lease_until,
  task.attempts,
  task.blocked_by,
]);
const stateLabel = (value) => ({
  completed: "✓ Completed",
  leased: "→ Active",
  ready: "○ Ready",
  failed: "! Failed",
  blocked: "! Blocked",
  dependency_failed: "! Dependency failed",
  waiting_dependency: "· Waiting",
  waiting_retry: "· Retrying",
  resource_conflict: "· Resource conflict",
  cancelled: "– Cancelled",
}[value] || value);
const remainingTime = (leaseUntil) => {
  if (!leaseUntil) return "";
  const seconds = Math.max(
    0,
    Math.floor((Date.parse(leaseUntil) - Date.now()) / 1000),
  );
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${seconds % 60}s left`;
};
const updatedTime = () => {
  if (state.lastSuccess === null) return "No successful refresh yet";
  const seconds = Math.max(
    0,
    Math.floor((Date.now() - state.lastSuccess) / 1000),
  );
  return seconds < 2 ? "Updated just now" : `Updated ${seconds}s ago`;
};
const tableCell = (className, label) => {
  const cell = element("td", className);
  cell.dataset.label = label;
  return cell;
};
const metadataItem = (label, value) => {
  const item = element("span", "meta-item");
  item.append(
    element("span", "meta-label", label),
    element("strong", "", value || "none"),
  );
  return item;
};
const updateStatusTime = () => {
  byId("last-updated").textContent = updatedTime();
  setTimeout(updateStatusTime, 1000);
};
const updateTimes = () => {
  for (const node of document.querySelectorAll("[data-lease-until]")) {
    node.textContent = remainingTime(node.dataset.leaseUntil) || "—";
  }
};

function renderTaskRow(task) {
  const row = element("tr", "task-row");
  row.setAttribute("data-task-id", task.id);
  if (task.state === "leased") {
    row.classList.add("active-row");
  }
  const previous = state.taskFingerprints.get(task.id);
  const next = fingerprint(task);
  if (previous !== undefined && previous !== next) {
    row.classList.add("changed");
  }
  state.taskFingerprints.set(task.id, next);

  const status = tableCell(`task-status state-${task.state}`, "Status");
  status.textContent = stateLabel(task.state);

  const taskCell = tableCell("task-cell", "Task");
  const primary = element("div", "task-primary");
  primary.append(
    element("span", "task-id", task.id),
    element("span", "task-title", task.title),
  );
  const metadata = element("div", "task-meta");
  metadata.append(
    metadataItem("Attempts", task.attempts),
    metadataItem("Depends on", task.depends_on),
    metadataItem("Resources", task.resources),
  );
  taskCell.append(primary, metadata);

  const assignee = tableCell("task-assignee", "Assignee");
  assignee.textContent = task.assignee || "Unassigned";

  const timing = tableCell("task-time", "Timing");
  timing.textContent = remainingTime(task.lease_until) || "—";
  timing.dataset.leaseUntil = task.lease_until || "";

  row.append(status, taskCell, assignee, timing);
  return row;
}

function renderWorkflow(workflow) {
  const section = element("section", "workflow-group");
  const heading = element("div", "workflow-heading");
  heading.append(
    element("h2", "workflow-name", workflow.id),
    element(
      "span",
      "workflow-progress",
      `${workflow.completed}/${workflow.total} completed · ` +
        `${workflow.active} active · ${workflow.attention} attention`,
    ),
  );

  const table = element("table", "queue-table");
  const tableHead = element("thead");
  const headingRow = element("tr");
  for (const label of ["Status", "Task", "Assignee", "Timing"]) {
    const heading = element("th", "", label);
    heading.scope = "col";
    headingRow.append(heading);
  }
  tableHead.append(headingRow);

  const tableBody = element("tbody");
  tableBody.append(...workflow.tasks.map(renderTaskRow));
  table.append(tableHead, tableBody);
  section.append(heading, table);
  return section;
}

function renderSnapshot(snapshot) {
  byId("queue-title").textContent = `${snapshot.queue_id} · rev ${snapshot.revision}`;
  const summary = byId("summary");
  summary.replaceChildren();
  summary.className = "summary-line";
  const waiting = Math.max(
    0,
    snapshot.counts.total - snapshot.counts.completed - snapshot.counts.active,
  );
  const summaryText = element("div", "summary-text");
  summaryText.append(
    element(
      "strong",
      "summary-progress",
      `${snapshot.counts.completed} of ${snapshot.counts.total} completed`,
    ),
    element(
      "span",
      "summary-counts",
      `${snapshot.counts.active} active · ${waiting} waiting · ` +
        `${snapshot.counts.attention} attention`,
    ),
  );
  const progress = element("progress", "queue-progress");
  progress.max = Math.max(1, snapshot.counts.total);
  progress.value = snapshot.counts.completed;
  progress.setAttribute("aria-label", "Queue completion");
  summary.append(summaryText, progress);

  const warnings = byId("warnings");
  warnings.replaceChildren();
  for (const warning of snapshot.warnings) {
    warnings.append(element(
      "article",
      "warning",
      `${warning.kind} · ${warning.task_id} · ${warning.title}`,
    ));
  }

  const workflows = byId("workflow-view");
  workflows.replaceChildren();
  if (snapshot.workflows.length === 0) {
    workflows.append(byId("empty-template").content.cloneNode(true));
  }
  for (const workflow of snapshot.workflows) {
    workflows.append(renderWorkflow(workflow));
  }
}

async function refreshEvents() {
  const payload = await api(`${endpoints.events}${state.eventSequence}`);
  const list = byId("activity-list");
  for (const event of payload.events) {
    list.append(element(
      "li",
      "activity-item",
      `${event.at || ""} · ${event.type} · ${event.task_id || "queue"}`,
    ));
    state.eventSequence = Math.max(state.eventSequence, event.seq);
  }
}

const schedule = (seconds) => {
  clearTimeout(state.timer);
  state.timer = setTimeout(poll, seconds * 1000);
};

async function poll() {
  if (state.stopped || state.polling) return;
  state.polling = true;
  let nextDelay = state.interval;
  try {
    const current = await api(endpoints.revision);
    state.interval = current.interval;
    if (current.revision !== state.revision) {
      const snapshot = await api(endpoints.snapshot);
      renderSnapshot(snapshot);
      await refreshEvents();
      state.revision = snapshot.revision;
    }
    updateTimes();
    state.lastSuccess = Date.now();
    state.retryDelay = state.interval;
    nextDelay = state.interval;
    setConnection("Live", "live");
  } catch (_error) {
    setConnection(
      "Retrying · queue temporarily unavailable",
      "retrying",
    );
    try {
      await api(endpoints.health);
      nextDelay = state.retryDelay;
      state.retryDelay = Math.min(state.retryDelay * 2, 15);
    } catch (_healthError) {
      state.stopped = true;
      setConnection("Stopped · dashboard server ended", "stopped");
    }
  } finally {
    state.polling = false;
    if (!state.stopped) schedule(nextDelay);
  }
}

function selectView(name) {
  const workflows = name === "workflow";
  byId("workflow-view").hidden = !workflows;
  byId("activity-view").hidden = workflows;
  byId("workflow-tab").setAttribute("aria-selected", String(workflows));
  byId("activity-tab").setAttribute("aria-selected", String(!workflows));
}

byId("workflow-tab").addEventListener("click", () => selectView("workflow"));
byId("activity-tab").addEventListener("click", () => selectView("activity"));
byId("manual-refresh").addEventListener("click", () => {
  clearTimeout(state.timer);
  state.stopped = false;
  state.retryDelay = state.interval;
  poll();
});
updateStatusTime();
poll();
