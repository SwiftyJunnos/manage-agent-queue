# Readable Queue Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan sequentially in the main thread. Mark each checkbox as it is completed.

**Goal:** Replace the dense dark workflow-card dashboard with the approved light, readable, two-line queue table while preserving all existing live observation behavior and security boundaries.

**Architecture:** Keep the current Python projection and HTTP server unchanged. Rebuild only the static browser renderer: `dashboard.js` will create one semantic table per workflow, `dashboard.css` will provide the light desktop and compact mobile layouts, and `index.html` will retain the existing controls and views with clearer queue wording. Static asset contract tests will drive the change and existing lifecycle, polling, event, redaction, and server tests will guard behavior outside the view.

**Tech Stack:** Build-free HTML, CSS, and browser JavaScript; Python standard-library `unittest` contract tests.

---

## Constraints

- Do not change the snapshot or events API schema.
- Do not add dependencies, a bundler, a framework, or external assets.
- Keep all queue-controlled content on safe DOM APIs such as `textContent`.
- Preserve revision polling, incremental Activity events, manual refresh, last-updated time, retry backoff, stopped-server detection, changed-task indication, and the empty state.
- Keep the server loopback-only, tokenized, read-only, and governed by its existing browser-consent flow.
- Render the approved light table, not a card grid, timeline, kanban board, or decorative dashboard.
- Implement and verify every task sequentially in the current worktree.

## Task 1: Lock the table renderer contract with failing tests

**Files:**

- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:5136-5215`

- [ ] **Step 1: Replace the expandable-row preservation test with the semantic table contract**

Replace `test_client_preserves_expanded_tasks_across_snapshots` with:

```python
    def test_client_renders_semantic_two_line_queue_tables(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'element("table", "queue-table")',
            'element("thead")',
            'element("tbody")',
            'heading.scope = "col"',
            '["Status", "Task", "Assignee", "Timing"]',
            'element("div", "task-primary")',
            'element("div", "task-meta")',
            'row.classList.add("active-row")',
        ):
            self.assertIn(required, javascript)
        for forbidden in (
            'element("details"',
            "openTasks",
            'element("article", "card"',
            'element("header", "workflow-header"',
        ):
            self.assertNotIn(forbidden, javascript)
```

This intentionally removes the old expanded-row contract: the approved table keeps all task metadata visible without disclosure controls.

- [ ] **Step 2: Add a compact-summary renderer contract**

Add beside the semantic table test:

```python
    def test_client_renders_one_compact_queue_summary(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'summary.className = "summary-line"',
            'element("strong", "summary-progress"',
            'element("span", "summary-counts"',
            'element("progress", "queue-progress")',
            'const waiting = Math.max(',
        ):
            self.assertIn(required, javascript)
        for forbidden in (
            'summary.className = "summary-grid"',
            'element("article", "card"',
        ):
            self.assertNotIn(forbidden, javascript)
```

The client derives the display-only waiting count as `max(0, total - completed - active)`. It does not add a field to the server projection.

- [ ] **Step 3: Tighten the static asset and responsive contracts**

In `test_assets_are_build_free_local_and_accessible`, replace the old 720-pixel assertion and add the light/table selectors:

```python
        self.assertIn('>Queue</button>', html)
        self.assertIn("color-scheme: light", css)
        self.assertIn(".queue-table", css)
        self.assertIn(".task-primary", css)
        self.assertIn(".task-meta", css)
        self.assertIn(".active-row", css)
        self.assertIn("@media (max-width: 760px)", css)
        self.assertNotIn(".summary-grid", css)
        self.assertNotIn(".workflow-header", css)
```

Keep the existing local-assets and accessibility assertions.

- [ ] **Step 4: Run the new tests and confirm the expected failure**

Run:

```bash
python3 -m unittest \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  -k DashboardAssetTests -v
```

Expected: failures for the missing semantic table strings, compact summary, `Queue` tab text, light color scheme, and 760-pixel breakpoint.

- [ ] **Step 5: Commit the red tests**

```bash
git add skills/manage-agent-queue/scripts/test_agent_queue.py
git diff --cached --check
git commit -m "test: 큐 테이블 표시 계약 추가"
```

## Task 2: Build semantic two-line queue rows

**Files:**

- Modify: `skills/manage-agent-queue/scripts/dashboard/dashboard.js:42-135`
- Test: `skills/manage-agent-queue/scripts/test_agent_queue.py:5136-5235`

- [ ] **Step 1: Add small safe-DOM helpers for cells and metadata**

Add below `updatedTime`:

```javascript
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
```

The helpers must continue to assign queue-controlled values through `textContent` only.

- [ ] **Step 2: Replace `renderTask` with a table-row renderer**

Replace the entire `renderTask(task, openTasks)` function with:

```javascript
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
```

- [ ] **Step 3: Add one semantic table per workflow**

Add before `renderSnapshot`:

```javascript
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
```

- [ ] **Step 4: Replace card-grid and workflow-card rendering in `renderSnapshot`**

Replace the existing summary loop with:

```javascript
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
```

Use a native `progress` element so the completion value remains semantic and does not require CSP-blocked inline styles.

Replace the workflow view body after the empty-state branch with:

```javascript
  for (const workflow of snapshot.workflows) {
    workflows.append(renderWorkflow(workflow));
  }
```

Delete the old `openTasks`, `section.workflow`, `header.workflow-header`, and task-details rendering.

- [ ] **Step 5: Keep countdowns honest for rows without a live lease**

Update `updateTimes` so the fallback em dash is retained:

```javascript
const updateTimes = () => {
  for (const node of document.querySelectorAll("[data-lease-until]")) {
    node.textContent = remainingTime(node.dataset.leaseUntil) || "—";
  }
};
```

- [ ] **Step 6: Run JavaScript syntax and asset tests**

Run:

```bash
node --check skills/manage-agent-queue/scripts/dashboard/dashboard.js
python3 -m unittest \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  -k DashboardAssetTests -v
```

Expected: JavaScript syntax passes. Semantic rendering and compact-summary assertions pass; CSS and tab-label assertions remain red until Task 3.

- [ ] **Step 7: Commit the renderer**

```bash
git add skills/manage-agent-queue/scripts/dashboard/dashboard.js
git diff --cached --check
git commit -m "feat: 큐를 두 줄 테이블로 표시"
```

## Task 3: Apply the approved light visual system and responsive layout

**Files:**

- Modify: `skills/manage-agent-queue/scripts/dashboard/index.html:18-24`
- Modify: `skills/manage-agent-queue/scripts/dashboard/dashboard.css:1-98`
- Test: `skills/manage-agent-queue/scripts/test_agent_queue.py:5136-5235`

- [ ] **Step 1: Rename the primary tab without changing its IDs or behavior**

In `index.html`, change only the visible label of `workflow-tab` from `Workflows` to `Queue`:

```html
    <button id="workflow-tab" type="button" aria-controls="workflow-view" aria-selected="true">Queue</button>
```

Keep `workflow-tab`, `workflow-view`, and the Activity tab unchanged so existing JavaScript and accessibility relationships remain stable.

- [ ] **Step 2: Replace dark tokens and card styles with light table tokens**

Replace `dashboard.css` with a light-only stylesheet containing these base tokens and structure:

```css
:root {
  color-scheme: light;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, sans-serif;
  --page: #f7f8fa;
  --surface: #ffffff;
  --text: #172033;
  --muted: #667085;
  --subtle: #98a2b3;
  --line: #e4e7ec;
  --line-strong: #d0d5dd;
  --accent: #2869d8;
  --ok: #16854c;
  --ok-soft: #eefaf3;
  --warn: #a15c00;
  --warn-soft: #fff8eb;
  --danger: #b42318;
  --focus: #84adff;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--page);
  color: var(--text);
  font-size: 15px;
  line-height: 1.45;
}
button { font: inherit; }
button:focus-visible {
  outline: 3px solid var(--focus);
  outline-offset: 2px;
}
.topbar, main, .tabs {
  width: min(1180px, calc(100% - 40px));
  margin-inline: auto;
}
.topbar {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 24px;
  padding: 28px 0 18px;
}
.eyebrow {
  margin: 0;
  color: var(--muted);
  font-size: .75rem;
  font-weight: 700;
  letter-spacing: .08em;
  text-transform: uppercase;
}
h1 { margin: 4px 0 0; font-size: 1.5rem; line-height: 1.25; }
.connection-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px 12px;
}
.connection { color: var(--muted); font-weight: 650; }
.connection::before { content: "●"; margin-right: 6px; font-size: .65em; }
.connection.live { color: var(--ok); }
.connection.retrying { color: var(--warn); }
.connection.stopped { color: var(--danger); }
.muted, .empty { color: var(--muted); }
#manual-refresh {
  padding: 7px 11px;
  border: 1px solid var(--line-strong);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text);
}
.tabs { display: flex; gap: 20px; border-bottom: 1px solid var(--line); }
.tabs button {
  margin-bottom: -1px;
  padding: 10px 2px;
  border: 0;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--muted);
}
.tabs button[aria-selected="true"] {
  border-bottom-color: var(--accent);
  color: var(--accent);
  font-weight: 700;
}
main { padding: 18px 0 48px; }
```

- [ ] **Step 3: Add compact summary, warning, group, and table styles**

Continue the same stylesheet with:

```css
.summary-line {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(180px, 320px);
  align-items: center;
  gap: 20px;
  padding: 4px 0 18px;
}
.summary-text { display: flex; align-items: baseline; flex-wrap: wrap; gap: 6px 14px; }
.summary-progress { font-size: 1rem; }
.summary-counts { color: var(--muted); }
.queue-progress {
  width: 100%;
  height: 6px;
  border: 0;
  border-radius: 999px;
  overflow: hidden;
  background: var(--line);
}
.queue-progress::-webkit-progress-bar { background: var(--line); }
.queue-progress::-webkit-progress-value { background: var(--accent); }
.queue-progress::-moz-progress-bar { background: var(--accent); }
.warning {
  margin: 0 0 10px;
  padding: 9px 12px;
  border-left: 3px solid var(--warn);
  background: var(--warn-soft);
  color: #633c00;
}
.workflow-group { margin-top: 18px; }
.workflow-heading {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 8px;
}
.workflow-name { margin: 0; font-size: 1rem; }
.workflow-progress { color: var(--muted); font-size: .875rem; }
.queue-table {
  width: 100%;
  border: 1px solid var(--line);
  border-collapse: separate;
  border-spacing: 0;
  background: var(--surface);
}
.queue-table th {
  padding: 9px 14px;
  border-bottom: 1px solid var(--line-strong);
  background: #fafbfc;
  color: var(--muted);
  font-size: .75rem;
  font-weight: 700;
  letter-spacing: .03em;
  text-align: left;
  text-transform: uppercase;
}
.queue-table th:first-child { width: 142px; }
.queue-table th:nth-child(3) { width: 150px; }
.queue-table th:nth-child(4) { width: 130px; }
.queue-table td {
  padding: 13px 14px;
  border-bottom: 1px solid var(--line);
  vertical-align: top;
}
.queue-table tbody tr:last-child td { border-bottom: 0; }
.task-row.active-row {
  background: var(--ok-soft);
}
.task-row.active-row > td:first-child { box-shadow: inset 3px 0 0 var(--ok); }
.task-status { color: var(--muted); font-weight: 650; white-space: nowrap; }
.state-leased, .state-completed { color: var(--ok); }
.state-failed, .state-blocked, .state-dependency_failed { color: var(--danger); }
.task-primary { display: flex; align-items: baseline; gap: 8px; min-width: 0; }
.task-id { color: var(--accent); font-weight: 750; white-space: nowrap; }
.task-title { min-width: 0; overflow-wrap: anywhere; font-weight: 650; }
.task-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 2px 14px;
  margin-top: 3px;
  color: var(--muted);
  font-size: .82rem;
}
.meta-item { display: inline-flex; gap: 4px; min-width: 0; }
.meta-label { color: var(--subtle); }
.meta-item strong { color: var(--muted); font-weight: 550; overflow-wrap: anywhere; }
.task-assignee, .task-time { color: var(--muted); overflow-wrap: anywhere; }
.task-time { font-variant-numeric: tabular-nums; white-space: nowrap; }
.task-row.changed { animation: changed 2.5s ease-out; }
.activity-item { margin-block: 8px; }
@keyframes changed {
  from { background: #eaf1ff; }
}
@media (prefers-reduced-motion: reduce) {
  .task-row.changed { animation: none; }
}
```

- [ ] **Step 4: Add a compact mobile table without horizontal scrolling**

Finish the stylesheet with:

```css
@media (max-width: 760px) {
  .topbar, main, .tabs { width: min(100% - 24px, 1180px); }
  .topbar { align-items: flex-start; flex-direction: column; gap: 10px; }
  .connection-actions { justify-content: flex-start; }
  .summary-line { grid-template-columns: 1fr; gap: 9px; }
  .workflow-heading { align-items: flex-start; flex-direction: column; gap: 2px; }
  .queue-table, .queue-table tbody, .queue-table tr { display: block; }
  .queue-table thead {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }
  .queue-table .task-row {
    display: grid;
    grid-template-columns: 96px minmax(0, 1fr);
    column-gap: 10px;
    padding: 12px;
    border-bottom: 1px solid var(--line);
  }
  .queue-table tbody tr:last-child { border-bottom: 0; }
  .queue-table td {
    display: block;
    padding: 0;
    border: 0;
  }
  .task-status { grid-column: 1; grid-row: 1 / span 3; }
  .task-cell, .task-assignee, .task-time { grid-column: 2; }
  .task-primary { align-items: flex-start; flex-direction: column; gap: 1px; }
  .task-meta { margin-top: 5px; }
  .task-assignee, .task-time { margin-top: 5px; font-size: .85rem; white-space: normal; }
  .task-assignee::before { content: "Assignee  "; color: var(--subtle); }
  .task-time::before { content: "Timing  "; color: var(--subtle); }
}
```

No task field is hidden at the responsive breakpoint.

- [ ] **Step 5: Run the asset test to green**

Run:

```bash
node --check skills/manage-agent-queue/scripts/dashboard/dashboard.js
python3 -m unittest \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  -k DashboardAssetTests -v
```

Expected: all `DashboardAssetTests` pass.

- [ ] **Step 6: Commit the approved visual layer**

```bash
git add skills/manage-agent-queue/scripts/dashboard/index.html \
  skills/manage-agent-queue/scripts/dashboard/dashboard.css
git diff --cached --check
git commit -m "style: 큐 테이블을 밝고 읽기 쉽게 정리"
```

## Task 4: Verify state behavior, accessibility, and documentation

**Files:**

- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:5136-5235`
- Modify: `skills/manage-agent-queue/references/queue-schema.md:193-199`
- Verify: `skills/manage-agent-queue/scripts/dashboard/index.html`
- Verify: `skills/manage-agent-queue/scripts/dashboard/dashboard.css`
- Verify: `skills/manage-agent-queue/scripts/dashboard/dashboard.js`

- [ ] **Step 1: Add a focused state-preservation contract**

Add to `DashboardAssetTests`:

```python
    def test_table_redesign_preserves_live_state_contracts(self):
        html = (self.assets / "index.html").read_text(encoding="utf-8")
        css = (self.assets / "dashboard.css").read_text(encoding="utf-8")
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'setConnection("Live", "live")',
            '"Retrying · queue temporarily unavailable"',
            '"Stopped · dashboard server ended"',
            'row.classList.add("changed")',
            'byId("empty-template")',
            'byId("activity-list")',
            'byId("manual-refresh")',
        ):
            self.assertIn(required, javascript)
        self.assertIn('aria-live="polite"', html)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)
```

- [ ] **Step 2: Run the focused tests**

Run:

```bash
python3 -m unittest \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  -k DashboardAssetTests -v
```

Expected: all dashboard asset tests pass.

- [ ] **Step 3: Document the visible browser contract**

In `skills/manage-agent-queue/references/queue-schema.md`, add after the first Local Dashboard paragraph:

```markdown
The Queue view presents a compact completion summary followed by one light, semantic task table per workflow. Each task row keeps status, task ID and title, attempts, dependencies, resources, assignee, and lease timing visible in a two-line hierarchy. At narrow widths the same fields stack without horizontal scrolling. Activity remains available as a secondary view.
```

Do not change command syntax, browser-consent language, API behavior, or server security documentation.

- [ ] **Step 4: Run all queue tests and syntax checks**

Run:

```bash
node --check skills/manage-agent-queue/scripts/dashboard/dashboard.js
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: JavaScript syntax passes and the full Python test module passes.

- [ ] **Step 5: Run static safety and scope checks**

Run:

```bash
rg -n "innerHTML|insertAdjacentHTML|document\.write|eval\(" \
  skills/manage-agent-queue/scripts/dashboard
rg -n "https?://" skills/manage-agent-queue/scripts/dashboard
git diff --check
git status --short
```

Expected:

- The unsafe-DOM search returns no matches.
- The external-URL search returns no matches.
- `git diff --check` reports no whitespace errors.
- `git status --short` lists only the intended implementation, tests, and documentation before commit.

- [ ] **Step 6: Inspect a realistic queue in the browser after consent**

Start the checked-in dashboard with a queue containing active, waiting, completed, attention, and unassigned tasks. Open the browser only after the user approves it.

Verify at desktop width:

- the compact summary is one line plus a thin progress element;
- the active row is identifiable by text, pale green fill, and a 3-pixel leading edge;
- long Korean titles wrap only inside the Task column;
- Attempts, Depends on, and Resources form a quiet second line;
- retrying and stopped states leave the latest table visible;
- Queue and Activity tabs and Refresh now are keyboard operable.

Verify below 760 pixels:

- the header is visually hidden but remains in the DOM;
- every task field remains visible;
- each row becomes a status/content block;
- no horizontal scrolling is introduced.

- [ ] **Step 7: Commit verification and documentation**

```bash
git add skills/manage-agent-queue/scripts/test_agent_queue.py \
  skills/manage-agent-queue/references/queue-schema.md
git diff --cached --check
git commit -m "docs: 큐 테이블 동작과 검증 기준 정리"
```

## Task 5: Final review and publication

**Files:**

- Review: all changes since `cd7d8b1`

- [ ] **Step 1: Review the complete implementation diff**

Run:

```bash
git diff --stat cd7d8b1
git diff cd7d8b1 -- \
  skills/manage-agent-queue/scripts/dashboard/index.html \
  skills/manage-agent-queue/scripts/dashboard/dashboard.css \
  skills/manage-agent-queue/scripts/dashboard/dashboard.js \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  skills/manage-agent-queue/references/queue-schema.md
```

Confirm the diff contains no Python projection/server change, API schema change, dependency, build step, mutation control, card summary grid, expandable task row, or hidden mobile task field.

- [ ] **Step 2: Re-run completion verification from a clean status**

Run:

```bash
node --check skills/manage-agent-queue/scripts/dashboard/dashboard.js
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
git diff --check cd7d8b1
git status --short --branch
```

Expected: syntax and tests pass, there are no whitespace errors, and the worktree contains no uncommitted implementation changes.

- [ ] **Step 3: Push the feature branch**

```bash
git push origin feature/local-queue-dashboard
```

- [ ] **Step 4: Update or open the pull request in Korean**

The pull request summary must state:

- the Queue view is now a light semantic table;
- task rows use a two-line primary/metadata hierarchy;
- active state and responsive behavior are easier to scan;
- polling, Activity, retry/stopped behavior, safe DOM rendering, and server security remain unchanged;
- the full queue test module passes.

Do not add an agent/tool prefix to the pull request title.
