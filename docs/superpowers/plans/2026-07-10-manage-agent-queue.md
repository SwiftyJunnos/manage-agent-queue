# Manage Agent Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and validate a skills.sh-compatible `manage-agent-queue` skill with a dependency-free Python 3 CLI for coordinating concurrent agents through a shared JSON queue and readable TSV projection.

**Architecture:** `queue.json` is the sole source of truth and `queue.tsv` is a repairable human-readable projection. The Python CLI serializes all state changes with a short-lived directory lock, applies schema and graph invariants, and exposes generic tasks, leases, retries, exclusive resources, and two workflow templates; the skill instructs the host runtime how to start agents but never starts them itself.

**Tech Stack:** Agent Skills `SKILL.md`, Python 3 standard library (`argparse`, `csv`, `json`, `multiprocessing`, `os`, `pathlib`, `secrets`, `tempfile`, `unittest`), skills CLI, skill-creator validation via `uv` and PyYAML.

---

## File Map

- Create `skills/manage-agent-queue/SKILL.md`: concise coordinator and worker operating protocol plus routing to references.
- Create `skills/manage-agent-queue/agents/openai.yaml`: generated UI metadata.
- Create `skills/manage-agent-queue/scripts/agent_queue.py`: complete queue model, persistence, locking, transitions, views, templates, and CLI.
- Create `skills/manage-agent-queue/scripts/test_agent_queue.py`: unit, subprocess, and multiprocessing coverage for the public contract.
- Create `skills/manage-agent-queue/references/queue-schema.md`: detailed JSON schema, stored and derived states, invariants, exit codes, and TSV contract.
- Create `skills/manage-agent-queue/references/workflow-templates.md`: adversarial-review and parallel-shards graphs with role context boundaries.
- Modify `docs/superpowers/specs/2026-07-10-manage-agent-queue-design.md` only if implementation reveals a necessary contract correction; do not broaden scope.

## Task 1: Scaffold the Publishable Skill

**Files:**
- Create: `skills/manage-agent-queue/SKILL.md`
- Create: `skills/manage-agent-queue/agents/openai.yaml`
- Create: `skills/manage-agent-queue/scripts/agent_queue.py`
- Create: `skills/manage-agent-queue/scripts/test_agent_queue.py`
- Create: `skills/manage-agent-queue/references/queue-schema.md`
- Create: `skills/manage-agent-queue/references/workflow-templates.md`

- [ ] **Step 1: Initialize the skill with deterministic interface metadata**

Run:

```bash
python3 /Users/junnos/.codex/skills/.system/skill-creator/scripts/init_skill.py \
  manage-agent-queue \
  --path skills \
  --resources scripts,references \
  --interface 'display_name=Manage Agent Queue' \
  --interface 'short_description=Coordinate agents with a safe shared task queue' \
  --interface 'default_prompt=Use $manage-agent-queue to coordinate this work across agents with dependencies, leases, and status tracking.'
```

Expected: `skills/manage-agent-queue/` exists with `SKILL.md`, `agents/openai.yaml`, `scripts/`, and `references/`.

- [ ] **Step 2: Replace generated boilerplate with a minimal valid skill**

Set `skills/manage-agent-queue/SKILL.md` to:

```markdown
---
name: manage-agent-queue
description: Coordinate multiple coding agents through a shared dependency-aware priority queue with leases, retries, exclusive resources, adversarial review, and human-readable status. Use when work should be divided among concurrent or sequential agents, when agents need safe task claiming across worktrees, or when a coordinator must track assignments and recovery without directly managing agent processes.
---

# Manage Agent Queue

Use `scripts/agent_queue.py` as the only queue writer. Do not edit generated queue files directly.

The complete coordinator and worker protocol is added after the CLI contract is executable.
```

Create empty executable entry and test files without implementation promises:

```python
#!/usr/bin/env python3
"""Manage a shared local queue for cooperating agents."""
```

```python
#!/usr/bin/env python3
"""Tests for the manage-agent-queue CLI."""
```

Create the two references with only their final titles so linked files exist:

```markdown
# Queue Schema
```

```markdown
# Workflow Templates
```

- [ ] **Step 3: Verify metadata and generated UI configuration**

Run:

```bash
sed -n '1,80p' skills/manage-agent-queue/SKILL.md
sed -n '1,80p' skills/manage-agent-queue/agents/openai.yaml
uv run --with pyyaml python /Users/junnos/.codex/skills/.system/skill-creator/scripts/quick_validate.py skills/manage-agent-queue
```

Expected: validation prints `Skill is valid!`; `openai.yaml` contains the three supplied interface values and the default prompt mentions `$manage-agent-queue`.

- [ ] **Step 4: Commit the valid scaffold**

```bash
git add skills/manage-agent-queue
git commit -m "chore: scaffold manage-agent-queue skill"
```

## Task 2: Initialize and Validate Queue State

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing initialization and validation tests**

Add the import setup and these tests to `test_agent_queue.py`:

```python
import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import agent_queue as aq


class QueueStateTests(unittest.TestCase):
    def test_new_state_has_versioned_monotonic_counters(self):
        state = aq.new_state("demo", aq.fixed_config())
        self.assertEqual(state["schema_version"], 1)
        self.assertEqual(state["queue_id"], "demo")
        self.assertEqual(state["revision"], 0)
        self.assertEqual(state["next_task_sequence"], 1)
        self.assertEqual(state["next_workflow_sequence"], 1)
        self.assertEqual(state["next_event_sequence"], 1)
        self.assertEqual(state["tasks"], {})
        self.assertEqual(state["events"], [])

    def test_validate_state_rejects_unknown_schema(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["schema_version"] = 2
        with self.assertRaisesRegex(aq.InvariantError, "schema_version"):
            aq.validate_state(state)

    def test_initialize_writes_json_and_revision_matched_tsv(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            state = aq.initialize_queue(queue_path, "demo", aq.fixed_config())
            self.assertEqual(json.loads(queue_path.read_text()), state)
            self.assertTrue(queue_path.with_suffix(".tsv").read_text().startswith("# queue_revision: 0\n"))
```

- [ ] **Step 2: Run the tests to verify failure**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL because `new_state`, `fixed_config`, `InvariantError`, and `initialize_queue` do not exist.

- [ ] **Step 3: Implement the versioned state and atomic writers**

Add these public constants, errors, time helper, configuration, state construction, validation, and persistence functions to `agent_queue.py`:

```python
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
STORED_STATUSES = {"pending", "leased", "completed", "failed", "blocked", "cancelled"}


class QueueError(Exception):
    exit_code = 2


class InvariantError(QueueError):
    exit_code = 6


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fixed_config():
    return {
        "default_lease_seconds": 900,
        "default_max_attempts": 3,
        "retry_backoff_seconds": 30,
        "lock_timeout_seconds": 5,
        "stale_lock_seconds": 30,
    }


def new_state(queue_id, config):
    now = utc_now()
    return {
        "schema_version": SCHEMA_VERSION,
        "queue_id": queue_id,
        "revision": 0,
        "next_task_sequence": 1,
        "next_workflow_sequence": 1,
        "next_event_sequence": 1,
        "created_at": now,
        "updated_at": now,
        "config": dict(config),
        "tasks": {},
        "events": [],
    }


def validate_state(state):
    if state.get("schema_version") != SCHEMA_VERSION:
        raise InvariantError("unsupported schema_version")
    required = {"queue_id", "revision", "next_task_sequence", "next_workflow_sequence", "next_event_sequence", "config", "tasks", "events"}
    missing = sorted(required.difference(state))
    if missing:
        raise InvariantError(f"missing queue fields: {', '.join(missing)}")
    if not isinstance(state["tasks"], dict) or not isinstance(state["events"], list):
        raise InvariantError("tasks must be an object and events must be an array")
    return state


def atomic_write_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def write_json(path, state):
    validate_state(state)
    atomic_write_text(path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def render_empty_tsv(revision):
    header = "id\tworkflow\trole\tstate\tpriority\tassignee\tlease_until\tattempts\tdepends_on\tblocked_by\tresources\ttitle\n"
    return f"# queue_revision: {revision}\n{header}"


def initialize_queue(path, queue_id, config):
    path = Path(path)
    if path.exists():
        raise QueueError(f"queue already exists: {path}")
    state = new_state(queue_id, config)
    write_json(path, state)
    atomic_write_text(path.with_suffix(".tsv"), render_empty_tsv(state["revision"]))
    return state
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit queue initialization**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: initialize versioned agent queue state"
```

## Task 3: Add Generic Tasks and Dependency Validation

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing task graph tests**

Add tests covering stable IDs, defaults, atomic batch rejection, missing dependencies, and cycles:

```python
class TaskGraphTests(unittest.TestCase):
    def setUp(self):
        self.state = aq.new_state("demo", aq.fixed_config())

    def test_add_task_assigns_defaults_and_monotonic_id(self):
        first = aq.add_task(self.state, {"title": "First"})
        second = aq.add_task(self.state, {"title": "Second", "priority": 7, "depends_on": [first["id"]]})
        self.assertEqual(first["id"], "T-000001")
        self.assertEqual(first["status"], "pending")
        self.assertEqual(first["max_attempts"], 3)
        self.assertEqual(second["id"], "T-000002")
        self.assertEqual(second["priority"], 7)

    def test_batch_is_atomic_when_dependency_is_missing(self):
        before = json.dumps(self.state, sort_keys=True)
        with self.assertRaisesRegex(aq.InvariantError, "missing dependency"):
            aq.add_task_batch(self.state, [{"title": "Broken", "depends_on": ["T-999999"]}])
        self.assertEqual(json.dumps(self.state, sort_keys=True), before)

    def test_cycle_is_rejected(self):
        with self.assertRaisesRegex(aq.InvariantError, "cycle"):
            aq.add_task_batch(self.state, [
                {"id": "T-000010", "title": "A", "depends_on": ["T-000011"]},
                {"id": "T-000011", "title": "B", "depends_on": ["T-000010"]},
            ])
```

- [ ] **Step 2: Run the graph tests to verify failure**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL because the task creation functions are missing.

- [ ] **Step 3: Implement task normalization and all-or-nothing batches**

Add `copy`, ID allocation, task normalization, graph validation, and batch commit:

```python
import copy
import re


def allocate_id(state, kind):
    key = "next_task_sequence" if kind == "task" else "next_workflow_sequence"
    prefix = "T" if kind == "task" else "W"
    value = state[key]
    state[key] += 1
    return f"{prefix}-{value:06d}"


def reserve_task_id(state, explicit_id=None):
    if explicit_id is None:
        return allocate_id(state, "task")
    match = re.fullmatch(r"T-(\d{6})", str(explicit_id))
    if not match:
        raise QueueError("explicit task id must match T-000001")
    sequence = int(match.group(1))
    state["next_task_sequence"] = max(state["next_task_sequence"], sequence + 1)
    return explicit_id


def normalize_task(state, raw):
    title = str(raw.get("title", "")).strip()
    if not title:
        raise QueueError("task title is required")
    task_id = reserve_task_id(state, raw.get("id"))
    now = utc_now()
    return {
        "id": task_id,
        "workflow_id": raw.get("workflow_id"),
        "role": raw.get("role"),
        "title": title,
        "description": str(raw.get("description", "")),
        "status": "pending",
        "priority": int(raw.get("priority", 0)),
        "depends_on": list(dict.fromkeys(raw.get("depends_on", []))),
        "resources": list(dict.fromkeys(raw.get("resources", []))),
        "labels": list(dict.fromkeys(raw.get("labels", []))),
        "attempts": 0,
        "max_attempts": int(raw.get("max_attempts", state["config"]["default_max_attempts"])),
        "available_at": None,
        "claim": None,
        "result": None,
        "last_error": None,
        "created_at": now,
        "updated_at": now,
    }


def validate_graph(tasks):
    for task_id, task in tasks.items():
        for dependency in task["depends_on"]:
            if dependency not in tasks:
                raise InvariantError(f"missing dependency {dependency} for {task_id}")
    visiting, visited = set(), set()

    def visit(task_id):
        if task_id in visiting:
            raise InvariantError(f"dependency cycle includes {task_id}")
        if task_id in visited:
            return
        visiting.add(task_id)
        for dependency in tasks[task_id]["depends_on"]:
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in tasks:
        visit(task_id)


def add_task_batch(state, raw_tasks):
    candidate = copy.deepcopy(state)
    created = []
    for raw in raw_tasks:
        task = normalize_task(candidate, raw)
        if task["id"] in candidate["tasks"]:
            raise InvariantError(f"duplicate task id: {task['id']}")
        candidate["tasks"][task["id"]] = task
        created.append(task)
    validate_graph(candidate["tasks"])
    state.clear()
    state.update(candidate)
    return created


def add_task(state, raw):
    return add_task_batch(state, [raw])[0]
```

Before returning the task, reject nonpositive `max_attempts`, non-string labels/resources, and a task depending on itself. Add focused assertions for each rejection to `TaskGraphTests`.

- [ ] **Step 4: Run all tests and verify graph behavior**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all initialization and task graph tests pass; the cycle case raises the documented error without mutating the original state.

- [ ] **Step 5: Commit generic task creation**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: add dependency-aware queue tasks"
```

## Task 4: Derive Status and Render TSV

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing readiness and TSV tests**

Add tests that construct completed, failed, future-retry, resource-conflicted, and ready tasks:

```python
class StatusProjectionTests(unittest.TestCase):
    def test_derived_states_cover_dependency_retry_and_resource_waits(self):
        state = aq.new_state("demo", aq.fixed_config())
        completed = aq.add_task(state, {"title": "done", "resources": ["file:a"]})
        completed["status"] = "completed"
        failed = aq.add_task(state, {"title": "failed"})
        failed["status"] = "failed"
        waiting = aq.add_task(state, {"title": "waiting", "depends_on": [completed["id"]]})
        broken = aq.add_task(state, {"title": "broken", "depends_on": [failed["id"]]})
        waiting["available_at"] = "2999-01-01T00:00:00Z"
        self.assertEqual(aq.derive_state(state, waiting, "2026-07-10T00:00:00Z"), "waiting_retry")
        self.assertEqual(aq.derive_state(state, broken, "2026-07-10T00:00:00Z"), "dependency_failed")

    def test_tsv_escapes_tabs_and_newlines_and_includes_revision(self):
        state = aq.new_state("demo", aq.fixed_config())
        aq.add_task(state, {"title": "line\tone\ntwo"})
        rendered = aq.render_tsv(state, "2026-07-10T00:00:00Z")
        self.assertTrue(rendered.startswith("# queue_revision: 0\n"))
        self.assertIn("line\\tone\\ntwo", rendered)
        self.assertEqual(len(rendered.splitlines()), 3)
```

- [ ] **Step 2: Run tests to verify status functions are absent**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL for missing `derive_state` and `render_tsv`.

- [ ] **Step 3: Implement deterministic derived states and TSV rows**

Add helpers with this precedence:

```python
import csv
import io


def dependency_blockers(state, task):
    return [task_id for task_id in task["depends_on"] if state["tasks"][task_id]["status"] != "completed"]


def leased_resources(state, excluding=None):
    result = set()
    for task in state["tasks"].values():
        if task["id"] != excluding and task["status"] == "leased":
            result.update(task["resources"])
    return result


def derive_state(state, task, now):
    if task["status"] != "pending":
        return task["status"]
    blockers = dependency_blockers(state, task)
    if any(state["tasks"][item]["status"] in {"failed", "blocked", "cancelled"} for item in blockers):
        return "dependency_failed"
    if blockers:
        return "waiting_dependency"
    if task["available_at"] and task["available_at"] > now:
        return "waiting_retry"
    if set(task["resources"]) & leased_resources(state, task["id"]):
        return "resource_conflict"
    return "ready"


def escape_tsv(value):
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n")


def render_tsv(state, now):
    output = io.StringIO(newline="")
    output.write(f"# queue_revision: {state['revision']}\n")
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    writer.writerow(["id", "workflow", "role", "state", "priority", "assignee", "lease_until", "attempts", "depends_on", "blocked_by", "resources", "title"])
    for task in sorted(state["tasks"].values(), key=lambda item: item["id"]):
        blockers = dependency_blockers(state, task)
        claim = task["claim"] or {}
        writer.writerow([escape_tsv(value) for value in [
            task["id"], task["workflow_id"] or "", task["role"] or "", derive_state(state, task, now),
            task["priority"], claim.get("agent_id", ""), claim.get("expires_at", ""),
            f"{task['attempts']}/{task['max_attempts']}", ",".join(task["depends_on"]), ",".join(blockers),
            ",".join(task["resources"]), task["title"],
        ]])
    return output.getvalue()
```

Use the same row builder for terminal table, JSON status, and TSV so filters cannot disagree. Add filters for workflow, assignee, role, label, and derived state.

- [ ] **Step 4: Run tests and inspect a real TSV fixture**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all tests pass and TSV user fields remain one physical line per task.

- [ ] **Step 5: Commit status projection**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: render queue status and tsv projection"
```

## Task 5: Claim Tasks with Leases, Priority, and Resource Exclusion

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing claim tests**

Add tests for priority, FIFO ties, filters, resource conflicts, retry availability, and token secrecy:

```python
class ClaimTests(unittest.TestCase):
    def test_claim_uses_priority_then_creation_order(self):
        state = aq.new_state("demo", aq.fixed_config())
        low = aq.add_task(state, {"title": "low", "priority": 1})
        first_high = aq.add_task(state, {"title": "first high", "priority": 9})
        aq.add_task(state, {"title": "second high", "priority": 9})
        claimed = aq.claim_task(state, "agent-a", now="2026-07-10T00:00:00Z")
        self.assertEqual(claimed["task"]["id"], first_high["id"])
        self.assertNotEqual(claimed["task"]["id"], low["id"])
        self.assertTrue(claimed["lease_token"])
        self.assertNotIn(claimed["lease_token"], aq.render_tsv(state, "2026-07-10T00:00:00Z"))

    def test_claim_skips_conflicting_resource(self):
        state = aq.new_state("demo", aq.fixed_config())
        first = aq.add_task(state, {"title": "writer one", "priority": 10, "resources": ["file:a"]})
        second = aq.add_task(state, {"title": "writer two", "priority": 9, "resources": ["file:a"]})
        aq.claim_task(state, "agent-a", now="2026-07-10T00:00:00Z")
        with self.assertRaises(aq.NoTaskAvailable):
            aq.claim_task(state, "agent-b", now="2026-07-10T00:00:00Z")
        self.assertEqual(second["status"], "pending")
        self.assertEqual(first["status"], "leased")
```

- [ ] **Step 2: Run tests to verify claim behavior is missing**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL for missing `claim_task` and `NoTaskAvailable`.

- [ ] **Step 3: Implement eligibility, deterministic selection, and lease creation**

Add:

```python
import secrets
from datetime import datetime, timedelta, timezone


class NoTaskAvailable(QueueError):
    exit_code = 3


def append_event(state, event_type, actor, task_id, details, now):
    event = {
        "seq": state["next_event_sequence"],
        "at": now,
        "type": event_type,
        "actor": actor,
        "task_id": task_id,
        "revision": state["revision"] + 1,
        "details": dict(details),
    }
    state["next_event_sequence"] += 1
    state["events"].append(event)
    return event


def add_seconds(timestamp, seconds):
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return (parsed + timedelta(seconds=seconds)).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def claim_task(state, agent_id, now=None, role=None, labels=None, lease_seconds=None):
    now = now or utc_now()
    requested_labels = set(labels or [])
    candidates = []
    for task in state["tasks"].values():
        if derive_state(state, task, now) != "ready":
            continue
        if role and task["role"] != role:
            continue
        if requested_labels and not requested_labels.issubset(task["labels"]):
            continue
        candidates.append(task)
    if not candidates:
        raise NoTaskAvailable("no eligible task")
    task = sorted(candidates, key=lambda item: (-item["priority"], item["created_at"], item["id"]))[0]
    token = secrets.token_urlsafe(32)
    duration = lease_seconds or state["config"]["default_lease_seconds"]
    task["attempts"] += 1
    task["status"] = "leased"
    task["available_at"] = None
    task["claim"] = {
        "agent_id": agent_id,
        "lease_token": token,
        "claimed_at": now,
        "heartbeat_at": now,
        "expires_at": add_seconds(now, duration),
    }
    task["updated_at"] = now
    return {"task": copy.deepcopy(task), "lease_token": token, "expires_at": task["claim"]["expires_at"]}
```

Reject empty agent IDs and nonpositive lease durations. Add an event helper now and call it for claims; event details include duration but not the token.

- [ ] **Step 4: Run claim tests and verify no token appears in views or events**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all tests pass; the chosen task follows priority and stable order; conflicting resources remain pending.

- [ ] **Step 5: Commit lease-based claiming**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: claim queue tasks with leases"
```

## Task 6: Implement Worker and Administrative State Transitions

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing lifecycle tests**

Add tests for heartbeat, completion, retryable and terminal failures, release, expiry, stale tokens, manual retry, blocking, unblocking, and cancellation:

```python
class LifecycleTests(unittest.TestCase):
    def test_expired_worker_cannot_complete_reclaimed_task(self):
        state = aq.new_state("demo", aq.fixed_config())
        task = aq.add_task(state, {"title": "work", "max_attempts": 2})
        first = aq.claim_task(state, "agent-a", now="2026-07-10T00:00:00Z", lease_seconds=1)
        aq.sweep_expired(state, "2026-07-10T00:00:02Z")
        second = aq.claim_task(state, "agent-b", now="2026-07-10T00:00:32Z", lease_seconds=30)
        with self.assertRaises(aq.LeaseError):
            aq.complete_task(state, task["id"], "agent-a", first["lease_token"], "late", [], "2026-07-10T00:00:33Z")
        aq.complete_task(state, task["id"], "agent-b", second["lease_token"], "done", ["result.md"], "2026-07-10T00:00:34Z")
        self.assertEqual(task["status"], "completed")

    def test_retry_grants_one_additional_attempt(self):
        state = aq.new_state("demo", aq.fixed_config())
        task = aq.add_task(state, {"title": "failed", "max_attempts": 1})
        task["status"] = "failed"
        task["attempts"] = 1
        aq.retry_task(state, task["id"], additional_attempts=1, now="2026-07-10T00:00:00Z")
        self.assertEqual(task["status"], "pending")
        self.assertEqual(task["max_attempts"], 2)
        self.assertEqual(task["attempts"], 1)
```

- [ ] **Step 2: Run lifecycle tests to verify failure**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL for missing transition functions and `LeaseError`.

- [ ] **Step 3: Implement token-checked lifecycle transitions**

Add:

```python
class LeaseError(QueueError):
    exit_code = 5


def require_lease(state, task_id, agent_id, token, now):
    task = state["tasks"].get(task_id)
    if not task or task["status"] != "leased" or not task["claim"]:
        raise LeaseError("task is not actively leased")
    claim = task["claim"]
    if claim["agent_id"] != agent_id or not secrets.compare_digest(claim["lease_token"], token):
        raise LeaseError("lease identity or token mismatch")
    if claim["expires_at"] <= now:
        raise LeaseError("lease expired")
    return task


def complete_task(state, task_id, agent_id, token, summary, artifacts, now=None):
    now = now or utc_now()
    task = require_lease(state, task_id, agent_id, token, now)
    task["status"] = "completed"
    task["result"] = {"summary": summary, "artifacts": list(artifacts)}
    task["claim"] = None
    task["updated_at"] = now
    append_event(state, "task.completed", agent_id, task_id, {"artifact_count": len(artifacts)}, now)
    return task


def heartbeat_task(state, task_id, agent_id, token, lease_seconds=None, now=None):
    now = now or utc_now()
    task = require_lease(state, task_id, agent_id, token, now)
    duration = lease_seconds or state["config"]["default_lease_seconds"]
    if duration <= 0:
        raise QueueError("lease duration must be positive")
    task["claim"]["heartbeat_at"] = now
    task["claim"]["expires_at"] = add_seconds(now, duration)
    task["updated_at"] = now
    append_event(state, "task.heartbeat", agent_id, task_id, {"lease_seconds": duration}, now)
    return task


def apply_retry_rule(state, task, message, now):
    task["claim"] = None
    task["last_error"] = {"message": message, "at": now}
    if task["attempts"] < task["max_attempts"]:
        task["status"] = "pending"
        task["available_at"] = add_seconds(now, state["config"]["retry_backoff_seconds"])
    else:
        task["status"] = "failed"
        task["available_at"] = None
    task["updated_at"] = now


def fail_task(state, task_id, agent_id, token, message, terminal=False, now=None):
    now = now or utc_now()
    task = require_lease(state, task_id, agent_id, token, now)
    if terminal:
        task["status"] = "failed"
        task["claim"] = None
        task["available_at"] = None
        task["last_error"] = {"message": message, "at": now}
        task["updated_at"] = now
    else:
        apply_retry_rule(state, task, message, now)
    append_event(state, "task.failed", agent_id, task_id, {"terminal": terminal, "status": task["status"]}, now)
    return task


def release_task(state, task_id, agent_id, token, now=None):
    now = now or utc_now()
    task = require_lease(state, task_id, agent_id, token, now)
    task["status"] = "pending"
    task["claim"] = None
    task["available_at"] = now
    task["updated_at"] = now
    append_event(state, "task.released", agent_id, task_id, {}, now)
    return task


def sweep_expired(state, now=None):
    now = now or utc_now()
    changed = []
    for task in state["tasks"].values():
        if task["status"] != "leased" or task["claim"]["expires_at"] > now:
            continue
        task["claim"] = None
        task["last_error"] = {"message": "lease expired", "at": now}
        if task["attempts"] < task["max_attempts"]:
            task["status"] = "pending"
            task["available_at"] = add_seconds(now, state["config"]["retry_backoff_seconds"])
        else:
            task["status"] = "failed"
            task["available_at"] = None
        task["updated_at"] = now
        changed.append(task["id"])
    return changed


def retry_task(state, task_id, additional_attempts=1, now=None):
    now = now or utc_now()
    task = state["tasks"].get(task_id)
    if not task or task["status"] != "failed":
        raise QueueError("retry requires a failed task")
    if additional_attempts <= 0:
        raise QueueError("additional attempts must be positive")
    task["max_attempts"] += additional_attempts
    task["status"] = "pending"
    task["available_at"] = now
    task["claim"] = None
    task["updated_at"] = now
    append_event(state, "task.retried", "operator", task_id, {"additional_attempts": additional_attempts}, now)
    return task


def block_task(state, task_id, reason, now=None):
    now = now or utc_now()
    task = state["tasks"].get(task_id)
    if not task or task["status"] != "pending":
        raise QueueError("block requires a pending task")
    task["status"] = "blocked"
    task["last_error"] = {"message": reason, "at": now, "kind": "blocked"}
    task["updated_at"] = now
    append_event(state, "task.blocked", "operator", task_id, {"reason": reason}, now)
    return task


def unblock_task(state, task_id, now=None):
    now = now or utc_now()
    task = state["tasks"].get(task_id)
    if not task or task["status"] != "blocked":
        raise QueueError("unblock requires a blocked task")
    task["status"] = "pending"
    task["available_at"] = now
    task["updated_at"] = now
    append_event(state, "task.unblocked", "operator", task_id, {}, now)
    return task


def cancel_task(state, task_id, now=None):
    now = now or utc_now()
    task = state["tasks"].get(task_id)
    if not task or task["status"] not in {"pending", "blocked", "failed"}:
        raise QueueError("cancel requires a pending, blocked, or failed task")
    task["status"] = "cancelled"
    task["available_at"] = None
    task["updated_at"] = now
    append_event(state, "task.cancelled", "operator", task_id, {}, now)
    return task
```

Refactor `sweep_expired` to call `apply_retry_rule` and append `task.lease_expired` for every expired task so expiry and explicit failure cannot drift.

- [ ] **Step 4: Run lifecycle tests**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all lifecycle tests pass; stale tokens and expired claims return exit-code class `5`; manual retry preserves history.

- [ ] **Step 5: Commit transition handling**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: manage queue task lifecycle"
```

## Task 7: Add Locking, Persistence Transactions, and the Public CLI

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing subprocess and concurrency tests**

Add a subprocess helper and tests that initialize through the CLI, claim from 16 processes, verify unique assignments, test lock timeout, stale-lock recovery, JSON output, and exit codes:

```python
import multiprocessing
import subprocess


CLI = SCRIPT_DIR / "agent_queue.py"


def run_cli(queue_path, *arguments):
    return subprocess.run(
        [sys.executable, str(CLI), "--queue", str(queue_path), *arguments],
        text=True,
        capture_output=True,
        check=False,
    )


def claim_once(queue_path, agent_id, output):
    result = run_cli(queue_path, "claim", "--agent", agent_id)
    output.put((result.returncode, result.stdout, result.stderr))


class CliConcurrencyTests(unittest.TestCase):
    def test_sixteen_processes_receive_unique_tasks(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            self.assertEqual(run_cli(queue_path, "init", "--id", "demo").returncode, 0)
            tasks_path = Path(directory) / "tasks.json"
            tasks_path.write_text(json.dumps([{"title": f"task {index}"} for index in range(16)]))
            self.assertEqual(run_cli(queue_path, "task", "add-batch", "--from-json", str(tasks_path)).returncode, 0)
            output = multiprocessing.Queue()
            processes = [multiprocessing.Process(target=claim_once, args=(queue_path, f"agent-{index}", output)) for index in range(16)]
            for process in processes:
                process.start()
            for process in processes:
                process.join(10)
                self.assertFalse(process.is_alive())
            results = [output.get(timeout=2) for _ in processes]
            task_ids = [json.loads(stdout)["task"]["id"] for code, stdout, stderr in results if code == 0]
            self.assertEqual(len(task_ids), 16)
            self.assertEqual(len(set(task_ids)), 16)
```

- [ ] **Step 2: Run tests to verify the CLI and lock are missing**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL because `agent_queue.py` has no argument parser, transaction wrapper, or directory lock.

- [ ] **Step 3: Implement queue location and a token-owned directory lock**

Add `resolve_queue_path()` that uses `--queue`, then `AGENT_QUEUE_PATH`, then the nearest ancestor containing `.git`, falling back to the current directory. Add a `QueueLock` context manager that:

```python
class LockTimeout(QueueError):
    exit_code = 4


class QueueLock:
    def __init__(self, queue_path, timeout_seconds, stale_seconds):
        self.path = Path(f"{queue_path}.lock")
        self.timeout_seconds = timeout_seconds
        self.stale_seconds = stale_seconds
        self.token = secrets.token_urlsafe(24)

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self.path.mkdir()
                owner = {"token": self.token, "pid": os.getpid(), "hostname": socket.gethostname(), "acquired_at": utc_now(), "stale_after": add_seconds(utc_now(), self.stale_seconds)}
                atomic_write_text(self.path / "owner.json", json.dumps(owner, sort_keys=True) + "\n")
                return self
            except FileExistsError:
                if reclaim_stale_lock(self.path, utc_now(), self.stale_seconds):
                    continue
                if time.monotonic() >= deadline:
                    raise LockTimeout(f"timed out acquiring {self.path}")
                time.sleep(random.uniform(0.01, 0.05))

    def __exit__(self, exc_type, exc, traceback):
        owner_path = self.path / "owner.json"
        if owner_path.exists() and json.loads(owner_path.read_text())["token"] == self.token:
            shutil.rmtree(self.path)


def reclaim_stale_lock(lock_path, now, stale_seconds):
    owner_path = lock_path / "owner.json"
    try:
        owner = json.loads(owner_path.read_text())
        stale = owner.get("stale_after", "") <= now
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        stale = time.time() - lock_path.stat().st_mtime > stale_seconds
    if not stale:
        return False
    orphan = lock_path.with_name(f"{lock_path.name}.orphan-{secrets.token_hex(8)}")
    try:
        os.replace(lock_path, orphan)
    except FileNotFoundError:
        return False
    shutil.rmtree(orphan, ignore_errors=True)
    return True
```

Import `random`, `shutil`, `socket`, and `time`. Add tests for a lock directory missing `owner.json` so it is reclaimed only after its filesystem age exceeds `stale_seconds`.

- [ ] **Step 4: Implement transactional mutation and argparse dispatch**

Create `load_state`, `commit_state`, and `mutate_queue` helpers:

```python
def load_state(path):
    try:
        state = json.loads(Path(path).read_text())
    except FileNotFoundError as error:
        raise QueueError(f"queue does not exist: {path}") from error
    except json.JSONDecodeError as error:
        raise InvariantError(f"invalid queue JSON: {error}") from error
    validate_state(state)
    validate_graph(state["tasks"])
    return state


def commit_state(path, state, now):
    state["revision"] += 1
    state["updated_at"] = now
    validate_state(state)
    validate_graph(state["tasks"])
    write_json(path, state)
    atomic_write_text(Path(path).with_suffix(".tsv"), render_tsv(state, now))


def peek_lock_config(path):
    defaults = fixed_config()
    try:
        config = json.loads(Path(path).read_text()).get("config", {})
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return defaults["lock_timeout_seconds"], defaults["stale_lock_seconds"]
    timeout = config.get("lock_timeout_seconds", defaults["lock_timeout_seconds"])
    stale = config.get("stale_lock_seconds", defaults["stale_lock_seconds"])
    if not isinstance(timeout, (int, float)) or timeout <= 0 or not isinstance(stale, (int, float)) or stale <= 0:
        return defaults["lock_timeout_seconds"], defaults["stale_lock_seconds"]
    return timeout, stale


def mutate_queue(path, callback, now=None):
    now = now or utc_now()
    lock_timeout, stale_lock = peek_lock_config(path)
    with QueueLock(path, lock_timeout, stale_lock):
        state = load_state(path)
        before = json.dumps(state, sort_keys=True)
        sweep_expired(state, now)
        result = callback(state, now)
        after = json.dumps(state, sort_keys=True)
        if after != before:
            commit_state(path, state, now)
        else:
            tsv_path = Path(path).with_suffix(".tsv")
            expected = f"# queue_revision: {state['revision']}\n"
            if not tsv_path.exists() or not tsv_path.read_text().startswith(expected):
                atomic_write_text(tsv_path, render_tsv(state, now))
        return result
```

Keep initialization under the same queue lock, but create `new_state` instead of calling `load_state`. Before commit, normalize event `revision` values to the single next queue revision so a sweep and command performed in one transaction share that revision.

Build subcommands matching the design:

```text
init
task add | add-batch | show
workflow add
claim | heartbeat | complete | fail | release
retry | block | unblock | cancel
status | events | sweep | doctor | export | compact
```

Use this entry-point shape:

```python
def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = args.handler(args)
        if result is not None:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except QueueError as error:
        print(str(error), file=sys.stderr)
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
```

`status` acquires the lock and sweeps expiry. It writes source state only when expiry changes it, but always repairs a missing or revision-stale TSV. Read-only commands validate before output.

- [ ] **Step 5: Run the full suite repeatedly to exercise concurrency**

Run:

```bash
for run in 1 2 3; do python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v || exit 1; done
```

Expected: all three runs pass; 16 successful claims have 16 unique task IDs; no process exceeds its 10-second join limit.

- [ ] **Step 6: Commit the transactional CLI**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: add atomic queue cli transactions"
```

## Task 8: Generate Built-in Workflow Graphs

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing template tests**

Add exact graph assertions:

```python
class WorkflowTemplateTests(unittest.TestCase):
    def test_adversarial_review_has_independent_read_only_reviews(self):
        state = aq.new_state("demo", aq.fixed_config())
        workflow = aq.add_adversarial_review(state, "Port API", 50, ["file:api.py"], 2)
        tasks = [state["tasks"][task_id] for task_id in workflow["task_ids"]]
        implement = next(task for task in tasks if task["role"] == "implement")
        reviews = [task for task in tasks if task["role"] == "review"]
        apply_task = next(task for task in tasks if task["role"] == "apply")
        verify = next(task for task in tasks if task["role"] == "verify")
        self.assertEqual(len(reviews), 2)
        self.assertTrue(all(task["depends_on"] == [implement["id"]] for task in reviews))
        self.assertTrue(all(task["resources"] == [] for task in reviews))
        self.assertEqual(set(apply_task["depends_on"]), {task["id"] for task in reviews})
        self.assertEqual(verify["depends_on"], [apply_task["id"]])

    def test_parallel_shards_reject_duplicate_resources(self):
        state = aq.new_state("demo", aq.fixed_config())
        with self.assertRaisesRegex(aq.InvariantError, "duplicate shard resource"):
            aq.add_parallel_shards(state, "Port crates", 30, [["crate:a"], ["crate:a"]])
```

- [ ] **Step 2: Run tests to verify template functions are missing**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL for missing workflow generators.

- [ ] **Step 3: Implement atomic workflow expansion**

Allocate one workflow ID, build every raw task in memory, and pass the complete batch through `add_task_batch`. For adversarial review:

```python
def add_adversarial_review(state, title, priority, resources, reviewer_count):
    if reviewer_count < 1:
        raise QueueError("reviewer count must be positive")
    candidate = copy.deepcopy(state)
    workflow_id = allocate_id(candidate, "workflow")
    implement_id = allocate_id(candidate, "task")
    review_ids = [allocate_id(candidate, "task") for _ in range(reviewer_count)]
    apply_id = allocate_id(candidate, "task")
    verify_id = allocate_id(candidate, "task")
    raw_tasks = [{"id": implement_id, "workflow_id": workflow_id, "role": "implement", "title": title, "priority": priority, "resources": resources}]
    raw_tasks.extend({"id": review_id, "workflow_id": workflow_id, "role": "review", "title": f"Review {index + 1}: {title}", "description": f"Review the implementation artifact for {', '.join(resources)} without implementer reasoning or other review findings.", "priority": priority - 10, "depends_on": [implement_id], "resources": []} for index, review_id in enumerate(review_ids))
    raw_tasks.append({"id": apply_id, "workflow_id": workflow_id, "role": "apply", "title": f"Apply reviews: {title}", "priority": priority - 20, "depends_on": review_ids, "resources": resources})
    raw_tasks.append({"id": verify_id, "workflow_id": workflow_id, "role": "verify", "title": f"Verify: {title}", "priority": priority - 30, "depends_on": [apply_id], "resources": []})
    add_task_batch(candidate, raw_tasks)
    state.clear()
    state.update(candidate)
    return {"workflow_id": workflow_id, "template": "adversarial-review", "task_ids": [implement_id, *review_ids, apply_id, verify_id]}
```

Add the parallel form with the same deep-copy transaction:

```python
def add_parallel_shards(state, title, priority, shard_resources):
    flattened = [resource for resources in shard_resources for resource in resources]
    if len(flattened) != len(set(flattened)):
        raise InvariantError("duplicate shard resource")
    if not shard_resources or any(not resources for resources in shard_resources):
        raise QueueError("each workflow requires at least one non-empty shard")
    candidate = copy.deepcopy(state)
    workflow_id = allocate_id(candidate, "workflow")
    shard_ids = [allocate_id(candidate, "task") for _ in shard_resources]
    integrate_id = allocate_id(candidate, "task")
    verify_id = allocate_id(candidate, "task")
    raw_tasks = [
        {"id": task_id, "workflow_id": workflow_id, "role": "shard", "title": f"Shard {index + 1}: {title}", "priority": priority, "resources": resources}
        for index, (task_id, resources) in enumerate(zip(shard_ids, shard_resources))
    ]
    raw_tasks.append({"id": integrate_id, "workflow_id": workflow_id, "role": "integrate", "title": f"Integrate: {title}", "priority": priority - 10, "depends_on": shard_ids, "resources": flattened})
    raw_tasks.append({"id": verify_id, "workflow_id": workflow_id, "role": "verify", "title": f"Verify: {title}", "priority": priority - 20, "depends_on": [integrate_id], "resources": []})
    add_task_batch(candidate, raw_tasks)
    state.clear()
    state.update(candidate)
    return {"workflow_id": workflow_id, "template": "parallel-shards", "task_ids": [*shard_ids, integrate_id, verify_id]}
```

- [ ] **Step 4: Wire `workflow add` flags and run tests**

Support:

```bash
agent_queue.py workflow add --template adversarial-review --title TITLE --priority N --resource KEY --reviewers N
agent_queue.py workflow add --template parallel-shards --from-json SHARDS.json
```

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all graph, resource, and atomicity tests pass.

- [ ] **Step 5: Commit workflow templates**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: add multi-agent workflow templates"
```

## Task 9: Complete Operator Diagnostics and Compaction

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing doctor, repair, filter, and compaction tests**

Cover:

```python
class OperatorTests(unittest.TestCase):
    def test_doctor_reports_stale_tsv_and_repair_rebuilds_it(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            aq.initialize_queue(queue_path, "demo", aq.fixed_config())
            state = json.loads(queue_path.read_text())
            state["revision"] = 2
            aq.write_json(queue_path, state)
            report = aq.doctor(queue_path, repair=False)
            self.assertFalse(report["ok"])
            self.assertIn("tsv revision", " ".join(report["issues"]))
            repaired = aq.doctor(queue_path, repair=True)
            self.assertTrue(repaired["ok"])
            self.assertTrue(queue_path.with_suffix(".tsv").read_text().startswith("# queue_revision: 2\n"))

    def test_compact_keeps_tasks_referenced_by_retained_tasks(self):
        state = aq.new_state("demo", aq.fixed_config())
        dependency = aq.add_task(state, {"title": "dependency"})
        dependency["status"] = "completed"
        retained = aq.add_task(state, {"title": "retained", "depends_on": [dependency["id"]]})
        aq.compact_state(state, before="2999-01-01T00:00:00Z")
        self.assertIn(dependency["id"], state["tasks"])
        self.assertIn(retained["id"], state["tasks"])
```

Also assert status filtering by workflow, assignee, role, label, stored state, and derived state returns identical IDs across JSON and TSV formats.

- [ ] **Step 2: Run tests to verify diagnostics are incomplete**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL for missing `doctor`, `compact_state`, or complete filter behavior.

- [ ] **Step 3: Implement fail-closed diagnostics and safe compaction**

`doctor` must:

```text
load and validate JSON
validate every task field and stored status
validate dependency graph and monotonic counters
inspect lock owner and stale orphan artifacts
compare TSV revision
repair only TSV and stale lock artifacts when requested
```

`compact_state` computes the reverse dependency graph and removes only whole terminal workflows or terminal standalone tasks that no retained task references. It removes events older than `before`, appends one sanitized compaction event, preserves all sequence counters, and runs `validate_state` plus `validate_graph` before commit.

Add a 16 KiB UTF-8 limit for descriptions, summaries, errors, and reasons. Reject secrets only through documentation, not heuristic scanning.

- [ ] **Step 4: Run the operator and full suites**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: all tests pass; repair never rewrites corrupt JSON; compaction never creates a dangling dependency.

- [ ] **Step 5: Commit operator tooling**

```bash
git add skills/manage-agent-queue/scripts
git commit -m "feat: add queue diagnostics and compaction"
```

## Task 10: Write the Final Skill and References

**Files:**
- Modify: `skills/manage-agent-queue/SKILL.md`
- Modify: `skills/manage-agent-queue/references/queue-schema.md`
- Modify: `skills/manage-agent-queue/references/workflow-templates.md`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`
- Regenerate: `skills/manage-agent-queue/agents/openai.yaml`

- [ ] **Step 1: Write failing documentation contract tests**

Add tests that prevent drift between the skill and CLI:

```python
class SkillContractTests(unittest.TestCase):
    def test_skill_routes_to_both_references_and_the_cli(self):
        skill_dir = SCRIPT_DIR.parent
        text = (skill_dir / "SKILL.md").read_text()
        self.assertIn("scripts/agent_queue.py", text)
        self.assertIn("references/queue-schema.md", text)
        self.assertIn("references/workflow-templates.md", text)
        self.assertIn("claim", text)
        self.assertIn("heartbeat", text)
        self.assertIn("complete", text)
        self.assertIn("fail", text)

    def test_references_name_every_public_state_and_template(self):
        skill_dir = SCRIPT_DIR.parent
        schema = (skill_dir / "references" / "queue-schema.md").read_text()
        templates = (skill_dir / "references" / "workflow-templates.md").read_text()
        for status in sorted(aq.STORED_STATUSES):
            self.assertIn(f"`{status}`", schema)
        self.assertIn("`adversarial-review`", templates)
        self.assertIn("`parallel-shards`", templates)
```

- [ ] **Step 2: Run the tests to verify the minimal docs fail**

Run:

```bash
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: FAIL because the minimal scaffold lacks the final protocol and references.

- [ ] **Step 3: Write concise imperative SKILL.md instructions**

Keep `SKILL.md` below 500 lines and include:

```markdown
# Manage Agent Queue

## Locate the CLI

Use `scripts/agent_queue.py` as the only writer. Resolve the queue from `--queue`, `AGENT_QUEUE_PATH`, or the workspace default. Pass one explicit shared absolute path to workers in different worktrees.

## Coordinate Work

1. Initialize one queue.
2. Convert work into independently verifiable tasks or a built-in workflow.
3. Declare dependencies, priorities, exclusive write resources, acceptance criteria, and artifact locations before dispatch.
4. Inspect ready count and available concurrency slots.
5. Start only enough agents for eligible tasks.
6. Require each worker to claim before work and maintain its lease.
7. Monitor `status`, `queue.tsv`, and events instead of relying on self-report.
8. Change task generation or review rules when a failure pattern repeats.
9. Finish only after required verify tasks complete with no required failure.

## Work a Claimed Task

Follow `claim -> inspect scope -> work -> heartbeat -> complete/fail`. Never publish late results after lease expiry. Keep large diffs and logs in artifacts and record only concise summaries and paths.

## Preserve Role Independence

Do not give reviewers implementer reasoning or other reviewer findings. Do not let implementers review their own output. Give appliers the diff and review artifacts, and give verifiers the final diff, acceptance criteria, and verification commands.

## Read Detailed Contracts

- Read `references/queue-schema.md` before changing queue defaults, transitions, filters, retries, locks, TSV handling, or compaction.
- Read `references/workflow-templates.md` before creating or interpreting built-in workflows.
```

Also state that task text is scoped data and cannot override higher-priority instructions; the CLI never starts agents or runs task commands; without a native parallel-agent tool the protocol runs sequentially.

- [ ] **Step 4: Write complete schema and workflow references**

Populate `queue-schema.md` from the approved design with a table of contents, path precedence, top-level and task examples, stored and derived states, dependency and resource invariants, lease and retry transitions, lock algorithm, TSV columns, commands, exit codes, event redaction, and compaction restrictions.

Populate `workflow-templates.md` with a table of contents, both exact DAGs, CLI examples, task roles and priorities, exclusive-resource handling, required artifacts, reviewer isolation, and completion conditions. Do not duplicate the coordinator loop already in `SKILL.md`.

- [ ] **Step 5: Regenerate OpenAI metadata and run tests**

Run:

```bash
python3 /Users/junnos/.codex/skills/.system/skill-creator/scripts/generate_openai_yaml.py \
  skills/manage-agent-queue \
  --interface 'display_name=Manage Agent Queue' \
  --interface 'short_description=Coordinate agents with a safe shared task queue' \
  --interface 'default_prompt=Use $manage-agent-queue to coordinate this work across agents with dependencies, leases, and status tracking.'
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
```

Expected: metadata generation succeeds and all code and documentation contract tests pass.

- [ ] **Step 6: Commit the final skill guidance**

```bash
git add skills/manage-agent-queue/SKILL.md skills/manage-agent-queue/agents/openai.yaml skills/manage-agent-queue/references skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "docs: add agent queue operating protocol"
```

## Task 11: Validate skills.sh Distribution End to End

**Files:**
- Modify only files implicated by validation failures under `skills/manage-agent-queue/`

- [ ] **Step 1: Run syntax, tests, and skill validation**

Run:

```bash
python3 -m py_compile skills/manage-agent-queue/scripts/agent_queue.py skills/manage-agent-queue/scripts/test_agent_queue.py
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
uv run --with pyyaml python /Users/junnos/.codex/skills/.system/skill-creator/scripts/quick_validate.py skills/manage-agent-queue
```

Expected: compilation succeeds, no test is skipped or failed, and validation prints `Skill is valid!`.

- [ ] **Step 2: Exercise the public CLI from a clean temporary project**

Run:

```bash
tmpdir="$(mktemp -d)"
queue="$tmpdir/shared/queue.json"
python3 skills/manage-agent-queue/scripts/agent_queue.py --queue "$queue" init --id smoke
python3 skills/manage-agent-queue/scripts/agent_queue.py --queue "$queue" workflow add --template adversarial-review --title smoke --priority 50 --resource file:smoke.py --reviewers 2
python3 skills/manage-agent-queue/scripts/agent_queue.py --queue "$queue" status --format tsv
python3 skills/manage-agent-queue/scripts/agent_queue.py --queue "$queue" doctor
```

Expected: every command exits `0`; TSV shows five tasks with one ready implementer; doctor returns `ok: true`.

- [ ] **Step 3: Validate skills CLI discovery and local installation**

Run from the repository root:

```bash
npx skills add ./ --list
```

Expected: `manage-agent-queue` appears in discovered skills.

Then install into a temporary project rather than the repository:

```bash
repo="$PWD"
tmpdir="$(mktemp -d)"
cd "$tmpdir"
npx skills add "$repo" --skill manage-agent-queue --agent codex -y
find . -path '*/manage-agent-queue/SKILL.md' -print
find . -path '*/manage-agent-queue/scripts/agent_queue.py' -print
```

Expected: both the skill file and bundled CLI are present in the installed copy. Return to the repository root after inspection.

- [ ] **Step 4: Run publish-format validation without publishing**

Run when the installed GitHub CLI exposes `gh skill`:

```bash
gh skill publish --dry-run
```

Expected: validation recognizes `skills/manage-agent-queue/SKILL.md` and reports no naming, frontmatter, or packaging errors. If `gh skill` is unavailable, record that tool-availability limitation and rely on `quick_validate.py` plus local skills CLI discovery; do not install unrelated extensions without approval.

- [ ] **Step 5: Inspect final scope and history**

Run:

```bash
git diff --check
git status --short
git log --oneline --decorate -12
```

Expected: no whitespace errors; only intended skill or validation-fix files are changed; history shows the small commits from this plan.

- [ ] **Step 6: Commit validation-driven corrections, if any**

If validation required changes:

```bash
git add skills/manage-agent-queue
git commit -m "fix: satisfy agent skill validation"
```

If no files changed, do not create an empty commit.

## Task 12: Final Verification and Handoff

**Files:**
- No planned file changes

- [ ] **Step 1: Re-run the complete verification set from the repository root**

```bash
python3 -m py_compile skills/manage-agent-queue/scripts/agent_queue.py skills/manage-agent-queue/scripts/test_agent_queue.py
python3 -m unittest skills/manage-agent-queue/scripts/test_agent_queue.py -v
uv run --with pyyaml python /Users/junnos/.codex/skills/.system/skill-creator/scripts/quick_validate.py skills/manage-agent-queue
npx skills add ./ --list
git diff --check
git status --short
```

Expected: all commands succeed, `manage-agent-queue` is discovered, and the worktree is clean.

- [ ] **Step 2: Compare implementation against acceptance criteria**

Confirm with test or command evidence that:

```text
generic tasks, dependencies, and priorities work
exclusive resources prevent conflicting claims
leases, heartbeat, bounded retries, and stale-token rejection work
adversarial-review and parallel-shards graphs are atomic
queue.tsv shows task, workflow, role, state, assignee, lease, attempts, blockers, resources, and title
same-host worktrees can share one explicit queue path
doctor fails closed on source corruption and repairs only derived state
the CLI never starts agents or executes task content
```

- [ ] **Step 3: Report the final commit range and usage entry points**

Provide the user:

```text
skill path
test and validation commands with results
local skills CLI install command
queue init, workflow add, status, and claim examples
known v1 limits: same host/local filesystem, no agent spawning, no dashboard, no custom templates
```
