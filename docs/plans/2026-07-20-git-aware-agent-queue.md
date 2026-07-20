# Git-Aware Agent Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This repository's `AGENTS.md` requires sequential work in the main thread; do not dispatch subagents.

**Goal:** Add opt-in Git-aware claims and completion validation so cooperating agents can commit concurrently only from isolated worktrees, branches, and declared path scopes.

**Architecture:** Keep queue schema, transitions, and CLI orchestration in `agent_queue.py`; add `git_queue.py` for shell-free Git observation, typed path scope logic, and commit evidence calculation. Schema version 2 stores compact bindings and results, while the same CLI continues to mutate version-1 generic queues until an explicit atomic migration enables Git-aware tasks.

**Tech Stack:** Python 3 standard library (`argparse`, `copy`, `hashlib`, `json`, `os`, `pathlib`, `subprocess`), Git CLI, file-backed queue locking, and `unittest` with temporary repositories and real Git worktrees.

---

## File Map

- Create `skills/manage-agent-queue/scripts/git_queue.py`: typed resource parsing and overlap, safe Git subprocess calls, clean attached-worktree observation, claim snapshot comparison, commit ancestry/path validation, and compact evidence.
- Create `skills/manage-agent-queue/scripts/test_git_queue.py`: focused unit and real-repository tests for Git observation, typed paths, ancestry, bounded failures, and worktree identity.
- Modify `skills/manage-agent-queue/scripts/agent_queue.py`: dual schema validation, explicit migration, Git task/workflow inputs, claim binding, derived ownership, compact completion results, expiry recovery, redaction, parser flags, and CLI orchestration.
- Modify `skills/manage-agent-queue/scripts/test_agent_queue.py`: version compatibility, migration, workflow role assignment, queue transitions, CLI claim/complete/recovery, bounded projections, and documentation contracts.
- Modify `skills/manage-agent-queue/SKILL.md`: isolated-worktree preparation, Git-aware claim/heartbeat/complete rules, recovery, and publication boundary.
- Modify `skills/manage-agent-queue/references/queue-schema.md`: schema version 2, typed resources, Git binding/result/recovery shapes, migration, derived state, command flags, errors, and privacy.
- Modify `skills/manage-agent-queue/references/workflow-templates.md`: Git-enabled adversarial-review and parallel-shards role assignment and artifact flow.
- Modify `README.md`: concise Git-aware quick start, safety boundary, explicit dashboard command, and v1 migration note.
- Reference `docs/specs/2026-07-20-git-aware-agent-queue-design.md`: approved behavior and acceptance criteria; change it only to resolve a verified contradiction.

## Baseline

The pre-feature suite currently runs 225 tests with one existing failure: `SkillContractTests.test_dashboard_requires_consent_fallback_and_cleanup` expects the public README to contain `$CLI serve --open`, but the latest README rewrite removed that explicit command. Task 7 restores the concrete dashboard invocation while adding the Git-aware quick start. Do not weaken or delete the contract assertion.

## Task 1: Isolate Git Observation and Typed Path Logic

**Files:**
- Create: `skills/manage-agent-queue/scripts/git_queue.py`
- Create: `skills/manage-agent-queue/scripts/test_git_queue.py`

- [ ] **Step 1: Write failing typed-path and repository-observation tests**

Create `test_git_queue.py` with a reusable real-repository fixture and the first contracts:

```python
#!/usr/bin/env python3
"""Tests for Git-aware queue helpers."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import git_queue as gq


def run_git(cwd, *arguments, check=True):
    return subprocess.run(
        ["git", "-C", str(cwd), *arguments],
        check=check,
        capture_output=True,
        text=True,
    )


class GitRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        run_git(self.root, "init", "-b", "main")
        run_git(self.root, "config", "user.name", "Queue Tests")
        run_git(self.root, "config", "user.email", "queue@example.test")
        (self.root / "README.md").write_text("base\n", encoding="utf-8")
        run_git(self.root, "add", "README.md")
        run_git(self.root, "commit", "-m", "base")


class TypedPathResourceTests(unittest.TestCase):
    def test_parse_accepts_canonical_file_and_directory_resources(self):
        self.assertEqual(
            gq.PathScope("file", "src/api.py"),
            gq.parse_path_resource("file:src/api.py"),
        )
        self.assertEqual(
            gq.PathScope("dir", "tests/api/"),
            gq.parse_path_resource("dir:tests/api/"),
        )

    def test_parse_rejects_unsafe_or_noncanonical_paths(self):
        for value in (
            "file:/tmp/a", "file:../a", "file:a/./b", "file:a//b",
            "file:a\\b", "file:", "dir:src", "dir:src/../tests/",
        ):
            with self.subTest(value=value):
                with self.assertRaises(gq.GitContextError):
                    gq.parse_path_resource(value)

    def test_overlap_respects_file_and_directory_boundaries(self):
        cases = (
            ("file:src/a.py", "file:src/a.py", True),
            ("file:src/a.py", "file:src/ab.py", False),
            ("dir:src/", "file:src/a.py", True),
            ("dir:src/api/", "dir:src/api/internal/", True),
            ("dir:src/api/", "dir:src/apis/", False),
        )
        for left, right, expected in cases:
            with self.subTest(left=left, right=right):
                self.assertEqual(expected, gq.resources_overlap([left], [right]))


class GitObservationTests(GitRepositoryTestCase):
    def test_observe_returns_private_paths_and_stable_public_identity(self):
        observed = gq.observe(self.root)

        self.assertEqual(str(self.root.resolve()), observed["worktree"])
        self.assertEqual("refs/heads/main", observed["branch"])
        self.assertEqual(
            run_git(self.root, "rev-parse", "HEAD").stdout.strip(),
            observed["head"],
        )
        self.assertRegex(observed["repository_id"], r"^[0-9a-f]{64}$")
        self.assertRegex(observed["worktree_id"], r"^[0-9a-f]{64}$")
        self.assertTrue(observed["clean"])
        self.assertTrue(observed["attached"])

    def test_observe_reports_dirty_and_detached_without_losing_identity(self):
        (self.root / "dirty.txt").write_text("dirty\n", encoding="utf-8")
        dirty = gq.observe(self.root)
        self.assertFalse(dirty["clean"])

        (self.root / "dirty.txt").unlink()
        run_git(self.root, "checkout", "--detach")
        detached = gq.observe(self.root)
        self.assertFalse(detached["attached"])
        self.assertIsNone(detached["branch"])
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run from `skills/manage-agent-queue/scripts`:

```bash
python3 -m unittest test_git_queue.TypedPathResourceTests \
  test_git_queue.GitObservationTests -v
```

Expected: import failure for `git_queue`.

- [ ] **Step 3: Implement the focused Git helper boundary**

Create `git_queue.py` with this initial public interface. Keep subprocess calls argument-vector based and return observations rather than queue-specific task state:

```python
#!/usr/bin/env python3
"""Observe and validate Git state for the local agent queue."""

import hashlib
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


MAX_OFFENDING_PATHS = 10


class GitContextError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PathScope:
    kind: str
    path: str


def _digest(value):
    return hashlib.sha256(os.fsencode(value)).hexdigest()


def _git(cwd, *arguments, allowed=(0,)):
    try:
        completed = subprocess.run(
            ["git", "-C", os.fspath(cwd), *arguments],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise GitContextError("git_unavailable", "cannot execute git") from error
    if completed.returncode not in allowed:
        message = completed.stderr.decode("utf-8", "replace").strip()
        raise GitContextError("git_command_failed", message or "git command failed")
    return completed.stdout


def parse_path_resource(resource):
    if not isinstance(resource, str) or ":" not in resource:
        raise GitContextError("git_path_resource", "invalid typed path resource")
    kind, path = resource.split(":", 1)
    if kind not in {"file", "dir"} or not path or "\\" in path or "\x00" in path:
        raise GitContextError("git_path_resource", f"invalid path resource: {resource}")
    if path.startswith("/") or any(part in {"", ".", ".."} for part in path.split("/")):
        if not (kind == "dir" and path.endswith("/") and all(
            part not in {"", ".", ".."} for part in path[:-1].split("/")
        )):
            raise GitContextError("git_path_resource", f"noncanonical path resource: {resource}")
    if kind == "dir" and not path.endswith("/"):
        raise GitContextError("git_path_resource", "dir resource must end with /")
    if kind == "file" and path.endswith("/"):
        raise GitContextError("git_path_resource", "file resource must name a file")
    PurePosixPath(path.rstrip("/"))
    return PathScope(kind, path)


def path_scopes(resources):
    scopes = []
    for resource in resources:
        if resource.startswith(("file:", "dir:")):
            scopes.append(parse_path_resource(resource))
    return scopes


def resources_overlap(left_resources, right_resources):
    for left in path_scopes(left_resources):
        for right in path_scopes(right_resources):
            if left.kind == right.kind == "file":
                overlaps = left.path == right.path
            elif left.kind == "dir" and right.kind == "dir":
                overlaps = left.path.startswith(right.path) or right.path.startswith(left.path)
            else:
                directory = left if left.kind == "dir" else right
                file_scope = right if left.kind == "dir" else left
                overlaps = file_scope.path.startswith(directory.path)
            if overlaps:
                return True
    return False


def observe(cwd):
    try:
        worktree = Path(_git(cwd, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    except GitContextError as error:
        raise GitContextError("git_not_worktree", "current directory is not a Git worktree") from error
    common_raw = _git(worktree, "rev-parse", "--git-common-dir").decode().strip()
    common_dir = (worktree / common_raw).resolve() if not os.path.isabs(common_raw) else Path(common_raw).resolve()
    branch_result = subprocess.run(
        ["git", "-C", os.fspath(worktree), "symbolic-ref", "-q", "HEAD"],
        check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    branch = branch_result.stdout.decode().strip() if branch_result.returncode == 0 else None
    head = _git(worktree, "rev-parse", "HEAD").decode().strip()
    dirty = bool(_git(worktree, "status", "--porcelain=v1", "-z", "--untracked-files=all"))
    return {
        "common_dir": str(common_dir),
        "worktree": str(worktree),
        "repository_id": _digest(str(common_dir)),
        "worktree_id": _digest(str(worktree)),
        "branch": branch,
        "head": head,
        "attached": branch is not None,
        "clean": not dirty,
    }
```

During implementation, simplify `parse_path_resource` if tests expose an unclear branch; preserve its exact accepted and rejected forms rather than broadening them.

- [ ] **Step 4: Run focused tests and syntax validation**

```bash
python3 -m unittest test_git_queue.TypedPathResourceTests \
  test_git_queue.GitObservationTests -v
python3 -m py_compile git_queue.py test_git_queue.py
```

Expected: all focused tests pass and `py_compile` exits 0.

- [ ] **Step 5: Commit the Git helper boundary**

```bash
git add skills/manage-agent-queue/scripts/git_queue.py \
  skills/manage-agent-queue/scripts/test_git_queue.py
git commit -m "feat: Git 작업 환경 관찰 경계 추가"
```

## Task 2: Introduce Schema Version 2 and Explicit Migration

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:42-102,554-850,1147-1376,3201-3285,3422-3520,3607-3813`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:220-360,3000-3120,4000-4800`

- [ ] **Step 1: Write failing version compatibility and migration tests**

Add `SchemaMigrationTests` after `QueueStateTests`:

```python
class SchemaMigrationTests(unittest.TestCase):
    NOW = "2026-07-20T00:00:00Z"

    def legacy_state(self):
        state = aq.new_state("legacy", aq.fixed_config())
        state["schema_version"] = 1
        for task in state["tasks"].values():
            task.pop("git_mode", None)
            task.pop("git_recovery", None)
        return state

    def test_new_queues_use_schema_two_with_git_defaults(self):
        state = aq.new_state("demo", aq.fixed_config())
        task = aq.add_task(state, {"title": "generic"})

        self.assertEqual(2, state["schema_version"])
        self.assertIsNone(task["git_mode"])
        self.assertIsNone(task["git_recovery"])

    def test_version_one_queue_keeps_generic_mutation_semantics(self):
        state = self.legacy_state()

        task = aq.add_task(state, {"title": "legacy generic"})

        self.assertEqual(1, state["schema_version"])
        self.assertNotIn("git_mode", task)
        aq.validate_state(state)

    def test_migrate_to_two_adds_defaults_and_one_event_atomically(self):
        state = self.legacy_state()
        task = aq.add_task(state, {"title": "legacy generic"})

        result = aq.migrate_state(state, 2, now=self.NOW)

        self.assertEqual({"from": 1, "to": 2}, result)
        self.assertEqual(2, state["schema_version"])
        self.assertIsNone(state["tasks"][task["id"]]["git_mode"])
        self.assertIsNone(state["tasks"][task["id"]]["git_recovery"])
        self.assertEqual("queue.migrated", state["events"][-1]["type"])

    def test_failed_migration_leaves_source_unchanged(self):
        state = self.legacy_state()
        state["tasks"]["bad"] = {}
        before = copy.deepcopy(state)

        with self.assertRaises(aq.InvariantError):
            aq.migrate_state(state, 2, now=self.NOW)

        self.assertEqual(before, state)
```

Add a CLI test that initializes v2, loads a hand-written valid v1 queue, mutates it generically, migrates it, and proves migration increments one revision and replaces JSON before TSV.

- [ ] **Step 2: Run schema tests and verify RED**

```bash
python3 -m unittest test_agent_queue.QueueStateTests \
  test_agent_queue.SchemaMigrationTests -v
```

Expected: failures because new queues still use schema 1 and `migrate_state` does not exist.

- [ ] **Step 3: Add dual-schema constants and validators**

Replace the single-version constants with version-indexed field sets:

```python
CURRENT_SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = {1, 2}
TASK_FIELDS_V1 = frozenset({...existing TASK_FIELDS...})
TASK_FIELDS_V2 = TASK_FIELDS_V1 | {"git_mode", "git_recovery"}
TASK_CREATION_FIELDS_V1 = frozenset({...existing TASK_CREATION_FIELDS...})
TASK_CREATION_FIELDS_V2 = TASK_CREATION_FIELDS_V1 | {"git_mode"}
CLAIM_FIELDS_V1 = frozenset({...existing CLAIM_FIELDS...})
CLAIM_FIELDS_V2 = CLAIM_FIELDS_V1 | {"git"}
```

Change `validate_task` to accept the owning schema version and validate exact fields for that version:

```python
def validate_task(task, expected_id=None, schema_version=CURRENT_SCHEMA_VERSION):
    task_fields = TASK_FIELDS_V1 if schema_version == 1 else TASK_FIELDS_V2
    claim_fields = CLAIM_FIELDS_V1 if schema_version == 1 else CLAIM_FIELDS_V2
    ...
```

Version 2 validation must enforce:

- `git_mode` is `None` or `"commit"`.
- `git_recovery` is `None` unless `git_mode == "commit"` and status is `pending` or `failed`.
- A Git-aware live claim has exact private Git binding fields.
- Generic claims use `git: null`; Git-aware claims use a binding object.
- Completed generic results retain `summary` and `artifacts`; Git-aware results add exact `git` evidence.
- Version 1 continues to reject version-2 fields and mutate with its original shape.

- [ ] **Step 4: Implement detached explicit migration and CLI routing**

Add:

```python
def migrate_state(state, target_version, now=None):
    validate_state(state)
    if state["schema_version"] != 1 or target_version != 2:
        raise InvariantError("only schema version 1 can migrate to 2")
    now = _canonical_now(now)
    candidate = copy.deepcopy(state)
    for task in candidate["tasks"].values():
        task["git_mode"] = None
        task["git_recovery"] = None
        if task["status"] == "leased":
            task["claim"]["git"] = None
    candidate["schema_version"] = 2
    _append_event_to_candidate(
        candidate, "queue.migrated", "operator", None,
        {"from": 1, "to": 2}, now,
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return {"from": 1, "to": 2}
```

Add parser and command routing:

```python
migrate = commands.add_parser("migrate", help="migrate queue schema")
migrate.add_argument("--to", type=int, choices=(2,), required=True)
```

Route it through `mutate_queue(..., auto_sweep=False)` so the existing queue lock, detached candidate, revision increment, JSON-first replacement, and TSV regeneration remain the only write path.

- [ ] **Step 5: Run schema, persistence, lock, and migration tests**

```bash
python3 -m unittest \
  test_agent_queue.QueueStateTests \
  test_agent_queue.SchemaMigrationTests \
  test_agent_queue.QueuePersistenceTests \
  test_agent_queue.QueueLockTests -v
```

Expected: selected tests pass for both versions. Update exact version assertions rather than weakening them.

- [ ] **Step 6: Commit schema version 2 and migration**

```bash
git add skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: 큐 스키마 v2 마이그레이션 추가"
```

## Task 3: Add Git-Aware Task and Workflow Definitions

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:554-633,988-1145,3367-3421,3433-3469,3640-3710`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:362-980,988-1150,3900-4300`

- [ ] **Step 1: Write failing creation and workflow-role tests**

Add tests proving:

```python
def test_git_task_requires_v2_and_a_typed_path_resource(self):
    state = aq.new_state("demo", aq.fixed_config())
    with self.assertRaisesRegex(aq.InvariantError, "file: or dir:"):
        aq.add_task(state, {"title": "git", "git_mode": "commit", "resources": ["scope:auth"]})

def test_git_enabled_adversarial_review_marks_only_writer_roles(self):
    state = aq.new_state("demo", aq.fixed_config())
    result = aq.add_adversarial_review(
        state, "Change", 20, ["dir:src/"], 2,
        git_commit=True, now="2026-07-20T00:00:00Z",
    )
    tasks = [state["tasks"][task_id] for task_id in result["task_ids"]]
    self.assertEqual(
        ["commit", None, None, "commit", None],
        [task["git_mode"] for task in tasks],
    )

def test_git_enabled_parallel_shards_marks_shards_and_integrator(self):
    state = aq.new_state("demo", aq.fixed_config())
    result = aq.add_parallel_shards(
        state, "Build", 20,
        [["file:src/a.py"], ["dir:src/b/"]],
        git_commit=True, now="2026-07-20T00:00:00Z",
    )
    tasks = [state["tasks"][task_id] for task_id in result["task_ids"]]
    self.assertEqual(
        ["commit", "commit", "commit", None],
        [task["git_mode"] for task in tasks],
    )
```

Add parser tests for `task add --git-commit`, adversarial `workflow add --git-commit`, and `parallel-shards` JSON field `"git_commit": true`. Assert version-1 queues reject every Git opt-in without changing revision or files.

- [ ] **Step 2: Run creation/workflow tests and verify RED**

```bash
python3 -m unittest test_agent_queue.TaskGraphTests \
  test_agent_queue.WorkflowTemplateTests \
  test_agent_queue.QueueCliTests -v
```

Expected: parser or signature failures for Git opt-in inputs.

- [ ] **Step 3: Normalize Git task definitions from one source of truth**

Import `git_queue as gq`. In `normalize_task`, select creation fields from the queue version, normalize `git_mode`, and validate typed scopes:

```python
git_mode = raw.get("git_mode") if state["schema_version"] == 2 else None
if raw.get("git_mode") is not None and state["schema_version"] == 1:
    raise InvariantError("Git-aware tasks require schema version 2")
if git_mode not in (None, "commit"):
    raise InvariantError("task git_mode must be commit or null")
if git_mode == "commit":
    try:
        scopes = gq.path_scopes(resources)
    except gq.GitContextError as error:
        raise InvariantError(str(error)) from error
    if not scopes:
        raise InvariantError("Git-aware task requires a file: or dir: resource")
```

Version-2 tasks receive `"git_mode": git_mode` and `"git_recovery": None`; version-1 tasks keep their exact old shape.

- [ ] **Step 4: Thread Git opt-in through workflow constructors and CLI inputs**

Use explicit booleans:

```python
def add_adversarial_review(
    state, title, priority, resources, reviewer_count,
    git_commit=False, now=None,
):
    writer_mode = "commit" if git_commit else None
    # add git_mode only to implement and apply raw tasks

def add_parallel_shards(
    state, title, priority, shard_resources,
    git_commit=False, now=None,
):
    writer_mode = "commit" if git_commit else None
    # add git_mode only to shards and integrate raw task
```

Add `--git-commit` to task add and adversarial workflow input. Extend `_parallel_workflow_input` allowed keys with `git_commit`, require a boolean, and pass it to `add_parallel_shards`. Include `git_commit` in workflow-created event details without duplicating path lists.

- [ ] **Step 5: Run creation/workflow tests**

```bash
python3 -m unittest test_agent_queue.TaskGraphTests \
  test_agent_queue.WorkflowTemplateTests \
  test_agent_queue.QueueCliTests -v
```

Expected: all selected tests pass; existing non-Git workflow shapes remain exact except for schema-v2 `git_mode` defaults.

- [ ] **Step 6: Commit Git-aware task definitions**

```bash
git add skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: Git-aware 큐 작업 정의 추가"
```

## Task 4: Bind Git Ownership During Claim

**Files:**
- Modify: `skills/manage-agent-queue/scripts/git_queue.py`
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:1495-1670,2184-2244,3302-3310,3471-3475,3730-3760`
- Modify: `skills/manage-agent-queue/scripts/test_git_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:1280-1775,2200-2450,4300-4700`

- [ ] **Step 1: Write failing claimability, conflict, and redaction tests**

Add tests that construct two real worktrees and assert:

- Dirty and detached contexts cannot claim a targeted Git task.
- A Git claim stores full private binding internally but `_safe_task`, status, TSV, events, and dashboard snapshots omit raw paths and tokens.
- Same worktree or same branch ownership conflicts.
- Same repository plus overlapping `file:`/`dir:` scopes conflicts.
- Disjoint scopes in different worktrees and branches can lease concurrently.
- Generic tasks can still claim outside a Git repository.
- `claim --task` returns a precise Git context error for its targeted Git task.
- Normal claim skips Git tasks incompatible with the current context and preserves priority ordering among compatible candidates.

Use a canonical test snapshot helper:

```python
def git_snapshot(repository="repo-1", worktree="wt-1", branch="refs/heads/a", head="a1"):
    return {
        "common_dir": "/private/repo/.git",
        "worktree": "/private/repo-wt",
        "repository_id": repository,
        "worktree_id": worktree,
        "branch": branch,
        "head": head,
        "attached": True,
        "clean": True,
    }
```

- [ ] **Step 2: Run claim tests and verify RED**

```bash
python3 -m unittest test_agent_queue.ClaimTaskTests \
  test_git_queue.GitObservationTests -v
```

Expected: `claim_task` does not accept a Git observation or derived ownership.

- [ ] **Step 3: Add claim binding and cooperative conflict selection**

Extend the in-memory transition without running subprocesses inside it:

```python
def claim_task(
    state, agent_id, now=None, role=None, labels=None, lease_seconds=None,
    git_observation=None, task_id=None, resume_git=False,
):
    ...
```

For a generic candidate, preserve current eligibility. For a Git-aware candidate:

1. Require a claimable observation (`attached` and `clean`).
2. Reject `git_recovery` unless `resume_git` is true.
3. Compare live Git bindings for same worktree ID, same repository/branch, and same-repository typed-path overlap.
4. Store the private binding under `claim["git"]` with `base` equal to the observed head.
5. Emit only repository/worktree/branch digests and attempt counts, never raw paths.

Add `git_recovery` to derived states after dependency and retry checks but before ordinary resource conflicts.

- [ ] **Step 4: Orchestrate pre-claim observation and post-claim drift release**

Add `--task` and `--resume-git` to `claim`. `_run_command` should call `gq.observe(Path.cwd())` before `mutate_queue`, but treat observation failure as `None` for untargeted generic claims. A targeted Git task converts `GitContextError` into a bounded `QueueError`.

After a Git claim commits, re-observe and compare repository, worktree, branch, head, attached state, and cleanliness. On mismatch, use the returned token to run `release_task` in a second queue transaction and return `git_claim_drift`. Do not execute Git while `QueueLock` is held.

- [ ] **Step 5: Run claim, status, dashboard, and concurrency tests**

```bash
python3 -m unittest \
  test_agent_queue.ClaimTaskTests \
  test_agent_queue.StatusProjectionTests \
  test_agent_queue.DashboardProjectionTests \
  test_git_queue.GitObservationTests -v
```

Expected: selected tests pass, private paths and lease tokens never appear in projections, and the existing exact-resource fast path remains for generic tasks.

- [ ] **Step 6: Commit claim-time Git ownership**

```bash
git add skills/manage-agent-queue/scripts/git_queue.py \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_git_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: claim에 Git worktree 소유권 바인딩"
```

## Task 5: Validate Completion and Persist Compact Commit Evidence

**Files:**
- Modify: `skills/manage-agent-queue/scripts/git_queue.py`
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:673-850,1780-1833,3302-3310,3487-3489,3760-3785`
- Modify: `skills/manage-agent-queue/scripts/test_git_queue.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:1775-1900,4300-4800`

- [ ] **Step 1: Write failing real-commit validation tests**

Add real-repository tests for:

- Two descendant commits succeed and return full base/head IDs, `commit_count == 2`, and a count only.
- Wrong HEAD, non-descendant commit, dirty worktree, and a changed path outside scope fail.
- `dir:src/` does not allow `src-other/a.py`.
- Rename source and destination are both checked by diffing with `--no-renames`.
- More than 10 violating paths produces 10 displayed paths and an omitted count.
- No-change succeeds only at the original clean head.

Assert compact evidence exactly:

```python
self.assertEqual(
    {
        "branch": "refs/heads/feature/a",
        "base": base,
        "head": head,
        "commit_count": 2,
        "changed_path_count": 27,
    },
    evidence,
)
self.assertNotIn("changed_paths", evidence)
```

- [ ] **Step 2: Run completion tests and verify RED**

```bash
python3 -m unittest test_git_queue.GitCompletionTests \
  test_agent_queue.LifecycleTransitionTests -v
```

Expected: missing Git completion validator and unsupported completion evidence.

- [ ] **Step 3: Implement commit and no-change validation in `git_queue.py`**

Add:

```python
def validate_completion(binding, resources, commit=None, no_change=False):
    current = observe(binding["worktree"])
    assert_same_binding(binding, current, require_same_head=False)
    require_claimable(current)
    base = binding["base"]
    if no_change:
        if commit is not None or current["head"] != base:
            raise GitContextError("git_head_mismatch", "no-change requires HEAD at base")
        return compact_evidence(binding["branch"], base, base, 0, 0)
    if commit is None:
        raise GitContextError("git_commit_required", "Git-aware completion requires --commit or --no-change")
    head = _git(binding["worktree"], "rev-parse", "--verify", f"{commit}^{{commit}}").decode().strip()
    if current["head"] != head:
        raise GitContextError("git_head_mismatch", "current HEAD does not match --commit")
    if subprocess.run(
        ["git", "-C", binding["worktree"], "merge-base", "--is-ancestor", base, head],
        check=False,
    ).returncode != 0 or base == head:
        raise GitContextError("git_non_descendant", "result must descend from and advance the claimed base")
    changed = _git(
        binding["worktree"], "diff", "--name-only", "-z", "--no-renames", base, head,
    ).decode("utf-8").split("\x00")
    changed = [path for path in changed if path]
    offenders = [path for path in changed if not path_is_allowed(path, resources)]
    if offenders:
        raise path_scope_error(offenders)
    count = int(_git(binding["worktree"], "rev-list", "--count", f"{base}..{head}").decode())
    return compact_evidence(binding["branch"], base, head, count, len(changed))
```

Implement `path_is_allowed`, `path_scope_error`, `assert_same_binding`, `require_claimable`, and `compact_evidence` beside it. Error strings must expose no raw worktree/common-dir paths and cap offenders at `MAX_OFFENDING_PATHS`.

- [ ] **Step 4: Thread evidence through queue completion and CLI**

Add mutually exclusive parser flags:

```python
completion = complete.add_mutually_exclusive_group()
completion.add_argument("--commit")
completion.add_argument("--no-change", action="store_true")
```

Before the queue mutation, read a consistent task snapshot, verify the live token, and call `gq.validate_completion` only for Git-aware tasks. Pass the detached evidence to:

```python
def complete_task(
    state, task_id, agent_id, token, summary, artifacts,
    git_evidence=None, now=None,
):
    ...
```

Version-2 results always store exact keys `summary`, `artifacts`, and `git`; generic results use `git: null`, while Git-aware results use compact evidence. Version-1 results retain exact keys `summary` and `artifacts`. `task.completed` event details store only artifact, commit, and changed-path counts. `_safe_task` must never add a changed-path list.

Re-observe after the queue commit. If drift is detected, return the completed result plus a top-level bounded warning; never reopen the terminal task.

- [ ] **Step 5: Run completion, output, and schema tests**

```bash
python3 -m unittest \
  test_git_queue.GitCompletionTests \
  test_agent_queue.LifecycleTransitionTests \
  test_agent_queue.QueueStateTests \
  test_agent_queue.QueueCliTests -v
```

Expected: every completion contract passes; serialized queue and normal CLI outputs contain `changed_path_count` but never `changed_paths`.

- [ ] **Step 6: Commit compact completion validation**

```bash
git add skills/manage-agent-queue/scripts/git_queue.py \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_git_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: Git 커밋 범위와 계보 검증"
```

## Task 6: Preserve and Resume Expired Git Work

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:1538-1570,1834-1970,2161-2183,3471-3497,3730-3800`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:1900-2200,4400-4800`
- Modify: `skills/manage-agent-queue/scripts/test_git_queue.py`

- [ ] **Step 1: Write failing expiry and recovery tests**

Cover these exact state transitions:

```python
def test_expired_git_lease_preserves_private_recovery_binding(self):
    task = self.add_git_task(max_attempts=2)
    claimed = aq.claim_task(
        self.state, "worker", git_observation=self.snapshot, now=self.NOW,
    )

    aq.sweep_expired(self.state, now=claimed["expires_at"])

    stored = self.state["tasks"][task["id"]]
    self.assertEqual("pending", stored["status"])
    self.assertIsNone(stored["claim"])
    self.assertEqual(self.snapshot["head"], stored["git_recovery"]["base"])
    self.assertEqual("git_recovery", aq.derive_state(self.state, stored, self.LATER))

def test_exhausted_git_attempt_keeps_recovery_across_retry(self):
    task = self.add_git_task(max_attempts=1)
    claimed = aq.claim_task(
        self.state, "worker", git_observation=self.snapshot, now=self.NOW,
    )
    aq.sweep_expired(self.state, now=claimed["expires_at"])
    self.assertEqual("failed", task["status"])
    recovery = copy.deepcopy(task["git_recovery"])

    aq.retry_task(self.state, task["id"], now=self.LATER)

    self.assertEqual(recovery, task["git_recovery"])
```

Add resume tests for unchanged base, descendant surviving commits, wrong worktree, wrong branch, divergent head, dirty state, ordinary-claim skip, and explicit cancellation clearing recovery.

- [ ] **Step 2: Run recovery tests and verify RED**

```bash
python3 -m unittest test_agent_queue.LifecycleTransitionTests \
  test_agent_queue.GitRecoveryTests \
  test_git_queue.GitRecoveryObservationTests -v
```

Expected: expired claims discard Git binding and no `git_recovery` state exists.

- [ ] **Step 3: Preserve recovery before clearing a Git claim**

Introduce one helper used by sweep and retryable failure:

```python
def _preserve_git_recovery(task):
    claim = task.get("claim")
    binding = claim.get("git") if isinstance(claim, dict) else None
    if task.get("git_mode") == "commit" and isinstance(binding, dict):
        task["git_recovery"] = copy.deepcopy(binding)
```

Call it before `apply_retry_rule` clears `claim`. Preserve recovery on `pending` and `failed`; `retry_task` leaves it intact; successful completion and cancellation clear it. A voluntary Git-aware `release` must preflight clean state and `HEAD == base` in `_run_command` so release cannot silently abandon a commit.

- [ ] **Step 4: Implement explicit resume claim**

`claim --resume-git` requires `--task`. In `claim_task`, require a stored recovery binding and a clean observation matching repository, worktree, and branch. Accept only:

- observed head equals recovery base; or
- recovery base is an ancestor of observed head and all surviving changes remain in scope.

The new live binding keeps the original base, receives a fresh token and expiry, clears `git_recovery`, increments attempts, and emits `task.claimed` with `resumed_git: true`. A failed validation leaves the task and recovery binding unchanged.

- [ ] **Step 5: Run transition, expiry, recovery, and token tests**

```bash
python3 -m unittest \
  test_agent_queue.LifecycleTransitionTests \
  test_agent_queue.GitRecoveryTests \
  test_git_queue.GitRecoveryObservationTests -v
```

Expected: selected tests pass, expired tokens remain rejected, and recovery never mutates Git.

- [ ] **Step 6: Commit Git lease recovery**

```bash
git add skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  skills/manage-agent-queue/scripts/test_git_queue.py
git commit -m "feat: 만료된 Git 작업 안전 복구"
```

## Task 7: Synchronize Skill and Public Documentation

**Files:**
- Modify: `skills/manage-agent-queue/SKILL.md`
- Modify: `skills/manage-agent-queue/references/queue-schema.md`
- Modify: `skills/manage-agent-queue/references/workflow-templates.md`
- Modify: `README.md`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:32-150`

- [ ] **Step 1: Strengthen documentation contract tests**

Extend `SkillContractTests` to require:

```python
for required in (
    "--git-commit",
    "--commit",
    "--no-change",
    "--resume-git",
    "migrate --to 2",
    "git_recovery",
    "file:",
    "dir:",
    "changed_path_count",
):
    self.assertIn(required, schema)

self.assertIn("$CLI serve --open", readme)
self.assertNotIn('"changed_paths"', schema)
self.assertNotIn('"changed_paths"', templates)
```

Add role assertions proving Git-enabled templates name only writer roles and state explicitly that the queue does not create worktrees, commit, merge, reset, or push.

- [ ] **Step 2: Run contract tests and verify RED**

```bash
python3 -m unittest test_agent_queue.SkillContractTests -v
```

Expected: failures for missing Git-aware public contracts and the existing README dashboard command drift.

- [ ] **Step 3: Update the skill's operational steps**

Keep `SKILL.md` compact. Add one Git-aware branch under coordination and worker execution:

```markdown
For commit-producing parallel work, create isolated worktrees and branches before dispatch. Add Git-aware writer tasks with canonical `file:`/`dir:` resources. Each worker must claim from its own clean attached worktree, heartbeat before committing, and complete with `--commit` or an explicit `--no-change`. Resume expired Git work only with `--task ... --resume-git`; never publish with an expired token.
```

Point detailed schema and workflow behavior to the existing references rather than duplicating it.

- [ ] **Step 4: Make schema and workflow references authoritative**

Update `queue-schema.md` with exact version-2 task, claim binding, recovery, compact result, derived-state precedence, migration, typed-resource overlap, command, error, privacy, and output contracts from the approved design.

Update `workflow-templates.md` with:

- `--git-commit` on adversarial review.
- `"git_commit": true` on parallel shards.
- Git mode only on implement/apply or shard/integrate writer roles.
- Per-writer isolated worktree/branch preparation.
- Commit artifacts passed to integration; no automatic Git execution.

- [ ] **Step 5: Add a concise README quick start and restore dashboard invocation**

Define a concrete CLI variable and show migration, Git-aware creation, claim, completion, and optional live view. The dashboard line must use the exact contract string:

```bash
CLI="python3 skills/manage-agent-queue/scripts/agent_queue.py --queue $QUEUE"
$CLI migrate --to 2
$CLI task add --title "HTTP shard" --git-commit \
  --resource file:src/http.py --resource dir:tests/http/
$CLI claim --agent shard-http
$CLI complete --task T-000001 --agent shard-http \
  --token "$TOKEN" --commit "$RESULT_HEAD" --summary "HTTP shard complete"
$CLI serve --open
```

Clarify that new queues already use v2, so migration is only for existing v1 queues. Keep worktree creation, Git commit, merge, reset, and push outside the queue.

- [ ] **Step 6: Run documentation and help-contract tests**

```bash
python3 -m unittest test_agent_queue.SkillContractTests \
  test_agent_queue.QueueCliTests -v
python3 skills/manage-agent-queue/scripts/agent_queue.py --help
python3 skills/manage-agent-queue/scripts/agent_queue.py claim --help
python3 skills/manage-agent-queue/scripts/agent_queue.py complete --help
```

Expected: tests pass and help text exposes every documented flag without raw private path fields.

- [ ] **Step 7: Commit synchronized Git-aware documentation**

```bash
git add README.md skills/manage-agent-queue/SKILL.md \
  skills/manage-agent-queue/references/queue-schema.md \
  skills/manage-agent-queue/references/workflow-templates.md \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "docs: Git-aware 큐 작업 계약 문서화"
```

## Task 8: Full Verification and Real Git Smoke Test

**Files:**
- Modify only files required to fix a verified failure from this task.

- [ ] **Step 1: Run the full automated suite without leaving bytecode residue**

Run from the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s skills/manage-agent-queue/scripts \
  -p 'test_*.py' -v
```

Expected: every test passes with `OK`; the count exceeds the 225-test baseline and the pre-existing README contract failure is resolved.

- [ ] **Step 2: Check syntax, whitespace, placeholders, private-path leaks, and path-list persistence**

```bash
python3 -m py_compile \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/git_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  skills/manage-agent-queue/scripts/test_git_queue.py
git diff --check
rg -n 'TBD|TODO|FIXME|implement later|"changed_paths"|common_dir|worktree' \
  skills/manage-agent-queue README.md
```

Expected: syntax and whitespace checks pass. There are no placeholders or persisted `"changed_paths"` fields. `common_dir` and raw `worktree` occur only in private in-process binding code, schema documentation that labels them private, and tests proving redaction.

- [ ] **Step 3: Run a real isolated-worktree Git-aware smoke flow**

Create a disposable repository and shared queue:

```bash
GIT_QUEUE_SMOKE_DIR="$(mktemp -d)"
GIT_QUEUE_SMOKE_REPO="$GIT_QUEUE_SMOKE_DIR/repo"
GIT_QUEUE_SMOKE_QUEUE="$GIT_QUEUE_SMOKE_DIR/queue.json"
GIT_QUEUE_CLI="python3 $PWD/skills/manage-agent-queue/scripts/agent_queue.py --queue $GIT_QUEUE_SMOKE_QUEUE"
git init -b main "$GIT_QUEUE_SMOKE_REPO"
git -C "$GIT_QUEUE_SMOKE_REPO" config user.name "Queue Smoke"
git -C "$GIT_QUEUE_SMOKE_REPO" config user.email "queue-smoke@example.test"
touch "$GIT_QUEUE_SMOKE_REPO/base.txt"
git -C "$GIT_QUEUE_SMOKE_REPO" add base.txt
git -C "$GIT_QUEUE_SMOKE_REPO" commit -m "base"
$GIT_QUEUE_CLI init --id git-smoke
$GIT_QUEUE_CLI task add --title "Write scoped file" --git-commit \
  --resource file:scoped.txt
```

From the disposable repository, claim, commit, and complete. Capture the returned token from JSON using Python rather than printing it into queue events:

```bash
GIT_QUEUE_CLAIM="$GIT_QUEUE_SMOKE_DIR/claim.json"
(cd "$GIT_QUEUE_SMOKE_REPO" && $GIT_QUEUE_CLI claim --agent smoke-worker) \
  > "$GIT_QUEUE_CLAIM"
GIT_QUEUE_TOKEN="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lease_token"])' "$GIT_QUEUE_CLAIM")"
touch "$GIT_QUEUE_SMOKE_REPO/scoped.txt"
git -C "$GIT_QUEUE_SMOKE_REPO" add scoped.txt
git -C "$GIT_QUEUE_SMOKE_REPO" commit -m "add scoped file"
GIT_QUEUE_HEAD="$(git -C "$GIT_QUEUE_SMOKE_REPO" rev-parse HEAD)"
(cd "$GIT_QUEUE_SMOKE_REPO" && $GIT_QUEUE_CLI complete \
  --task T-000001 --agent smoke-worker --token "$GIT_QUEUE_TOKEN" \
  --commit "$GIT_QUEUE_HEAD" --summary "scoped commit complete")
```

Expected: completion exits 0, reports one commit and one changed path, and neither command output nor queue projections expose private common-directory/worktree paths or a `changed_paths` list.

- [ ] **Step 4: Prove out-of-scope completion fails without completing**

Add a second Git-aware task scoped to `allowed.txt`, claim it, commit `outside.txt`, and attempt completion with the new head.

Expected: completion exits with the documented runtime error, prints at most the bounded offending path set, and `status --format json` leaves the task leased rather than completed. Restore or discard only the disposable repository after recording the result; never reset the implementation worktree.

- [ ] **Step 5: Verify migration and recovery smoke paths**

Use a fixture version-1 queue to run `migrate --to 2`, then create a Git-aware task with a short lease. Commit within scope, allow the lease to expire, run `sweep`, confirm `git_recovery`, then use `retry` if attempts exhausted and `claim --task ... --resume-git` from the same worktree.

Expected: migration changes one revision atomically; recovery retains the original base, issues a new token, completes the surviving commit, and performs no Git reset, checkout, merge, or push.

- [ ] **Step 6: Inspect commit scope and repository state**

```bash
git status --short --branch
git log --oneline --decorate -10
git diff --check HEAD~7..HEAD
git diff --stat HEAD~7..HEAD
```

Expected: the worktree is clean. Feature history contains focused commits for helper boundary, schema migration, task definitions, claim ownership, completion validation, recovery, and documentation. The diff contains only the planned scripts, tests, skill references, README, plan, and approved spec correction.

- [ ] **Step 7: Route any verified failure back to its owning task**

If Steps 1-6 reveal a failure, return to the task that owns the affected contract, add or strengthen the exact regression test, make it pass, commit the correction with that task's scope, and rerun Task 8 from Step 1. If no correction is needed, do not create an empty verification commit.
