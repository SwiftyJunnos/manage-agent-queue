# Queue Schema and CLI Contract

Use this reference when changing or diagnosing the queue format, state machine, locking, projections, or public CLI. `queue.json` is authoritative; `queue.tsv` is derived.

## Contents

- [Path resolution](#path-resolution)
- [Top-level state](#top-level-state)
- [Task state](#task-state)
- [Stored and derived states](#stored-and-derived-states)
- [Graph, priority, and resources](#graph-priority-and-resources)
- [Leases, retries, and transitions](#leases-retries-and-transitions)
- [Transactions and locking](#transactions-and-locking)
- [TSV projection](#tsv-projection)
- [Events and redaction](#events-and-redaction)
- [Public commands](#public-commands)
- [Exit codes](#exit-codes)
- [Doctor](#doctor)
- [Compaction](#compaction)

## Path Resolution

Resolve one absolute path in this precedence order:

1. global `--queue PATH`;
2. `AGENT_QUEUE_PATH`;
3. `.agent-queue/queue.json` below the nearest current-directory ancestor containing a `.git` file or directory, or below the current directory when none exists.

Expansion handles `~`; it does not resolve symlinks or invoke Git. Processes in different worktrees must receive the same explicit absolute path. Version 1 supports one local machine and local-filesystem locking, not multi-host or network-filesystem coordination.

## Top-Level State

The exact schema-version-1 shape is:

```json
{
  "schema_version": 1,
  "queue_id": "project-port",
  "revision": 17,
  "next_task_sequence": 5,
  "next_workflow_sequence": 2,
  "next_event_sequence": 43,
  "created_at": "2026-07-10T05:00:00Z",
  "updated_at": "2026-07-10T05:12:30Z",
  "config": {
    "default_lease_seconds": 900,
    "default_max_attempts": 3,
    "retry_backoff_seconds": 30,
    "lock_timeout_seconds": 5,
    "stale_lock_seconds": 30
  },
  "tasks": {},
  "events": []
}
```

Reject missing or extra fields. Require canonical UTC timestamps (`YYYY-MM-DDTHH:MM:SSZ`), positive configuration integers, and a nonnegative revision. IDs are queue-local, monotonic six-digit values (`T-000001`, `W-000001`) and are never reused after deletion or compaction. Event sequences are positive and increasing; their cursor remains monotonic.

Every accepted transaction validates a detached candidate and, when changed, increments the queue revision once. JSON is strict UTF-8 with finite JSON values and metadata depth at most 64.

## Task State

Each stored task has every field below:

```json
{
  "id": "T-000004",
  "workflow_id": "W-000001",
  "role": "apply",
  "title": "Apply review findings",
  "description": "Apply valid findings and record the final diff.",
  "status": "pending",
  "priority": 30,
  "depends_on": ["T-000002", "T-000003"],
  "resources": ["file:src/api.py"],
  "labels": ["python", "port"],
  "attempts": 0,
  "max_attempts": 3,
  "available_at": null,
  "claim": null,
  "result": null,
  "last_error": null,
  "created_at": "2026-07-10T05:00:00Z",
  "updated_at": "2026-07-10T05:00:00Z"
}
```

Task creation accepts `id`, `workflow_id`, `role`, `title`, `description`, `priority`, `depends_on`, `resources`, `labels`, and `max_attempts`. It defaults description to empty, priority to zero, lists to empty, and attempts from queue configuration. Require a nonblank title, integer priority, positive maximum attempts, and string lists. Deduplicate `depends_on`, `resources`, and `labels` while preserving first occurrence; stored canonical lists must be duplicate-free. Generic tasks may use null workflow and role.

Allow `available_at` only on `pending`. Allow `claim` only on `leased`, `result` only on `completed`, and a blocking error kind only on `blocked`. A completed result is exactly:

```json
{"summary": "Applied two findings", "artifacts": ["artifacts/final.diff"]}
```

Limit descriptions, result summaries, failure messages, and block reasons to 16,384 valid UTF-8 bytes. Put large diffs, logs, and reports in artifact files.

## Stored and Derived States

Persist only these states:

- `pending`: unclaimed; readiness is derived.
- `leased`: owned by one live claim.
- `completed`: terminal success with a result.
- `failed`: terminal failure after exhaustion or `fail --terminal`.
- `blocked`: manually paused with a blocking reason.
- `cancelled`: intentionally abandoned.

Views derive these states with fixed precedence for a pending task:

- `dependency_failed`: any dependency is `failed`, `blocked`, or `cancelled`.
- `waiting_dependency`: at least one dependency is not `completed`.
- `waiting_retry`: `available_at` is later than the current time.
- `resource_conflict`: a declared resource belongs to another unexpired `leased` task.
- `ready`: no preceding condition applies.
- `leased`: mirrors the stored active state and is also a filterable derived name.

Non-pending rows display their stored state. A dependency failure does not rewrite the dependent task.

## Graph, Priority, and Resources

Require every dependency to exist, reject self-edges and cycles, and validate batch/workflow additions all-or-nothing. A task becomes dependency-ready only after every dependency is `completed`.

Treat resource strings as exact, case-sensitive exclusive keys. Do not infer path overlap. Coordinators must choose a consistent granularity such as `file:src/api.py`, `crate:runtime`, or `scope:auth`. An unexpired lease reserves all listed resources; expired leases do not block eligibility.

`claim` may filter by exact role and a required subset of labels. It excludes exhausted tasks and selects among eligible tasks by priority descending, then numeric task ID ascending. Selection and lease creation share one transaction.

## Leases, Retries, and Transitions

A successful claim increments `attempts` and stores:

```json
{
  "agent_id": "reviewer-2",
  "lease_token": "lq_<random>",
  "claimed_at": "2026-07-10T05:12:30Z",
  "heartbeat_at": "2026-07-10T05:12:30Z",
  "expires_at": "2026-07-10T05:27:30Z"
}
```

Require task ID, matching agent ID, matching token, and an unexpired lease for `heartbeat`, `complete`, `fail`, and `release`. Heartbeat extends expiry from the command time. Completion stores a summary and artifact list. Release returns to immediate `pending` without refunding the attempt.

A retryable failure or swept expiry clears the claim and records the error. When `attempts < max_attempts`, return to `pending` and set `available_at` to current time plus retry backoff; otherwise set `failed`. `fail --terminal` skips remaining attempts. `retry` accepts only `failed`, adds one maximum attempt by default (or `--additional-attempts N`), and returns to immediate `pending` while retaining prior history.

`block` accepts only `pending`. `unblock` accepts only `blocked`. `cancel` accepts `pending`, `blocked`, or `failed`; it rejects `leased` and `completed`. Expired leases are swept before normal mutations, claims, and status, or explicitly with `sweep`.

The model provides at-least-once assignment. It cannot guarantee exactly-once external side effects; make worker actions idempotent and reject late publication.

## Transactions and Locking

For each queue, use persistent regular file `<queue>.lock.guard` containing marker `LQG1` plus owner directory `<queue>.lock/owner.json`. Use `fcntl.flock` on POSIX or `msvcrt.locking` on Windows; fail closed when neither backend exists. Reject symlinked, replaced, malformed, or nonregular guards and unsafe lock paths.

The transaction algorithm is:

1. Open/create the guard without following symlinks and acquire its exclusive kernel lock with bounded jitter and timeout.
2. Create the lock directory. Its owner records random token, PID, hostname, acquisition time, and stale-after time.
3. Under the guard, identify safely stale lock directories, rename them to random orphan paths, and remove them before retrying.
4. Read and fully validate `queue.json`; copy it; optionally sweep; apply the transition; validate again.
5. On change, increment one revision, normalize new event revisions, write same-directory temporary JSON, flush and `fsync`, then `os.replace` JSON.
6. Atomically replace TSV after JSON. On no-op, repair a missing/stale TSV without changing JSON.
7. Remove the owned lock directory only if its token still matches, then release the kernel guard.

Do no agent work or external commands inside this critical section. A callback failure rolls back both its mutation and the automatic sweep. JSON remains authoritative if a process stops between JSON and TSV replacement.

## TSV Projection

Generate `queue.tsv` beside `queue.json`. Begin with `# queue_revision: N`, then these columns in order:

| Column | Meaning |
|---|---|
| `id` | Task ID |
| `workflow` | Workflow ID or empty |
| `role` | Role or empty |
| `state` | Stored/derived display state |
| `priority` | Integer priority |
| `assignee` | Lease agent or empty |
| `lease_until` | Expiry or empty |
| `attempts` | `attempts/max_attempts` |
| `depends_on` | Comma-separated dependency IDs |
| `blocked_by` | Incomplete dependency or conflicting task IDs |
| `resources` | Comma-separated exclusive keys |
| `title` | Task title |

Escape tabs, CR/LF, backslashes, and unsafe controls so every task remains one row. Never import TSV edits; status, export, mutation, or `doctor --repair` may overwrite them. `status` defaults to a terminal table and supports JSON/TSV output plus workflow, assignee, role, label, and stored-or-derived state filters.

## Events and Redaction

Each event has exactly `seq`, `at`, `type`, `actor`, `task_id`, `revision`, and `details`. Events are ordered by increasing sequence, nondecreasing timestamp and revision, reference retained tasks when task-scoped, and never persist a future revision.

Creation and transitions emit `task.added`, `workflow.created`, `task.claimed`, `task.heartbeat`, `task.completed`, `task.failed`, `task.released`, `task.lease_expired`, `task.retried`, `task.blocked`, `task.unblocked`, `task.cancelled`, and `queue.compacted` as applicable. Recursively remove every `lease_token` key from event details. `task show`, status, TSV, and events never expose the token; only successful `claim` returns it. Events also omit result bodies and include bounded metadata such as counts and states.

## Public Commands

Invoke `python3 scripts/agent_queue.py [--queue PATH] COMMAND`. Run `--help` for complete flags.

| Command | Contract |
|---|---|
| `init` | Create JSON and empty TSV; set ID and optional lease/retry defaults. |
| `task add` | Add one task from flags or `--from-json`. |
| `task add-batch` | Atomically add a nonempty JSON array. |
| `task show` | Return one redacted task snapshot. |
| `workflow add` | Add one built-in workflow atomically. |
| `claim` | Atomically sweep, choose, and lease eligible work. |
| `heartbeat` | Extend a matching live lease. |
| `complete` | Store concise success summary/artifact paths. |
| `fail` | Retry or terminally fail matching leased work. |
| `release` | Return matching leased work to pending. |
| `retry` | Grant attempts to a failed task. |
| `block` | Pause pending work with a reason. |
| `unblock` | Resume blocked work. |
| `cancel` | Cancel eligible non-active work. |
| `status` | Sweep, regenerate TSV, and render/filter status. |
| `events` | Return sanitized history, optionally by task. |
| `sweep` | Process expired leases explicitly. |
| `export` | Print canonical `--format tsv`. |
| `doctor` | Diagnose source, guard, locks, artifacts, and TSV. |
| `compact` | Remove eligible closed history before a cutoff. |

Mutating commands emit JSON on stdout. Normal errors use stderr, except `doctor`, which always emits its structured report on stdout for known diagnostics.

## Exit Codes

| Code | Meaning |
|---:|---|
| `0` | Success, including a clean or fully repaired doctor report |
| `2` | Argparse error, invalid user input, missing/unreadable queue, or other runtime queue error |
| `3` | No eligible task is available to claim |
| `4` | Queue/doctor lock acquisition timed out |
| `5` | Lease expired or task/agent/token identity mismatch |
| `6` | Persisted schema, UTF-8, revision, graph, guard, lock, or other invariant failure; doctor also uses this for unresolved non-timeout issues |

Code `1` is not assigned by the CLI contract.

## Doctor

`doctor` returns `{ok, queue, revision, issues, repairs}` and acquires the kernel guard. It diagnoses missing/unsafe/invalid source JSON, guard availability/validity, lock timeout or unsafe/stale/orphan locks, orphan quarantine directories, and missing/malformed/stale/content-mismatched TSV.

Use `doctor --repair` only to rebuild a safe derived TSV and remove safely identified stale lock directories or recognized real-directory quarantine artifacts. Never rewrite, guess, or salvage corrupted `queue.json`; never follow or delete symlinks or arbitrary similarly named files. Do not overwrite unreadable or unsafe TSV paths. Report failed cleanup/rebuild attempts as unresolved issues.

## Compaction

`compact --before` accepts `YYYY-MM-DD` (midnight UTC) or canonical UTC timestamp. It does not auto-sweep.

Eligible standalone tasks must be `completed`, `failed`, or `cancelled` and have `updated_at` strictly before the cutoff. A workflow is an indivisible unit: every member must meet that rule. Retain any candidate unit required by a retained task, propagating dependency closure across candidate units. Remove events older than the cutoff or referencing removed task IDs in `task_id` or nested detail values. Validate the remaining graph, append `queue.compacted` with exact removal counts/IDs, and preserve all sequence cursors. A no-op changes neither revision nor TSV bytes.
