# Manage Agent Queue

**A safe, file-backed task queue for coordinating multiple coding agents.**

Manage Agent Queue gives coordinators and workers one shared operational view:
who owns work, what is ready, which dependencies are blocking progress, and
how to recover after an interrupted agent. It uses an atomic local queue rather
than coordinator notes, manually edited spreadsheets, or best-effort claims.

The repository ships a [Codex skill](skills/manage-agent-queue/SKILL.md) and a
dependency-free Python CLI. The CLI manages queue state only—it does not start
agents or execute their tasks.

## Why this exists

Parallel agents are useful only when their work is independently claimable and
their coordination survives interruptions. Without a shared contract, two
agents can edit the same resource, a dead worker can strand a task, or a review
can quietly become self-review.

This project makes those coordination decisions explicit:

- Decompose work into verifiable tasks with dependencies and acceptance criteria.
- Atomically claim work with a bounded lease before side effects begin.
- Reserve declared exclusive resources while a lease is active.
- Observe one queue, generated TSV projection, and sanitized event history.
- Recover expired leases and stale local artifacts without guessing at source state.

## What it guarantees

| Guarantee | What it means in practice |
| --- | --- |
| One source of truth | \`queue.json\` is authoritative. \`queue.tsv\` is a generated, human-readable projection. |
| Atomic claims | Task selection and lease creation happen in one local transaction. |
| Dependency-aware work | A task is ready only after all declared dependencies complete successfully. |
| Explicit ownership | Leases carry an agent ID, opaque token, and expiry time; later worker actions must prove ownership. |
| Scoped concurrency | Exact resource keys prevent concurrently leased tasks from taking the same declared resource. |
| Observable recovery | \`status\`, \`events\`, \`sweep\`, and \`doctor\` make stalled work and local queue artifacts inspectable. |
| Role independence | The protocol supports separate implementer, reviewer, applier, and verifier roles. |

## Install

Clone the repository and use Python 3. No third-party package installation is required.

\`\`\`bash
git clone https://github.com/SwiftyJunnos/agent-manage-system.git
cd agent-manage-system
python3 --version
\`\`\`

To use the workflow as a Codex skill, make the
[\`skills/manage-agent-queue\`](skills/manage-agent-queue) directory available in
your Codex skills location. The standalone CLI remains usable directly from this checkout.

Set these paths once for the examples below. Keep the queue outside the skill
checkout when it represents another project.

\`\`\`bash
REPO="$(pwd)"
QUEUE="/absolute/path/to/your-project/.agent-queue/queue.json"
CLI="python3 $REPO/skills/manage-agent-queue/scripts/agent_queue.py --queue $QUEUE"
\`\`\`

When agents work from different Git worktrees, give every one of them this same
absolute \`QUEUE\` path. Do not rely on each worktree's default queue path.

## Quick start

Create one queue, add one scoped task, and have a worker claim it.

\`\`\`bash
$CLI init --id checkout-improvements

$CLI task add \
  --title "Document the checkout flow" \
  --description "Add a verified getting-started section to README.md." \
  --role implementer \
  --priority 50 \
  --resource file:README.md \
  --label docs

$CLI claim --agent implementer-01 --role implementer --label docs
\`\`\`

\`claim\` returns a task ID and lease token. Keep that token private: it is
required for \`heartbeat\`, \`complete\`, \`fail\`, and \`release\`, but it is never
shown in status, TSV, or event output.

While work is still in progress, renew the lease before it expires:

\`\`\`bash
$CLI heartbeat \
  --task T-000001 \
  --agent implementer-01 \
  --token 'lq_token_returned_by_claim'
\`\`\`

Finish with a concise result and artifact paths rather than embedding large
diffs or logs in queue state:

\`\`\`bash
$CLI complete \
  --task T-000001 \
  --agent implementer-01 \
  --token 'lq_token_returned_by_claim' \
  --summary "Added and checked the getting-started section." \
  --artifact artifacts/readme-diff.txt

$CLI status
\`\`\`

Use the actual task ID and token returned by your queue. The values above are
illustrative placeholders, not values to reuse.

## How to operate it

### Coordinator responsibilities

1. Initialize exactly one shared queue.
2. Split work into verifiable tasks before dispatch. Declare dependencies,
   priority, exclusive resources, acceptance criteria, and expected artifacts.
3. Inspect \`status\` and available concurrency before starting agents.
4. Give each worker the shared absolute queue path, stable agent ID, claim
   filters, lease expectations, and narrowly scoped task instructions.
5. Monitor \`status\`, \`queue.tsv\`, and \`events\`—not only agent self-reports.
6. Finish only after required verification is recorded and no required task is
   failed or dependency-failed.

### Worker responsibilities

Workers follow this lifecycle:

\`\`\`text
claim -> inspect declared scope -> work -> heartbeat -> complete or fail
\`\`\`

- Claim before causing side effects.
- Work only on the claimed task and its declared exclusive resources.
- Heartbeat before expiry; use \`release\` if the task is abandoned.
- Do not publish a result after the lease expires.
- Store large outputs as artifacts and record only concise, non-secret summaries.

### Keep review independent

Avoid self-review. Give reviewers the final diff and acceptance criteria, not
the implementer's reasoning or another reviewer's findings. Give appliers the
diff and review artifacts. Give verifiers the final diff, the criteria, and the
commands they must run.

## Built-in workflows

\`workflow add\` creates a validated dependency graph in one transaction. It is
useful when the task shape is known before workers are dispatched.

### Adversarial review

Create isolated reviewers followed by an apply and verification path:

\`\`\`bash
$CLI workflow add \
  --template adversarial-review \
  --title "Review authentication change" \
  --reviewers 2 \
  --resource scope:authentication
\`\`\`

Use this for a change that needs independent scrutiny. Reviewers receive the
diff and acceptance criteria; they do not inherit each other's conclusions.

### Parallel shards

Create a dependency-aware graph for independent work partitions:

\`\`\`bash
$CLI workflow add \
  --template parallel-shards \
  --title "Migrate isolated modules" \
  --resource scope:module-migration
\`\`\`

Use this only when the shards have clearly separate resources. A shared
resource key intentionally prevents simultaneous claims.

Read the [workflow templates reference](skills/manage-agent-queue/references/workflow-templates.md)
before selecting or interpreting either template.

## Diagnose and recover

Start with observation; do not hand-edit queue files.

\`\`\`bash
$CLI status
$CLI events
$CLI export --format tsv
\`\`\`

Use these operational commands when their preconditions are true:

| Situation | Command | Result |
| --- | --- | --- |
| A lease has expired | \`sweep\` | Clears expired leases and applies the task retry policy. |
| A failed task is ready for another attempt | \`retry --task T-000001\` | Returns a failed task to pending with additional attempts. |
| Work must wait for an external decision | \`block --task T-000001 --reason \"…\"\` | Makes a pending task explicitly blocked. |
| A blocked task can resume | \`unblock --task T-000001\` | Returns it to pending. |
| The TSV or local lock artifacts need diagnosis | \`doctor\` | Emits a structured diagnostic report. |
| A safe derived-file or stale-artifact repair is available | \`doctor --repair\` | Repairs only safe derived artifacts; never guesses at corrupted JSON. |
| Closed history is no longer needed | \`compact --before 2026-07-01\` | Compacts eligible terminal history while preserving required dependencies. |

\`doctor\` is deliberately fail-closed. It can rebuild a safe derived TSV and
remove recognized stale local artifacts, but it never salvages, rewrites, or
guesses at corrupted \`queue.json\`.

## Command map

Run \`$CLI --help\` for every flag. The public command groups are:

| Need | Commands |
| --- | --- |
| Start a queue | \`init\` |
| Create work | \`task add\`, \`task add-batch\`, \`workflow add\` |
| Inspect work | \`status\`, \`task show\`, \`events\`, \`export\` |
| Acquire and maintain work | \`claim\`, \`heartbeat\`, \`release\` |
| Record an outcome | \`complete\`, \`fail\`, \`retry\` |
| Change availability | \`block\`, \`unblock\`, \`cancel\`, \`sweep\` |
| Diagnose or reduce state | \`doctor\`, \`compact\` |

For the exact state schema, transition rules, filters, exit codes, and locking
model, read the [queue schema and CLI contract](skills/manage-agent-queue/references/queue-schema.md).

## Safety and limits

- **Do not edit \`queue.json\` or \`queue.tsv\` directly.** Use the CLI as the only
  writer; the TSV is regenerated from JSON.
- **Do not put secrets in descriptions, result summaries, events, or TSV-visible
  fields.** Lease tokens are redacted from persistent views.
- **Use exact resource keys.** \`file:src/api.py\` and \`file:src\` are different
  keys; the queue does not infer path overlap.
- **Expect at-least-once assignment.** A lease prevents simultaneous ownership
  while active; it cannot make external side effects exactly once. Make those
  side effects idempotent and reject late publication.
- **Use a local filesystem.** Version 1 coordinates processes on one machine;
  it is not a multi-host or network-filesystem queue.
- **Keep queue operations short.** The transaction lock protects queue state,
  not the work itself. Do not run agents or external work inside it.

## Project layout

\`\`\`text
skills/manage-agent-queue/
├── SKILL.md                         # Coordinator and worker protocol
├── agents/openai.yaml               # Codex-facing skill metadata
├── scripts/agent_queue.py           # Dependency-free queue CLI
├── scripts/test_agent_queue.py      # Unit, contract, concurrency, and recovery tests
└── references/
    ├── queue-schema.md              # State, transaction, and CLI contract
    └── workflow-templates.md        # Built-in graph templates
\`\`\`

## Test

Run the full suite from the repository root:

\`\`\`bash
python3 -m unittest discover \
  -s skills/manage-agent-queue/scripts \
  -p 'test_*.py' \
  -v
\`\`\`

The suite covers state validation, dependencies, leases, concurrent claims,
retry behavior, persistence, TSV projection, workflow creation, diagnostics,
compaction, and the published skill contract.

## Detailed references

- [Skill protocol](skills/manage-agent-queue/SKILL.md)
- [Queue schema and CLI contract](skills/manage-agent-queue/references/queue-schema.md)
- [Workflow templates](skills/manage-agent-queue/references/workflow-templates.md)
