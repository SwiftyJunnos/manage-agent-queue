# Manage Agent Queue

A small, file-backed queue for coordinating multiple coding agents safely.

<p align="center">
  <a href="https://skills.sh/SwiftyJunnos/manage-agent-queue"><img src="https://skills.sh/b/SwiftyJunnos/manage-agent-queue" alt="skills.sh installs" /></a>
  <img src="https://img.shields.io/badge/Agent%20Skill-agent%20coordination-182235?style=flat-square" alt="Agent Skill: agent coordination" />
  <img src="https://img.shields.io/badge/runtime-Python%203-D66853?style=flat-square" alt="Runtime: Python 3" />
</p>

Use it when agents need to claim shared work, respect dependencies, avoid
overlapping resources, and leave an observable recovery trail. It provides the
state model and CLI only—it never starts agents or executes their tasks.

## Quick start

Clone the repository. Python 3 is the only runtime dependency.

```bash
git clone https://github.com/SwiftyJunnos/agent-manage-system.git
cd agent-manage-system

REPO="$(pwd)"
QUEUE="/absolute/path/to/your-project/.agent-queue/queue.json"
CLI="python3 $REPO/skills/manage-agent-queue/scripts/agent_queue.py --queue $QUEUE"
```

Create one queue:

```bash
$CLI init --id checkout-improvements
```

When an agent offers live observation and you approve it, open the local dashboard:

```bash
$CLI serve --open
```

The foreground command prints a private `http://127.0.0.1:...` URL. If the browser cannot be opened automatically, visit that printed URL manually. Stop the command with `Ctrl-C` when coordination ends. The dashboard is read-only; use the normal CLI commands to change tasks.

For terminal-only observation, use `$CLI status` and `$CLI events`.

Add work and let a worker claim it.

```bash
$CLI task add \
  --title "Document the checkout flow" \
  --role implementer \
  --priority 50 \
  --resource file:README.md \
  --label docs

$CLI claim --agent implementer-01 --role implementer --label docs
```

The claim response includes a task ID and lease token. Keep the token private,
then use it to finish the task:

```bash
$CLI complete \
  --task T-000001 \
  --agent implementer-01 \
  --token 'lq_token_returned_by_claim' \
  --summary "Added and verified the documentation."

$CLI status
```

Use the actual ID and token returned by your queue; the example values are
placeholders.

## How to use it

- **Coordinator:** create one shared queue, split work into verifiable tasks,
  declare dependencies and exact resource keys, then dispatch workers with the
  same absolute queue path.
- **Worker:** follow `claim -> work -> heartbeat -> complete or fail`. Work only
  inside the claimed scope; release work you abandon.
- **Reviewer:** review independently. Give reviewers the diff and acceptance
  criteria, not the implementer's reasoning or other review findings.

For work across Git worktrees, pass the same absolute `QUEUE` path to every
agent. Do not manually edit `queue.json` or generated `queue.tsv`.

## Useful commands

| Need | Command |
| --- | --- |
| Add work | `task add`, `task add-batch`, `workflow add` |
| Inspect live progress | `serve --open` after user approval |
| Inspect progress | `status`, `events`, `export --format tsv` |
| Maintain a lease | `heartbeat`, `complete`, `fail`, `release` |
| Recover work | `sweep`, `retry`, `block`, `unblock`, `cancel` |
| Diagnose local state | `doctor`, `doctor --repair`, `compact` |

Two workflow templates are included: `adversarial-review` for independent
review and `parallel-shards` for safely separated work.

## Safety

- `queue.json` is authoritative; `queue.tsv` is a generated view.
- Claims and leases are atomic, but assignment is at-least-once. Make external
  side effects idempotent.
- Resource keys are exact strings; choose and use a consistent granularity.
- Do not store secrets in task text, summaries, events, or TSV-visible fields.
- Version 1 is for one machine and a local filesystem, not distributed locking.

## References

- [Skill protocol](skills/manage-agent-queue/SKILL.md)
- [Queue schema and CLI contract](skills/manage-agent-queue/references/queue-schema.md)
- [Workflow templates](skills/manage-agent-queue/references/workflow-templates.md)

Run the test suite from the repository root:

```bash
python3 -m unittest discover -s skills/manage-agent-queue/scripts -p 'test_*.py' -v
```
