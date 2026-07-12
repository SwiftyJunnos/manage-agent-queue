# Built-in Workflow Templates

Use this reference before creating or interpreting the two version-1 templates. The CLI creates the graphs and queue metadata atomically; agents must still produce the acceptance and artifact content described here.

## Contents

- [Common invocation rules](#common-invocation-rules)
- [Adversarial review](#adversarial-review)
- [Adversarial review context and artifacts](#adversarial-review-context-and-artifacts)
- [Parallel shards](#parallel-shards)
- [Parallel shards context and artifacts](#parallel-shards-context-and-artifacts)
- [Completion checks](#completion-checks)

## Common Invocation Rules

Place the global queue option before the command. Use one shared absolute path:

```bash
QUEUE=/absolute/shared/path/queue.json
python3 scripts/agent_queue.py --queue "$QUEUE" workflow add --help
```

Each invocation creates one `W-NNNNNN` workflow plus all member tasks in one transaction. Workflow and task IDs remain monotonic. If any input or graph invariant fails, create nothing. The CLI does not start roles, read artifacts, run verification, or enforce the semantic quality of summaries.

Claim a role through the same atomic worker operation used for generic tasks:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" claim \
  --agent reviewer-1 --role review
```

Use the lease token only in that worker's later `heartbeat`, `complete`, `fail`, or `release` calls.

## Adversarial Review

Use `adversarial-review` to create an implement–independent review–apply–verify graph:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" workflow add \
  --template adversarial-review \
  --title "Port HTTP module" \
  --priority 50 \
  --resource file:src/http.py \
  --reviewers 2
```

Omit `--priority` for zero. Omit `--reviewers` for two. Supply a positive reviewer count and repeat `--resource` for each unique, nonblank exclusive writer key. This template does not accept `--from-json`.

For base priority `P` and `R` reviewers, create `R + 3` tasks:

```text
implement (P, writer resources)
  ├─ review-1 (P-10, no exclusive resources) ─┐
  ├─ review-2 (P-10, no exclusive resources) ─┼─ apply (P-20, writer resources) ─ verify (P-30, none)
  └─ review-R (P-10, no exclusive resources) ─┘
```

The exact roles and dependencies are:

| Role | Count | Depends on | Exclusive resources | Generated priority |
|---|---:|---|---|---:|
| `implement` | 1 | none | all supplied resources | `P` |
| `review` | `R` | implement task | none | `P - 10` |
| `apply` | 1 | every review task | all supplied resources | `P - 20` |
| `verify` | 1 | apply task | none | `P - 30` |

Review descriptions name target resources for inspection but do not reserve them. This permits independent read-only reviews to run concurrently after the implementer's lease is complete. Only implement and apply reserve the write scope.

### Adversarial Review Context and Artifacts

Preserve role isolation; the queue does not enforce it automatically:

- Give `implement` requirements, acceptance criteria, source context, and shared guides. Require a concise implementation summary plus a diff or commit artifact path.
- Give each `review` the implementation diff and acceptance criteria only. Do not provide implementer reasoning, another reviewer's findings, or a suggested verdict. Require one independent findings artifact; an empty findings list is valid.
- Give `apply` the original diff and every independent review artifact. Require a final diff/commit artifact and a concise disposition of findings.
- Give `verify` the final diff, acceptance criteria, and exact verification commands only. Do not ask it to rely on implement/apply claims. Require a verification report with commands, outcomes, and any blocking failure.

Do not let the implementer claim a `review` task for its own work. Keep large review reports out of queue summaries; pass their paths with repeated `--artifact` flags on completion.

Example reviewer completion:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" complete \
  --task T-000002 --agent reviewer-1 --token "$TOKEN" \
  --summary "Independent review complete; 2 findings" \
  --artifact artifacts/T-000002-findings.json
```

Treat the workflow as successful only when `verify` is `completed`, every required predecessor is `completed`, and no required task is `failed` or derived `dependency_failed`. A completed review means the review artifact exists; it does not mean the implementation is defect-free.

## Parallel Shards

Use `parallel-shards` with JSON containing exactly `title`, `shards`, and optional `priority`:

```json
{
  "title": "Port runtime modules",
  "priority": 40,
  "shards": [
    ["file:src/http.py"],
    ["file:src/fs.py", "file:src/path.py"],
    ["file:tests/runtime_test.py"]
  ]
}
```

Create the workflow:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" workflow add \
  --template parallel-shards \
  --from-json /absolute/path/parallel-shards.json
```

Do not combine `--from-json` with title, priority, resource, or reviewers flags. Require a nonempty shard list and a nonempty resource list in each shard. The CLI deduplicates repeated keys within one shard but rejects any resource appearing in more than one shard.

For `S` shards and base priority `P`, create `S + 2` tasks:

```text
shard-1 (P, resources-1) ─┐
shard-2 (P, resources-2) ─┼─ integrate (P-10, union of shard resources) ─ verify (P-20, none)
shard-S (P, resources-S) ─┘
```

The exact roles and dependencies are:

| Role | Count | Depends on | Exclusive resources | Generated priority |
|---|---:|---|---|---:|
| `shard` | `S` | none | that shard's keys | `P` |
| `integrate` | 1 | every shard task | ordered union of shard keys | `P - 10` |
| `verify` | 1 | integrate task | none | `P - 20` |

Distinct resource sets allow shards to be leased concurrently. Integration becomes dependency-ready only after every shard completes and then reserves the combined write scope. Verification is modeled as read-only.

### Parallel Shards Context and Artifacts

- Give each `shard` only its requirements, shared acceptance contract, and declared write resources. Require a shard diff/commit and any shard-specific test report paths.
- Give `integrate` all shard artifacts, the combined acceptance contract, and the intended reconciliation scope. Require the integrated diff/commit and integration notes.
- Give `verify` the final integrated diff, acceptance criteria, and exact verification commands. Require the final verification report.

Do not use overlapping resource names to force parallelism. Redesign the shards or use explicit dependencies when the actual write scopes overlap. Resource keys protect only exact matches; coordinators must declare a common granularity.

Example shard completion:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" complete \
  --task T-000001 --agent shard-http --token "$TOKEN" \
  --summary "HTTP shard complete" \
  --artifact artifacts/http.diff \
  --artifact artifacts/http-tests.txt
```

## Completion Checks

Before declaring either workflow complete:

1. Inspect `status --workflow W-NNNNNN --format json` and the generated TSV rather than role self-reports.
2. Confirm the required `verify` task is `completed` and every required dependency is `completed`.
3. Confirm no required row is `failed`, `dependency_failed`, `blocked`, or still leased/waiting.
4. Confirm every role's promised artifact path exists and contains its independently required evidence.
5. Inspect sanitized `events --task T-NNNNNN` when retry, lease expiry, or unexpected ownership affected trust in the result.
