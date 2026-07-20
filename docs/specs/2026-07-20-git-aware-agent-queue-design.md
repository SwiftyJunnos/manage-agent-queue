# Git-Aware Agent Queue Design

**Status:** Approved in conversation; awaiting review of this written specification  
**Date:** 2026-07-20

## Summary

Extend `manage-agent-queue` with optional Git-aware task validation. A Git-aware task binds its lease to the claiming worker's actual repository, worktree, branch, and starting commit. Completion succeeds only when the worker presents a clean, in-scope commit result descended from that starting commit.

The queue remains a coordinator and validator. It does not create worktrees, run commits, merge branches, reset repositories, or block Git commands executed outside the queue. Safety is cooperative among workers that use the shared queue.

## Goals

- Prevent two live Git-aware tasks from owning the same worktree or branch.
- Allow independent tasks in distinct worktrees and branches to proceed concurrently.
- Bind Git ownership from observed state at `claim` time instead of trusting coordinator-supplied paths.
- Verify commit ancestry and declared write scope at `complete` time.
- Preserve compact queue state and bounded CLI output when commits touch many paths.
- Recover safely when a lease expires after an external Git side effect.
- Preserve version-1 behavior for queues that do not opt into Git-aware tasks.

## Non-goals

- Creating or deleting worktrees and branches.
- Running `git add`, `git commit`, merge, rebase, cherry-pick, reset, or push.
- Preventing a human or process that bypasses the queue from changing Git state.
- Guaranteeing exactly-once Git side effects.
- Inferring semantic ownership from arbitrary resource keys such as `scope:auth`.
- Storing every changed path in queue results or events.

## User-Facing Model

### Opt-in tasks

Generic tasks retain their current behavior. A task becomes Git-aware only when its stored `git_mode` is `commit`.

Flag-based creation uses:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" task add \
  --title "Implement HTTP shard" \
  --git-commit \
  --resource file:src/http.py \
  --resource dir:tests/http/
```

JSON task creation accepts:

```json
{
  "title": "Implement HTTP shard",
  "git_mode": "commit",
  "resources": ["file:src/http.py", "dir:tests/http/"]
}
```

Omitting `--git-commit` or `git_mode` creates a generic task with `git_mode: null` after migration to schema version 2.

### Workflow templates

`adversarial-review` accepts `--git-commit`. The generated `implement` and `apply` tasks are Git-aware; `review` and `verify` remain read-only generic tasks.

`parallel-shards` accepts optional top-level JSON field `"git_commit": true`. Every `shard` and the `integrate` task is Git-aware; `verify` remains read-only. Existing workflow inputs without this field retain their current behavior.

Each Git-aware writer still claims and commits from its own prepared worktree and branch. Integration consumes shard commit artifacts using the existing workflow contract; the queue does not merge them automatically.

### Completion

A worker completes a Git-aware task with either a commit result or an explicit no-change result:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" complete \
  --task T-000001 --agent shard-http --token "$TOKEN" \
  --commit "$RESULT_HEAD" \
  --summary "HTTP shard complete"
```

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" complete \
  --task T-000004 --agent applier --token "$TOKEN" \
  --no-change \
  --summary "No valid findings required changes"
```

`--commit` and `--no-change` are mutually exclusive and are accepted only for Git-aware tasks. A Git-aware completion requires exactly one. Generic task completion rejects both flags.

`--no-change` succeeds only when the worktree is clean and `HEAD` still equals the recorded base. Workflow acceptance criteria may forbid a no-change result even though the queue supports it mechanically.

## Typed Path Resources

A Git-aware task requires at least one `file:` or `dir:` resource. These resources are the single source of truth for permitted commit paths.

Canonical forms are:

- `file:<repo-relative-posix-path>` for one path.
- `dir:<repo-relative-posix-directory>/` for a directory subtree.

Typed paths must be nonempty, relative, normalized POSIX paths. Reject absolute paths, empty segments, `.` or `..` segments, backslashes, NUL bytes, and a `dir:` value without a trailing slash. Do not resolve the path through the filesystem; validation is lexical and relative to the repository root.

For Git-aware claims in the same repository, typed path conflicts use path overlap rather than exact-string equality:

- Equal `file:` paths conflict.
- A `file:` path below a leased `dir:` conflicts.
- Equal or nested `dir:` paths conflict.
- Sibling files and disjoint directory subtrees do not conflict.

Arbitrary resources retain exact, case-sensitive matching. Generic tasks retain version-1 resource behavior. The Git-aware guarantee covers only tasks that opt in and claim through the Git-aware path.

## Claim Binding

### Observed Git identity

Before entering the queue transaction, `claim` inspects Git using argument-vector subprocess calls, never shell interpolation. It records:

- Canonical repository identity derived from the real path of the common Git directory.
- Canonical worktree root real path.
- Full symbolic branch ref, such as `refs/heads/feature/http`.
- Full object ID of the starting `HEAD`; do not assume SHA-1 length.

Claim rejects:

- A directory outside a Git worktree.
- Detached `HEAD`.
- A dirty index or worktree, including untracked files and excluding ignored files.
- A Git-aware task without a typed path resource.
- Unsafe or invalid typed path resources.

The repository identity persisted in ordinary projections is a digest of the canonical common-directory path. The raw common-directory path stays in the private binding needed for local revalidation and is omitted from status, TSV, events, and bounded error output.

### Derived ownership

The queue transaction attaches two internal exclusive ownership keys to the lease:

- `git-worktree:<digest-of-canonical-worktree>`
- `git-ref:<repository-id>:<digest-of-full-branch-ref>`

The keys are derived by the CLI and cannot be supplied or overridden as task resources. A candidate is ineligible when an unexpired Git-aware lease in the same repository owns the same worktree, the same branch, or an overlapping typed path scope.

Among Git-aware tasks compatible with the claiming worktree, selection still uses priority descending and task ID ascending. Selection, conflict evaluation, and storing the binding occur in one queue transaction.

Git subprocesses do not run inside the queue critical section. To narrow the resulting time-of-check/time-of-use window, the CLI re-inspects repository, worktree, branch, `HEAD`, and cleanliness immediately after the claim transaction. If the snapshot changed, it releases the just-created lease using its token and returns a bounded claim-drift error.

This protects cooperating queue workers. It cannot make Git state and queue state one cross-process atomic transaction or stop out-of-band Git commands.

## Completion Validation

Before changing queue state, Git-aware `complete` verifies the live token and inspects the bound worktree. It then checks:

1. Repository identity, canonical worktree, and full branch ref match the claim binding.
2. The worktree and index are clean under the same rule used at claim.
3. For `--commit`, current `HEAD` equals the supplied object ID.
4. The recorded base is an ancestor of the result head, and the result differs from the base.
5. Every changed path in `base..head` is covered by a declared `file:` or `dir:` resource.
6. For `--no-change`, current `HEAD` equals the base.

Multiple commits are allowed. Path validation diffs the base and head as trees, with rename detection disabled so both removal and addition paths are checked independently. Paths are consumed from NUL-delimited Git output.

After preflight validation, the queue completion transaction checks the live token again and persists the result. The CLI then re-inspects the binding and clean state. If Git drift occurs after the queue commit, it reports the completed queue result together with a distinct post-completion drift warning; it does not rewrite a terminal queue task. This residual race is part of the documented cooperative boundary.

## Compact Results and Output

Successful Git-aware completion persists compact evidence:

```json
{
  "summary": "HTTP shard complete",
  "artifacts": [],
  "git": {
    "branch": "refs/heads/feature/http",
    "base": "<full-object-id>",
    "head": "<full-object-id>",
    "commit_count": 3,
    "changed_path_count": 27
  }
}
```

The result does not store `changed_paths`. The path list is reproducible from `base..head` while the commits remain available.

- JSON source state stores full object IDs.
- Human tables render abbreviated object IDs.
- Default status and event projections continue to omit raw result bodies.
- A path-scope error prints at most the first 10 offending paths plus the omitted count.
- A dedicated detailed inspection may recompute paths from Git; it does not add them to queue state.

The no-change form stores identical base and head with zero commit and path counts.

## Lease Expiry and Recovery

Git is an external side effect, so lease expiry cannot assume that no commit occurred. When a Git-aware lease expires or is swept, the task follows the existing attempt and backoff rules and retains a private recovery binding containing the prior repository, worktree, branch, and base. The binding is preserved on both retryable `pending` tasks and attempt-exhausted `failed` tasks.

After retry backoff, a pending task derives as `git_recovery` instead of `ready`. Ordinary `claim` skips it. If the task exhausted its attempts, the operator first uses the existing `retry` command; retry returns it to pending without clearing the recovery binding. A worker then resumes it explicitly:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" claim \
  --agent shard-http-2 --task T-000001 --resume-git
```

Resume requires the same canonical repository, worktree, and branch; a clean worktree; and one of these head states:

- `HEAD == base`: no commit survived, so the new attempt continues from the original base.
- `base` is an ancestor of `HEAD`: one or more commits may have survived, so the new lease retains the original base and can complete after inspecting the commits.

Resume rejects a missing worktree, a changed branch, a divergent head, dirty state, or out-of-scope changes. It never resets or checks out Git state. If the original worktree cannot be restored safely, the coordinator may cancel the pending task and create a replacement task; cancellation records intentional abandonment without mutating Git.

A worker may not complete or publish using an expired token. Push remains outside this feature and must occur only after a live task has completed and the workflow's verification contract succeeds.

## Schema Version 2 and Migration

Schema version 2 adds:

- Task `git_mode`, either `null` or `"commit"`.
- Optional private `git_binding` inside a live claim.
- Optional private `git_recovery` on a pending or failed task; `retry` preserves it and terminal cancellation clears it.
- Required nullable `git` field inside every version-2 completed result: `null` for generic tasks and compact evidence for Git-aware tasks. Version-1 results retain their original two-field shape.
- The derived `git_recovery` display state.

New queues initialize as version 2. The new CLI continues to validate, display, and mutate version-1 queues using version-1 semantics. Version-1 queues cannot add Git-aware tasks or Git-aware workflows.

Migration is explicit:

```bash
python3 scripts/agent_queue.py --queue "$QUEUE" migrate --to 2
```

Migration runs under the existing queue lock, validates the version-1 source, creates a detached version-2 candidate, adds `git_mode: null` to every task, validates the candidate, increments the revision once, emits one migration event, and atomically replaces JSON before TSV. A failure leaves the version-1 JSON and TSV unchanged. Migration is one-way; no downgrade command is provided.

## Error Handling

Git command failures are queue runtime errors and do not mutate task state unless the command is the post-claim drift release. Error messages identify the failed invariant and a safe next action without exposing raw common-directory paths or unbounded path lists.

Representative error categories are:

- `git_not_worktree`
- `git_detached_head`
- `git_dirty`
- `git_binding_conflict`
- `git_claim_drift`
- `git_binding_mismatch`
- `git_head_mismatch`
- `git_non_descendant`
- `git_path_scope`
- `git_recovery_required`
- `git_recovery_mismatch`

Lease identity and expiry errors retain their existing exit code. Invalid Git task definitions use the existing input-error code. Git binding conflicts behave like no eligible work. Completion and recovery mismatches use the existing lease/identity failure code where the token is invalid and the runtime queue-error code where Git state is invalid. The implementation plan must keep the public exit-code table unambiguous.

## Security and Privacy

- Invoke Git without a shell.
- Parse path output using NUL delimiters.
- Treat task titles, summaries, branch names, and Git paths as untrusted display data.
- Do not expose raw common-directory or worktree paths in TSV, events, dashboard data, or ordinary errors.
- Do not store changed-path lists in queue state.
- Continue redacting lease tokens from every projection and event.
- Reject symlink or queue corruption through the existing queue validation path; Git inspection does not weaken queue locking.

## Test Strategy

Use temporary repositories and real Git worktrees for end-to-end CLI tests, with focused unit tests for typed path parsing and overlap.

Required coverage:

- Version-1 generic queues retain their existing mutation behavior.
- Version-1 to version-2 migration succeeds atomically and rolls back on failure.
- Git-aware creation rejects missing or invalid typed path resources.
- Clean branch claims succeed; dirty and detached claims fail without a lease.
- Concurrent claims reject the same worktree or branch.
- Distinct worktrees and branches with disjoint scopes can be leased concurrently.
- `file:`/`dir:` equality, nesting, sibling, and path-boundary cases behave correctly.
- Multiple descendant commits complete successfully.
- Non-descendant, wrong-HEAD, dirty, and out-of-scope completions fail without completing the task.
- Rename source and destination paths are both scope-checked.
- Explicit no-change completion requires `HEAD == base` and clean state.
- Expiry produces `git_recovery`; normal claim skips it; valid resume succeeds.
- Missing, divergent, dirty, or wrong-branch recovery fails closed.
- Success results omit `changed_paths` and store only counts plus full object IDs.
- Large out-of-scope diffs produce bounded errors.
- Existing adversarial-review and parallel-shards behavior is unchanged without Git opt-in.
- Git-enabled workflows assign Git mode only to writer roles.

## Documentation Changes

Implementation updates must keep one source of truth at each level:

- `SKILL.md`: add the short operational rule for preparing isolated worktrees, claiming before Git side effects, and completing before publication.
- `references/queue-schema.md`: own schema-v2 fields, state transitions, migration, binding, validation, recovery, output, and exit-code contracts.
- `references/workflow-templates.md`: own Git-enabled role assignment and commit artifact flow.
- `README.md`: explain the user-facing safety boundary and minimum commands without duplicating the detailed schema.

## Acceptance Criteria

The feature is complete when:

1. Existing generic queue tests pass unchanged or with schema-version-aware fixtures.
2. A Git-aware task cannot be leased concurrently on the same worktree or branch as another live Git-aware task.
3. Two disjoint Git-aware tasks can be leased and committed concurrently from separate worktrees and branches.
4. Completion cannot record a commit outside the claimed ancestry or typed path scope.
5. Claim and completion both reject dirty worktrees.
6. Multiple commits and explicit no-change results follow the contracts above.
7. Expired Git work can be resumed without treating external commits as exactly-once effects.
8. Successful queue state and normal projections never contain the changed-path list.
9. Version-1 queues keep generic behavior and migrate atomically only when explicitly requested.
10. Skill, schema, workflow, README, CLI help, and tests describe the same behavior without duplicated competing rules.
