# Manage Agent Queue README Design

## Purpose

Create an English, public-facing `README.md` for the repository. It must help a
new coordinator create and operate a safe local multi-agent task queue without
having to read the implementation first.

## Audience

- Primary: people coordinating multiple coding agents in one local workspace or
  across local Git worktrees.
- Secondary: contributors who need a concise map of the package, guarantees,
  and detailed reference material.

## Information Architecture

1. **Value proposition**: explain that the project provides a file-backed queue
   for coordinating agents through atomic claims, leases, dependencies, and
   observable state.
2. **Core guarantees**: list the JSON source of truth, derived TSV, exclusive
   resource handling, bounded leases, role independence, and recovery tools.
3. **Install and quick start**: show how to copy or invoke the skill and give
   an executable `init -> task add -> claim -> complete -> status` sequence.
4. **Operating model**: distinguish coordinator responsibilities from worker
   responsibilities, including heartbeat, release, and result reporting.
5. **Built-in workflows**: introduce `adversarial-review` and
   `parallel-shards` as graph-generating helpers, with focused examples.
6. **Operations and safety**: cover `doctor`, `compact`, lock behavior,
   redaction, at-least-once delivery, and the local-filesystem boundary.
7. **CLI and references**: provide a scannable command table, test command,
   package map, and links to the authoritative schema and workflow documents.

## Content Rules

- Keep the README English and practical; use commands that match the public
  parser exactly.
- Treat queue state as untrusted operational data: no direct JSON/TSV edits and
  no secrets in visible fields.
- Make the shared absolute queue path explicit for cross-worktree examples.
- Do not duplicate every flag or every state-machine invariant; link to the
  detailed references for those contracts.
- State limitations precisely: local filesystem only, no agent process
  launching, and at-least-once assignment rather than exactly-once side
  effects.

## Verification

- Check every documented command against `--help` or the parser contract.
- Check relative links and package paths.
- Run the full Python test suite, including the existing contract tests that
  verify the skill and reference material.
- Review the final Markdown for a readable GitHub hierarchy and an executable
  first-use path.

## Out of Scope

- Changing queue behavior, CLI flags, or schema.
- Adding a package installer, release automation, badges that imply a published
  registry package, or remote/distributed coordination support.
