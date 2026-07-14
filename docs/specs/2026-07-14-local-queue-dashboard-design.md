# Local Queue Dashboard Design

## Summary

Add a read-only local web dashboard to `manage-agent-queue`. The dashboard makes a live queue easy to follow without depending on Codex, Claude Code, GPT-specific UI, or an external service. An agent using the skill must ask the user before starting it, open the user's default browser only after consent, and stop the server when the coordination work ends.

The existing `status` and `events` commands remain the terminal fallback. This design does not add a separate terminal `watch` interface.

## Goals

- Make current workflow progress and recent changes understandable at a glance.
- Work in any agent tool that can run the Python CLI and, when permitted, open a browser.
- Make the feature discoverable from the normal skill and CLI flow.
- Keep the dashboard local, read-only, dependency-free, and safe to leave open during a coordination session.
- Tie the server lifecycle to the agent's queue-coordination work and clean it up reliably.

## Non-goals

- Mutating tasks from the browser.
- Starting, stopping, or messaging agents.
- Remote access, multi-machine coordination, authentication for network deployment, or a hosted service.
- Replacing the JSON source of truth, generated TSV, `status`, or `events`.
- Adding a JavaScript build system or third-party Python package.

## User Experience

### Consent and launch

When an agent begins coordinating through `manage-agent-queue`, the skill asks once:

> 실시간 큐 진행 상황을 브라우저에서 볼까요?

If the user agrees, the agent starts:

```bash
python3 scripts/agent_queue.py --queue /absolute/path/queue.json serve --open
```

The README's existing quick start defines the `$CLI` shell variable and shows the shorter equivalent:

```bash
$CLI serve --open
```

The command binds to `127.0.0.1` on an automatically selected available port, waits until the server is ready, prints the private local URL, and then opens the default browser. If browser opening is unavailable or denied, the command keeps serving and clearly prints the URL for manual opening.

If the user declines, the agent does not ask again during that coordination session. It gives the existing `status` and `events` commands as alternatives.

### Dashboard layout

The dashboard has two views:

1. **Workflows** is the default. It groups tasks by workflow and shows each workflow's completed count, total count, percentage, active count, and failed or blocked count.
2. **Activity** shows sanitized queue events in chronological order, with the newest change easy to locate.

The top of the Workflows view contains:

- queue ID, revision, and time since the last successful refresh;
- overall completed, active, ready, and blocked or failed counts;
- an attention area for failed tasks, blocked tasks, dependency failures, and leases expiring within two minutes.

Every task remains visible as one compact row. The default row contains state, task ID, title, assignee, and relevant elapsed or remaining time. Details such as dependencies, attempts, resources, and workflow metadata expand on demand. A task changed since the preceding successful refresh receives a temporary visual highlight. State is always conveyed by text and shape as well as color.

The layout removes secondary metadata as the viewport narrows, while retaining state, title, assignee, and warnings. The Activity view uses the same state vocabulary and task links as the Workflows view.

### Refresh behavior

The page checks the queue revision every two seconds. It fetches a new dashboard snapshot only when the revision changes. Events are requested incrementally after the last displayed event sequence. A manual refresh control retries immediately after an error.

The browser shows three connection states without discarding the last valid snapshot:

- **Live:** the latest poll succeeded.
- **Retrying:** the queue or server could not be read; the page displays the error and retries with a bounded backoff.
- **Stopped:** the local server has shut down; the page stops polling and explains that the agent session ended.

## CLI Contract

Add a `serve` command with these options:

```text
serve [--open] [--host 127.0.0.1] [--port 0]
      [--interval SECONDS] [--idle-timeout SECONDS]
```

- `--open` requests default-browser launch after server readiness.
- `--host` defaults to `127.0.0.1`. Version 1 rejects non-loopback hosts instead of implying remote security.
- `--port 0` asks the operating system for an available port.
- `--interval` defaults to `2` and is passed to the page as its polling interval.
- `--idle-timeout` defaults to `300`. The server exits after that many seconds with no dashboard or API request. Active browser polling prevents idle shutdown.

The command runs in the foreground. `Ctrl-C` performs a clean shutdown and returns exit code 0. Startup and runtime errors use the CLI's existing structured error behavior and nonzero exit codes.

The agent that started the server owns its cleanup. At the end of queue coordination, the skill requires the agent to stop the foreground process and verify that it exited. The idle timeout is a fallback for an abandoned browser or interrupted tool session, not the primary shutdown mechanism.

## Architecture

Keep the feature inside the existing standard-library Python CLI, separated into focused units:

- **Dashboard projection:** converts the validated queue snapshot and current time into sanitized workflow summaries, task rows, warnings, and event records. It reuses the existing derived-state and redaction rules.
- **HTTP server:** a `ThreadingHTTPServer`-based loopback server that serves fixed routes, validates the access token, applies response headers, and manages idle shutdown.
- **Static client:** checked-in HTML, CSS, and JavaScript assets stored beside the script. The files require no compilation and make no external requests.
- **Browser launcher:** uses Python's `webbrowser` module after the server socket is listening. Failure is reported without stopping the server.
- **Command orchestration:** parses `serve` options, generates the access token, starts the foreground server, handles signals, and emits the URL and shutdown messages.

The dashboard reads through the same locking and validation boundary as other CLI readers. Like `status`, producing a current projection automatically sweeps expired leases; the browser exposes no endpoint for administrative mutations such as complete, fail, retry, block, or cancel.

### HTTP surface

Every route is beneath a cryptographically random per-process token prefix:

```text
/<token>/
/<token>/assets/dashboard.css
/<token>/assets/dashboard.js
/<token>/api/revision
/<token>/api/snapshot
/<token>/api/events?after=<sequence>
/<token>/api/health
```

`revision` is a small polling response. `snapshot` returns only dashboard projection fields, not the raw queue document. `events` returns the existing sanitized event form and supports incremental retrieval. `health` lets the page distinguish a stopped session from a temporary queue read error while the server is reachable.

## Security and Privacy

- Bind only to loopback and reject a non-loopback `--host` in version 1.
- Generate the URL token with `secrets` for every server process and reject requests without an exact token match.
- Do not enable CORS and reject unexpected `Host` headers.
- Send a restrictive Content Security Policy that permits only same-origin scripts, styles, and connections; ship no external fonts, scripts, analytics, or images.
- Send `Cache-Control: no-store`, `Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`, and frame-denial headers.
- Never return the queue filesystem path, lock metadata, lease tokens, raw result bodies, or fields outside the explicit dashboard projection.
- Escape all queue-controlled text through DOM text nodes; do not inject it as HTML.
- Keep the server read-only from the browser's perspective.

## Discoverability and Documentation

- Describe `serve` in top-level `--help` as the local live workflow dashboard.
- Add concise examples, lifecycle behavior, privacy boundaries, and manual-URL fallback to `serve --help`.
- Make the README quick start show the consent-aware dashboard flow before static observation commands.
- Change the skill's Observe guidance to offer the dashboard once, then fall back to `status`, `events`, and generated TSV.
- Add a required cleanup step to the skill's coordination procedure.
- After `init`, include machine-readable next-action suggestions for `serve` and `status` without automatically opening anything.
- When `status` writes a human-readable table to a TTY, append a one-line hint for `serve --open`. Do not add the hint to JSON or TSV output.
- Keep all prompts and documentation explicit that browser opening happens only after user approval.

## Error Handling

- **Missing or invalid queue:** keep the server alive, return a bounded sanitized error to the page, and retry so a repaired queue can recover without relaunching.
- **Lock contention:** retain the last valid snapshot, show a retrying state, and retry with bounded backoff.
- **Browser launch failure:** print the URL and continue serving.
- **Asset or route error:** return a minimal fixed 404 response without filesystem details.
- **Client disconnect:** tolerate broken pipes without terminating the server.
- **Shutdown:** stop accepting requests, close the server socket, wait for request threads to finish within a bound, and then exit.
- **Unexpected internal error:** avoid tracebacks or queue content in HTTP responses; preserve normal CLI diagnostics on stderr.

## Testing

Use the existing `unittest` suite and temporary queue fixtures.

### Projection tests

- workflow grouping, progress counts, global counts, and stable ordering;
- warning precedence for failure, block, dependency failure, and lease expiry;
- changed-task identification between revisions;
- sanitization and absence of lease tokens, queue paths, and result bodies;
- narrow-layout data priorities represented in the client model.

### HTTP and lifecycle tests

- loopback-only host validation and automatic port selection;
- exact access-token and Host validation;
- revision, snapshot, incremental events, and health responses;
- security and cache headers on HTML, assets, and API responses;
- startup readiness before browser launch;
- browser-launch failure fallback;
- idle timeout, `Ctrl-C`, and explicit agent cleanup;
- missing, invalid, locked, and subsequently repaired queue behavior;
- shutdown with an in-flight or disconnected request.

### Client tests

Keep rendering logic as small pure JavaScript functions where practical. Validate the static client with fixture-driven tests for workflow rendering, activity appends, changed-row highlighting, responsive field priorities, and connection-state transitions. The Python suite also checks that assets exist, contain no external URLs, and use safe text rendering rather than HTML injection.

### Documentation contract tests

Extend the existing skill and README tests to require:

- the consent question before `serve --open`;
- the decline fallback;
- the cleanup obligation;
- the `serve` quick-start example and help description;
- no claim that the dashboard works remotely or mutates the queue.

## Acceptance Criteria

- A user can approve the offer, have an agent start the dashboard, and see a browser page without installing dependencies.
- Workflow progress, all task rows, important warnings, and recent activity update automatically while the queue changes.
- A declined offer causes no browser or server side effect and is not repeated in the same session.
- The same CLI command works independently of the agent product being used.
- The dashboard cannot mutate queue tasks and does not expose defined sensitive fields.
- Browser-open failure leaves a usable printed URL.
- Finishing the agent's coordination work stops the server; an abandoned inactive server self-terminates.
- Existing JSON, TSV, `status`, `events`, and queue state-machine contracts continue to pass their tests.
