# Local Queue Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This repository's `AGENTS.md` requires sequential work in the main thread; do not dispatch subagents.

**Goal:** Add a consent-aware, dependency-free local browser dashboard that shows workflow progress and queue activity in real time and shuts down with the coordinating agent session.

**Architecture:** Keep queue semantics in `agent_queue.py`, add a focused `queue_dashboard.py` module for projection and loopback HTTP serving, and ship build-free HTML/CSS/JavaScript assets beside the CLI. Browser endpoints receive only sanitized projections, use a per-process URL token, and expose no task mutation routes.

**Tech Stack:** Python 3 standard library (`argparse`, `http.server`, `json`, `secrets`, `threading`, `time`, `urllib`, `webbrowser`), static HTML/CSS/JavaScript, `unittest`.

---

## File Map

- Create `skills/manage-agent-queue/scripts/queue_dashboard.py`: pure dashboard projection, secure route dispatch, HTTP response headers, foreground server loop, idle shutdown, and browser launch.
- Create `skills/manage-agent-queue/scripts/dashboard/index.html`: accessible two-view dashboard shell and connection status.
- Create `skills/manage-agent-queue/scripts/dashboard/dashboard.css`: responsive workflow, task, warning, and activity presentation.
- Create `skills/manage-agent-queue/scripts/dashboard/dashboard.js`: revision polling, incremental events, safe DOM rendering, change highlighting, and stopped/retrying states.
- Modify `skills/manage-agent-queue/scripts/agent_queue.py`: parser contract, queue-backed data-source callbacks, `serve` orchestration, `init` next actions, and TTY-only `status` hint.
- Modify `skills/manage-agent-queue/scripts/test_agent_queue.py`: projection, HTTP security, lifecycle, CLI, asset, and documentation contract tests.
- Modify `skills/manage-agent-queue/SKILL.md`: ask-once consent, decline fallback, dashboard observation, and mandatory cleanup.
- Modify `skills/manage-agent-queue/references/queue-schema.md`: `serve` command, read behavior, loopback security, and lifecycle contract.
- Modify `README.md`: dashboard-first quick start, manual URL fallback, and terminal alternatives.
- Reference `docs/specs/2026-07-14-local-queue-dashboard-design.md`: approved behavior and acceptance criteria; change it only if implementation reveals a real contradiction.

## Task 1: Build the Sanitized Dashboard Projection

**Files:**
- Create: `skills/manage-agent-queue/scripts/queue_dashboard.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:22-25,2329-2490`

- [ ] **Step 1: Write failing projection tests**

Add the import beside `import agent_queue as aq`:

```python
import queue_dashboard as qd
```

Add this class after `StatusProjectionTests`:

```python
class DashboardProjectionTests(unittest.TestCase):
    NOW = "2026-07-10T06:00:00Z"

    def row(self, task_id, workflow, state, **overrides):
        row = {
            "id": task_id,
            "workflow": workflow,
            "role": "implement",
            "state": state,
            "priority": 10,
            "assignee": "",
            "lease_until": "",
            "attempts": "0/3",
            "depends_on": "",
            "blocked_by": "",
            "resources": "",
            "title": task_id,
        }
        row.update(overrides)
        return row

    def test_projection_groups_workflows_and_computes_counts(self):
        rows = [
            self.row("T-000001", "W-000001", "completed", title="Done"),
            self.row(
                "T-000002", "W-000001", "leased", title="Working",
                assignee="agent-1", lease_until="2026-07-10T06:01:30Z",
            ),
            self.row("T-000003", "", "ready", title="Loose"),
        ]

        value = qd.build_snapshot("demo", 7, rows, self.NOW)

        self.assertEqual("demo", value["queue_id"])
        self.assertEqual(7, value["revision"])
        self.assertEqual(
            {"total": 3, "completed": 1, "active": 1, "ready": 1,
             "attention": 1},
            value["counts"],
        )
        self.assertEqual(["W-000001", "unassigned"], [
            workflow["id"] for workflow in value["workflows"]
        ])
        self.assertEqual(50, value["workflows"][0]["progress_percent"])
        self.assertEqual("lease_expiring", value["warnings"][0]["kind"])

    def test_projection_warning_precedence_and_redaction_boundary(self):
        rows = [
            self.row("T-000004", "W-1", "blocked", blocked_by="T-000001"),
            self.row("T-000003", "W-1", "dependency_failed",
                     blocked_by="T-000002"),
            self.row("T-000002", "W-1", "failed"),
            self.row("T-000001", "W-1", "resource_conflict",
                     blocked_by="T-000009", resources="repo"),
        ]

        value = qd.build_snapshot("demo", 8, rows, self.NOW)

        self.assertEqual(
            ["failed", "blocked", "dependency_failed"],
            [warning["kind"] for warning in value["warnings"]],
        )
        serialized = json.dumps(value)
        self.assertNotIn("lease_token", serialized)
        self.assertNotIn("result", serialized)
        self.assertNotIn("description", serialized)

    def test_events_after_returns_detached_sanitized_sequence(self):
        events = [
            {"seq": 1, "at": self.NOW, "type": "task.added", "actor": "operator",
             "task_id": "T-000001", "revision": 1, "details": {}},
            {"seq": 2, "at": self.NOW, "type": "task.claimed", "actor": "agent-1",
             "task_id": "T-000001", "revision": 2,
             "details": {"lease_token": "must-not-leak", "agent_id": "agent-1"}},
        ]

        value = qd.events_after(events, 1)

        self.assertEqual([2], [event["seq"] for event in value])
        self.assertNotIn("lease_token", repr(value))
        value[0]["details"]["agent_id"] = "changed"
        self.assertEqual("agent-1", events[1]["details"]["agent_id"])
```

- [ ] **Step 2: Run the projection tests and verify RED**

Run:

```bash
python3 -m unittest \
  test_agent_queue.DashboardProjectionTests -v
```

from `skills/manage-agent-queue/scripts`.

Expected: import failure for `queue_dashboard` or missing `build_snapshot`.

- [ ] **Step 3: Implement the minimal pure projection**

Create `queue_dashboard.py` with these public projection functions and constants:

```python
#!/usr/bin/env python3
"""Render and serve a read-only local dashboard for an agent queue."""

import copy
from datetime import datetime, timezone


WARNING_STATES = ("failed", "blocked", "dependency_failed")
ACTIVE_STATES = {"leased"}
READY_STATES = {"ready"}


def _parse_utc(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )


def _warning(row, now):
    if row["state"] in WARNING_STATES:
        return {
            "kind": row["state"],
            "task_id": row["id"],
            "title": row["title"],
            "blocked_by": row["blocked_by"],
        }
    if row["state"] == "leased" and row["lease_until"]:
        remaining = int((_parse_utc(row["lease_until"]) - now).total_seconds())
        if 0 <= remaining <= 120:
            return {
                "kind": "lease_expiring",
                "task_id": row["id"],
                "title": row["title"],
                "remaining_seconds": remaining,
            }
    return None


def build_snapshot(queue_id, revision, rows, generated_at):
    now = _parse_utc(generated_at)
    grouped = {}
    warnings = []
    for source in rows:
        row = copy.deepcopy(source)
        workflow_id = row["workflow"] or "unassigned"
        grouped.setdefault(workflow_id, []).append(row)
        warning = _warning(row, now)
        if warning is not None:
            warnings.append(warning)

    workflows = []
    for workflow_id, tasks in grouped.items():
        completed = sum(task["state"] == "completed" for task in tasks)
        workflows.append({
            "id": workflow_id,
            "completed": completed,
            "total": len(tasks),
            "active": sum(task["state"] in ACTIVE_STATES for task in tasks),
            "attention": sum(task["state"] in WARNING_STATES for task in tasks),
            "progress_percent": round(completed * 100 / len(tasks)),
            "tasks": tasks,
        })

    warning_order = {name: index for index, name in enumerate(
        (*WARNING_STATES, "lease_expiring")
    )}
    warnings.sort(key=lambda value: (warning_order[value["kind"]], value["task_id"]))
    workflows.sort(key=lambda value: value["id"])
    return {
        "queue_id": queue_id,
        "revision": revision,
        "generated_at": generated_at,
        "counts": {
            "total": len(rows),
            "completed": sum(row["state"] == "completed" for row in rows),
            "active": sum(row["state"] in ACTIVE_STATES for row in rows),
            "ready": sum(row["state"] in READY_STATES for row in rows),
            "attention": len(warnings),
        },
        "warnings": warnings,
        "workflows": workflows,
    }


def events_after(events, sequence):
    result = []
    for source in events:
        if source["seq"] <= sequence:
            continue
        event = copy.deepcopy(source)
        if isinstance(event.get("details"), dict):
            event["details"].pop("lease_token", None)
        result.append(event)
    return result
```

- [ ] **Step 4: Run projection tests and verify GREEN**

Run:

```bash
python3 -m unittest test_agent_queue.DashboardProjectionTests -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit the projection boundary**

```bash
git add skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: 큐 대시보드 프로젝션 추가"
```

## Task 2: Add the Secure Loopback HTTP Surface

**Files:**
- Modify: `skills/manage-agent-queue/scripts/queue_dashboard.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`
- Create: `skills/manage-agent-queue/scripts/dashboard/index.html`
- Create: `skills/manage-agent-queue/scripts/dashboard/dashboard.css`
- Create: `skills/manage-agent-queue/scripts/dashboard/dashboard.js`

- [ ] **Step 1: Write failing route and security tests**

Add imports to the test file:

```python
import http.client
import threading
```

Add a `DashboardHttpTests` fixture that starts the server on an ephemeral port:

```python
class DashboardHttpTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = {"queue_id": "demo", "revision": 3, "workflows": []}
        self.events = [{"seq": 3, "type": "task.added", "details": {}}]
        self.server = qd.create_server(
            "127.0.0.1", 0, "fixed-token", 2,
            revision_loader=lambda: 3,
            snapshot_loader=lambda: self.snapshot,
            events_loader=lambda after: [
                event for event in self.events if event["seq"] > after
            ],
            asset_dir=SCRIPT_DIR / "dashboard",
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, host=None):
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2
        )
        connection.request(
            method, path,
            headers={"Host": host or f"127.0.0.1:{self.server.server_port}"},
        )
        response = connection.getresponse()
        body = response.read()
        headers = dict(response.getheaders())
        connection.close()
        return response.status, headers, body

    def test_token_host_and_fixed_routes_are_enforced(self):
        self.assertEqual(404, self.request("GET", "/wrong/api/revision")[0])
        self.assertEqual(
            400,
            self.request("GET", "/fixed-token/api/revision", host="evil.test")[0],
        )
        status, _headers, body = self.request(
            "GET", "/fixed-token/api/revision"
        )
        self.assertEqual(200, status)
        self.assertEqual({"revision": 3, "interval": 2}, json.loads(body))
        status, _headers, body = self.request(
            "GET", "/fixed-token/api/events?after=2"
        )
        self.assertEqual([3], [event["seq"] for event in json.loads(body)["events"]])
        self.assertEqual(
            404, self.request("GET", "/fixed-token/assets/../agent_queue.py")[0]
        )

    def test_every_success_response_has_security_headers(self):
        for path in (
            "/fixed-token/",
            "/fixed-token/assets/dashboard.css",
            "/fixed-token/assets/dashboard.js",
            "/fixed-token/api/snapshot",
        ):
            with self.subTest(path=path):
                status, headers, _body = self.request("GET", path)
                self.assertEqual(200, status)
                self.assertEqual("no-store", headers["Cache-Control"])
                self.assertEqual("no-referrer", headers["Referrer-Policy"])
                self.assertEqual("nosniff", headers["X-Content-Type-Options"])
                self.assertEqual("DENY", headers["X-Frame-Options"])
                self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
                self.assertNotIn("Access-Control-Allow-Origin", headers)

    def test_health_and_loader_errors_do_not_kill_server(self):
        self.server.dashboard.snapshot_loader = lambda: (_ for _ in ()).throw(
            RuntimeError("private /tmp/queue.json detail")
        )
        status, _headers, body = self.request("GET", "/fixed-token/api/snapshot")
        self.assertEqual(503, status)
        self.assertEqual({"error": "queue temporarily unavailable"}, json.loads(body))
        self.assertNotIn(b"/tmp/queue.json", body)
        self.assertEqual(200, self.request("GET", "/fixed-token/api/health")[0])
```

- [ ] **Step 2: Add the smallest valid static assets and verify RED is only server API**

Create the three asset files with valid minimal content:

```html
<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Agent Queue</title><link rel="stylesheet" href="assets/dashboard.css"></head><body><main id="app"></main><script src="assets/dashboard.js"></script></body></html>
```

```css
:root { color-scheme: light dark; }
```

```javascript
"use strict";
```

Run:

```bash
python3 -m unittest test_agent_queue.DashboardHttpTests -v
```

Expected: failure because `create_server` is not defined.

- [ ] **Step 3: Implement fixed-route HTTP serving**

Extend `queue_dashboard.py` with:

```python
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit


SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; img-src 'self'; base-uri 'none'; "
        "form-action 'none'; frame-ancestors 'none'"
    ),
}
ASSETS = {
    "": ("index.html", "text/html; charset=utf-8"),
    "assets/dashboard.css": ("dashboard.css", "text/css; charset=utf-8"),
    "assets/dashboard.js": ("dashboard.js", "text/javascript; charset=utf-8"),
}


class DashboardServer(ThreadingHTTPServer):
    """Threaded local server with bounded request thread cleanup."""

    daemon_threads = True
    block_on_close = False

    def __init__(self, *args, **kwargs):
        self._request_threads = set()
        self._request_threads_lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def process_request(self, request, client_address):
        thread = threading.Thread(
            target=self._tracked_request,
            args=(request, client_address),
            daemon=self.daemon_threads,
        )
        with self._request_threads_lock:
            self._request_threads.add(thread)
        thread.start()

    def _tracked_request(self, request, client_address):
        try:
            self.process_request_thread(request, client_address)
        finally:
            with self._request_threads_lock:
                self._request_threads.discard(threading.current_thread())

    def server_close(self, timeout=2.0):
        """Close the socket and wait at most timeout for active requests."""
        super().server_close()
        deadline = time.monotonic() + timeout
        while True:
            with self._request_threads_lock:
                active = [
                    thread
                    for thread in self._request_threads
                    if thread.is_alive()
                ]
            if not active:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            active[0].join(timeout=remaining)


def _handler_class():
    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            return

        def _send(self, status, body, content_type):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for name, value in SECURITY_HEADERS.items():
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _json(self, status, value):
            body = json.dumps(value, allow_nan=False, sort_keys=True).encode("utf-8")
            self._send(status, body, "application/json; charset=utf-8")

        def do_GET(self):
            app = self.server.dashboard
            app.last_request = time.monotonic()
            if self.headers.get("Host") not in {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }:
                self._json(400, {"error": "invalid host"})
                return
            parsed = urlsplit(self.path)
            prefix = f"/{app.token}/"
            provided_prefix = parsed.path[: len(prefix)]
            if len(provided_prefix) != len(prefix) or not secrets.compare_digest(
                provided_prefix, prefix
            ):
                self._json(404, {"error": "not found"})
                return
            route = parsed.path[len(prefix):]
            try:
                if route in ASSETS:
                    filename, content_type = ASSETS[route]
                    self._send(200, (app.asset_dir / filename).read_bytes(), content_type)
                elif route == "api/revision":
                    self._json(200, {
                        "revision": app.revision_loader(),
                        "interval": app.interval,
                    })
                elif route == "api/snapshot":
                    self._json(200, app.snapshot_loader())
                elif route == "api/events":
                    values = parse_qs(parsed.query)
                    try:
                        after = int(values.get("after", ["0"])[0])
                    except ValueError:
                        self._json(400, {"error": "invalid after parameter"})
                        return
                    self._json(200, {"events": app.events_loader(after)})
                elif route == "api/health":
                    self._json(200, {"ok": True})
                else:
                    self._json(404, {"error": "not found"})
            except (OSError, RuntimeError, ValueError):
                self._json(503, {"error": "queue temporarily unavailable"})

    return DashboardHandler


def create_server(host, port, token, interval, revision_loader,
                  snapshot_loader, events_loader, asset_dir):
    if host != "127.0.0.1":
        raise ValueError("dashboard host must be 127.0.0.1")
    server = DashboardServer((host, port), _handler_class())
    server.dashboard = SimpleNamespace(
        token=token,
        interval=interval,
        revision_loader=revision_loader,
        snapshot_loader=snapshot_loader,
        events_loader=events_loader,
        asset_dir=Path(asset_dir),
        last_request=time.monotonic(),
    )
    return server
```

Keep the route map exact; do not serve arbitrary paths from the asset directory.

- [ ] **Step 4: Run HTTP and projection tests**

Run:

```bash
python3 -m unittest \
  test_agent_queue.DashboardProjectionTests \
  test_agent_queue.DashboardHttpTests -v
```

Expected: 6 tests pass.

- [ ] **Step 5: Commit the secure HTTP boundary**

```bash
git add skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/dashboard \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: 로컬 큐 대시보드 서버 추가"
```

## Task 3: Wire the Queue Data Source and `serve` CLI

**Files:**
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:1-25,3290-3330,3336-3343,3401-3499,3502-3710`
- Modify: `skills/manage-agent-queue/scripts/queue_dashboard.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:4139-4680`

- [ ] **Step 1: Write failing parser, data-source, and process tests**

Add:

```python
class DashboardCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "queue.json"
        aq.initialize_queue(self.queue, "demo", aq.fixed_config())
        state = aq.load_state(self.queue)
        aq.add_task(state, {"title": "Visible", "workflow_id": "W-000001"})
        aq.commit_state(self.queue, state, aq.utc_now())

    def test_parser_exposes_bounded_serve_options(self):
        args = aq.build_parser().parse_args([
            "serve", "--open", "--port", "0", "--interval", "1",
            "--idle-timeout", "30",
        ])
        self.assertTrue(args.open_browser)
        self.assertEqual("127.0.0.1", args.host)
        self.assertEqual(0, args.port)
        self.assertEqual(1, args.interval)
        self.assertEqual(30, args.idle_timeout)
        for arguments in (("serve", "--port", "-1"),
                          ("serve", "--port", "65536"),
                          ("serve", "--host", "0.0.0.0")):
            self.assertEqual(2, run_cli(*arguments).returncode)

    def test_dashboard_loaders_return_current_sanitized_data(self):
        loaders = aq.dashboard_loaders(self.queue)
        revision = loaders.revision()
        snapshot = loaders.snapshot()
        events = loaders.events(0)
        self.assertEqual(snapshot["revision"], revision)
        self.assertEqual("Visible", snapshot["workflows"][0]["tasks"][0]["title"])
        self.assertNotIn("lease_token", json.dumps(snapshot) + json.dumps(events))

    def test_serve_prints_ready_url_and_stops_on_sigint(self):
        process = subprocess.Popen(
            [sys.executable, str(SCRIPT_PATH), "--queue", str(self.queue),
             "serve", "--port", "0", "--idle-timeout", "30"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        ready = process.stdout.readline().strip()
        self.assertRegex(ready, r"^http://127\.0\.0\.1:\d+/[A-Za-z0-9_-]+/$")
        process.send_signal(getattr(__import__("signal"), "SIGINT"))
        stdout, stderr = process.communicate(timeout=3)
        self.assertEqual(0, process.returncode, stderr)
        self.assertIn("dashboard stopped", stdout)
```

- [ ] **Step 2: Run the CLI tests and verify RED**

Run:

```bash
python3 -m unittest test_agent_queue.DashboardCliTests -v
```

Expected: failures for the missing parser, loaders, and serving entry point.

- [ ] **Step 3: Add parser validation and queue-backed callbacks**

In `agent_queue.py`, import the module:

```python
from types import SimpleNamespace

import queue_dashboard as dashboard
```

Add bounded port and loopback parsers beside `_positive`:

```python
def _port(value):
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("must be an integer from 0 to 65535") from error
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError("must be an integer from 0 to 65535")
    return number


def _loopback_host(value):
    if value != "127.0.0.1":
        raise argparse.ArgumentTypeError("must be 127.0.0.1")
    return value
```

Add the parser before `return parser`:

```python
    serve = commands.add_parser(
        "serve", help="show the live workflow dashboard in a local browser"
    )
    serve.add_argument("--open", dest="open_browser", action="store_true")
    serve.add_argument("--host", type=_loopback_host, default="127.0.0.1")
    serve.add_argument("--port", type=_port, default=0)
    serve.add_argument("--interval", type=_positive, default=2)
    serve.add_argument("--idle-timeout", type=_positive, default=300)
```

Add a callback bundle before `_run_command`:

```python
def dashboard_loaders(path):
    path = Path(path)

    def current():
        state, now, _projection = _status_transaction_details(path)
        rows = status_rows(state, now)
        return state, now, rows

    def revision():
        state, _now, _rows = current()
        return state["revision"]

    def snapshot():
        state, now, rows = current()
        return dashboard.build_snapshot(
            state["queue_id"], state["revision"], rows, now
        )

    def events(after):
        state = read_queue_snapshot(path)
        return dashboard.events_after(state["events"], after)

    return SimpleNamespace(revision=revision, snapshot=snapshot, events=events)
```

- [ ] **Step 4: Implement the foreground lifecycle and browser fallback**

Add to `queue_dashboard.py`:

```python
import secrets
import webbrowser


def serve(host, port, interval, idle_timeout, open_browser,
          revision_loader, snapshot_loader, events_loader, asset_dir,
          output):
    token = secrets.token_urlsafe(24)
    server = create_server(
        host, port, token, interval, revision_loader, snapshot_loader,
        events_loader, asset_dir,
    )
    url = f"http://{host}:{server.server_port}/{token}/"
    output.write(url + "\n")
    output.flush()
    if open_browser and not webbrowser.open(url):
        output.write(f"browser did not open; visit {url}\n")
        output.flush()
    server.timeout = min(0.5, idle_timeout)
    try:
        while time.monotonic() - server.dashboard.last_request < idle_timeout:
            server.handle_request()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    output.write("dashboard stopped\n")
    output.flush()
    return 0
```

Handle `serve` before normal `_run_command` result emission in `main`, because it is a long-running streaming command:

```python
        if args.command == "serve":
            loaders = dashboard_loaders(path)
            try:
                return dashboard.serve(
                    args.host, args.port, args.interval, args.idle_timeout,
                    args.open_browser, loaders.revision, loaders.snapshot,
                    loaders.events, Path(__file__).with_name("dashboard"),
                    sys.stdout,
                )
            except OSError as error:
                raise QueueError(f"cannot start dashboard: {error}") from error
```

Place this branch immediately after queue-path resolution and before `_run_command(args, path)`. Preserve existing `QueueError` handling.

- [ ] **Step 5: Run the CLI and existing parser suites**

Run:

```bash
python3 -m unittest \
  test_agent_queue.DashboardCliTests \
  test_agent_queue.QueueCliTests.test_parser_help_and_invalid_arguments_use_argparse_code_two \
  test_agent_queue.QueueCliTests.test_status_events_export_and_crash_repair_do_not_bump_revision -v
```

Expected: all selected tests pass; the subprocess test exits within 3 seconds.

- [ ] **Step 6: Commit the CLI lifecycle**

```bash
git add skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "feat: 큐 대시보드 실행 명령 추가"
```

## Task 4: Implement the Browser Dashboard

**Files:**
- Modify: `skills/manage-agent-queue/scripts/dashboard/index.html`
- Modify: `skills/manage-agent-queue/scripts/dashboard/dashboard.css`
- Modify: `skills/manage-agent-queue/scripts/dashboard/dashboard.js`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing static client contract tests**

Add:

```python
class DashboardAssetTests(unittest.TestCase):
    def setUp(self):
        self.assets = SCRIPT_DIR / "dashboard"

    def test_assets_are_build_free_local_and_accessible(self):
        html = (self.assets / "index.html").read_text(encoding="utf-8")
        css = (self.assets / "dashboard.css").read_text(encoding="utf-8")
        javascript = (self.assets / "dashboard.js").read_text(encoding="utf-8")
        combined = html + css + javascript
        self.assertIn('id="workflow-view"', html)
        self.assertIn('id="activity-view"', html)
        self.assertIn('aria-live="polite"', html)
        self.assertIn("@media (max-width: 720px)", css)
        self.assertNotRegex(combined, r"https?://")

    def test_client_polls_revision_and_uses_safe_dom_apis(self):
        javascript = (self.assets / "dashboard.js").read_text(encoding="utf-8")
        for required in (
            "api/revision", "api/snapshot", "api/events?after=",
            "textContent", "setTimeout", "data-task-id", "manual-refresh",
            "remainingTime", "updatedTime", "retryDelay", "Retrying", "Stopped",
            "queue temporarily unavailable", "dashboard server ended",
        ):
            self.assertIn(required, javascript)
        for forbidden in ("innerHTML", "insertAdjacentHTML", "eval(", "document.write"):
            self.assertNotIn(forbidden, javascript)
```

- [ ] **Step 2: Run asset tests and verify RED**

Run:

```bash
python3 -m unittest test_agent_queue.DashboardAssetTests -v
```

Expected: failures for missing views, accessibility markers, responsive CSS, and polling code.

- [ ] **Step 3: Build the semantic HTML shell**

Replace `index.html` with a document containing these fixed IDs and no inline script or style:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Agent Queue</title>
  <link rel="stylesheet" href="assets/dashboard.css">
</head>
<body>
  <header class="topbar">
    <div><p class="eyebrow">Agent queue</p><h1 id="queue-title">Loading…</h1></div>
    <div class="connection-actions">
      <div>
        <div id="connection" class="connection" aria-live="polite">Connecting</div>
        <div id="last-updated" class="muted">No successful refresh yet</div>
      </div>
      <button id="manual-refresh" type="button">Refresh now</button>
    </div>
  </header>
  <nav class="tabs" aria-label="Dashboard views">
    <button id="workflow-tab" type="button" aria-controls="workflow-view" aria-selected="true">Workflows</button>
    <button id="activity-tab" type="button" aria-controls="activity-view" aria-selected="false">Activity</button>
  </nav>
  <main>
    <section id="summary" aria-label="Queue summary"></section>
    <section id="warnings" aria-label="Attention"></section>
    <section id="workflow-view" aria-labelledby="workflow-tab"></section>
    <section id="activity-view" aria-labelledby="activity-tab" hidden><ol id="activity-list"></ol></section>
  </main>
  <template id="empty-template"><p class="empty">No queue activity yet.</p></template>
  <script src="assets/dashboard.js"></script>
</body>
</html>
```

- [ ] **Step 4: Add responsive state-aware styles**

Implement CSS variables and component classes used by the client. The stylesheet must include these complete responsive rules:

```css
:root {
  color-scheme: light dark;
  font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
  --surface: #15171a; --panel: #202328; --text: #f4f5f6;
  --muted: #a8afb8; --line: #343943; --accent: #70a7ff;
  --ok: #59c98b; --warn: #f1bd62; --danger: #ff7777;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--surface); color: var(--text); }
button { font: inherit; }
.topbar, main, .tabs { width: min(1120px, calc(100% - 32px)); margin-inline: auto; }
.topbar { display: flex; justify-content: space-between; gap: 24px; padding: 28px 0 18px; }
.eyebrow { margin: 0; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }
h1 { margin: 4px 0 0; font-size: 1.5rem; }
.connection-actions { display: flex; align-items: center; gap: 10px; }
.connection { color: var(--muted); }
.connection.live { color: var(--ok); }
.connection.retrying { color: var(--warn); }
.connection.stopped { color: var(--danger); }
.tabs { display: flex; gap: 8px; border-bottom: 1px solid var(--line); }
.tabs button { padding: 10px 14px; border: 0; background: transparent; color: var(--muted); }
.tabs button[aria-selected="true"] { color: var(--text); border-bottom: 2px solid var(--accent); }
main { padding: 20px 0 48px; }
.summary-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; }
.card, .workflow, .warning { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; }
.card { padding: 14px; }
.workflow { margin-top: 14px; overflow: hidden; }
.workflow-header, .task-row { display: grid; grid-template-columns: 110px minmax(220px, 1fr) 160px 130px; gap: 12px; padding: 12px 14px; align-items: center; }
.task-row { border-top: 1px solid var(--line); }
.task-row.changed { animation: changed 2.5s ease-out; }
.task-details { grid-column: 1 / -1; color: var(--muted); }
.warning { margin: 10px 0; padding: 12px 14px; border-color: var(--warn); }
.state-completed { color: var(--ok); }
.state-failed, .state-blocked, .state-dependency_failed { color: var(--danger); }
.muted { color: var(--muted); }
@keyframes changed { from { background: color-mix(in srgb, var(--accent) 28%, transparent); } }
@media (max-width: 720px) {
  .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .workflow-header, .task-row { grid-template-columns: 86px minmax(0, 1fr); }
  .task-assignee, .task-time, .workflow-secondary { display: none; }
  .topbar { align-items: flex-start; flex-direction: column; gap: 8px; }
}
```

- [ ] **Step 5: Implement safe polling and rendering**

In `dashboard.js`, derive the token base from `location.pathname`, create every queue-controlled string with `textContent`, and implement these functions:

```javascript
"use strict";

const base = location.pathname.endsWith("/") ? location.pathname : `${location.pathname}/`;
const state = {
  revision: null, eventSequence: 0, taskFingerprints: new Map(),
  stopped: false, polling: false, interval: 2, retryDelay: 2, timer: null,
  lastSuccess: null
};
const byId = (id) => document.getElementById(id);
const element = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = String(text);
  return node;
};
const api = async (path) => {
  const response = await fetch(`${base}api/${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
};
const setConnection = (label, className) => {
  const node = byId("connection");
  node.textContent = label;
  node.className = `connection ${className}`;
};
const fingerprint = (task) => JSON.stringify([
  task.state, task.assignee, task.lease_until, task.attempts, task.blocked_by
]);
const stateLabel = (value) => ({
  completed: "✓ Completed", leased: "→ Active", ready: "○ Ready",
  failed: "! Failed", blocked: "! Blocked",
  dependency_failed: "! Dependency failed",
  waiting_dependency: "· Waiting", waiting_retry: "· Retrying",
  resource_conflict: "· Resource conflict", cancelled: "– Cancelled"
}[value] || value);
const remainingTime = (leaseUntil) => {
  if (!leaseUntil) return "";
  const seconds = Math.max(0, Math.floor((Date.parse(leaseUntil) - Date.now()) / 1000));
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${seconds % 60}s left`;
};
const updatedTime = () => {
  if (state.lastSuccess === null) return "No successful refresh yet";
  const seconds = Math.max(0, Math.floor((Date.now() - state.lastSuccess) / 1000));
  return seconds < 2 ? "Updated just now" : `Updated ${seconds}s ago`;
};
const updateStatusTime = () => {
  byId("last-updated").textContent = updatedTime();
  setTimeout(updateStatusTime, 1000);
};
const updateTimes = () => {
  for (const node of document.querySelectorAll("[data-lease-until]")) {
    node.textContent = remainingTime(node.dataset.leaseUntil);
  }
};

function renderTask(task, openTasks) {
  const row = element("details", "task-row");
  row.setAttribute("data-task-id", task.id);
  if (openTasks.has(task.id)) row.open = true;
  const previous = state.taskFingerprints.get(task.id);
  const next = fingerprint(task);
  if (previous !== undefined && previous !== next) row.classList.add("changed");
  state.taskFingerprints.set(task.id, next);
  const time = element("span", "task-time", remainingTime(task.lease_until));
  time.dataset.leaseUntil = task.lease_until || "";
  row.append(
    element("summary", `state-${task.state}`, stateLabel(task.state)),
    element("span", "task-title", `${task.id}  ${task.title}`),
    element("span", "task-assignee", task.assignee || "Unassigned"),
    time
  );
  row.append(element(
    "div", "task-details",
    `Attempts ${task.attempts} · Depends on ${task.depends_on || "none"} · Resources ${task.resources || "none"}`
  ));
  return row;
}

function renderSnapshot(snapshot) {
  byId("queue-title").textContent = `${snapshot.queue_id} · rev ${snapshot.revision}`;
  const summary = byId("summary");
  summary.replaceChildren();
  summary.className = "summary-grid";
  for (const [label, value] of Object.entries(snapshot.counts)) {
    summary.append(element("article", "card", `${label}  ${value}`));
  }
  const warnings = byId("warnings");
  warnings.replaceChildren();
  for (const warning of snapshot.warnings) {
    warnings.append(element("article", "warning", `${warning.kind} · ${warning.task_id} · ${warning.title}`));
  }
  const workflows = byId("workflow-view");
  const openTasks = new Set(
    Array.from(
      document.querySelectorAll("details[data-task-id][open]"),
      (node) => node.dataset.taskId
    )
  );
  workflows.replaceChildren();
  if (snapshot.workflows.length === 0) {
    workflows.append(byId("empty-template").content.cloneNode(true));
  }
  for (const workflow of snapshot.workflows) {
    const section = element("section", "workflow");
    const header = element("header", "workflow-header");
    header.append(
      element("strong", "", workflow.id),
      element("span", "", `${workflow.completed}/${workflow.total} · ${workflow.progress_percent}%`),
      element("span", "workflow-secondary", `${workflow.active} active`),
      element("span", "workflow-secondary", `${workflow.attention} attention`)
    );
    section.append(header, ...workflow.tasks.map((task) => renderTask(task, openTasks)));
    workflows.append(section);
  }
}

async function refreshEvents() {
  const payload = await api(`events?after=${state.eventSequence}`);
  const list = byId("activity-list");
  for (const event of payload.events) {
    list.append(element("li", "activity-item", `${event.at || ""} · ${event.type} · ${event.task_id || "queue"}`));
    state.eventSequence = Math.max(state.eventSequence, event.seq);
  }
}

const schedule = (seconds) => {
  clearTimeout(state.timer);
  state.timer = setTimeout(poll, seconds * 1000);
};

async function poll() {
  if (state.stopped || state.polling) return;
  state.polling = true;
  let nextDelay = state.interval;
  try {
    const current = await api("revision");
    state.interval = current.interval;
    if (current.revision !== state.revision) {
      const snapshot = await api("snapshot");
      renderSnapshot(snapshot);
      await refreshEvents();
      state.revision = snapshot.revision;
    }
    updateTimes();
    state.lastSuccess = Date.now();
    state.retryDelay = state.interval;
    nextDelay = state.interval;
    setConnection("Live", "live");
  } catch (_error) {
    setConnection("Retrying · queue temporarily unavailable", "retrying");
    try {
      await api("health");
      nextDelay = state.retryDelay;
      state.retryDelay = Math.min(state.retryDelay * 2, 15);
    } catch (_healthError) {
      state.stopped = true;
      setConnection("Stopped · dashboard server ended", "stopped");
    }
  } finally {
    state.polling = false;
    if (!state.stopped) schedule(nextDelay);
  }
}

function selectView(name) {
  const workflows = name === "workflow";
  byId("workflow-view").hidden = !workflows;
  byId("activity-view").hidden = workflows;
  byId("workflow-tab").setAttribute("aria-selected", String(workflows));
  byId("activity-tab").setAttribute("aria-selected", String(!workflows));
}

byId("workflow-tab").addEventListener("click", () => selectView("workflow"));
byId("activity-tab").addEventListener("click", () => selectView("activity"));
byId("manual-refresh").addEventListener("click", () => {
  clearTimeout(state.timer);
  state.stopped = false;
  state.retryDelay = state.interval;
  poll();
});
updateStatusTime();
poll();
```

The `api/revision` response carries the validated `--interval` value, so the client uses the selected polling interval without inline script. Failed queue reads double the retry delay up to 15 seconds; a successful read resets it. Time labels update on every successful poll even when the queue revision is unchanged.

- [ ] **Step 6: Run asset, HTTP, and projection tests**

Run:

```bash
python3 -m unittest \
  test_agent_queue.DashboardAssetTests \
  test_agent_queue.DashboardHttpTests \
  test_agent_queue.DashboardProjectionTests -v
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit the browser client**

```bash
git add skills/manage-agent-queue/scripts/dashboard \
  skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py \
  docs/specs/2026-07-14-local-queue-dashboard-design.md
git commit -m "feat: 큐 워크플로 대시보드 화면 추가"
```

## Task 5: Make Lifecycle and Recovery Behavior Deterministic

**Files:**
- Modify: `skills/manage-agent-queue/scripts/queue_dashboard.py`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py`

- [ ] **Step 1: Write failing idle, disconnect, and recovery tests**

Add tests that use short bounded times and injectable clock/browser functions:

```python
class DashboardLifecycleTests(unittest.TestCase):
    def in_flight_server(self):
        started = threading.Event()
        release = threading.Event()

        def load_snapshot():
            started.set()
            release.wait(timeout=2)
            return {"revision": 1}

        server = qd.create_server(
            "127.0.0.1", 0, "token", 2, lambda: 1, load_snapshot,
            lambda _after: [], SCRIPT_DIR / "dashboard",
        )
        serving = threading.Thread(target=server.serve_forever, daemon=True)
        serving.start()
        response = {}
        request = threading.Thread(
            target=lambda: response.setdefault(
                "value", request_server(server, "/token/api/snapshot")
            ),
            daemon=True,
        )
        request.start()
        self.assertTrue(started.wait(timeout=1))
        server.shutdown()
        serving.join(timeout=1)
        self.assertFalse(serving.is_alive())
        return server, request, release, response

    def test_server_close_waits_for_inflight_request_within_bound(self):
        server, request, release, response = self.in_flight_server()
        timer = threading.Timer(0.05, release.set)
        timer.start()
        self.assertTrue(server.server_close(timeout=1))
        timer.join(timeout=1)
        request.join(timeout=1)
        self.assertFalse(request.is_alive())
        self.assertEqual(200, response["value"][0])

    def test_server_close_returns_after_bound_with_active_request(self):
        server, request, release, _response = self.in_flight_server()
        started = time.monotonic()
        self.assertFalse(server.server_close(timeout=0.05))
        self.assertLess(time.monotonic() - started, 0.25)
        release.set()
        request.join(timeout=1)
        self.assertFalse(request.is_alive())

    def test_idle_server_exits_without_browser_requests(self):
        output = io.StringIO()
        started = time.monotonic()
        code = qd.serve(
            "127.0.0.1", 0, 2, 1, False,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard", output=output,
            browser_open=lambda _url: True,
        )
        self.assertEqual(0, code)
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertIn("dashboard stopped", output.getvalue())

    def test_browser_failure_prints_manual_url_and_keeps_serving(self):
        output = io.StringIO()
        code = qd.serve(
            "127.0.0.1", 0, 2, 1, True,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard", output=output,
            browser_open=lambda _url: False,
        )
        self.assertEqual(0, code)
        self.assertIn("browser did not open; visit http://127.0.0.1:", output.getvalue())

    def test_browser_exception_prints_manual_url_and_closes_cleanly(self):
        output = io.StringIO()

        def fail_to_open(_url):
            raise qd.webbrowser.Error("no browser")

        code = qd.serve(
            "127.0.0.1", 0, 2, 1, True,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard", output=output,
            browser_open=fail_to_open,
        )
        self.assertEqual(0, code)
        self.assertIn("browser did not open; visit http://127.0.0.1:", output.getvalue())
        self.assertIn("dashboard stopped", output.getvalue())

    def test_snapshot_loader_recovers_after_one_failure(self):
        attempts = iter((RuntimeError("corrupt"), {"revision": 2}))

        def load():
            result = next(attempts)
            if isinstance(result, Exception):
                raise result
            return result

        server = qd.create_server(
            "127.0.0.1", 0, "token", 2, lambda: 2, load,
            lambda _after: [], SCRIPT_DIR / "dashboard",
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join(2)))
        # Use the DashboardHttpTests request helper shape for both calls.
        self.assertEqual(503, request_server(server, "/token/api/snapshot")[0])
        status, _headers, body = request_server(server, "/token/api/snapshot")
        self.assertEqual(200, status)
        self.assertEqual(2, json.loads(body)["revision"])
```

Extract the duplicated HTTP request code into a top-level `request_server(server, path, host=None)` helper used by both HTTP test classes.

- [ ] **Step 2: Run lifecycle tests and verify RED**

Run:

```bash
python3 -m unittest test_agent_queue.DashboardLifecycleTests -v
```

Expected: failure because `serve` does not accept injectable `browser_open`; recovery may expose an unhandled iterator error until the route boundary is corrected.

- [ ] **Step 3: Inject browser opening and bound cleanup**

Change the public `serve` signature and call site:

```python
def serve(host, port, interval, idle_timeout, open_browser,
          revision_loader, snapshot_loader, events_loader, asset_dir,
          output, browser_open=webbrowser.open):
    # Create the server and print its manual URL before this boundary.
    try:
        opened = True
        if open_browser:
            try:
                opened = browser_open(url)
            except (OSError, webbrowser.Error):
                opened = False
        if not opened:
            output.write(f"browser did not open; visit {url}\n")
            output.flush()
        # Run the existing bounded handle_request loop here.
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close(timeout=2.0)
```

Catch only expected queue/data exceptions in routes. Let programmer errors fail tests instead of converting every exception into a retry response. Treat `OSError`, `UnicodeError`, JSON validation errors passed in through the data-source adapter, lock timeout, and queue errors as unavailable; have `agent_queue.dashboard_loaders` wrap its existing `QueueError`/`InvariantError` failures as `dashboard.DashboardDataUnavailable("queue temporarily unavailable")`.

Add the explicit boundary:

```python
class DashboardDataUnavailable(Exception):
    """A bounded client-safe dashboard read failure."""


# In the handler:
except DashboardDataUnavailable:
    self._json(503, {"error": "queue temporarily unavailable"})
```

- [ ] **Step 4: Run all dashboard tests**

Run:

```bash
python3 -m unittest \
  test_agent_queue.DashboardProjectionTests \
  test_agent_queue.DashboardHttpTests \
  test_agent_queue.DashboardCliTests \
  test_agent_queue.DashboardAssetTests \
  test_agent_queue.DashboardLifecycleTests -v
```

Expected: all dashboard tests pass without leaked processes or sockets.

- [ ] **Step 5: Commit lifecycle hardening**

```bash
git add skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "fix: 큐 대시보드 수명주기 안정화"
```

## Task 6: Add Consent-Aware Discovery and Documentation

**Files:**
- Modify: `skills/manage-agent-queue/SKILL.md:8-46`
- Modify: `skills/manage-agent-queue/references/queue-schema.md:190-235`
- Modify: `README.md:18-90`
- Modify: `skills/manage-agent-queue/scripts/agent_queue.py:3401-3499,3506-3520,3670-3710`
- Modify: `skills/manage-agent-queue/scripts/test_agent_queue.py:28-118,4139-4680`

- [ ] **Step 1: Write failing discovery and consent contract tests**

Extend `SkillContractTests`:

```python
    def test_dashboard_requires_consent_fallback_and_cleanup(self):
        skill = (self.skill_dir / "SKILL.md").read_text(encoding="utf-8")
        readme = (self.skill_dir.parent.parent / "README.md").read_text(encoding="utf-8")
        schema = (self.skill_dir / "references" / "queue-schema.md").read_text(
            encoding="utf-8"
        )
        for required in (
            "실시간 큐 진행 상황을 브라우저에서 볼까요?",
            "serve --open", "ask once", "status", "events", "stop",
        ):
            self.assertIn(required, skill)
        self.assertLess(
            skill.index("실시간 큐 진행 상황을 브라우저에서 볼까요?"),
            skill.index("serve --open"),
        )
        self.assertIn("$CLI serve --open", readme)
        self.assertIn("manual", readme.lower())
        self.assertIn("`serve`", schema)
        self.assertIn("127.0.0.1", schema)
        self.assertIn("read-only", schema)
```

Add CLI discovery tests:

```python
    def test_init_and_human_status_discover_dashboard_without_changing_machine_output(self):
        output = self.init()
        self.assertEqual(
            ["serve --open", "status"], output["next_actions"]
        )
        self.add("work")
        table = self.cli("status")
        self.assertNotIn("serve --open", table.stdout)  # captured stdout is not a TTY
        machine = self.json_output(self.cli("status", "--format", "json"))
        self.assertNotIn("hint", machine)

    def test_help_names_live_local_dashboard(self):
        top = run_cli("--help")
        serve = run_cli("serve", "--help")
        self.assertEqual(0, top.returncode)
        self.assertIn("live workflow dashboard", top.stdout)
        self.assertIn("127.0.0.1", serve.stdout)
        self.assertIn("--idle-timeout", serve.stdout)
```

- [ ] **Step 2: Run discovery tests and verify RED**

Run:

```bash
python3 -m unittest \
  test_agent_queue.SkillContractTests.test_dashboard_requires_consent_fallback_and_cleanup \
  test_agent_queue.QueueCliTests.test_init_and_human_status_discover_dashboard_without_changing_machine_output \
  test_agent_queue.QueueCliTests.test_help_names_live_local_dashboard -v
```

Expected: failures because the docs and `next_actions` do not yet mention the dashboard.

- [ ] **Step 3: Update the skill's required operating sequence**

Add this section after `Establish One Queue`:

```markdown
## Offer Live Observation

After resolving or initializing the shared queue, ask once: **실시간 큐 진행 상황을 브라우저에서 볼까요?**

- Ask before running `serve --open`; opening a browser is always opt-in.
- If accepted, run the server in a foreground tool session and retain the session handle.
- If declined, do not ask again during this coordination session. Offer `status` and `events` instead.
- When coordination ends or is abandoned, stop the server process and verify it exited.
- If browser opening fails, give the printed local URL for manual opening.

The dashboard is a read-only loopback view. Continue all queue mutations through `agent_queue.py`.
```

Change the Observe quick-reference row to:

```markdown
| Observe live (after consent) | `serve --open` |
| Observe in terminal | `status`, `events`, `export --format tsv` |
```

Add “stop the dashboard server and verify exit” to the final coordinator step.

- [ ] **Step 4: Update CLI output without contaminating machine formats**

Change init success output in `_run_command` to:

```python
        return {
            "ok": True,
            "queue_id": state["queue_id"],
            "revision": 0,
            "next_actions": ["serve --open", "status"],
        }
```

Keep `status --format json` and TSV byte-for-byte compatible. In `main`, append the hint only when all of these are true: command is `status`, format is `table`, and `sys.stdout.isatty()` is true.

```python
        if isinstance(result, str):
            sys.stdout.write(result)
            if (args.command == "status" and args.format == "table"
                    and sys.stdout.isatty()):
                sys.stdout.write("Live dashboard: agent_queue.py serve --open\n")
```

- [ ] **Step 5: Update README and schema with exact boundaries**

In README Quick start, after `$CLI` definition and queue initialization, add:

````markdown
When an agent offers live observation and you approve it, open the local dashboard:

```bash
$CLI serve --open
```

The foreground command prints a private `http://127.0.0.1:...` URL. If the browser cannot be opened automatically, visit that printed URL manually. Stop the command with `Ctrl-C` when coordination ends. The dashboard is read-only; use the normal CLI commands to change tasks.

For terminal-only observation, use `$CLI status` and `$CLI events`.
````

Add the `serve` row to the schema command table:

```markdown
| `serve` | Run the tokenized, read-only workflow dashboard on `127.0.0.1`; foreground until `Ctrl-C` or idle timeout. |
```

Document the exact options, consent responsibility, access-token URL, fixed route surface, lack of CORS, automatic expiry sweep during revision reads, browser fallback, and exit code 0 on `Ctrl-C`.

- [ ] **Step 6: Run documentation and CLI tests**

Run:

```bash
python3 -m unittest \
  test_agent_queue.SkillContractTests \
  test_agent_queue.QueueCliTests.test_cli_init_creates_both_files_and_second_is_code_two_unchanged \
  test_agent_queue.QueueCliTests.test_parser_help_and_invalid_arguments_use_argparse_code_two \
  test_agent_queue.QueueCliTests.test_init_and_human_status_discover_dashboard_without_changing_machine_output \
  test_agent_queue.QueueCliTests.test_help_names_live_local_dashboard -v
```

Expected: all selected tests pass. Update the existing init assertion to include `next_actions`; do not weaken it to a subset check.

- [ ] **Step 7: Commit discoverability and consent**

```bash
git add README.md skills/manage-agent-queue/SKILL.md \
  skills/manage-agent-queue/references/queue-schema.md \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/test_agent_queue.py
git commit -m "docs: 큐 대시보드 사용 흐름 안내"
```

## Task 7: Full Verification and Manual Smoke Test

**Files:**
- Modify only files required to fix a verified failure from this task.

- [ ] **Step 1: Run the full automated suite**

Run from the repository root:

```bash
python3 -m unittest discover \
  -s skills/manage-agent-queue/scripts \
  -p 'test_*.py' -v
```

Expected: all existing 196 tests plus the new dashboard tests pass with `OK`; no process or socket remains after the suite.

- [ ] **Step 2: Check formatting, incomplete markers, and external asset references**

Run:

```bash
git diff --check
rg -n 'TBD|TODO|FIXME|implement later|innerHTML|insertAdjacentHTML|https?://' \
  skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/dashboard \
  skills/manage-agent-queue/SKILL.md \
  skills/manage-agent-queue/references/queue-schema.md \
  README.md
```

Expected: `git diff --check` exits 0. The search has no findings except intentional documentation URLs already present in README badges; dashboard assets contain no external URL or unsafe HTML injection API.

- [ ] **Step 3: Run a real browser-free HTTP smoke test**

Create a temporary queue and start the server without `--open`, capturing its printed URL:

```bash
TMP_DIR="$(mktemp -d)"
QUEUE="$TMP_DIR/queue.json"
CLI="python3 skills/manage-agent-queue/scripts/agent_queue.py --queue $QUEUE"
$CLI init --id dashboard-smoke
$CLI workflow add --template adversarial-review \
  --title "Dashboard smoke" --resource repo --reviewers 2
$CLI serve --idle-timeout 10 > "$TMP_DIR/dashboard.log" &
SERVER_PID=$!
while ! test -s "$TMP_DIR/dashboard.log"; do sleep 0.1; done
DASHBOARD_URL="$(head -n 1 "$TMP_DIR/dashboard.log")"
curl --fail --silent --show-error "${DASHBOARD_URL}api/snapshot"
```

Expected: `DASHBOARD_URL` matches the tokenized loopback URL shape asserted by `DashboardCliTests`; `curl` returns HTTP 200 JSON containing `dashboard-smoke`, one workflow, and task rows. The server remains alive as `SERVER_PID`. Do not include `--open` in automated verification because browser launch requires user approval.

- [ ] **Step 4: Verify shutdown and cleanup**

Stop the captured server process and wait for cleanup:

```bash
kill -INT "$SERVER_PID"
wait "$SERVER_PID"
curl --fail --silent "${DASHBOARD_URL}api/health"
```

Expected: `wait` exits 0, `dashboard.log` contains `dashboard stopped`, and the final `curl` fails because the URL no longer accepts a connection. Inspect the PID with `ps -p "$SERVER_PID"`; it must not exist.

- [ ] **Step 5: Inspect the final diff against the approved design**

Run:

```bash
BASE_SHA="$(git merge-base main HEAD)"
git status --short
git diff --stat "$BASE_SHA"...HEAD
git diff "$BASE_SHA"...HEAD -- \
  skills/manage-agent-queue/scripts/agent_queue.py \
  skills/manage-agent-queue/scripts/queue_dashboard.py \
  skills/manage-agent-queue/scripts/dashboard \
  skills/manage-agent-queue/SKILL.md \
  skills/manage-agent-queue/references/queue-schema.md \
  README.md
```

Expected: the merge-base comparison includes every feature commit, and only the planned dashboard, CLI, tests, and documentation files changed. Check every acceptance criterion in `docs/specs/2026-07-14-local-queue-dashboard-design.md` against the diff and test evidence.

- [ ] **Step 6: Route any verified failure back to its owning task**

If Steps 1-5 reveal a failure, return to the task that owns the affected file, add or strengthen the exact regression test there, make it pass, repeat that task's commit step, and then rerun Task 7 from Step 1. If no correction is needed, do not create an empty commit.
