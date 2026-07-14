#!/usr/bin/env python3
"""Render and serve a read-only local dashboard for an agent queue."""

import copy
import json
import secrets
import time
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit


WARNING_STATES = ("failed", "blocked", "dependency_failed")
ACTIVE_STATES = {"leased"}
READY_STATES = {"ready"}
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
    "assets/dashboard.css": (
        "dashboard.css",
        "text/css; charset=utf-8",
    ),
    "assets/dashboard.js": (
        "dashboard.js",
        "text/javascript; charset=utf-8",
    ),
}


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
        remaining = int(
            (_parse_utc(row["lease_until"]) - now).total_seconds()
        )
        if 0 <= remaining <= 120:
            return {
                "kind": "lease_expiring",
                "task_id": row["id"],
                "title": row["title"],
                "remaining_seconds": remaining,
            }
    return None


def build_snapshot(queue_id, revision, rows, generated_at):
    """Build a detached, browser-safe workflow projection."""
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
        workflows.append(
            {
                "id": workflow_id,
                "completed": completed,
                "total": len(tasks),
                "active": sum(
                    task["state"] in ACTIVE_STATES for task in tasks
                ),
                "attention": sum(
                    task["state"] in WARNING_STATES for task in tasks
                ),
                "progress_percent": round(completed * 100 / len(tasks)),
                "tasks": tasks,
            }
        )

    warning_order = {
        name: index
        for index, name in enumerate((*WARNING_STATES, "lease_expiring"))
    }
    warnings.sort(
        key=lambda value: (
            warning_order[value["kind"]],
            value["task_id"],
        )
    )
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
    """Return detached sanitized events after a sequence number."""
    result = []
    for source in events:
        if source["seq"] <= sequence:
            continue
        event = copy.deepcopy(source)
        if isinstance(event.get("details"), dict):
            event["details"].pop("lease_token", None)
        result.append(event)
    return result


class DashboardServer(ThreadingHTTPServer):
    """Threaded local server with bounded request thread cleanup."""

    daemon_threads = True


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
            body = json.dumps(
                value,
                allow_nan=False,
                sort_keys=True,
            ).encode("utf-8")
            self._send(
                status,
                body,
                "application/json; charset=utf-8",
            )

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
            if not parsed.path.startswith(prefix):
                self._json(404, {"error": "not found"})
                return

            route = parsed.path[len(prefix) :]
            try:
                if route in ASSETS:
                    filename, content_type = ASSETS[route]
                    self._send(
                        200,
                        (app.asset_dir / filename).read_bytes(),
                        content_type,
                    )
                elif route == "api/revision":
                    self._json(
                        200,
                        {
                            "revision": app.revision_loader(),
                            "interval": app.interval,
                        },
                    )
                elif route == "api/snapshot":
                    self._json(200, app.snapshot_loader())
                elif route == "api/events":
                    values = parse_qs(parsed.query)
                    after = int(values.get("after", ["0"])[0])
                    self._json(
                        200,
                        {"events": app.events_loader(after)},
                    )
                elif route == "api/health":
                    self._json(200, {"ok": True})
                else:
                    self._json(404, {"error": "not found"})
            except (OSError, RuntimeError, ValueError):
                self._json(
                    503,
                    {"error": "queue temporarily unavailable"},
                )

    return DashboardHandler


def create_server(
    host,
    port,
    token,
    interval,
    revision_loader,
    snapshot_loader,
    events_loader,
    asset_dir,
):
    """Create a fixed-route loopback dashboard server."""
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


def serve(
    host,
    port,
    interval,
    idle_timeout,
    open_browser,
    revision_loader,
    snapshot_loader,
    events_loader,
    asset_dir,
    output,
):
    """Run the dashboard in the foreground until interrupted or idle."""
    token = secrets.token_urlsafe(24)
    server = create_server(
        host,
        port,
        token,
        interval,
        revision_loader,
        snapshot_loader,
        events_loader,
        asset_dir,
    )
    url = f"http://{host}:{server.server_port}/{token}/"
    output.write(url + "\n")
    output.flush()
    if open_browser and not webbrowser.open(url):
        output.write(f"browser did not open; visit {url}\n")
        output.flush()

    server.timeout = min(0.5, idle_timeout)
    try:
        while (
            time.monotonic() - server.dashboard.last_request
            < idle_timeout
        ):
            server.handle_request()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

    output.write("dashboard stopped\n")
    output.flush()
    return 0
