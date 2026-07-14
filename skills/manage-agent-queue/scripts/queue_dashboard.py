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
