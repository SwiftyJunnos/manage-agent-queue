#!/usr/bin/env python3
"""Manage a shared local queue for cooperating agents."""

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 1
STORED_STATUSES = {
    "pending",
    "leased",
    "completed",
    "failed",
    "blocked",
    "cancelled",
}


class QueueError(Exception):
    exit_code = 2


class InvariantError(QueueError):
    exit_code = 6


def utc_now():
    """Return the current UTC time with second precision."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def fixed_config():
    """Return the queue's default operating configuration."""
    return {
        "default_lease_seconds": 900,
        "default_max_attempts": 3,
        "retry_backoff_seconds": 30,
        "lock_timeout_seconds": 5,
        "stale_lock_seconds": 30,
    }


def new_state(queue_id, config):
    """Create an empty, versioned queue state."""
    if not isinstance(queue_id, str) or not queue_id.strip():
        raise InvariantError("queue_id must be a non-blank string")

    now = utc_now()
    state = {
        "schema_version": SCHEMA_VERSION,
        "queue_id": queue_id,
        "revision": 0,
        "next_task_sequence": 1,
        "next_workflow_sequence": 1,
        "next_event_sequence": 1,
        "created_at": now,
        "updated_at": now,
        "config": dict(config),
        "tasks": {},
        "events": [],
    }
    return validate_state(state)


def validate_state(state):
    """Validate the stored queue invariants and return the state."""
    if not isinstance(state, dict):
        raise InvariantError("state must be an object")

    required_fields = (
        "schema_version",
        "queue_id",
        "revision",
        "next_task_sequence",
        "next_workflow_sequence",
        "next_event_sequence",
        "created_at",
        "updated_at",
        "config",
        "tasks",
        "events",
    )
    missing_fields = sorted(set(required_fields).difference(state))
    extra_fields = sorted(set(state).difference(required_fields))
    if missing_fields or extra_fields:
        details = []
        if missing_fields:
            details.append(f"missing: {', '.join(missing_fields)}")
        if extra_fields:
            details.append(f"unexpected: {', '.join(extra_fields)}")
        raise InvariantError(f"state fields mismatch ({'; '.join(details)})")

    if (
        not isinstance(state["schema_version"], int)
        or isinstance(state["schema_version"], bool)
        or state["schema_version"] != SCHEMA_VERSION
    ):
        raise InvariantError(
            f"schema_version must be {SCHEMA_VERSION}, got {state['schema_version']!r}"
        )
    if not isinstance(state["queue_id"], str) or not state["queue_id"].strip():
        raise InvariantError("queue_id must be a non-blank string")
    if (
        not isinstance(state["revision"], int)
        or isinstance(state["revision"], bool)
        or state["revision"] < 0
    ):
        raise InvariantError("revision must be a nonnegative integer")

    for field in (
        "next_task_sequence",
        "next_workflow_sequence",
        "next_event_sequence",
    ):
        value = state[field]
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise InvariantError(f"{field} must be a positive integer")

    for field in ("created_at", "updated_at"):
        value = state[field]
        if not isinstance(value, str) or re.fullmatch(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
            value,
            flags=re.ASCII,
        ) is None:
            raise InvariantError(f"{field} must match %Y-%m-%dT%H:%M:%SZ")
        try:
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as error:
            raise InvariantError(
                f"{field} must match %Y-%m-%dT%H:%M:%SZ"
            ) from error

    if not isinstance(state["tasks"], dict):
        raise InvariantError("tasks must be an object")
    if not isinstance(state["events"], list):
        raise InvariantError("events must be a list")

    config = state["config"]
    if not isinstance(config, dict):
        raise InvariantError("config must be an object")
    expected_config = fixed_config()
    missing_keys = sorted(set(expected_config).difference(config))
    extra_keys = sorted(set(config).difference(expected_config))
    if missing_keys or extra_keys:
        details = []
        if missing_keys:
            details.append(f"missing: {', '.join(missing_keys)}")
        if extra_keys:
            details.append(f"unexpected: {', '.join(extra_keys)}")
        raise InvariantError(f"config keys mismatch ({'; '.join(details)})")

    for key in expected_config:
        value = config[key]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
        ):
            raise InvariantError(f"config.{key} must be a positive integer")

    return state


def atomic_write_text(path, text):
    """Atomically replace a UTF-8 text file using a same-directory temporary."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            text=True,
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def write_json(path, state):
    """Validate and atomically write queue state as deterministic JSON."""
    validate_state(state)
    try:
        text = json.dumps(
            state,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except ValueError as error:
        for field in ("tasks", "events"):
            try:
                json.dumps(state[field], allow_nan=False)
            except ValueError:
                raise InvariantError(
                    f"{field} must contain finite JSON values"
                ) from error
        raise InvariantError("state must contain finite JSON values") from error
    atomic_write_text(path, text)


def render_empty_tsv(revision):
    """Render an empty queue projection for the given revision."""
    header = (
        "id\tworkflow\trole\tstate\tpriority\tassignee\tlease_until\tattempts\t"
        "depends_on\tblocked_by\tresources\ttitle"
    )
    return f"# queue_revision: {revision}\n{header}\n"


def initialize_queue(path, queue_id, config):
    """Create a new JSON queue and its empty TSV projection."""
    path = Path(path)
    if path.exists():
        raise QueueError(f"queue already exists: {path}")

    state = new_state(queue_id, config)
    write_json(path, state)
    atomic_write_text(path.with_suffix(".tsv"), render_empty_tsv(state["revision"]))
    return state
