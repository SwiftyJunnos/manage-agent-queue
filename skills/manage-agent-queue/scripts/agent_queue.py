#!/usr/bin/env python3
"""Manage a shared local queue for cooperating agents."""

import argparse
import copy
import errno
import json
import os
import random
import re
import secrets
import shutil
import socket
import stat
import sys
import tempfile
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import git_queue as gq

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - exercised by isolated import test
    _fcntl = None

try:
    import msvcrt as _msvcrt
except ImportError:  # pragma: no cover - unavailable on POSIX
    _msvcrt = None


if _fcntl is not None:
    LOCK_BACKEND = "fcntl"
elif _msvcrt is not None:
    LOCK_BACKEND = "msvcrt"
else:
    LOCK_BACKEND = None


SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = frozenset({1, 2})
MAX_ID_SEQUENCE = 999_999
MAX_JSON_METADATA_DEPTH = 64
MAX_TEXT_BYTES = 16 * 1024
STORED_STATUSES = {
    "pending",
    "leased",
    "completed",
    "failed",
    "blocked",
    "cancelled",
}
DERIVED_STATUSES = {
    "ready",
    "waiting_dependency",
    "dependency_failed",
    "waiting_retry",
    "resource_conflict",
    "leased",
}
STATUS_FILTER_STATES = tuple(sorted(STORED_STATUSES | DERIVED_STATUSES))
GUARD_MARKER = b"LQG1"
TASK_ID_PATTERN = re.compile(r"T-(\d{6})", flags=re.ASCII)
WORKFLOW_ID_PATTERN = re.compile(r"W-(\d{6})", flags=re.ASCII)
TASK_FIELDS_V1 = frozenset({
    "id",
    "workflow_id",
    "role",
    "title",
    "description",
    "status",
    "priority",
    "depends_on",
    "resources",
    "labels",
    "attempts",
    "max_attempts",
    "available_at",
    "claim",
    "result",
    "last_error",
    "created_at",
    "updated_at",
})
TASK_FIELDS_V2 = TASK_FIELDS_V1 | {"git_mode", "git_recovery"}
TASK_CREATION_FIELDS_V1 = frozenset({
    "id",
    "workflow_id",
    "role",
    "title",
    "description",
    "priority",
    "depends_on",
    "resources",
    "labels",
    "max_attempts",
})
TASK_CREATION_FIELDS_V2 = TASK_CREATION_FIELDS_V1 | {"git_mode"}
CLAIM_FIELDS_V1 = frozenset({
    "agent_id",
    "lease_token",
    "claimed_at",
    "heartbeat_at",
    "expires_at",
})
CLAIM_FIELDS_V2 = CLAIM_FIELDS_V1 | {"git"}
GIT_BINDING_FIELDS = frozenset(
    {
        "common_dir",
        "worktree",
        "repository_id",
        "worktree_id",
        "branch",
        "base",
    }
)
EVENT_FIELDS = {
    "seq",
    "at",
    "type",
    "actor",
    "task_id",
    "revision",
    "details",
}
TSV_COLUMNS = (
    "id",
    "workflow",
    "role",
    "state",
    "priority",
    "assignee",
    "lease_until",
    "attempts",
    "depends_on",
    "blocked_by",
    "resources",
    "title",
)
# BMP bases from Unicode's emoji-variation-sequences data, plus the supported
# supplementary emoji span. Source chart:
# https://www.unicode.org/emoji/charts-16.0/emoji-variants.html
EMOJI_VARIATION_BASE_RANGES = (
    (0x0030, 0x0039),
    (0x2194, 0x2199),
    (0x21A9, 0x21AA),
    (0x231A, 0x231B),
    (0x23E9, 0x23F3),
    (0x23F8, 0x23FA),
    (0x25AA, 0x25AB),
    (0x25FB, 0x25FE),
    (0x2600, 0x2604),
    (0x2614, 0x2615),
    (0x2622, 0x2623),
    (0x262E, 0x262F),
    (0x2638, 0x263A),
    (0x2648, 0x2653),
    (0x265F, 0x2660),
    (0x2665, 0x2666),
    (0x267E, 0x267F),
    (0x2692, 0x2697),
    (0x269B, 0x269C),
    (0x26A0, 0x26A1),
    (0x26AA, 0x26AB),
    (0x26B0, 0x26B1),
    (0x26BD, 0x26BE),
    (0x26C4, 0x26C5),
    (0x26CE, 0x26CF),
    (0x26D3, 0x26D4),
    (0x26E9, 0x26EA),
    (0x26F0, 0x26F5),
    (0x26F7, 0x26FA),
    (0x2708, 0x270D),
    (0x2733, 0x2734),
    (0x2753, 0x2755),
    (0x2795, 0x2797),
    (0x2934, 0x2935),
    (0x2B05, 0x2B07),
    (0x2B1B, 0x2B1C),
    (0x1F000, 0x1FAFF),
)
EMOJI_VARIATION_BASE_CODEPOINTS = frozenset(
    {
        0x0023,
        0x002A,
        0x00A9,
        0x00AE,
        0x203C,
        0x2049,
        0x2122,
        0x2139,
        0x2328,
        0x23CF,
        0x24C2,
        0x25B6,
        0x25C0,
        0x260E,
        0x2611,
        0x2618,
        0x261D,
        0x2620,
        0x2626,
        0x262A,
        0x2640,
        0x2642,
        0x2663,
        0x2668,
        0x267B,
        0x2699,
        0x26A7,
        0x26C8,
        0x26D1,
        0x26FD,
        0x2702,
        0x2705,
        0x270F,
        0x2712,
        0x2714,
        0x2716,
        0x271D,
        0x2721,
        0x2728,
        0x2744,
        0x2747,
        0x274C,
        0x274E,
        0x2757,
        0x2763,
        0x2764,
        0x27A1,
        0x27B0,
        0x27BF,
        0x2B50,
        0x2B55,
        0x3030,
        0x303D,
        0x3297,
        0x3299,
    }
)
# Coalesced Unicode 16 Extended_Pictographic property ranges. Source:
# https://www.unicode.org/Public/16.0.0/ucd/emoji/emoji-data.txt
EXTENDED_PICTOGRAPHIC_RANGES = (
    (0x00A9, 0x00A9),
    (0x00AE, 0x00AE),
    (0x203C, 0x203C),
    (0x2049, 0x2049),
    (0x2122, 0x2122),
    (0x2139, 0x2139),
    (0x2194, 0x2199),
    (0x21A9, 0x21AA),
    (0x231A, 0x231B),
    (0x2328, 0x2328),
    (0x2388, 0x2388),
    (0x23CF, 0x23CF),
    (0x23E9, 0x23F3),
    (0x23F8, 0x23FA),
    (0x24C2, 0x24C2),
    (0x25AA, 0x25AB),
    (0x25B6, 0x25B6),
    (0x25C0, 0x25C0),
    (0x25FB, 0x25FE),
    (0x2600, 0x2605),
    (0x2607, 0x2612),
    (0x2614, 0x2685),
    (0x2690, 0x2705),
    (0x2708, 0x2712),
    (0x2714, 0x2714),
    (0x2716, 0x2716),
    (0x271D, 0x271D),
    (0x2721, 0x2721),
    (0x2728, 0x2728),
    (0x2733, 0x2734),
    (0x2744, 0x2744),
    (0x2747, 0x2747),
    (0x274C, 0x274C),
    (0x274E, 0x274E),
    (0x2753, 0x2755),
    (0x2757, 0x2757),
    (0x2763, 0x2767),
    (0x2795, 0x2797),
    (0x27A1, 0x27A1),
    (0x27B0, 0x27B0),
    (0x27BF, 0x27BF),
    (0x2934, 0x2935),
    (0x2B05, 0x2B07),
    (0x2B1B, 0x2B1C),
    (0x2B50, 0x2B50),
    (0x2B55, 0x2B55),
    (0x3030, 0x3030),
    (0x303D, 0x303D),
    (0x3297, 0x3297),
    (0x3299, 0x3299),
    (0x1F000, 0x1F0FF),
    (0x1F10D, 0x1F10F),
    (0x1F12F, 0x1F12F),
    (0x1F16C, 0x1F171),
    (0x1F17E, 0x1F17F),
    (0x1F18E, 0x1F18E),
    (0x1F191, 0x1F19A),
    (0x1F1AD, 0x1F1E5),
    (0x1F201, 0x1F20F),
    (0x1F21A, 0x1F21A),
    (0x1F22F, 0x1F22F),
    (0x1F232, 0x1F23A),
    (0x1F23C, 0x1F23F),
    (0x1F249, 0x1F3FA),
    (0x1F400, 0x1F53D),
    (0x1F546, 0x1F64F),
    (0x1F680, 0x1F6FF),
    (0x1F774, 0x1F77F),
    (0x1F7D5, 0x1F7FF),
    (0x1F80C, 0x1F80F),
    (0x1F848, 0x1F84F),
    (0x1F85A, 0x1F85F),
    (0x1F888, 0x1F88F),
    (0x1F8AE, 0x1F8FF),
    (0x1F90C, 0x1F93A),
    (0x1F93C, 0x1F945),
    (0x1F947, 0x1FAFF),
    (0x1FC00, 0x1FFFD),
)


class QueueError(Exception):
    exit_code = 2


class InvariantError(QueueError):
    exit_code = 6


class NoTaskAvailable(QueueError):
    exit_code = 3


class LeaseError(QueueError):
    exit_code = 5


class LockTimeout(QueueError):
    exit_code = 4


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


def allocate_id(state, kind):
    """Allocate the next monotonic task or workflow identifier."""
    fields = {
        "task": ("next_task_sequence", "T"),
        "workflow": ("next_workflow_sequence", "W"),
    }
    if kind not in fields:
        raise InvariantError("id kind must be task or workflow")
    sequence_field, prefix = fields[kind]
    sequence = state[sequence_field]
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence <= 0:
        raise InvariantError(f"{sequence_field} must be a positive integer")
    if sequence > MAX_ID_SEQUENCE:
        raise InvariantError(f"{kind} id sequence exhausted at {prefix}-999999")
    state[sequence_field] += 1
    return f"{prefix}-{sequence:06d}"


def reserve_task_id(state, explicit_id=None):
    """Reserve an explicit task ID or allocate the next task ID."""
    if explicit_id is None:
        return allocate_id(state, "task")
    if not isinstance(explicit_id, str):
        raise InvariantError("task id must be between T-000001 and T-999999")
    match = TASK_ID_PATTERN.fullmatch(explicit_id)
    if match is None or int(match.group(1)) == 0:
        raise InvariantError("task id must be between T-000001 and T-999999")
    sequence = int(match.group(1))
    state["next_task_sequence"] = max(
        state["next_task_sequence"], sequence + 1
    )
    return explicit_id


def _task_id_sequence(task_id):
    if not isinstance(task_id, str):
        raise InvariantError("task id must be between T-000001 and T-999999")
    match = TASK_ID_PATTERN.fullmatch(task_id)
    if match is None or int(match.group(1)) == 0:
        raise InvariantError("task id must be between T-000001 and T-999999")
    return int(match.group(1))


def _workflow_id_sequence(workflow_id):
    if not isinstance(workflow_id, str):
        raise InvariantError("task workflow_id must be a string or null")
    match = WORKFLOW_ID_PATTERN.fullmatch(workflow_id)
    if match is None or int(match.group(1)) == 0:
        raise InvariantError(
            "task workflow_id must be between W-000001 and W-999999"
        )
    return int(match.group(1))


def _reserve_batch_ids(state, raw_tasks):
    """Reserve all new explicit IDs against the pre-batch queue history."""
    starting_task_sequence = state["next_task_sequence"]
    starting_workflow_sequence = state["next_workflow_sequence"]
    explicit_task_ids = set()
    existing_workflow_ids = {
        task["workflow_id"]
        for task in state["tasks"].values()
        if task["workflow_id"] is not None
    }
    new_workflow_ids = set()

    for raw in raw_tasks:
        if not isinstance(raw, dict):
            continue
        explicit_task_id = raw.get("id")
        if explicit_task_id is not None:
            sequence = _task_id_sequence(explicit_task_id)
            if (
                explicit_task_id in state["tasks"]
                or explicit_task_id in explicit_task_ids
            ):
                raise InvariantError(f"duplicate task id: {explicit_task_id}")
            if sequence < starting_task_sequence:
                raise InvariantError(
                    f"historical task id cannot be reused: {explicit_task_id}"
                )
            explicit_task_ids.add(explicit_task_id)

        workflow_id = raw.get("workflow_id")
        if workflow_id is None:
            continue
        sequence = _workflow_id_sequence(workflow_id)
        if workflow_id in existing_workflow_ids:
            continue
        if sequence < starting_workflow_sequence:
            raise InvariantError(
                f"historical workflow id cannot be reused: {workflow_id}"
            )
        new_workflow_ids.add(workflow_id)

    if new_workflow_ids:
        state["next_workflow_sequence"] = max(
            starting_workflow_sequence,
            max(
                _workflow_id_sequence(workflow_id)
                for workflow_id in new_workflow_ids
            )
            + 1,
        )
    return explicit_task_ids


def _assign_batch_task_ids(state, raw_tasks, explicit_task_ids):
    """Assign generated IDs from the starting cursor around explicit reservations."""
    starting_sequence = state["next_task_sequence"]
    reserved_sequences = {
        _task_id_sequence(task_id) for task_id in explicit_task_ids
    }
    generated_sequence = starting_sequence
    prepared_tasks = []
    for raw in raw_tasks:
        if not isinstance(raw, dict) or raw.get("id") is not None:
            prepared_tasks.append(raw)
            continue
        while generated_sequence in reserved_sequences:
            generated_sequence += 1
        if generated_sequence > MAX_ID_SEQUENCE:
            raise InvariantError("task id sequence exhausted at T-999999")
        prepared = dict(raw)
        prepared["id"] = f"T-{generated_sequence:06d}"
        prepared_tasks.append(prepared)
        reserved_sequences.add(generated_sequence)
        generated_sequence += 1

    if reserved_sequences:
        state["next_task_sequence"] = max(reserved_sequences) + 1
    return prepared_tasks


def _validate_timestamp(value, field):
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


def _is_valid_timestamp(value):
    try:
        _validate_timestamp(value, "timestamp")
    except InvariantError:
        return False
    return True


def add_seconds(timestamp, seconds):
    """Add a positive number of seconds to a canonical UTC timestamp."""
    _validate_timestamp(timestamp, "timestamp")
    if (
        not isinstance(seconds, int)
        or isinstance(seconds, bool)
        or seconds <= 0
    ):
        raise InvariantError("seconds must be a positive integer")
    value = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    try:
        result = value + timedelta(seconds=seconds)
    except OverflowError as error:
        raise InvariantError("timestamp addition exceeds supported range") from error
    return result.strftime("%Y-%m-%dT%H:%M:%SZ")


def _deduplicated_string_list(raw, field):
    if not isinstance(raw, list):
        raise InvariantError(f"{field} must be a list")
    if any(not isinstance(value, str) for value in raw):
        raise InvariantError(f"{field} values must be strings")
    return list(dict.fromkeys(raw))


def _validate_bounded_text(value, field, *, nonempty=False):
    """Validate text stored directly in queue state using a UTF-8 byte cap."""
    if not isinstance(value, str) or (nonempty and not value):
        qualifier = "nonempty " if nonempty else ""
        raise InvariantError(f"{field} must be a {qualifier}string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise InvariantError(
            f"{field} must contain valid UTF-8 text"
        ) from error
    if len(encoded) > MAX_TEXT_BYTES:
        raise InvariantError(
            f"{field} must be at most {MAX_TEXT_BYTES} UTF-8 bytes"
        )
    return value


def normalize_task(state, raw):
    """Create a canonical pending task from generic task input."""
    if not isinstance(raw, dict):
        raise InvariantError("task must be an object")
    if any(not isinstance(field, str) for field in raw):
        raise InvariantError("task creation input must use string field names")
    schema_version = state["schema_version"]
    creation_fields = (
        TASK_CREATION_FIELDS_V1
        if schema_version == 1
        else TASK_CREATION_FIELDS_V2
    )
    unknown_fields = sorted(set(raw).difference(creation_fields))
    if unknown_fields:
        raise InvariantError(
            f"unknown task creation fields: {', '.join(unknown_fields)}"
        )

    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        raise InvariantError("task title must be a non-blank string")
    description = raw.get("description", "")
    _validate_bounded_text(description, "task description")

    priority = raw.get("priority", 0)
    if not isinstance(priority, int) or isinstance(priority, bool):
        raise InvariantError("task priority must be an integer")
    max_attempts = raw.get(
        "max_attempts", state["config"]["default_max_attempts"]
    )
    if (
        not isinstance(max_attempts, int)
        or isinstance(max_attempts, bool)
        or max_attempts <= 0
    ):
        raise InvariantError("task max_attempts must be a positive integer")

    workflow_id = raw.get("workflow_id")
    if workflow_id is not None and not isinstance(workflow_id, str):
        raise InvariantError("task workflow_id must be a string or null")
    if workflow_id is not None:
        match = WORKFLOW_ID_PATTERN.fullmatch(workflow_id)
        if match is None or int(match.group(1)) == 0:
            raise InvariantError(
                "task workflow_id must be between W-000001 and W-999999"
            )
        state["next_workflow_sequence"] = max(
            state["next_workflow_sequence"], int(match.group(1)) + 1
        )
    role = raw.get("role")
    if role is not None and not isinstance(role, str):
        raise InvariantError("task role must be a string or null")

    depends_on = _deduplicated_string_list(
        raw.get("depends_on", []), "task depends_on"
    )
    resources = _deduplicated_string_list(
        raw.get("resources", []), "task resources"
    )
    git_mode = raw.get("git_mode") if schema_version == 2 else None
    if git_mode not in (None, "commit"):
        raise InvariantError("task git_mode must be commit or null")
    if git_mode == "commit":
        try:
            scopes = gq.path_scopes(resources)
        except gq.GitContextError as error:
            raise InvariantError(str(error)) from error
        if not scopes:
            raise InvariantError(
                "Git-aware task requires a file: or dir: resource"
            )
    labels = _deduplicated_string_list(raw.get("labels", []), "task labels")
    task_id = reserve_task_id(state, raw.get("id"))
    now = utc_now()
    task = {
        "id": task_id,
        "workflow_id": workflow_id,
        "role": role,
        "title": title.strip(),
        "description": description,
        "status": "pending",
        "priority": priority,
        "depends_on": depends_on,
        "resources": resources,
        "labels": labels,
        "attempts": 0,
        "max_attempts": max_attempts,
        "available_at": None,
        "claim": None,
        "result": None,
        "last_error": None,
        "created_at": now,
        "updated_at": now,
    }
    if schema_version == 2:
        task["git_mode"] = git_mode
        task["git_recovery"] = None
    validate_task(
        task,
        expected_id=task_id,
        schema_version=schema_version,
    )
    return task


def _json_validation_error(value):
    """Return why metadata is unsafe JSON, using bounded iterative traversal."""
    active_containers = set()
    stack = [("visit", value, 0)]
    while stack:
        action, item, depth = stack.pop()
        if action == "leave":
            active_containers.remove(item)
            continue
        if item is None or isinstance(item, (bool, str, int)):
            continue
        if isinstance(item, float):
            try:
                json.dumps(item, allow_nan=False)
            except ValueError:
                return "must contain JSON values"
            continue
        if not isinstance(item, (list, dict)):
            return "must contain JSON values"
        if depth > MAX_JSON_METADATA_DEPTH:
            return f"exceeds maximum depth {MAX_JSON_METADATA_DEPTH}"

        container_id = id(item)
        if container_id in active_containers:
            return "contains a circular reference"
        active_containers.add(container_id)
        stack.append(("leave", container_id, depth))
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                return "must contain JSON values"
            children = item.values()
        else:
            children = item
        stack.extend(
            ("visit", child, depth + 1) for child in reversed(list(children))
        )
    return None


def validate_task(task, expected_id=None, schema_version=SCHEMA_VERSION):
    """Validate one stored task's exact canonical schema."""
    if not isinstance(task, dict):
        raise InvariantError("task must be an object")
    if any(not isinstance(field, str) for field in task):
        raise InvariantError("task field names must be strings")
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise InvariantError(f"unsupported task schema version: {schema_version}")
    task_fields = TASK_FIELDS_V1 if schema_version == 1 else TASK_FIELDS_V2
    claim_fields = CLAIM_FIELDS_V1 if schema_version == 1 else CLAIM_FIELDS_V2
    missing_fields = sorted(task_fields.difference(task))
    extra_fields = sorted(set(task).difference(task_fields))
    if missing_fields or extra_fields:
        details = []
        if missing_fields:
            details.append(f"missing: {', '.join(missing_fields)}")
        if extra_fields:
            details.append(f"unexpected: {', '.join(extra_fields)}")
        raise InvariantError(f"task fields mismatch ({'; '.join(details)})")

    task_id = task["id"]
    task_id_match = (
        TASK_ID_PATTERN.fullmatch(task_id) if isinstance(task_id, str) else None
    )
    if task_id_match is None or int(task_id_match.group(1)) == 0:
        raise InvariantError("task id must be between T-000001 and T-999999")
    if expected_id is not None and task_id != expected_id:
        raise InvariantError(
            f"tasks.{expected_id}.id must equal dictionary key, got {task_id}"
        )

    for field in ("workflow_id", "role"):
        value = task[field]
        if value is not None and not isinstance(value, str):
            raise InvariantError(f"task {field} must be a string or null")
    workflow_id = task["workflow_id"]
    if workflow_id is not None:
        workflow_id_match = WORKFLOW_ID_PATTERN.fullmatch(workflow_id)
        if workflow_id_match is None or int(workflow_id_match.group(1)) == 0:
            raise InvariantError(
                "task workflow_id must be between W-000001 and W-999999"
            )
    if not isinstance(task["title"], str) or not task["title"].strip():
        raise InvariantError("task title must be a non-blank string")
    _validate_bounded_text(task["description"], "task description")
    if (
        not isinstance(task["status"], str)
        or task["status"] not in STORED_STATUSES
    ):
        raise InvariantError("task status must be a stored status")
    if not isinstance(task["priority"], int) or isinstance(task["priority"], bool):
        raise InvariantError("task priority must be an integer")

    if schema_version == 2:
        if task["git_mode"] not in (None, "commit"):
            raise InvariantError("task git_mode must be commit or null")
        recovery = task["git_recovery"]
        if recovery is not None:
            if task["git_mode"] != "commit":
                raise InvariantError(
                    "task git_recovery requires git_mode commit"
                )
            if task["status"] not in {"pending", "failed"}:
                raise InvariantError(
                    "task git_recovery requires pending or failed status"
                )
            if not isinstance(recovery, dict):
                raise InvariantError("task git_recovery must be an object or null")

    for field in ("depends_on", "resources", "labels"):
        values = task[field]
        canonical = _deduplicated_string_list(values, f"task {field}")
        if values != canonical:
            raise InvariantError(f"task {field} must not contain duplicates")

    attempts = task["attempts"]
    if (
        not isinstance(attempts, int)
        or isinstance(attempts, bool)
        or attempts < 0
    ):
        raise InvariantError("task attempts must be a nonnegative integer")
    max_attempts = task["max_attempts"]
    if (
        not isinstance(max_attempts, int)
        or isinstance(max_attempts, bool)
        or max_attempts <= 0
    ):
        raise InvariantError("task max_attempts must be a positive integer")
    if attempts > max_attempts:
        raise InvariantError("task attempts must not exceed max_attempts")

    available_at = task["available_at"]
    if available_at is not None:
        _validate_timestamp(available_at, "task available_at")
    for field in ("claim", "result", "last_error"):
        value = task[field]
        if value is not None and not isinstance(value, dict):
            raise InvariantError(f"task {field} must be an object or null")
        if value is not None:
            json_error = _json_validation_error(value)
            if json_error is not None:
                raise InvariantError(f"task {field} {json_error}")
    claim = task["claim"]
    if task["status"] == "leased":
        if claim is None:
            raise InvariantError("leased task claim must be an object")
        missing_claim_fields = sorted(claim_fields.difference(claim))
        extra_claim_fields = sorted(set(claim).difference(claim_fields))
        if missing_claim_fields or extra_claim_fields:
            raise InvariantError("task claim keys must match the lease schema")
        for field in ("agent_id", "lease_token"):
            if not isinstance(claim[field], str) or not claim[field].strip():
                raise InvariantError(
                    f"task claim.{field} must be a non-blank string"
                )
        for field in ("claimed_at", "heartbeat_at", "expires_at"):
            _validate_timestamp(claim[field], f"task claim.{field}")
        if claim["claimed_at"] > claim["heartbeat_at"]:
            raise InvariantError(
                "task claim.claimed_at must not follow heartbeat_at"
            )
        if claim["heartbeat_at"] >= claim["expires_at"]:
            raise InvariantError(
                "task claim.heartbeat_at must precede expires_at"
            )
        if attempts < 1:
            raise InvariantError("leased task attempts must be at least 1")
        if schema_version == 2:
            if task["git_mode"] is None and claim["git"] is not None:
                raise InvariantError("generic task claim.git must be null")
            if task["git_mode"] == "commit" and claim["git"] is None:
                raise InvariantError("Git-aware task claim.git must be an object")
            if task["git_mode"] == "commit":
                git_binding = claim["git"]
                if not isinstance(git_binding, dict) or set(
                    git_binding
                ) != GIT_BINDING_FIELDS:
                    raise InvariantError(
                        "Git-aware task claim.git fields must match binding schema"
                    )
                for field in GIT_BINDING_FIELDS:
                    if not isinstance(
                        git_binding[field], str
                    ) or not git_binding[field]:
                        raise InvariantError(
                            f"task claim.git.{field} must be nonempty"
                        )
    elif claim is not None:
        raise InvariantError("non-leased task claim must be null")

    result = task["result"]
    if task["status"] == "completed":
        if result is None:
            raise InvariantError("completed task result must be an object")
    elif result is not None:
        raise InvariantError("task result must be null unless status is completed")
    if result is not None:
        result_fields = (
            {"summary", "artifacts"}
            if schema_version == 1
            else {"summary", "artifacts", "git"}
        )
        if set(result) != result_fields:
            raise InvariantError(
                "task result keys must match the schema version"
            )
        if not isinstance(result["summary"], str) or not result["summary"]:
            raise InvariantError("task result.summary must be a nonempty string")
        _validate_bounded_text(
            result["summary"], "task result.summary", nonempty=True
        )
        artifacts = result["artifacts"]
        if not isinstance(artifacts, list) or any(
            not isinstance(artifact, str) or not artifact
            for artifact in artifacts
        ):
            raise InvariantError(
                "task result.artifacts must be a list of nonempty strings"
            )
        if schema_version == 2:
            git_result = result["git"]
            if task["git_mode"] is None and git_result is not None:
                raise InvariantError("generic task result.git must be null")
            if task["git_mode"] == "commit" and not isinstance(
                git_result, dict
            ):
                raise InvariantError(
                    "Git-aware task result.git must be an object"
                )

    last_error = task["last_error"]
    if last_error is not None:
        allowed_error_fields = {"message", "at", "kind"}
        if (
            set(last_error) not in ({"message", "at"}, allowed_error_fields)
        ):
            raise InvariantError(
                "task last_error keys must be message, at, and optional kind"
            )
        if (
            not isinstance(last_error["message"], str)
            or not last_error["message"]
        ):
            raise InvariantError(
                "task last_error.message must be a nonempty string"
            )
        _validate_bounded_text(
            last_error["message"], "task last_error.message", nonempty=True
        )
        _validate_timestamp(last_error["at"], "task last_error.at")
        if "kind" in last_error and last_error["kind"] != "blocked":
            raise InvariantError("task last_error.kind must equal blocked")
    if task["status"] == "blocked" and (
        last_error is None or last_error.get("kind") != "blocked"
    ):
        raise InvariantError(
            "blocked task last_error.kind must equal blocked"
        )
    if task["status"] == "failed":
        if last_error is None:
            raise InvariantError("failed task last_error must be an object")
        if "kind" in last_error:
            raise InvariantError(
                "failed task last_error must not include kind"
            )

    if task["status"] != "pending" and available_at is not None:
        raise InvariantError(
            "task available_at must be null unless status is pending"
        )
    _validate_timestamp(task["created_at"], "task created_at")
    _validate_timestamp(task["updated_at"], "task updated_at")
    if task["updated_at"] < task["created_at"]:
        raise InvariantError("task updated_at must not precede created_at")
    return task


def validate_graph(tasks):
    """Validate that task dependencies exist and form a directed acyclic graph."""
    dependents = {task_id: [] for task_id in tasks}
    indegrees = {}
    for task_id, task in tasks.items():
        indegrees[task_id] = len(task["depends_on"])
        for dependency in task["depends_on"]:
            if dependency == task_id:
                raise InvariantError(
                    f"tasks.{task_id}.depends_on cannot include itself"
                )
            if dependency not in tasks:
                raise InvariantError(
                    f"missing dependency {dependency} in tasks.{task_id}.depends_on"
                )
            dependents[dependency].append(task_id)

    ready = [task_id for task_id, degree in indegrees.items() if degree == 0]
    visited_count = 0
    ready_index = 0
    while ready_index < len(ready):
        task_id = ready[ready_index]
        ready_index += 1
        visited_count += 1
        for dependent in dependents[task_id]:
            indegrees[dependent] -= 1
            if indegrees[dependent] == 0:
                ready.append(dependent)

    if visited_count != len(tasks):
        cyclic_task = next(
            task_id for task_id, degree in indegrees.items() if degree > 0
        )
        raise InvariantError(
            f"dependency cycle includes tasks.{cyclic_task}.depends_on"
        )


def _add_task_batch_to_candidate(candidate, raw_tasks):
    """Append a prepared batch to an already isolated candidate state."""
    explicit_task_ids = _reserve_batch_ids(candidate, raw_tasks)
    prepared_tasks = _assign_batch_task_ids(
        candidate, raw_tasks, explicit_task_ids
    )
    created_ids = []
    for raw in prepared_tasks:
        task = normalize_task(candidate, raw)
        task_id = task["id"]
        if task_id in candidate["tasks"]:
            raise InvariantError(f"duplicate task id: {task_id}")
        candidate["tasks"][task_id] = task
        created_ids.append(task_id)
    return created_ids


def add_task_batch(state, raw_tasks):
    """Validate and atomically append a nonempty batch of generic tasks."""
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise InvariantError("raw_tasks must be a nonempty list")
    candidate = copy.deepcopy(state)
    validate_state(candidate)
    created_ids = _add_task_batch_to_candidate(candidate, raw_tasks)
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return [copy.deepcopy(state["tasks"][task_id]) for task_id in created_ids]


def add_task(state, raw):
    """Validate and atomically append one generic task."""
    return add_task_batch(state, [raw])[0]


def _workflow_title_priority(title, priority):
    if not isinstance(title, str) or not title.strip():
        raise InvariantError("workflow title must be a non-blank string")
    if not isinstance(priority, int) or isinstance(priority, bool):
        raise InvariantError("workflow priority must be an integer")
    return title.strip()


def _workflow_ids(state, task_count):
    workflow_sequence = state["next_workflow_sequence"]
    task_sequence = state["next_task_sequence"]
    for sequence, field in (
        (workflow_sequence, "next_workflow_sequence"),
        (task_sequence, "next_task_sequence"),
    ):
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence <= 0
        ):
            raise InvariantError(f"{field} must be a positive integer")
    if workflow_sequence > MAX_ID_SEQUENCE:
        raise InvariantError("workflow id sequence exhausted at W-999999")
    if task_sequence + task_count - 1 > MAX_ID_SEQUENCE:
        raise InvariantError("task id sequence exhausted at T-999999")
    workflow_id = f"W-{workflow_sequence:06d}"
    task_ids = [
        f"T-{sequence:06d}"
        for sequence in range(task_sequence, task_sequence + task_count)
    ]
    return workflow_id, task_ids


def _commit_workflow(state, raw_tasks, template, details, now):
    candidate = copy.deepcopy(state)
    created_ids = _add_task_batch_to_candidate(candidate, raw_tasks)
    workflow_id = raw_tasks[0]["workflow_id"]
    _append_event_to_candidate(
        candidate,
        "workflow.created",
        "operator",
        None,
        {
            "template": template,
            "workflow_id": workflow_id,
            "task_ids": created_ids,
            "task_count": len(created_ids),
            **details,
        },
        utc_now() if now is None else now,
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return {
        "workflow_id": workflow_id,
        "template": template,
        "task_ids": created_ids,
    }


def add_adversarial_review(
    state,
    title,
    priority,
    resources,
    reviewer_count,
    git_commit=False,
    now=None,
):
    """Atomically add an implement-review-apply-verify workflow."""
    validate_state(state)
    title = _workflow_title_priority(title, priority)
    if not isinstance(resources, list):
        raise InvariantError("workflow resources must be a list")
    if any(not isinstance(resource, str) for resource in resources):
        raise InvariantError("workflow resources must contain strings")
    if any(not resource.strip() for resource in resources):
        raise InvariantError("workflow resources must contain non-blank strings")
    if len(set(resources)) != len(resources):
        raise InvariantError("workflow resources must be unique")
    if (
        not isinstance(reviewer_count, int)
        or isinstance(reviewer_count, bool)
        or reviewer_count <= 0
    ):
        raise InvariantError("reviewer_count must be a positive integer")
    if not isinstance(git_commit, bool):
        raise InvariantError("git_commit must be a boolean")

    task_count = reviewer_count + 3
    workflow_id, task_ids = _workflow_ids(state, task_count)
    implement_id = task_ids[0]
    review_ids = task_ids[1:1 + reviewer_count]
    apply_id = task_ids[-2]
    target_resources = ", ".join(resources)
    raw_tasks = [{
        "id": implement_id,
        "workflow_id": workflow_id,
        "role": "implement",
        "title": title,
        "description": f"Implement {title} for resources: {target_resources}.",
        "priority": priority,
        "depends_on": [],
        "resources": list(resources),
        **({"git_mode": "commit"} if git_commit else {}),
    }]
    raw_tasks.extend({
        "id": review_id,
        "workflow_id": workflow_id,
        "role": "review",
        "title": f"Review {index}: {title}",
        "description": (
            "For target resources: "
            f"{target_resources}. Independently review the implementation "
            "artifact without implementer reasoning or other reviewer findings."
        ),
        "priority": priority - 10,
        "depends_on": [implement_id],
        "resources": [],
    } for index, review_id in enumerate(review_ids, start=1))
    raw_tasks.extend((
        {
            "id": apply_id,
            "workflow_id": workflow_id,
            "role": "apply",
            "title": f"Apply reviews: {title}",
            "description": f"Apply all review findings for {title}.",
            "priority": priority - 20,
            "depends_on": review_ids,
            "resources": list(resources),
            **({"git_mode": "commit"} if git_commit else {}),
        },
        {
            "id": task_ids[-1],
            "workflow_id": workflow_id,
            "role": "verify",
            "title": f"Verify: {title}",
            "description": f"Verify the reviewed implementation for {title}.",
            "priority": priority - 30,
            "depends_on": [apply_id],
            "resources": [],
        },
    ))
    return _commit_workflow(
        state,
        raw_tasks,
        "adversarial-review",
        {
            "reviewer_count": reviewer_count,
            **({"git_commit": True} if git_commit else {}),
        },
        now,
    )


def add_parallel_shards(
    state,
    title,
    priority,
    shard_resources,
    git_commit=False,
    now=None,
):
    """Atomically add parallel resource shards followed by integration."""
    validate_state(state)
    title = _workflow_title_priority(title, priority)
    if not isinstance(shard_resources, list) or not shard_resources:
        raise InvariantError("shard_resources must be a nonempty list")
    if not isinstance(git_commit, bool):
        raise InvariantError("git_commit must be a boolean")
    normalized_shards = []
    seen_resources = set()
    for shard in shard_resources:
        if not isinstance(shard, list) or not shard:
            raise InvariantError("each shard must be a nonempty resource list")
        if any(not isinstance(resource, str) for resource in shard):
            raise InvariantError("shard resources must be strings")
        if any(not resource.strip() for resource in shard):
            raise InvariantError("shard resources must be non-blank strings")
        normalized = list(dict.fromkeys(shard))
        duplicate = next(
            (resource for resource in normalized if resource in seen_resources),
            None,
        )
        if duplicate is not None:
            raise InvariantError(
                f"resource appears in more than one shard: {duplicate}"
            )
        if git_commit:
            overlapping = next(
                (
                    prior
                    for prior in normalized_shards
                    if gq.resources_overlap(prior, normalized)
                ),
                None,
            )
            if overlapping is not None:
                raise InvariantError(
                    "Git-aware shard path resources overlap"
                )
        seen_resources.update(normalized)
        normalized_shards.append(normalized)

    shard_count = len(normalized_shards)
    workflow_id, task_ids = _workflow_ids(state, shard_count + 2)
    shard_ids = task_ids[:shard_count]
    integrate_id = task_ids[-2]
    flattened_resources = [
        resource for shard in normalized_shards for resource in shard
    ]
    raw_tasks = [{
        "id": task_id,
        "workflow_id": workflow_id,
        "role": "shard",
        "title": f"Shard {index}: {title}",
        "description": f"Complete shard {index} for {title}.",
        "priority": priority,
        "depends_on": [],
        "resources": shard,
        **({"git_mode": "commit"} if git_commit else {}),
    } for index, (task_id, shard) in enumerate(
        zip(shard_ids, normalized_shards), start=1
    )]
    raw_tasks.extend((
        {
            "id": integrate_id,
            "workflow_id": workflow_id,
            "role": "integrate",
            "title": f"Integrate: {title}",
            "description": f"Integrate all shards for {title}.",
            "priority": priority - 10,
            "depends_on": shard_ids,
            "resources": flattened_resources,
            **({"git_mode": "commit"} if git_commit else {}),
        },
        {
            "id": task_ids[-1],
            "workflow_id": workflow_id,
            "role": "verify",
            "title": f"Verify: {title}",
            "description": f"Verify the integrated result for {title}.",
            "priority": priority - 20,
            "depends_on": [integrate_id],
            "resources": [],
        },
    ))
    return _commit_workflow(
        state,
        raw_tasks,
        "parallel-shards",
        {
            "shard_count": shard_count,
            **({"git_commit": True} if git_commit else {}),
        },
        now,
    )


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


def migrate_state(state, target_version, now=None):
    """Atomically transform a valid version-1 candidate into version 2."""
    validate_state(state)
    if state["schema_version"] != 1 or target_version != 2:
        raise InvariantError("only schema version 1 can migrate to 2")
    now = _canonical_now(now)
    candidate = copy.deepcopy(state)
    for task in candidate["tasks"].values():
        task["git_mode"] = None
        task["git_recovery"] = None
        if task["status"] == "leased":
            task["claim"]["git"] = None
        if task["status"] == "completed":
            task["result"]["git"] = None
    candidate["schema_version"] = 2
    _append_event_to_candidate(
        candidate,
        "queue.migrated",
        "operator",
        None,
        {"from": 1, "to": 2},
        now,
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return {"from": 1, "to": 2}


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
        or state["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS
    ):
        raise InvariantError(
            "schema_version must be one of "
            f"{sorted(SUPPORTED_SCHEMA_VERSIONS)}, "
            f"got {state['schema_version']!r}"
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
        if (
            field in ("next_task_sequence", "next_workflow_sequence")
            and value > MAX_ID_SEQUENCE + 1
        ):
            raise InvariantError(f"{field} exceeds its six-digit id range")

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

    maximum_task_sequence = 0
    maximum_workflow_sequence = 0
    maximum_task_id = None
    maximum_workflow_id = None
    for task_id, task in state["tasks"].items():
        validate_task(
            task,
            expected_id=task_id,
            schema_version=state["schema_version"],
        )
        task_sequence = int(TASK_ID_PATTERN.fullmatch(task_id).group(1))
        if task_sequence > maximum_task_sequence:
            maximum_task_sequence = task_sequence
            maximum_task_id = task_id
        workflow_id = task["workflow_id"]
        if workflow_id is not None:
            workflow_sequence = int(
                WORKFLOW_ID_PATTERN.fullmatch(workflow_id).group(1)
            )
            if workflow_sequence > maximum_workflow_sequence:
                maximum_workflow_sequence = workflow_sequence
                maximum_workflow_id = workflow_id
    if state["next_task_sequence"] <= maximum_task_sequence:
        raise InvariantError(
            f"next_task_sequence must be greater than stored task {maximum_task_id}"
        )
    if state["next_workflow_sequence"] <= maximum_workflow_sequence:
        raise InvariantError(
            "next_workflow_sequence must be greater than stored workflow "
            f"{maximum_workflow_id}"
        )
    validate_graph(state["tasks"])

    previous_event_sequence = 0
    previous_event_revision = 0
    previous_event_at = None
    for index, event in enumerate(state["events"]):
        if not isinstance(event, dict):
            raise InvariantError(f"events[{index}] must be an object")
        if set(event) != EVENT_FIELDS:
            raise InvariantError(f"events[{index}] fields must match event schema")
        sequence = event["seq"]
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence <= previous_event_sequence
            or sequence >= state["next_event_sequence"]
        ):
            raise InvariantError(
                f"events[{index}].seq must increase below next_event_sequence"
            )
        previous_event_sequence = sequence
        _validate_timestamp(event["at"], f"events[{index}].at")
        if previous_event_at is not None and event["at"] < previous_event_at:
            raise InvariantError(
                f"events[{index}].at must be nondecreasing by sequence"
            )
        previous_event_at = event["at"]
        for field in ("type", "actor"):
            if (
                not isinstance(event[field], str)
                or not event[field].strip()
            ):
                raise InvariantError(
                    f"events[{index}].{field} must be a non-blank string"
                )
        task_id = event["task_id"]
        if task_id is not None:
            if not isinstance(task_id, str) or not task_id.strip():
                raise InvariantError(
                    f"events[{index}].task_id must be a non-blank string or null"
                )
            if task_id not in state["tasks"]:
                raise InvariantError(
                    f"events[{index}].task_id must reference a stored task"
                )
        revision = event["revision"]
        if (
            not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision <= 0
            or revision < previous_event_revision
            or revision > state["revision"] + 1
        ):
            raise InvariantError(
                f"events[{index}].revision must be nondecreasing between 1 "
                "and state revision plus 1"
            )
        previous_event_revision = revision
        details = event["details"]
        if not isinstance(details, dict):
            raise InvariantError(f"events[{index}].details must be an object")
        json_error = _json_validation_error(details)
        if json_error is not None:
            raise InvariantError(f"events[{index}].details {json_error}")
        if _contains_event_lease_token(details):
            raise InvariantError(
                f"events[{index}].details must not contain lease_token"
            )

    return state


def validate_persisted_state(state):
    """Reject candidate-only future event revisions at a disk boundary."""
    validate_state(state)
    if any(event["revision"] > state["revision"] for event in state["events"]):
        raise InvariantError("persisted event has a future revision")
    return state


def _sanitize_event_details(value):
    if isinstance(value, dict):
        return {
            key: _sanitize_event_details(child)
            for key, child in value.items()
            if key != "lease_token"
        }
    if isinstance(value, list):
        return [_sanitize_event_details(child) for child in value]
    return value


def _contains_event_lease_token(value):
    if isinstance(value, dict):
        return "lease_token" in value or any(
            _contains_event_lease_token(child) for child in value.values()
        )
    if isinstance(value, list):
        return any(_contains_event_lease_token(child) for child in value)
    return False


def _append_event_to_candidate(
    state, event_type, actor, task_id, details, now
):
    """Append a validated event to an already isolated candidate state."""
    for value, field in ((event_type, "event_type"), (actor, "actor")):
        if not isinstance(value, str) or not value.strip():
            raise InvariantError(f"{field} must be a non-blank string")
    _validate_timestamp(now, "now")
    if state["events"] and now < state["events"][-1]["at"]:
        raise InvariantError("now must not precede the latest event time")
    if task_id is not None:
        if not isinstance(task_id, str) or not task_id.strip():
            raise InvariantError("task_id must be a non-blank string or null")
        if task_id not in state["tasks"]:
            raise InvariantError("task_id must reference a stored task")
    if not isinstance(details, dict):
        raise InvariantError("event details must be an object")
    json_error = _json_validation_error(details)
    if json_error is not None:
        raise InvariantError(f"event details {json_error}")

    sequence = state["next_event_sequence"]
    event = {
        "seq": sequence,
        "at": now,
        "type": event_type,
        "actor": actor,
        "task_id": task_id,
        "revision": state["revision"] + 1,
        "details": _sanitize_event_details(details),
    }
    state["next_event_sequence"] += 1
    state["events"].append(event)
    return event


def append_event(state, event_type, actor, task_id, details, now):
    """Atomically append one canonical, secret-free queue event."""
    validate_state(state)
    candidate = copy.deepcopy(state)
    event = _append_event_to_candidate(
        candidate, event_type, actor, task_id, details, now
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return copy.deepcopy(event)


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
    validate_persisted_state(state)
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


def dependency_blockers(state, task):
    """Return dependency IDs that have not completed, preserving task order."""
    return [
        dependency_id
        for dependency_id in task["depends_on"]
        if state["tasks"][dependency_id]["status"] != "completed"
    ]


def leased_resources(state, excluding=None, now=None):
    """Map resources to active leased task IDs without exposing claim data."""
    now = utc_now() if now is None else now
    resources = {}
    for task_id in sorted(state["tasks"]):
        task = state["tasks"][task_id]
        if task_id == excluding or task["status"] != "leased":
            continue
        claim = task["claim"]
        expires_at = claim.get("expires_at") if isinstance(claim, dict) else None
        if _is_valid_timestamp(expires_at) and expires_at <= now:
            continue
        for resource in task["resources"]:
            resources.setdefault(resource, []).append(task_id)
    return resources


def _resource_conflicts(task, active_resources):
    return sorted(
        {
            task_id
            for resource in task["resources"]
            for task_id in active_resources.get(resource, [])
            if task_id != task["id"]
        }
    )


def _has_resource_conflict(task, active_resources):
    return any(
        active_resources.get(resource) for resource in task["resources"]
    )


def _derive_state_with_resources(
    state, task, now, active_resources, resource_conflict_probe=None
):
    if task["status"] != "pending":
        return task["status"]

    dependencies = [
        state["tasks"][dependency_id]
        for dependency_id in task["depends_on"]
    ]
    if any(
        dependency["status"] in {"failed", "blocked", "cancelled"}
        for dependency in dependencies
    ):
        return "dependency_failed"
    if dependency_blockers(state, task):
        return "waiting_dependency"
    if task["available_at"] is not None and task["available_at"] > now:
        return "waiting_retry"
    if resource_conflict_probe is None:
        resource_conflict_probe = _resource_conflicts
    if resource_conflict_probe(task, active_resources):
        return "resource_conflict"
    return "ready"


def derive_state(state, task, now):
    """Derive the display state for a task using fixed readiness precedence."""
    active_resources = leased_resources(state, now=now)
    return _derive_state_with_resources(state, task, now, active_resources)


def _has_git_claim_conflict(state, task, observation, now):
    for active in state["tasks"].values():
        if active["status"] != "leased" or active["id"] == task["id"]:
            continue
        claim = active.get("claim")
        if not isinstance(claim, dict) or claim["expires_at"] <= now:
            continue
        binding = claim.get("git")
        if not isinstance(binding, dict):
            continue
        if binding["worktree_id"] == observation["worktree_id"]:
            return True
        if binding["repository_id"] != observation["repository_id"]:
            continue
        if binding["branch"] == observation["branch"]:
            return True
        if gq.resources_overlap(active["resources"], task["resources"]):
            return True
    return False


def claim_task(
    state,
    agent_id,
    now=None,
    role=None,
    labels=None,
    lease_seconds=None,
    git_observation=None,
    task_id=None,
    resume_git=False,
):
    """Claim the highest-priority eligible task with an in-memory lease."""
    validate_state(state)
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise InvariantError("agent_id must be a non-blank string")
    now = utc_now() if now is None else now
    _validate_timestamp(now, "now")
    if role is not None and (
        not isinstance(role, str) or not role.strip()
    ):
        raise InvariantError("role must be a non-blank string or null")
    if task_id is not None:
        _task_id_sequence(task_id)
        if task_id not in state["tasks"]:
            raise InvariantError(f"task not found: {task_id}")
    if not isinstance(resume_git, bool):
        raise InvariantError("resume_git must be a boolean")
    if resume_git and task_id is None:
        raise InvariantError("resume_git requires task_id")
    if labels is None:
        required_labels = set()
    elif isinstance(labels, (list, set)):
        if any(not isinstance(label, str) for label in labels):
            raise InvariantError("labels values must be strings")
        required_labels = set(labels)
    else:
        raise InvariantError("labels must be a list or set")
    if lease_seconds is None:
        lease_seconds = state["config"]["default_lease_seconds"]
    if (
        not isinstance(lease_seconds, int)
        or isinstance(lease_seconds, bool)
        or lease_seconds <= 0
    ):
        raise InvariantError("lease_seconds must be a positive integer")

    active_resources = leased_resources(state, now=now)
    eligible = []
    for task in state["tasks"].values():
        if task_id is not None and task["id"] != task_id:
            continue
        if role is not None and task["role"] != role:
            continue
        if not required_labels.issubset(task["labels"]):
            continue
        if task["attempts"] >= task["max_attempts"]:
            continue
        if task["updated_at"] > now:
            continue
        if (
            _derive_state_with_resources(
                state,
                task,
                now,
                active_resources,
                resource_conflict_probe=_has_resource_conflict,
            )
            != "ready"
        ):
            continue
        if task.get("git_mode") == "commit":
            try:
                gq.require_claimable(git_observation)
            except gq.GitContextError as error:
                if task_id is not None:
                    raise InvariantError(str(error)) from error
                continue
            if task.get("git_recovery") is not None and not resume_git:
                continue
            if _has_git_claim_conflict(
                state, task, git_observation, now
            ):
                continue
        elif resume_git:
            raise InvariantError("resume_git requires a Git-aware task")
        eligible.append(task)
    if not eligible:
        raise NoTaskAvailable("no task is available")

    selected = min(
        eligible,
        key=lambda task: (-task["priority"], _task_id_sequence(task["id"])),
    )
    random_token = secrets.token_urlsafe(32)
    if not isinstance(random_token, str) or not random_token.strip():
        raise InvariantError("generated lease_token must be a non-blank string")
    lease_token = f"lq_{random_token}"
    expires_at = add_seconds(now, lease_seconds)

    candidate = copy.deepcopy(state)
    claimed = candidate["tasks"][selected["id"]]
    claimed["attempts"] += 1
    claimed["status"] = "leased"
    claimed["available_at"] = None
    claimed["claim"] = {
        "agent_id": agent_id,
        "lease_token": lease_token,
        "claimed_at": now,
        "heartbeat_at": now,
        "expires_at": expires_at,
    }
    if candidate["schema_version"] == 2:
        claimed["claim"]["git"] = (
            gq.claim_binding(git_observation)
            if claimed["git_mode"] == "commit"
            else None
        )
    claimed["updated_at"] = now
    append_event(
        candidate,
        "task.claimed",
        agent_id,
        claimed["id"],
        {"lease_seconds": lease_seconds, "attempt": claimed["attempts"]},
        now,
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return {
        "task": copy.deepcopy(state["tasks"][claimed["id"]]),
        "lease_token": lease_token,
        "expires_at": expires_at,
    }


def _canonical_now(now):
    now = utc_now() if now is None else now
    _validate_timestamp(now, "now")
    return now


def _require_monotonic_task_time(task, now, error_type=InvariantError):
    if now < task["updated_at"]:
        raise error_type("now must not be earlier than task updated_at")


def _require_task(state, task_id):
    _task_id_sequence(task_id)
    task = state["tasks"].get(task_id)
    if task is None:
        raise InvariantError(f"task not found: {task_id}")
    return task


def _commit_transition(state, candidate, task_id):
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return copy.deepcopy(state["tasks"][task_id])


def _require_lease_live(state, task_id, agent_id, token, now):
    """Return a live task only after validating current lease ownership."""
    try:
        validate_state(state)
    except InvariantError as error:
        raise LeaseError(f"invalid queue state: {error}") from error
    try:
        _task_id_sequence(task_id)
    except InvariantError as error:
        raise LeaseError("task_id must identify a canonical task") from error
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise LeaseError("agent_id must be a non-blank string")
    if not isinstance(token, str) or not token.strip():
        raise LeaseError("lease token must be a non-blank string")
    try:
        _validate_timestamp(now, "now")
    except InvariantError as error:
        raise LeaseError("now must be a canonical UTC timestamp") from error

    task = state["tasks"].get(task_id)
    if task is None:
        raise LeaseError(f"task not found: {task_id}")
    if task["status"] != "leased" or task["claim"] is None:
        raise LeaseError(f"task is not leased: {task_id}")
    claim = task["claim"]
    if claim["agent_id"] != agent_id:
        raise LeaseError("lease agent does not match")
    if not secrets.compare_digest(claim["lease_token"], token):
        raise LeaseError("lease token does not match")
    _require_monotonic_task_time(task, now, LeaseError)
    if now < claim["heartbeat_at"]:
        raise LeaseError("now must not be earlier than claim heartbeat_at")
    if claim["expires_at"] <= now:
        raise LeaseError("lease is expired")
    return task


def require_lease(state, task_id, agent_id, token, now):
    """Return a detached task only when the worker owns a live lease."""
    return copy.deepcopy(
        _require_lease_live(state, task_id, agent_id, token, now)
    )


def heartbeat_task(
    state,
    task_id,
    agent_id,
    token,
    lease_seconds=None,
    now=None,
):
    """Extend a live worker lease from the current time."""
    validate_state(state)
    now = _canonical_now(now)
    if lease_seconds is None:
        lease_seconds = state["config"]["default_lease_seconds"]
    if (
        not isinstance(lease_seconds, int)
        or isinstance(lease_seconds, bool)
        or lease_seconds <= 0
    ):
        raise InvariantError("lease_seconds must be a positive integer")
    _require_lease_live(state, task_id, agent_id, token, now)
    expires_at = add_seconds(now, lease_seconds)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["claim"]["heartbeat_at"] = now
    task["claim"]["expires_at"] = expires_at
    task["updated_at"] = now
    append_event(
        candidate,
        "task.heartbeat",
        agent_id,
        task_id,
        {"lease_seconds": lease_seconds},
        now,
    )
    return _commit_transition(state, candidate, task_id)


def _validate_text(value, field):
    _validate_bounded_text(value, field, nonempty=True)


def _validate_artifacts(artifacts):
    if not isinstance(artifacts, list) or any(
        not isinstance(artifact, str) or not artifact
        for artifact in artifacts
    ):
        raise InvariantError(
            "artifacts must be a list of nonempty strings"
        )
    json_error = _json_validation_error(artifacts)
    if json_error is not None:
        raise InvariantError(f"artifacts {json_error}")


def complete_task(
    state,
    task_id,
    agent_id,
    token,
    summary,
    artifacts,
    now=None,
):
    """Complete a live leased task with an exact result payload."""
    validate_state(state)
    _validate_text(summary, "summary")
    _validate_artifacts(artifacts)
    now = _canonical_now(now)
    _require_lease_live(state, task_id, agent_id, token, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["status"] = "completed"
    task["result"] = {
        "summary": summary,
        "artifacts": copy.deepcopy(artifacts),
    }
    if candidate["schema_version"] == 2:
        task["result"]["git"] = None
    task["claim"] = None
    task["available_at"] = None
    task["updated_at"] = now
    append_event(
        candidate,
        "task.completed",
        agent_id,
        task_id,
        {"artifact_count": len(artifacts)},
        now,
    )
    return _commit_transition(state, candidate, task_id)


def apply_retry_rule(state, task, message, now):
    """Apply the queue's retry policy to a failed leased attempt."""
    task["claim"] = None
    task["last_error"] = {"message": message, "at": now}
    if task["attempts"] < task["max_attempts"]:
        task["status"] = "pending"
        task["available_at"] = add_seconds(
            now, state["config"]["retry_backoff_seconds"]
        )
    else:
        task["status"] = "failed"
        task["available_at"] = None
    task["updated_at"] = now
    return task


def fail_task(
    state,
    task_id,
    agent_id,
    token,
    message,
    terminal=False,
    now=None,
):
    """Record a worker failure and either retry or terminate the task."""
    validate_state(state)
    _validate_text(message, "message")
    if not isinstance(terminal, bool):
        raise InvariantError("terminal must be a boolean")
    now = _canonical_now(now)
    _require_lease_live(state, task_id, agent_id, token, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    if terminal:
        task["status"] = "failed"
        task["claim"] = None
        task["available_at"] = None
        task["last_error"] = {"message": message, "at": now}
        task["updated_at"] = now
    else:
        apply_retry_rule(candidate, task, message, now)
    append_event(
        candidate,
        "task.failed",
        agent_id,
        task_id,
        {"terminal": terminal, "status": task["status"]},
        now,
    )
    return _commit_transition(state, candidate, task_id)


def release_task(state, task_id, agent_id, token, now=None):
    """Voluntarily return a leased task to immediate pending readiness."""
    validate_state(state)
    now = _canonical_now(now)
    _require_lease_live(state, task_id, agent_id, token, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["status"] = "pending"
    task["claim"] = None
    task["available_at"] = now
    task["updated_at"] = now
    append_event(
        candidate, "task.released", agent_id, task_id, {}, now
    )
    return _commit_transition(state, candidate, task_id)


def sweep_expired(state, now=None):
    """Expire every elapsed lease in deterministic task-ID order."""
    validate_state(state)
    now = _canonical_now(now)
    candidate = copy.deepcopy(state)
    changed = []
    for task_id in sorted(candidate["tasks"]):
        task = candidate["tasks"][task_id]
        if (
            task["status"] != "leased"
            or task["claim"]["expires_at"] > now
        ):
            continue
        _require_monotonic_task_time(task, now)
        apply_retry_rule(candidate, task, "lease expired", now)
        _append_event_to_candidate(
            candidate,
            "task.lease_expired",
            "system",
            task_id,
            {"status": task["status"], "attempt": task["attempts"]},
            now,
        )
        changed.append(task_id)
    if not changed:
        return []
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return copy.deepcopy(changed)


def retry_task(state, task_id, additional_attempts=1, now=None):
    """Administratively grant more attempts to a failed task."""
    validate_state(state)
    if (
        not isinstance(additional_attempts, int)
        or isinstance(additional_attempts, bool)
        or additional_attempts <= 0
    ):
        raise InvariantError("additional_attempts must be a positive integer")
    now = _canonical_now(now)
    task = _require_task(state, task_id)
    if task["status"] != "failed":
        raise InvariantError("only a failed task can be retried")
    _require_monotonic_task_time(task, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["max_attempts"] += additional_attempts
    task["status"] = "pending"
    task["available_at"] = now
    task["claim"] = None
    task["updated_at"] = now
    append_event(
        candidate,
        "task.retried",
        "operator",
        task_id,
        {"additional_attempts": additional_attempts},
        now,
    )
    return _commit_transition(state, candidate, task_id)


def block_task(state, task_id, reason, now=None):
    """Administratively block a pending task with an audit reason."""
    validate_state(state)
    _validate_text(reason, "reason")
    now = _canonical_now(now)
    task = _require_task(state, task_id)
    if task["status"] != "pending":
        raise InvariantError("only a pending task can be blocked")
    _require_monotonic_task_time(task, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["status"] = "blocked"
    task["available_at"] = None
    task["claim"] = None
    task["last_error"] = {
        "message": reason,
        "at": now,
        "kind": "blocked",
    }
    task["updated_at"] = now
    append_event(
        candidate,
        "task.blocked",
        "operator",
        task_id,
        {"reason": reason},
        now,
    )
    return _commit_transition(state, candidate, task_id)


def unblock_task(state, task_id, now=None):
    """Administratively return a blocked task to immediate readiness."""
    validate_state(state)
    now = _canonical_now(now)
    task = _require_task(state, task_id)
    if task["status"] != "blocked":
        raise InvariantError("only a blocked task can be unblocked")
    _require_monotonic_task_time(task, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["status"] = "pending"
    task["available_at"] = now
    task["claim"] = None
    task["updated_at"] = now
    append_event(
        candidate, "task.unblocked", "operator", task_id, {}, now
    )
    return _commit_transition(state, candidate, task_id)


def parse_compaction_cutoff(value):
    """Parse a date or canonical UTC timestamp into a canonical cutoff."""
    if not isinstance(value, str):
        raise InvariantError(
            "before must be YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ"
        )
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value, flags=re.ASCII):
        candidate = value + "T00:00:00Z"
    else:
        candidate = value
    try:
        _validate_timestamp(candidate, "before")
    except InvariantError as error:
        raise InvariantError(
            "before must be YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ"
        ) from error
    return candidate


def _contains_task_reference(value, task_ids):
    """Return whether JSON metadata contains an exact removed task ID value."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            if item in task_ids:
                return True
        elif isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return False


def compact_state(state, before, now=None):
    """Prune closed history while preserving all retained dependency closure."""
    validate_state(state)
    cutoff = parse_compaction_cutoff(before)
    now = _canonical_now(now)
    candidate = copy.deepcopy(state)
    terminal = {"completed", "failed", "cancelled"}

    workflow_members = {}
    task_units = {}
    for task_id, task in candidate["tasks"].items():
        workflow_id = task["workflow_id"]
        unit = ("workflow", workflow_id) if workflow_id is not None else (
            "task", task_id
        )
        task_units[task_id] = unit
        if workflow_id is not None:
            workflow_members.setdefault(workflow_id, []).append(task_id)

    candidate_units = set()
    for workflow_id, task_ids in workflow_members.items():
        if all(
            candidate["tasks"][task_id]["status"] in terminal
            and candidate["tasks"][task_id]["updated_at"] < cutoff
            for task_id in task_ids
        ):
            candidate_units.add(("workflow", workflow_id))
    for task_id, task in candidate["tasks"].items():
        if (
            task["workflow_id"] is None
            and task["status"] in terminal
            and task["updated_at"] < cutoff
        ):
            candidate_units.add(("task", task_id))

    # A candidate unit is retained if any retained unit depends on it. Propagate
    # that requirement backwards through candidates without recursion.
    required_units = set()
    reverse_unit_edges = {unit: set() for unit in candidate_units}
    for dependent_id, task in candidate["tasks"].items():
        dependent_unit = task_units[dependent_id]
        for dependency_id in task["depends_on"]:
            dependency_unit = task_units[dependency_id]
            if dependency_unit not in candidate_units:
                continue
            if dependent_unit not in candidate_units:
                required_units.add(dependency_unit)
            elif dependent_unit != dependency_unit:
                reverse_unit_edges.setdefault(dependent_unit, set()).add(
                    dependency_unit
                )
    stack = list(required_units)
    while stack:
        retained_unit = stack.pop()
        for dependency_unit in reverse_unit_edges.get(retained_unit, ()):
            if dependency_unit not in required_units:
                required_units.add(dependency_unit)
                stack.append(dependency_unit)

    removed_units = candidate_units.difference(required_units)
    removed_task_ids = sorted(
        task_id
        for task_id, unit in task_units.items()
        if unit in removed_units
    )
    removed_workflow_ids = sorted(
        unit_id for kind, unit_id in removed_units if kind == "workflow"
    )
    removed_task_set = set(removed_task_ids)
    kept_events = []
    removed_event_count = 0
    for event in candidate["events"]:
        if (
            event["at"] < cutoff
            or event["task_id"] in removed_task_set
            or _contains_task_reference(event["details"], removed_task_set)
        ):
            removed_event_count += 1
        else:
            kept_events.append(event)

    summary = {
        "removed_event_count": removed_event_count,
        "removed_task_count": len(removed_task_ids),
        "removed_task_ids": removed_task_ids,
        "removed_workflow_count": len(removed_workflow_ids),
        "removed_workflow_ids": removed_workflow_ids,
    }
    if not removed_task_ids and not removed_event_count:
        return copy.deepcopy(summary)

    for task_id in removed_task_ids:
        del candidate["tasks"][task_id]
    candidate["events"] = kept_events
    _append_event_to_candidate(
        candidate, "queue.compacted", "operator", None, summary, now
    )
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return copy.deepcopy(summary)


def cancel_task(state, task_id, now=None):
    """Administratively cancel a non-active, non-completed task."""
    validate_state(state)
    now = _canonical_now(now)
    task = _require_task(state, task_id)
    if task["status"] not in {"pending", "blocked", "failed"}:
        raise InvariantError(
            f"cannot cancel task with status {task['status']}"
        )
    _require_monotonic_task_time(task, now)

    candidate = copy.deepcopy(state)
    task = candidate["tasks"][task_id]
    task["status"] = "cancelled"
    task["available_at"] = None
    task["claim"] = None
    task["updated_at"] = now
    append_event(
        candidate, "task.cancelled", "operator", task_id, {}, now
    )
    return _commit_transition(state, candidate, task_id)


def status_rows(
    state,
    now,
    workflow=None,
    assignee=None,
    role=None,
    labels=None,
    state_filter=None,
):
    """Return redacted, stable task rows for status displays."""
    validate_state(state)
    _validate_timestamp(now, "now")
    required_labels = set(labels or [])
    active_resources = leased_resources(state, now=now)
    rows = []
    for task_id in sorted(state["tasks"]):
        task = state["tasks"][task_id]
        claim = task["claim"] if isinstance(task["claim"], dict) else {}
        task_assignee = claim.get("agent_id", "")
        if workflow is not None and task["workflow_id"] != workflow:
            continue
        if assignee is not None and task_assignee != assignee:
            continue
        if role is not None and task["role"] != role:
            continue
        if not required_labels.issubset(task["labels"]):
            continue
        derived = _derive_state_with_resources(
            state, task, now, active_resources
        )
        if (
            state_filter is not None
            and state_filter not in {task["status"], derived}
        ):
            continue

        if derived in {"dependency_failed", "waiting_dependency"}:
            blocked_by = dependency_blockers(state, task)
        elif derived == "resource_conflict":
            blocked_by = _resource_conflicts(task, active_resources)
        else:
            blocked_by = []
        rows.append(
            {
                "id": task_id,
                "workflow": task["workflow_id"] or "",
                "role": task["role"] or "",
                "state": derived,
                "priority": task["priority"],
                "assignee": task_assignee,
                "lease_until": claim.get("expires_at", ""),
                "attempts": f'{task["attempts"]}/{task["max_attempts"]}',
                "depends_on": ",".join(task["depends_on"]),
                "blocked_by": ",".join(blocked_by),
                "resources": ",".join(task["resources"]),
                "title": task["title"],
            }
        )
    return rows


def escape_tsv(value):
    """Escape a value for stable, visible TSV and terminal display."""
    escapes = {
        "\\": "\\\\",
        "\t": "\\t",
        "\r": "\\r",
        "\n": "\\n",
    }
    result = []
    for character in str(value):
        if character in escapes:
            result.append(escapes[character])
        elif (
            unicodedata.category(character) in {"Cc", "Cf", "Cs", "Zl", "Zp"}
            and character != "\u200d"
        ):
            result.append(f"\\u{ord(character):04X}")
        else:
            result.append(character)
    return "".join(result)


def render_tsv(
    state,
    now,
    workflow=None,
    assignee=None,
    role=None,
    labels=None,
    state_filter=None,
):
    """Render a complete one-line-per-task TSV status projection."""
    rows = status_rows(
        state,
        now,
        workflow=workflow,
        assignee=assignee,
        role=role,
        labels=labels,
        state_filter=state_filter,
    )
    lines = [
        f'# queue_revision: {state["revision"]}',
        "\t".join(TSV_COLUMNS),
    ]
    lines.extend(
        "\t".join(escape_tsv(row[column]) for column in TSV_COLUMNS)
        for row in rows
    )
    return "\n".join(lines) + "\n"


def _is_regional_indicator(character):
    return "\U0001f1e6" <= character <= "\U0001f1ff"


def _is_variation_selector(character):
    return (
        "\ufe00" <= character <= "\ufe0f"
        or "\U000e0100" <= character <= "\U000e01ef"
    )


def _is_emoji_modifier(character):
    return "\U0001f3fb" <= character <= "\U0001f3ff"


def _codepoint_in_ranges(codepoint, ranges):
    return any(start <= codepoint <= end for start, end in ranges)


def _is_emoji_variation_base(character):
    """Return whether one character is in the supported emoji base ranges."""
    codepoint = ord(character)
    return (
        codepoint in EMOJI_VARIATION_BASE_CODEPOINTS
        or _codepoint_in_ranges(codepoint, EMOJI_VARIATION_BASE_RANGES)
    )


def _is_extended_pictographic(character):
    return _codepoint_in_ranges(ord(character), EXTENDED_PICTOGRAPHIC_RANGES)


def _is_zwj_joinable_pictograph(character):
    # Legacy text symbols below U+2300 are Extended_Pictographic but do not form
    # supported terminal emoji ZWJ clusters in this deliberately small heuristic.
    return ord(character) >= 0x2300 and _is_extended_pictographic(character)


def _is_cluster_attachment(character):
    return (
        unicodedata.category(character).startswith("M")
        or _is_variation_selector(character)
        or _is_emoji_modifier(character)
    )


def _base_display_width(character):
    category = unicodedata.category(character)
    if category.startswith("C") or category in {"Zl", "Zp"}:
        return 0
    return 2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1


def display_width(value):
    """Return terminal cells with small stdlib-only emoji cluster handling."""
    characters = unicodedata.normalize("NFC", str(value))
    width = 0
    index = 0
    while index < len(characters):
        character = characters[index]
        if _is_regional_indicator(character):
            run_end = index + 1
            while (
                run_end < len(characters)
                and _is_regional_indicator(characters[run_end])
            ):
                run_end += 1
            width += ((run_end - index + 1) // 2) * 2
            index = run_end
            continue
        if _is_cluster_attachment(character) or character == "\u200d":
            index += 1
            continue

        cluster_width = _base_display_width(character)
        cluster_accepts_vs16 = _is_emoji_variation_base(character)
        cluster_accepts_keycap = character in "#*0123456789"
        cluster_joins_zwj = _is_zwj_joinable_pictograph(character)
        index += 1
        while index < len(characters):
            while (
                index < len(characters)
                and _is_cluster_attachment(characters[index])
            ):
                if characters[index] == "\ufe0f" and cluster_accepts_vs16:
                    cluster_width = max(cluster_width, 2)
                if characters[index] == "\u20e3" and cluster_accepts_keycap:
                    cluster_width = max(cluster_width, 2)
                index += 1
            if index >= len(characters) or characters[index] != "\u200d":
                break
            index += 1
            if index >= len(characters):
                break
            if (
                not cluster_joins_zwj
                or not _is_zwj_joinable_pictograph(characters[index])
            ):
                break
            cluster_width = max(
                cluster_width, _base_display_width(characters[index])
            )
            index += 1
        width += cluster_width
    return width


def _pad_display(value, width):
    return value + " " * (width - display_width(value))


def format_terminal_table(rows):
    """Format status rows as a small dependency-free aligned table."""
    display_rows = [
        [escape_tsv(row[column]) for column in TSV_COLUMNS] for row in rows
    ]
    widths = [
        max(
            [
                display_width(column),
                *(display_width(row[index]) for row in display_rows),
            ]
        )
        for index, column in enumerate(TSV_COLUMNS)
    ]
    lines = [
        "  ".join(
            _pad_display(column, widths[index])
            for index, column in enumerate(TSV_COLUMNS)
        ),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(
            _pad_display(value, widths[index])
            for index, value in enumerate(row)
        )
        for row in display_rows
    )
    return "\n".join(lines)


def render_empty_tsv(revision):
    """Render an empty queue projection for the given revision."""
    header = "\t".join(TSV_COLUMNS)
    return f"# queue_revision: {revision}\n{header}\n"


def resolve_queue_path(explicit=None, environ=None, cwd=None):
    """Resolve the shared queue without invoking Git or another process."""
    environment = os.environ if environ is None else environ
    working_directory = Path.cwd() if cwd is None else Path(cwd)
    if explicit is not None:
        if not str(explicit).strip():
            raise QueueError("--queue must not be blank")
        selected = Path(explicit)
    elif "AGENT_QUEUE_PATH" in environment:
        value = environment["AGENT_QUEUE_PATH"]
        if not isinstance(value, str) or not value.strip():
            raise QueueError("AGENT_QUEUE_PATH must not be blank")
        selected = Path(value)
    else:
        absolute_cwd = working_directory.expanduser().absolute()
        root = next(
            (parent for parent in (absolute_cwd, *absolute_cwd.parents)
             if (parent / ".git").exists()),
            absolute_cwd,
        )
        selected = root / ".agent-queue" / "queue.json"
    return selected.expanduser().absolute()


def _strict_json_constant(value):
    raise ValueError(f"non-finite JSON constant {value}")


def _read_json_text(text, source):
    try:
        return json.loads(text, parse_constant=_strict_json_constant)
    except (json.JSONDecodeError, ValueError) as error:
        raise InvariantError(f"invalid JSON in {source}: {error}") from error


def load_state(path):
    """Load and fully validate a queue state from strict JSON."""
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise QueueError(f"queue does not exist: {path}") from error
    except UnicodeError as error:
        raise InvariantError(f"invalid UTF-8 in queue {path}: {error}") from error
    except OSError as error:
        raise QueueError(f"cannot read queue {path}: {error}") from error
    state = _read_json_text(text, path)
    try:
        return validate_persisted_state(state)
    except InvariantError as error:
        raise InvariantError(f"invalid queue state: {error}") from error


def tsv_revision(path):
    """Return a TSV projection revision, or None for missing/malformed data."""
    try:
        first_line = Path(path).read_text(encoding="utf-8").splitlines()[0]
    except (OSError, IndexError, UnicodeError):
        return None
    match = re.fullmatch(r"# queue_revision: (0|[1-9]\d*)", first_line)
    return int(match.group(1)) if match is not None else None


def _diagnostic_item(code, path, message):
    return {"code": code, "path": str(path), "message": message}


def _remove_diagnostic_directory(path, lock_path):
    """Remove one already-validated directory without following replacements."""
    path = Path(path)
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
        raise InvariantError(f"diagnostic target must be a real directory: {path}")
    lock_path = Path(lock_path)
    quarantine = lock_path.with_name(
        f".{lock_path.name}.orphan-{secrets.token_hex(12)}"
    )
    os.replace(path, quarantine)
    moved = os.lstat(quarantine)
    if (before.st_dev, before.st_ino) != (moved.st_dev, moved.st_ino):
        raise InvariantError(f"diagnostic target changed while removing: {path}")
    try:
        shutil.rmtree(quarantine)
    except OSError:
        return False
    try:
        os.lstat(quarantine)
    except FileNotFoundError:
        return True
    return False


def doctor(path, repair=False, now=None):
    """Inspect source truth and derived queue artifacts under the kernel guard."""
    if not isinstance(repair, bool):
        raise QueueError("repair must be a boolean")
    path = Path(path).expanduser().absolute()
    report = {
        "ok": False,
        "queue": str(path),
        "revision": None,
        "issues": [],
        "repairs": [],
    }
    timeout, stale = peek_lock_config(path)
    lock = QueueLock(path, timeout, stale)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        report["issues"].append(_diagnostic_item(
            "guard.unavailable", lock.guard_path,
            f"queue lock guard parent is unavailable: {error}",
        ))
        return report
    try:
        lock._acquire_guard(time.monotonic() + timeout)
    except LockTimeout as error:
        report["issues"].append(_diagnostic_item(
            "lock.timeout", lock.guard_path,
            f"could not acquire queue lock guard: {error}",
        ))
        return report
    except InvariantError as error:
        report["issues"].append(_diagnostic_item(
            "guard.invalid", lock.guard_path,
            f"queue lock guard is invalid: {error}",
        ))
        return report
    except QueueError as error:
        report["issues"].append(_diagnostic_item(
            "guard.unavailable", lock.guard_path,
            f"queue lock guard is unavailable: {error}",
        ))
        return report
    try:
        try:
            source_stat = os.lstat(path)
        except FileNotFoundError:
            report["issues"].append(_diagnostic_item(
                "source.missing", path, "queue source JSON is missing"
            ))
            return report
        except OSError as error:
            report["issues"].append(_diagnostic_item(
                "source.unreadable", path,
                f"cannot inspect queue source JSON: {error}",
            ))
            return report
        if stat.S_ISLNK(source_stat.st_mode) or not stat.S_ISREG(source_stat.st_mode):
            report["issues"].append(_diagnostic_item(
                "source.unsafe", path,
                "queue source JSON must be a real regular file",
            ))
            return report
        try:
            source_text = path.read_text(encoding="utf-8")
        except OSError as error:
            report["issues"].append(_diagnostic_item(
                "source.unreadable", path,
                f"cannot read queue source JSON: {error}",
            ))
            return report
        except UnicodeError as error:
            report["issues"].append(_diagnostic_item(
                "source.invalid", path,
                f"queue source JSON is invalid UTF-8: {error}",
            ))
            return report
        try:
            state = _read_json_text(source_text, path)
            validate_persisted_state(state)
        except (UnicodeError, InvariantError) as error:
            report["issues"].append(_diagnostic_item(
                "source.invalid", path, f"queue source JSON is invalid: {error}"
            ))
            return report

        report["revision"] = state["revision"]
        projection_now = _canonical_now(now)

        # A lock directory cannot be live while this process owns the kernel
        # guard. Only stale, safely-identifiable directories may be repaired.
        try:
            lock_stat = os.lstat(lock.path)
        except FileNotFoundError:
            lock_stat = None
        except OSError as error:
            report["issues"].append(_diagnostic_item(
                "lock.unreadable", lock.path,
                f"cannot inspect queue lock directory: {error}",
            ))
            return report
        if lock_stat is not None:
            if stat.S_ISLNK(lock_stat.st_mode) or not stat.S_ISDIR(lock_stat.st_mode):
                raise InvariantError("queue lock must be a real directory")
            try:
                owner_stat = os.lstat(lock.owner_path)
            except FileNotFoundError:
                owner_stat = None
            except OSError as error:
                report["issues"].append(_diagnostic_item(
                    "lock.unreadable", lock.owner_path,
                    f"cannot inspect queue lock owner: {error}",
                ))
                return report
            if owner_stat is not None and (
                stat.S_ISLNK(owner_stat.st_mode)
                or not stat.S_ISREG(owner_stat.st_mode)
            ):
                report["issues"].append(_diagnostic_item(
                    "lock.owner_unsafe", lock.owner_path,
                    "lock owner must be a real regular file",
                ))
            else:
                reclaimable = lock._reclaimable()
                if reclaimable is not None:
                    report["issues"].append(_diagnostic_item(
                        "lock.stale", lock.path, "stale queue lock directory"
                    ))
                    if repair:
                        if lock._rename_and_remove("orphan"):
                            report["repairs"].append(_diagnostic_item(
                                "lock.removed", lock.path,
                                "removed stale queue lock directory",
                            ))
                        else:
                            report["issues"].append(_diagnostic_item(
                                "lock.remove_failed", lock.path,
                                "stale queue lock cleanup was not completed",
                            ))
                else:
                    report["issues"].append(_diagnostic_item(
                        "lock.orphan", lock.path,
                        "queue lock metadata is not safely stale",
                    ))

        artifact_pattern = re.compile(
            rf"^\.{re.escape(lock.path.name)}\.(?:orphan|release)-[0-9a-f]{{24}}$",
            flags=re.ASCII,
        )
        legacy_artifact_pattern = re.compile(
            rf"^\.\.{re.escape(lock.path.name)}\."
            rf"(?:orphan|release)-[0-9a-f]{{24}}\.doctor-[0-9a-f]{{24}}$",
            flags=re.ASCII,
        )
        try:
            artifacts = sorted(
                child for child in path.parent.iterdir()
                if (
                    artifact_pattern.fullmatch(child.name)
                    or legacy_artifact_pattern.fullmatch(child.name)
                )
            )
        except OSError as error:
            report["issues"].append(_diagnostic_item(
                "lock_artifacts.unreadable", path.parent,
                f"cannot inspect orphan lock artifacts: {error}",
            ))
            return report
        for artifact in artifacts:
            try:
                artifact_stat = os.lstat(artifact)
            except OSError as error:
                report["issues"].append(_diagnostic_item(
                    "lock_artifacts.unreadable", artifact,
                    f"cannot inspect orphan lock artifact: {error}",
                ))
                return report
            if stat.S_ISLNK(artifact_stat.st_mode) or not stat.S_ISDIR(
                artifact_stat.st_mode
            ):
                report["issues"].append(_diagnostic_item(
                    "lock_artifact.unsafe", artifact,
                    "orphan lock artifact must be a real directory",
                ))
                continue
            report["issues"].append(_diagnostic_item(
                "lock_artifact.orphan", artifact,
                "orphan lock artifact directory",
            ))
            if repair:
                if _remove_diagnostic_directory(artifact, lock.path):
                    report["repairs"].append(_diagnostic_item(
                        "lock_artifact.removed", artifact,
                        "removed orphan lock artifact directory",
                    ))
                else:
                    report["issues"].append(_diagnostic_item(
                        "lock_artifact.remove_failed", artifact,
                        "orphan lock artifact cleanup was not completed",
                    ))

        tsv_path = path.with_suffix(".tsv")
        expected = render_tsv(state, projection_now)
        tsv_issue = None
        try:
            tsv_stat = os.lstat(tsv_path)
        except FileNotFoundError:
            tsv_issue = _diagnostic_item(
                "tsv.missing", tsv_path, "derived TSV is missing"
            )
        except OSError as error:
            tsv_issue = _diagnostic_item(
                "tsv.unreadable", tsv_path,
                f"cannot inspect derived TSV: {error}",
            )
        else:
            if stat.S_ISLNK(tsv_stat.st_mode) or not stat.S_ISREG(tsv_stat.st_mode):
                tsv_issue = _diagnostic_item(
                    "tsv.unsafe", tsv_path,
                    "derived TSV must be a real regular file",
                )
            else:
                try:
                    actual = tsv_path.read_text(encoding="utf-8")
                except OSError as error:
                    tsv_issue = _diagnostic_item(
                        "tsv.unreadable", tsv_path,
                        f"cannot read derived TSV: {error}",
                    )
                except UnicodeError:
                    tsv_issue = _diagnostic_item(
                        "tsv.malformed", tsv_path,
                        "derived TSV is not readable UTF-8",
                    )
                else:
                    revision = tsv_revision(tsv_path)
                    if revision is None:
                        tsv_issue = _diagnostic_item(
                            "tsv.malformed", tsv_path,
                            "derived TSV has a malformed revision or header",
                        )
                    elif revision != state["revision"]:
                        tsv_issue = _diagnostic_item(
                            "tsv.stale", tsv_path,
                            "derived TSV revision does not match source",
                        )
                    elif actual != expected:
                        tsv_issue = _diagnostic_item(
                            "tsv.content", tsv_path,
                            "derived TSV content does not match source",
                        )
        if tsv_issue is not None:
            report["issues"].append(tsv_issue)
            if repair and tsv_issue["code"] not in {
                "tsv.unsafe", "tsv.unreadable"
            }:
                try:
                    atomic_write_text(tsv_path, expected)
                except OSError as error:
                    report["issues"].append(_diagnostic_item(
                        "tsv.repair_failed", tsv_path,
                        f"could not rebuild derived TSV: {error}",
                    ))
                else:
                    report["repairs"].append(_diagnostic_item(
                        "tsv.rebuilt", tsv_path,
                        "rebuilt derived TSV from source",
                    ))

        repaired_codes = {item["code"] for item in report["repairs"]}
        repair_for_issue = {
            "lock.stale": "lock.removed",
            "lock_artifact.orphan": "lock_artifact.removed",
            "tsv.missing": "tsv.rebuilt",
            "tsv.malformed": "tsv.rebuilt",
            "tsv.stale": "tsv.rebuilt",
            "tsv.content": "tsv.rebuilt",
        }
        report["ok"] = not report["issues"] or (
            repair and all(
                repair_for_issue.get(issue["code"]) in repaired_codes
                for issue in report["issues"]
            )
        )
        return report
    except InvariantError as error:
        report["revision"] = None
        report["issues"] = [_diagnostic_item(
            "lock.invalid", lock.path, f"queue lock is invalid: {error}"
        )]
        report["repairs"] = []
        return report
    except OSError as error:
        report["ok"] = False
        report["issues"].append(_diagnostic_item(
            "filesystem.error", path,
            f"queue diagnostic filesystem operation failed: {error}",
        ))
        return report
    finally:
        try:
            lock._release_guard()
        except OSError as error:
            report["ok"] = False
            report["issues"].append(_diagnostic_item(
                "guard.release_failed", lock.guard_path,
                f"queue lock guard release failed: {error}",
            ))


def peek_lock_config(path):
    """Read only safe lock timing hints from an otherwise untrusted file."""
    defaults = fixed_config()
    fallback = (
        defaults["lock_timeout_seconds"], defaults["stale_lock_seconds"]
    )
    try:
        source = _read_json_text(Path(path).read_text(encoding="utf-8"), path)
        config = source["config"]
        timeout = config["lock_timeout_seconds"]
        stale = config["stale_lock_seconds"]
    except (OSError, UnicodeError, KeyError, TypeError, InvariantError):
        return fallback
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value <= 0
        for value in (timeout, stale)
    ):
        return fallback
    return timeout, stale


def _parse_lock_owner(path):
    try:
        raw = _read_json_text(Path(path).read_text(encoding="utf-8"), path)
    except (OSError, UnicodeError, InvariantError):
        return None
    fields = {"token", "pid", "hostname", "acquired_at", "stale_after"}
    if not isinstance(raw, dict) or set(raw) != fields:
        return None
    if (
        not isinstance(raw["token"], str) or not raw["token"]
        or not isinstance(raw["pid"], int) or isinstance(raw["pid"], bool)
        or raw["pid"] <= 0
        or not isinstance(raw["hostname"], str) or not raw["hostname"]
    ):
        return None
    try:
        _validate_timestamp(raw["acquired_at"], "acquired_at")
        _validate_timestamp(raw["stale_after"], "stale_after")
    except InvariantError:
        return None
    return raw


class QueueLock:
    """Same-host mutual exclusion using a kernel guard plus owner directory."""

    def __init__(self, queue_path, lock_timeout=5, stale_seconds=30):
        for value, name in ((lock_timeout, "lock_timeout"),
                            (stale_seconds, "stale_seconds")):
            if (not isinstance(value, (int, float)) or isinstance(value, bool)
                    or value <= 0):
                raise QueueError(f"{name} must be positive")
        self.queue_path = Path(queue_path)
        self.path = Path(str(self.queue_path) + ".lock")
        self.guard_path = Path(str(self.queue_path) + ".lock.guard")
        self.owner_path = self.path / "owner.json"
        self.lock_timeout = lock_timeout
        self.stale_seconds = stale_seconds
        self.token = secrets.token_urlsafe(24)
        self.acquired = False
        self._guard_descriptor = None

    def _open_guard(self):
        flags = os.O_RDWR
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW

        try:
            descriptor = os.open(
                self.guard_path, flags | os.O_CREAT | os.O_EXCL, 0o600
            )
        except FileExistsError:
            return self._open_existing_guard(flags)
        except OSError as error:
            raise QueueError(f"cannot create queue lock guard: {error}") from error

        created = os.fstat(descriptor)
        try:
            remaining = memoryview(GUARD_MARKER)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("short write initializing queue lock guard")
                remaining = remaining[written:]
            os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            return descriptor
        except Exception as error:
            os.close(descriptor)
            try:
                current = os.lstat(self.guard_path)
                if (current.st_dev, current.st_ino) == (
                        created.st_dev, created.st_ino):
                    self.guard_path.unlink()
            except OSError:
                pass
            if isinstance(error, QueueError):
                raise
            raise QueueError(
                f"cannot initialize queue lock guard: {error}"
            ) from error

    def _open_existing_guard(self, flags):
        try:
            before = os.lstat(self.guard_path)
        except OSError as error:
            raise QueueError(f"cannot inspect queue lock guard: {error}") from error
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise InvariantError("queue lock guard must be a regular file")
        try:
            descriptor = os.open(self.guard_path, flags)
        except OSError as error:
            raise QueueError(f"cannot open queue lock guard: {error}") from error
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or (before.st_dev, before.st_ino)
                != (opened.st_dev, opened.st_ino)
            ):
                raise InvariantError("queue lock guard changed while opening")
            if opened.st_size != len(GUARD_MARKER):
                raise InvariantError(
                    "queue lock guard has an invalid marker; manual repair required"
                )
            os.lseek(descriptor, 0, os.SEEK_SET)
            marker = b""
            while len(marker) < len(GUARD_MARKER):
                chunk = os.read(descriptor, len(GUARD_MARKER) - len(marker))
                if not chunk:
                    break
                marker += chunk
            if marker != GUARD_MARKER:
                raise InvariantError(
                    "queue lock guard has an invalid marker; manual repair required"
                )
            os.lseek(descriptor, 0, os.SEEK_SET)
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    def _try_guard_lock(self, descriptor):
        try:
            if LOCK_BACKEND == "fcntl":
                _fcntl.flock(
                    descriptor, _fcntl.LOCK_EX | _fcntl.LOCK_NB
                )
            elif LOCK_BACKEND == "msvcrt":
                os.lseek(descriptor, 0, os.SEEK_SET)
                _msvcrt.locking(descriptor, _msvcrt.LK_NBLCK, 1)
            else:
                raise QueueError(
                    "no supported local locking backend is available"
                )
            return True
        except (BlockingIOError, PermissionError):
            return False
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                return False
            raise QueueError(f"cannot lock queue guard: {error}") from error

    def _unlock_guard(self, descriptor):
        if LOCK_BACKEND == "fcntl":
            _fcntl.flock(descriptor, _fcntl.LOCK_UN)
        elif LOCK_BACKEND == "msvcrt":
            os.lseek(descriptor, 0, os.SEEK_SET)
            _msvcrt.locking(descriptor, _msvcrt.LK_UNLCK, 1)

    def _acquire_guard(self, deadline):
        if LOCK_BACKEND is None:
            raise QueueError(
                "no supported local locking backend is available"
            )
        descriptor = self._open_guard()
        try:
            while not self._try_guard_lock(descriptor):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeout(
                        f"timed out waiting for queue lock: {self.path}"
                    )
                time.sleep(min(remaining, random.uniform(0.005, 0.02)))
        except Exception:
            os.close(descriptor)
            raise
        self._guard_descriptor = descriptor

    def _release_guard(self):
        descriptor = self._guard_descriptor
        self._guard_descriptor = None
        if descriptor is None:
            return
        try:
            self._unlock_guard(descriptor)
        finally:
            os.close(descriptor)

    def _lock_identity(self):
        try:
            directory = os.lstat(self.path)
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(directory.st_mode) or not stat.S_ISDIR(directory.st_mode):
            raise InvariantError("queue lock must be a real directory")
        owner = _parse_lock_owner(self.owner_path)
        try:
            owner_stat = self.owner_path.stat()
            owner_marker = (owner_stat.st_ino, owner_stat.st_size,
                            owner_stat.st_mtime_ns)
        except FileNotFoundError:
            owner_marker = None
        return (
            directory.st_dev,
            directory.st_ino,
            owner["token"] if owner is not None else None,
            owner_marker,
        )

    def _reclaimable(self):
        identity = self._lock_identity()
        if identity is None:
            return None
        owner = _parse_lock_owner(self.owner_path)
        if owner is not None:
            return identity if owner["stale_after"] <= utc_now() else None
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return None
        return identity if age > self.stale_seconds else None

    def _reclaim(self, expected_identity):
        owns_guard = self._guard_descriptor is not None
        if not owns_guard:
            self.queue_path.parent.mkdir(parents=True, exist_ok=True)
            self._acquire_guard(time.monotonic() + self.lock_timeout)
        try:
            if self._lock_identity() != expected_identity:
                return False
            return self._rename_and_remove("orphan")
        finally:
            if not owns_guard:
                self._release_guard()

    def _rename_and_remove(self, kind):
        try:
            source = os.lstat(self.path)
        except FileNotFoundError:
            return False
        if stat.S_ISLNK(source.st_mode) or not stat.S_ISDIR(source.st_mode):
            raise InvariantError("queue lock must be a real directory")
        orphan = self.path.with_name(
            f".{self.path.name}.{kind}-{secrets.token_hex(12)}"
        )
        try:
            os.replace(self.path, orphan)
        except FileNotFoundError:
            return False
        except OSError:
            return False
        moved = os.lstat(orphan)
        if (moved.st_dev, moved.st_ino) != (source.st_dev, source.st_ino):
            raise InvariantError("queue lock changed while removing")
        try:
            shutil.rmtree(orphan)
        except OSError:
            return False
        try:
            os.lstat(orphan)
        except FileNotFoundError:
            return True
        return False

    def __enter__(self):
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.lock_timeout
        self._acquire_guard(deadline)
        try:
            while True:
                try:
                    self.path.mkdir()
                except FileExistsError:
                    expected = self._reclaimable()
                    if expected is not None:
                        if self._lock_identity() == expected:
                            self._rename_and_remove("orphan")
                        continue
                else:
                    acquired_at = utc_now()
                    owner = {
                        "token": self.token,
                        "pid": os.getpid(),
                        "hostname": socket.gethostname(),
                        "acquired_at": acquired_at,
                        "stale_after": add_seconds(
                            acquired_at, max(1, int(self.stale_seconds))
                        ),
                    }
                    try:
                        atomic_write_text(
                            self.owner_path,
                            json.dumps(owner, allow_nan=False, sort_keys=True) + "\n",
                        )
                    except Exception:
                        try:
                            self.path.rmdir()
                        except OSError:
                            pass
                        raise
                    self.acquired = True
                    return self
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeout(
                        f"timed out waiting for queue lock: {self.path}"
                    )
                time.sleep(min(remaining, random.uniform(0.005, 0.02)))
        except Exception:
            self._release_guard()
            raise

    def __exit__(self, _error_type, _error, _traceback):
        if not self.acquired:
            self._release_guard()
            return False
        try:
            owner = _parse_lock_owner(self.owner_path)
            if (owner is None or not secrets.compare_digest(
                    owner["token"], self.token)):
                self.acquired = False
                return False
            self._rename_and_remove("release")
        finally:
            self.acquired = False
            self._release_guard()
        return False


def _render_projection(state, now):
    return render_tsv(state, now)


def _repair_tsv(path, state, now, force=False):
    tsv_path = Path(path).with_suffix(".tsv")
    if force or tsv_revision(tsv_path) != state["revision"]:
        atomic_write_text(tsv_path, _render_projection(state, now))


def commit_state(path, state, now):
    """Commit one revision, replacing canonical JSON before its TSV view."""
    now = _canonical_now(now)
    candidate = copy.deepcopy(state)
    validate_state(candidate)
    old_revision = candidate["revision"]
    candidate["revision"] = old_revision + 1
    candidate["updated_at"] = now
    for event in candidate["events"]:
        if event["revision"] == old_revision + 1:
            event["revision"] = candidate["revision"]
    validate_state(candidate)
    write_json(path, candidate)
    _repair_tsv(path, candidate, now, force=True)
    return copy.deepcopy(candidate)


def mutate_queue(
    path,
    callback,
    now=None,
    *,
    auto_sweep=True,
    user_input_errors=False,
):
    """Sweep and mutate one detached state under one queue transaction."""
    path = Path(path)
    requested_now = now
    timeout, stale = peek_lock_config(path)
    with QueueLock(path, timeout, stale):
        now = _canonical_now(requested_now)
        source = load_state(path)
        candidate = copy.deepcopy(source)
        if auto_sweep:
            sweep_expired(candidate, now=now)
        try:
            result = callback(candidate)
        except InvariantError as error:
            if user_input_errors:
                raise QueueError(str(error)) from error
            raise
        validate_state(candidate)
        if candidate != source:
            committed = commit_state(path, candidate, now)
            candidate.clear()
            candidate.update(committed)
        else:
            _repair_tsv(path, candidate, now)
        return copy.deepcopy(result)


def read_queue_snapshot(path):
    """Return a consistent detached state without changing its revision."""
    path = Path(path)
    timeout, stale = peek_lock_config(path)
    with QueueLock(path, timeout, stale):
        return copy.deepcopy(load_state(path))


def _status_transaction_details(path, now=None):
    """Return state, clock, and the exact TSV projection produced under lock."""
    path = Path(path)
    requested_now = now
    timeout, stale = peek_lock_config(path)
    with QueueLock(path, timeout, stale):
        now = _canonical_now(requested_now)
        source = load_state(path)
        candidate = copy.deepcopy(source)
        sweep_expired(candidate, now=now)
        if candidate != source:
            candidate = commit_state(path, candidate, now)
            projection = path.with_suffix(".tsv").read_text(encoding="utf-8")
        else:
            projection = _render_projection(candidate, now)
            atomic_write_text(path.with_suffix(".tsv"), projection)
        return copy.deepcopy(candidate), now, projection


def status_transaction(path, now=None):
    """Sweep under lock and always rewrite the canonical TSV projection."""
    state, _now, _projection = _status_transaction_details(path, now)
    return state


def initialize_queue(path, queue_id, config):
    """Race-safely create a new JSON queue and its TSV projection."""
    path = Path(path)
    defaults = fixed_config()
    with QueueLock(
        path,
        defaults["lock_timeout_seconds"],
        defaults["stale_lock_seconds"],
    ):
        if path.exists():
            raise QueueError(f"queue already exists: {path}")
        state = new_state(queue_id, config)
        write_json(path, state)
        _repair_tsv(path, state, state["updated_at"], force=True)
        return copy.deepcopy(state)


def _safe_task(task):
    safe = copy.deepcopy(task)
    if isinstance(safe.get("claim"), dict):
        safe["claim"].pop("lease_token", None)
        binding = safe["claim"].get("git")
        if isinstance(binding, dict):
            binding.pop("common_dir", None)
            binding.pop("worktree", None)
    recovery = safe.get("git_recovery")
    if isinstance(recovery, dict):
        recovery.pop("common_dir", None)
        recovery.pop("worktree", None)
    return safe


def _json_input(path):
    try:
        text = sys.stdin.read() if str(path) == "-" else Path(path).read_text(
            encoding="utf-8"
        )
    except (OSError, UnicodeError) as error:
        raise QueueError(f"cannot read JSON input {path}: {error}") from error
    try:
        return _read_json_text(text, path)
    except InvariantError as error:
        raise QueueError(str(error)) from error


def _emit_json(value):
    try:
        print(json.dumps(value, allow_nan=False, sort_keys=True))
    except (TypeError, ValueError) as error:
        raise InvariantError(f"command result is not finite JSON: {error}") from error


def _user_operation(callback):
    """Map semantic errors in user-supplied operations to usage errors."""
    try:
        return callback()
    except InvariantError as error:
        raise QueueError(str(error)) from error


def _positive(value):
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _port(value):
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "must be an integer from 0 to 65535"
        ) from error
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError(
            "must be an integer from 0 to 65535"
        )
    return number


def _loopback_host(value):
    if value != "127.0.0.1":
        raise argparse.ArgumentTypeError("must be 127.0.0.1")
    return value


def _task_input_from_args(args):
    if args.from_json is not None:
        direct_values = (
            args.title,
            args.description,
            args.role,
            args.workflow,
            args.priority,
            args.depends_on,
            args.resource,
            args.label,
            args.max_attempts,
            args.git_commit,
        )
        if any(value not in (None, []) for value in direct_values):
            raise QueueError("--from-json cannot be combined with task fields")
        raw = _json_input(args.from_json)
        if not isinstance(raw, dict):
            raise QueueError("task JSON input must be an object")
        return raw
    if args.title is None:
        raise QueueError("task add requires --title or --from-json")
    raw = {"title": args.title}
    mappings = {
        "description": args.description,
        "role": args.role,
        "workflow_id": args.workflow,
        "priority": args.priority,
        "depends_on": args.depends_on,
        "resources": args.resource,
        "labels": args.label,
        "max_attempts": args.max_attempts,
        "git_mode": "commit" if args.git_commit else None,
    }
    raw.update({key: value for key, value in mappings.items() if value is not None})
    return raw


def _parallel_workflow_input(path):
    raw = _json_input(path)
    if not isinstance(raw, dict):
        raise QueueError("parallel-shards JSON input must be an object")
    required = {"title", "shards"}
    allowed = required | {"priority", "git_commit"}
    missing = sorted(required.difference(raw))
    unknown = sorted(set(raw).difference(allowed))
    if missing:
        raise QueueError(
            f"parallel-shards JSON input missing keys: {', '.join(missing)}"
        )
    if unknown:
        raise QueueError(
            f"parallel-shards JSON input has unknown keys: {', '.join(unknown)}"
        )
    if "git_commit" in raw and not isinstance(raw["git_commit"], bool):
        raise QueueError("parallel-shards git_commit must be a boolean")
    return raw


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", help="queue.json path")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="initialize a queue")
    init.add_argument("--id", required=True)
    init.add_argument("--lease-seconds", type=_positive)
    init.add_argument("--max-attempts", type=_positive)
    init.add_argument("--retry-backoff", type=_positive)

    migrate = commands.add_parser("migrate", help="migrate queue schema")
    migrate.add_argument("--to", type=int, choices=(2,), required=True)

    task = commands.add_parser("task", help="manage tasks")
    task_commands = task.add_subparsers(dest="task_command", required=True)
    add = task_commands.add_parser("add", help="add one task")
    add.add_argument("--title")
    add.add_argument("--description")
    add.add_argument("--role")
    add.add_argument("--workflow")
    add.add_argument("--priority", type=int)
    add.add_argument("--depends-on", action="append")
    add.add_argument("--resource", action="append")
    add.add_argument("--label", action="append")
    add.add_argument("--max-attempts", type=_positive)
    add.add_argument("--git-commit", action="store_const", const=True)
    add.add_argument("--from-json")
    add_batch_parser = task_commands.add_parser(
        "add-batch", help="add a JSON task batch"
    )
    add_batch_parser.add_argument("--from-json", required=True)
    show = task_commands.add_parser("show", help="show one task")
    show.add_argument("task_id")

    workflow = commands.add_parser("workflow", help="manage workflows")
    workflow_commands = workflow.add_subparsers(
        dest="workflow_command", required=True
    )
    workflow_add = workflow_commands.add_parser(
        "add", help="add a built-in workflow"
    )
    workflow_add.add_argument(
        "--template",
        choices=("adversarial-review", "parallel-shards"),
        required=True,
    )
    workflow_add.add_argument("--title")
    workflow_add.add_argument("--priority", type=int)
    workflow_add.add_argument("--resource", action="append")
    workflow_add.add_argument("--reviewers", type=_positive)
    workflow_add.add_argument("--git-commit", action="store_const", const=True)
    workflow_add.add_argument("--from-json")

    claim = commands.add_parser("claim", help="claim eligible work")
    claim.add_argument("--agent", required=True)
    claim.add_argument("--role")
    claim.add_argument("--label", action="append")
    claim.add_argument("--lease-seconds", type=_positive)
    claim.add_argument("--task")
    claim.add_argument("--resume-git", action="store_true")

    def lease_parser(name, help_text):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--task", required=True)
        command.add_argument("--agent", required=True)
        command.add_argument("--token", required=True)
        return command

    heartbeat = lease_parser("heartbeat", "extend a lease")
    heartbeat.add_argument("--lease-seconds", type=_positive)
    complete = lease_parser("complete", "complete a task")
    complete.add_argument("--summary", required=True)
    complete.add_argument("--artifact", action="append", default=[])
    fail = lease_parser("fail", "fail a task")
    fail.add_argument("--error", required=True)
    fail.add_argument("--terminal", action="store_true")
    lease_parser("release", "release a task")

    retry = commands.add_parser("retry", help="retry a failed task")
    retry.add_argument("task_id")
    retry.add_argument("--additional-attempts", type=_positive, default=1)
    block = commands.add_parser("block", help="block a task")
    block.add_argument("task_id")
    block.add_argument("--reason", required=True)
    for name in ("unblock", "cancel"):
        command = commands.add_parser(name, help=f"{name} a task")
        command.add_argument("task_id")

    status = commands.add_parser("status", help="show queue status")
    status.add_argument("--format", choices=("table", "json", "tsv"), default="table")
    status.add_argument("--workflow")
    status.add_argument("--assignee")
    status.add_argument("--role")
    status.add_argument("--label", action="append")
    status.add_argument("--state", choices=STATUS_FILTER_STATES)
    events = commands.add_parser("events", help="show sanitized events")
    events.add_argument("--task")
    commands.add_parser("sweep", help="sweep expired leases")
    export = commands.add_parser("export", help="export queue projection")
    export.add_argument("--format", choices=("tsv",), required=True)
    doctor_parser = commands.add_parser("doctor", help="diagnose queue artifacts")
    doctor_parser.add_argument("--repair", action="store_true")
    compact = commands.add_parser("compact", help="compact closed queue history")
    compact.add_argument("--before", required=True)
    serve = commands.add_parser(
        "serve",
        help="show the live workflow dashboard in a local browser",
        description=(
            "Serve the read-only live workflow dashboard on 127.0.0.1. "
            "Run in the foreground until Ctrl-C or the idle timeout."
        ),
    )
    serve.add_argument(
        "--open",
        dest="open_browser",
        action="store_true",
    )
    serve.add_argument(
        "--host",
        type=_loopback_host,
        default="127.0.0.1",
        help="loopback host; only 127.0.0.1 is accepted",
    )
    serve.add_argument(
        "--port",
        type=_port,
        default=0,
        help="port from 0 to 65535; 0 selects an available port",
    )
    serve.add_argument(
        "--interval",
        type=_positive,
        default=2,
        help="polling interval in seconds",
    )
    serve.add_argument(
        "--idle-timeout",
        type=_positive,
        default=300,
        help="exit after this many seconds without a request",
    )
    return parser


def dashboard_loaders(path):
    """Build queue-backed callbacks for the read-only dashboard."""
    import queue_dashboard as dashboard

    path = Path(path)

    def available(callback):
        try:
            return callback()
        except QueueError as error:
            raise dashboard.DashboardDataUnavailable() from error

    def current():
        def load():
            state, now, _projection = _status_transaction_details(path)
            rows = status_rows(state, now)
            return state, now, rows

        return available(load)

    def revision():
        state, _now, _rows = current()
        return state["revision"]

    def snapshot():
        state, now, rows = current()
        return dashboard.build_snapshot(
            state["queue_id"],
            state["revision"],
            rows,
            now,
        )

    def events(after):
        def load():
            state = read_queue_snapshot(path)
            return dashboard.events_after(state["events"], after)

        return available(load)

    return SimpleNamespace(
        revision=revision,
        snapshot=snapshot,
        events=events,
    )


def _run_command(args, path):
    def user_mutation(callback):
        return mutate_queue(path, callback, user_input_errors=True)

    if args.command == "init":
        config = fixed_config()
        if args.lease_seconds is not None:
            config["default_lease_seconds"] = args.lease_seconds
        if args.max_attempts is not None:
            config["default_max_attempts"] = args.max_attempts
        if args.retry_backoff is not None:
            config["retry_backoff_seconds"] = args.retry_backoff
        state = _user_operation(
            lambda: initialize_queue(path, args.id, config)
        )
        return {
            "ok": True,
            "queue_id": state["queue_id"],
            "revision": 0,
            "next_actions": ["serve --open", "status"],
        }

    if args.command == "migrate":
        summary = mutate_queue(
            path,
            lambda state: migrate_state(state, args.to),
            auto_sweep=False,
            user_input_errors=True,
        )
        return {"ok": True, **summary}

    if args.command == "doctor":
        return doctor(path, repair=args.repair)

    if args.command == "compact":
        before = _user_operation(
            lambda: parse_compaction_cutoff(args.before)
        )
        summary = mutate_queue(
            path,
            lambda state: compact_state(state, before),
            auto_sweep=False,
            user_input_errors=True,
        )
        return {"ok": True, **summary}

    if args.command == "task" and args.task_command == "show":
        state = read_queue_snapshot(path)
        task = _user_operation(lambda: _require_task(state, args.task_id))
        return {"ok": True, "task": _safe_task(task), "revision": state["revision"]}

    if args.command == "events":
        state = read_queue_snapshot(path)
        if args.task is not None:
            _user_operation(lambda: _require_task(state, args.task))
        events = [event for event in state["events"]
                  if args.task is None or event["task_id"] == args.task]
        return {"ok": True, "queue_id": state["queue_id"],
                "revision": state["revision"], "events": copy.deepcopy(events)}

    if args.command == "task" and args.task_command == "add":
        raw = _task_input_from_args(args)

        def add_one(state):
            task = add_task(state, raw)
            append_event(state, "task.added", "operator", task["id"], {}, utc_now())
            return _safe_task(task)

        task_result = user_mutation(add_one)
        return {"ok": True, "task": task_result}

    if args.command == "task" and args.task_command == "add-batch":
        raw_tasks = _json_input(args.from_json)
        if not isinstance(raw_tasks, list):
            raise QueueError("task batch JSON input must be an array")

        def add_many(state):
            tasks = add_task_batch(state, raw_tasks)
            event_now = utc_now()
            for task in tasks:
                append_event(state, "task.added", "operator", task["id"], {}, event_now)
            return [_safe_task(task) for task in tasks]

        tasks = user_mutation(add_many)
        return {"ok": True, "tasks": tasks}

    if args.command == "workflow" and args.workflow_command == "add":
        if args.template == "adversarial-review":
            if args.from_json is not None:
                raise QueueError(
                    "--from-json cannot be used with adversarial-review"
                )
            if args.title is None:
                raise QueueError("adversarial-review requires --title")
            workflow_result = user_mutation(
                lambda state: add_adversarial_review(
                    state,
                    args.title,
                    0 if args.priority is None else args.priority,
                    [] if args.resource is None else args.resource,
                    2 if args.reviewers is None else args.reviewers,
                    git_commit=bool(args.git_commit),
                    now=utc_now(),
                )
            )
        else:
            if args.from_json is None:
                raise QueueError("parallel-shards requires --from-json")
            if any(value is not None for value in (
                args.title,
                args.priority,
                args.resource,
                args.reviewers,
                args.git_commit,
            )):
                raise QueueError(
                    "parallel-shards --from-json cannot be combined with "
                    "template fields"
                )
            raw = _parallel_workflow_input(args.from_json)
            workflow_result = user_mutation(
                lambda state: add_parallel_shards(
                    state,
                    raw["title"],
                    raw.get("priority", 0),
                    raw["shards"],
                    git_commit=raw.get("git_commit", False),
                    now=utc_now(),
                )
            )
        return {"ok": True, **workflow_result}

    if args.command == "claim":
        if args.resume_git and args.task is None:
            raise QueueError("--resume-git requires --task")

        git_observation = None
        git_observation_error = None
        try:
            git_observation = gq.observe(Path.cwd())
        except gq.GitContextError as error:
            git_observation_error = error

        if args.task is not None:
            snapshot = read_queue_snapshot(path)
            target = _user_operation(
                lambda: _require_task(snapshot, args.task)
            )
            if target.get("git_mode") == "commit":
                if git_observation is None:
                    raise QueueError(
                        f"{git_observation_error.code}: "
                        f"{git_observation_error}"
                    )
                try:
                    gq.require_claimable(git_observation)
                except gq.GitContextError as error:
                    raise QueueError(
                        f"{error.code}: {error}"
                    ) from error

        claim_result = user_mutation(
            lambda state: claim_task(
                state, args.agent, role=args.role, labels=args.label,
                lease_seconds=args.lease_seconds,
                git_observation=git_observation,
                task_id=args.task,
                resume_git=args.resume_git,
            ),
        )
        binding = claim_result["task"]["claim"].get("git")
        if isinstance(binding, dict):
            try:
                current_observation = gq.observe(Path.cwd())
                gq.assert_claim_snapshot(binding, current_observation)
            except gq.GitContextError as error:
                try:
                    user_mutation(
                        lambda state: release_task(
                            state,
                            claim_result["task"]["id"],
                            args.agent,
                            claim_result["lease_token"],
                        )
                    )
                except QueueError as rollback_error:
                    raise QueueError(
                        f"{error.code}: {error}; "
                        f"claim rollback failed: {rollback_error}"
                    ) from rollback_error
                raise QueueError(f"{error.code}: {error}") from error
        return {
            "ok": True,
            "task": _safe_task(claim_result["task"]),
            "lease_token": claim_result["lease_token"],
            "expires_at": claim_result["expires_at"],
        }

    if args.command == "heartbeat":
        task_result = user_mutation(
            lambda state: heartbeat_task(
                state, args.task, args.agent, args.token,
                lease_seconds=args.lease_seconds,
            )
        )
    elif args.command == "complete":
        task_result = user_mutation(
            lambda state: complete_task(
                state, args.task, args.agent, args.token,
                args.summary, args.artifact,
            )
        )
    elif args.command == "fail":
        task_result = user_mutation(
            lambda state: fail_task(
                state, args.task, args.agent, args.token,
                args.error, terminal=args.terminal,
            )
        )
    elif args.command == "release":
        task_result = user_mutation(
            lambda state: release_task(
                state, args.task, args.agent, args.token
            )
        )
    elif args.command == "retry":
        task_result = user_mutation(
            lambda state: retry_task(
                state, args.task_id, args.additional_attempts
            )
        )
    elif args.command == "block":
        task_result = user_mutation(
            lambda state: block_task(state, args.task_id, args.reason)
        )
    elif args.command == "unblock":
        task_result = user_mutation(
            lambda state: unblock_task(state, args.task_id)
        )
    elif args.command == "cancel":
        task_result = user_mutation(
            lambda state: cancel_task(state, args.task_id)
        )
    else:
        task_result = None
    if task_result is not None:
        return {"ok": True, "task": _safe_task(task_result)}

    if args.command == "sweep":
        changed = mutate_queue(
            path, lambda state: sweep_expired(state), auto_sweep=False
        )
        return {"ok": True, "swept": changed}

    if args.command in {"status", "export"}:
        state, now, projection = _status_transaction_details(path)
        if args.command == "export":
            return projection
        rows = status_rows(
            state, now, workflow=args.workflow, assignee=args.assignee,
            role=args.role, labels=args.label, state_filter=args.state,
        )
        if args.format == "json":
            return {"queue_id": state["queue_id"],
                    "revision": state["revision"], "rows": rows}
        if args.format == "tsv":
            return render_tsv(
                state, now, workflow=args.workflow, assignee=args.assignee,
                role=args.role, labels=args.label, state_filter=args.state,
            )
        return format_terminal_table(rows) + "\n"
    raise QueueError("unsupported command")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        path = resolve_queue_path(args.queue)
        if args.command == "serve":
            import queue_dashboard as dashboard

            loaders = dashboard_loaders(path)
            try:
                return dashboard.serve(
                    args.host,
                    args.port,
                    args.interval,
                    args.idle_timeout,
                    args.open_browser,
                    loaders.revision,
                    loaders.snapshot,
                    loaders.events,
                    Path(__file__).with_name("dashboard"),
                    sys.stdout,
                )
            except OSError as error:
                raise QueueError(
                    f"cannot start dashboard: {error}"
                ) from error
        result = _run_command(args, path)
        if isinstance(result, str):
            sys.stdout.write(result)
            if (
                args.command == "status"
                and args.format == "table"
                and sys.stdout.isatty()
            ):
                sys.stdout.write(
                    "Live dashboard: agent_queue.py serve --open\n"
                )
        else:
            _emit_json(result)
        if args.command == "doctor" and not result["ok"]:
            if any(
                issue["code"] == "lock.timeout"
                for issue in result["issues"]
            ):
                return LockTimeout.exit_code
            return InvariantError.exit_code
        return 0
    except QueueError as error:
        print(f"error: {error}", file=sys.stderr)
        return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
