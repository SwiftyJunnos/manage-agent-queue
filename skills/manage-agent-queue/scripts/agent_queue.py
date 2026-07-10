#!/usr/bin/env python3
"""Manage a shared local queue for cooperating agents."""

import copy
import json
import os
import re
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 1
MAX_ID_SEQUENCE = 999_999
MAX_JSON_METADATA_DEPTH = 64
STORED_STATUSES = {
    "pending",
    "leased",
    "completed",
    "failed",
    "blocked",
    "cancelled",
}
TASK_ID_PATTERN = re.compile(r"T-(\d{6})", flags=re.ASCII)
WORKFLOW_ID_PATTERN = re.compile(r"W-(\d{6})", flags=re.ASCII)
TASK_FIELDS = {
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
}
TASK_CREATION_FIELDS = {
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


def _deduplicated_string_list(raw, field):
    if not isinstance(raw, list):
        raise InvariantError(f"{field} must be a list")
    if any(not isinstance(value, str) for value in raw):
        raise InvariantError(f"{field} values must be strings")
    return list(dict.fromkeys(raw))


def normalize_task(state, raw):
    """Create a canonical pending task from generic task input."""
    if not isinstance(raw, dict):
        raise InvariantError("task must be an object")
    if any(not isinstance(field, str) for field in raw):
        raise InvariantError("task creation input must use string field names")
    unknown_fields = sorted(set(raw).difference(TASK_CREATION_FIELDS))
    if unknown_fields:
        raise InvariantError(
            f"unknown task creation fields: {', '.join(unknown_fields)}"
        )

    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        raise InvariantError("task title must be a non-blank string")
    description = raw.get("description", "")
    if not isinstance(description, str):
        raise InvariantError("task description must be a string")

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
    validate_task(task, expected_id=task_id)
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


def validate_task(task, expected_id=None):
    """Validate one stored task's exact canonical schema."""
    if not isinstance(task, dict):
        raise InvariantError("task must be an object")
    if any(not isinstance(field, str) for field in task):
        raise InvariantError("task field names must be strings")
    missing_fields = sorted(TASK_FIELDS.difference(task))
    extra_fields = sorted(set(task).difference(TASK_FIELDS))
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
    if not isinstance(task["description"], str):
        raise InvariantError("task description must be a string")
    if (
        not isinstance(task["status"], str)
        or task["status"] not in STORED_STATUSES
    ):
        raise InvariantError("task status must be a stored status")
    if not isinstance(task["priority"], int) or isinstance(task["priority"], bool):
        raise InvariantError("task priority must be an integer")

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
    if claim is not None and "agent_id" in claim:
        agent_id = claim["agent_id"]
        if not isinstance(agent_id, str) or not agent_id.strip():
            raise InvariantError("task claim.agent_id must be a non-blank string")
    if claim is not None and "expires_at" in claim:
        _validate_timestamp(claim["expires_at"], "task claim.expires_at")
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


def add_task_batch(state, raw_tasks):
    """Validate and atomically append a nonempty batch of generic tasks."""
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise InvariantError("raw_tasks must be a nonempty list")
    candidate = copy.deepcopy(state)
    validate_state(candidate)
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
    validate_state(candidate)
    state.clear()
    state.update(candidate)
    return [copy.deepcopy(state["tasks"][task_id]) for task_id in created_ids]


def add_task(state, raw):
    """Validate and atomically append one generic task."""
    return add_task_batch(state, [raw])[0]


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
        validate_task(task, expected_id=task_id)
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


def _derive_state_with_resources(state, task, now, active_resources):
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
    if _resource_conflicts(task, active_resources):
        return "resource_conflict"
    return "ready"


def derive_state(state, task, now):
    """Derive the display state for a task using fixed readiness precedence."""
    active_resources = leased_resources(state, now=now)
    return _derive_state_with_resources(state, task, now, active_resources)


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


def initialize_queue(path, queue_id, config):
    """Create a new JSON queue and its empty TSV projection."""
    path = Path(path)
    if path.exists():
        raise QueueError(f"queue already exists: {path}")

    state = new_state(queue_id, config)
    write_json(path, state)
    atomic_write_text(path.with_suffix(".tsv"), render_empty_tsv(state["revision"]))
    return state
