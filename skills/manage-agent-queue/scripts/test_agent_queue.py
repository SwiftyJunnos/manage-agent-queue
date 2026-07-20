#!/usr/bin/env python3
"""Tests for the manage-agent-queue CLI."""

import copy
import http.client
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import agent_queue as aq
import queue_dashboard as qd


SCRIPT_PATH = SCRIPT_DIR / "agent_queue.py"


class SkillContractTests(unittest.TestCase):
    def setUp(self):
        self.skill_dir = SCRIPT_DIR.parent

    def test_skill_frontmatter_and_routes_match_the_public_contract(self):
        text = (self.skill_dir / "SKILL.md").read_text(encoding="utf-8")
        frontmatter = text.split("---", 2)[1].strip().splitlines()

        self.assertEqual(
            [
                "name: manage-agent-queue",
                (
                    "description: Use when coordinating multiple agents that need "
                    "shared task claiming, dependency ordering, worktree-safe "
                    "ownership, progress visibility, or recovery after an agent stops."
                ),
            ],
            frontmatter,
        )
        for required in (
            "scripts/agent_queue.py",
            "references/queue-schema.md",
            "references/workflow-templates.md",
            "claim",
            "heartbeat",
            "complete",
            "fail",
            "one JSON source of truth",
            "generated TSV",
            "stable agent ID",
            "claim filters",
            "heartbeat expectation",
            "scoped task instructions",
            "Refuse work outside",
            "Never place secrets in descriptions, summaries, events, or TSV-visible fields",
        ):
            self.assertIn(required, text)
        self.assertLess(len(text.splitlines()), 500)

    def test_schema_reference_names_every_public_state_command_and_column(self):
        schema = (
            self.skill_dir / "references" / "queue-schema.md"
        ).read_text(encoding="utf-8")

        for status in sorted(aq.STORED_STATUSES | aq.DERIVED_STATUSES):
            self.assertIn(f"`{status}`", schema)
        for command in (
            "init",
            "task add",
            "task add-batch",
            "task show",
            "workflow add",
            "claim",
            "heartbeat",
            "complete",
            "fail",
            "release",
            "retry",
            "block",
            "unblock",
            "cancel",
            "status",
            "events",
            "sweep",
            "export",
            "doctor",
            "compact",
        ):
            self.assertIn(f"`{command}`", schema)
        for column in aq.TSV_COLUMNS:
            self.assertIn(f"`{column}`", schema)
        for exit_code in range(7):
            if exit_code != 1:
                self.assertIn(f"`{exit_code}`", schema)
        self.assertIn(
            "Deduplicate `depends_on`, `resources`, and `labels` while preserving "
            "first occurrence",
            schema,
        )

    def test_workflow_reference_names_templates_roles_and_cli_entrypoint(self):
        templates = (
            self.skill_dir / "references" / "workflow-templates.md"
        ).read_text(encoding="utf-8")

        for template in ("adversarial-review", "parallel-shards"):
            self.assertIn(f"`{template}`", templates)
        for role in ("implement", "review", "apply", "verify", "shard", "integrate"):
            self.assertIn(f"`{role}`", templates)
        self.assertIn("scripts/agent_queue.py", templates)
        self.assertIn("--from-json", templates)

    def test_git_aware_contract_is_documented_without_path_list_storage(self):
        skill = (self.skill_dir / "SKILL.md").read_text(encoding="utf-8")
        schema = (
            self.skill_dir / "references" / "queue-schema.md"
        ).read_text(encoding="utf-8")
        templates = (
            self.skill_dir / "references" / "workflow-templates.md"
        ).read_text(encoding="utf-8")
        readme = (self.skill_dir.parent.parent / "README.md").read_text(
            encoding="utf-8"
        )

        for required in (
            "--git-commit",
            "--commit",
            "--no-change",
            "--resume-git",
            "migrate --to 2",
            "git_recovery",
            "file:",
            "dir:",
            "changed_path_count",
        ):
            with self.subTest(required=required):
                self.assertIn(required, schema)
        for required in (
            "--git-commit",
            "--commit",
            "--no-change",
            "--resume-git",
            "file:",
            "dir:",
        ):
            self.assertIn(required, skill)
        self.assertNotIn('"changed_paths"', schema)
        self.assertNotIn('"changed_paths"', templates)
        self.assertIn("writer roles only", templates)
        self.assertIn(
            "does not create worktrees, commit, merge, reset, or push",
            templates,
        )
        self.assertIn("git_queue.py", readme)
        for command in (
            "$CLI migrate --to 2",
            "$CLI task add",
            "$CLI claim",
            "$CLI complete",
            "$CLI serve --open",
        ):
            self.assertIn(command, readme)

    def test_dashboard_requires_consent_fallback_and_cleanup(self):
        skill = (self.skill_dir / "SKILL.md").read_text(encoding="utf-8")
        readme = (self.skill_dir.parent.parent / "README.md").read_text(
            encoding="utf-8"
        )
        schema = (
            self.skill_dir / "references" / "queue-schema.md"
        ).read_text(encoding="utf-8")
        for required in (
            "실시간 큐 진행 상황을 브라우저에서 볼까요?",
            "serve --open",
            "ask once",
            "status",
            "events",
            "stop",
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


def run_cli(*arguments, cwd=None, env=None, timeout=10):
    command = [sys.executable, str(SCRIPT_PATH), *map(str, arguments)]
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def communicate_all(processes, timeout):
    """Bound a process group and always terminate/reap it on failure."""
    deadline = time.monotonic() + timeout
    outputs = []
    try:
        for process in processes:
            outputs.append(
                process.communicate(timeout=max(0.01, deadline - time.monotonic()))
            )
        return outputs
    except BaseException:
        for process in processes:
            if process.poll() is None:
                process.kill()
        for process in processes:
            process.communicate()
        raise


def request_server(server, path, host=None):
    connection = http.client.HTTPConnection(
        "127.0.0.1",
        server.server_port,
        timeout=2,
    )
    connection.request(
        "GET",
        path,
        headers={
            "Host": host or f"127.0.0.1:{server.server_port}"
        },
    )
    response = connection.getresponse()
    body = response.read()
    headers = dict(response.getheaders())
    connection.close()
    return response.status, headers, body


def canonical_claim(
    agent_id="agent-1",
    lease_token="test-lease-token",
    claimed_at="2026-07-10T05:00:00Z",
    heartbeat_at="2026-07-10T05:00:00Z",
    expires_at="2026-07-10T07:00:00Z",
    git=None,
):
    return {
        "agent_id": agent_id,
        "lease_token": lease_token,
        "claimed_at": claimed_at,
        "heartbeat_at": heartbeat_at,
        "expires_at": expires_at,
        "git": git,
    }


def canonical_result(summary="done", artifacts=None, git=None):
    return {
        "summary": summary,
        "artifacts": [] if artifacts is None else artifacts,
        "git": git,
    }


def git_snapshot(
    repository_id="repo-1",
    worktree_id="wt-1",
    branch="refs/heads/feature-a",
    head="base-a",
    clean=True,
    attached=True,
):
    return {
        "common_dir": "/private/repository/.git",
        "worktree": f"/private/worktrees/{worktree_id}",
        "repository_id": repository_id,
        "worktree_id": worktree_id,
        "branch": branch if attached else None,
        "head": head,
        "attached": attached,
        "clean": clean,
    }


class QueueStateTests(unittest.TestCase):
    def test_new_state_has_versioned_monotonic_counters(self):
        state = aq.new_state("demo", aq.fixed_config())

        self.assertEqual(2, state["schema_version"])
        self.assertEqual("demo", state["queue_id"])
        self.assertEqual(0, state["revision"])
        self.assertEqual(1, state["next_task_sequence"])
        self.assertEqual(1, state["next_workflow_sequence"])
        self.assertEqual(1, state["next_event_sequence"])
        self.assertEqual({}, state["tasks"])
        self.assertEqual([], state["events"])

    def test_validate_state_rejects_unknown_schema(self):
        state = aq.new_state("demo", aq.fixed_config())

        for unknown_schema in (3, True):
            with self.subTest(schema_version=unknown_schema):
                state["schema_version"] = unknown_schema
                with self.assertRaisesRegex(aq.InvariantError, "schema_version"):
                    aq.validate_state(state)

    def test_validate_state_rejects_unknown_top_level_field(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["future_field"] = "not supported by schema version 2"

        with self.assertRaisesRegex(aq.InvariantError, "state.*future_field"):
            aq.validate_state(state)

    def test_validate_state_rejects_non_string_timestamp(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["created_at"] = None

        with self.assertRaisesRegex(aq.InvariantError, "created_at"):
            aq.validate_state(state)

    def test_validate_state_rejects_non_utc_second_precision_timestamp(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["updated_at"] = "2026-07-10T00:00:00.123+00:00"

        with self.assertRaisesRegex(aq.InvariantError, "updated_at"):
            aq.validate_state(state)

    def test_validate_state_rejects_single_digit_timestamp_component(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["created_at"] = "2026-7-10T00:00:00Z"

        with self.assertRaisesRegex(aq.InvariantError, "created_at"):
            aq.validate_state(state)

    def test_validate_state_rejects_lowercase_z_timestamp(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["updated_at"] = "2026-07-10T00:00:00z"

        with self.assertRaisesRegex(aq.InvariantError, "updated_at"):
            aq.validate_state(state)

    def test_validate_state_rejects_extra_config_key(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["config"]["unexpected"] = 1

        with self.assertRaisesRegex(aq.InvariantError, "config.*unexpected"):
            aq.validate_state(state)

    def test_validate_state_rejects_non_integer_config_value(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["config"]["default_max_attempts"] = 1.5

        with self.assertRaisesRegex(
            aq.InvariantError, "config.default_max_attempts"
        ):
            aq.validate_state(state)

    def test_write_json_rejects_nested_nan_without_creating_temp_file(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            state = aq.new_state("demo", aq.fixed_config())
            state["events"].append({"score": float("nan")})

            with self.assertRaisesRegex(aq.InvariantError, "events"):
                aq.write_json(queue_path, state)

            self.assertEqual([], list(Path(directory).iterdir()))

    def test_render_empty_tsv_has_exact_bytes(self):
        expected = (
            b"# queue_revision: 7\n"
            b"id\tworkflow\trole\tstate\tpriority\tassignee\tlease_until\t"
            b"attempts\tdepends_on\tblocked_by\tresources\ttitle\n"
        )

        self.assertEqual(expected, aq.render_empty_tsv(7).encode("utf-8"))

    def test_write_json_has_deterministic_sorted_bytes_and_trailing_newline(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            state = aq.new_state("demo", aq.fixed_config())
            expected = (
                json.dumps(state, allow_nan=False, indent=2, sort_keys=True) + "\n"
            ).encode("utf-8")

            aq.write_json(queue_path, state)

            self.assertEqual(expected, queue_path.read_bytes())

    def test_initialize_rejects_existing_queue_without_changing_it(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"
            queue_path.write_bytes(b"existing queue\n")

            with self.assertRaisesRegex(aq.QueueError, "already exists"):
                aq.initialize_queue(queue_path, "demo", aq.fixed_config())

            self.assertEqual(b"existing queue\n", queue_path.read_bytes())
            self.assertFalse(queue_path.with_suffix(".tsv").exists())

    def test_atomic_write_replace_failure_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"

            with mock.patch.object(
                aq.os, "replace", side_effect=OSError("replace failed")
            ):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    aq.atomic_write_text(queue_path, "queue contents\n")

            self.assertEqual([], list(Path(directory).iterdir()))

    def test_initialize_writes_json_and_revision_matched_tsv(self):
        with tempfile.TemporaryDirectory() as directory:
            queue_path = Path(directory) / "queue.json"

            state = aq.initialize_queue(queue_path, "demo", aq.fixed_config())

            self.assertEqual(state, json.loads(queue_path.read_text(encoding="utf-8")))
            self.assertTrue(
                queue_path.with_suffix(".tsv")
                .read_text(encoding="utf-8")
                .startswith("# queue_revision: 0\n")
            )


class TaskGraphTests(unittest.TestCase):
    def setUp(self):
        self.state = aq.new_state("demo", aq.fixed_config())

    def test_add_task_assigns_canonical_defaults_and_first_id(self):
        with mock.patch.object(aq, "utc_now", return_value="2026-07-10T06:00:00Z"):
            task = aq.add_task(self.state, {"title": "First"})

        self.assertEqual(
            {
                "id": "T-000001",
                "workflow_id": None,
                "role": None,
                "title": "First",
                "description": "",
                "status": "pending",
                "priority": 0,
                "depends_on": [],
                "resources": [],
                "labels": [],
                "attempts": 0,
                "max_attempts": 3,
                "available_at": None,
                "claim": None,
                "result": None,
                "last_error": None,
                "git_mode": None,
                "git_recovery": None,
                "created_at": "2026-07-10T06:00:00Z",
                "updated_at": "2026-07-10T06:00:00Z",
            },
            task,
        )
        self.assertIsNot(task, self.state["tasks"]["T-000001"])
        self.assertEqual(task, self.state["tasks"]["T-000001"])
        self.assertEqual(2, self.state["next_task_sequence"])

    def test_git_task_requires_version_two_and_typed_path_resource(self):
        with self.assertRaisesRegex(aq.InvariantError, "file: or dir:"):
            aq.add_task(
                self.state,
                {
                    "title": "Git task",
                    "git_mode": "commit",
                    "resources": ["scope:auth"],
                },
            )

        valid = aq.add_task(
            self.state,
            {
                "title": "Scoped Git task",
                "git_mode": "commit",
                "resources": ["file:src/api.py", "dir:tests/api/"],
            },
        )
        self.assertEqual("commit", valid["git_mode"])

        legacy = aq.new_state("legacy", aq.fixed_config())
        legacy["schema_version"] = 1
        with self.assertRaisesRegex(aq.InvariantError, "Git-aware|unknown"):
            aq.add_task(
                legacy,
                {
                    "title": "Legacy Git task",
                    "git_mode": "commit",
                    "resources": ["file:src/api.py"],
                },
            )

    def test_second_task_has_monotonic_id_timestamp_priority_and_dependency(self):
        with mock.patch.object(
            aq,
            "utc_now",
            side_effect=[
                "2026-07-10T06:00:00Z",
                "2026-07-10T06:00:01Z",
            ],
        ):
            first = aq.add_task(self.state, {"title": "First"})
            second = aq.add_task(
                self.state,
                {"title": "Second", "priority": 7, "depends_on": [first["id"]]},
            )

        self.assertEqual("T-000002", second["id"])
        self.assertEqual(7, second["priority"])
        self.assertEqual(["T-000001"], second["depends_on"])
        self.assertLessEqual(first["created_at"], second["created_at"])

    def test_explicit_id_advances_sequence_and_generated_id_does_not_collide(self):
        explicit = aq.add_task(
            self.state,
            {"id": "T-000010", "title": "Explicit"},
        )
        generated = aq.add_task(self.state, {"title": "Generated"})

        self.assertEqual("T-000010", explicit["id"])
        self.assertEqual("T-000011", generated["id"])
        self.assertEqual(12, self.state["next_task_sequence"])

    def test_task_id_zero_is_rejected_without_mutating_state(self):
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "T-000001.*T-999999"):
            aq.add_task(self.state, {"id": "T-000000", "title": "Zero"})

        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_task_allocation_rejects_exhausted_sequence_atomically(self):
        maximum = aq.add_task(
            self.state,
            {"id": "T-999999", "title": "Last task ID"},
        )
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "task id sequence exhausted"):
            aq.add_task(self.state, {"title": "No ID remains"})

        self.assertEqual("T-999999", maximum["id"])
        self.assertEqual(1_000_000, self.state["next_task_sequence"])
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_workflow_allocation_rejects_exhausted_sequence(self):
        self.state["next_workflow_sequence"] = 999_999

        self.assertEqual("W-999999", aq.allocate_id(self.state, "workflow"))
        with self.assertRaisesRegex(
            aq.InvariantError, "workflow id sequence exhausted"
        ):
            aq.allocate_id(self.state, "workflow")

    def test_validate_state_requires_counter_beyond_stored_task_id(self):
        aq.add_task(self.state, {"id": "T-000010", "title": "Stored"})
        self.state["next_task_sequence"] = 10

        with self.assertRaisesRegex(
            aq.InvariantError, "next_task_sequence.*T-000010"
        ):
            aq.validate_state(self.state)

    def test_workflow_id_is_bounded_and_advances_workflow_counter(self):
        for workflow_id in ("W-000000", "W-1", "W-1000000"):
            with self.subTest(workflow_id=workflow_id):
                with self.assertRaisesRegex(
                    aq.InvariantError, "workflow_id.*W-000001.*W-999999"
                ):
                    aq.add_task(
                        self.state,
                        {"title": "Invalid workflow", "workflow_id": workflow_id},
                    )

        task = aq.add_task(
            self.state,
            {"title": "Workflow task", "workflow_id": "W-000005"},
        )
        self.assertEqual("W-000005", task["workflow_id"])
        self.assertEqual(6, self.state["next_workflow_sequence"])
        self.state["next_workflow_sequence"] = 5
        with self.assertRaisesRegex(
            aq.InvariantError, "next_workflow_sequence.*W-000005"
        ):
            aq.validate_state(self.state)

    def test_deleting_task_does_not_reuse_its_sequence(self):
        task = aq.add_task(
            self.state,
            {"id": "T-000010", "title": "Temporary"},
        )
        del self.state["tasks"][task["id"]]
        aq.validate_state(self.state)

        replacement = aq.add_task(self.state, {"title": "Replacement"})

        self.assertEqual("T-000011", replacement["id"])

    def test_deleted_explicit_task_id_cannot_be_reused(self):
        created = aq.add_task(
            self.state,
            {"id": "T-000010", "title": "Historical"},
        )
        del self.state["tasks"][created["id"]]
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "historical task id"):
            aq.add_task(
                self.state,
                {"id": "T-000010", "title": "Reused"},
            )

        self.assertEqual(11, self.state["next_task_sequence"])
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_deleted_workflow_id_cannot_be_reused(self):
        created = aq.add_task(
            self.state,
            {"title": "Historical", "workflow_id": "W-000010"},
        )
        del self.state["tasks"][created["id"]]
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "historical workflow id"):
            aq.add_task(
                self.state,
                {"title": "Reused", "workflow_id": "W-000010"},
            )

        self.assertEqual(11, self.state["next_workflow_sequence"])
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_batch_allows_out_of_order_new_explicit_task_ids(self):
        created = aq.add_task_batch(
            self.state,
            [
                {"id": "T-000010", "title": "Higher first"},
                {"id": "T-000005", "title": "Lower second"},
            ],
        )

        self.assertEqual(["T-000010", "T-000005"], [task["id"] for task in created])
        self.assertEqual(11, self.state["next_task_sequence"])

    def test_batch_allocates_generated_id_before_explicit_max_in_both_orders(self):
        orders = (
            [
                {"title": "Generated"},
                {"id": "T-999999", "title": "Explicit maximum"},
            ],
            [
                {"id": "T-999999", "title": "Explicit maximum"},
                {"title": "Generated"},
            ],
        )

        for raw_tasks in orders:
            with self.subTest(order=[task["title"] for task in raw_tasks]):
                state = aq.new_state("demo", aq.fixed_config())

                created = aq.add_task_batch(state, raw_tasks)
                created_by_title = {task["title"]: task for task in created}

                self.assertEqual("T-000001", created_by_title["Generated"]["id"])
                self.assertEqual(
                    "T-999999", created_by_title["Explicit maximum"]["id"]
                )
                self.assertEqual(1_000_000, state["next_task_sequence"])
                with self.assertRaisesRegex(
                    aq.InvariantError, "task id sequence exhausted"
                ):
                    aq.add_task(state, {"title": "No ID remains"})

    def test_batch_allows_multiple_tasks_in_one_new_workflow(self):
        created = aq.add_task_batch(
            self.state,
            [
                {"title": "First", "workflow_id": "W-000010"},
                {"title": "Second", "workflow_id": "W-000010"},
            ],
        )

        self.assertEqual(
            ["W-000010", "W-000010"],
            [task["workflow_id"] for task in created],
        )
        self.assertEqual(11, self.state["next_workflow_sequence"])

    def test_task_can_join_workflow_still_present_in_state(self):
        existing = aq.add_task(
            self.state,
            {"title": "Existing", "workflow_id": "W-000010"},
        )

        joined = aq.add_task(
            self.state,
            {"title": "Joined", "workflow_id": "W-000010"},
        )

        self.assertEqual("W-000010", existing["workflow_id"])
        self.assertEqual("W-000010", joined["workflow_id"])
        self.assertEqual(11, self.state["next_workflow_sequence"])

    def test_explicit_paths_reject_invalid_counter_types_as_invariants(self):
        cases = (
            (
                "next_task_sequence",
                {"id": "T-000010", "title": "Task"},
            ),
            (
                "next_workflow_sequence",
                {"title": "Workflow", "workflow_id": "W-000010"},
            ),
        )

        for field, raw in cases:
            with self.subTest(field=field):
                state = aq.new_state("demo", aq.fixed_config())
                state[field] = "invalid"
                with self.assertRaisesRegex(aq.InvariantError, field):
                    aq.add_task(state, raw)

    def test_batch_can_reference_explicit_ids_in_same_batch(self):
        created = aq.add_task_batch(
            self.state,
            [
                {
                    "id": "T-000021",
                    "title": "Dependent",
                    "depends_on": ["T-000020"],
                },
                {"id": "T-000020", "title": "Dependency"},
            ],
        )

        self.assertEqual(["T-000021", "T-000020"], [task["id"] for task in created])
        self.assertIsNot(created[0], self.state["tasks"]["T-000021"])
        self.assertEqual(created[0], self.state["tasks"]["T-000021"])
        self.assertEqual(22, self.state["next_task_sequence"])

    def test_created_task_returns_are_detached_from_committed_state(self):
        single = aq.add_task(self.state, {"title": "Single"})
        single["title"] = "Mutated single"
        self.assertEqual("Single", self.state["tasks"][single["id"]]["title"])

        batch = aq.add_task_batch(
            self.state,
            [{"title": "Batch", "labels": ["original"]}],
        )
        batch[0]["labels"].append("mutated")

        self.assertEqual(
            ["original"], self.state["tasks"][batch[0]["id"]]["labels"]
        )

    def test_batch_failures_leave_original_state_byte_for_byte_unchanged(self):
        existing = aq.add_task(self.state, {"id": "T-000005", "title": "Existing"})
        cases = {
            "missing dependency": [
                {"title": "Broken", "depends_on": ["T-999999"]}
            ],
            "duplicate task id": [
                {"id": existing["id"], "title": "Duplicate"}
            ],
            "task must be an object": ["not an object"],
            "dependency cycle": [
                {
                    "id": "T-000010",
                    "title": "A",
                    "depends_on": ["T-000011"],
                },
                {
                    "id": "T-000011",
                    "title": "B",
                    "depends_on": ["T-000010"],
                },
            ],
        }

        for message, raw_tasks in cases.items():
            with self.subTest(message=message):
                before = json.dumps(self.state, allow_nan=False, sort_keys=True)
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.add_task_batch(self.state, raw_tasks)
                self.assertEqual(
                    before,
                    json.dumps(self.state, allow_nan=False, sort_keys=True),
                )

    def test_add_task_batch_requires_nonempty_list(self):
        for raw_tasks in (None, {}, (), []):
            with self.subTest(raw_tasks=raw_tasks):
                with self.assertRaisesRegex(aq.InvariantError, "nonempty list"):
                    aq.add_task_batch(self.state, raw_tasks)

    def test_validate_graph_rejects_missing_self_and_cycle_dependencies(self):
        valid = aq.add_task(self.state, {"title": "Valid"})
        cases = {
            "missing dependency": {
                valid["id"]: {
                    **valid,
                    "depends_on": ["T-999999"],
                }
            },
            "depends_on.*itself": {
                valid["id"]: {
                    **valid,
                    "depends_on": [valid["id"]],
                }
            },
            "dependency cycle": {
                "T-000010": {
                    **valid,
                    "id": "T-000010",
                    "depends_on": ["T-000011"],
                },
                "T-000011": {
                    **valid,
                    "id": "T-000011",
                    "depends_on": ["T-000010"],
                },
            },
        }

        for message, tasks in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_graph(tasks)

        self.assertIsNone(aq.validate_graph({}))

    def test_validate_graph_accepts_chain_beyond_python_recursion_limit(self):
        tasks = {
            f"T-{sequence:06d}": {
                "depends_on": (
                    [f"T-{sequence + 1:06d}"] if sequence < 1400 else []
                )
            }
            for sequence in range(1, 1401)
        }

        self.assertIsNone(aq.validate_graph(tasks))

    def test_normalize_deduplicates_task_lists_preserving_first_occurrence(self):
        task = aq.add_task(
            self.state,
            {
                "title": "Canonical lists",
                "depends_on": [],
                "resources": ["file:b", "file:a", "file:b"],
                "labels": ["python", "queue", "python"],
            },
        )

        self.assertEqual(["file:b", "file:a"], task["resources"])
        self.assertEqual(["python", "queue"], task["labels"])

    def test_normalize_rejects_invalid_scalar_fields(self):
        cases = (
            ({"title": "  "}, "title"),
            ({"title": 123}, "title"),
            ({"title": "Bad ID", "id": "T-1"}, "id"),
            ({"title": "Bool priority", "priority": True}, "priority"),
            ({"title": "Float priority", "priority": 1.5}, "priority"),
            ({"title": "Zero attempts", "max_attempts": 0}, "max_attempts"),
            ({"title": "Bool attempts", "max_attempts": True}, "max_attempts"),
            ({"title": "Workflow type", "workflow_id": 1}, "workflow_id"),
            ({"title": "Role type", "role": ["review"]}, "role"),
            ({"title": "Description type", "description": 1}, "description"),
        )

        for raw, message in cases:
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.add_task(self.state, raw)

    def test_normalize_rejects_non_list_or_non_string_collections(self):
        cases = (
            ({"depends_on": "T-000001"}, "depends_on.*list"),
            ({"resources": ("file:a",)}, "resources.*list"),
            ({"labels": {"queue"}}, "labels.*list"),
            ({"depends_on": [1]}, "depends_on.*string"),
            ({"resources": [None]}, "resources.*string"),
            ({"labels": [False]}, "labels.*string"),
        )

        for fields, message in cases:
            with self.subTest(fields=fields):
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.add_task(self.state, {"title": "Invalid collection", **fields})

    def test_normalize_rejects_unknown_creation_keys_atomically(self):
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "unknown.*depend_on"):
            aq.add_task(
                self.state,
                {"title": "Typo", "depend_on": ["T-000001"]},
            )

        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_normalize_rejects_non_string_creation_key(self):
        with self.assertRaisesRegex(aq.InvariantError, "string field names"):
            aq.add_task(self.state, {"title": "Invalid key", 1: "value"})

    def test_validate_state_rejects_noncanonical_stored_task(self):
        task = aq.add_task(self.state, {"title": "Stored"})
        cases = (
            (lambda candidate: candidate.pop("description"), "task fields.*description"),
            (lambda candidate: candidate.update({"unexpected": True}), "task fields.*unexpected"),
            (lambda candidate: candidate.update({"status": "ready"}), "status"),
            (lambda candidate: candidate.update({"status": []}), "status"),
            (lambda candidate: candidate.update({"attempts": True}), "attempts"),
            (lambda candidate: candidate.update({"max_attempts": 0}), "max_attempts"),
            (
                lambda candidate: candidate.update(
                    {"attempts": candidate["max_attempts"] + 1}
                ),
                "attempts.*max_attempts",
            ),
            (lambda candidate: candidate.update({"priority": False}), "priority"),
            (lambda candidate: candidate.update({"created_at": None}), "created_at"),
            (
                lambda candidate: candidate.update(
                    {
                        "created_at": "2026-07-10T06:00:01Z",
                        "updated_at": "2026-07-10T06:00:00Z",
                    }
                ),
                "updated_at.*created_at",
            ),
            (lambda candidate: candidate.update({"available_at": 1}), "available_at"),
            (lambda candidate: candidate.update({"resources": ["x", "x"]}), "resources.*duplicate"),
            (lambda candidate: candidate.update({"labels": [1]}), "labels.*string"),
            (lambda candidate: candidate.update({"claim": "worker"}), "claim"),
            (lambda candidate: candidate.update({"result": ["done"]}), "result"),
            (lambda candidate: candidate.update({"last_error": float("nan")}), "last_error"),
        )

        for mutate, message in cases:
            with self.subTest(message=message):
                candidate = json.loads(json.dumps(self.state))
                mutate(candidate["tasks"][task["id"]])
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_state(candidate)

    def test_validate_state_rejects_task_id_that_differs_from_dictionary_key(self):
        task = aq.add_task(self.state, {"title": "Stored"})
        self.state["tasks"][task["id"]]["id"] = "T-000002"

        with self.assertRaisesRegex(aq.InvariantError, "tasks.T-000001.id"):
            aq.validate_state(self.state)

    def test_validate_state_rejects_non_string_stored_task_key(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        self.state["tasks"][created["id"]][1] = "unexpected"

        with self.assertRaisesRegex(aq.InvariantError, "task field names"):
            aq.validate_state(self.state)

    def test_validate_state_accepts_canonical_completed_result_and_error(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]
        task["status"] = "completed"
        task["result"] = canonical_result(artifacts=["report.txt"])
        task["last_error"] = {
            "message": "earlier retry",
            "at": "2026-07-10T06:00:00Z",
        }

        self.assertIs(self.state, aq.validate_state(self.state))

    def test_validate_state_rejects_malformed_lifecycle_combinations(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        cases = (
            ({"result": canonical_result()}, "result.*completed"),
            ({"status": "completed", "result": None}, "completed.*result"),
            (
                {
                    "status": "completed",
                    "result": {
                        "summary": "done",
                        "artifacts": [],
                        "git": None,
                        "extra": 1,
                    },
                },
                "result.*keys",
            ),
            (
                {
                    "status": "completed",
                    "result": {"summary": 1, "artifacts": [], "git": None},
                },
                "result.*summary",
            ),
            (
                {
                    "status": "completed",
                    "result": {
                        "summary": "done",
                        "artifacts": [""],
                        "git": None,
                    },
                },
                "result.*artifacts",
            ),
            ({"last_error": {"message": "bad"}}, "last_error.*keys"),
            (
                {
                    "last_error": {
                        "message": "blocked",
                        "at": "2026-07-10T06:00:00Z",
                        "kind": "other",
                    }
                },
                "last_error.*kind",
            ),
            ({"status": "failed"}, "failed.*last_error"),
            (
                {
                    "status": "failed",
                    "last_error": {
                        "message": "blocked before failure",
                        "at": "2026-07-10T06:00:00Z",
                        "kind": "blocked",
                    },
                },
                "failed.*kind",
            ),
            (
                {
                    "status": "failed",
                    "last_error": {
                        "message": "failed",
                        "at": "2026-07-10T06:00:00Z",
                    },
                    "available_at": "2026-07-10T06:00:00Z",
                },
                "available_at",
            ),
            ({"status": "leased", "attempts": 1, "claim": canonical_claim(), "available_at": "2026-07-10T06:00:00Z"}, "available_at"),
        )
        for updates, message in cases:
            with self.subTest(updates=updates):
                candidate = copy.deepcopy(self.state)
                candidate["tasks"][created["id"]].update(updates)
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_state(candidate)

    def test_validate_state_rejects_non_json_future_metadata(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]

        for field, value in (
            ("claim", {"worker": object()}),
            ("result", {"score": float("nan")}),
            ("last_error", {"codes": {1, 2}}),
        ):
            with self.subTest(field=field):
                task[field] = value
                with self.assertRaisesRegex(aq.InvariantError, field):
                    aq.validate_state(self.state)
                task[field] = None

    def test_validate_state_rejects_circular_dict_metadata(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]
        circular = {}
        circular["self"] = circular
        task["claim"] = circular

        with self.assertRaisesRegex(aq.InvariantError, "claim.*circular"):
            aq.validate_state(self.state)

    def test_validate_state_rejects_circular_list_metadata(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]
        circular = []
        circular.append(circular)
        task["result"] = {"items": circular}

        with self.assertRaisesRegex(aq.InvariantError, "result.*circular"):
            aq.validate_state(self.state)

    def test_validate_state_rejects_metadata_beyond_depth_64(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]
        metadata = {}
        cursor = metadata
        for _ in range(65):
            child = {}
            cursor["child"] = child
            cursor = child
        task["last_error"] = metadata

        with self.assertRaisesRegex(
            aq.InvariantError, "last_error.*maximum depth 64"
        ):
            aq.validate_state(self.state)


class SchemaMigrationTests(unittest.TestCase):
    NOW = "2026-07-20T00:00:00Z"

    def legacy_state(self):
        state = aq.new_state("legacy", aq.fixed_config())
        state["schema_version"] = 1
        for task in state["tasks"].values():
            task.pop("git_mode", None)
            task.pop("git_recovery", None)
            if isinstance(task.get("claim"), dict):
                task["claim"].pop("git", None)
            if isinstance(task.get("result"), dict):
                task["result"].pop("git", None)
        aq.validate_state(state)
        return state

    def test_new_queues_use_schema_two_with_git_defaults(self):
        state = aq.new_state("demo", aq.fixed_config())
        task = aq.add_task(state, {"title": "generic"})

        self.assertEqual(2, state["schema_version"])
        self.assertIsNone(task["git_mode"])
        self.assertIsNone(task["git_recovery"])

    def test_version_one_queue_keeps_generic_mutation_semantics(self):
        state = self.legacy_state()

        task = aq.add_task(state, {"title": "legacy generic"})

        self.assertEqual(1, state["schema_version"])
        self.assertNotIn("git_mode", task)
        self.assertNotIn("git_recovery", task)
        aq.validate_state(state)

    def test_migrate_to_two_adds_defaults_and_one_event_atomically(self):
        state = self.legacy_state()
        task = aq.add_task(state, {"title": "legacy generic"})

        result = aq.migrate_state(state, 2, now=self.NOW)

        self.assertEqual({"from": 1, "to": 2}, result)
        self.assertEqual(2, state["schema_version"])
        self.assertIsNone(state["tasks"][task["id"]]["git_mode"])
        self.assertIsNone(state["tasks"][task["id"]]["git_recovery"])
        self.assertEqual("queue.migrated", state["events"][-1]["type"])
        self.assertEqual(
            {"from": 1, "to": 2},
            state["events"][-1]["details"],
        )

    def test_failed_migration_leaves_source_unchanged(self):
        state = self.legacy_state()
        state["tasks"]["bad"] = {}
        before = copy.deepcopy(state)

        with self.assertRaises(aq.InvariantError):
            aq.migrate_state(state, 2, now=self.NOW)

        self.assertEqual(before, state)


class WorkflowTemplateTests(unittest.TestCase):
    def setUp(self):
        self.state = aq.new_state("demo", aq.fixed_config())
        self.now = "2026-07-11T01:02:03Z"

    def test_adversarial_review_builds_exact_graph_for_reviewer_counts(self):
        for reviewer_count in (1, 2, 3):
            with self.subTest(reviewer_count=reviewer_count):
                state = aq.new_state("demo", aq.fixed_config())
                result = aq.add_adversarial_review(
                    state, "Ship change", 40, ["src/a.py", "src/b.py"],
                    reviewer_count, now=self.now,
                )
                count = reviewer_count + 3
                ids = [f"T-{index:06d}" for index in range(1, count + 1)]
                self.assertEqual({
                    "workflow_id": "W-000001",
                    "template": "adversarial-review",
                    "task_ids": ids,
                }, result)
                tasks = [state["tasks"][task_id] for task_id in ids]
                self.assertEqual(
                    ["implement"] + ["review"] * reviewer_count
                    + ["apply", "verify"],
                    [task["role"] for task in tasks],
                )
                self.assertEqual(
                    ["Ship change"]
                    + [f"Review {i}: Ship change" for i in range(1, reviewer_count + 1)]
                    + ["Apply reviews: Ship change", "Verify: Ship change"],
                    [task["title"] for task in tasks],
                )
                self.assertEqual(
                    [40] + [30] * reviewer_count + [20, 10],
                    [task["priority"] for task in tasks],
                )
                review_ids = ids[1:1 + reviewer_count]
                self.assertEqual(
                    [[]] + [[ids[0]]] * reviewer_count
                    + [review_ids, [ids[-2]]],
                    [task["depends_on"] for task in tasks],
                )
                self.assertEqual(
                    [["src/a.py", "src/b.py"]]
                    + [[]] * reviewer_count
                    + [["src/a.py", "src/b.py"], []],
                    [task["resources"] for task in tasks],
                )
                for review in tasks[1:1 + reviewer_count]:
                    self.assertIn("src/a.py, src/b.py", review["description"])
                    self.assertIn("review the implementation artifact", review["description"].lower())
                    self.assertIn("without implementer reasoning or other reviewer findings", review["description"].lower())
                self.assertEqual(count + 1, state["next_task_sequence"])
                self.assertEqual(2, state["next_workflow_sequence"])
                self.assertEqual({
                    "template": "adversarial-review",
                    "workflow_id": "W-000001",
                    "task_ids": ids,
                    "task_count": count,
                    "reviewer_count": reviewer_count,
                }, state["events"][0]["details"])

    def test_git_enabled_adversarial_review_marks_only_writer_roles(self):
        result = aq.add_adversarial_review(
            self.state,
            "Change",
            20,
            ["dir:src/"],
            2,
            git_commit=True,
            now=self.now,
        )
        tasks = [
            self.state["tasks"][task_id]
            for task_id in result["task_ids"]
        ]

        self.assertEqual(
            ["commit", None, None, "commit", None],
            [task["git_mode"] for task in tasks],
        )
        self.assertTrue(self.state["events"][0]["details"]["git_commit"])

    def test_adversarial_review_readiness_isolated_by_graph_and_resources(self):
        result = aq.add_adversarial_review(
            self.state, "Change", 0, ["shared"], 3, now=self.now
        )
        implement_id, *review_and_tail = result["task_ids"]
        review_ids = review_and_tail[:3]
        apply_id, verify_id = review_and_tail[3:]
        self.state["tasks"][implement_id]["status"] = "completed"
        self.state["tasks"][implement_id]["result"] = canonical_result()
        rows = {row["id"]: row for row in aq.status_rows(self.state, self.now)}
        self.assertEqual(["ready"] * 3, [rows[task_id]["state"] for task_id in review_ids])
        self.assertEqual([[]] * 3, [self.state["tasks"][task_id]["resources"] for task_id in review_ids])
        self.assertEqual("waiting_dependency", rows[apply_id]["state"])
        self.assertEqual("waiting_dependency", rows[verify_id]["state"])
        for task_id in review_ids:
            self.state["tasks"][task_id]["status"] = "completed"
            self.state["tasks"][task_id]["result"] = canonical_result()
        rows = {row["id"]: row for row in aq.status_rows(self.state, self.now)}
        self.assertEqual("ready", rows[apply_id]["state"])
        self.assertEqual("waiting_dependency", rows[verify_id]["state"])

    def test_parallel_shards_builds_exact_graph_and_normalizes_each_shard(self):
        for shards in ([['a', 'a']], [['a'], ['b', 'c'], ['d']]):
            with self.subTest(shards=shards):
                state = aq.new_state("demo", aq.fixed_config())
                result = aq.add_parallel_shards(
                    state, "Build", 9, shards, now=self.now
                )
                normalized = [list(dict.fromkeys(shard)) for shard in shards]
                shard_count = len(shards)
                ids = [f"T-{i:06d}" for i in range(1, shard_count + 3)]
                self.assertEqual({
                    "workflow_id": "W-000001",
                    "template": "parallel-shards",
                    "task_ids": ids,
                }, result)
                tasks = [state["tasks"][task_id] for task_id in ids]
                self.assertEqual(
                    [f"Shard {i}: Build" for i in range(1, shard_count + 1)]
                    + ["Integrate: Build", "Verify: Build"],
                    [task["title"] for task in tasks],
                )
                self.assertEqual(
                    ["shard"] * shard_count + ["integrate", "verify"],
                    [task["role"] for task in tasks],
                )
                self.assertEqual(
                    [9] * shard_count + [-1, -11],
                    [task["priority"] for task in tasks],
                )
                flattened = [resource for shard in normalized for resource in shard]
                self.assertEqual(
                    normalized + [flattened, []],
                    [task["resources"] for task in tasks],
                )
                self.assertEqual(
                    [[]] * shard_count + [ids[:shard_count], [ids[-2]]],
                    [task["depends_on"] for task in tasks],
                )
                self.assertEqual({
                    "template": "parallel-shards",
                    "workflow_id": "W-000001",
                    "task_ids": ids,
                    "task_count": shard_count + 2,
                    "shard_count": shard_count,
                }, state["events"][0]["details"])

    def test_git_enabled_parallel_shards_marks_writers_and_rejects_overlap(self):
        result = aq.add_parallel_shards(
            self.state,
            "Build",
            20,
            [["file:src/a.py"], ["dir:src/b/"]],
            git_commit=True,
            now=self.now,
        )
        tasks = [
            self.state["tasks"][task_id]
            for task_id in result["task_ids"]
        ]
        self.assertEqual(
            ["commit", "commit", "commit", None],
            [task["git_mode"] for task in tasks],
        )
        self.assertTrue(self.state["events"][0]["details"]["git_commit"])

        other = aq.new_state("overlap", aq.fixed_config())
        with self.assertRaisesRegex(aq.InvariantError, "overlap"):
            aq.add_parallel_shards(
                other,
                "Unsafe",
                20,
                [["dir:src/"], ["file:src/a.py"]],
                git_commit=True,
                now=self.now,
            )

    def test_workflow_api_copies_once_and_validates_source_and_candidate_once(self):
        real_deepcopy = aq.copy.deepcopy
        real_validate_state = aq.validate_state
        real_validate_graph = aq.validate_graph
        with mock.patch.object(
            aq.copy, "deepcopy", wraps=real_deepcopy
        ) as deepcopy_spy, mock.patch.object(
            aq, "validate_state", wraps=real_validate_state
        ) as validate_state_spy, mock.patch.object(
            aq, "validate_graph", wraps=real_validate_graph
        ) as validate_graph_spy:
            result = aq.add_adversarial_review(
                self.state, "Efficient", 0, ["r"], 2, now=self.now
            )

        self.assertEqual(5, len(result["task_ids"]))
        self.assertEqual(1, deepcopy_spy.call_count)
        self.assertEqual(2, validate_state_spy.call_count)
        self.assertEqual(2, validate_graph_spy.call_count)

    def test_workflow_templates_respect_advanced_historical_counters(self):
        aq.add_task(self.state, {"title": "old"})
        del self.state["tasks"]["T-000001"]
        self.state["next_workflow_sequence"] = 4
        before_task = self.state["next_task_sequence"]
        before_workflow = self.state["next_workflow_sequence"]

        result = aq.add_adversarial_review(
            self.state, "Fresh", 0, ["r"], 1, now=self.now
        )

        self.assertEqual("W-000004", result["workflow_id"])
        self.assertEqual(
            [f"T-{index:06d}" for index in range(2, 6)], result["task_ids"]
        )
        self.assertEqual(before_task + 4, self.state["next_task_sequence"])
        self.assertEqual(before_workflow + 1, self.state["next_workflow_sequence"])
        self.assertNotIn("T-000001", self.state["tasks"])

    def test_workflow_templates_never_preallocate_ids(self):
        with mock.patch.object(
            aq, "allocate_id", side_effect=AssertionError("must not preallocate")
        ):
            first = aq.add_adversarial_review(
                self.state, "First", 0, ["a"], 1, now=self.now
            )
            second = aq.add_parallel_shards(
                self.state, "Second", 0, [["b"]], now=self.now
            )
        self.assertEqual("W-000001", first["workflow_id"])
        self.assertEqual("W-000002", second["workflow_id"])
        self.assertEqual("T-000005", second["task_ids"][0])

    def test_workflow_results_are_detached_and_event_is_exact_and_sanitized(self):
        result = aq.add_adversarial_review(
            self.state, "Do not leak lease_token", 5, ["secret"], 2,
            now=self.now,
        )
        event = self.state["events"][0]
        self.assertEqual({
            "seq": 1,
            "at": self.now,
            "type": "workflow.created",
            "actor": "operator",
            "task_id": None,
            "revision": 1,
            "details": {
                "template": "adversarial-review",
                "workflow_id": "W-000001",
                "task_ids": [f"T-{index:06d}" for index in range(1, 6)],
                "task_count": 5,
                "reviewer_count": 2,
            },
        }, event)
        self.assertNotIn("lease_token", json.dumps(event))
        result["task_ids"].append("T-999999")
        result["workflow_id"] = "W-999999"
        self.assertEqual(5, len(self.state["tasks"]))
        self.assertEqual("W-000001", next(iter(self.state["tasks"].values()))["workflow_id"])
        self.assertEqual(
            [f"T-{index:06d}" for index in range(1, 6)],
            event["details"]["task_ids"],
        )

    def test_invalid_workflow_inputs_and_id_exhaustion_are_byte_atomic(self):
        cases = (
            lambda state: aq.add_adversarial_review(state, "", 0, ["r"], 1, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", True, ["r"], 1, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", 0, "r", 1, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", 0, ["r", "r"], 1, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", 0, [""], 1, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", 0, ["r"], True, now=self.now),
            lambda state: aq.add_adversarial_review(state, "x", 0, ["r"], 0, now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, [], now=self.now),
            lambda state: aq.add_parallel_shards(state, "", 0, [["r"]], now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", False, [["r"]], now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, "r", now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, [[]], now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, [["r"], ["r"]], now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, [[" "]], now=self.now),
            lambda state: aq.add_parallel_shards(state, "x", 0, [[1]], now=self.now),
        )
        for operation in cases:
            with self.subTest(operation=operation):
                state = aq.new_state("demo", aq.fixed_config())
                before = json.dumps(state, sort_keys=True).encode()
                with self.assertRaises(aq.InvariantError):
                    operation(state)
                self.assertEqual(before, json.dumps(state, sort_keys=True).encode())
        for field in ("next_task_sequence", "next_workflow_sequence"):
            state = aq.new_state("demo", aq.fixed_config())
            state[field] = aq.MAX_ID_SEQUENCE + 1
            before = copy.deepcopy(state)
            with self.assertRaisesRegex(aq.InvariantError, "exhausted"):
                aq.add_adversarial_review(state, "x", 0, ["r"], 1, now=self.now)
            self.assertEqual(before, state)
        state = aq.new_state("demo", aq.fixed_config())
        state["next_task_sequence"] = aq.MAX_ID_SEQUENCE - 1
        before = copy.deepcopy(state)
        with self.assertRaisesRegex(aq.InvariantError, "task.*exhausted"):
            aq.add_adversarial_review(state, "x", 0, ["r"], 1, now=self.now)
        self.assertEqual(before, state)

    def test_workflow_apis_reject_malformed_state_without_raw_key_error(self):
        operations = (
            lambda state: aq.add_adversarial_review(
                state, "x", 0, ["r"], 1, now=self.now
            ),
            lambda state: aq.add_parallel_shards(
                state, "x", 0, [["r"]], now=self.now
            ),
        )
        for operation in operations:
            with self.subTest(operation=operation):
                malformed = {}
                before = copy.deepcopy(malformed)
                with self.assertRaises(aq.InvariantError):
                    operation(malformed)
                self.assertEqual(before, malformed)

    def test_event_failure_rolls_back_tasks_and_counters(self):
        aq.append_event(
            self.state, "prior", "operator", None, {}, "2026-07-11T02:00:00Z"
        )
        before = copy.deepcopy(self.state)
        with self.assertRaisesRegex(aq.InvariantError, "latest event"):
            aq.add_parallel_shards(
                self.state, "x", 0, [["r"]], now="2026-07-11T01:00:00Z"
            )
        self.assertEqual(before, self.state)


class ClaimTaskTests(unittest.TestCase):
    CREATED = "2026-07-10T05:00:00Z"
    NOW = "2026-07-10T06:00:00Z"

    def setUp(self):
        self.clock = mock.patch.object(aq, "utc_now", return_value=self.CREATED)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.state = aq.new_state("demo", aq.fixed_config())

    def add(self, title, **fields):
        created = aq.add_task(self.state, {"title": title, **fields})
        return self.state["tasks"][created["id"]]

    def test_no_task_available_has_exit_code_three(self):
        self.assertTrue(issubclass(aq.NoTaskAvailable, aq.QueueError))
        self.assertEqual(3, aq.NoTaskAvailable.exit_code)

    def test_add_seconds_requires_canonical_time_and_positive_integer(self):
        self.assertEqual(
            "2026-07-10T06:15:00Z", aq.add_seconds(self.NOW, 900)
        )
        for timestamp, seconds in (
            ("2026-07-10T06:00:00+00:00", 1),
            ("9999-12-31T23:59:59Z", 1),
            (self.NOW, 0),
            (self.NOW, -1),
            (self.NOW, True),
            (self.NOW, 1.5),
        ):
            with self.subTest(timestamp=timestamp, seconds=seconds):
                with self.assertRaises(aq.InvariantError):
                    aq.add_seconds(timestamp, seconds)

    def test_claim_sorts_by_priority_then_creation_sequence(self):
        self.add("Low", priority=1)
        first_high = self.add("First high", priority=9)
        second_high = self.add("Second high", priority=9)

        first = aq.claim_task(self.state, "agent-1", now=self.NOW)
        second = aq.claim_task(self.state, "agent-2", now=self.NOW)

        self.assertEqual(first_high["id"], first["task"]["id"])
        self.assertEqual(second_high["id"], second["task"]["id"])

    def test_claim_applies_dependency_retry_attempt_role_and_label_filters(self):
        dependency = self.add("Dependency")
        self.add("Blocked", priority=100, depends_on=[dependency["id"]])
        retry = self.add("Retry", priority=90)
        retry["available_at"] = "2026-07-10T06:00:01Z"
        exhausted = self.add("Exhausted", priority=80, max_attempts=1)
        exhausted["attempts"] = 1
        self.add(
            "Wrong role",
            priority=70,
            role="builder",
            labels=["python", "queue"],
        )
        target = self.add(
            "Target",
            priority=1,
            role="reviewer",
            labels=["python", "queue", "safe"],
        )

        result = aq.claim_task(
            self.state,
            "agent-1",
            now=self.NOW,
            role="reviewer",
            labels={"queue", "python"},
        )

        self.assertEqual(target["id"], result["task"]["id"])

    def test_resource_conflict_skips_higher_priority_for_disjoint_task(self):
        holder = self.add("Holder", priority=100, resources=["repo"])
        aq.claim_task(self.state, "holder", now=self.NOW)
        conflicting = self.add("Conflict", priority=90, resources=["repo"])
        disjoint = self.add("Disjoint", priority=10, resources=["db"])

        result = aq.claim_task(self.state, "worker", now=self.NOW)

        self.assertEqual(disjoint["id"], result["task"]["id"])
        self.assertEqual(
            "pending", self.state["tasks"][conflicting["id"]]["status"]
        )
        self.assertEqual("leased", self.state["tasks"][holder["id"]]["status"])
        with self.assertRaises(aq.NoTaskAvailable):
            aq.claim_task(self.state, "worker-2", now=self.NOW)

    def test_git_claim_requires_clean_attached_observation_and_stores_binding(self):
        task = self.add(
            "Git task",
            git_mode="commit",
            resources=["file:src/api.py"],
        )

        for observation, message in (
            (git_snapshot(clean=False), "clean"),
            (git_snapshot(attached=False), "attached"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.claim_task(
                        self.state,
                        "worker",
                        now=self.NOW,
                        git_observation=observation,
                        task_id=task["id"],
                    )

        claimed = aq.claim_task(
            self.state,
            "worker",
            now=self.NOW,
            git_observation=git_snapshot(),
            task_id=task["id"],
        )
        binding = claimed["task"]["claim"]["git"]
        self.assertEqual("base-a", binding["base"])
        self.assertEqual("repo-1", binding["repository_id"])
        safe = aq._safe_task(claimed["task"])
        serialized = json.dumps(safe)
        self.assertNotIn("/private/repository", serialized)
        self.assertNotIn("/private/worktrees", serialized)
        self.assertNotIn(claimed["lease_token"], serialized)

    def test_git_claim_blocks_derived_ownership_and_allows_disjoint_worktrees(self):
        holder = self.add(
            "Holder",
            priority=100,
            git_mode="commit",
            resources=["dir:src/"],
        )
        aq.claim_task(
            self.state,
            "holder",
            now=self.NOW,
            git_observation=git_snapshot(),
            task_id=holder["id"],
        )

        cases = (
            (
                "same worktree",
                ["file:other.txt"],
                git_snapshot(worktree_id="wt-1", branch="refs/heads/other"),
            ),
            (
                "same branch",
                ["file:other.txt"],
                git_snapshot(worktree_id="wt-2"),
            ),
            (
                "overlapping scope",
                ["file:src/api.py"],
                git_snapshot(
                    worktree_id="wt-3",
                    branch="refs/heads/feature-c",
                ),
            ),
        )
        for title, resources, observation in cases:
            task = self.add(
                title,
                git_mode="commit",
                resources=resources,
            )
            with self.subTest(title=title):
                with self.assertRaises(aq.NoTaskAvailable):
                    aq.claim_task(
                        self.state,
                        title,
                        now=self.NOW,
                        git_observation=observation,
                        task_id=task["id"],
                    )

        disjoint = self.add(
            "Disjoint",
            git_mode="commit",
            resources=["file:tests/api.py"],
        )
        claimed = aq.claim_task(
            self.state,
            "disjoint",
            now=self.NOW,
            git_observation=git_snapshot(
                worktree_id="wt-4",
                branch="refs/heads/feature-d",
            ),
            task_id=disjoint["id"],
        )
        self.assertEqual(disjoint["id"], claimed["task"]["id"])

    def test_generic_claim_does_not_require_git_observation(self):
        task = self.add("Generic")

        claimed = aq.claim_task(
            self.state,
            "worker",
            now=self.NOW,
            git_observation=None,
            task_id=task["id"],
        )

        self.assertEqual(task["id"], claimed["task"]["id"])
        self.assertIsNone(claimed["task"]["claim"]["git"])

    def test_claim_eligibility_uses_boolean_resource_conflict_probe(self):
        self.add("Holder", priority=100, resources=["repo"])
        aq.claim_task(self.state, "holder", now=self.NOW)
        for number in range(20):
            self.add(
                f"Conflict {number}",
                priority=100 - number,
                resources=["repo"],
            )
        target = self.add("Disjoint", priority=1, resources=["db"])

        with mock.patch.object(
            aq,
            "_resource_conflicts",
            side_effect=AssertionError("full blocker lists built during claim"),
        ) as blocker_lists:
            result = aq.claim_task(self.state, "worker", now=self.NOW)

        self.assertEqual(target["id"], result["task"]["id"])
        blocker_lists.assert_not_called()

    def test_claim_has_exact_transition_detached_result_and_sanitized_event(self):
        task = self.add("Claim me")
        before_revision = self.state["revision"]

        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="TOP-SECRET-TOKEN"
        ):
            result = aq.claim_task(
                self.state,
                "agent-1",
                now=self.NOW,
                lease_seconds=30,
            )

        stored = self.state["tasks"][task["id"]]
        self.assertEqual(1, stored["attempts"])
        self.assertEqual("leased", stored["status"])
        self.assertIsNone(stored["available_at"])
        self.assertEqual(self.NOW, stored["updated_at"])
        self.assertEqual(
            {
                "agent_id": "agent-1",
                "lease_token": "lq_TOP-SECRET-TOKEN",
                "claimed_at": self.NOW,
                "heartbeat_at": self.NOW,
                "expires_at": "2026-07-10T06:00:30Z",
                "git": None,
            },
            stored["claim"],
        )
        self.assertEqual("lq_TOP-SECRET-TOKEN", result["lease_token"])
        self.assertEqual(stored["claim"]["expires_at"], result["expires_at"])
        self.assertEqual(stored, result["task"])
        self.assertIsNot(stored, result["task"])
        result["task"]["title"] = "mutated"
        self.assertEqual("Claim me", stored["title"])
        self.assertEqual(before_revision, self.state["revision"])
        self.assertEqual(2, self.state["next_event_sequence"])
        self.assertEqual(
            {
                "seq": 1,
                "at": self.NOW,
                "type": "task.claimed",
                "actor": "agent-1",
                "task_id": task["id"],
                "revision": before_revision + 1,
                "details": {"lease_seconds": 30, "attempt": 1},
            },
            self.state["events"][0],
        )
        status = aq.status_rows(self.state, self.NOW)
        rendered = aq.render_tsv(self.state, self.NOW)
        for projection in (repr(self.state["events"]), repr(status), rendered):
            self.assertNotIn("TOP-SECRET-TOKEN", projection)
            self.assertNotIn("lease_token", projection)

    def test_append_event_deep_copies_and_recursively_removes_lease_tokens(self):
        task = self.add("Stored")
        details = {
            "attempt": 1,
            "lease_token": "outer-secret",
            "nested": [{"lease_token": "inner-secret", "safe": True}],
        }

        event = aq.append_event(
            self.state,
            "task.claimed",
            "agent-1",
            task["id"],
            details,
            self.NOW,
        )
        details["nested"][0]["safe"] = False

        self.assertEqual(
            {"attempt": 1, "nested": [{"safe": True}]}, event["details"]
        )
        self.assertNotIn("secret", repr(self.state["events"]))

        before = json.dumps(self.state, sort_keys=True)
        with self.assertRaisesRegex(aq.InvariantError, "task_id"):
            aq.append_event(
                self.state,
                "task.claimed",
                "agent-1",
                [],
                {},
                self.NOW,
            )
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_invalid_claim_inputs_are_atomic(self):
        self.add("Ready", role="reviewer", labels=["queue"])
        cases = (
            ({"agent_id": ""}, "agent_id"),
            ({"agent_id": "   "}, "agent_id"),
            ({"agent_id": 7}, "agent_id"),
            ({"agent_id": "agent", "role": ""}, "role"),
            ({"agent_id": "agent", "role": 7}, "role"),
            ({"agent_id": "agent", "labels": "queue"}, "labels"),
            ({"agent_id": "agent", "labels": [1]}, "labels"),
            ({"agent_id": "agent", "lease_seconds": 0}, "lease_seconds"),
            ({"agent_id": "agent", "lease_seconds": True}, "lease_seconds"),
            ({"agent_id": "agent", "now": "tomorrow"}, "now"),
        )
        for kwargs, message in cases:
            with self.subTest(kwargs=kwargs):
                before = json.dumps(self.state, sort_keys=True)
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.claim_task(self.state, **kwargs)
                self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_no_candidate_is_exit_three_and_byte_identical(self):
        task = self.add("Future")
        task["available_at"] = "2026-07-10T06:00:01Z"
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaises(aq.NoTaskAvailable) as raised:
            aq.claim_task(self.state, "agent-1", now=self.NOW)

        self.assertEqual(3, raised.exception.exit_code)
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_claim_skips_task_updated_after_now_without_mutation(self):
        task = self.add("Future update")
        task["updated_at"] = "2026-07-10T06:00:01Z"
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaises(aq.NoTaskAvailable):
            aq.claim_task(self.state, "agent-1", now=self.NOW)

        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_validate_state_rejects_malformed_claim_contracts(self):
        task = self.add("Stored")
        valid = canonical_claim()
        cases = (
            ("leased without claim", "leased", 1, None, "claim"),
            ("pending with claim", "pending", 1, valid, "claim"),
            ("leased zero attempts", "leased", 0, valid, "attempts"),
            (
                "claim keys",
                "leased",
                1,
                {**valid, "extra": True},
                "claim.*keys",
            ),
            (
                "token type",
                "leased",
                1,
                {**valid, "lease_token": 7},
                "lease_token",
            ),
            (
                "blank token",
                "leased",
                1,
                {**valid, "lease_token": ""},
                "lease_token",
            ),
            (
                "heartbeat precedes claim",
                "leased",
                1,
                {**valid, "claimed_at": "2026-07-10T05:01:00Z"},
                "claimed_at.*heartbeat_at",
            ),
            (
                "heartbeat equals expiry",
                "leased",
                1,
                {**valid, "heartbeat_at": valid["expires_at"]},
                "heartbeat_at.*expires_at",
            ),
        )
        for name, status, attempts, claim, message in cases:
            with self.subTest(name=name):
                candidate = copy.deepcopy(self.state)
                stored = candidate["tasks"][task["id"]]
                stored.update(
                    {"status": status, "attempts": attempts, "claim": claim}
                )
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_state(candidate)

    def test_validate_state_rejects_invalid_events_and_counters(self):
        self.add("Ready")
        aq.claim_task(self.state, "agent-1", now=self.NOW)
        mutations = (
            (lambda state: state["events"][0].pop("actor"), "event.*fields"),
            (lambda state: state["events"][0].update({"seq": True}), "event.*seq"),
            (lambda state: state["events"][0].update({"at": "now"}), "event.*at"),
            (
                lambda state: state["events"][0].update({"task_id": "T-999999"}),
                "event.*task_id",
            ),
            (
                lambda state: state["events"][0].update({"revision": 0}),
                "event.*revision",
            ),
            (
                lambda state: state["events"][0].update(
                    {"details": {"score": float("nan")}}
                ),
                "event.*details",
            ),
            (
                lambda state: state["events"][0].update(
                    {"details": {"lease_token": "persisted-secret"}}
                ),
                "event.*details",
            ),
            (
                lambda state: state.update({"next_event_sequence": 1}),
                "next_event_sequence",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                candidate = copy.deepcopy(self.state)
                mutate(candidate)
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_state(candidate)

        unordered = copy.deepcopy(self.state)
        unordered["events"].append(
            {**copy.deepcopy(unordered["events"][0]), "seq": 1}
        )
        unordered["next_event_sequence"] = 3
        with self.assertRaisesRegex(aq.InvariantError, "event.*seq"):
            aq.validate_state(unordered)

    def test_validate_state_bounds_and_orders_event_revisions(self):
        first = self.add("First")
        second = self.add("Second")
        third = self.add("Third")
        aq.append_event(
            self.state,
            "task.claimed",
            "agent-1",
            first["id"],
            {"attempt": 1},
            self.NOW,
        )
        self.state["revision"] = 1
        aq.append_event(
            self.state,
            "task.claimed",
            "agent-2",
            second["id"],
            {"attempt": 1},
            self.NOW,
        )
        aq.append_event(
            self.state,
            "task.claimed",
            "agent-3",
            third["id"],
            {"attempt": 1},
            self.NOW,
        )
        self.assertEqual(
            [1, 2, 2],
            [event["revision"] for event in self.state["events"]],
        )

        too_new = copy.deepcopy(self.state)
        too_new["events"][2]["revision"] = too_new["revision"] + 2
        with self.assertRaisesRegex(aq.InvariantError, "event.*revision"):
            aq.validate_state(too_new)

        descending = copy.deepcopy(self.state)
        descending["events"][0]["revision"] = 2
        descending["events"][1]["revision"] = 1
        with self.assertRaisesRegex(aq.InvariantError, "event.*revision"):
            aq.validate_state(descending)

        self.assertIs(self.state, aq.validate_state(self.state))

    def test_validate_state_rejects_descending_event_timestamps(self):
        self.add("First")
        self.add("Second")
        aq.claim_task(self.state, "agent-1", now=self.NOW)
        aq.claim_task(
            self.state, "agent-2", now="2026-07-10T06:00:01Z"
        )
        candidate = copy.deepcopy(self.state)
        candidate["events"][1]["at"] = "2026-07-10T05:59:59Z"

        with self.assertRaisesRegex(aq.InvariantError, "event.*at.*nondecreasing"):
            aq.validate_state(candidate)

    def test_append_event_rejects_backdated_time_atomically(self):
        task = self.add("Ready")
        aq.append_event(
            self.state,
            "task.first",
            "operator",
            task["id"],
            {},
            self.NOW,
        )
        before = json.dumps(self.state, sort_keys=True)

        with self.assertRaisesRegex(aq.InvariantError, "now.*event"):
            aq.append_event(
                self.state,
                "task.second",
                "operator",
                task["id"],
                {},
                "2026-07-10T05:59:59Z",
            )

        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_expired_lease_does_not_block_but_owner_remains_leased(self):
        owner = self.add("Expired owner", resources=["repo"])
        owner.update(
            {
                "status": "leased",
                "attempts": 1,
                "claim": canonical_claim(
                    claimed_at="2026-07-10T04:00:00Z",
                    heartbeat_at="2026-07-10T05:00:00Z",
                    expires_at="2026-07-10T05:59:59Z",
                ),
            }
        )
        contender = self.add("Contender", resources=["repo"])

        result = aq.claim_task(self.state, "agent-2", now=self.NOW)

        self.assertEqual(contender["id"], result["task"]["id"])
        self.assertEqual(
            "leased", self.state["tasks"][owner["id"]]["status"]
        )

    def test_sequential_claims_do_not_double_lease_task_or_resource(self):
        first = self.add("First", priority=10, resources=["repo"])
        conflict = self.add("Conflict", priority=9, resources=["repo"])
        second = self.add("Second", priority=1, resources=["db"])

        first_result = aq.claim_task(self.state, "agent-1", now=self.NOW)
        second_result = aq.claim_task(self.state, "agent-2", now=self.NOW)

        self.assertEqual(first["id"], first_result["task"]["id"])
        self.assertEqual(second["id"], second_result["task"]["id"])
        self.assertNotEqual(
            first_result["task"]["id"], second_result["task"]["id"]
        )
        self.assertEqual(
            "pending", self.state["tasks"][conflict["id"]]["status"]
        )

    def test_mocked_token_never_leaks_to_event_status_or_tsv(self):
        self.add("Ready")
        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="MOCKED-SECRET"
        ):
            result = aq.claim_task(self.state, "agent-1", now=self.NOW)

        self.assertEqual("lq_MOCKED-SECRET", result["lease_token"])
        public = (
            repr(self.state["events"])
            + repr(aq.status_rows(self.state, self.NOW))
            + aq.render_tsv(self.state, self.NOW)
        )
        self.assertNotIn("MOCKED-SECRET", public)
        self.assertNotIn("lease_token", public)


class LifecycleTransitionTests(unittest.TestCase):
    CREATED = "2026-07-10T05:00:00Z"
    NOW = "2026-07-10T06:00:00Z"

    def setUp(self):
        self.clock = mock.patch.object(aq, "utc_now", return_value=self.CREATED)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.state = aq.new_state("demo", aq.fixed_config())

    def add(self, title="Task", **fields):
        created = aq.add_task(self.state, {"title": title, **fields})
        return self.state["tasks"][created["id"]]

    def claim(self, agent="agent-1", now=None, **task_fields):
        task_fields.setdefault("priority", 1000)
        task = self.add(**task_fields)
        with mock.patch.object(aq.secrets, "token_urlsafe", return_value="SECRET"):
            result = aq.claim_task(
                self.state, agent, now=now or self.NOW, lease_seconds=60
            )
        return task, result["lease_token"]

    def assert_atomic_error(self, error, pattern, callable_):
        before = json.dumps(self.state, sort_keys=True)
        with self.assertRaisesRegex(error, pattern):
            callable_()
        self.assertEqual(before, json.dumps(self.state, sort_keys=True))

    def test_lease_error_fences_wrong_missing_inactive_and_expired_workers(self):
        self.assertTrue(issubclass(aq.LeaseError, aq.QueueError))
        self.assertEqual(5, aq.LeaseError.exit_code)
        task, token = self.claim()
        cases = (
            ("agent-2", token, self.NOW, "agent"),
            ("agent-1", "WRONG", self.NOW, "token"),
            ("agent-1", token, "2026-07-10T06:01:00Z", "expired"),
        )
        for agent, candidate_token, now, message in cases:
            with self.subTest(message=message):
                self.assert_atomic_error(
                    aq.LeaseError,
                    message,
                    lambda: aq.require_lease(
                        self.state, task["id"], agent, candidate_token, now
                    ),
                )
        self.assert_atomic_error(
            aq.LeaseError,
            "not found",
            lambda: aq.require_lease(
                self.state, "T-999999", "agent-1", token, self.NOW
            ),
        )
        self.state["tasks"][task["id"]].update({"status": "pending", "claim": None})
        self.assert_atomic_error(
            aq.LeaseError,
            "not leased",
            lambda: aq.require_lease(
                self.state, task["id"], "agent-1", token, self.NOW
            ),
        )

    def test_require_lease_reports_corrupt_state_as_lease_error(self):
        task, token = self.claim()
        self.state["config"]["default_lease_seconds"] = 0
        self.assert_atomic_error(
            aq.LeaseError,
            "invalid queue state.*config",
            lambda: aq.require_lease(
                self.state, task["id"], "agent-1", token, self.NOW
            ),
        )

    def test_require_lease_returns_a_detached_task(self):
        task, token = self.claim()

        returned = aq.require_lease(
            self.state, task["id"], "agent-1", token, self.NOW
        )
        returned["title"] = "mutated"
        returned["claim"]["lease_token"] = "mutated-token"

        stored = self.state["tasks"][task["id"]]
        self.assertEqual("Task", stored["title"])
        self.assertEqual(token, stored["claim"]["lease_token"])

    def test_heartbeat_extends_from_now_and_redacts_token(self):
        task, token = self.claim()
        returned = aq.heartbeat_task(
            self.state,
            task["id"],
            "agent-1",
            token,
            lease_seconds=90,
            now="2026-07-10T06:00:30Z",
        )
        stored = self.state["tasks"][task["id"]]
        self.assertEqual("2026-07-10T06:00:30Z", stored["claim"]["heartbeat_at"])
        self.assertEqual("2026-07-10T06:02:00Z", stored["claim"]["expires_at"])
        self.assertEqual(1, stored["attempts"])
        self.assertIsNot(stored, returned)
        event = self.state["events"][-1]
        self.assertEqual("task.heartbeat", event["type"])
        self.assertEqual({"lease_seconds": 90}, event["details"])
        self.assertNotIn(token, repr(event))

    def test_heartbeat_invalid_duration_and_overflow_are_atomic(self):
        task, token = self.claim()
        self.state["tasks"][task["id"]]["claim"].update(
            {
                "heartbeat_at": "9999-12-31T23:59:57Z",
                "expires_at": "9999-12-31T23:59:59Z",
            }
        )
        for seconds, now, message in (
            (0, "9999-12-31T23:59:58Z", "lease_seconds"),
            (True, "9999-12-31T23:59:58Z", "lease_seconds"),
            (2, "9999-12-31T23:59:58Z", "addition"),
        ):
            with self.subTest(seconds=seconds):
                self.assert_atomic_error(
                    aq.InvariantError,
                    message,
                    lambda: aq.heartbeat_task(
                        self.state,
                        task["id"],
                        "agent-1",
                        token,
                        lease_seconds=seconds,
                        now=now,
                    ),
                )

    def test_worker_transitions_reject_backdated_now_as_lease_error(self):
        task, token = self.claim()
        calls = (
            lambda: aq.heartbeat_task(
                self.state,
                task["id"],
                "agent-1",
                token,
                now="2026-07-10T05:59:59Z",
            ),
            lambda: aq.complete_task(
                self.state,
                task["id"],
                "agent-1",
                token,
                summary="done",
                artifacts=[],
                now="2026-07-10T05:59:59Z",
            ),
            lambda: aq.fail_task(
                self.state,
                task["id"],
                "agent-1",
                token,
                message="failed",
                now="2026-07-10T05:59:59Z",
            ),
            lambda: aq.release_task(
                self.state,
                task["id"],
                "agent-1",
                token,
                now="2026-07-10T05:59:59Z",
            ),
        )
        for call in calls:
            with self.subTest(call=call):
                self.assert_atomic_error(
                    aq.LeaseError, "now.*earlier", call
                )

    def test_complete_sets_exact_result_event_and_detached_return(self):
        task, token = self.claim()
        returned = aq.complete_task(
            self.state,
            task["id"],
            "agent-1",
            token,
            summary="Finished",
            artifacts=["a.txt", "b.json"],
            now="2026-07-10T06:00:30Z",
        )
        stored = self.state["tasks"][task["id"]]
        self.assertEqual("completed", stored["status"])
        self.assertEqual(
            {
                "summary": "Finished",
                "artifacts": ["a.txt", "b.json"],
                "git": None,
            },
            stored["result"],
        )
        self.assertIsNone(stored["claim"])
        self.assertIsNone(stored["available_at"])
        self.assertIsNot(stored, returned)
        self.assertEqual(
            {"artifact_count": 2}, self.state["events"][-1]["details"]
        )
        self.assertNotIn(token, repr(self.state["events"][-1]))

    def test_git_complete_persists_exact_compact_evidence_and_counts(self):
        task = self.add(
            title="Git task",
            priority=1000,
            git_mode="commit",
            resources=["dir:src/"],
        )
        observation = git_snapshot(head="a" * 40)
        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="SECRET"
        ):
            claimed = aq.claim_task(
                self.state,
                "agent-1",
                now=self.NOW,
                git_observation=observation,
                task_id=task["id"],
            )
        evidence = {
            "branch": observation["branch"],
            "base": "a" * 40,
            "head": "b" * 40,
            "commit_count": 2,
            "changed_path_count": 27,
        }

        returned = aq.complete_task(
            self.state,
            task["id"],
            "agent-1",
            claimed["lease_token"],
            summary="Finished",
            artifacts=["report.txt"],
            git_evidence=evidence,
            now="2026-07-10T06:00:30Z",
        )

        self.assertEqual(evidence, returned["result"]["git"])
        self.assertEqual(
            {
                "artifact_count": 1,
                "commit_count": 2,
                "changed_path_count": 27,
            },
            self.state["events"][-1]["details"],
        )
        evidence["changed_path_count"] = 999
        self.assertEqual(27, returned["result"]["git"]["changed_path_count"])
        self.assertNotIn("changed_paths", json.dumps(returned))

    def test_complete_requires_evidence_only_for_git_tasks_atomically(self):
        generic, generic_token = self.claim(title="Generic")
        evidence = {
            "branch": "refs/heads/main",
            "base": "a" * 40,
            "head": "b" * 40,
            "commit_count": 1,
            "changed_path_count": 1,
        }
        self.assert_atomic_error(
            aq.InvariantError,
            "generic.*Git evidence",
            lambda: aq.complete_task(
                self.state,
                generic["id"],
                "agent-1",
                generic_token,
                "done",
                [],
                git_evidence=evidence,
                now=self.NOW,
            ),
        )

        git_task = self.add(
            title="Git",
            priority=2000,
            git_mode="commit",
            resources=["file:src/a.py"],
        )
        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="GIT-SECRET"
        ):
            git_claim = aq.claim_task(
                self.state,
                "git-agent",
                now=self.NOW,
                git_observation=git_snapshot(head="a" * 40),
                task_id=git_task["id"],
            )
        self.assert_atomic_error(
            aq.InvariantError,
            "Git-aware.*evidence",
            lambda: aq.complete_task(
                self.state,
                git_task["id"],
                "git-agent",
                git_claim["lease_token"],
                "done",
                [],
                now=self.NOW,
            ),
        )

    def test_complete_validates_payload_before_lease_and_is_atomic(self):
        task, _ = self.claim()
        cases = (
            ({"summary": "", "artifacts": []}, "summary"),
            ({"summary": "ok", "artifacts": "a"}, "artifacts"),
            ({"summary": "ok", "artifacts": [""]}, "artifacts"),
            ({"summary": "ok", "artifacts": [1]}, "artifacts"),
        )
        for payload, message in cases:
            with self.subTest(payload=payload):
                self.assert_atomic_error(
                    aq.InvariantError,
                    message,
                    lambda: aq.complete_task(
                        self.state,
                        task["id"],
                        "wrong-agent",
                        "wrong-token",
                        now=self.NOW,
                        **payload,
                    ),
                )

    def test_fail_retries_with_backoff_then_exhausts_or_terminates(self):
        retry_task, retry_token = self.claim(max_attempts=2)
        returned = aq.fail_task(
            self.state,
            retry_task["id"],
            "agent-1",
            retry_token,
            message="retry me",
            now=self.NOW,
        )
        self.assertEqual("pending", returned["status"])
        self.assertEqual("2026-07-10T06:00:30Z", returned["available_at"])
        self.assertEqual(
            {"message": "retry me", "at": self.NOW}, returned["last_error"]
        )
        self.assertEqual(
            {"terminal": False, "status": "pending"},
            self.state["events"][-1]["details"],
        )

        exhausted, exhausted_token = self.claim(max_attempts=1)
        final = aq.fail_task(
            self.state,
            exhausted["id"],
            "agent-1",
            exhausted_token,
            message="final",
            now=self.NOW,
        )
        self.assertEqual("failed", final["status"])
        self.assertIsNone(final["available_at"])

        terminal, terminal_token = self.claim(max_attempts=3)
        stopped = aq.fail_task(
            self.state,
            terminal["id"],
            "agent-1",
            terminal_token,
            message="stop",
            terminal=True,
            now=self.NOW,
        )
        self.assertEqual("failed", stopped["status"])
        self.assertEqual(1, stopped["attempts"])
        self.assertEqual(True, self.state["events"][-1]["details"]["terminal"])

    def test_release_returns_pending_immediately_and_retains_attempt(self):
        task, token = self.claim()
        released = aq.release_task(
            self.state, task["id"], "agent-1", token, now=self.NOW
        )
        self.assertEqual("pending", released["status"])
        self.assertEqual(self.NOW, released["available_at"])
        self.assertEqual(1, released["attempts"])
        self.assertIsNone(released["claim"])
        self.assertEqual("task.released", self.state["events"][-1]["type"])

    def test_sweep_expired_is_deterministic_retry_aware_and_secret_free(self):
        first, first_token = self.claim(max_attempts=2)
        second, second_token = self.claim(max_attempts=1)
        third, third_token = self.claim(max_attempts=2)
        self.state["tasks"][first["id"]]["claim"].update(
            {
                "claimed_at": "2026-07-10T05:59:00Z",
                "heartbeat_at": "2026-07-10T05:59:59Z",
                "expires_at": self.NOW,
            }
        )
        self.state["tasks"][second["id"]]["claim"].update(
            {
                "claimed_at": "2026-07-10T05:58:00Z",
                "heartbeat_at": "2026-07-10T05:59:58Z",
                "expires_at": "2026-07-10T05:59:59Z",
            }
        )
        self.state["tasks"][third["id"]]["claim"]["expires_at"] = "2026-07-10T06:00:01Z"
        unexpired_before = copy.deepcopy(self.state["tasks"][third["id"]])

        changed = aq.sweep_expired(self.state, now=self.NOW)

        self.assertEqual([first["id"], second["id"]], changed)
        self.assertEqual("pending", self.state["tasks"][first["id"]]["status"])
        self.assertEqual("failed", self.state["tasks"][second["id"]]["status"])
        self.assertEqual(unexpired_before, self.state["tasks"][third["id"]])
        events = self.state["events"][-2:]
        self.assertEqual([first["id"], second["id"]], [event["task_id"] for event in events])
        self.assertEqual(1, len({event["revision"] for event in events}))
        self.assertEqual(
            [
                {"status": "pending", "attempt": 1},
                {"status": "failed", "attempt": 1},
            ],
            [event["details"] for event in events],
        )
        for token in (first_token, second_token, third_token):
            self.assertNotIn(token, repr(events))

    def test_sweep_validates_and_copies_queue_once_for_many_expirations(self):
        for index in range(8):
            task = self.add(f"Expired {index}", max_attempts=2)
            task["status"] = "leased"
            task["attempts"] = 1
            task["claim"] = canonical_claim(
                agent_id=f"agent-{index}",
                lease_token=f"secret-{index}",
                claimed_at="2026-07-10T05:30:00Z",
                heartbeat_at="2026-07-10T05:59:00Z",
                expires_at=self.NOW,
            )

        real_deepcopy = copy.deepcopy
        queue_copy_count = 0

        def tracking_deepcopy(value):
            nonlocal queue_copy_count
            if isinstance(value, dict) and "schema_version" in value:
                queue_copy_count += 1
            return real_deepcopy(value)

        with mock.patch.object(
            aq, "validate_state", wraps=aq.validate_state
        ) as validate, mock.patch.object(
            aq.copy, "deepcopy", side_effect=tracking_deepcopy
        ):
            changed = aq.sweep_expired(self.state, now=self.NOW)

        self.assertEqual(8, len(changed))
        self.assertEqual(2, validate.call_count)
        self.assertEqual(1, queue_copy_count)

    def test_sweep_large_batch_preserves_id_order_and_retry_outcomes(self):
        expected_ids = []
        for index in range(24):
            task = self.add(
                f"Expired {index}", max_attempts=1 if index % 2 else 2
            )
            task["status"] = "leased"
            task["attempts"] = 1
            task["claim"] = canonical_claim(
                agent_id=f"agent-{index}",
                lease_token=f"secret-{index}",
                claimed_at="2026-07-10T05:30:00Z",
                heartbeat_at="2026-07-10T05:59:00Z",
                expires_at=self.NOW,
            )
            expected_ids.append(task["id"])

        changed = aq.sweep_expired(self.state, now=self.NOW)

        self.assertEqual(expected_ids, changed)
        events = self.state["events"]
        self.assertEqual(expected_ids, [event["task_id"] for event in events])
        self.assertEqual(list(range(1, 25)), [event["seq"] for event in events])
        self.assertEqual(1, len({event["revision"] for event in events}))
        self.assertEqual(
            ["pending" if index % 2 == 0 else "failed" for index in range(24)],
            [self.state["tasks"][task_id]["status"] for task_id in expected_ids],
        )

    def test_stale_worker_is_fenced_after_sweep_and_reclaim(self):
        task, stale_token = self.claim(max_attempts=2)
        self.state["tasks"][task["id"]]["claim"].update(
            {
                "claimed_at": "2026-07-10T05:59:00Z",
                "heartbeat_at": "2026-07-10T05:59:59Z",
                "expires_at": self.NOW,
            }
        )
        aq.sweep_expired(self.state, now=self.NOW)
        with mock.patch.object(aq.secrets, "token_urlsafe", return_value="NEW"):
            reclaimed = aq.claim_task(
                self.state,
                "agent-2",
                now="2026-07-10T06:00:30Z",
                lease_seconds=60,
            )
        self.assert_atomic_error(
            aq.LeaseError,
            "agent|token",
            lambda: aq.complete_task(
                self.state,
                task["id"],
                "agent-1",
                stale_token,
                summary="stale",
                artifacts=[],
                now="2026-07-10T06:00:31Z",
            ),
        )
        completed = aq.complete_task(
            self.state,
            task["id"],
            "agent-2",
            reclaimed["lease_token"],
            summary="fresh",
            artifacts=[],
            now="2026-07-10T06:00:31Z",
        )
        self.assertEqual("completed", completed["status"])

    def test_manual_retry_preserves_attempts_and_error_and_validates_source(self):
        task, token = self.claim(max_attempts=1)
        aq.fail_task(
            self.state, task["id"], "agent-1", token, message="failed", now=self.NOW
        )
        retried = aq.retry_task(
            self.state, task["id"], additional_attempts=2, now=self.NOW
        )
        self.assertEqual("pending", retried["status"])
        self.assertEqual(1, retried["attempts"])
        self.assertEqual(3, retried["max_attempts"])
        self.assertEqual({"message": "failed", "at": self.NOW}, retried["last_error"])
        self.assertEqual(
            {"additional_attempts": 2}, self.state["events"][-1]["details"]
        )
        for value in (0, True):
            self.assert_atomic_error(
                aq.InvariantError,
                "additional_attempts",
                lambda value=value: aq.retry_task(
                    self.state, task["id"], additional_attempts=value, now=self.NOW
                ),
            )
        self.assert_atomic_error(
            aq.InvariantError,
            "failed",
            lambda: aq.retry_task(self.state, task["id"], now=self.NOW),
        )

    def test_block_unblock_cancel_allowed_transitions_and_events(self):
        task = self.add()
        task["attempts"] = 1
        blocked = aq.block_task(self.state, task["id"], "needs input", now=self.NOW)
        self.assertEqual("blocked", blocked["status"])
        self.assertEqual(1, blocked["attempts"])
        blocked_error = {
            "message": "needs input",
            "at": self.NOW,
            "kind": "blocked",
        }
        self.assertEqual(blocked_error, blocked["last_error"])
        self.assertEqual({"reason": "needs input"}, self.state["events"][-1]["details"])
        unblocked = aq.unblock_task(self.state, task["id"], now=self.NOW)
        self.assertEqual("pending", unblocked["status"])
        self.assertEqual(blocked_error, unblocked["last_error"])
        self.assertEqual(self.NOW, unblocked["available_at"])
        cancelled = aq.cancel_task(self.state, task["id"], now=self.NOW)
        self.assertEqual("cancelled", cancelled["status"])
        self.assertIsNone(cancelled["available_at"])
        self.assertEqual(
            ["task.blocked", "task.unblocked", "task.cancelled"],
            [event["type"] for event in self.state["events"][-3:]],
        )

    def test_admin_disallowed_transitions_and_invalid_reasons_are_atomic(self):
        task = self.add()
        self.assert_atomic_error(
            aq.InvariantError,
            "reason",
            lambda: aq.block_task(self.state, task["id"], "", now=self.NOW),
        )
        aq.block_task(self.state, task["id"], "wait", now=self.NOW)
        self.assert_atomic_error(
            aq.InvariantError,
            "pending",
            lambda: aq.block_task(self.state, task["id"], "again", now=self.NOW),
        )
        aq.unblock_task(self.state, task["id"], now=self.NOW)
        self.assert_atomic_error(
            aq.InvariantError,
            "blocked",
            lambda: aq.unblock_task(self.state, task["id"], now=self.NOW),
        )
        task2, token = self.claim()
        self.assert_atomic_error(
            aq.InvariantError,
            "cannot cancel|status",
            lambda: aq.cancel_task(self.state, task2["id"], now=self.NOW),
        )
        aq.complete_task(
            self.state,
            task2["id"],
            "agent-1",
            token,
            summary="done",
            artifacts=[],
            now=self.NOW,
        )
        self.assert_atomic_error(
            aq.InvariantError,
            "cannot cancel|status",
            lambda: aq.cancel_task(self.state, task2["id"], now=self.NOW),
        )

    def test_admin_transitions_reject_backdated_now_atomically(self):
        backdated = "2026-07-10T06:00:29Z"
        future = "2026-07-10T06:00:30Z"

        retry_task, retry_token = self.claim(max_attempts=1)
        aq.fail_task(
            self.state,
            retry_task["id"],
            "agent-1",
            retry_token,
            message="failed",
            now=self.NOW,
        )
        self.state["tasks"][retry_task["id"]]["updated_at"] = future
        self.assert_atomic_error(
            aq.InvariantError,
            "now.*updated_at",
            lambda: aq.retry_task(
                self.state, retry_task["id"], now=backdated
            ),
        )

        self.state = aq.new_state("demo", aq.fixed_config())
        pending = self.add()
        pending["updated_at"] = future
        self.assert_atomic_error(
            aq.InvariantError,
            "now.*updated_at",
            lambda: aq.block_task(
                self.state, pending["id"], "wait", now=backdated
            ),
        )
        self.assert_atomic_error(
            aq.InvariantError,
            "now.*updated_at",
            lambda: aq.cancel_task(self.state, pending["id"], now=backdated),
        )

        pending["updated_at"] = self.NOW
        aq.block_task(self.state, pending["id"], "wait", now=self.NOW)
        self.state["tasks"][pending["id"]]["updated_at"] = future
        self.assert_atomic_error(
            aq.InvariantError,
            "now.*updated_at",
            lambda: aq.unblock_task(
                self.state, pending["id"], now=backdated
            ),
        )

    def test_sweep_rejects_expired_task_updated_after_now_atomically(self):
        task, _ = self.claim(max_attempts=2)
        stored = self.state["tasks"][task["id"]]
        stored["claim"].update(
            {
                "claimed_at": "2026-07-10T05:59:00Z",
                "heartbeat_at": "2026-07-10T05:59:59Z",
                "expires_at": self.NOW,
            }
        )
        stored["updated_at"] = "2026-07-10T06:00:01Z"

        self.assert_atomic_error(
            aq.InvariantError,
            "now.*updated_at",
            lambda: aq.sweep_expired(self.state, now=self.NOW),
        )

    def test_every_transition_rejects_corrupt_state_without_mutation(self):
        task, token = self.claim()
        self.state["config"]["default_lease_seconds"] = 0
        calls = (
            lambda: aq.heartbeat_task(self.state, task["id"], "agent-1", token, now=self.NOW),
            lambda: aq.complete_task(self.state, task["id"], "agent-1", token, summary="x", artifacts=[], now=self.NOW),
            lambda: aq.fail_task(self.state, task["id"], "agent-1", token, message="x", now=self.NOW),
            lambda: aq.release_task(self.state, task["id"], "agent-1", token, now=self.NOW),
            lambda: aq.sweep_expired(self.state, now=self.NOW),
            lambda: aq.retry_task(self.state, task["id"], now=self.NOW),
            lambda: aq.block_task(self.state, task["id"], "x", now=self.NOW),
            lambda: aq.unblock_task(self.state, task["id"], now=self.NOW),
            lambda: aq.cancel_task(self.state, task["id"], now=self.NOW),
        )
        for call in calls:
            with self.subTest(call=call):
                self.assert_atomic_error(aq.InvariantError, "config", call)

    def test_status_and_tsv_follow_transitions_without_token(self):
        task, token = self.claim()
        aq.release_task(self.state, task["id"], "agent-1", token, now=self.NOW)
        rows = aq.status_rows(self.state, self.NOW)
        tsv = aq.render_tsv(self.state, self.NOW)
        self.assertEqual("ready", rows[0]["state"])
        self.assertEqual("1/3", rows[0]["attempts"])
        self.assertNotIn(token, repr(rows) + tsv)


class GitRecoveryTests(unittest.TestCase):
    CREATED = "2026-07-10T05:00:00Z"
    NOW = "2026-07-10T06:00:00Z"
    EXPIRES = "2026-07-10T06:01:00Z"
    LATER = "2026-07-10T06:01:31Z"

    def setUp(self):
        self.clock = mock.patch.object(aq, "utc_now", return_value=self.CREATED)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.state = aq.new_state("demo", aq.fixed_config())
        self.observation = git_snapshot(head="a" * 40)

    def add_git(self, max_attempts=2):
        created = aq.add_task(
            self.state,
            {
                "title": "Git task",
                "git_mode": "commit",
                "resources": ["dir:src/"],
                "max_attempts": max_attempts,
            },
        )
        return self.state["tasks"][created["id"]]

    def claim_git(self, task, agent="worker", now=None, **extra):
        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value=f"SECRET-{agent}"
        ):
            return aq.claim_task(
                self.state,
                agent,
                now=now or self.NOW,
                lease_seconds=60,
                git_observation=self.observation,
                task_id=task["id"],
                **extra,
            )

    def expire(self, task):
        self.claim_git(task)
        aq.sweep_expired(self.state, now=self.EXPIRES)
        return self.state["tasks"][task["id"]]

    def test_expired_git_lease_preserves_private_recovery_binding(self):
        task = self.add_git(max_attempts=2)

        stored = self.expire(task)

        self.assertEqual("pending", stored["status"])
        self.assertIsNone(stored["claim"])
        self.assertEqual(
            self.observation["head"], stored["git_recovery"]["base"]
        )
        self.assertEqual(
            "git_recovery",
            aq.derive_state(self.state, stored, self.LATER),
        )
        safe = json.dumps(aq._safe_task(stored))
        self.assertNotIn("/private/repository", safe)
        self.assertNotIn("/private/worktrees", safe)

    def test_exhausted_git_attempt_keeps_recovery_across_retry(self):
        task = self.add_git(max_attempts=1)
        stored = self.expire(task)
        self.assertEqual("failed", stored["status"])
        recovery = copy.deepcopy(stored["git_recovery"])
        self.assertIsNotNone(recovery)

        aq.retry_task(self.state, task["id"], now=self.LATER)

        self.assertEqual(
            recovery, self.state["tasks"][task["id"]]["git_recovery"]
        )

    def test_retryable_failure_preserves_recovery(self):
        task = self.add_git(max_attempts=2)
        claimed = self.claim_git(task)

        failed = aq.fail_task(
            self.state,
            task["id"],
            "worker",
            claimed["lease_token"],
            "retry",
            now="2026-07-10T06:00:30Z",
        )

        self.assertEqual("pending", failed["status"])
        self.assertEqual(
            self.observation["head"], failed["git_recovery"]["base"]
        )

    def test_ordinary_claim_skips_recovery_and_explicit_resume_keeps_base(self):
        task = self.add_git(max_attempts=3)
        stored = self.expire(task)
        recovery = copy.deepcopy(stored["git_recovery"])
        with self.assertRaises(aq.NoTaskAvailable):
            self.claim_git(task, agent="ordinary", now=self.LATER)

        context = {"binding": recovery, "head": self.observation["head"]}
        resumed = self.claim_git(
            task,
            agent="resumer",
            now=self.LATER,
            resume_git=True,
            git_resume=context,
        )

        self.assertEqual(recovery, resumed["task"]["claim"]["git"])
        self.assertIsNone(resumed["task"]["git_recovery"])
        self.assertEqual(2, resumed["task"]["attempts"])
        self.assertTrue(self.state["events"][-1]["details"]["resumed_git"])

    def test_invalid_resume_context_is_atomic(self):
        task = self.add_git(max_attempts=3)
        stored = self.expire(task)
        before = copy.deepcopy(self.state)
        invalid = {
            "binding": copy.deepcopy(stored["git_recovery"]),
            "head": "b" * 40,
        }

        with self.assertRaisesRegex(aq.InvariantError, "recovery"):
            self.claim_git(
                task,
                agent="resumer",
                now=self.LATER,
                resume_git=True,
                git_resume=invalid,
            )

        self.assertEqual(before, self.state)

    def test_cancel_clears_recovery_binding(self):
        task = self.add_git(max_attempts=2)
        self.expire(task)

        cancelled = aq.cancel_task(self.state, task["id"], now=self.LATER)

        self.assertEqual("cancelled", cancelled["status"])
        self.assertIsNone(cancelled["git_recovery"])


class StatusProjectionTests(unittest.TestCase):
    NOW = "2026-07-10T06:00:00Z"

    def setUp(self):
        self.state = aq.new_state("demo", aq.fixed_config())

    def add(self, title, **fields):
        created = aq.add_task(self.state, {"title": title, **fields})
        return self.state["tasks"][created["id"]]

    def test_completed_dependency_makes_pending_task_ready(self):
        dependency = self.add("Dependency")
        dependency["status"] = "completed"
        dependency["result"] = canonical_result()
        task = self.add("Ready", depends_on=[dependency["id"]])

        self.assertEqual([], aq.dependency_blockers(self.state, task))
        self.assertEqual("ready", aq.derive_state(self.state, task, self.NOW))

    def test_unfinished_dependencies_are_ordered_blockers(self):
        first = self.add("First")
        completed = self.add("Completed")
        completed["status"] = "completed"
        completed["result"] = canonical_result()
        third = self.add("Third")
        third["status"] = "leased"
        third["attempts"] = 1
        third["claim"] = canonical_claim()
        task = self.add(
            "Waiting",
            depends_on=[third["id"], completed["id"], first["id"]],
        )

        self.assertEqual(
            [third["id"], first["id"]],
            aq.dependency_blockers(self.state, task),
        )
        self.assertEqual(
            "waiting_dependency", aq.derive_state(self.state, task, self.NOW)
        )

    def test_dependency_failure_precedes_retry_and_resource_conflict(self):
        failed = self.add("Failed dependency")
        failed["status"] = "failed"
        failed["last_error"] = {"message": "failed", "at": self.NOW}
        leased = self.add("Lease holder", resources=["repo"])
        leased["status"] = "leased"
        leased["attempts"] = 1
        leased["claim"] = canonical_claim()
        task = self.add(
            "Blocked",
            depends_on=[failed["id"]],
            resources=["repo"],
        )
        task["available_at"] = "2026-07-10T07:00:00Z"

        self.assertEqual(
            "dependency_failed", aq.derive_state(self.state, task, self.NOW)
        )
        row = next(
            row
            for row in aq.status_rows(self.state, self.NOW)
            if row["id"] == task["id"]
        )
        self.assertEqual(failed["id"], row["blocked_by"])

    def test_future_availability_waits_after_dependencies_complete(self):
        dependency = self.add("Dependency")
        dependency["status"] = "completed"
        dependency["result"] = canonical_result()
        task = self.add("Retry", depends_on=[dependency["id"]])
        task["available_at"] = "2026-07-10T06:00:01Z"

        self.assertEqual(
            "waiting_retry", aq.derive_state(self.state, task, self.NOW)
        )

    def test_resource_conflict_lists_active_leases_and_excludes_self(self):
        first = self.add("First lease", resources=["repo", "db"])
        first["status"] = "leased"
        first["attempts"] = 1
        first["claim"] = canonical_claim()
        second = self.add("Second lease", resources=["repo"])
        second["status"] = "leased"
        second["attempts"] = 1
        second["claim"] = canonical_claim(
            claimed_at="2026-07-10T04:00:00Z",
            heartbeat_at="2026-07-10T05:00:00Z",
            expires_at="2026-07-10T05:59:59Z",
        )
        task = self.add("Contender", resources=["repo"])

        self.assertEqual(
            {"repo": [first["id"]], "db": [first["id"]]},
            aq.leased_resources(self.state, now=self.NOW),
        )
        self.assertEqual(
            {},
            aq.leased_resources(
                self.state, excluding=first["id"], now=self.NOW
            ),
        )
        self.assertEqual(
            "resource_conflict", aq.derive_state(self.state, task, self.NOW)
        )
        row = next(
            row
            for row in aq.status_rows(self.state, self.NOW)
            if row["id"] == task["id"]
        )
        self.assertEqual(first["id"], row["blocked_by"])

    def test_nonpending_stored_statuses_display_unchanged(self):
        for status in (
            "completed",
            "failed",
            "blocked",
            "cancelled",
            "leased",
        ):
            with self.subTest(status=status):
                task = self.add(status)
                task["status"] = status
                if status == "completed":
                    task["result"] = canonical_result()
                elif status == "failed":
                    task["last_error"] = {
                        "message": "failed",
                        "at": self.NOW,
                    }
                elif status == "blocked":
                    task["last_error"] = {
                        "message": "blocked",
                        "at": self.NOW,
                        "kind": "blocked",
                    }
                elif status == "leased":
                    task["attempts"] = 1
                    task["claim"] = canonical_claim()
                self.assertEqual(
                    status, aq.derive_state(self.state, task, self.NOW)
                )

    def test_status_rows_sort_by_task_id_not_priority(self):
        first = self.add("First", priority=-10)
        second = self.add("Second", priority=100)
        third = self.add("Third", priority=0)

        self.assertEqual(
            [first["id"], second["id"], third["id"]],
            [row["id"] for row in aq.status_rows(self.state, self.NOW)],
        )

    def test_status_rows_builds_active_lease_resource_map_once(self):
        holder = self.add("Holder", resources=["repo"])
        holder["status"] = "leased"
        holder["attempts"] = 1
        holder["claim"] = canonical_claim()
        contenders = [
            self.add(f"Contender {number}", resources=["repo"])
            for number in range(5)
        ]

        with mock.patch.object(
            aq, "leased_resources", wraps=aq.leased_resources
        ) as build_resource_map:
            rows = aq.status_rows(self.state, self.NOW)

        self.assertEqual(1, build_resource_map.call_count)
        rows_by_id = {row["id"]: row for row in rows}
        for contender in contenders:
            self.assertEqual(
                "resource_conflict", rows_by_id[contender["id"]]["state"]
            )
            self.assertEqual(
                holder["id"], rows_by_id[contender["id"]]["blocked_by"]
            )

    def test_status_rows_applies_cheap_filters_before_deriving_state(self):
        target = self.add("Target", workflow_id="W-000003")
        for number in range(4):
            self.add(f"Filtered {number}", workflow_id="W-000004")

        with mock.patch.object(
            aq,
            "_derive_state_with_resources",
            create=True,
            return_value="ready",
        ) as derive:
            rows = aq.status_rows(
                self.state, self.NOW, workflow="W-000003"
            )

        self.assertEqual([target["id"]], [row["id"] for row in rows])
        self.assertEqual(1, derive.call_count)

    def test_status_filters_select_the_same_task(self):
        target = self.add(
            "Target",
            workflow_id="W-000003",
            role="reviewer",
            labels=["queue", "python", "safe"],
        )
        target["status"] = "leased"
        target["attempts"] = 1
        target["claim"] = canonical_claim(agent_id="agent-7")
        other = self.add(
            "Other",
            workflow_id="W-000004",
            role="builder",
            labels=["queue"],
        )
        other["available_at"] = "2026-07-10T07:00:00Z"
        expected = [target["id"]]

        filters = (
            {"workflow": "W-000003"},
            {"assignee": "agent-7"},
            {"role": "reviewer"},
            {"labels": ["queue", "python"]},
            {"state_filter": "leased"},
        )
        for selected_filter in filters:
            with self.subTest(selected_filter=selected_filter):
                self.assertEqual(
                    expected,
                    [
                        row["id"]
                        for row in aq.status_rows(
                            self.state, self.NOW, **selected_filter
                        )
                    ],
                )

    def test_tsv_has_exact_header_values_and_one_physical_line_per_task(self):
        dependency = self.add("Dependency", resources=["file:dep"])
        dependency["status"] = "leased"
        dependency["attempts"] = 1
        dependency["claim"] = canonical_claim(agent_id="dep-agent")
        task = self.add(
            "Back\\slash\tLine\r\nNext",
            workflow_id="W-000003",
            role="review",
            priority=4,
            depends_on=[dependency["id"]],
            resources=["file:code"],
            max_attempts=5,
        )
        task["attempts"] = 2
        self.state["revision"] = 9
        expected = (
            "# queue_revision: 9\n"
            "id\tworkflow\trole\tstate\tpriority\tassignee\tlease_until\t"
            "attempts\tdepends_on\tblocked_by\tresources\ttitle\n"
            "T-000001\t\t\tleased\t0\tdep-agent\t2026-07-10T07:00:00Z\t"
            "1/3\t\t\tfile:dep\tDependency\n"
            "T-000002\tW-000003\treview\twaiting_dependency\t4\t\t"
            "\t2/5\tT-000001\tT-000001\tfile:code\t"
            "Back\\\\slash\\tLine\\r\\nNext\n"
        )

        rendered = aq.render_tsv(self.state, self.NOW)

        self.assertEqual(expected, rendered)
        self.assertEqual(4, len(rendered.splitlines()))
        rows = aq.status_rows(self.state, self.NOW)
        table = aq.format_terminal_table(rows)
        self.assertIn("lease_until", table)
        self.assertIn("T-000002", table)

    def test_status_projection_redacts_tokens_results_and_errors(self):
        task = self.add("Secret-bearing task")
        task["status"] = "leased"
        task["attempts"] = 1
        task["claim"] = canonical_claim(lease_token="TOP-SECRET-TOKEN")
        task["last_error"] = {
            "message": "ERROR-SECRET",
            "at": self.NOW,
        }
        completed = self.add("Completed secret")
        completed["status"] = "completed"
        completed["result"] = {
            "summary": "RESULT-SECRET",
            "artifacts": [],
            "git": None,
        }

        rows = aq.status_rows(self.state, self.NOW)
        rendered = aq.render_tsv(self.state, self.NOW)

        self.assertEqual(
            {
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
            },
            set(rows[0]),
        )
        for secret in (
            "lease_token",
            "TOP-SECRET-TOKEN",
            "arbitrary_result",
            "RESULT-SECRET",
            "arbitrary_error",
            "ERROR-SECRET",
        ):
            with self.subTest(secret=secret):
                self.assertNotIn(secret, repr(rows))
                self.assertNotIn(secret, rendered)

    def test_empty_render_matches_existing_empty_projection_bytes(self):
        self.state["revision"] = 7

        self.assertEqual(
            aq.render_empty_tsv(7), aq.render_tsv(self.state, self.NOW)
        )

    def test_empty_terminal_table_still_has_headings_without_ansi(self):
        table = aq.format_terminal_table([])

        self.assertIn("id", table)
        self.assertIn("lease_until", table)
        self.assertNotIn("\x1b", table)

    def test_terminal_table_escapes_user_control_codes(self):
        task = self.add("\x1b[31mred\x1b[0m\x9bunsafe")
        rows = aq.status_rows(self.state, self.NOW)

        table = aq.format_terminal_table(rows)

        self.assertNotIn("\x1b", table)
        self.assertNotIn("\x9b", table)
        self.assertIn("\\u001B[31mred\\u001B[0m\\u009Bunsafe", table)
        self.assertEqual(task["id"], rows[0]["id"])

    def test_shared_display_escaping_covers_separators_and_bidi_controls(self):
        controls = (
            ("\v", "\\u000B"),
            ("\f", "\\u000C"),
            ("\x1c", "\\u001C"),
            ("\x85", "\\u0085"),
            ("\u2028", "\\u2028"),
            ("\u2029", "\\u2029"),
            ("\x1b", "\\u001B"),
            ("\x9b", "\\u009B"),
            ("\u061c", "\\u061C"),
            ("\u200e", "\\u200E"),
            ("\u200f", "\\u200F"),
            ("\u202a", "\\u202A"),
            ("\u202b", "\\u202B"),
            ("\u202c", "\\u202C"),
            ("\u202d", "\\u202D"),
            ("\u202e", "\\u202E"),
            ("\u2066", "\\u2066"),
            ("\u2067", "\\u2067"),
            ("\u2068", "\\u2068"),
            ("\u2069", "\\u2069"),
        )
        raw = "slash\\\t\r\n" + "".join(
            character for character, _ in controls
        )
        expected = "slash\\\\\\t\\r\\n" + "".join(
            escaped for _, escaped in controls
        )
        task = self.add(raw)

        rendered = aq.render_tsv(self.state, self.NOW)
        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )

        self.assertEqual(expected, aq.escape_tsv(raw))
        self.assertIn(expected, rendered)
        self.assertIn(expected, table)
        self.assertEqual(3, len(rendered.splitlines()))
        for character, _ in controls:
            with self.subTest(codepoint=f"U+{ord(character):04X}"):
                self.assertNotIn(character, rendered)
                self.assertNotIn(character, table)
        self.assertEqual(task["id"], self.state["tasks"][task["id"]]["id"])

    def test_terminal_table_aligns_wide_emoji_and_combining_text(self):
        self.add("Korean", role="검토")
        self.add("Emoji", role="🙂")
        self.add("Combining", role="e\u0301")

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        lines = table.splitlines()
        state_offsets = [
            aq.display_width(line[: line.index("ready")])
            for line in lines[2:]
        ]

        self.assertEqual([state_offsets[0]] * 3, state_offsets)
        self.assertEqual(2, aq.display_width("한"))
        self.assertEqual(2, aq.display_width("🙂"))
        self.assertEqual(1, aq.display_width("e\u0301"))
        self.assertEqual(0, aq.display_width("\x1b"))

    def test_display_width_treats_common_emoji_sequences_as_one_cluster(self):
        cases = (
            ("👍🏽", 2),
            ("1️⃣", 2),
            ("👩‍💻", 2),
            ("🇰🇷", 2),
        )

        for value, expected_width in cases:
            with self.subTest(value=value):
                self.assertEqual(expected_width, aq.display_width(value))

    def test_terminal_table_places_column_after_zwj_emoji_at_literal_cell(self):
        self.add("ZWJ row", role="👩‍💻")

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        row = table.splitlines()[2]
        prefix_before_state = row[: row.index("ready")]
        literal_cell_position = len(
            prefix_before_state.replace("👩‍💻", "XX")
        )

        self.assertEqual(26, literal_cell_position)
        self.assertTrue(prefix_before_state.endswith("👩‍💻    "))

    def test_display_width_promotes_vs16_emoji_presentation(self):
        cases = (
            ("❤️", 2),
            ("✈️", 2),
            ("☺️", 2),
            ("☀️", 2),
            ("❤︎", 1),
        )

        for value, expected_width in cases:
            with self.subTest(value=value):
                self.assertEqual(expected_width, aq.display_width(value))

    def test_terminal_table_places_column_after_vs16_emoji_at_literal_cell(self):
        self.add("VS16 row", role="❤️")

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        row = table.splitlines()[2]
        prefix_before_state = row[: row.index("ready")]
        literal_cell_position = len(prefix_before_state.replace("❤️", "XX"))

        self.assertEqual(26, literal_cell_position)
        self.assertTrue(prefix_before_state.endswith("❤️    "))

    def test_display_width_normalizes_and_limits_emoji_joining(self):
        cases = (
            ("가", 2),
            ("A\u200dB", 2),
            ("A\ufe0f", 1),
        )

        for value, expected_width in cases:
            with self.subTest(value=value):
                self.assertEqual(expected_width, aq.display_width(value))

    def test_terminal_table_places_columns_after_literal_cluster_widths(self):
        roles = ("가", "A\u200dB", "A\ufe0f")
        replacements = (("가", "XX"), ("A\u200dB", "XX"), ("A\ufe0f", "X"))
        expected_suffixes = ("가    ", "A\u200dB    ", "A\ufe0f     ")
        for number, role in enumerate(roles):
            self.add(f"Literal row {number}", role=role)

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        prefixes = [
            row[: row.index("ready")] for row in table.splitlines()[2:]
        ]

        for prefix, replacement, expected_suffix in zip(
            prefixes, replacements, expected_suffixes
        ):
            with self.subTest(expected_suffix=expected_suffix):
                literal_cell_position = len(
                    prefix.replace(replacement[0], replacement[1])
                )
                self.assertEqual(26, literal_cell_position)
                self.assertTrue(prefix.endswith(expected_suffix))

    def test_display_width_promotes_standard_bmp_emoji_vs_bases(self):
        cases = (
            ("©️", 2),
            ("®️", 2),
            ("™️", 2),
            ("↔️", 2),
            ("▶️", 2),
            ("◼️", 2),
            ("Ⓜ️", 2),
            ("★️", 1),
        )

        for value, expected_width in cases:
            with self.subTest(value=value):
                self.assertEqual(expected_width, aq.display_width(value))

    def test_terminal_table_places_column_after_bmp_emoji_at_literal_cell(self):
        self.add("BMP VS row", role="©️")

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        row = table.splitlines()[2]
        prefix_before_state = row[: row.index("ready")]
        literal_cell_position = len(prefix_before_state.replace("©️", "XX"))

        self.assertEqual(26, literal_cell_position)
        self.assertTrue(prefix_before_state.endswith("©️    "))

    def test_display_width_does_not_join_variation_only_bases(self):
        cases = (
            ("1\u200d2", 2),
            ("©\u200d®", 2),
            ("↔\u200d↕", 2),
            ("👨‍👩‍👧‍👦", 2),
            ("🏳️‍🌈", 2),
        )

        for value, expected_width in cases:
            with self.subTest(value=value):
                self.assertEqual(expected_width, aq.display_width(value))

    def test_terminal_table_keeps_variation_only_join_bases_separate(self):
        roles = ("1\u200d2", "©\u200d®", "↔\u200d↕")
        for number, role in enumerate(roles):
            self.add(f"Separate ZWJ row {number}", role=role)

        table = aq.format_terminal_table(
            aq.status_rows(self.state, self.NOW)
        )
        prefixes = [
            row[: row.index("ready")] for row in table.splitlines()[2:]
        ]

        for prefix, role in zip(prefixes, roles):
            with self.subTest(role=role):
                literal_cell_position = len(prefix.replace(role, "XX"))
                self.assertEqual(26, literal_cell_position)
                self.assertTrue(prefix.endswith(f"{role}    "))

    def test_validation_rejects_invalid_available_at_and_visible_claim_fields(self):
        task = self.add("Stored")
        cases = (
            ("available_at", "2026-07-10T06:00:00+00:00", "available_at"),
            ("claim", {"agent_id": ""}, "claim"),
            ("claim", {"agent_id": 7}, "claim"),
            ("claim", {"expires_at": "tomorrow"}, "claim"),
            ("claim", {"expires_at": None}, "claim"),
        )

        for field, value, message in cases:
            with self.subTest(field=field, value=value):
                candidate = json.loads(json.dumps(self.state))
                candidate["tasks"][task["id"]][field] = value
                with self.assertRaisesRegex(aq.InvariantError, message):
                    aq.validate_state(candidate)


class QueuePersistenceTests(unittest.TestCase):
    NOW = "2099-07-11T01:00:00Z"

    def test_resolve_queue_path_precedence_git_file_directory_and_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            explicit = root / "explicit.json"
            environment = {"AGENT_QUEUE_PATH": str(root / "environment.json")}
            nested = root / "repo" / "packages" / "app"
            nested.mkdir(parents=True)
            (root / "repo" / ".git").write_text("gitdir: elsewhere\n")

            self.assertEqual(
                explicit.absolute(),
                aq.resolve_queue_path(explicit, environment, nested),
            )
            self.assertEqual(
                (root / "environment.json").absolute(),
                aq.resolve_queue_path(None, environment, nested),
            )
            self.assertEqual(
                (root / "repo" / ".agent-queue" / "queue.json").absolute(),
                aq.resolve_queue_path(None, {}, nested),
            )

            (root / "repo" / ".git").unlink()
            (root / "repo" / ".git").mkdir()
            self.assertEqual(
                (root / "repo" / ".agent-queue" / "queue.json").absolute(),
                aq.resolve_queue_path(None, {}, nested),
            )
            (root / "repo" / ".git").rmdir()
            self.assertEqual(
                (nested / ".agent-queue" / "queue.json").absolute(),
                aq.resolve_queue_path(None, {}, nested),
            )

            for value in ("", "  "):
                with self.subTest(value=value):
                    with self.assertRaises(aq.QueueError):
                        aq.resolve_queue_path(value, {}, nested)
                    with self.assertRaises(aq.QueueError):
                        aq.resolve_queue_path(
                            None, {"AGENT_QUEUE_PATH": value}, nested
                        )

    def test_load_state_rejects_constants_schema_graph_and_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            with self.assertRaises(aq.QueueError):
                aq.load_state(path)

            for text in ('{"x": NaN}', '{"x": Infinity}', "not-json"):
                path.write_text(text, encoding="utf-8")
                with self.subTest(text=text):
                    with self.assertRaises(aq.InvariantError):
                        aq.load_state(path)

            path.write_bytes(b"\xff")
            with self.assertRaises(aq.InvariantError):
                aq.load_state(path)

            state = aq.new_state("demo", aq.fixed_config())
            state["schema_version"] = 9
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaises(aq.InvariantError):
                aq.load_state(path)

            future_event = aq.new_state("demo", aq.fixed_config())
            task = aq.add_task(future_event, {"title": "work"})
            aq.append_event(
                future_event, "task.added", "operator", task["id"], {},
                "2099-07-11T01:00:00Z",
            )
            path.write_text(json.dumps(future_event), encoding="utf-8")
            with self.assertRaisesRegex(aq.InvariantError, "future revision"):
                aq.load_state(path)

    def test_commit_state_increments_once_normalizes_events_and_writes_json_first(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            state = aq.new_state("demo", aq.fixed_config())
            aq.add_task(state, {"title": "work"})
            aq.append_event(
                state, "task.added", "operator", "T-000001", {}, self.NOW
            )
            writes = []
            real_atomic_write = aq.atomic_write_text

            def recording_write(target, text):
                writes.append(Path(target).suffix)
                return real_atomic_write(target, text)

            with mock.patch.object(aq, "atomic_write_text", recording_write):
                committed = aq.commit_state(path, state, self.NOW)

            self.assertEqual(1, committed["revision"])
            self.assertEqual([1], [event["revision"] for event in committed["events"]])
            self.assertEqual([".json", ".tsv"], writes)
            self.assertEqual(1, aq.tsv_revision(path.with_suffix(".tsv")))

    def test_mutate_callback_failure_after_sweep_preserves_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            state = aq.new_state("demo", aq.fixed_config())
            task = aq.add_task(state, {"title": "work"})
            aq.claim_task(state, "agent", now="2099-07-11T00:00:00Z", lease_seconds=1)
            aq.commit_state(path, state, "2099-07-11T00:00:00Z")
            before_json = path.read_bytes()
            before_tsv = path.with_suffix(".tsv").read_bytes()

            def fail(_state):
                raise RuntimeError("callback failed")

            with self.assertRaisesRegex(RuntimeError, "callback failed"):
                aq.mutate_queue(path, fail, now=self.NOW)

            self.assertEqual(before_json, path.read_bytes())
            self.assertEqual(before_tsv, path.with_suffix(".tsv").read_bytes())
            self.assertEqual(task["id"], "T-000001")

    def test_mutate_sweep_and_callback_share_one_revision_and_noop_repairs_tsv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            state = aq.new_state("demo", aq.fixed_config())
            aq.add_task(state, {"title": "expired"})
            aq.add_task(state, {"title": "block me"})
            aq.claim_task(state, "agent", now="2099-07-11T00:00:00Z", lease_seconds=1)
            aq.commit_state(path, state, "2099-07-11T00:00:00Z")

            result = aq.mutate_queue(
                path,
                lambda candidate: aq.block_task(
                    candidate, "T-000002", "pause", now=self.NOW
                ),
                now=self.NOW,
            )
            committed = aq.load_state(path)
            self.assertEqual("blocked", result["status"])
            self.assertEqual(2, committed["revision"])
            self.assertEqual(
                [2, 2], [event["revision"] for event in committed["events"][-2:]]
            )

            path.with_suffix(".tsv").unlink()
            unchanged = path.read_bytes()
            aq.mutate_queue(path, lambda _candidate: None, now=self.NOW)
            self.assertEqual(unchanged, path.read_bytes())
            self.assertEqual(2, aq.tsv_revision(path.with_suffix(".tsv")))


class TextLimitTests(unittest.TestCase):
    def setUp(self):
        self.state = aq.new_state("demo", aq.fixed_config())

    def test_utf8_text_limit_accepts_exact_bytes_and_rejects_one_byte_over(self):
        exact = "가" * (aq.MAX_TEXT_BYTES // 3) + "x"
        over = exact + "x"
        self.assertEqual(aq.MAX_TEXT_BYTES, len(exact.encode("utf-8")))

        task = aq.add_task(self.state, {"title": "boundary", "description": exact})
        self.assertEqual(exact, task["description"])
        before = copy.deepcopy(self.state)
        with self.assertRaisesRegex(aq.InvariantError, "description.*16384 UTF-8 bytes"):
            aq.add_task(self.state, {"title": "over", "description": over})
        self.assertEqual(before, self.state)

    def test_summary_error_and_reason_enforce_utf8_bytes_atomically(self):
        exact = "한" * (aq.MAX_TEXT_BYTES // 3) + "x"
        over = exact + "x"
        for field, operation in (
            ("summary", lambda state, value: aq.complete_task(
                state, "T-000001", "agent", "token", value, [],
                now="2026-07-11T00:00:01Z")),
            ("message", lambda state, value: aq.fail_task(
                state, "T-000001", "agent", "token", value, terminal=True,
                now="2026-07-11T00:00:01Z")),
        ):
            with self.subTest(field=field):
                state = aq.new_state("demo", aq.fixed_config())
                aq.add_task(state, {"title": field})
                task = state["tasks"]["T-000001"]
                task["created_at"] = task["updated_at"] = "2026-07-11T00:00:00Z"
                task["status"] = "leased"
                task["attempts"] = 1
                task["claim"] = canonical_claim(
                    agent_id="agent",
                    lease_token="token",
                    claimed_at="2026-07-11T00:00:00Z",
                    heartbeat_at="2026-07-11T00:00:00Z",
                    expires_at="2026-07-11T01:00:00Z",
                )
                operation(state, exact)
                self.assertEqual(
                    exact,
                    state["tasks"]["T-000001"][
                        "result" if field == "summary" else "last_error"
                    ]["summary" if field == "summary" else "message"],
                )

                state = aq.new_state("demo", aq.fixed_config())
                aq.add_task(state, {"title": field})
                task = state["tasks"]["T-000001"]
                task["created_at"] = task["updated_at"] = "2026-07-11T00:00:00Z"
                task["status"] = "leased"
                task["attempts"] = 1
                task["claim"] = canonical_claim(
                    agent_id="agent",
                    lease_token="token",
                    claimed_at="2026-07-11T00:00:00Z",
                    heartbeat_at="2026-07-11T00:00:00Z",
                    expires_at="2026-07-11T01:00:00Z",
                )
                before = copy.deepcopy(state)
                with self.assertRaisesRegex(aq.InvariantError, "16384 UTF-8 bytes"):
                    operation(state, over)
                self.assertEqual(before, state)

        state = aq.new_state("demo", aq.fixed_config())
        aq.add_task(state, {"title": "block"})
        before = copy.deepcopy(state)
        with self.assertRaisesRegex(aq.InvariantError, "reason.*16384 UTF-8 bytes"):
            aq.block_task(state, "T-000001", over)
        self.assertEqual(before, state)
        aq.block_task(state, "T-000001", exact)
        self.assertEqual(exact, state["tasks"]["T-000001"]["last_error"]["message"])

    def test_persisted_oversize_text_is_corruption(self):
        for field in ("description", "summary", "error", "reason"):
            with self.subTest(field=field):
                state = aq.new_state("demo", aq.fixed_config())
                created = aq.add_task(state, {"title": "stored"})
                task = state["tasks"][created["id"]]
                oversized = "x" * (aq.MAX_TEXT_BYTES + 1)
                if field == "description":
                    task["description"] = oversized
                elif field == "summary":
                    task["status"] = "completed"
                    task["result"] = canonical_result(summary=oversized)
                else:
                    task["status"] = "failed" if field == "error" else "blocked"
                    task["last_error"] = {
                        "message": oversized,
                        "at": task["updated_at"],
                        **({"kind": "blocked"} if field == "reason" else {}),
                    }
                with self.assertRaisesRegex(
                    aq.InvariantError, "16384 UTF-8 bytes"
                ):
                    aq.validate_state(state)

    def test_lone_surrogate_runtime_inputs_are_code_two_and_atomic(self):
        lone_surrogate = "\ud800"

        def invoke(queue, *arguments):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = aq.main(["--queue", str(queue), *arguments])
            return code, stdout.getvalue(), stderr.getvalue()

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            description_queue = directory / "description.json"
            aq.initialize_queue(description_queue, "demo", aq.fixed_config())
            raw_path = directory / "surrogate.json"
            raw_path.write_text(
                json.dumps({"title": "bad", "description": lone_surrogate}),
                encoding="utf-8",
            )
            before = (
                description_queue.read_bytes(),
                description_queue.with_suffix(".tsv").read_bytes(),
            )
            code, stdout, stderr = invoke(
                description_queue, "task", "add", "--from-json", str(raw_path)
            )
            self.assertEqual(2, code)
            self.assertEqual("", stdout)
            self.assertNotIn("Traceback", stderr)
            self.assertIn("description", stderr)
            self.assertEqual(before[0], description_queue.read_bytes())
            self.assertEqual(before[1], description_queue.with_suffix(".tsv").read_bytes())

            for field, command in (
                ("summary", "complete"),
                ("message", "fail"),
                ("reason", "block"),
            ):
                with self.subTest(field=field):
                    queue = directory / f"{field}.json"
                    aq.initialize_queue(queue, "demo", aq.fixed_config())
                    state = aq.load_state(queue)
                    task = aq.add_task(state, {"title": field})
                    aq.write_json(queue, state)
                    aq.atomic_write_text(
                        queue.with_suffix(".tsv"),
                        aq.render_tsv(state, state["updated_at"]),
                    )
                    if command in {"complete", "fail"}:
                        claim = aq.mutate_queue(
                            queue, lambda value: aq.claim_task(value, "agent")
                        )
                        arguments = [
                            command, "--task", task["id"], "--agent", "agent",
                            "--token", claim["lease_token"],
                            "--summary" if command == "complete" else "--error",
                            lone_surrogate,
                        ]
                    else:
                        arguments = ["block", task["id"], "--reason", lone_surrogate]
                    before = (
                        queue.read_bytes(), queue.with_suffix(".tsv").read_bytes()
                    )
                    code, stdout, stderr = invoke(queue, *arguments)
                    self.assertEqual(2, code)
                    self.assertEqual("", stdout)
                    self.assertNotIn("Traceback", stderr)
                    self.assertIn(field, stderr)
                    self.assertEqual(before[0], queue.read_bytes())
                    self.assertEqual(before[1], queue.with_suffix(".tsv").read_bytes())

    def test_lone_surrogate_persisted_fields_are_code_six_for_status_and_doctor(self):
        lone_surrogate = "\ud800"
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for field in ("description", "summary", "error", "reason"):
                with self.subTest(field=field):
                    queue = directory / f"{field}.json"
                    state = aq.new_state("demo", aq.fixed_config())
                    created = aq.add_task(state, {"title": field})
                    task = state["tasks"][created["id"]]
                    if field == "description":
                        task["description"] = lone_surrogate
                    elif field == "summary":
                        task["status"] = "completed"
                        task["result"] = canonical_result(
                            summary=lone_surrogate
                        )
                    else:
                        task["status"] = "failed" if field == "error" else "blocked"
                        task["last_error"] = {
                            "message": lone_surrogate,
                            "at": task["updated_at"],
                            **({"kind": "blocked"} if field == "reason" else {}),
                        }
                    queue.write_text(
                        json.dumps(state, ensure_ascii=True), encoding="utf-8"
                    )
                    queue.with_suffix(".tsv").write_text(
                        "do not modify\n", encoding="utf-8"
                    )
                    before_json = queue.read_bytes()
                    before_tsv = queue.with_suffix(".tsv").read_bytes()
                    status = run_cli(
                        "--queue", queue, "status", "--format", "json"
                    )
                    self.assertEqual(6, status.returncode, status.stderr)
                    self.assertNotIn("Traceback", status.stderr)
                    doctor = run_cli("--queue", queue, "doctor", "--repair")
                    self.assertEqual(6, doctor.returncode, doctor.stderr)
                    self.assertEqual("", doctor.stderr)
                    report = json.loads(doctor.stdout)
                    self.assertEqual("source.invalid", report["issues"][0]["code"])
                    self.assertEqual(before_json, queue.read_bytes())
                    self.assertEqual(before_tsv, queue.with_suffix(".tsv").read_bytes())


class DoctorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "queue.json"
        self.state = aq.initialize_queue(self.queue, "demo", aq.fixed_config())

    def test_healthy_queue_and_exact_tsv_diagnostics(self):
        report = aq.doctor(self.queue, now=self.state["updated_at"])
        self.assertEqual({
            "ok": True,
            "queue": str(self.queue.absolute()),
            "revision": 0,
            "issues": [],
            "repairs": [],
        }, report)

        expected_codes = {
            "missing": "tsv.missing",
            "malformed": "tsv.malformed",
            "stale": "tsv.stale",
        }
        for kind, code in expected_codes.items():
            with self.subTest(kind=kind):
                aq.atomic_write_text(
                    self.queue.with_suffix(".tsv"),
                    aq.render_tsv(self.state, self.state["updated_at"]),
                )
                if kind == "missing":
                    self.queue.with_suffix(".tsv").unlink()
                elif kind == "malformed":
                    self.queue.with_suffix(".tsv").write_text("bad\n")
                else:
                    self.queue.with_suffix(".tsv").write_text(
                        aq.render_empty_tsv(99), encoding="utf-8"
                    )
                report = aq.doctor(self.queue, now=self.state["updated_at"])
                self.assertFalse(report["ok"])
                self.assertEqual([code], [issue["code"] for issue in report["issues"]])

    def test_repair_rebuilds_exact_tsv_without_json_revision_or_event_change(self):
        before_json = self.queue.read_bytes()
        self.queue.with_suffix(".tsv").write_text("wrong\n", encoding="utf-8")

        report = aq.doctor(
            self.queue, repair=True, now=self.state["updated_at"]
        )

        self.assertTrue(report["ok"])
        self.assertEqual(["tsv.malformed"], [item["code"] for item in report["issues"]])
        self.assertEqual(["tsv.rebuilt"], [item["code"] for item in report["repairs"]])
        self.assertEqual(before_json, self.queue.read_bytes())
        self.assertEqual(
            aq.render_tsv(self.state, self.state["updated_at"]).encode(),
            self.queue.with_suffix(".tsv").read_bytes(),
        )
        self.assertEqual(0, aq.load_state(self.queue)["revision"])
        self.assertEqual([], aq.load_state(self.queue)["events"])

    def test_corrupt_source_is_reported_and_never_repaired(self):
        cases = {
            "json": b'{"schema_version": NaN}\n',
            "schema": json.dumps({**self.state, "schema_version": 3}).encode(),
            "graph": None,
            "counter": json.dumps({**self.state, "next_task_sequence": 0}).encode(),
            "event": None,
        }
        graph = copy.deepcopy(self.state)
        created = aq.add_task(graph, {"title": "bad graph"})
        graph["tasks"][created["id"]]["depends_on"] = ["T-999999"]
        cases["graph"] = json.dumps(graph).encode()
        event = copy.deepcopy(self.state)
        event["events"] = [{
            "seq": 1, "at": event["updated_at"], "type": "bad",
            "actor": "test", "task_id": "T-999999", "revision": 1,
            "details": {},
        }]
        event["next_event_sequence"] = 2
        cases["event"] = json.dumps(event).encode()

        for name, source in cases.items():
            with self.subTest(name=name):
                self.queue.write_bytes(source)
                self.queue.with_suffix(".tsv").write_bytes(b"do not touch\n")
                before_json = self.queue.read_bytes()
                before_tsv = self.queue.with_suffix(".tsv").read_bytes()
                report = aq.doctor(self.queue, repair=True)
                self.assertFalse(report["ok"])
                self.assertIsNone(report["revision"])
                self.assertEqual("source.invalid", report["issues"][0]["code"])
                self.assertEqual([], report["repairs"])
                self.assertEqual(before_json, self.queue.read_bytes())
                self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())
                aq.write_json(self.queue, self.state)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_hostile_symlink_lock_and_orphan_are_never_followed(self):
        target = Path(self.temporary.name) / "target"
        target.mkdir()
        marker = target / "marker"
        marker.write_text("safe", encoding="utf-8")
        lock_path = Path(str(self.queue) + ".lock")
        os.symlink(target, lock_path, target_is_directory=True)
        report = aq.doctor(self.queue, repair=True)
        self.assertEqual(["lock.invalid"], [
            issue["code"] for issue in report["issues"]
        ])
        self.assertEqual("safe", marker.read_text())
        self.assertTrue(lock_path.is_symlink())
        lock_path.unlink()

        orphan = lock_path.with_name(f".{lock_path.name}.orphan-{'d' * 24}")
        os.symlink(target, orphan, target_is_directory=True)
        report = aq.doctor(self.queue, repair=True, now=self.state["updated_at"])
        self.assertFalse(report["ok"])
        self.assertIn("lock_artifact.unsafe", [i["code"] for i in report["issues"]])
        self.assertTrue(orphan.is_symlink())
        self.assertEqual("safe", marker.read_text())

    def test_valid_stale_lock_and_orphan_directories_are_cleaned_deterministically(self):
        lock_path = Path(str(self.queue) + ".lock")
        lock_path.mkdir()
        old = time.time() - 120
        os.utime(lock_path, (old, old))
        artifact = lock_path.with_name(f".{lock_path.name}.orphan-{'a' * 24}")
        artifact.mkdir()
        (artifact / "owner.json").write_text("{}", encoding="utf-8")

        report = aq.doctor(self.queue, repair=True, now=self.state["updated_at"])

        self.assertTrue(report["ok"])
        self.assertEqual(
            ["lock.stale", "lock_artifact.orphan"],
            [item["code"] for item in report["issues"]],
        )
        self.assertEqual(
            ["lock.removed", "lock_artifact.removed"],
            [item["code"] for item in report["repairs"]],
        )
        self.assertFalse(lock_path.exists())
        self.assertFalse(artifact.exists())

    def test_corrupt_guard_is_fail_closed_and_live_guard_times_out(self):
        guard = Path(str(self.queue) + ".lock.guard")
        guard.write_bytes(b"bad")
        before_json = self.queue.read_bytes()
        before_tsv = self.queue.with_suffix(".tsv").read_bytes()
        report = aq.doctor(self.queue, repair=True)
        self.assertEqual(["guard.invalid"], [
            issue["code"] for issue in report["issues"]
        ])
        self.assertEqual(b"bad", guard.read_bytes())
        self.assertEqual(before_json, self.queue.read_bytes())
        self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

        guard.write_bytes(aq.GUARD_MARKER)
        self.state["config"]["lock_timeout_seconds"] = 1
        aq.write_json(self.queue, self.state)
        with aq.QueueLock(self.queue, lock_timeout=1, stale_seconds=30):
            report = aq.doctor(self.queue, repair=True)
        self.assertEqual(["lock.timeout"], [
            issue["code"] for issue in report["issues"]
        ])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_tsv_symlink_is_reported_without_touching_target(self):
        target = Path(self.temporary.name) / "target.tsv"
        target.write_text("victim\n", encoding="utf-8")
        self.queue.with_suffix(".tsv").unlink()
        os.symlink(target, self.queue.with_suffix(".tsv"))

        report = aq.doctor(
            self.queue, repair=True, now=self.state["updated_at"]
        )

        self.assertFalse(report["ok"])
        self.assertEqual(["tsv.unsafe"], [item["code"] for item in report["issues"]])
        self.assertEqual([], report["repairs"])
        self.assertEqual("victim\n", target.read_text(encoding="utf-8"))

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_cli_guard_and_lock_failures_always_emit_structured_report(self):
        expected_base = {
            "ok": False,
            "queue": str(self.queue.absolute()),
            "revision": None,
            "repairs": [],
        }
        guard = Path(str(self.queue) + ".lock.guard")
        lock_path = Path(str(self.queue) + ".lock")
        target = Path(self.temporary.name) / "victim"
        target.write_text("safe", encoding="utf-8")

        for name, setup, issue_code in (
            ("corrupt_guard", lambda: guard.write_bytes(b"bad"), "guard.invalid"),
            (
                "symlink_guard",
                lambda: os.symlink(target, guard),
                "guard.invalid",
            ),
            (
                "nonregular_lock",
                lambda: (guard.write_bytes(aq.GUARD_MARKER),
                         lock_path.write_text("hostile", encoding="utf-8")),
                "lock.invalid",
            ),
        ):
            with self.subTest(name=name):
                if guard.is_symlink() or guard.exists():
                    guard.unlink()
                if lock_path.exists() or lock_path.is_symlink():
                    lock_path.unlink()
                setup()
                before_json = self.queue.read_bytes()
                before_tsv = self.queue.with_suffix(".tsv").read_bytes()
                result = run_cli(
                    "--queue", self.queue, "doctor", "--repair", timeout=8
                )
                self.assertEqual(6, result.returncode, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                report = json.loads(result.stdout)
                self.assertEqual(expected_base, {
                    key: report[key] for key in expected_base
                })
                self.assertEqual([issue_code], [
                    issue["code"] for issue in report["issues"]
                ])
                self.assertEqual(before_json, self.queue.read_bytes())
                self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())
                self.assertEqual("safe", target.read_text(encoding="utf-8"))

        if lock_path.exists():
            lock_path.unlink()
        if guard.is_symlink() or guard.exists():
            guard.unlink()
        guard.write_bytes(aq.GUARD_MARKER)
        self.state["config"]["lock_timeout_seconds"] = 1
        aq.write_json(self.queue, self.state)
        with aq.QueueLock(self.queue, lock_timeout=1, stale_seconds=30):
            result = run_cli(
                "--queue", self.queue, "doctor", "--repair", timeout=8
            )
        self.assertEqual(4, result.returncode, result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(expected_base, {key: report[key] for key in expected_base})
        self.assertEqual(["lock.timeout"], [
            issue["code"] for issue in report["issues"]
        ])

    def test_filesystem_inspection_errors_emit_structured_fail_closed_json(self):
        real_lstat = aq.os.lstat

        def invoke():
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = aq.main([
                    "--queue", str(self.queue), "doctor", "--repair"
                ])
            self.assertEqual(6, code)
            self.assertEqual("", stderr.getvalue())
            self.assertNotEqual("", stdout.getvalue())
            return json.loads(stdout.getvalue())

        selected_paths = (
            ("source.unreadable", self.queue),
            ("lock.unreadable", Path(str(self.queue) + ".lock")),
            ("tsv.unreadable", self.queue.with_suffix(".tsv")),
        )
        for expected_code, selected in selected_paths:
            with self.subTest(expected_code=expected_code):
                def selective_lstat(path):
                    if Path(path) == selected:
                        raise PermissionError("injected unreadable path")
                    return real_lstat(path)

                before_json = self.queue.read_bytes()
                before_tsv = self.queue.with_suffix(".tsv").read_bytes()
                with mock.patch.object(aq.os, "lstat", side_effect=selective_lstat):
                    report = invoke()
                self.assertEqual([expected_code], [
                    issue["code"] for issue in report["issues"]
                ])
                self.assertEqual([], report["repairs"])
                self.assertEqual(before_json, self.queue.read_bytes())
                self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

        real_read_text = aq.Path.read_text

        def selective_read_text(selected_path, *args, **kwargs):
            if selected_path == self.queue:
                raise PermissionError("injected unreadable source")
            return real_read_text(selected_path, *args, **kwargs)

        with mock.patch.object(
            aq.Path, "read_text", selective_read_text
        ):
            report = invoke()
        self.assertEqual(["source.unreadable"], [
            issue["code"] for issue in report["issues"]
        ])
        self.assertEqual([], report["repairs"])

        before_json = self.queue.read_bytes()
        before_tsv = self.queue.with_suffix(".tsv").read_bytes()
        with mock.patch.object(
            aq.Path, "iterdir", side_effect=PermissionError("injected iteration")
        ):
            report = invoke()
        self.assertEqual(["lock_artifacts.unreadable"], [
            issue["code"] for issue in report["issues"]
        ])
        self.assertEqual([], report["repairs"])
        self.assertEqual(before_json, self.queue.read_bytes())
        self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

    def test_stale_lock_cleanup_failure_is_not_reported_as_a_repair(self):
        lock_path = Path(str(self.queue) + ".lock")
        lock_path.mkdir()
        old = time.time() - 120
        os.utime(lock_path, (old, old))

        with mock.patch.object(
            aq.QueueLock, "_rename_and_remove", return_value=False
        ):
            report = aq.doctor(
                self.queue, repair=True, now=self.state["updated_at"]
            )

        self.assertFalse(report["ok"])
        self.assertEqual(
            ["lock.stale", "lock.remove_failed"],
            [issue["code"] for issue in report["issues"]],
        )
        self.assertEqual([], report["repairs"])
        self.assertTrue(lock_path.is_dir())

    def test_stale_lock_quarantine_cleanup_failure_remains_fail_closed(self):
        lock_path = Path(str(self.queue) + ".lock")
        lock_path.mkdir()
        old = time.time() - 120
        os.utime(lock_path, (old, old))

        with mock.patch.object(
            aq.shutil, "rmtree", side_effect=OSError("injected cleanup failure")
        ):
            report = aq.doctor(
                self.queue, repair=True, now=self.state["updated_at"]
            )

        self.assertFalse(report["ok"])
        self.assertIn("lock.remove_failed", [
            issue["code"] for issue in report["issues"]
        ])
        self.assertNotIn("lock.removed", [
            repair["code"] for repair in report["repairs"]
        ])
        quarantines = [
            child for child in lock_path.parent.iterdir()
            if "orphan-" in child.name or "doctor-" in child.name
        ]
        self.assertTrue(quarantines)

    def test_guard_release_failures_preserve_structured_doctor_report(self):
        real_release = aq.QueueLock._release_guard
        real_close = aq.os.close

        def invoke(queue):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = aq.main(["--queue", str(queue), "doctor"])
            self.assertEqual(6, code)
            self.assertEqual("", stderr.getvalue())
            self.assertNotEqual("", stdout.getvalue())
            report = json.loads(stdout.getvalue())
            self.assertFalse(report["ok"])
            self.assertEqual([], report["repairs"])
            self.assertNotIn("Traceback", stdout.getvalue())
            return report

        def release_then_fail(lock):
            real_release(lock)
            raise OSError("injected release failure")

        def close_then_fail(descriptor):
            real_close(descriptor)
            raise OSError("injected close failure")

        cases = (
            ("release", mock.patch.object(
                aq.QueueLock, "_release_guard", release_then_fail
            )),
            ("unlock", mock.patch.object(
                aq.QueueLock, "_unlock_guard",
                side_effect=OSError("injected unlock failure"),
            )),
            ("close", mock.patch.object(
                aq.os, "close", side_effect=close_then_fail
            )),
        )
        for name, patcher in cases:
            with self.subTest(name=name), patcher:
                report = invoke(self.queue)
            self.assertEqual(["guard.release_failed"], [
                issue["code"] for issue in report["issues"]
            ])

        missing_queue = Path(self.temporary.name) / "missing.json"
        with mock.patch.object(
            aq.QueueLock, "_release_guard", release_then_fail
        ):
            report = invoke(missing_queue)
        self.assertEqual(
            ["source.missing", "guard.release_failed"],
            [issue["code"] for issue in report["issues"]],
        )

    def test_failed_artifact_cleanup_is_rediscovered_by_next_doctor(self):
        lock_path = Path(str(self.queue) + ".lock")
        artifact = lock_path.with_name(
            f".{lock_path.name}.orphan-{'a' * 24}"
        )
        artifact.mkdir()

        with mock.patch.object(
            aq.shutil, "rmtree", side_effect=OSError("injected cleanup failure")
        ):
            first = aq.doctor(
                self.queue, repair=True, now=self.state["updated_at"]
            )
        self.assertFalse(first["ok"])
        self.assertIn("lock_artifact.remove_failed", [
            issue["code"] for issue in first["issues"]
        ])

        second = aq.doctor(
            self.queue, repair=False, now=self.state["updated_at"]
        )
        self.assertFalse(second["ok"])
        self.assertIn("lock_artifact.orphan", [
            issue["code"] for issue in second["issues"]
        ])

        for child in list(self.queue.parent.iterdir()):
            if child.is_dir() and "orphan-" in child.name:
                aq.shutil.rmtree(child)
        legacy = lock_path.with_name(
            f"..{lock_path.name}.orphan-{'b' * 24}.doctor-{'c' * 24}"
        )
        legacy.mkdir()
        legacy_report = aq.doctor(
            self.queue, repair=False, now=self.state["updated_at"]
        )
        self.assertFalse(legacy_report["ok"])
        self.assertIn("lock_artifact.orphan", [
            issue["code"] for issue in legacy_report["issues"]
        ])


class CompactionTests(unittest.TestCase):
    OLD = "2020-01-01T00:00:00Z"
    CUTOFF = "2021-01-01T00:00:00Z"
    NOW = "2026-07-11T00:00:00Z"

    def task(self, state, title, status="completed", workflow_id=None,
             depends_on=None, updated_at=None):
        created = aq.add_task(state, {
            "title": title,
            "workflow_id": workflow_id,
            "depends_on": depends_on or [],
        })
        task = state["tasks"][created["id"]]
        task["created_at"] = self.OLD
        task["updated_at"] = updated_at or self.OLD
        task["status"] = status
        if status == "completed":
            task["result"] = canonical_result()
        elif status == "failed":
            task["last_error"] = {"message": "bad", "at": task["updated_at"]}
        elif status == "blocked":
            task["last_error"] = {
                "message": "wait", "at": task["updated_at"], "kind": "blocked"
            }
        elif status == "leased":
            task["attempts"] = 1
            task["claim"] = canonical_claim(
                claimed_at=self.OLD, heartbeat_at=self.OLD,
                expires_at="2030-01-01T00:00:00Z",
            )
        return task

    def test_parse_compaction_cutoff_accepts_date_and_canonical_utc(self):
        self.assertEqual(self.CUTOFF, aq.parse_compaction_cutoff("2021-01-01"))
        self.assertEqual(self.CUTOFF, aq.parse_compaction_cutoff(self.CUTOFF))
        for invalid in ("", "2021-1-1", "2021-01-01T00:00:00+00:00", "no"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(aq.InvariantError):
                    aq.parse_compaction_cutoff(invalid)

    def test_referenced_completed_dependency_is_retained(self):
        state = aq.new_state("demo", aq.fixed_config())
        dependency = self.task(state, "old")
        self.task(state, "pending", status="pending", depends_on=[dependency["id"]])
        before = copy.deepcopy(state)

        summary = aq.compact_state(state, self.CUTOFF, now=self.NOW)

        self.assertEqual(0, summary["removed_task_count"])
        self.assertEqual(before, state)

    def test_prunes_standalone_and_whole_workflow_but_retains_ineligible_groups(self):
        state = aq.new_state("demo", aq.fixed_config())
        standalone = self.task(state, "standalone")
        whole = [
            self.task(state, "whole-a", workflow_id="W-000001"),
            self.task(state, "whole-b", status="failed", workflow_id="W-000001"),
        ]
        retained = [
            self.task(state, "partial-old", workflow_id="W-000002"),
            self.task(state, "partial-new", workflow_id="W-000002", updated_at=self.NOW),
            self.task(state, "blocked", status="blocked"),
            self.task(state, "pending", status="pending"),
            self.task(state, "leased", status="leased"),
        ]

        summary = aq.compact_state(state, self.CUTOFF, now=self.NOW)

        self.assertEqual(
            [standalone["id"], *(task["id"] for task in whole)],
            summary["removed_task_ids"],
        )
        self.assertEqual(["W-000001"], summary["removed_workflow_ids"])
        self.assertTrue(all(task["id"] in state["tasks"] for task in retained))
        aq.validate_state(state)

    def test_event_cleanup_counts_new_task_events_and_old_unrelated_events(self):
        state = aq.new_state("demo", aq.fixed_config())
        removed = self.task(state, "remove")
        retained = self.task(state, "keep", status="pending")
        aq.append_event(state, "old.unrelated", "test", None, {}, self.OLD)
        aq.append_event(state, "new.removed", "test", removed["id"], {}, "2025-01-01T00:00:00Z")
        aq.append_event(
            state, "new.details", "test", None,
            {"task_ids": [removed["id"]]}, "2025-01-01T12:00:00Z",
        )
        aq.append_event(state, "new.retained", "test", retained["id"], {}, "2025-01-02T00:00:00Z")
        counters = (
            state["next_task_sequence"], state["next_workflow_sequence"],
            state["next_event_sequence"],
        )

        summary = aq.compact_state(state, self.CUTOFF, now=self.NOW)

        self.assertEqual(3, summary["removed_event_count"])
        self.assertEqual([removed["id"]], summary["removed_task_ids"])
        self.assertEqual(counters[:2], (
            state["next_task_sequence"], state["next_workflow_sequence"]
        ))
        self.assertEqual(counters[2] + 1, state["next_event_sequence"])
        event = state["events"][-1]
        self.assertEqual("queue.compacted", event["type"])
        self.assertIsNone(event["task_id"])
        self.assertEqual({
            "removed_event_count": 3,
            "removed_task_count": 1,
            "removed_task_ids": [removed["id"]],
            "removed_workflow_count": 0,
            "removed_workflow_ids": [],
        }, event["details"])
        aq.validate_state(state)

    def test_counters_are_not_reused_and_large_chain_is_iterative(self):
        state = aq.new_state("demo", aq.fixed_config())
        first = self.task(state, "task 0")
        template = copy.deepcopy(first)
        previous = first["id"]
        for index in range(1, 1500):
            task_id = f"T-{index + 1:06d}"
            task = copy.deepcopy(template)
            task["id"] = task_id
            task["title"] = f"task {index}"
            task["depends_on"] = [previous]
            state["tasks"][task_id] = task
            previous = task_id
        state["next_task_sequence"] = 1501
        next_task = state["next_task_sequence"]

        summary = aq.compact_state(state, self.CUTOFF, now=self.NOW)

        self.assertEqual(1500, summary["removed_task_count"])
        new_task = aq.add_task(state, {"title": "new"})
        self.assertEqual(f"T-{next_task:06d}", new_task["id"])

    def test_invalid_or_corrupt_inputs_leave_state_byte_identical(self):
        state = aq.new_state("demo", aq.fixed_config())
        self.task(state, "old")
        for cutoff in ("bad", "2021-1-1"):
            before = copy.deepcopy(state)
            with self.assertRaises(aq.InvariantError):
                aq.compact_state(state, cutoff, now=self.NOW)
            self.assertEqual(before, state)
        state["next_task_sequence"] = 0
        before = copy.deepcopy(state)
        with self.assertRaises(aq.InvariantError):
            aq.compact_state(state, self.CUTOFF, now=self.NOW)
        self.assertEqual(before, state)


class QueueLockTests(unittest.TestCase):
    def assert_hostile_guard_rejected(self, path):
        lock = aq.QueueLock(path, lock_timeout=0.05, stale_seconds=1)
        with self.assertRaises(aq.QueueError):
            with lock:
                self.fail("hostile guard was accepted")
        self.assertFalse(lock.path.exists())
        self.assertEqual(
            [], list(path.parent.glob(f".{lock.path.name}.*"))
        )

    def test_guard_symlink_never_modifies_empty_or_nonempty_victim(self):
        for initial in (b"", b"do-not-touch"):
            with self.subTest(initial=initial), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "queue.json"
                victim = Path(directory) / "victim"
                victim.write_bytes(initial)
                Path(str(path) + ".lock.guard").symlink_to(victim)

                self.assert_hostile_guard_rejected(path)

                self.assertEqual(initial, victim.read_bytes())

    def test_nonregular_guard_fifo_and_directory_fail_closed(self):
        makers = [("directory", lambda guard: guard.mkdir())]
        if hasattr(os, "mkfifo"):
            makers.append(("fifo", lambda guard: os.mkfifo(guard)))
        for name, make_guard in makers:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "queue.json"
                make_guard(Path(str(path) + ".lock.guard"))

                self.assert_hostile_guard_rejected(path)

    def test_existing_guard_requires_exact_marker_without_repair(self):
        for contents in (b"", b"\0", b"LQG", b"LQG1-extra"):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "queue.json"
                guard = Path(str(path) + ".lock.guard")
                guard.write_bytes(contents)

                self.assert_hostile_guard_rejected(path)

                self.assertEqual(contents, guard.read_bytes())

    def test_lock_backend_helpers_use_selected_portable_api(self):
        lock = aq.QueueLock("queue.json")
        fake_fcntl = mock.Mock(LOCK_EX=1, LOCK_NB=2, LOCK_UN=4)
        with mock.patch.object(aq, "LOCK_BACKEND", "fcntl"):
            with mock.patch.object(aq, "_fcntl", fake_fcntl):
                self.assertTrue(lock._try_guard_lock(9))
                lock._unlock_guard(9)
        self.assertEqual(
            [mock.call(9, 3), mock.call(9, 4)],
            fake_fcntl.flock.call_args_list,
        )

        fake_msvcrt = mock.Mock(LK_NBLCK=5, LK_UNLCK=6)
        with mock.patch.object(aq, "LOCK_BACKEND", "msvcrt"):
            with mock.patch.object(aq, "_msvcrt", fake_msvcrt):
                with mock.patch.object(aq.os, "lseek") as seek:
                    self.assertTrue(lock._try_guard_lock(11))
                    lock._unlock_guard(11)
        self.assertEqual(
            [mock.call(11, 5, 1), mock.call(11, 6, 1)],
            fake_msvcrt.locking.call_args_list,
        )
        self.assertEqual(2, seek.call_count)

    def test_live_holder_is_not_reclaimed_after_metadata_stales(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            ready = Path(directory) / "holder-ready"
            holder_source = "\n".join((
                "import pathlib, sys, time",
                f"sys.path.insert(0, {str(SCRIPT_DIR)!r})",
                "import agent_queue as aq",
                "path = pathlib.Path(sys.argv[1])",
                "ready = pathlib.Path(sys.argv[2])",
                "with aq.QueueLock(path, lock_timeout=1, stale_seconds=1):",
                "    ready.write_text('ready')",
                "    time.sleep(10)",
            ))
            holder = subprocess.Popen(
                [sys.executable, "-c", holder_source, str(path), str(ready)]
            )
            try:
                deadline = time.monotonic() + 3
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists())
                time.sleep(1.1)
                with self.assertRaises(aq.LockTimeout):
                    with aq.QueueLock(
                        path, lock_timeout=0.1, stale_seconds=1
                    ):
                        self.fail("live holder was reclaimed")
            finally:
                holder.terminate()
                holder.wait(timeout=5)

            with aq.QueueLock(path, lock_timeout=1, stale_seconds=1):
                self.assertTrue(Path(str(path) + ".lock").exists())
            self.assertFalse(Path(str(path) + ".lock").exists())
            self.assertTrue(Path(str(path) + ".lock.guard").exists())

    def test_module_loads_without_fcntl_and_has_no_unsafe_fallback(self):
        source = "\n".join((
            "import builtins, runpy, sys",
            "real_import = builtins.__import__",
            "def guarded_import(name, *args, **kwargs):",
            "    if name == 'fcntl':",
            "        raise ImportError('simulated missing fcntl')",
            "    return real_import(name, *args, **kwargs)",
            "builtins.__import__ = guarded_import",
            "namespace = runpy.run_path(sys.argv[1])",
            "assert namespace['LOCK_BACKEND'] in (None, 'msvcrt')",
        ))
        result = subprocess.run(
            [sys.executable, "-c", source, str(SCRIPT_PATH)],
            text=True, capture_output=True, timeout=10, check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)

        with mock.patch.object(aq, "LOCK_BACKEND", None, create=True):
            with tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(aq.QueueError, "locking backend"):
                    with aq.QueueLock(Path(directory) / "queue.json"):
                        pass

    def test_lock_timeout_and_owner_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            with aq.QueueLock(path, lock_timeout=1, stale_seconds=30) as owner:
                data = json.loads(
                    (Path(str(path) + ".lock") / "owner.json").read_text()
                )
                self.assertEqual(owner.token, data["token"])
                self.assertEqual(os.getpid(), data["pid"])
                self.assertEqual(socket.gethostname(), data["hostname"])
                self.assertEqual(
                    {"token", "pid", "hostname", "acquired_at", "stale_after"},
                    set(data),
                )
                with self.assertRaises(aq.LockTimeout) as caught:
                    with aq.QueueLock(path, lock_timeout=0.05, stale_seconds=30):
                        pass
                self.assertEqual(4, caught.exception.exit_code)

    def test_kernel_guard_honors_lock_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            ready = Path(directory) / "ready"
            holder_source = "\n".join((
                "import pathlib, sys, time",
                f"sys.path.insert(0, {str(SCRIPT_DIR)!r})",
                "import agent_queue as aq",
                "with aq.QueueLock(sys.argv[1], lock_timeout=1, stale_seconds=30):",
                "    pathlib.Path(sys.argv[2]).write_text('ready')",
                "    time.sleep(2)",
            ))
            holder = subprocess.Popen(
                [sys.executable, "-c", holder_source, str(path), str(ready)]
            )
            try:
                deadline = time.monotonic() + 2
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(ready.exists())
                started = time.monotonic()
                with self.assertRaises(aq.LockTimeout):
                    with aq.QueueLock(path, lock_timeout=0.05, stale_seconds=30):
                        pass
                self.assertLess(time.monotonic() - started, 0.5)
            finally:
                holder.kill()
                holder.wait(timeout=5)

    def test_valid_stale_owner_reclaimed_and_foreign_cleanup_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            lock_path = Path(str(path) + ".lock")
            lock_path.mkdir(parents=True)
            (lock_path / "owner.json").write_text(
                json.dumps(
                    {
                        "token": "old",
                        "pid": 1,
                        "hostname": "host",
                        "acquired_at": "2020-01-01T00:00:00Z",
                        "stale_after": "2020-01-01T00:00:01Z",
                    }
                )
            )
            lock = aq.QueueLock(path, lock_timeout=1, stale_seconds=30)
            lock.__enter__()
            owner_path = lock_path / "owner.json"
            owner = json.loads(owner_path.read_text())
            owner["token"] = "foreign"
            owner_path.write_text(json.dumps(owner))
            lock.__exit__(None, None, None)
            self.assertTrue(lock_path.exists())

    def test_stale_reclaim_refuses_aba_fresh_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            lock_path = Path(str(path) + ".lock")
            lock_path.mkdir(parents=True)
            (lock_path / "owner.json").write_text(json.dumps({
                "token": "stale",
                "pid": 1,
                "hostname": "host",
                "acquired_at": "2020-01-01T00:00:00Z",
                "stale_after": "2020-01-01T00:00:01Z",
            }))
            late_contender = aq.QueueLock(path, lock_timeout=1, stale_seconds=30)
            observed = late_contender._lock_identity()
            first = aq.QueueLock(path, lock_timeout=1, stale_seconds=30)
            self.assertTrue(first._reclaim(observed))
            first.__enter__()

            with self.assertRaises(aq.LockTimeout):
                late_contender._reclaim(observed)
            owner = json.loads((lock_path / "owner.json").read_text())
            self.assertEqual(first.token, owner["token"])
            first.__exit__(None, None, None)

    def test_missing_or_corrupt_owner_reclaimed_only_after_mtime_age(self):
        for contents in (None, "not-json", b"\xff"):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "queue.json"
                lock_path = Path(str(path) + ".lock")
                lock_path.mkdir(parents=True)
                if contents is not None:
                    if isinstance(contents, bytes):
                        (lock_path / "owner.json").write_bytes(contents)
                    else:
                        (lock_path / "owner.json").write_text(contents)
                with self.assertRaises(aq.LockTimeout):
                    with aq.QueueLock(path, lock_timeout=0.05, stale_seconds=1):
                        pass
                old = time.time() - 5
                os.utime(lock_path, (old, old))
                with aq.QueueLock(path, lock_timeout=1, stale_seconds=1):
                    pass
                self.assertFalse(lock_path.exists())

    def test_peek_lock_config_uses_only_positive_integers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            for value, expected in (
                ({"config": {"lock_timeout_seconds": 2, "stale_lock_seconds": 9}}, (2, 9)),
                ({"config": {"lock_timeout_seconds": 0, "stale_lock_seconds": True}}, (5, 30)),
                ({"broken": True}, (5, 30)),
            ):
                path.write_text(json.dumps(value), encoding="utf-8")
                self.assertEqual(expected, aq.peek_lock_config(path))


class QueueCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "queue.json"

    def cli(self, *arguments, timeout=10):
        return run_cli("--queue", self.queue, *arguments, timeout=timeout)

    def json_output(self, result):
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("", result.stderr)
        return json.loads(result.stdout)

    def init(self, **overrides):
        arguments = ["init", "--id", "demo"]
        for name, value in overrides.items():
            arguments.extend((f"--{name.replace('_', '-')}", str(value)))
        return self.json_output(self.cli(*arguments))

    def add(self, title="work", *extra):
        return self.json_output(self.cli("task", "add", "--title", title, *extra))

    def make_git_repo(self):
        root = Path(self.temporary.name) / "repo"
        root.mkdir()
        subprocess.run(
            ["git", "-C", str(root), "init", "-b", "main"],
            check=True,
            capture_output=True,
            text=True,
        )
        for key, value in (
            ("user.name", "Queue CLI Tests"),
            ("user.email", "queue-cli@example.test"),
        ):
            subprocess.run(
                ["git", "-C", str(root), "config", key, value],
                check=True,
                capture_output=True,
                text=True,
            )
        (root / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(
            ["git", "-C", str(root), "add", "base.txt"],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "-C", str(root), "commit", "-m", "base"],
            check=True,
            capture_output=True,
            text=True,
        )
        return root

    def test_cli_init_creates_both_files_and_second_is_code_two_unchanged(self):
        output = self.init(lease_seconds=10, max_attempts=4, retry_backoff=2)
        self.assertTrue(output["ok"])
        self.assertTrue(self.queue.exists())
        self.assertTrue(self.queue.with_suffix(".tsv").exists())
        before = self.queue.read_bytes()
        result = self.cli("init", "--id", "again")
        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertNotEqual("", result.stderr)
        self.assertEqual(before, self.queue.read_bytes())

    def test_cli_migrates_version_one_queue_atomically(self):
        state = aq.new_state("legacy", aq.fixed_config())
        state["schema_version"] = 1
        aq.write_json(self.queue, state)
        aq.atomic_write_text(
            self.queue.with_suffix(".tsv"),
            aq.render_tsv(state, state["updated_at"]),
        )
        added = self.add("legacy generic")
        self.assertNotIn("git_mode", added["task"])
        before_revision = aq.load_state(self.queue)["revision"]

        migrated = self.json_output(self.cli("migrate", "--to", "2"))

        self.assertEqual(
            {"ok": True, "from": 1, "to": 2},
            migrated,
        )
        stored = aq.load_state(self.queue)
        self.assertEqual(2, stored["schema_version"])
        self.assertEqual(before_revision + 1, stored["revision"])
        self.assertEqual("queue.migrated", stored["events"][-1]["type"])
        self.assertEqual(
            stored["revision"],
            aq.tsv_revision(self.queue.with_suffix(".tsv")),
        )

    def test_init_and_machine_status_discover_dashboard_safely(self):
        output = self.init()
        self.assertEqual(
            ["serve --open", "status"],
            output["next_actions"],
        )
        self.add("work")
        table = self.cli("status")
        self.assertNotIn("serve --open", table.stdout)
        machine = self.json_output(
            self.cli("status", "--format", "json")
        )
        self.assertNotIn("hint", machine)

    def test_help_names_live_local_dashboard(self):
        top = run_cli("--help")
        serve = run_cli("serve", "--help")
        self.assertEqual(0, top.returncode)
        self.assertIn("live workflow dashboard", top.stdout)
        self.assertEqual(0, serve.returncode)
        self.assertIn("127.0.0.1", serve.stdout)
        self.assertIn("--idle-timeout", serve.stdout)

    def test_human_tty_status_points_to_live_dashboard(self):
        class TtyOutput(io.StringIO):
            def isatty(self):
                return True

        self.init()
        self.add("work")
        output = TtyOutput()

        with redirect_stdout(output):
            code = aq.main(
                ["--queue", str(self.queue), "status"]
            )

        self.assertEqual(0, code)
        self.assertIn(
            "Live dashboard: agent_queue.py serve --open",
            output.getvalue(),
        )

    def test_task_add_batch_show_and_json_errors_are_atomic(self):
        self.init()
        added = self.add(
            "one", "--description", "desc", "--role", "dev", "--priority", "3",
            "--resource", "repo", "--label", "backend"
        )
        self.assertEqual("T-000001", added["task"]["id"])
        shown = self.json_output(self.cli("task", "show", "T-000001"))
        self.assertEqual("one", shown["task"]["title"])
        self.assertNotIn("lease_token", shown["task"].get("claim") or {})

        batch_path = Path(self.temporary.name) / "batch.json"
        batch_path.write_text(
            json.dumps([{"title": "two"}, {"title": "three", "depends_on": ["T-000002"]}])
        )
        batch = self.json_output(
            self.cli("task", "add-batch", "--from-json", batch_path)
        )
        self.assertEqual(["T-000002", "T-000003"], [task["id"] for task in batch["tasks"]])
        before = self.queue.read_bytes()
        bad = Path(self.temporary.name) / "bad.json"
        bad.write_text('[{"title":"four"}, NaN]')
        result = self.cli("task", "add-batch", "--from-json", bad)
        self.assertEqual(2, result.returncode)
        self.assertEqual(before, self.queue.read_bytes())

    def test_cli_creates_both_workflow_templates_with_one_revision_and_event(self):
        self.init()
        adversarial = self.json_output(self.cli(
            "workflow", "add", "--template", "adversarial-review",
            "--title", "Review me", "--priority", "20",
            "--resource", "a", "--resource", "b", "--reviewers", "3",
        ))
        self.assertEqual({
            "ok": True,
            "workflow_id": "W-000001",
            "template": "adversarial-review",
            "task_ids": [f"T-{i:06d}" for i in range(1, 7)],
        }, adversarial)
        state = aq.load_state(self.queue)
        self.assertEqual(1, state["revision"])
        self.assertEqual(1, len(state["events"]))
        self.assertEqual(1, state["events"][0]["revision"])
        self.assertNotIn("lease_token", self.queue.read_text())
        tsv = self.cli("status", "--format", "tsv", "--workflow", "W-000001")
        self.assertEqual(0, tsv.returncode, tsv.stderr)
        self.assertEqual(6, sum(line.startswith("T-") for line in tsv.stdout.splitlines()))

        input_path = Path(self.temporary.name) / "shards.json"
        input_path.write_text(json.dumps({
            "title": "Parallel", "priority": 7,
            "shards": [["left"], ["middle"], ["right"]],
        }))
        parallel = self.json_output(self.cli(
            "workflow", "add", "--template", "parallel-shards",
            "--from-json", input_path,
        ))
        self.assertEqual("W-000002", parallel["workflow_id"])
        self.assertEqual("parallel-shards", parallel["template"])
        self.assertEqual([f"T-{i:06d}" for i in range(7, 12)], parallel["task_ids"])
        state = aq.load_state(self.queue)
        self.assertEqual(2, state["revision"])
        self.assertEqual(2, len(state["events"]))
        self.assertEqual({
            "template": "parallel-shards",
            "workflow_id": "W-000002",
            "task_ids": [f"T-{i:06d}" for i in range(7, 12)],
            "task_count": 5,
            "shard_count": 3,
        }, state["events"][-1]["details"])

    def test_cli_workflow_user_errors_are_code_two_and_atomic(self):
        self.init()
        paths = []
        for name, value in (
            ("missing", {"title": "x"}),
            ("unknown", {"title": "x", "shards": [["r"]], "extra": 1}),
            ("bad-priority", {"title": "x", "priority": True, "shards": [["r"]]}),
            ("duplicate", {"title": "x", "shards": [["r"], ["r"]]}),
        ):
            path = Path(self.temporary.name) / f"{name}.json"
            path.write_text(json.dumps(value))
            paths.append(path)
        cases = (
            ("workflow", "add", "--template", "adversarial-review", "--title", "x", "--resource", "r", "--from-json", paths[0]),
            ("workflow", "add", "--template", "parallel-shards", "--from-json", paths[0]),
            ("workflow", "add", "--template", "parallel-shards", "--from-json", paths[1]),
            ("workflow", "add", "--template", "parallel-shards", "--from-json", paths[2]),
            ("workflow", "add", "--template", "parallel-shards", "--from-json", paths[3]),
            ("workflow", "add", "--template", "parallel-shards", "--title", "x", "--from-json", paths[3]),
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                before_json = self.queue.read_bytes()
                before_tsv = self.queue.with_suffix(".tsv").read_bytes()
                result = self.cli(*arguments)
                self.assertEqual(2, result.returncode, result.stderr)
                self.assertEqual("", result.stdout)
                self.assertEqual(before_json, self.queue.read_bytes())
                self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

    def test_cli_workflow_corrupt_queue_is_code_six(self):
        self.queue.write_text('{"revision": NaN}', encoding="utf-8")
        before = self.queue.read_bytes()
        result = self.cli(
            "workflow", "add", "--template", "adversarial-review",
            "--title", "x", "--resource", "r",
        )
        self.assertEqual(6, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertEqual(before, self.queue.read_bytes())

    def test_concurrent_workflow_creation_serializes_unique_graphs(self):
        self.init()
        processes = [
            subprocess.Popen(
                [
                    sys.executable, str(SCRIPT_PATH), "--queue", str(self.queue),
                    "workflow", "add", "--template", "adversarial-review",
                    "--title", f"flow {index}", "--resource", f"r-{index}",
                    "--reviewers", "2",
                ],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for index in range(6)
        ]
        outputs = communicate_all(processes, 20)
        self.assertTrue(all(process.returncode == 0 for process in processes), outputs)
        results = [json.loads(stdout) for stdout, _stderr in outputs]
        self.assertEqual(6, len({result["workflow_id"] for result in results}))
        self.assertEqual(30, len({task_id for result in results for task_id in result["task_ids"]}))
        state = aq.load_state(self.queue)
        self.assertEqual(6, state["revision"])
        self.assertEqual(6, len(state["events"]))
        self.assertEqual(30, len(state["tasks"]))
        aq.validate_persisted_state(state)
        expected_by_workflow = {
            result["workflow_id"]: tuple(result["task_ids"])
            for result in results
        }
        events_by_workflow = {}
        for event in state["events"]:
            details = event["details"]
            workflow_id = details["workflow_id"]
            self.assertNotIn(workflow_id, events_by_workflow)
            events_by_workflow[workflow_id] = tuple(details["task_ids"])
            self.assertEqual(
                [workflow_id] * len(details["task_ids"]),
                [state["tasks"][task_id]["workflow_id"]
                 for task_id in details["task_ids"]],
            )
        self.assertEqual(expected_by_workflow, events_by_workflow)

    def test_workflow_parser_namespace_and_template_flag_validation(self):
        task_args = aq.build_parser().parse_args(["task", "add", "--title", "x"])
        self.assertFalse(hasattr(task_args, "workflow_command"))
        workflow_args = aq.build_parser().parse_args([
            "workflow", "add", "--template", "adversarial-review",
            "--title", "x", "--resource", "r",
        ])
        self.assertEqual("add", workflow_args.workflow_command)
        self.assertEqual("adversarial-review", workflow_args.template)
        invalid = run_cli("workflow", "add", "--template", "not-real")
        self.assertEqual(2, invalid.returncode)
        self.assertEqual("", invalid.stdout)

    def test_cli_accepts_git_task_and_workflow_opt_in(self):
        self.init()
        task = self.add(
            "Git task",
            "--git-commit",
            "--resource",
            "file:src/api.py",
        )["task"]
        self.assertEqual("commit", task["git_mode"])

        adversarial = self.json_output(
            self.cli(
                "workflow",
                "add",
                "--template",
                "adversarial-review",
                "--title",
                "Review Git",
                "--resource",
                "dir:src/review/",
                "--git-commit",
            )
        )
        state = aq.load_state(self.queue)
        self.assertEqual(
            ["commit", None, None, "commit", None],
            [
                state["tasks"][task_id]["git_mode"]
                for task_id in adversarial["task_ids"]
            ],
        )

        input_path = Path(self.temporary.name) / "git-shards.json"
        input_path.write_text(
            json.dumps(
                {
                    "title": "Git shards",
                    "shards": [["file:src/a.py"], ["file:src/b.py"]],
                    "git_commit": True,
                }
            ),
            encoding="utf-8",
        )
        parallel = self.json_output(
            self.cli(
                "workflow",
                "add",
                "--template",
                "parallel-shards",
                "--from-json",
                input_path,
            )
        )
        state = aq.load_state(self.queue)
        self.assertEqual(
            ["commit", "commit", "commit", None],
            [
                state["tasks"][task_id]["git_mode"]
                for task_id in parallel["task_ids"]
            ],
        )

    def test_lifecycle_commands_and_token_redaction(self):
        self.init(retry_backoff=1)
        self.add("work")
        claim = self.json_output(self.cli("claim", "--agent", "a"))
        token = claim["lease_token"]
        self.assertNotIn(token, self.queue.with_suffix(".tsv").read_text())

        heartbeat = self.json_output(
            self.cli("heartbeat", "--task", "T-000001", "--agent", "a", "--token", token)
        )
        self.assertNotIn(token, json.dumps(heartbeat))
        wrong = self.cli(
            "complete", "--task", "T-000001", "--agent", "a", "--token", "wrong", "--summary", "done"
        )
        self.assertEqual(5, wrong.returncode)
        complete = self.json_output(
            self.cli("complete", "--task", "T-000001", "--agent", "a", "--token", token, "--summary", "done", "--artifact", "out.txt")
        )
        self.assertEqual("completed", complete["task"]["status"])
        none = self.cli("claim", "--agent", "b")
        self.assertEqual(3, none.returncode)

    def test_cli_claim_binds_current_git_worktree_and_redacts_private_paths(self):
        root = self.make_git_repo()
        self.init()
        task = self.add(
            "Git claim",
            "--git-commit",
            "--resource",
            "file:scoped.txt",
        )["task"]

        result = run_cli(
            "--queue",
            self.queue,
            "claim",
            "--agent",
            "git-worker",
            "--task",
            task["id"],
            cwd=root,
        )
        output = self.json_output(result)

        self.assertEqual(task["id"], output["task"]["id"])
        self.assertNotIn(str(root), result.stdout)
        self.assertNotIn(str(root / ".git"), result.stdout)
        self.assertEqual(
            "refs/heads/main",
            output["task"]["claim"]["git"]["branch"],
        )
        stored = aq.load_state(self.queue)["tasks"][task["id"]]
        self.assertEqual(str(root.resolve()), stored["claim"]["git"]["worktree"])

    def test_cli_claim_releases_lease_when_git_snapshot_drifts(self):
        self.init()
        task = self.add(
            "Git claim drift",
            "--git-commit",
            "--resource",
            "file:scoped.txt",
        )["task"]
        before = git_snapshot()
        after = git_snapshot(head="changed-after-claim")
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(
            aq.gq, "observe", side_effect=(before, after)
        ), mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="PRIVATE-LEASE-TOKEN"
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = aq.main([
                "--queue",
                str(self.queue),
                "claim",
                "--agent",
                "git-worker",
                "--task",
                task["id"],
            ])

        self.assertEqual(2, code)
        self.assertEqual("", stdout.getvalue())
        self.assertIn("git_claim_drift", stderr.getvalue())
        self.assertNotIn("PRIVATE-LEASE-TOKEN", stderr.getvalue())
        self.assertNotIn("/private/", stderr.getvalue())
        stored = aq.load_state(self.queue)["tasks"][task["id"]]
        self.assertEqual("pending", stored["status"])
        self.assertIsNone(stored["claim"])

    def test_cli_requires_explicit_resume_and_restores_original_base(self):
        self.init(retry_backoff=1)
        task = self.add(
            "Git recovery",
            "--git-commit",
            "--resource",
            "dir:src/",
        )["task"]
        observation = git_snapshot(head="a" * 40)
        started_at = aq.load_state(self.queue)["tasks"][task["id"]][
            "updated_at"
        ]
        expires_at = aq.add_seconds(started_at, 60)
        resume_at = aq.add_seconds(expires_at, 2)

        def claim_then_expire(state):
            claimed = aq.claim_task(
                state,
                "first-worker",
                now=started_at,
                lease_seconds=60,
                git_observation=observation,
                task_id=task["id"],
            )
            aq.sweep_expired(
                state, now=claimed["expires_at"]
            )

        aq.mutate_queue(
            self.queue,
            claim_then_expire,
            auto_sweep=False,
        )

        def invoke(*arguments):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                aq.gq, "observe", return_value=observation
            ), mock.patch.object(
                aq, "utc_now", return_value=resume_at
            ), redirect_stdout(stdout), redirect_stderr(stderr):
                code = aq.main([
                    "--queue", str(self.queue), "claim", *arguments
                ])
            return code, stdout.getvalue(), stderr.getvalue()

        code, stdout, stderr = invoke(
            "--agent", "ordinary", "--task", task["id"]
        )
        self.assertEqual(2, code)
        self.assertEqual("", stdout)
        self.assertIn("git_recovery_required", stderr)

        code, stdout, stderr = invoke(
            "--agent", "resumer", "--task", task["id"], "--resume-git"
        )
        self.assertEqual(0, code, stderr)
        output = json.loads(stdout)
        self.assertEqual(
            observation["head"], output["task"]["claim"]["git"]["base"]
        )
        self.assertIsNone(output["task"]["git_recovery"])
        stored = aq.load_state(self.queue)["tasks"][task["id"]]
        self.assertEqual(
            observation["head"], stored["claim"]["git"]["base"]
        )

    def test_cli_resume_drift_restores_recovery_binding(self):
        self.init(retry_backoff=1)
        task = self.add(
            "Git recovery drift",
            "--git-commit",
            "--resource",
            "dir:src/",
        )["task"]
        observation = git_snapshot(head="a" * 40)
        started_at = aq.load_state(self.queue)["tasks"][task["id"]][
            "updated_at"
        ]
        expires_at = aq.add_seconds(started_at, 60)
        resume_at = aq.add_seconds(expires_at, 2)

        def claim_then_expire(state):
            claimed = aq.claim_task(
                state,
                "first-worker",
                now=started_at,
                lease_seconds=60,
                git_observation=observation,
                task_id=task["id"],
            )
            aq.sweep_expired(state, now=claimed["expires_at"])

        aq.mutate_queue(
            self.queue,
            claim_then_expire,
            auto_sweep=False,
        )
        recovery = copy.deepcopy(
            aq.load_state(self.queue)["tasks"][task["id"]]["git_recovery"]
        )
        drifted = dict(observation, head="b" * 40)
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(
            aq.gq, "observe", side_effect=(observation, drifted)
        ), mock.patch.object(
            aq, "utc_now", return_value=resume_at
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = aq.main([
                "--queue",
                str(self.queue),
                "claim",
                "--agent",
                "resumer",
                "--task",
                task["id"],
                "--resume-git",
            ])

        self.assertEqual(2, code)
        self.assertEqual("", stdout.getvalue())
        self.assertIn("git_claim_drift", stderr.getvalue())
        stored = aq.load_state(self.queue)["tasks"][task["id"]]
        self.assertEqual("pending", stored["status"])
        self.assertIsNone(stored["claim"])
        self.assertEqual(recovery, stored["git_recovery"])

    def test_cli_git_release_rejects_advanced_head(self):
        root = self.make_git_repo()
        self.init()
        task = self.add(
            "Git release",
            "--git-commit",
            "--resource",
            "file:scoped.txt",
        )["task"]
        claim = self.json_output(run_cli(
            "--queue",
            self.queue,
            "claim",
            "--agent",
            "git-worker",
            "--task",
            task["id"],
            cwd=root,
        ))
        base = claim["task"]["claim"]["git"]["base"]
        (root / "scoped.txt").write_text("committed\n", encoding="utf-8")
        subprocess.run(
            ["git", "-C", str(root), "add", "scoped.txt"],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "-C", str(root), "commit", "-m", "advance"],
            check=True,
            capture_output=True,
            text=True,
        )
        release_args = (
            "--queue", self.queue,
            "release",
            "--task", task["id"],
            "--agent", "git-worker",
            "--token", claim["lease_token"],
        )

        rejected = run_cli(*release_args, cwd=root)
        self.assertEqual(2, rejected.returncode)
        self.assertIn("git_head_mismatch", rejected.stderr)
        self.assertEqual(
            "leased",
            aq.load_state(self.queue)["tasks"][task["id"]]["status"],
        )

        subprocess.run(
            ["git", "-C", str(root), "reset", "--hard", base],
            check=True,
            capture_output=True,
            text=True,
        )
        released = self.json_output(run_cli(*release_args, cwd=root))
        self.assertEqual("pending", released["task"]["status"])
        self.assertIsNone(released["task"]["git_recovery"])

    def test_cli_complete_validates_commit_and_persists_counts_only(self):
        root = self.make_git_repo()
        self.init()
        task = self.add(
            "Git complete",
            "--git-commit",
            "--resource",
            "file:scoped.txt",
        )["task"]
        claim = self.json_output(run_cli(
            "--queue",
            self.queue,
            "claim",
            "--agent",
            "git-worker",
            "--task",
            task["id"],
            cwd=root,
        ))
        (root / "scoped.txt").write_text("done\n", encoding="utf-8")
        subprocess.run(
            ["git", "-C", str(root), "add", "scoped.txt"],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "-C", str(root), "commit", "-m", "complete"],
            check=True,
            capture_output=True,
            text=True,
        )
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        result = run_cli(
            "--queue",
            self.queue,
            "complete",
            "--task",
            task["id"],
            "--agent",
            "git-worker",
            "--token",
            claim["lease_token"],
            "--summary",
            "done",
            "--commit",
            head,
            cwd=root,
        )
        output = self.json_output(result)

        self.assertEqual(1, output["task"]["result"]["git"]["commit_count"])
        self.assertEqual(
            1, output["task"]["result"]["git"]["changed_path_count"]
        )
        self.assertNotIn('"changed_paths"', result.stdout)
        stored = aq.load_state(self.queue)["tasks"][task["id"]]
        self.assertNotIn("changed_paths", stored["result"]["git"])

    def test_cli_complete_rejects_git_flags_for_generic_tasks(self):
        self.init()
        self.add("Generic")
        claim = self.json_output(self.cli("claim", "--agent", "worker"))

        result = self.cli(
            "complete",
            "--task",
            "T-000001",
            "--agent",
            "worker",
            "--token",
            claim["lease_token"],
            "--summary",
            "done",
            "--no-change",
        )

        self.assertEqual(2, result.returncode)
        stored = aq.load_state(self.queue)["tasks"]["T-000001"]
        self.assertEqual("leased", stored["status"])

    def test_cli_git_complete_requires_mode_and_accepts_no_change(self):
        root = self.make_git_repo()
        self.init()
        task = self.add(
            "Git no change",
            "--git-commit",
            "--resource",
            "file:base.txt",
        )["task"]
        claim = self.json_output(run_cli(
            "--queue",
            self.queue,
            "claim",
            "--agent",
            "git-worker",
            "--task",
            task["id"],
            cwd=root,
        ))
        common = (
            "--task", task["id"],
            "--agent", "git-worker",
            "--token", claim["lease_token"],
            "--summary", "done",
        )

        missing = run_cli(
            "--queue", self.queue, "complete", *common, cwd=root
        )
        self.assertEqual(2, missing.returncode)
        self.assertIn("git_commit_required", missing.stderr)
        self.assertEqual(
            "leased",
            aq.load_state(self.queue)["tasks"][task["id"]]["status"],
        )

        completed = self.json_output(run_cli(
            "--queue",
            self.queue,
            "complete",
            *common,
            "--no-change",
            cwd=root,
        ))
        evidence = completed["task"]["result"]["git"]
        self.assertEqual(0, evidence["commit_count"])
        self.assertEqual(0, evidence["changed_path_count"])

    def test_cli_completion_drift_warns_without_reopening_task(self):
        root = self.make_git_repo()
        self.init()
        task = self.add(
            "Git warning",
            "--git-commit",
            "--resource",
            "file:base.txt",
        )["task"]
        claim = self.json_output(run_cli(
            "--queue",
            self.queue,
            "claim",
            "--agent",
            "git-worker",
            "--task",
            task["id"],
            cwd=root,
        ))
        binding = aq.load_state(self.queue)["tasks"][task["id"]]["claim"]["git"]
        evidence = {
            "branch": binding["branch"],
            "base": binding["base"],
            "head": binding["base"],
            "commit_count": 0,
            "changed_path_count": 0,
        }
        drifted = {
            "common_dir": binding["common_dir"],
            "worktree": binding["worktree"],
            "repository_id": binding["repository_id"],
            "worktree_id": binding["worktree_id"],
            "branch": binding["branch"],
            "head": "f" * 40,
            "attached": True,
            "clean": True,
        }
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(
            aq.gq, "validate_completion", return_value=evidence
        ), mock.patch.object(
            aq.gq, "observe", return_value=drifted
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = aq.main([
                "--queue",
                str(self.queue),
                "complete",
                "--task",
                task["id"],
                "--agent",
                "git-worker",
                "--token",
                claim["lease_token"],
                "--summary",
                "done",
                "--no-change",
            ])

        self.assertEqual(0, code, stderr.getvalue())
        output = json.loads(stdout.getvalue())
        self.assertEqual("git_completion_drift", output["warning"]["code"])
        self.assertEqual("completed", output["task"]["status"])
        self.assertEqual(
            "completed",
            aq.load_state(self.queue)["tasks"][task["id"]]["status"],
        )
        self.assertNotIn(claim["lease_token"], stdout.getvalue())
        self.assertNotIn(str(root), stdout.getvalue())

    def test_generated_dash_token_round_trips_as_separate_cli_argument(self):
        self.init()
        self.add("dash token")

        def invoke(*arguments):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                try:
                    code = aq.main(["--queue", str(self.queue), *arguments])
                except SystemExit as error:
                    code = error.code
            return code, stdout.getvalue(), stderr.getvalue()

        with mock.patch.object(
            aq.secrets, "token_urlsafe", return_value="-forced-token"
        ):
            code, stdout, stderr = invoke("claim", "--agent", "agent")
        self.assertEqual(0, code, stderr)
        token = json.loads(stdout)["lease_token"]
        self.assertIn("-forced-token", token)

        code, _stdout, stderr = invoke(
            "heartbeat", "--task", "T-000001", "--agent", "agent",
            "--token", token,
        )
        self.assertEqual(0, code, stderr)
        code, status_stdout, stderr = invoke("status", "--format", "json")
        self.assertEqual(0, code, stderr)
        self.assertNotIn(token, status_stdout)
        code, _stdout, stderr = invoke(
            "complete", "--task", "T-000001", "--agent", "agent",
            "--token", token, "--summary", "done",
        )
        self.assertEqual(0, code, stderr)
        code, events_stdout, stderr = invoke("events")
        self.assertEqual(0, code, stderr)
        self.assertNotIn(token, events_stdout)

    def test_release_fail_retry_block_unblock_cancel_and_sweep_commands(self):
        self.init(retry_backoff=1)
        for title in ("release", "fail", "admin"):
            self.add(title)
        first = self.json_output(self.cli("claim", "--agent", "a"))
        released = self.json_output(
            self.cli("release", "--task", first["task"]["id"], "--agent", "a", "--token", first["lease_token"])
        )
        self.assertEqual("pending", released["task"]["status"])
        reclaimed = self.json_output(self.cli("claim", "--agent", "a"))
        failed = self.json_output(
            self.cli("fail", "--task", reclaimed["task"]["id"], "--agent", "a", "--token", reclaimed["lease_token"], "--error", "bad", "--terminal")
        )
        self.assertEqual("failed", failed["task"]["status"])
        retried = self.json_output(self.cli("retry", reclaimed["task"]["id"]))
        self.assertEqual("pending", retried["task"]["status"])
        blocked = self.json_output(self.cli("block", "T-000002", "--reason", "wait"))
        self.assertEqual("blocked", blocked["task"]["status"])
        self.assertEqual("pending", self.json_output(self.cli("unblock", "T-000002"))["task"]["status"])
        self.assertEqual("cancelled", self.json_output(self.cli("cancel", "T-000003"))["task"]["status"])
        self.assertTrue(self.json_output(self.cli("sweep"))["ok"])

    def test_cli_lock_timeout_is_code_four(self):
        self.init()
        with aq.QueueLock(self.queue, lock_timeout=1, stale_seconds=30):
            result = self.cli("status", "--format", "json", timeout=8)
        self.assertEqual(4, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("timed out", result.stderr)

    def test_conflicting_resource_is_claimed_only_once_concurrently(self):
        self.init()
        batch_path = Path(self.temporary.name) / "resources.json"
        batch_path.write_text(json.dumps([
            {"title": "shared one", "resources": ["shared"]},
            {"title": "shared two", "resources": ["shared"]},
            {"title": "disjoint", "resources": ["other"]},
        ]))
        self.json_output(self.cli("task", "add-batch", "--from-json", batch_path))
        processes = [
            subprocess.Popen(
                [sys.executable, str(SCRIPT_PATH), "--queue", str(self.queue), "claim", "--agent", f"r-{index}"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            ) for index in range(4)
        ]
        outputs = communicate_all(processes, 15)
        self.assertTrue(all(process.returncode in (0, 3) for process in processes), outputs)
        claims = [json.loads(stdout) for process, (stdout, _stderr) in zip(processes, outputs) if process.returncode == 0]
        self.assertEqual(2, len(claims))
        resources = [claim["task"]["resources"] for claim in claims]
        self.assertEqual(1, sum("shared" in value for value in resources))
        self.assertEqual(1, sum("other" in value for value in resources))

    def test_status_events_export_and_crash_repair_do_not_bump_revision(self):
        self.init()
        self.add("work")
        revision = aq.load_state(self.queue)["revision"]
        self.queue.with_suffix(".tsv").write_text(
            f"# queue_revision: {revision}\ncorrupt\n", encoding="utf-8"
        )
        status = self.json_output(self.cli("status", "--format", "json"))
        self.assertEqual("demo", status["queue_id"])
        self.assertEqual(revision, status["revision"])
        self.assertEqual(1, len(status["rows"]))
        self.assertEqual(revision, aq.tsv_revision(self.queue.with_suffix(".tsv")))
        self.assertIn("T-000001", self.queue.with_suffix(".tsv").read_text())
        events = self.json_output(self.cli("events"))
        self.assertNotIn("lease_token", json.dumps(events))
        exported = self.cli("export", "--format", "tsv")
        self.assertEqual(0, exported.returncode)
        self.assertEqual(self.queue.with_suffix(".tsv").read_text(), exported.stdout)

    def test_export_reuses_projection_rendered_inside_transaction_lock(self):
        self.init()
        self.add("exported")
        lock_path = Path(str(self.queue) + ".lock")
        render_lock_states = []
        real_render = aq._render_projection

        def tracked_render(state, now):
            render_lock_states.append(lock_path.is_dir())
            return real_render(state, now)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(aq, "_render_projection", tracked_render):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = aq.main([
                    "--queue", str(self.queue), "export", "--format", "tsv"
                ])

        self.assertEqual(0, code, stderr.getvalue())
        self.assertEqual(self.queue.with_suffix(".tsv").read_text(), stdout.getvalue())
        self.assertTrue(render_lock_states)
        self.assertTrue(all(render_lock_states), render_lock_states)

    def test_strict_json_corruption_is_code_six_and_never_written(self):
        self.queue.write_text('{"revision": NaN}', encoding="utf-8")
        before = self.queue.read_bytes()
        result = self.cli("status", "--format", "json")
        self.assertEqual(6, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertEqual(before, self.queue.read_bytes())

    def test_explicit_sweep_returns_expired_ids_and_commits_once(self):
        state = aq.new_state("demo", aq.fixed_config())
        task = aq.add_task(state, {"title": "expired"})
        for target in (state, state["tasks"][task["id"]]):
            target["created_at"] = "2020-01-01T00:00:00Z"
            target["updated_at"] = "2020-01-01T00:00:00Z"
        aq.claim_task(
            state, "agent", now="2020-01-01T00:00:00Z", lease_seconds=1
        )
        aq.commit_state(self.queue, state, "2020-01-01T00:00:00Z")

        result = self.json_output(self.cli("sweep"))
        self.assertEqual(["T-000001"], result["swept"])
        persisted = aq.load_state(self.queue)
        self.assertEqual(2, persisted["revision"])
        expiry_events = [
            event for event in persisted["events"]
            if event["type"] == "task.lease_expired"
        ]
        self.assertEqual(1, len(expiry_events))
        self.assertEqual(2, expiry_events[0]["revision"])

    def test_user_task_semantic_errors_are_code_two_and_atomic(self):
        blank_queue = Path(self.temporary.name) / "blank-id.json"
        blank_init = run_cli(
            "--queue", blank_queue, "init", "--id", "   "
        )
        self.assertEqual(2, blank_init.returncode)
        self.assertFalse(blank_queue.exists())
        self.init()
        self.add("existing")
        cases = (
            ("task", "add", "--title", "   "),
            ("task", "add", "--title", "missing dep", "--depends-on", "T-999999"),
            ("task", "show", "T-999999"),
            ("retry", "T-999999"),
            ("block", "T-999999", "--reason", "wait"),
            ("unblock", "not-a-task"),
            ("cancel", "T-999999"),
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                before_json = self.queue.read_bytes()
                before_tsv = self.queue.with_suffix(".tsv").read_bytes()
                result = self.cli(*arguments)
                self.assertEqual(2, result.returncode, result.stderr)
                self.assertEqual("", result.stdout)
                self.assertNotEqual("", result.stderr)
                self.assertEqual(before_json, self.queue.read_bytes())
                self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

    def test_status_rejects_unknown_state_with_argparse_code_two(self):
        result = self.cli(
            "status", "--state", "definitely-not-a-state", "--format", "json"
        )
        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("invalid choice", result.stderr)

    def test_cli_text_limit_is_code_two_but_persisted_corruption_is_code_six(self):
        self.init()
        over = "한" * ((aq.MAX_TEXT_BYTES // 3) + 1)
        before_json = self.queue.read_bytes()
        before_tsv = self.queue.with_suffix(".tsv").read_bytes()
        result = self.cli("task", "add", "--title", "too large", "--description", over)
        self.assertEqual(2, result.returncode, result.stderr)
        self.assertEqual("", result.stdout)
        self.assertEqual(before_json, self.queue.read_bytes())
        self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

        state = aq.load_state(self.queue)
        created = aq.add_task(state, {"title": "corrupt"})
        state["tasks"][created["id"]]["description"] = "x" * (aq.MAX_TEXT_BYTES + 1)
        self.queue.write_text(json.dumps(state), encoding="utf-8")
        corrupt = self.cli("status", "--format", "json")
        self.assertEqual(6, corrupt.returncode)
        self.assertEqual("", corrupt.stdout)

    def test_doctor_cli_json_exit_codes_and_repair(self):
        self.init()
        healthy = self.cli("doctor")
        self.assertEqual(0, healthy.returncode, healthy.stderr)
        self.assertEqual("", healthy.stderr)
        self.assertTrue(json.loads(healthy.stdout)["ok"])

        self.queue.with_suffix(".tsv").unlink()
        broken = self.cli("doctor")
        self.assertEqual(6, broken.returncode)
        self.assertEqual("", broken.stderr)
        self.assertFalse(json.loads(broken.stdout)["ok"])
        repaired = self.cli("doctor", "--repair")
        self.assertEqual(0, repaired.returncode, repaired.stderr)
        self.assertTrue(json.loads(repaired.stdout)["ok"])

        self.queue.write_text("{bad", encoding="utf-8")
        before_tsv = self.queue.with_suffix(".tsv").read_bytes()
        corrupt = self.cli("doctor", "--repair")
        self.assertEqual(6, corrupt.returncode)
        self.assertEqual("", corrupt.stderr)
        self.assertFalse(json.loads(corrupt.stdout)["ok"])
        self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

    def test_compact_cli_commits_once_regenerates_tsv_and_noop_does_not_churn(self):
        state = aq.new_state("demo", aq.fixed_config())
        created = aq.add_task(state, {"title": "old"})
        task = state["tasks"][created["id"]]
        task["created_at"] = task["updated_at"] = "2020-01-01T00:00:00Z"
        task["status"] = "completed"
        task["result"] = canonical_result()
        aq.write_json(self.queue, state)
        aq.atomic_write_text(
            self.queue.with_suffix(".tsv"), aq.render_tsv(state, state["updated_at"])
        )

        compacted = self.json_output(
            self.cli("compact", "--before", "2021-01-01")
        )
        self.assertEqual(1, compacted["removed_task_count"])
        persisted = aq.load_state(self.queue)
        self.assertEqual(1, persisted["revision"])
        self.assertEqual("queue.compacted", persisted["events"][-1]["type"])
        self.assertEqual(1, aq.tsv_revision(self.queue.with_suffix(".tsv")))

        before_json = self.queue.read_bytes()
        before_tsv = self.queue.with_suffix(".tsv").read_bytes()
        noop = self.json_output(
            self.cli("compact", "--before", "2021-01-01T00:00:00Z")
        )
        self.assertEqual(0, noop["removed_task_count"])
        self.assertEqual(0, noop["removed_event_count"])
        self.assertEqual(before_json, self.queue.read_bytes())
        self.assertEqual(before_tsv, self.queue.with_suffix(".tsv").read_bytes())

        invalid = self.cli("compact", "--before", "not-a-date")
        self.assertEqual(2, invalid.returncode)
        self.assertEqual(before_json, self.queue.read_bytes())

    def test_parser_help_and_invalid_arguments_use_argparse_code_two(self):
        for arguments in (
            ("--help",), ("task", "--help"), ("workflow", "--help"),
            ("workflow", "add", "--help"), ("status", "--help"),
            ("workflow", "add"), ("compact",),
        ):
            result = run_cli(*arguments)
            if "--help" in arguments:
                self.assertEqual(0, result.returncode, arguments)
            else:
                self.assertEqual(2, result.returncode, arguments)

    def test_sixteen_processes_claim_unique_tasks_without_residue(self):
        self.init(lease_seconds=30)
        batch_path = Path(self.temporary.name) / "sixteen.json"
        batch_path.write_text(json.dumps([{"title": f"task {index}"} for index in range(16)]))
        self.json_output(self.cli("task", "add-batch", "--from-json", batch_path))
        processes = [
            subprocess.Popen(
                [sys.executable, str(SCRIPT_PATH), "--queue", str(self.queue), "claim", "--agent", f"a-{index}"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for index in range(16)
        ]
        outputs = communicate_all(processes, 20)
        self.assertTrue(all(process.returncode == 0 for process in processes), outputs)
        claims = [json.loads(stdout) for stdout, _stderr in outputs]
        self.assertEqual(16, len({claim["task"]["id"] for claim in claims}))
        self.assertEqual(16, len({claim["lease_token"] for claim in claims}))
        state = aq.load_state(self.queue)
        self.assertEqual(17, state["revision"])
        self.assertEqual(16, len([task for task in state["tasks"].values() if task["status"] == "leased"]))
        self.assertFalse(Path(str(self.queue) + ".lock").exists())
        self.assertTrue(Path(str(self.queue) + ".lock.guard").is_file())
        self.assertEqual([], list(self.queue.parent.glob("*.tmp")))


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
                "T-000002",
                "W-000001",
                "leased",
                title="Working",
                assignee="agent-1",
                lease_until="2026-07-10T06:01:30Z",
            ),
            self.row("T-000003", "", "ready", title="Loose"),
        ]

        value = qd.build_snapshot("demo", 7, rows, self.NOW)

        self.assertEqual("demo", value["queue_id"])
        self.assertEqual(7, value["revision"])
        self.assertEqual(
            {
                "total": 3,
                "completed": 1,
                "active": 1,
                "ready": 1,
                "attention": 1,
            },
            value["counts"],
        )
        self.assertEqual(
            ["W-000001", "unassigned"],
            [workflow["id"] for workflow in value["workflows"]],
        )
        self.assertEqual(50, value["workflows"][0]["progress_percent"])
        self.assertEqual("lease_expiring", value["warnings"][0]["kind"])

    def test_projection_warning_precedence_and_redaction_boundary(self):
        rows = [
            self.row("T-000004", "W-1", "blocked", blocked_by="T-000001"),
            self.row(
                "T-000003",
                "W-1",
                "dependency_failed",
                blocked_by="T-000002",
            ),
            self.row("T-000002", "W-1", "failed"),
            self.row(
                "T-000001",
                "W-1",
                "resource_conflict",
                blocked_by="T-000009",
                resources="repo",
            ),
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
            {
                "seq": 1,
                "at": self.NOW,
                "type": "task.added",
                "actor": "operator",
                "task_id": "T-000001",
                "revision": 1,
                "details": {},
            },
            {
                "seq": 2,
                "at": self.NOW,
                "type": "task.claimed",
                "actor": "agent-1",
                "task_id": "T-000001",
                "revision": 2,
                "details": {
                    "lease_token": "must-not-leak",
                    "agent_id": "agent-1",
                },
            },
        ]

        value = qd.events_after(events, 1)

        self.assertEqual([2], [event["seq"] for event in value])
        self.assertNotIn("lease_token", repr(value))
        value[0]["details"]["agent_id"] = "changed"
        self.assertEqual("agent-1", events[1]["details"]["agent_id"])


class DashboardHttpTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = {
            "queue_id": "demo",
            "revision": 3,
            "workflows": [],
        }
        self.events = [
            {"seq": 3, "type": "task.added", "details": {}}
        ]
        self.server = qd.create_server(
            "127.0.0.1",
            0,
            "fixed-token",
            2,
            revision_loader=lambda: 3,
            snapshot_loader=lambda: self.snapshot,
            events_loader=lambda after: [
                event for event in self.events if event["seq"] > after
            ],
            asset_dir=SCRIPT_DIR / "dashboard",
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, host=None):
        self.assertEqual("GET", method)
        return request_server(self.server, path, host=host)

    def test_token_host_and_fixed_routes_are_enforced(self):
        self.assertEqual(
            404,
            self.request("GET", "/wrong/api/revision")[0],
        )
        self.assertEqual(
            400,
            self.request(
                "GET",
                "/fixed-token/api/revision",
                host="evil.test",
            )[0],
        )
        status, _headers, body = self.request(
            "GET",
            "/fixed-token/api/revision",
        )
        self.assertEqual(200, status)
        self.assertEqual(
            {"revision": 3, "interval": 2},
            json.loads(body),
        )
        status, _headers, body = self.request(
            "GET",
            "/fixed-token/api/events?after=2",
        )
        self.assertEqual(
            [3],
            [event["seq"] for event in json.loads(body)["events"]],
        )
        self.assertEqual(
            404,
            self.request(
                "GET",
                "/fixed-token/assets/../agent_queue.py",
            )[0],
        )

    def test_token_prefix_uses_constant_time_comparison(self):
        with mock.patch.object(
            qd.secrets,
            "compare_digest",
            wraps=qd.secrets.compare_digest,
        ) as compare:
            self.assertEqual(
                200,
                self.request(
                    "GET",
                    "/fixed-token/api/revision",
                )[0],
            )

        compare.assert_called_once_with(
            "/fixed-token/",
            "/fixed-token/",
        )

    def test_invalid_event_cursor_is_a_bad_request(self):
        status, _headers, body = self.request(
            "GET",
            "/fixed-token/api/events?after=invalid",
        )

        self.assertEqual(400, status)
        self.assertEqual(
            {"error": "invalid after parameter"},
            json.loads(body),
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
                self.assertEqual(
                    "no-referrer",
                    headers["Referrer-Policy"],
                )
                self.assertEqual(
                    "nosniff",
                    headers["X-Content-Type-Options"],
                )
                self.assertEqual("DENY", headers["X-Frame-Options"])
                self.assertIn(
                    "default-src 'none'",
                    headers["Content-Security-Policy"],
                )
                self.assertNotIn("Access-Control-Allow-Origin", headers)

    def test_health_and_loader_errors_do_not_kill_server(self):
        self.server.dashboard.snapshot_loader = lambda: (
            _ for _ in ()
        ).throw(
            qd.DashboardDataUnavailable(
                "private /tmp/queue.json detail"
            )
        )
        status, _headers, body = self.request(
            "GET",
            "/fixed-token/api/snapshot",
        )
        self.assertEqual(503, status)
        self.assertEqual(
            {"error": "queue temporarily unavailable"},
            json.loads(body),
        )
        self.assertNotIn(b"/tmp/queue.json", body)
        self.assertEqual(
            200,
            self.request("GET", "/fixed-token/api/health")[0],
        )


class DashboardCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.queue = Path(self.temporary.name) / "queue.json"
        aq.initialize_queue(self.queue, "demo", aq.fixed_config())
        result = run_cli(
            "--queue",
            self.queue,
            "task",
            "add",
            "--title",
            "Visible",
        )
        self.assertEqual(0, result.returncode, result.stderr)

    def test_parser_exposes_bounded_serve_options(self):
        args = aq.build_parser().parse_args(
            [
                "serve",
                "--open",
                "--port",
                "0",
                "--interval",
                "1",
                "--idle-timeout",
                "30",
            ]
        )
        self.assertTrue(args.open_browser)
        self.assertEqual("127.0.0.1", args.host)
        self.assertEqual(0, args.port)
        self.assertEqual(1, args.interval)
        self.assertEqual(30, args.idle_timeout)
        for arguments in (
            ("serve", "--port", "-1"),
            ("serve", "--port", "65536"),
            ("serve", "--host", "0.0.0.0"),
        ):
            self.assertEqual(2, run_cli(*arguments).returncode)

    def test_dashboard_loaders_return_current_sanitized_data(self):
        loaders = aq.dashboard_loaders(self.queue)

        revision = loaders.revision()
        snapshot = loaders.snapshot()
        events = loaders.events(0)

        self.assertEqual(snapshot["revision"], revision)
        self.assertEqual(
            "Visible",
            snapshot["workflows"][0]["tasks"][0]["title"],
        )
        serialized = json.dumps(snapshot) + json.dumps(events)
        self.assertNotIn("lease_token", serialized)

    def test_serve_prints_ready_url_and_stops_on_sigint(self):
        process = subprocess.Popen(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--queue",
                str(self.queue),
                "serve",
                "--port",
                "0",
                "--idle-timeout",
                "30",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(
            lambda: process.poll() is None and process.kill()
        )
        ready = process.stdout.readline().strip()
        self.assertRegex(
            ready,
            r"^http://127\.0\.0\.1:\d+/[A-Za-z0-9_-]+/$",
        )
        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=3)
        self.assertEqual(0, process.returncode, stderr)
        self.assertIn("dashboard stopped", stdout)


class DashboardAssetTests(unittest.TestCase):
    def setUp(self):
        self.assets = SCRIPT_DIR / "dashboard"

    def test_assets_are_build_free_local_and_accessible(self):
        html = (self.assets / "index.html").read_text(encoding="utf-8")
        css = (self.assets / "dashboard.css").read_text(encoding="utf-8")
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )
        combined = html + css + javascript
        self.assertIn('id="workflow-view"', html)
        self.assertIn('id="activity-view"', html)
        self.assertIn('aria-live="polite"', html)
        self.assertIn('>Queue</button>', html)
        self.assertIn("color-scheme: light", css)
        self.assertIn(".queue-table", css)
        self.assertIn(".task-primary", css)
        self.assertIn(".task-meta", css)
        self.assertIn(".active-row", css)
        self.assertIn("@media (max-width: 760px)", css)
        self.assertNotIn(".summary-grid", css)
        self.assertNotIn(".workflow-header", css)
        self.assertNotRegex(combined, r"https?://")

    def test_client_polls_revision_and_uses_safe_dom_apis(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )
        for required in (
            "api/revision",
            "api/snapshot",
            "api/events?after=",
            "textContent",
            "setTimeout",
            "data-task-id",
            "manual-refresh",
            "last-updated",
            "remainingTime",
            "updatedTime",
            "retryDelay",
            "Retrying",
            "Stopped",
            "queue temporarily unavailable",
            "dashboard server ended",
        ):
            self.assertIn(required, javascript)
        for forbidden in (
            "innerHTML",
            "insertAdjacentHTML",
            "eval(",
            "document.write",
        ):
            self.assertNotIn(forbidden, javascript)

    def test_client_commits_revision_after_events_refresh(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )
        poll = javascript[javascript.index("async function poll()") :]

        self.assertLess(
            poll.index("await refreshEvents();"),
            poll.index("state.revision = snapshot.revision;"),
        )

    def test_client_renders_semantic_two_line_queue_tables(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'element("table", "queue-table")',
            'element("thead")',
            'element("tbody")',
            'heading.scope = "col"',
            '["Status", "Task", "Assignee", "Timing"]',
            'element("div", "task-primary")',
            'element("div", "task-meta")',
            'row.classList.add("active-row")',
        ):
            self.assertIn(required, javascript)
        for forbidden in (
            'element("details"',
            "openTasks",
            'element("article", "card"',
            'element("header", "workflow-header"',
        ):
            self.assertNotIn(forbidden, javascript)

    def test_client_renders_one_compact_queue_summary(self):
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'summary.className = "summary-line"',
            '"summary-progress"',
            '"summary-counts"',
            'element("progress", "queue-progress")',
            'const waiting = Math.max(',
        ):
            self.assertIn(required, javascript)
        for forbidden in (
            'summary.className = "summary-grid"',
            'element("article", "card"',
        ):
            self.assertNotIn(forbidden, javascript)

    def test_client_uses_empty_template_for_empty_workflows(self):
        html = (self.assets / "index.html").read_text(encoding="utf-8")
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        self.assertIn('id="empty-template"', html)
        self.assertIn('byId("empty-template")', javascript)
        self.assertIn("cloneNode(true)", javascript)

    def test_table_redesign_preserves_live_state_contracts(self):
        html = (self.assets / "index.html").read_text(encoding="utf-8")
        css = (self.assets / "dashboard.css").read_text(encoding="utf-8")
        javascript = (self.assets / "dashboard.js").read_text(
            encoding="utf-8"
        )

        for required in (
            'setConnection("Live", "live")',
            '"Retrying · queue temporarily unavailable"',
            '"Stopped · dashboard server ended"',
            'row.classList.add("changed")',
            'byId("empty-template")',
            'byId("activity-list")',
            'byId("manual-refresh")',
        ):
            self.assertIn(required, javascript)
        self.assertIn('aria-live="polite"', html)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)

    def test_plan_uses_long_fence_around_nested_bash_example(self):
        plan = (
            SCRIPT_DIR.parents[2]
            / "docs"
            / "plans"
            / "2026-07-14-local-queue-dashboard.md"
        ).read_text(encoding="utf-8")
        example = plan[plan.index("In README Quick start") :]

        self.assertIn("````markdown\n", example)
        self.assertIn("\n````\n\nAdd the `serve` row", example)


class DashboardLifecycleTests(unittest.TestCase):
    def in_flight_server(self):
        started = threading.Event()
        release = threading.Event()

        def load_snapshot():
            started.set()
            release.wait(timeout=2)
            return {"revision": 1}

        server = qd.create_server(
            "127.0.0.1",
            0,
            "token",
            2,
            lambda: 1,
            load_snapshot,
            lambda _after: [],
            SCRIPT_DIR / "dashboard",
        )
        serving = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        serving.start()
        response = {}

        def request_snapshot():
            response["value"] = request_server(
                server,
                "/token/api/snapshot",
            )

        request = threading.Thread(target=request_snapshot, daemon=True)
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

        drained = server.server_close(timeout=1)

        timer.join(timeout=1)
        request.join(timeout=1)
        self.assertTrue(drained)
        self.assertFalse(request.is_alive())
        self.assertEqual(200, response["value"][0])

    def test_server_close_returns_after_bound_with_active_request(self):
        server, request, release, _response = self.in_flight_server()
        started = time.monotonic()

        drained = server.server_close(timeout=0.05)

        self.assertFalse(drained)
        self.assertLess(time.monotonic() - started, 0.25)
        release.set()
        request.join(timeout=1)
        self.assertFalse(request.is_alive())

    def test_idle_server_exits_without_browser_requests(self):
        output = io.StringIO()
        started = time.monotonic()

        code = qd.serve(
            "127.0.0.1",
            0,
            2,
            1,
            False,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard",
            output=output,
            browser_open=lambda _url: True,
        )

        self.assertEqual(0, code)
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertIn("dashboard stopped", output.getvalue())

    def test_browser_failure_prints_manual_url_and_keeps_serving(self):
        output = io.StringIO()

        code = qd.serve(
            "127.0.0.1",
            0,
            2,
            1,
            True,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard",
            output=output,
            browser_open=lambda _url: False,
        )

        self.assertEqual(0, code)
        self.assertIn(
            "browser did not open; visit http://127.0.0.1:",
            output.getvalue(),
        )

    def test_browser_exception_prints_manual_url_and_closes_cleanly(self):
        output = io.StringIO()

        def fail_to_open(_url):
            raise qd.webbrowser.Error("no browser")

        code = qd.serve(
            "127.0.0.1",
            0,
            2,
            1,
            True,
            revision_loader=lambda: 0,
            snapshot_loader=lambda: {},
            events_loader=lambda _after: [],
            asset_dir=SCRIPT_DIR / "dashboard",
            output=output,
            browser_open=fail_to_open,
        )

        self.assertEqual(0, code)
        self.assertIn(
            "browser did not open; visit http://127.0.0.1:",
            output.getvalue(),
        )
        self.assertIn("dashboard stopped", output.getvalue())

    def test_snapshot_loader_recovers_after_one_data_failure(self):
        attempts = iter(
            (
                qd.DashboardDataUnavailable(),
                {"revision": 2},
            )
        )

        def load():
            result = next(attempts)
            if isinstance(result, Exception):
                raise result
            return result

        server = qd.create_server(
            "127.0.0.1",
            0,
            "token",
            2,
            lambda: 2,
            load,
            lambda _after: [],
            SCRIPT_DIR / "dashboard",
        )
        thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
        )
        thread.start()

        def stop_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.addCleanup(stop_server)
        self.assertEqual(
            503,
            request_server(server, "/token/api/snapshot")[0],
        )
        status, _headers, body = request_server(
            server,
            "/token/api/snapshot",
        )
        self.assertEqual(200, status)
        self.assertEqual(2, json.loads(body)["revision"])


if __name__ == "__main__":
    unittest.main()
