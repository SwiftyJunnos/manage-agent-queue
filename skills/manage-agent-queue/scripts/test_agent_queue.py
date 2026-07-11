#!/usr/bin/env python3
"""Tests for the manage-agent-queue CLI."""

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import agent_queue as aq


def canonical_claim(
    agent_id="agent-1",
    lease_token="test-lease-token",
    claimed_at="2026-07-10T05:00:00Z",
    heartbeat_at="2026-07-10T05:00:00Z",
    expires_at="2026-07-10T07:00:00Z",
):
    return {
        "agent_id": agent_id,
        "lease_token": lease_token,
        "claimed_at": claimed_at,
        "heartbeat_at": heartbeat_at,
        "expires_at": expires_at,
    }


class QueueStateTests(unittest.TestCase):
    def test_new_state_has_versioned_monotonic_counters(self):
        state = aq.new_state("demo", aq.fixed_config())

        self.assertEqual(1, state["schema_version"])
        self.assertEqual("demo", state["queue_id"])
        self.assertEqual(0, state["revision"])
        self.assertEqual(1, state["next_task_sequence"])
        self.assertEqual(1, state["next_workflow_sequence"])
        self.assertEqual(1, state["next_event_sequence"])
        self.assertEqual({}, state["tasks"])
        self.assertEqual([], state["events"])

    def test_validate_state_rejects_unknown_schema(self):
        state = aq.new_state("demo", aq.fixed_config())

        for unknown_schema in (2, True):
            with self.subTest(schema_version=unknown_schema):
                state["schema_version"] = unknown_schema
                with self.assertRaisesRegex(aq.InvariantError, "schema_version"):
                    aq.validate_state(state)

    def test_validate_state_rejects_unknown_top_level_field(self):
        state = aq.new_state("demo", aq.fixed_config())
        state["future_field"] = "not supported by schema version 1"

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
                "created_at": "2026-07-10T06:00:00Z",
                "updated_at": "2026-07-10T06:00:00Z",
            },
            task,
        )
        self.assertIsNot(task, self.state["tasks"]["T-000001"])
        self.assertEqual(task, self.state["tasks"]["T-000001"])
        self.assertEqual(2, self.state["next_task_sequence"])

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
        task["result"] = {"summary": "done", "artifacts": ["report.txt"]}
        task["last_error"] = {
            "message": "earlier retry",
            "at": "2026-07-10T06:00:00Z",
        }

        self.assertIs(self.state, aq.validate_state(self.state))

    def test_validate_state_rejects_malformed_lifecycle_combinations(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        cases = (
            ({"result": {"summary": "done", "artifacts": []}}, "result.*completed"),
            ({"status": "completed", "result": None}, "completed.*result"),
            (
                {
                    "status": "completed",
                    "result": {"summary": "done", "artifacts": [], "extra": 1},
                },
                "result.*keys",
            ),
            (
                {
                    "status": "completed",
                    "result": {"summary": 1, "artifacts": []},
                },
                "result.*summary",
            ),
            (
                {
                    "status": "completed",
                    "result": {"summary": "done", "artifacts": [""]},
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
                "lease_token": "TOP-SECRET-TOKEN",
                "claimed_at": self.NOW,
                "heartbeat_at": self.NOW,
                "expires_at": "2026-07-10T06:00:30Z",
            },
            stored["claim"],
        )
        self.assertEqual("TOP-SECRET-TOKEN", result["lease_token"])
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

        self.assertEqual("MOCKED-SECRET", result["lease_token"])
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
            {"summary": "Finished", "artifacts": ["a.txt", "b.json"]},
            stored["result"],
        )
        self.assertIsNone(stored["claim"])
        self.assertIsNone(stored["available_at"])
        self.assertIsNot(stored, returned)
        self.assertEqual(
            {"artifact_count": 2}, self.state["events"][-1]["details"]
        )
        self.assertNotIn(token, repr(self.state["events"][-1]))

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
        dependency["result"] = {"summary": "done", "artifacts": []}
        task = self.add("Ready", depends_on=[dependency["id"]])

        self.assertEqual([], aq.dependency_blockers(self.state, task))
        self.assertEqual("ready", aq.derive_state(self.state, task, self.NOW))

    def test_unfinished_dependencies_are_ordered_blockers(self):
        first = self.add("First")
        completed = self.add("Completed")
        completed["status"] = "completed"
        completed["result"] = {"summary": "done", "artifacts": []}
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
        dependency["result"] = {"summary": "done", "artifacts": []}
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
                    task["result"] = {"summary": "done", "artifacts": []}
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


if __name__ == "__main__":
    unittest.main()
