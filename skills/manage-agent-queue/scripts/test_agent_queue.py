#!/usr/bin/env python3
"""Tests for the manage-agent-queue CLI."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import agent_queue as aq


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

    def test_validate_state_accepts_structured_future_metadata(self):
        created = aq.add_task(self.state, {"title": "Stored"})
        task = self.state["tasks"][created["id"]]
        task["available_at"] = "2026-07-10T06:01:00Z"
        task["claim"] = {"worker": "agent-1", "lease_seconds": 30}
        task["result"] = {"summary": "done", "artifacts": ["report.txt"]}
        task["last_error"] = {"message": "retry", "terminal": False}

        self.assertIs(self.state, aq.validate_state(self.state))

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


if __name__ == "__main__":
    unittest.main()
