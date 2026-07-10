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


if __name__ == "__main__":
    unittest.main()
