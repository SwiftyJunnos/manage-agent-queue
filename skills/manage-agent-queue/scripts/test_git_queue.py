#!/usr/bin/env python3
"""Tests for Git-aware queue helpers."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import git_queue as gq


def run_git(cwd, *arguments, check=True):
    return subprocess.run(
        ["git", "-C", str(cwd), *arguments],
        check=check,
        capture_output=True,
        text=True,
    )


class GitRepositoryTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        run_git(self.root, "init", "-b", "main")
        run_git(self.root, "config", "user.name", "Queue Tests")
        run_git(self.root, "config", "user.email", "queue@example.test")
        (self.root / "README.md").write_text("base\n", encoding="utf-8")
        run_git(self.root, "add", "README.md")
        run_git(self.root, "commit", "-m", "base")


class TypedPathResourceTests(unittest.TestCase):
    def test_parse_accepts_canonical_file_and_directory_resources(self):
        self.assertEqual(
            gq.PathScope("file", "src/api.py"),
            gq.parse_path_resource("file:src/api.py"),
        )
        self.assertEqual(
            gq.PathScope("dir", "tests/api/"),
            gq.parse_path_resource("dir:tests/api/"),
        )

    def test_parse_rejects_unsafe_or_noncanonical_paths(self):
        for value in (
            "file:/tmp/a",
            "file:../a",
            "file:a/./b",
            "file:a//b",
            "file:a\\b",
            "file:",
            "file:src/",
            "dir:src",
            "dir:src/../tests/",
        ):
            with self.subTest(value=value):
                with self.assertRaises(gq.GitContextError):
                    gq.parse_path_resource(value)

    def test_overlap_respects_file_and_directory_boundaries(self):
        cases = (
            ("file:src/a.py", "file:src/a.py", True),
            ("file:src/a.py", "file:src/ab.py", False),
            ("dir:src/", "file:src/a.py", True),
            ("dir:src/api/", "dir:src/api/internal/", True),
            ("dir:src/api/", "dir:src/apis/", False),
        )
        for left, right, expected in cases:
            with self.subTest(left=left, right=right):
                self.assertEqual(
                    expected,
                    gq.resources_overlap([left], [right]),
                )


class GitObservationTests(GitRepositoryTestCase):
    def test_observe_returns_private_paths_and_stable_public_identity(self):
        observed = gq.observe(self.root)

        self.assertEqual(str(self.root.resolve()), observed["worktree"])
        self.assertEqual("refs/heads/main", observed["branch"])
        self.assertEqual(
            run_git(self.root, "rev-parse", "HEAD").stdout.strip(),
            observed["head"],
        )
        self.assertRegex(observed["repository_id"], r"^[0-9a-f]{64}$")
        self.assertRegex(observed["worktree_id"], r"^[0-9a-f]{64}$")
        self.assertTrue(observed["clean"])
        self.assertTrue(observed["attached"])

    def test_observe_reports_dirty_and_detached_without_losing_identity(self):
        (self.root / "dirty.txt").write_text("dirty\n", encoding="utf-8")
        dirty = gq.observe(self.root)
        self.assertFalse(dirty["clean"])

        (self.root / "dirty.txt").unlink()
        run_git(self.root, "checkout", "--detach")
        detached = gq.observe(self.root)
        self.assertFalse(detached["attached"])
        self.assertIsNone(detached["branch"])

    def test_claim_binding_requires_clean_attached_state_and_tracks_base(self):
        observed = gq.observe(self.root)

        binding = gq.claim_binding(observed)

        self.assertEqual(observed["head"], binding["base"])
        self.assertNotIn("clean", binding)
        self.assertNotIn("attached", binding)

        for field, value, code in (
            ("clean", False, "git_dirty"),
            ("attached", False, "git_detached_head"),
        ):
            with self.subTest(field=field):
                invalid = dict(observed)
                invalid[field] = value
                with self.assertRaises(gq.GitContextError) as raised:
                    gq.claim_binding(invalid)
                self.assertEqual(code, raised.exception.code)

    def test_claim_snapshot_comparison_detects_head_and_identity_drift(self):
        observed = gq.observe(self.root)
        binding = gq.claim_binding(observed)
        gq.assert_claim_snapshot(binding, observed)

        for field in ("repository_id", "worktree_id", "branch", "head"):
            with self.subTest(field=field):
                changed = dict(observed)
                changed[field] = "changed"
                with self.assertRaises(gq.GitContextError) as raised:
                    gq.assert_claim_snapshot(binding, changed)
                self.assertEqual("git_claim_drift", raised.exception.code)


if __name__ == "__main__":
    unittest.main()
