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


class GitCompletionTests(GitRepositoryTestCase):
    def binding(self):
        return gq.claim_binding(gq.observe(self.root))

    def commit_files(self, paths, message):
        for path in paths:
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(f"{message}: {path}\n", encoding="utf-8")
        run_git(self.root, "add", "-A")
        run_git(self.root, "commit", "-m", message)
        return run_git(self.root, "rev-parse", "HEAD").stdout.strip()

    def assert_git_error(self, code, callback):
        with self.assertRaises(gq.GitContextError) as raised:
            callback()
        self.assertEqual(code, raised.exception.code)
        return str(raised.exception)

    def test_descendant_commits_return_exact_compact_evidence(self):
        binding = self.binding()
        base = binding["base"]
        self.commit_files(
            [f"src/first-{index:02d}.txt" for index in range(12)],
            "first",
        )
        head = self.commit_files(
            [f"src/second-{index:02d}.txt" for index in range(15)],
            "second",
        )

        evidence = gq.validate_completion(
            binding,
            ["dir:src/"],
            commit=head,
        )

        self.assertEqual(
            {
                "branch": "refs/heads/main",
                "base": base,
                "head": head,
                "commit_count": 2,
                "changed_path_count": 27,
            },
            evidence,
        )
        self.assertNotIn("changed_paths", evidence)

    def test_completion_rejects_wrong_head_non_descendant_and_dirty_tree(self):
        binding = self.binding()
        head = self.commit_files(["src/change.txt"], "advance")
        self.assert_git_error(
            "git_head_mismatch",
            lambda: gq.validate_completion(
                binding, ["dir:src/"], commit=binding["base"]
            ),
        )

        tree = run_git(
            self.root, "rev-parse", f"{binding['base']}^{{tree}}"
        ).stdout.strip()
        divergent = run_git(
            self.root, "commit-tree", tree, "-m", "divergent"
        ).stdout.strip()
        run_git(self.root, "reset", "--hard", divergent)
        self.assert_git_error(
            "git_non_descendant",
            lambda: gq.validate_completion(
                binding, ["dir:src/"], commit=divergent
            ),
        )

        run_git(self.root, "reset", "--hard", head)
        (self.root / "dirty.txt").write_text("dirty\n", encoding="utf-8")
        self.assert_git_error(
            "git_dirty",
            lambda: gq.validate_completion(
                binding, ["dir:src/"], commit=head
            ),
        )

    def test_directory_scope_uses_path_boundaries(self):
        binding = self.binding()
        head = self.commit_files(["src-other/a.py"], "outside")

        message = self.assert_git_error(
            "git_path_scope",
            lambda: gq.validate_completion(
                binding, ["dir:src/"], commit=head
            ),
        )

        self.assertIn("src-other/a.py", message)

    def test_rename_checks_source_and_destination_with_no_rename_diff(self):
        self.commit_files(["src/inside.txt"], "source")
        binding = self.binding()
        run_git(self.root, "mv", "src/inside.txt", "outside.txt")
        run_git(self.root, "commit", "-m", "rename")
        head = run_git(self.root, "rev-parse", "HEAD").stdout.strip()

        message = self.assert_git_error(
            "git_path_scope",
            lambda: gq.validate_completion(
                binding, ["file:src/inside.txt"], commit=head
            ),
        )

        self.assertIn("outside.txt", message)

    def test_scope_error_caps_displayed_paths_and_reports_omitted_count(self):
        binding = self.binding()
        offenders = [f"outside/{index:02d}.txt" for index in range(12)]
        head = self.commit_files(offenders, "outside")

        message = self.assert_git_error(
            "git_path_scope",
            lambda: gq.validate_completion(
                binding, ["dir:src/"], commit=head
            ),
        )

        for path in offenders[:gq.MAX_OFFENDING_PATHS]:
            self.assertIn(path, message)
        for path in offenders[gq.MAX_OFFENDING_PATHS:]:
            self.assertNotIn(path, message)
        self.assertIn("(+2 more)", message)
        self.assertNotIn(str(self.root), message)

    def test_no_change_requires_the_original_clean_head(self):
        binding = self.binding()
        evidence = gq.validate_completion(
            binding,
            ["file:README.md"],
            no_change=True,
        )
        self.assertEqual(
            {
                "branch": "refs/heads/main",
                "base": binding["base"],
                "head": binding["base"],
                "commit_count": 0,
                "changed_path_count": 0,
            },
            evidence,
        )

        self.commit_files(["README.md"], "advance")
        self.assert_git_error(
            "git_head_mismatch",
            lambda: gq.validate_completion(
                binding, ["file:README.md"], no_change=True
            ),
        )


class GitRecoveryObservationTests(GitRepositoryTestCase):
    def binding(self):
        return gq.claim_binding(gq.observe(self.root))

    def commit_file(self, path, message):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{message}\n", encoding="utf-8")
        run_git(self.root, "add", "-A")
        run_git(self.root, "commit", "-m", message)
        return run_git(self.root, "rev-parse", "HEAD").stdout.strip()

    def assert_git_error(self, code, callback):
        with self.assertRaises(gq.GitContextError) as raised:
            callback()
        self.assertEqual(code, raised.exception.code)

    def test_recovery_accepts_unchanged_base_and_scoped_descendants(self):
        binding = self.binding()
        unchanged = gq.validate_recovery(
            binding, ["dir:src/"], gq.observe(self.root)
        )
        self.assertEqual(
            {"binding": binding, "head": binding["base"]},
            unchanged,
        )

        head = self.commit_file("src/recovered.py", "survived")
        descendant = gq.validate_recovery(
            binding, ["dir:src/"], gq.observe(self.root)
        )
        self.assertEqual(binding, descendant["binding"])
        self.assertEqual(head, descendant["head"])

    def test_recovery_rejects_dirty_wrong_identity_branch_and_scope(self):
        binding = self.binding()
        observed = gq.observe(self.root)
        for field, value, code in (
            ("worktree_id", "wrong-worktree", "git_binding_mismatch"),
            ("branch", "refs/heads/other", "git_binding_mismatch"),
            ("clean", False, "git_dirty"),
        ):
            with self.subTest(field=field):
                changed = dict(observed)
                changed[field] = value
                self.assert_git_error(
                    code,
                    lambda changed=changed: gq.validate_recovery(
                        binding, ["dir:src/"], changed
                    ),
                )

        self.commit_file("outside.txt", "outside")
        self.assert_git_error(
            "git_path_scope",
            lambda: gq.validate_recovery(
                binding, ["dir:src/"], gq.observe(self.root)
            ),
        )

    def test_recovery_rejects_divergent_head(self):
        binding = self.binding()
        tree = run_git(
            self.root, "rev-parse", f"{binding['base']}^{{tree}}"
        ).stdout.strip()
        divergent = run_git(
            self.root, "commit-tree", tree, "-m", "divergent"
        ).stdout.strip()
        run_git(self.root, "reset", "--hard", divergent)

        self.assert_git_error(
            "git_recovery_mismatch",
            lambda: gq.validate_recovery(
                binding, ["file:README.md"], gq.observe(self.root)
            ),
        )

    def test_release_requires_clean_original_head(self):
        binding = self.binding()
        gq.validate_release(binding)

        self.commit_file("README.md", "advance")
        self.assert_git_error(
            "git_head_mismatch",
            lambda: gq.validate_release(binding),
        )


if __name__ == "__main__":
    unittest.main()
