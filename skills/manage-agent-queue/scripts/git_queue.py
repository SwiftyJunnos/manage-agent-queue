#!/usr/bin/env python3
"""Observe and validate Git state for the local agent queue."""

import hashlib
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


MAX_OFFENDING_PATHS = 10


class GitContextError(Exception):
    """A bounded Git observation or validation failure."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PathScope:
    kind: str
    path: str


def _digest(value):
    return hashlib.sha256(os.fsencode(value)).hexdigest()


def _git(cwd, *arguments, allowed=(0,)):
    try:
        completed = subprocess.run(
            ["git", "-C", os.fspath(cwd), *arguments],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise GitContextError(
            "git_unavailable", "cannot execute git"
        ) from error
    if completed.returncode not in allowed:
        raise GitContextError(
            "git_command_failed", "git command failed"
        )
    return completed


def parse_path_resource(resource):
    if not isinstance(resource, str) or ":" not in resource:
        raise GitContextError(
            "git_path_resource", "invalid typed path resource"
        )
    kind, path = resource.split(":", 1)
    if kind not in {"file", "dir"}:
        raise GitContextError(
            "git_path_resource", f"invalid path resource: {resource}"
        )
    if not path or path.startswith("/") or "\\" in path or "\x00" in path:
        raise GitContextError(
            "git_path_resource", f"invalid path resource: {resource}"
        )
    if kind == "file" and path.endswith("/"):
        raise GitContextError(
            "git_path_resource", "file resource must name a file"
        )
    if kind == "dir" and not path.endswith("/"):
        raise GitContextError(
            "git_path_resource", "dir resource must end with /"
        )
    lexical_path = path[:-1] if kind == "dir" else path
    if not lexical_path or any(
        part in {"", ".", ".."} for part in lexical_path.split("/")
    ):
        raise GitContextError(
            "git_path_resource", f"noncanonical path resource: {resource}"
        )
    return PathScope(kind, path)


def path_scopes(resources):
    return [
        parse_path_resource(resource)
        for resource in resources
        if resource.startswith(("file:", "dir:"))
    ]


def resources_overlap(left_resources, right_resources):
    for left in path_scopes(left_resources):
        for right in path_scopes(right_resources):
            if left.kind == right.kind == "file":
                overlaps = left.path == right.path
            elif left.kind == right.kind == "dir":
                overlaps = (
                    left.path.startswith(right.path)
                    or right.path.startswith(left.path)
                )
            else:
                directory = left if left.kind == "dir" else right
                file_scope = right if left.kind == "dir" else left
                overlaps = file_scope.path.startswith(directory.path)
            if overlaps:
                return True
    return False


def observe(cwd):
    try:
        worktree_output = _git(cwd, "rev-parse", "--show-toplevel").stdout
    except GitContextError as error:
        raise GitContextError(
            "git_not_worktree",
            "current directory is not a Git worktree",
        ) from error
    worktree = Path(
        worktree_output.decode("utf-8", "strict").strip()
    ).resolve()
    common_raw = _git(worktree, "rev-parse", "--git-common-dir").stdout
    common_value = common_raw.decode("utf-8", "strict").strip()
    common_dir = Path(common_value)
    if not common_dir.is_absolute():
        common_dir = worktree / common_dir
    common_dir = common_dir.resolve()

    branch_result = _git(
        worktree, "symbolic-ref", "-q", "HEAD", allowed=(0, 1)
    )
    branch = (
        branch_result.stdout.decode("utf-8", "strict").strip()
        if branch_result.returncode == 0
        else None
    )
    head = _git(worktree, "rev-parse", "HEAD").stdout.decode(
        "ascii", "strict"
    ).strip()
    dirty = bool(
        _git(
            worktree,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
        ).stdout
    )
    return {
        "common_dir": str(common_dir),
        "worktree": str(worktree),
        "repository_id": _digest(str(common_dir)),
        "worktree_id": _digest(str(worktree)),
        "branch": branch,
        "head": head,
        "attached": branch is not None,
        "clean": not dirty,
    }


def require_claimable(observation):
    if not isinstance(observation, dict):
        raise GitContextError(
            "git_not_worktree", "Git-aware claim requires a worktree"
        )
    if not observation.get("attached") or not observation.get("branch"):
        raise GitContextError(
            "git_detached_head", "Git-aware claim requires an attached branch"
        )
    if not observation.get("clean"):
        raise GitContextError(
            "git_dirty", "Git-aware claim requires a clean worktree"
        )
    for field in (
        "common_dir",
        "worktree",
        "repository_id",
        "worktree_id",
        "branch",
        "head",
    ):
        if not isinstance(observation.get(field), str) or not observation[field]:
            raise GitContextError(
                "git_observation_invalid",
                f"Git observation field {field} must be nonempty",
            )
    return observation


def claim_binding(observation):
    observed = require_claimable(observation)
    return {
        "common_dir": observed["common_dir"],
        "worktree": observed["worktree"],
        "repository_id": observed["repository_id"],
        "worktree_id": observed["worktree_id"],
        "branch": observed["branch"],
        "base": observed["head"],
    }


def assert_claim_snapshot(binding, observation):
    observed = require_claimable(observation)
    expected = {
        "repository_id": binding.get("repository_id"),
        "worktree_id": binding.get("worktree_id"),
        "branch": binding.get("branch"),
        "head": binding.get("base"),
    }
    actual = {
        "repository_id": observed["repository_id"],
        "worktree_id": observed["worktree_id"],
        "branch": observed["branch"],
        "head": observed["head"],
    }
    if actual != expected:
        raise GitContextError(
            "git_claim_drift", "Git state changed while claiming the task"
        )
    return observed


def assert_same_binding(binding, observation, require_same_head=False):
    observed = require_claimable(observation)
    expected = {
        "repository_id": binding.get("repository_id"),
        "worktree_id": binding.get("worktree_id"),
        "branch": binding.get("branch"),
    }
    actual = {
        "repository_id": observed["repository_id"],
        "worktree_id": observed["worktree_id"],
        "branch": observed["branch"],
    }
    if actual != expected:
        raise GitContextError(
            "git_binding_mismatch",
            "Git repository, worktree, or branch no longer matches the claim",
        )
    if require_same_head and observed["head"] != binding.get("base"):
        raise GitContextError(
            "git_head_mismatch", "current HEAD no longer matches the claimed base"
        )
    return observed


def path_is_allowed(path, resources):
    if not isinstance(path, str) or not path:
        return False
    for scope in path_scopes(resources):
        if scope.kind == "file" and path == scope.path:
            return True
        if scope.kind == "dir" and path.startswith(scope.path):
            return True
    return False


def _display_path(path, limit=200):
    escaped = path.encode("unicode_escape", "backslashreplace").decode("ascii")
    if len(escaped) > limit:
        return escaped[: limit - 3] + "..."
    return escaped


def path_scope_error(offenders):
    ordered = sorted(set(offenders))
    visible = ordered[:MAX_OFFENDING_PATHS]
    omitted = len(ordered) - len(visible)
    message = "paths outside declared scope: " + ", ".join(
        _display_path(path) for path in visible
    )
    if omitted:
        message += f" (+{omitted} more)"
    return GitContextError("git_path_scope", message)


def compact_evidence(branch, base, head, commit_count, changed_path_count):
    return {
        "branch": branch,
        "base": base,
        "head": head,
        "commit_count": commit_count,
        "changed_path_count": changed_path_count,
    }


def validate_completion(binding, resources, commit=None, no_change=False):
    if not isinstance(binding, dict):
        raise GitContextError(
            "git_binding_invalid", "Git completion requires a claim binding"
        )
    if not isinstance(no_change, bool):
        raise GitContextError(
            "git_completion_mode", "no_change must be a boolean"
        )
    if commit is not None and (
        not isinstance(commit, str) or not commit.strip()
    ):
        raise GitContextError(
            "git_commit_invalid", "commit must be a nonempty revision"
        )

    current = observe(binding.get("worktree", ""))
    assert_same_binding(binding, current)
    base = binding.get("base")
    if not isinstance(base, str) or not base:
        raise GitContextError(
            "git_binding_invalid", "Git claim base must be nonempty"
        )

    if no_change:
        if commit is not None:
            raise GitContextError(
                "git_completion_mode",
                "--commit and --no-change cannot be combined",
            )
        if current["head"] != base:
            raise GitContextError(
                "git_head_mismatch", "no-change requires HEAD at the claimed base"
            )
        return compact_evidence(binding["branch"], base, base, 0, 0)

    if commit is None:
        raise GitContextError(
            "git_commit_required",
            "Git-aware completion requires --commit or --no-change",
        )

    head = _git(
        binding["worktree"],
        "rev-parse",
        "--verify",
        f"{commit}^{{commit}}",
    ).stdout.decode("ascii", "strict").strip()
    if current["head"] != head:
        raise GitContextError(
            "git_head_mismatch", "current HEAD does not match --commit"
        )

    ancestry = _git(
        binding["worktree"],
        "merge-base",
        "--is-ancestor",
        base,
        head,
        allowed=(0, 1),
    )
    if ancestry.returncode != 0 or base == head:
        raise GitContextError(
            "git_non_descendant",
            "result must descend from and advance the claimed base",
        )

    changed_raw = _git(
        binding["worktree"],
        "diff",
        "--name-only",
        "-z",
        "--no-renames",
        base,
        head,
    ).stdout
    try:
        changed = [
            path
            for path in changed_raw.decode("utf-8", "strict").split("\x00")
            if path
        ]
    except UnicodeDecodeError as error:
        raise GitContextError(
            "git_path_encoding", "changed paths must be valid UTF-8"
        ) from error
    offenders = [
        path for path in changed if not path_is_allowed(path, resources)
    ]
    if offenders:
        raise path_scope_error(offenders)

    count_output = _git(
        binding["worktree"],
        "rev-list",
        "--count",
        f"{base}..{head}",
    ).stdout.decode("ascii", "strict").strip()
    return compact_evidence(
        binding["branch"],
        base,
        head,
        int(count_output),
        len(changed),
    )


def assert_completion_snapshot(binding, evidence, observation):
    observed = assert_same_binding(binding, observation)
    if observed["head"] != evidence.get("head"):
        raise GitContextError(
            "git_completion_drift",
            "Git state changed after completion validation",
        )
    return observed
