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
        message = completed.stderr.decode("utf-8", "replace").strip()
        raise GitContextError(
            "git_command_failed", message or "git command failed"
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
