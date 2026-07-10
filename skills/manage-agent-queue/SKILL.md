---
name: manage-agent-queue
description: Use when coordinating multiple agents that need shared task claiming, dependency ordering, worktree-safe ownership, progress visibility, or recovery after an agent stops.
---

# Manage Agent Queue

Use `scripts/agent_queue.py` as the only queue writer. Do not edit generated queue files directly.

The complete coordinator and worker protocol is added after the CLI contract is executable.
