---
description: 'Start work on a Malachi issue: branch, worktree, board, and a background planning session'
argument-hint: <issue-number>
---

Start work on the Malachi issue named by $ARGUMENTS.

Follow the `start-issue-work` skill exactly: read the issue and the work related to it, create or
fast-forward its branch from `origin/main`, add the worktree at `~/malachi-<issue number>`, clear the
upstream on a new branch, move the issue to Ready on the board, and launch the background planning
session in plan mode, with the session named exactly after the branch.

Finish by printing the `claude attach <id>` command on its own line, so it can be copied without
reading past it.

Stop and report instead of proceeding if the issue has no `## PR` section with a branch name, if it is
missing the repo's standard sections, or if its branch already carries commits of its own. Those are
decisions for the user, not defaults to pick.
