---
description: 'Open the draft PR for the issue this branch implements, with the issue''s fields and the PR template filled in'
argument-hint: "[issue-number]"
---

Open the pull request for the Malachi issue named by $ARGUMENTS. With no argument, take the issue number
from the worktree directory (`~/malachi-<N>`), and ask if there is none.

Follow the `open-issue-pr` skill exactly: prove the checked-out branch is the one the issue's `## PR`
section names and that it is pushed, derive the Conventional Commits title from the branch, choose the
type of change and its version label, fill `.github/pull_request_template.md` with the issue's PR
description, its Verification section and `Closes #<N>`, and open the PR as a draft with the issue's
assignees, labels, milestone and project fields.

Show the title, the type and the body before creating the PR. Finish by printing the PR URL on its own
line.

Stop and report instead of proceeding if the branch is not pushed, already has an open PR, carries
uncommitted changes, or does not match the issue. Never commit or push to get past one of those.
