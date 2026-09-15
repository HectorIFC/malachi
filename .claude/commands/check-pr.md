---
description: After a push, report which CI jobs failed and what reviewers said, with each finding verified
argument-hint: "[pr-number]"
---

Check the pull request for this branch. If $ARGUMENTS names a PR number, check that one instead.

Follow the `review-pr-feedback` skill: read the CI status and the logs of anything that failed, gather
comments from both the review and the conversation endpoints for every author, and verify each finding
against the code before accepting it.

Report what blocks first, then what is advisory, then what you declined and why. For every finding say
what it claims, whether it holds, and what you propose. Do not paste comment bodies as the report.

Do not fix anything yet unless the user asked for it in the same breath. Findings that change the plan
come back to them first.
