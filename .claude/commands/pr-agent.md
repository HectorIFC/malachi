---
description: 'Run the PR-Agent port (review, improve, describe) on this branch or a PR by number, terminal only'
argument-hint: "[review|improve|describe|all] [pr-number] [threshold=N] [max-findings=N]"
---

Run PR-Agent's review, improve and describe on this branch. If $ARGUMENTS names a tool, run only that
one; if it names a pull request number, review that pull request instead of the branch.

Follow the `pr-agent-review` skill exactly: pin the diff with `scripts/pin-diff.sh`, check it against the
Plan and Verification of the issue it implements, score the code suggestions in an independent pass, and
print everything to the terminal as numbered items with lettered options, the recommended one first.

Post nothing to GitHub and run nothing from a pull request. Apply nothing before the contributor
chooses.
