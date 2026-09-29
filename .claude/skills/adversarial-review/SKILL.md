---
name: adversarial-review
description: 'Review the changes on the current branch adversarially before calling the work done, with parallel reviewer subagents and an independent verifier that tries to refute every finding. Use when the auto-review Stop hook asks for it, or when the user asks to review, audit, check or double-check the current changes, the branch, the diff or the implementation, in any wording and in any language. Reads REVIEW.md for what counts and how severe, never applies a fix on its own, and ends by offering the contributor numbered findings with lettered options (the recommended one first) to choose from.'
---

# Adversarial review of the branch

A reviewer that shares the author's context shares the author's blind spots. This review is done by
subagents that see only the diff and the repository's criteria, never the reasoning that produced the
change, and every finding they raise is then handed to another subagent whose only job is to break it.
What survives is shown to the contributor, who decides what happens to each one. Nothing is changed
before that decision.

The criteria live in `REVIEW.md` at the repository root. It is the same file the managed Code Review
reads on pull requests, so a finding here and a finding there are judged by one standard. Read it in
full before step 2, together with `CLAUDE.md` and `CONTRIBUTING.md`.

Inside a worktree, run git as `/usr/bin/git`: a shell hook rewrites a bare `git` and the guard then
blocks it.

## 1. Pin down the diff

The repository's own script does it, the same one `pr-agent-review` uses, so the two reviews can never
disagree about what the diff is. Run it from this checkout, into a scratch directory
(`$CLAUDE_JOB_DIR/tmp/adversarial-review` in a background job, otherwise `mktemp -d`):

```
S=$(mktemp -d)                  # or S="$CLAUDE_JOB_DIR/tmp/adversarial-review" in a background job
"$(/usr/bin/git rev-parse --show-toplevel)/scripts/pin-diff.sh" pin "$S"; rc=$?
```

It covers everything the branch changes: its commits since `origin/main`, what is modified and what is
untracked, leaving out only the files the `prepare-commits` skill generates (`commit_message.sh`,
`commit_<n>.patch`, `commit_message.txt`, `commit_message_<n>.txt`), by exact name and only while
untracked. It writes `$S/diff.patch` (the full diff, untracked files included as new files),
`$S/files.txt` (the changed files) and `$S/base.txt` (the commit it compared against).

`rc` 3 means nothing to review, which is an answer: say so in one line and stop. Any `rc` other than 0
or 3 means the pin failed (1 when git could not read a file, 2 on a usage error) and the files it wrote
are incomplete: report the script's message and stop rather than review a diff that is missing files.

Every subagent reads `$S/diff.patch`, so they all read the same bytes rather than a diff taken at a
different moment.

## 2. Review in parallel, one dimension per subagent

Launch the reviewers **in a single message**, so they run concurrently. One `Explore` subagent per
dimension below that the diff actually touches; skip a dimension with nothing to look at (a docs-only
change needs no performance reviewer), and never launch more than five.

| Dimension | Looks for |
|---|---|
| Correctness | Wrong results, crashes, races, error paths that lose or hide a failure, edge cases the change does not handle |
| Durability and cluster | Anything that can lose or misreport acknowledged data, fence or failover gaps, a destructive act on a view that may be stale |
| NorthGuard alignment | A change that moves Malachi away from the NorthGuard design `REVIEW.md` describes, including a pre-existing divergence the change deepens |
| Tests | A behaviour with no test, a test that would pass without the change, an assertion too weak to fail, a flaky construction |
| Conventions and performance | The repository rules in `REVIEW.md` (logging through I18n, no em dash, docs that promise what the code delivers), and hot-path cost |

Each reviewer's prompt carries, and only carries:

- the path of the diff file and the list of changed files;
- the full text of `REVIEW.md`;
- its dimension and the table row above;
- where to look for the NorthGuard reference: `docs/ARCHITECTURE.md`, and the meetup transcript
  `northguard_meetup_transcript.txt` at the root of the main checkout when it exists on this machine
  (it is not in the repository);
- the instruction to read the surrounding code, not only the hunks, and to return findings in the shape
  below or the single word `NONE`.

It never carries the conversation, the plan, or why the author made a choice. That is the point.

A finding is:

```
file: path/to/file.ex
line: 123
severity: important | nit
title: one line
claim: what is wrong, in two sentences at most
failure: the concrete input or sequence of events that produces the wrong outcome
evidence: the code that shows it (quoted lines), and a command that would demonstrate it if there is one
```

A finding with no concrete failure is not a finding. Drop it before it reaches step 3.

## 3. Try to refute every finding

Merge the reviewers' findings and collapse duplicates (same file, same line range, same claim). Then,
**in a single message**, launch one `Explore` subagent per finding, or one per dimension when there are
more than ten findings, each told:

> You are trying to prove this finding WRONG. Read the code it cites and everything that calls it. It
> survives only if you can trace the failure end to end in the actual code. Answer CONFIRMED (you traced
> it), PLAUSIBLE (the failure is real but depends on something you could not see), or REFUTED (the code
> prevents it: say where). Quote the lines your verdict rests on.

The verifier also gets the diff file and `REVIEW.md`, and nothing about who raised the finding or why.

Keep CONFIRMED and PLAUSIBLE. Drop REFUTED, and say how many were dropped so the contributor knows the
filter ran. A verdict that cites no code counts as REFUTED.

Where a finding can be demonstrated cheaply (a single `mix test path:line`, a script run over a fixture),
the verifier may run it; it never edits a file.

## 4. Hand the decision to the contributor

No surviving finding: say that the review found nothing that survived verification, name the dimensions
covered and how many findings were refuted, and stop.

Otherwise present every finding, most severe first, numbered, in the contributor's language:

- the file and line, the claim, the failure scenario, the verdict and the evidence it rests on;
- two or three options, lettered, **the recommended one first**, each with its effort, its risk and what
  else it touches. "Leave it" is an option whenever that is reasonable, and for a finding outside the
  change's scope it is usually the recommended one, with a note of where it should go instead;
- which option is recommended and why, against the preferences in `CLAUDE.md`, and whether it moves
  Malachi toward or away from NorthGuard.

Then ask with `AskUserQuestion`, up to four findings per call, each option labelled with its finding
number and letter (`3A`, `3B`) so the answers cannot be misread. Ask again for the rest.

## 5. Apply only what was chosen

Apply exactly the options the contributor picked, nothing else, and follow the repository's own rules
while doing it: a test that fails without the fix, `mix format`, `mix credo --strict` and the tests that
cover the files touched. Report what changed per finding, and which findings were left as they were.

The fixes are not reviewed in this same turn: the stop that ends it follows the hook's own request and
is let through. The hook keeps a fingerprint of the diff it last asked about, so the fixes are reviewed
at the end of the next turn that finds the diff different, and a turn that changed nothing does not
start the same review again. Say that in the report, so the contributor knows the fixes are still
unreviewed. The hook records the diff as reviewed when it ASKS, so a review that is interrupted does not
come back on its own: if this review is cut short, say so, and that the contributor can ask for it. A finding the contributor declined is not raised again unless the code it rests on changed.

## What not to do

- Do not fix anything before the contributor chooses, even a finding that looks obvious.
- Do not give reviewers or verifiers the author's reasoning, the plan or the conversation.
- Do not show a finding without a failure scenario and code evidence, or one a verifier refuted.
- Do not commit, push, or run the prepare-commits script. The contributor commits.
