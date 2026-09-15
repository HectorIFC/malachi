---
name: review-pr-feedback
description: 'Check a pull request after a push: which CI jobs failed and why, and what reviewers said. Use when the user asks about CI, jobs, checks, builds, or review comments on a PR, in any wording and in any language, including right after they report having committed. Gathers both comment endpoints for every author, reads the failing job logs, verifies each finding against the code before accepting it, and reports what blocks versus what is advice.'
---

# Checking a pull request after a push

Two questions, asked together because the user asks them together: did CI go red, and did anyone say
anything. The work is not reading the answers, it is judging them.

## 1. Find the pull request

```
gh pr view --json number,title,state,headRefName,url
```

from inside the worktree, or `gh pr list --head <branch>` when the branch is known but not checked out.
No PR yet means the push has not opened one; say that rather than guessing a number.

## 2. CI: status first, then the failing logs

```
gh pr checks <N>
gh run list --branch <branch> --limit 10 --json databaseId,name,status,conclusion
```

Checks still running are not checks that passed. Report them as pending, and offer to wait rather than
reading a verdict into an unfinished run.

For each failure, read the log rather than the name:

```
gh run view --job <job-id> --log-failed
```

Then answer the question the user actually has, which is never "which job is red" but "why, and is it
mine". Three outcomes, and they need different words:

- **Caused by this change.** Say what broke and where.
- **A known flake.** Say so, and say how you know, with the evidence: a rerun of the same commit that
  passed, or a failure that predates the branch. A flake asserted without evidence is a guess that
  happens to be convenient.
- **Failing on main too.** Check before blaming the branch: `gh run list --branch main --limit 5`. A
  drill that fails the same way on an untouched main is not this PR's defect.

## 3. Comments: both endpoints, every author

Review comments and conversation comments are different endpoints and both matter:

```
gh api repos/HectorIFC/malachi/pulls/<N>/comments --paginate   # inline, anchored to a line
gh api repos/HectorIFC/malachi/issues/<N>/comments --paginate  # general conversation
```

Do not filter by author. This is an OSS project: today the comments come from `coderabbitai[bot]` and
`github-actions[bot]`, tomorrow from anyone, and a filter written for today's reviewers silently drops
tomorrow's. Sort by `created_at`, and use `in_reply_to_id` to tell a fresh comment from a reply in a
thread already handled.

Bot bodies carry collapsed `<details>` blocks and HTML comment markers that bury the finding; strip
them before reading. A finding usually states a severity of its own. Treat that as the reviewer's
opinion, not as the answer: severity is about consequence in this codebase, which the reviewer cannot
always see.

## 4. Verify each finding before accepting it

This is the step that matters, and the reason this is a skill rather than a script.

**Read the cited code yourself.** A finding is a claim about the code, and claims are checkable. Open
the file at the line, trace the callers, and decide from what is there.

Findings fall into four outcomes, and each needs a different action:

- **Right, and as described.** Fix it.
- **Right, and worse than described.** Happens often, and it is the most valuable outcome. A comment
  labelled minor about two documentation lines once turned out to describe a moduledoc asserting a
  guarantee the code did not implement, which is the same class of defect that had already cost 519
  acknowledged records. Fix it, and say why the severity moved.
- **Right, but already handled.** The working tree may already contain the fix. Confirm it, and say so
  rather than changing it twice.
- **Wrong, or not applicable.** Say so with the evidence, and reply on the thread. A finding declined
  with reasoning is a better outcome than one silently ignored, and on an OSS project the reply is read
  by people deciding whether it is worth reviewing here again.

Never accept a finding because a bot is usually right, and never dismiss one because a bot is
sometimes wrong. Both are ways of not checking.

## 5. A human comment is not a finding

A person asking a question wants an answer, not a patch. A person proposing a different approach wants
the tradeoff engaged with, not a silent change of direction. Answer in the thread, and bring anything
that changes the plan back to the user before acting on it.

## 6. Report

Lead with what blocks: failing jobs caused by this change, and findings that are real. Then what is
advisory. Then what was declined and why.

For each finding, the user needs three things to act: what it says, whether it holds, and what you
propose. A list of comment bodies is not a report, it is the raw material.

If a fix follows, the verification bar is the same as any other change here, including that a new test
must fail without it.
