## Context

[PR-Agent](https://github.com/The-PR-Agent/pr-agent) (MIT) is an open source pull request reviewer with
three tools worth having: `/review`, `/improve` and `/describe`. It talks to its model over an HTTP API
through LiteLLM, so running it here means a separate per-token bill on top of the Claude Code
subscription the project already pays for. Its credentials cannot be swapped for the subscription's:
the subscription authenticates Claude Code itself, not third party tools.

What makes PR-Agent good is not its transport but its prompts and its pipeline, and both port cleanly
to a Claude Code skill. The reference is a local clone at `~/pr-agent`, commit `10bbd9a4`:

- **`/review`** (`pr_agent/settings/pr_reviewer_prompts.toml`, `pr_agent/tools/pr_reviewer.py`) returns
  a typed review: `key_issues_to_review` (file, lines, a concrete trigger), `security_concerns`,
  `relevant_tests`, `estimated_effort_to_review_[1-5]`, `risk_level`, `merge_recommendation`,
  `review_priority_files`, `todo_sections`, `can_be_split`, and a **`ticket_compliance_check`** that
  lists the ticket's requirements as fully compliant, not compliant, or needing human verification
  (`pr_agent/tools/ticket_pr_compliance_check.py`).
- **`/improve`** (`pr_agent/settings/code_suggestions/pr_code_suggestions_prompts.toml`) proposes
  concrete edits (`existing_code` to `improved_code`, with a label and a one sentence summary), then a
  **second, independent pass** (`pr_code_suggestions_reflect_prompts.toml`) scores every suggestion from
  0 to 10 with explicit rules (0 for a suggestion that only asks to verify, capped scores for error
  handling and type checks, 8 to 10 reserved for critical issues) and drops what falls under the
  threshold.
- **`/describe`** (`pr_agent/settings/pr_description_prompts.toml`) returns a type, a title, one to four
  summary bullets, a per file walkthrough and an optional change diagram.

The Malachi repository already has neighbours, which this work must not duplicate:

- `.claude/skills/adversarial-review/SKILL.md` finds defects with parallel reviewers per dimension and
  an independent verifier that tries to refute each finding, judged by `REVIEW.md`. It runs from the
  auto-review Stop hook.
- `.claude/skills/open-issue-pr/SKILL.md` writes a PR body from the issue's own `## PR` section.

Malachi issues are unusually good tickets for a compliance check: every one carries a `## Plan` with the
chosen option and a `## Verification` list, so "does this diff do what the issue decided" is answerable
line by line.

Decisions already taken by the owner:

1. **Scope: the full port**, all three tools (1B).
2. **A new skill**, separate from `adversarial-review`, invoked on demand, never from the Stop hook (2A).
3. **Target: the current branch by default, or a pull request by number** (third party and
   Dependabot PRs included), read with `gh` (3A).
4. **Output to the terminal only.** Numbered findings and suggestions with lettered options, the
   recommended one first, and nothing applied until the contributor chooses. The skill never posts to
   GitHub (4A).

## Plan

**Part 1. The diff, shared.** Pin the diff exactly as `adversarial-review` step 1 does (merge base with
`origin/main`, committed, modified and untracked files, excluding the `prepare-commits` artefacts), or,
for `<pr number>`, `gh pr diff <n>` plus `gh pr view <n>` for the head and base. Render it in PR-Agent's
hunk format (`prompt_fragments.toml`, `diff_hunk_format`: `__new hunk__` and `__old hunk__` with line
numbers), which is what every prompt expects.

- **A. Extract the pinning into one script both skills call** (for example
  `.claude/skills/_shared/pin-diff.sh`), so the two reviewers can never disagree about what the diff is.
- **B. Copy the steps into the new skill.** Faster, and two copies of the same procedure that will drift.
- Recommendation: **A**.

**Part 2. The ticket.** Find the issue the diff implements: for a branch, the issue whose `## PR` section
names it (the lookup `open-issue-pr` step 1 already does); for a PR, `closingIssuesReferences`, then the
branch. Feed its `## Plan` and `## Verification` to the compliance check as the requirements. No issue
found is an answer: the section says so rather than inventing requirements.

**Part 3. `review`.** Port `pr_review_prompt` with its output schema. Two changes from upstream:

- Drop the "you only see changed code segments" caveat. Here the reviewer reads the files the diff
  touches, their callers and their tests, which is the one real advantage over the API version.
- Inject `REVIEW.md`, `CLAUDE.md` and `CONTRIBUTING.md` into the slot PR-Agent calls `skills_context`
  and `repo_context`, so severity and the repository rules (I18n logging, no em dash, NorthGuard
  alignment, Linux only evidence) are the ones every other review here uses.

**Part 4. `improve`.** Port the suggestion prompt and the reflection prompt as two separate subagents, so
the scorer never sees the author's reasoning, mirroring the independence `adversarial-review` gets from
its verifier. Keep the upstream score rules and the default `focus_only_on_problems=true`. The threshold
is a skill argument with the upstream default.

**Part 5. `describe`.** Port the description prompt, rendered for the terminal. It does not open or edit
a PR (`open-issue-pr` does that), and where the issue's `## PR` description and the diff disagree, it
says so, which is the part `open-issue-pr` cannot see.

**Part 6. Packaging.**

- **A. One skill with three subcommands** (`pr-agent-review [review|improve|describe|all] [pr number]`),
  one `SKILL.md` and the adapted prompts under `prompts/`.
- **B. Three skills.** Three descriptions competing for the same trigger phrases, and the shared steps
  repeated three times.
- Recommendation: **A**. The adapted prompts keep PR-Agent's copyright notice and MIT text beside them
  (`prompts/LICENSE-pr-agent`), and a header in each records the upstream file and commit they came
  from, so a later sync is a diff against `~/pr-agent`.

**Large diffs.** PR-Agent splits a diff into chunks sized to its model's context. Here a diff over a size
bound is split by file into groups, one subagent per group, results merged, which is the pattern
`adversarial-review` already uses with a cap of five reviewers.

## Risks and open questions

- **Two reviewers for one diff.** `review` and `adversarial-review` will sometimes disagree. That is the
  point of a second opinion, but the skill must say which one to trust for what: `adversarial-review`
  for defects (it refutes), this one for compliance, suggestions and the effort and risk summary.
- **The skill's trigger phrases must not steal from `adversarial-review`**, whose description already
  claims "review, audit, check" in any language. The new description names PR-Agent and its three
  tools explicitly.
- **The upstream prompts move** (the clone was pushed to the day it was taken). The pinned commit header
  makes drift visible; syncing stays a manual decision.
- **Cost is subscription usage, not zero.** `improve` is two passes and `all` is four or more subagent
  runs; a large diff multiplies that.
- **Third party PR content is untrusted input.** A PR body, a commit message or a code comment can carry
  text aimed at the reviewer. The skill treats all of it as data, reports any such text to the
  contributor, and never runs a command a PR suggests.
- Open: whether `can_be_split` and the `contribution_time_cost_estimate` fields earn their place, or are
  dropped from the port as noise for a single maintainer.

## Verification

- **Known defects are caught.** Run `review` and `improve` on the commits that preceded three fixes the
  history already records, each a defect later found in review, and show each is reported:
  - `4f59184` (a reconcile task in flight going stale when a synchronous pass lands, #238);
  - `b6be008` (a reconcile pass that installs nothing reported as current, #247);
  - the parent of the Content-Length fix in #259.
- **Compliance works both ways.** On a branch that implements its issue, every Verification item is
  compliant or marked for human verification. On the same branch with one planned item reverted, that
  item is listed as not compliant.
- **The reflection pass filters.** A seeded suggestion that only asks to verify something scores 0 and is
  dropped.
- **PR mode works on a third party PR:** a Dependabot PR by number, read only, with nothing posted
  (checked in the PR's timeline).
- **Injected instructions are reported, not followed:** a fixture PR body carrying an instruction to the
  reviewer.
- `describe` on a merged PR reports where its PR body and its diff disagree.
- Part 1A: `adversarial-review` still pins the same diff after the extraction, shown on one branch before
  and after.
- No em dash in any added file, and the license notice is present.

## PR

**Branch**

```
feat/pr-agent-review-skill
```

**Description**

```
Adds a pr-agent-review skill that ports PR-Agent's review, improve and describe tools to Claude Code, so they run on the existing subscription instead of a per-token API. It reviews the current branch or a pull request by number, checks the diff against the Plan and Verification of the issue it implements, scores its own code suggestions in an independent pass, and prints everything to the terminal without posting to GitHub. The diff pinning it shares with adversarial-review moves into one script both skills call.
```
