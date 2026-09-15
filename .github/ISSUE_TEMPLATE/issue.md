---
name: Issue
about: "The standard shape for anything tracked here: a bug, a change, a measurement, a doc gap"
title: ''
labels: ''
assignees: HectorIFC
---

<!--
The same five sections for every issue, whatever its label. They exist because each one answers a
question that costs real time when it is missing:

  Context      what is wrong and how it is known, so nobody re-derives it
  Plan         the options that were weighed, so a rejected one is not re-proposed later
  Risks        what could break or is still unknown, so a plan is not trusted past its evidence
  Verification how anyone tells the work is done and not merely finished
  PR           where the work lands

Delete these comments as you fill the sections in. An issue that cannot fill them is usually not ready
to be worked on, and saying that is more useful than filing it half-formed.

Labels carry the triage: a `bug` label always means Priority P0 on the board.
-->

## Context

<!--
What is wrong, why it matters, and how it is known. Prefer evidence over assertion: a file and line, a
measured number, a log line, a failing run. If the claim came from reading code rather than from
running it, say so, and say what would confirm it.

State the consequence in the terms someone else would feel it: data unreachable, a node down, a
misleading number in a README, an operator who cannot tell two failures apart.
-->

## Plan

<!--
Two or more options, each with its cost, and a recommendation.

**A. ...** what it changes, what it costs, what it risks.
**B. ...**
**C. Do nothing.** Almost always worth stating, because it names what living with the problem means.

Recommendation: ... and why, in the terms of the tradeoff rather than as a preference.

One option is not a plan, it is a decision whose work is hidden. Recording the rejected options is what
stops the same idea coming back in six months with the same argument.
-->

## Risks and open questions

<!--
Required. What could this break, what is assumed but unverified, what would have to be measured before
the plan is safe. An honest open question here is worth more than a confident sentence that turns out
to be wrong. When nothing is at risk, say what was checked to conclude that: an empty section and one
that says none, because X was checked, read the same only until X turns out to matter.
-->

## Verification

<!--
How the work is proven, not merely finished.

- The test that fails before the change and passes after (say which, and what it asserts).
- Non-vacuity: would this test still fail if the fix were reverted? A test that passes either way
  proves nothing, and that has happened here before.
- The checks this repo runs: full suite including multinode, mix format --check-formatted,
  mix credo --strict, mix dialyzer, mix docs --warnings-as-errors, coverage on touched files.
- For anything performance related, the measurement itself is the deliverable, including when the
  answer is "no measurable difference". State the harness, the repetitions, and the noise floor
  BEFORE running, or the measurement will confirm whatever was already believed.
-->

## PR

**Branch**

<!--
The branch name for this work, in a code block so it can be copied. It is read by tooling, not only by
people: the start-issue-work skill takes the branch name from here to create the worktree, so a missing
or paraphrased name stops the automation.

Name it after what the work does, not after the mechanism it was expected to use: a name that promises
an approach becomes a lie when the plan changes.
-->

```

```

**Description**

<!-- One or two sentences for the PR body, in a code block so it can be copied as-is. -->

```

```
