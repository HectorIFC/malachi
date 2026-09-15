# Contributing to Malachi

Malachi is an OSS reimplementation of LinkedIn's NorthGuard: a CP log broker on the BEAM, where a
record belongs to a segment, a segment is the unit of replication, and an acknowledged write is durable
on a majority before the client hears about it.

The mechanics of contributing (fork, branch, quality checks, Conventional Commits, the git hooks setup)
are in the [README's Contributing section](README.md#-contributing). This document covers what the
README does not: where to start reading, what "done" means here, and the conventions this project
learned the hard way.

## Where to start

**Start from the [project board](https://github.com/users/HectorIFC/projects/4).** It is ordered, and
every open issue carries a Priority and a Size, so anything in Backlog sized XS or S is a reasonable
entry point. The [`good first issue`](https://github.com/HectorIFC/malachi/labels/good%20first%20issue)
label exists and is worth watching, but it is not always populated: the board is the reliable list.

**Read the umbrella before touching its theme.** Some areas have an issue that records the triage
behind them, including the options that were evaluated and rejected, and why:

| Theme | Read first |
| --- | --- |
| Throughput, the path to 1M msgs/s | [#85](https://github.com/HectorIFC/malachi/issues/85), plus the `throughput` label |
| Diskless topics on object storage | [#109](https://github.com/HectorIFC/malachi/issues/109) |
| Security assessment | [#71](https://github.com/HectorIFC/malachi/issues/71) |

This is not ceremony. #85 lists 14 throughput techniques that were evaluated and **not** adopted, each
with its reason (O_DIRECT conflicts with the BEAM deployment model; acking from page cache weakens the
fsync-before-ack contract the chaos certification enforces). Proposing one of them again without
engaging with the recorded reason wastes your time first.

**Understand the model before changing it.** [docs/guides/log-model.md](docs/guides/log-model.md)
explains what the log guarantees and, just as importantly, what it deliberately does not.
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the shorter map.

## How an issue is shaped here

Every issue uses the same five sections, whatever its label:
[Context](.github/ISSUE_TEMPLATE/issue.md), Plan, Risks and open questions, Verification, PR. The
template explains what each one is for. Two of them are worth calling out:

**The Plan lists options, not a decision.** Two or more, each with its cost, plus "do nothing" and a
recommendation. An issue with one option is a decision whose reasoning is hidden, and the rejected
options are what stop the same idea returning in six months with the same argument.

**The PR section's branch name is read by tooling.** It goes in a code block, and the repository's
`start-issue-work` skill takes the branch name from there to create the worktree. A paraphrased or
missing name stops the automation.

A `bug` label always means Priority P0 on the board.

## What "done" means

Passing CI is the floor, not the bar. Before a change is ready:

```bash
mix test --include multinode
mix format --check-formatted
mix credo --strict
mix dialyzer
mix docs --warnings-as-errors
mix coveralls
```

Coverage aims at 100% on the files a change touches, and 80% is the floor.

Changes to durability, replication, storage or failover also run the chaos drills, which inject real
faults into a 3-node cluster and certify that every acknowledged write survives:

```bash
./scripts/docker-chaos-test.sh      # node faults: kill, partition, stall, rolling restart
./scripts/docker-storage-chaos.sh   # storage faults: corruption, truncation, a full volume
```

### Three conventions that are not obvious

**A test must fail without the fix.** Write the change, then revert it and confirm the new test goes
red. This is not a formality: a set of six tests in this repository was once presented as the
regression guard for a durability fix, and every one of them passed on a tree that still had the bug.
Reverting half the fix would have kept the suite green and shipped the loss again. State in the PR
which tests fail without the change, and how many.

**A performance decision carries its benchmark, and "no difference" is a result.** Say what you will
measure, on which harness, with how many repetitions, and what noise floor makes a difference real,
*before* you run it. Otherwise the measurement confirms whatever was already believed. Issue
[#82](https://github.com/HectorIFC/malachi/issues/82) is the worked example, and it cuts both ways.
Switching `fsync` to `fdatasync` was implemented and measured against an A-A control: the median
per-flush latency moved by less than the control's own variance, the change was not merged, and that
measurement is what showed that [#83](https://github.com/HectorIFC/malachi/issues/83) had to land
before #82 could be answered. Re-measured on preallocated segments, the median still tied while the
p99 halved (381us to 182us), so the issue stayed open with a sharper question instead of being closed
on its first answer. "No difference" held for the statistic that was measured, and said nothing about
the one that was not.

**Documentation that promises more than the code delivers is a defect, not a rough edge.** A moduledoc
here once claimed the segment store fenced a returning old primary. It did not, and the gap between the
sentence and the code cost 519 acknowledged records before anyone noticed. If you change what the code
guarantees, change the sentence that describes it in the same commit; if you find a sentence that
overstates, fixing it is a contribution on its own.

## Pull requests

Keep unrelated fixes in their own commits. Pre-existing debt found along the way (a credo finding, a
stale doc, an inconsistent helper) is welcome, in a separate commit from the feature, so a revert of one
does not drag the other.

Write the commit message for the person who will read it in a year with no memory of the discussion:
what changed, why the alternative was not taken, and what it does not cover. Prose over bullet lists.

Reviews here are adversarial on purpose. A reviewer trying to break your change and failing is worth
more than one agreeing with it, and a finding against your own work is a good outcome, not an
embarrassment.

## Getting help

Open an issue with the template, or comment on an existing one. An observation you cannot yet fit into
a full Plan is still worth filing: blank issues are enabled for exactly that, and triage will shape it.
