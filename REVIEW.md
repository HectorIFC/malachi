# Review instructions

These are the review criteria for Malachi. The managed Code Review reads this file on every pull request,
the local `adversarial-review` skill (`.claude/skills/adversarial-review/`) hands it to each of its
reviewers, and the `pr-agent-review` skill (`.claude/skills/pr-agent-review/`) gives it to PR-Agent's
prompts as their review standards, so a change is judged by one standard wherever it is reviewed. `CLAUDE.md` and
`CONTRIBUTING.md` still apply; this file says what a reviewer should flag and how hard.

Reviews here are adversarial on purpose (`CONTRIBUTING.md`). The job is to find the input, the
interleaving or the failure that breaks the change, not to agree with it. A finding against good work
is a good outcome.

## What Malachi is, for a reviewer

Malachi is an open source reimplementation of LinkedIn's NorthGuard log broker on the BEAM. A change is
measured against two things: whether it keeps every acknowledged write, and whether it keeps Malachi
shaped like NorthGuard. `docs/ARCHITECTURE.md` describes that shape. The short version a reviewer needs:

- metadata is sharded across vnodes, each a Raft group whose leader is its **coordinator** and carries
  out the protocols on the metadata it owns;
- metadata is routed to its owning vnode by hash; the only state every node knows is which vnodes exist,
  who is alive, and where they are, spread by gossip;
- the unit of replication is the segment; a failed replica seals the segment and a new one takes over;
- every replica fsyncs before a produce is acknowledged.

A change that moves away from this, or deepens a divergence that already exists, is a finding even when
the code is correct.

## Severity

**Important** means one of these, and nothing else:

- acknowledged data can be lost, misreported (a successful read that returns nothing where records
  exist), or deleted;
- a destructive act (removing a directory, deleting a segment, fencing, sealing) can happen on a view or
  a state that may be stale or incomplete;
- a crash, deadlock or unbounded wait on a path a client or a control plane loop depends on;
- a divergence from the NorthGuard design above that the change introduces or deepens;
- a test presented as a regression guard that passes without the change;
- documentation that promises a guarantee the code does not deliver.

Everything else is a **nit**.

## Nit volume

At most five nits per review, the most useful first. If there are more, say how many were left out.
Formatting and naming nits that `mix format` and `mix credo --strict` already enforce are never raised.

## Skip

- `deps/`, `_build/`, `doc/`, `cover/`, `tmp/`, and `benchmark/published/`, which a CI job refreshes.
- The files the `prepare-commits` skill generates at the root: `commit_message.sh`, `commit_<n>.patch`,
  `commit_message*.txt`.
- Style preferences with no rule behind them in this file, `CLAUDE.md` or `CONTRIBUTING.md`.

## Repository checks, on every change

- **Every Logger call goes through `Malachi.I18n`.** No raw string reaches Logger, and two call sites
  that say different things use two keys.
- **No em dash** anywhere: code, comments, docs, commit messages.
- **A new or changed behaviour has a test that fails without the change.** The PR or commit says which
  tests fail without it, and how that was shown.
- **Coverage on touched files aims at 100%, 80% is the floor.** A new branch with no test is a finding.
- **Docs move with the code.** A moduledoc, a guide or a comment that describes a guarantee must match
  what the code now does; `mix docs --warnings-as-errors` must pass, which rules out a doc link to a
  private function.
- **Pre-existing debt goes in its own commit**, apart from the feature it was found next to.
- **Performance claims carry a measurement** taken on Linux (local Docker or CI), with the noise floor
  stated. Nothing is measured on macOS.
- **Remote calls on a server's loop are bounded.** A call that can wait on another node (ra, the data
  plane, a peer) inside a `handle_*` callback needs a timeout far below the default five seconds, or
  belongs in a task.
- **A function a Raft leader applies must exist on every node.** A consistent query or command that
  ships a Malachi function to a leader breaks a rolling upgrade or a rolled back build; ra applies it
  without a catch.

## Verification bar

A finding is raised only with:

- the file and line it is about;
- a concrete failure: the input, the interleaving or the fault that produces the wrong outcome;
- the code that shows it, quoted, and a command that demonstrates it when one exists.

A suspicion without a failure scenario is not raised. A finding that depends on something the reviewer
could not see is labelled as such rather than stated as fact. Every finding is checked by a second pass
that tries to refute it against the actual code, and only what survives is reported.

## Re-review

When the change was already reviewed, raise only what is new or what the latest commits changed. A
finding the author declined or answered is not raised again unless the code it rests on changed; say
that it was left as decided.

## Summary shape

Start with one line: how many important findings, how many nits, how many candidates were refuted. Then
the findings, important first, each numbered, with its file and line, the failure, and the evidence.
When options are offered, letter them and put the recommended one first, and say whether each moves
Malachi toward or away from NorthGuard. No praise, no restating the diff.
