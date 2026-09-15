---
name: prepare-commits
description: 'Prepare commits for the user to run, never commit. Use when the user asks to prepare, split, organize or write commits, commit messages, or a commit script for the current changes, in any wording and in any language, including a closing request to prepare the commit at the end of a task. Groups every change in the working tree into commits (hunks of one file may go to different commits), writes one message file and one patch per commit, and a script that applies each patch to the index and commits it in order, then deletes the patches, the messages and itself. Verifies that replaying the patches on HEAD reproduces the intended tree before handing over.'
---

# Preparing commits for the user to run

The user commits; the agent prepares. What gets handed over is one command that creates every commit in
order with the right changes in each, and leaves nothing behind. The agent never runs `git commit` or
`git push`, even when the user asks it to in the moment: preparing is the whole job.

Inside a worktree, run git as `/usr/bin/git`. A shell hook rewrites a bare `git` and the guard then
blocks it.

## 0. Look for an earlier set before writing a new one

```
ls commit_message* commit_*.patch 2>/dev/null
/usr/bin/git log --format=%s -10
```

A script from an earlier request may already have run. Compare the first line of each message file
with the recent subjects. **If the commits already landed, the files are stale leftovers: delete them,
never rewrite them.** Rewriting a script after the user ran it recreates exactly the files its cleanup
removed. If they have not landed, the user may still mean to run them: ask before replacing them.

## 1. Inventory every change

```
/usr/bin/git status --porcelain
/usr/bin/git diff HEAD --stat
```

Every entry is assigned to a commit or raised with the user. **Never leave a change out on your own
judgment**, including one you did not make, such as a file deleted by someone else: the user may well
expect it committed, and a change silently left behind is discovered only after the push. Ask when a
change's origin or purpose is unclear, and ask before building, not after handing over.

Changes that are clearly not work (a local `.env`, editor state) are named in the question too, so the
user decides rather than the agent.

## 2. Group the changes into commits

One purpose per commit, in an order where each builds on the last:

- A fix for pre-existing debt found along the way goes in its own commit, apart from the feature, so
  reverting one does not drag the other (see `CONTRIBUTING.md`).
- A bug found and fixed while building a feature is its own commit when it stands alone.
- Tests go in the commit whose behaviour they pin, not in a trailing "tests" commit.
- One file may belong to several commits: split it by hunk, never force it into one.

Show the grouping (subject and files per commit, and any file split across commits) and ask when more
than one split is reasonable. When the split is plain, state it and proceed.

## 3. Build one tree per commit, in private indexes

Never use the user's index: they may have staged something of their own. Each state is built in a
private index file and captured with `git write-tree`, so nothing in the working tree or the real index
moves.

```
scratch=$(mktemp -d)                      # or $CLAUDE_JOB_DIR/tmp in a background job
export GIT=/usr/bin/git
$GIT rev-parse HEAD > "$scratch/base"
```

For each commit `k`, start from the previous tree (HEAD's tree for the first) and add that commit's
changes:

```
GIT_INDEX_FILE="$scratch/idx$k" $GIT read-tree "<previous tree>"
GIT_INDEX_FILE="$scratch/idx$k" $GIT add -- <whole files of commit k>
GIT_INDEX_FILE="$scratch/idx$k" $GIT apply --cached "$scratch/hunks$k.patch"   # only if a file is split
tree_k=$(GIT_INDEX_FILE="$scratch/idx$k" $GIT write-tree)
```

A split file gets a patch holding only the hunks of commit `k`, cut from `git diff HEAD -- <file>`
(whole hunks, in order; `git apply` absorbs the offsets). **The last commit is always built from the
whole working tree** rather than from a list, so no change can fall between the lists:

```
GIT_INDEX_FILE="$scratch/idxN" $GIT read-tree "<previous tree>"
GIT_INDEX_FILE="$scratch/idxN" $GIT add -A -- . ':!commit_message*' ':!commit_*.patch' <exclusions the user agreed to>
```

Two pathspec rules, both learned by breaking them:

- Never name an ignored path in a pathspec, not even as an exclusion (`':!tmp'` when `tmp/` is
  ignored). `git add` aborts on it and stages nothing. Ignored paths are left out already.
- A deleted file is a change like any other: `add -A` stages the deletion. Exclude it by name only if
  the user agreed to leave it out.

When the work was done in stages during the session, the simplest exact trees are the snapshots taken
at the end of each stage, with the same private-index commands over the working tree at that moment.

Then one patch per commit:

```
$GIT diff --binary <previous tree> "$tree_k" > commit_$k.patch
```

## 4. Prove the patches replay to the intended tree

```
GIT_INDEX_FILE="$scratch/check" $GIT read-tree HEAD
for p in commit_*.patch; do GIT_INDEX_FILE="$scratch/check" $GIT apply --cached "$p"; done   # in order
[ "$(GIT_INDEX_FILE="$scratch/check" $GIT write-tree)" = "<last tree>" ] && echo replay ok
```

Do not hand over without `replay ok`. With more than nine commits, list the patches explicitly rather
than trusting the glob's order.

## 5. Write the messages

`commit_message.txt` for a single commit, `commit_message_1.txt`, `commit_message_2.txt` and so on for
several, at the repository root. The repository ignores `*.txt`, so they do not show in `git status`.

- Conventional Commits subject, `type(scope): summary`, in English.
- A prose body for the reader a year from now: what changed, why the alternative was not taken, what it
  does not cover (`CONTRIBUTING.md`).
- No double quotes and no em dashes anywhere in the message.
- The attribution trailer only as the session's instructions give it; if the user removed it from a
  message, leave it out of the next ones.
- Closing keywords (`Closes #N`) only on the commit that completes the issue.

## 6. Write the script

`commit_message.sh` at the repository root, made executable in the same step (a file written by the
edit tool is not executable, and the user's first run then fails with permission denied):

```bash
#!/usr/bin/env bash
# Creates the commits for <what>, in order, from the patches beside this script, then deletes the
# patches, the messages and itself. Each patch is applied to the INDEX only, so a file split across
# commits lands in each with exactly its own hunks; nothing in the working tree changes.
set -euo pipefail

cd "$(dirname "$0")"
GIT=/usr/bin/git
BASE=<sha of HEAD when the patches were built>

commits=(
  "commit_1.patch commit_message_1.txt"
  "commit_2.patch commit_message_2.txt"
)

# The patches were cut against BASE; on any other HEAD they would apply to the wrong tree or not at all.
if [ "$("$GIT" rev-parse HEAD)" != "$BASE" ]; then
  echo "HEAD moved since the patches were built (expected $BASE); nothing was committed" >&2
  exit 1
fi

# Something already staged would ride along in the first commit. Refuse rather than unstage it.
if ! "$GIT" diff --cached --quiet; then
  echo "the index has staged changes; commit or unstage them first; nothing was committed" >&2
  exit 1
fi

for entry in "${commits[@]}"; do
  read -r patch message <<< "$entry"
  [ -f "$patch" ] && [ -f "$message" ] || { echo "missing $patch or $message; nothing was committed" >&2; exit 1; }
done

for entry in "${commits[@]}"; do
  read -r patch message <<< "$entry"
  "$GIT" apply --cached --check "$patch"
  "$GIT" apply --cached "$patch"
  "$GIT" commit -q -F "$message"
  echo "committed: $("$GIT" log -1 --format=%s)"
done

rm -f -- commit_*.patch commit_message*.txt "$0"
"$GIT" status --short
```

```
chmod +x commit_message.sh && bash -n commit_message.sh
```

A single commit with no split file may skip the patch and `git add` its files instead, keeping the same
checks and the same cleanup.

## 7. Hand over

Give the user the one command, on its own line, and what it will create:

```
! <repository root>/commit_message.sh
```

List each commit's subject and files, name anything deliberately left uncommitted and why (agreed with
the user in step 1), and remind that pushing is theirs. Delete the scratch directory.

After handing over, do not touch the script, the patches or the messages again without first checking
`git log` for their subjects (step 0).

## What not to do

- Do not run `git commit` or `git push`, and do not run the script.
- Do not use the user's index, `git stash` or `git reset` to build commits.
- Do not leave a change out, or fold an unrelated one in, without asking.
- Do not stage whole paths per commit when a file spans commits.
- Do not name an ignored path in a pathspec.
- Do not hand over without `replay ok` and an executable script.
