---
name: start-issue-work
description: Set up and start planning work on a Malachi issue. Use when the user asks to begin, plan, pick up, or move on to an issue by number, in any wording and in any language, including a bare request to create its worktree or move it to Ready. Creates or updates the branch from origin/main, adds a git worktree, moves the issue to Ready on the board, launches a background planning session in plan mode, and hands back the attach command.
---

# Starting work on a Malachi issue

One issue, one branch, one worktree, one planning session. The user supervises each session in its own
terminal, so several issues can be in planning at once without competing for a checkout.

The user should not have to restate any of this. Ask only what the issue itself cannot answer.

## 1. Read the issue first

```
gh issue view <N> --json number,title,state,labels,milestone,body
```

Two things come out of the body:

- **The branch name**, from the `## PR` section's `**Branch**` block. It is authoritative: never invent
  one, and never reuse a name the issue does not give. If the section is missing or the issue lacks the
  repo's standard sections (Context, Plan, Verification, PR), stop and say so. An incomplete issue is
  triage work, not planning work, and planning against one produces a plan nobody can verify.
- **What the issue does not know.** Issues are written at a point in time and the tree moves. Check for
  related work before launching, because this is the part that changes a plan:
  - `gh api repos/HectorIFC/malachi/issues/<N>/dependencies/blocked_by` and `.../blocking`
  - the umbrella parent and sub-issues, via `addSubIssue`-style GraphQL or the issue's `parent` field
  - `git log --oneline origin/main -15` and `gh pr list --state merged --limit 10` for work that landed
    since the issue was written
  - issues the body cites: are they open, closed, or merged?

## 2. Set up the branch

The branch may already exist, and the three cases differ:

```
git fetch --prune
git rev-parse --verify --quiet refs/heads/<branch>          # local?
git rev-parse --verify --quiet refs/remotes/origin/<branch> # remote?
```

- **Neither exists**: create from `origin/main`.
- **Remote exists, no commits of its own** (`git rev-list --count origin/main..origin/<branch>` is 0):
  create the local branch tracking it, then `git merge --ff-only origin/main`. Fast-forward, so no merge
  commit and no rewriting of published history.
- **Remote exists with commits**: do not touch it. Report what is on it and let the user decide.

Always base on `origin/main`, never on the local `main`, which is often behind. Check with
`git rev-list --count main..origin/main` and say so if it is.

## 3. Create the worktree

```
git worktree add ~/malachi-<N> -b <branch> origin/main
```

for a new branch, or without `-b` when the local branch already exists. The path is `~/malachi-<N>`,
named for the issue number, which is how the user finds it later.

**Then clear the upstream if the branch is new:**

```
git branch --unset-upstream <branch>
```

Creating a branch from a remote ref makes git track that ref, so a new branch created from `origin/main`
would have `main` as its upstream and a later `git push` would aim at main. A new branch should have no
upstream; the first push then has to say `-u` explicitly.

## 4. Move the issue to Ready on the board

Project `PVT_kwHOAKYOJs4BP0nB`. Find the item, adding it to the board if it is not there:

```
gh api graphql -f query='{repository(owner:"HectorIFC",name:"malachi"){issue(number:<N>){projectItems(first:3){nodes{id}}}}}'
```

Then set Status (`PVTSSF_lAHOAKYOJs4BP0nBzg-Hr40`) to Ready (`61e4505c`) with
`updateProjectV2ItemFieldValue`. The other Status options, for when the user asks to move an issue
rather than start one: Backlog `f75ad846`, In progress `47fc9ee4`, In review `df73e18b`, Done
`98236657`. Other fields on that board: Priority
(`PVTSSF_lAHOAKYOJs4BP0nBzg-HsB0`: P0 `79628723`, P1 `0a877460`, P2 `da944a9c`) and Size
(`PVTSSF_lAHOAKYOJs4BP0nBzg-HsB4`: XS `6c6483d2`, S `f784b110`, M `7515a9f1`, L `817d0097`,
XL `db339eb2`). A `bug` label always means Priority P0.

## 5. Launch the planning session

From inside the worktree, in plan mode, in the background:

```
cd ~/malachi-<N>
claude --bg -n "<branch>" --permission-mode plan "<prompt>"
```

**The session name is the branch name, exactly, with no prefix and no issue number added.** Not the
issue title, not a shortened form. Several sessions run at once, and the name is what `claude agents`
shows and what the user reads to tell them apart: a name that matches the branch says which worktree
the session is in and which branch its work will land on, without opening it. A name that paraphrases
forces a lookup every time.

The prompt carries, in this order:

1. The task: plan the implementation of issue #N, and `gh issue view <N>` to read it.
2. That the branch is already created and checked out in this worktree, and not to create another.
3. **The context step 1 found that the issue does not have.** This is the part worth writing carefully:
   a merged PR that changes the premise, a sibling issue whose measurement already refuted an approach,
   a design that was tried and rejected with evidence. Without it the session re-derives, or worse,
   re-proposes something already disproved.
4. The decisions the issue leaves open, so the plan closes them with a recommendation instead of
   discovering them mid-implementation.
5. A pointer to the repo's `CLAUDE.md`: present each question with options and tradeoffs, and ask before
   assuming a direction.

## 6. Hand back the attach command

Print it on its own, as the first thing the user can act on, not buried in prose:

```
claude attach <id>
```

`claude agents --json` lists the running sessions with their names and directories.

## What not to do

- Do not plan in the current session when the user asked to start work on an issue. The point of the
  worktree is that planning happens in its own checkout, under its own supervision.
- Do not invent a branch name, a worktree path, or a Size.
- Do not force-update a branch that has commits of its own.
- Do not skip the related-work check to save a step. It is the step that most often changes the plan.
