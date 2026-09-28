#!/usr/bin/env bash
# Claude Code Stop hook: when a turn ends with this branch carrying changes, keep Claude going for one
# more step and have it run the repository's adversarial-review skill on them.
#
# How it works: a Stop hook that prints {"decision":"block","reason":...} stops Claude from finishing
# and hands it the reason as its next instruction. Claude then sets stop_hook_active on the stop that
# follows, and that stop is let through, so the review runs once per turn and never loops.
#
# Silent (exit 0, no output) when:
#   - this stop follows the review this hook asked for (stop_hook_active is true);
#   - MALACHI_SKIP_AUTO_REVIEW=1 is set;
#   - the session is in plan mode, where nothing is implemented yet;
#   - the directory is not a git work tree;
#   - the branch has no changes: nothing committed since it left origin/main, nothing modified, nothing
#     untracked, apart from the files the prepare-commits skill generates;
#   - the changes are exactly the ones it already asked to have reviewed. It keeps a fingerprint (the
#     base and the id of the tree the working tree would commit) in this worktree's own git directory,
#     so a turn that changed no content (an answer to the review's options, a question, a turn opened by
#     a background task finishing, a commit of reviewed work) does not start the same review again. A
#     turn that changes content does. The fingerprint is recorded
#     when the review is ASKED for, so a review that was interrupted does not come back by itself; ask
#     for it by name.
#
# Reads the hook input on stdin. Always exits 0: a hook that fails must not wedge the session.
set -uo pipefail

input=$(cat)

field_is() {
  printf '%s' "$input" | grep -Eq "\"$1\"[[:space:]]*:[[:space:]]*$2"
}

field_is stop_hook_active 'true' && exit 0
[ "${MALACHI_SKIP_AUTO_REVIEW:-}" = "1" ] && exit 0
field_is permission_mode '"plan"' && exit 0

# git reads these ahead of the directory it runs in, so a session started from inside a git hook would
# otherwise have this look at another repository.
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE

cd "${CLAUDE_PROJECT_DIR:-$PWD}" 2> /dev/null || exit 0
git rev-parse --is-inside-work-tree > /dev/null 2>&1 || exit 0

# The branch's changes are everything since it left origin/main. Without that ref (a fresh clone of a
# fork, no remote) only what is uncommitted counts. A repository with no commit yet has nothing to
# compare against.
git rev-parse --verify --quiet HEAD > /dev/null || exit 0
base=$(git merge-base HEAD origin/main 2> /dev/null) || base=$(git rev-parse HEAD)

# Excluded by exact name, never by pattern: the prepare-commits skill's own script and patches are not
# work to review, and a wildcard would also hide a real file that happens to match. Only an untracked
# one is dropped; a file of that name already committed is the project's own.
generated='^(commit_message\.sh|commit_[0-9]+\.patch)$'

# What the review would look at is the tree the working tree would commit, so that is what is
# fingerprinted, in git's own terms: content, file modes, symlink targets and submodule commits alike,
# and no file-name sorting that depends on a locale. It is built with git add -A in a private index and
# a private object directory, with the repository's objects as a read-only alternate, so nothing is
# written to the repository (which may be a read-only mount) and the contributor's index is left
# alone. The index starts as a copy of the real one, so git reuses its stat cache; it is written back
# whole (core.splitIndex off), since a split index would put its shared part in the repository. Objects
# are stored uncompressed: they are thrown away at the end, and compressing every untracked byte on
# every stop is what made a large tree outlast the hook's timeout. A tree id changes
# exactly when some content does: a question, an answer to the review's options, a turn opened by a
# background task, or a commit of reviewed work does not ask again, and any content change does.
objects=$(git rev-parse --git-path objects)
case "$objects" in /*) ;; *) objects="$PWD/$objects" ;; esac
index=$(git rev-parse --git-path index)
work=$(mktemp -d "${TMPDIR:-/tmp}/malachi-auto-review.XXXXXX" 2> /dev/null) || work=""

# LC_ALL=C: the one message read back from git ("unable to index file") has to come out in English
# whatever the contributor's locale. core.safecrlf off: with it on, one file whose line endings a
# .gitattributes rule would convert makes git add fatal, and the tree would silently be the old one.
private_git() {
  LC_ALL=C GIT_INDEX_FILE="$work/index" GIT_OBJECT_DIRECTORY="$work/objects" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$objects" \
    git -c core.splitIndex=false -c core.compression=0 -c core.looseCompression=0 -c core.safecrlf=false "$@"
}

fingerprint=""
if [ -n "$work" ] && mkdir "$work/objects" 2> /dev/null; then
  trap 'rm -rf "$work"' EXIT
  cp "$index" "$work/index" 2> /dev/null || private_git read-tree HEAD 2> /dev/null
  # --ignore-errors: a file git cannot read is left out rather than failing the whole question. The
  # names git could not index go into the fingerprint, so that file still counts; nothing else git
  # says does, since its warnings (a line-ending conversion, an embedded repository) come and go with
  # the stat cache and would make a commit of reviewed work look like new work.
  # 0 is a clean add and 1 is one that skipped what it could not read; anything else means git gave up,
  # the private index is still the old one, and a tree built from it would hide every change. Then
  # the hook asks without a memory (below) rather than trust it.
  private_git add -A --ignore-errors -- . 2> "$work/errors"
  added=$?
  if [ "$added" -le 1 ]; then
    while IFS= read -r -d '' name; do
      [[ "$name" =~ $generated ]] && private_git rm -q --cached -- "$name" 2> /dev/null
    done < <(git ls-files -z --others --exclude-standard 2> /dev/null)
    tree=$(private_git write-tree 2> /dev/null) || tree=""
  fi
fi

if [ -n "${tree:-}" ]; then
  unreadable=$(grep 'unable to index file' "$work/errors" 2> /dev/null | LC_ALL=C sort)
  listed=$(private_git diff --name-only -z "$base" "$tree" -- 2> /dev/null | tr -cd '\0' | wc -c)
  changed=$((listed + $(printf '%s' "$unreadable" | grep -c .)))
  fingerprint="$base $tree $(printf '%s' "$unreadable" | git hash-object --stdin)"
else
  # No private tree (TMPDIR missing or not writable, or git gave up building it): ask on any change,
  # without a memory of what was asked, rather than stay silent about work nobody reviewed. Counted without
  # touching the index: what differs from HEAD, from `status` (which, unlike `diff`, honours
  # --no-optional-locks and leaves the index alone), plus what the branch committed since the base, from
  # a tree-to-tree diff.
  changed=0
  while IFS= read -r -d '' name; do
    changed=$((changed + 1))
  done < <(
    {
      git --no-optional-locks status --porcelain --no-renames -z --untracked-files=all 2> /dev/null |
        while IFS= read -r -d '' entry; do
          [[ "${entry:0:2}" == "??" && "${entry:3}" =~ $generated ]] || printf '%s\0' "${entry:3}"
        done
      git diff --name-only -z "$base" HEAD -- 2> /dev/null
    } | LC_ALL=C sort -z -u
  )
fi

[ "$changed" -gt 0 ] || exit 0

# Kept in this worktree's own git directory. One that cannot be written (a read-only mount) keeps it in
# a directory of this user's own under the temporary directory, named after the git directory, so the
# hook still asks once per diff there rather than on every stop. That directory is trusted only when
# this user created it and it is not a symlink: a shared /tmp is writable by everyone, and a name any
# user can compute is a name another user can plant a link at. Anything else and the hook stays quiet.
git_dir=$(git rev-parse --absolute-git-dir 2> /dev/null)
state="$git_dir/malachi-auto-review"
if ! { : >> "$state"; } 2> /dev/null; then
  private="${TMPDIR:-/tmp}/malachi-auto-review.$(id -u)"
  mkdir -m 700 "$private" 2> /dev/null
  { [ -d "$private" ] && [ ! -L "$private" ] && [ -O "$private" ]; } || exit 0
  state="$private/$(printf '%s' "$git_dir" | git hash-object --stdin)"
fi

if [ -n "$fingerprint" ]; then
  [ "$(cat "$state" 2> /dev/null)" = "$fingerprint" ] && exit 0
  { printf '%s\n' "$fingerprint" > "$state"; } 2> /dev/null
fi

printf '{"decision":"block","reason":"%s"}\n' \
  "This turn ended with ${changed} changed file(s) on the branch. Before finishing, run the adversarial-review skill (Skill tool, skill: adversarial-review) over the current diff and follow it exactly: it reviews against REVIEW.md with parallel reviewers, verifies every finding adversarially, and ends by offering the contributor numbered findings with lettered options. Do not apply any fix before the contributor chooses. If the contributor already asked in this conversation not to review, say so in one line and stop. To turn this off, set MALACHI_SKIP_AUTO_REVIEW=1."
exit 0
