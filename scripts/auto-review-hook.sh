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

# What the review would look at is the tree the working tree would commit, so that is what is
# fingerprinted: built in a private index, so the contributor's own index is never touched, and
# started from a copy of it, so git reuses its stat cache instead of hashing every file again. A tree
# id changes exactly when some content does. Committing reviewed work, which only moves files from
# untracked to committed, leaves it as it was, and so does not start the same review again.
index=$(mktemp "${TMPDIR:-/tmp}/malachi-auto-review-index.XXXXXX" 2> /dev/null) || exit 0
trap 'rm -f "$index"' EXIT
cp "$(git rev-parse --git-path index)" "$index" 2> /dev/null || GIT_INDEX_FILE="$index" git read-tree HEAD
GIT_INDEX_FILE="$index" git add -A -- . 2> /dev/null || exit 0

# Excluded by exact name, never by pattern: the prepare-commits skill's own script and patches are not
# work to review, and a wildcard would also hide a real file that happens to match. Only an untracked
# one is dropped; a file of that name already committed is the project's own. NUL-separated, which git
# prints verbatim, since a C-quoted name is not a path.
generated='^(commit_message\.sh|commit_[0-9]+\.patch)$'
while IFS= read -r -d '' name; do
  [[ "$name" =~ $generated ]] && GIT_INDEX_FILE="$index" git rm -q --cached -- "$name" 2> /dev/null
done < <(git ls-files -z --others --exclude-standard 2> /dev/null)

tree=$(GIT_INDEX_FILE="$index" git write-tree 2> /dev/null) || exit 0
changed=$(git diff --name-only -z "$base" "$tree" -- 2> /dev/null | tr -cd '\0' | wc -c)

[ "$changed" -gt 0 ] || exit 0

fingerprint="$base $tree"

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

[ "$(cat "$state" 2> /dev/null)" = "$fingerprint" ] && exit 0
{ printf '%s\n' "$fingerprint" > "$state"; } 2> /dev/null

printf '{"decision":"block","reason":"%s"}\n' \
  "This turn ended with ${changed} changed file(s) on the branch. Before finishing, run the adversarial-review skill (Skill tool, skill: adversarial-review) over the current diff and follow it exactly: it reviews against REVIEW.md with parallel reviewers, verifies every finding adversarially, and ends by offering the contributor numbered findings with lettered options. Do not apply any fix before the contributor chooses. If the contributor already asked in this conversation not to review, say so in one line and stop. To turn this off, set MALACHI_SKIP_AUTO_REVIEW=1."
exit 0
