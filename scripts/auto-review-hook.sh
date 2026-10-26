#!/usr/bin/env bash
# Claude Code Stop hook: when a branch with commits ahead of origin/main has never been reviewed, says so
# once, in a message the contributor sees. It never blocks the turn. The review itself is the
# adversarial-review skill's, run when the contributor asks for it or when prepare-commits starts it, and
# whether a round has run is review-state.sh's to say.
#
# Why it does not block: a Stop hook that blocks keeps Claude working on the turn, and every fix then
# asked for another review. That loop is what the round cap in review-state.sh ends, and a hook that asked
# again on every change would be the loop itself.
#
# Silent (exit 0, no output) when:
#   - stop_hook_active is true (the stop that follows an earlier message of this hook);
#   - MALACHI_SKIP_AUTO_REVIEW=1 is set;
#   - the session is in plan mode, where nothing is implemented yet;
#   - the directory is not a git work tree, has no commit, or has no origin/main to compare with;
#   - nothing is committed on the branch since it left origin/main;
#   - a round has already run for this base, or the reminder was given (review-state.sh remind).
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

# Where review-state.sh is, before the cd below moves away from the hook's own directory.
here=$(cd "$(dirname "$0")" && pwd) || exit 0

# git reads these ahead of the directory it runs in, so a session started from inside a git hook would
# otherwise have this look at another repository.
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE

cd "${CLAUDE_PROJECT_DIR:-$PWD}" 2> /dev/null || exit 0
git rev-parse --is-inside-work-tree > /dev/null 2>&1 || exit 0
git rev-parse --verify --quiet HEAD > /dev/null || exit 0
base=$(git merge-base HEAD origin/main 2> /dev/null) || exit 0
ahead=$(git rev-list --count "$base..HEAD" 2> /dev/null) || exit 0
[ "$ahead" -gt 0 ] || exit 0

"$here/review-state.sh" remind || exit 0

printf '%s\n' '{"systemMessage":"This branch has commits ahead of origin/main and no adversarial review has run on them yet. Before the work is called done, run the adversarial-review skill: its first round is the full review. To turn this reminder off for a session, set MALACHI_SKIP_AUTO_REVIEW=1."}'
exit 0
