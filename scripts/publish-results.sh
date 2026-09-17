#!/usr/bin/env bash
# Publishes the results a push run of .github/workflows/results.yml measured, by committing them to main.
#
# Runs in a checkout of the commit the run measured (PUBLISH_SHA), after the collect step has copied this
# run's results into benchmark/published. Only the files that step changed are this run's. Each is
# written to main only when main does not already hold a result for a NEWER commit in that file, told by
# the commit its `meta.git_ref` names:
#
# - main has no such file, or its ref cannot be resolved: this run's result is written. A published file
#   that says nothing about its provenance must not block every later run from publishing.
# - main's file measured an older commit than PUBLISH_SHA: this run's result is written.
# - main's file measured PUBLISH_SHA itself: this run's result replaces it. That is a rerun of a run that
#   already published, and the rerun is the later measurement of the same commit.
# - main's file measured a commit newer than PUBLISH_SHA, or one the history cannot order against it:
#   main's file is kept, with a notice. Stale results never overwrite newer ones.
#
# When nothing is left to write the run ends green with a notice saying why; nothing about it is wrong.
#
# The results commit is always rebuilt on the latest main rather than rebased onto it. This job spends
# minutes measuring after its checkout, so main has usually moved by the time it publishes, and a
# rebase of one results commit onto another stopped on a content conflict instead of deciding which
# result to keep (run 35147990781, attempt 2). Rebuilding also repeats the decision against whatever
# main holds at each attempt, so a push that loses a race is retried with fresh information, a bounded
# number of times: a push that keeps losing is a busy main, and failing then is the honest outcome.
#
# Needs the full history (the checkout's fetch-depth: 0), because ordering two commits is an ancestry
# question.
#
# Env: PUBLISH_SHA (required), PUBLISH_REMOTE (origin), PUBLISH_BRANCH (main), PUBLISH_ATTEMPTS (3).
set -euo pipefail

PUBLISHED=benchmark/published
REMOTE="${PUBLISH_REMOTE:-origin}"
BRANCH="${PUBLISH_BRANCH:-main}"
ATTEMPTS="${PUBLISH_ATTEMPTS:-3}"
SUBJECT="chore(results): refresh published benchmark and chaos results from CI"

fail() { echo "$*" >&2; exit 1; }

[ -n "${PUBLISH_SHA:-}" ] || fail "PUBLISH_SHA is required: the commit this run measured"
case "$ATTEMPTS" in
  '' | *[!0-9]* | 0*) fail "PUBLISH_ATTEMPTS must be a positive integer, got '$ATTEMPTS'" ;;
esac
command -v jq > /dev/null 2>&1 || fail "jq is required"
[ "$(git rev-parse --is-shallow-repository)" = false ] ||
  fail "the checkout is shallow; ordering commits needs the full history (fetch-depth: 0)"
ours="$(git rev-parse --verify --quiet "${PUBLISH_SHA}^{commit}")" ||
  fail "PUBLISH_SHA $PUBLISH_SHA is not a commit in this repository"

# This run's results: the files the collect step added or changed. A deletion is never this run's
# result, since the collect step only copies files in on a push.
mapfile -t files < <(git status --porcelain --untracked-files=all -- "$PUBLISHED" |
  awk '$1 != "D" { print $NF }' | grep -E '\.json$' || true)
if [ "${#files[@]}" = 0 ]; then
  echo "results are unchanged; nothing to commit"
  exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
for f in "${files[@]}"; do
  mkdir -p "$work/$(dirname "$f")"
  cp "$f" "$work/$f"
done

# The commit a published file on main says it measured, as a full sha, or nothing.
published_ref() {
  local ref
  ref="$(git show "$REMOTE/$BRANCH:$1" 2> /dev/null | jq -r '.meta.git_ref // empty' 2> /dev/null)" || return 0
  [ -n "$ref" ] || return 0
  git rev-parse --verify --quiet "${ref}^{commit}" 2> /dev/null || true
}

# Writes this run's copy of $1 over the checkout of main, or keeps main's, and says which and why.
decide() {
  local f="$1" theirs
  if ! git cat-file -e "$REMOTE/$BRANCH:$f" 2> /dev/null; then
    echo "$f: main has none; writing this run's result"
  else
    theirs="$(published_ref "$f")"
    if [ -z "$theirs" ]; then
      echo "$f: main's copy names no commit this history has; writing this run's result"
    elif [ "$theirs" = "$ours" ]; then
      echo "$f: main's copy measured this same commit; this rerun replaces it"
    elif git merge-base --is-ancestor "$theirs" "$ours"; then
      echo "$f: main's copy measured an older commit (${theirs:0:7}); writing this run's result"
    elif git merge-base --is-ancestor "$ours" "$theirs"; then
      echo "::notice::$f: main already holds a result for a newer commit (${theirs:0:7}); keeping it"
      return 0
    else
      echo "::notice::$f: main's copy measured ${theirs:0:7}, which cannot be ordered against ${ours:0:7}; keeping it"
      return 0
    fi
  fi
  mkdir -p "$(dirname "$f")"
  cp "$work/$f" "$f"
}

for attempt in $(seq 1 "$ATTEMPTS"); do
  git fetch --quiet "$REMOTE" "$BRANCH"
  git checkout --quiet --force --detach "$REMOTE/$BRANCH"
  git clean --quiet --force -- "$PUBLISHED"

  for f in "${files[@]}"; do
    decide "$f"
  done

  if [ -z "$(git status --porcelain --untracked-files=all -- "$PUBLISHED")" ]; then
    echo "::notice::nothing to publish: main already holds these results or newer ones"
    exit 0
  fi

  git add -- "$PUBLISHED"
  git commit --quiet -m "$SUBJECT" -m "Measured on ${ours}."
  if git push --quiet "$REMOTE" "HEAD:refs/heads/$BRANCH"; then
    echo "published the results of ${ours:0:7} to $BRANCH"
    exit 0
  fi
  echo "push rejected (attempt ${attempt}); rebuilding on the current $BRANCH"
done

fail "still could not push after $ATTEMPTS attempts"
