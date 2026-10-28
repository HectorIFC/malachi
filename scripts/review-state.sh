#!/usr/bin/env bash
# The adversarial review's memory for the branch in this worktree: how many rounds have run against its
# current base, the tree the last complete round reviewed, and the findings already decided. The
# adversarial-review skill drives it; the Stop hook asks once, through remind, whether a branch with
# commits ahead of origin/main has been reviewed at all.
#
#   review-state.sh status            base, rounds, the cap, the reviewed tree, whether the reminder was given
#   review-state.sh begin [--extra] [--pinned <tree>]
#                                     start a round: count it, and record the tree it reviews. With --pinned,
#                                     the working tree must be that tree (the pin wrote it to tree.txt), or
#                                     nothing is counted. Exit 4 when the cap is reached and --extra was not
#                                     given; exit 5 when the tree is not the pinned one
#   review-state.sh done              the round finished: the tree it recorded becomes the reviewed one
#   review-state.sh reviewed          print the reviewed tree; nothing when there is none
#   review-state.sh decide <status> <file> <anchor> <title>
#                                     record a finding as fixed, declined, followup or open
#   review-state.sh decisions         print the decided findings, tab separated, oldest first
#   review-state.sh remind            exit 0 once, when no round has run yet for this base; 1 otherwise
#
# The round count and the reviewed tree restart when the base changes: a rebase onto a newer origin/main
# is a new branch as far as review goes, so its first round is a full one. Without origin/main the base
# is the constant no-base, which changes once origin/main exists. Decisions survive a base change.
#
# Where it lives: the two trees are kept alive by refs in this worktree's own namespace
# (refs/worktree/malachi-pending while a round runs, refs/worktree/malachi-reviewed after it). The counters
# and the decisions sit beside them in the git directory. When the git directory cannot be written (a
# read-only mount) they go under TMPDIR, in a directory of this user's own, and when that cannot be
# used either nothing is counted: the round still runs, and says so.
#
# Exit: 0 done; 1 remind had nothing to say, or a decision could not be stored; 2 usage; 4 begin refused (cap);
# 5 begin refused (the working tree is not the pinned one).
set -uo pipefail

# git reads these ahead of the directory it runs in, so a session started from inside a git hook would
# otherwise have this look at another repository.
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE

# shellcheck source-path=SCRIPTDIR
# shellcheck source=review_lib.sh
. "$(dirname "$0")/review_lib.sh"

# The cap on rounds for one base. The one place it is written down; REVIEW.md and the skill refer to it
# by name and never repeat the number.
CAP=3
PENDING=refs/worktree/malachi-pending
REVIEWED=refs/worktree/malachi-reviewed

usage() {
  echo "usage: review-state.sh status | begin [--extra] [--pinned <tree>] | done | reviewed | decide <status> <file> <anchor> <title> | decisions | remind" >&2
  exit 2
}

# Sets STATE and FINDINGS to the files that hold the counters and the decisions. Returns 1 when neither
# the git directory nor a private directory under TMPDIR takes writes.
locate() {
  local git_dir private key
  git_dir=$(git rev-parse --absolute-git-dir 2> /dev/null) || return 1
  if { : >> "$git_dir/malachi-review-state"; } 2> /dev/null; then
    STATE="$git_dir/malachi-review-state"
    FINDINGS="$git_dir/malachi-review-findings"
    return 0
  fi
  # A shared temporary directory is writable by everyone, and a name any user can compute is a name
  # another user can plant a link at: the directory is used only when this user made it and it is not a
  # link.
  private="${TMPDIR:-/tmp}/malachi-review.$(id -u)"
  mkdir -m 700 "$private" 2> /dev/null
  { [ -d "$private" ] && [ ! -L "$private" ] && [ -O "$private" ]; } || return 1
  key=$(printf '%s' "$git_dir" | git hash-object --stdin) || return 1
  STATE="$private/$key"
  FINDINGS="$private/$key.findings"
  { : >> "$STATE"; } 2> /dev/null
}

# The state file holds one "key value" line per key. Values are single words: a base id, a count, 0 or 1.
state_get() {
  awk -v k="$1" '$1 == k { v = $2 } END { print v }' "$STATE" 2> /dev/null
}

state_set() {
  local tmp="$STATE.$$"
  { awk -v k="$1" '$1 != k' "$STATE" 2> /dev/null; printf '%s %s\n' "$1" "$2"; } > "$tmp" && mv "$tmp" "$STATE"
}

current_base() {
  git merge-base HEAD origin/main 2> /dev/null || echo no-base
}

# Brings the counters up to date with the base. A base that differs from the recorded one starts over:
# no rounds, no reviewed tree, no reminder given. Decisions are not touched.
sync_base() {
  local base
  base=$(current_base)
  [ "$(state_get base)" = "$base" ] && return 0
  git update-ref -d "$PENDING" 2> /dev/null
  git update-ref -d "$REVIEWED" 2> /dev/null
  state_set base "$base" && state_set rounds 0 && state_set reminded 0
}

# What the next round is, for a warning that a round's tree could not be recorded. A reviewed tree that is
# still kept makes the next round a delta from it, whatever this round's failure was.
next_round_wording() {
  if git rev-parse --verify --quiet "$REVIEWED^{tree}" > /dev/null; then
    echo "the next round is a delta from the last complete tree"
  else
    echo "the next round is a full review"
  fi
}

rounds_run() {
  local rounds
  rounds=$(state_get rounds)
  echo "${rounds:-0}"
}

status() {
  [ $# -eq 0 ] || usage
  if ! locate; then
    echo "review-state: nowhere to keep the round count" >&2
    return 0
  fi
  sync_base
  printf 'base: %s\n' "$(state_get base)"
  printf 'rounds: %s of %s\n' "$(rounds_run)" "$CAP"
  printf 'reviewed: %s\n' "$(git rev-parse --verify --quiet "$REVIEWED^{tree}" || echo none)"
  printf 'pending: %s\n' "$(git rev-parse --verify --quiet "$PENDING^{tree}" || echo none)"
  printf 'reminded: %s\n' "$(state_get reminded)"
}

begin() {
  local extra=0 pinned="" pinned_given=0 rounds work tree commit
  while [ $# -gt 0 ]; do
    case "$1" in
      --extra) extra=1; shift ;;
      --pinned)
        [ $# -ge 2 ] && [ -n "$2" ] || usage
        pinned=$2
        pinned_given=1
        shift 2
        ;;
      *) usage ;;
    esac
  done

  if locate; then
    sync_base
    rounds=$(rounds_run)
    if [ "$rounds" -ge "$CAP" ] && [ "$extra" -eq 0 ]; then
      echo "review-state: $CAP rounds have run for this base and the cap is reached; ask the contributor for one extra round, then begin --extra" >&2
      exit 4
    fi
  else
    echo "review-state: nowhere to keep the round count, so this round is not counted against the cap" >&2
    rounds=0
  fi

  # The tree this round will review, recorded now, so that done can promote exactly it.
  git update-ref -d "$PENDING" 2> /dev/null
  work=$(mktemp -d "${TMPDIR:-/tmp}/malachi-review.XXXXXX" 2> /dev/null) || work=""
  tree=""
  if [ -n "$work" ]; then
    # Expanded now: a local the trap reads at exit is gone by then, and set -u would fail on it.
    # shellcheck disable=SC2064 # expanded now on purpose: the local is gone when the trap runs
    trap "rm -rf \"$work\"" EXIT
    tree=$(review_build_tree "$work" 2> /dev/null) || tree=""
    if [ -s "$work/unreadable" ]; then
      echo "review-state: some files could not be read; this round is not recorded as reviewed" >&2
      tree=""
    fi
  fi
  # The round reviews the diff its pin cut. A working tree that has moved since the pin (or that cannot be
  # built at all) is not that diff, so nothing is recorded and nothing is counted.
  if [ "$pinned_given" -eq 1 ] && [ "$tree" != "$pinned" ]; then
    echo "review-state: the working tree is not the one the pin read; pin again. Nothing was counted." >&2
    exit 5
  fi

  if [ -n "$tree" ]; then
    commit=$(GIT_AUTHOR_NAME=malachi-review GIT_AUTHOR_EMAIL=review@malachi.invalid \
      GIT_COMMITTER_NAME=malachi-review GIT_COMMITTER_EMAIL=review@malachi.invalid \
      git commit-tree "$tree" -m "adversarial review round" 2> /dev/null) \
      && git update-ref "$PENDING" "$commit" 2> /dev/null \
      || echo "review-state: the tree of this round could not be recorded; $(next_round_wording)" >&2
  else
    echo "review-state: the tree of this round could not be recorded; $(next_round_wording)" >&2
  fi

  if [ "$extra" -eq 1 ] && [ "$rounds" -ge "$CAP" ]; then
    echo "extra round $((rounds + 1)), past the cap of $CAP"
  else
    echo "round $((rounds + 1)) of $CAP"
  fi
  # Counted only when the counters could be stored at all.
  [ -n "${STATE:-}" ] && state_set rounds "$((rounds + 1))"
  return 0
}

done_round() {
  [ $# -eq 0 ] || usage
  if ! locate; then
    echo "review-state: nowhere to keep the reviewed tree; $(next_round_wording)" >&2
    return 0
  fi
  sync_base
  if git rev-parse --verify --quiet "$PENDING^{commit}" > /dev/null; then
    git update-ref "$REVIEWED" "$PENDING" && git update-ref -d "$PENDING"
  else
    echo "review-state: no round was recorded to finish; the reviewed tree is unchanged" >&2
  fi
  return 0
}

reviewed() {
  [ $# -eq 0 ] || usage
  locate && sync_base || return 0
  git rev-parse --verify --quiet "$REVIEWED^{tree}" 2> /dev/null || true
}

# Tabs and line breaks in a field would split the row, so they become spaces.
clean_field() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}

decide() {
  [ $# -eq 4 ] || usage
  case "$1" in
    fixed | declined | followup | open) ;;
    *) usage ;;
  esac
  if ! locate; then
    echo "review-state: nowhere to keep the decision; it is not recorded, so the finding may come back" >&2
    exit 1
  fi
  sync_base
  printf '%s\t%s\t%s\t%s\t%s\n' "$(rounds_run)" "$1" "$(clean_field "$2")" "$(clean_field "$3")" "$(clean_field "$4")" >> "$FINDINGS"
}

decisions() {
  [ $# -eq 0 ] || usage
  locate || return 0
  cat "$FINDINGS" 2> /dev/null || true
}

# The Stop hook's question: has this branch had a round yet? It says so once, and only then.
remind() {
  [ $# -eq 0 ] || usage
  locate && sync_base || exit 1
  [ "$(rounds_run)" -eq 0 ] && [ "$(state_get reminded)" != 1 ] || exit 1
  state_set reminded 1
  exit 0
}

# The tree is built from the checkout's top, whatever directory the caller runs in: from a subdirectory,
# git add would take in only that directory's paths, and the rest would keep the HEAD content.
if top=$(git rev-parse --show-toplevel 2> /dev/null); then
  cd "$top" || exit 2
fi

[ $# -ge 1 ] || usage
command=$1
shift
case "$command" in
  status) status "$@" ;;
  begin) begin "$@" ;;
  done) done_round "$@" ;;
  reviewed) reviewed "$@" ;;
  decide) decide "$@" ;;
  decisions) decisions "$@" ;;
  remind) remind "$@" ;;
  *) usage ;;
esac
