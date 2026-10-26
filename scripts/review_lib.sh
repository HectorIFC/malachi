# shellcheck shell=bash
# Shared by review-state.sh and pin-diff.sh. Sourced, never executed. The caller is in the checkout's top
# directory.
#
# review_build_tree <workdir>
#   Prints the id of the tree the working tree would commit: tracked files as they are now, untracked files
#   included, and the files prepare-commits generates at the root left out. The tree is built in a copy of
#   a private index that starts from HEAD and is thrown away, so the contributor's index does not move.
#   Its objects go into the repository's object store; git gc drops them unless a ref keeps them, which
#   review-state does.
#   One line per file git could not read goes to <workdir>/unreadable. A tree built without such a file is
#   not the working tree, so a caller must not record it as the one reviewed.
#   Prints nothing and returns 1 when git gave up (a required filter failed, a read-only repository).

# The untracked names prepare-commits writes at the root. Exact names, never a wildcard: a pattern would
# also hide a real file that happens to match, such as a commit_message.ex.
REVIEW_GENERATED_RE='^(commit_message\.sh|commit_[0-9]+\.patch|commit_message\.txt|commit_message_[0-9]+\.txt)$'

review_is_generated() {
  [[ "$1" =~ $REVIEW_GENERATED_RE ]]
}

# LC_ALL=C: the one message read back from git ("unable to index file") has to come out in English
# whatever the contributor's locale. core.safecrlf off: with it on, one file whose line endings a
# .gitattributes rule would convert makes git add fatal, and the tree would silently be the old one.
# core.splitIndex off: a split index would put its shared part in the repository.
review_git() {
  local index=$1
  shift
  LC_ALL=C GIT_INDEX_FILE="$index" git -c core.splitIndex=false -c core.safecrlf=false "$@"
}

review_build_tree() {
  local work=$1 index name added tree
  index="$work/index"
  # Starts from HEAD, not from a copy of the contributor's index: a copy carries its stat cache, and an
  # entry whose stat still matches is taken as unchanged, so an edit that keeps the size and lands within
  # the timestamp's granularity would leave the old content in the tree. With HEAD every entry is hashed.
  if git rev-parse --verify --quiet HEAD > /dev/null; then
    review_git "$index" read-tree HEAD 2> /dev/null || return 1
  else
    review_git "$index" read-tree --empty 2> /dev/null || return 1
  fi

  review_git "$index" add -A --ignore-errors -- . 2> "$work/errors"
  added=$?
  # 0 is a clean add and 1 one that skipped what it could not read. Anything else means git gave up: the
  # index is then still the old one, and a tree built from it would hide every change.
  [ "$added" -le 1 ] || return 1

  while IFS= read -r -d '' name; do
    review_is_generated "$name" && review_git "$index" rm -q --cached -- "$name" 2> /dev/null
  done < <(git ls-files -z --others --exclude-standard 2> /dev/null)

  grep 'unable to index file' "$work/errors" 2> /dev/null > "$work/unreadable" || true
  tree=$(review_git "$index" write-tree 2> /dev/null) || return 1
  printf '%s\n' "$tree"
}
