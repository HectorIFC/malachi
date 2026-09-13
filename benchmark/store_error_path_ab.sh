#!/usr/bin/env sh
# Paired A/B of the storage error path (issue #147): the baseline tree against the branch tree, with an
# A-A control, interleaved by run. See benchmark/store_error_path_ab.exs for what is measured and the
# verdict rule. Needs Elixir on PATH and a real filesystem under AB_DIR (not tmpfs, not macOS: the
# decision is about Linux fsync).
#
# Usage: benchmark/store_error_path_ab.sh BASELINE_TREE BRANCH_TREE [OUT_DIR]
#   AB_REPS      store-case repetitions per arm (default 15)
#   AB_E2E_REPS  throughput_1m repetitions per arm (default 5, 0 skips the case)
#   AB_DIR       scratch directory for the store case (default $TMPDIR/store_error_path_ab)
set -eu

BASELINE=$(cd "$1" && pwd)
BRANCH=$(cd "$2" && pwd)
OUT=${3:-${TMPDIR:-/tmp}/store_error_path_ab_results}
REPS=${AB_REPS:-15}
E2E_REPS=${AB_E2E_REPS:-5}
export AB_DIR=${AB_DIR:-${TMPDIR:-/tmp}/store_error_path_ab}

say() { printf '[ab] %s\n' "$*"; }

# The harness is new with #147, so the baseline tree gets the branch's copy. It only calls APIs both
# trees have (`ElixirStore.open/append/sync/close`), so the baseline measures its own store code.
mkdir -p "$BASELINE/benchmark/support"
cp "$BRANCH/benchmark/store_error_path_ab.exs" "$BASELINE/benchmark/store_error_path_ab.exs"
cp "$BRANCH/benchmark/support/paired_stats.exs" "$BASELINE/benchmark/support/paired_stats.exs"

build() {
  say "building $1"
  (
    cd "$1"
    for attempt in 1 2 3; do
      mix deps.get >/dev/null && break
      say "deps.get failed (attempt $attempt/3)"
      [ "$attempt" -lt 3 ] && sleep 5
    done
    mix compile >/dev/null
  )
}

tree_of() {
  if [ "$1" = branch ]; then echo "$BRANCH"; else echo "$BASELINE"; fi
}

shuffled_arms() { printf 'main_a1\nmain_a2\nbranch\n' | shuf; }

# One sample of `case` for `arm`, captured whole: the analyzer finds the number in it.
run_one() {
  kase=$1 rep=$2 arm=$3
  file="$OUT/$kase/$rep-$arm.out"
  if [ "$kase" = store ]; then
    (cd "$(tree_of "$arm")" && AB_MODE=sample mix run --no-start benchmark/store_error_path_ab.exs) >"$file" 2>&1
  else
    (cd "$(tree_of "$arm")" && mix run benchmark/throughput_1m.exs) >"$file" 2>&1
  fi
}

run_case() {
  kase=$1 reps=$2
  [ "$reps" -gt 0 ] || return 0
  mkdir -p "$OUT/$kase"
  # One discarded pass per arm: the first run on a fresh directory pays for cold page cache and
  # first-touch allocation, which is a property of the harness rather than of either tree.
  for arm in $(shuffled_arms); do run_one "$kase" warm "$arm"; done
  rep=1
  while [ "$rep" -le "$reps" ]; do
    order=$(shuffled_arms | tr '\n' ' ')
    say "$kase rep $rep/$reps: $order"
    for arm in $order; do run_one "$kase" "$rep" "$arm"; done
    rep=$((rep + 1))
  done
}

rm -rf "$OUT"
mkdir -p "$OUT"
build "$BASELINE"
build "$BRANCH"
run_case store "$REPS"
run_case e2e "$E2E_REPS"

say "analyzing"
cd "$BRANCH"
AB_MODE=analyze AB_RESULTS="$OUT" AB_OUT="$OUT/report.json" mix run --no-start benchmark/store_error_path_ab.exs
