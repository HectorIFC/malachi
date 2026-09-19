#!/usr/bin/env sh
# Paired A/B of the storage hot path (issues #147 and #164): the baseline tree against the branch tree,
# with an A-A control, interleaved by run. See benchmark/store_error_path_ab.exs for what is measured and
# the verdict rule. Needs Elixir on PATH and a real filesystem under AB_DIR (not tmpfs, not macOS: the
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

. "$BRANCH/benchmark/support/ab_lib.sh"

# The harness is new with #147, so the baseline tree gets the branch's copy, with the support files it
# loads. It only calls APIs both trees have (`ElixirStore.open/append/sync/close`), so the baseline
# measures its own store code.
mkdir -p "$BASELINE/benchmark/support"
cp "$BRANCH/benchmark/store_error_path_ab.exs" "$BASELINE/benchmark/store_error_path_ab.exs"
cp "$BRANCH/benchmark/support/paired_stats.exs" "$BASELINE/benchmark/support/paired_stats.exs"
cp "$BRANCH/benchmark/support/ab_run.exs" "$BASELINE/benchmark/support/ab_run.exs"
cp "$BRANCH/benchmark/support/e2e_sample.exs" "$BASELINE/benchmark/support/e2e_sample.exs"

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

rm -rf "$OUT"
mkdir -p "$OUT"
build "$BASELINE"
build "$BRANCH"
run_case store "$REPS"
run_case e2e "$E2E_REPS"

# The analyzer is told which cases this run asked for, so a case that was asked for and left no samples
# is a run with no verdict rather than one that passes on whatever else it measured. The store case is
# always asked for: it is the path this experiment exists to measure, and AB_REPS=0 judges nothing about it.
EXPECTED=store
[ "$E2E_REPS" -gt 0 ] && EXPECTED="$EXPECTED e2e"

say "analyzing"
cd "$BRANCH"
AB_MODE=analyze AB_RESULTS="$OUT" AB_OUT="$OUT/report.json" AB_EXPECTED="$EXPECTED" \
  mix run --no-start benchmark/store_error_path_ab.exs
