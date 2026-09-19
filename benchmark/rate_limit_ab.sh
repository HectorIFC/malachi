#!/usr/bin/env sh
# Paired A/B of the publish quota check (issue #151): the baseline tree against the branch tree, with an
# A-A control, interleaved by run. See benchmark/rate_limit_ab.exs for what is measured and
# support/paired_stats.exs for the verdict rule. Needs Elixir on PATH; only a Linux run counts.
#
# Usage: benchmark/rate_limit_ab.sh BASELINE_TREE BRANCH_TREE [OUT_DIR]
#   AB_REPS  repetitions per arm (default 7, a verdict needs at least 5)
set -eu

BASELINE=$(cd "$1" && pwd)
BRANCH=$(cd "$2" && pwd)
OUT=${3:-${TMPDIR:-/tmp}/rate_limit_ab_results}
REPS=${AB_REPS:-7}

. "$BRANCH/benchmark/support/ab_lib.sh"

# The harness is new with #151, so the baseline tree gets the branch's copy, with the support files it
# loads. It only calls APIs both trees have (`RateLimiter.start_link/1`, `check_limit_in_caller/3`), so
# the baseline measures its own limiter.
mkdir -p "$BASELINE/benchmark/support"
for file in rate_limit_ab.exs support/ab_run.exs support/paired_stats.exs; do
  cp "$BRANCH/benchmark/$file" "$BASELINE/benchmark/$file"
done

# One sample for `arm`, captured whole: the analyzer finds the numbers in it.
run_one() {
  kase=$1 rep=$2 arm=$3
  (cd "$(tree_of "$arm")" && AB_MODE=sample mix run --no-start benchmark/rate_limit_ab.exs) \
    >"$OUT/$kase/$rep-$arm.out" 2>&1
}

rm -rf "$OUT"
mkdir -p "$OUT"
build "$BASELINE"
build "$BRANCH"
run_case check "$REPS"

say "analyzing"
cd "$BRANCH"
AB_MODE=analyze AB_RESULTS="$OUT" AB_OUT="$OUT/report.json" mix run --no-start benchmark/rate_limit_ab.exs
