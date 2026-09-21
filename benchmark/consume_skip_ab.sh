#!/usr/bin/env sh
# Paired A/B of the consume path (issue #191): the baseline tree against the branch tree, with an A-A
# control, interleaved by run. See benchmark/consume_skip_ab.exs for what is measured and
# support/paired_stats.exs for the verdict rule. Needs Elixir on PATH; only a Linux run counts.
#
# Usage: benchmark/consume_skip_ab.sh BASELINE_TREE BRANCH_TREE [OUT_DIR]
#   AB_REPS  repetitions per arm and case (default 7, a verdict needs at least 5)
#   AB_DIR   scratch directory for the broker's segments (default $TMPDIR/consume_skip_ab)
set -eu

BASELINE=$(cd "$1" && pwd)
BRANCH=$(cd "$2" && pwd)
OUT=${3:-${TMPDIR:-/tmp}/consume_skip_ab_results}
REPS=${AB_REPS:-7}
export AB_DIR=${AB_DIR:-${TMPDIR:-/tmp}/consume_skip_ab}

. "$BRANCH/benchmark/support/ab_lib.sh"

# The harness is new with #191, so the baseline tree gets the branch's copy, with the support files it
# loads. It only calls APIs both trees have (`BrokerServer.start_link/2`, `split_range/2`,
# `LogApi.create_topic/2`, `produce/3`, `fetch/4`), so the baseline measures its own consume path.
mkdir -p "$BASELINE/benchmark/support"
for file in consume_skip_ab.exs support/ab_run.exs support/paired_stats.exs; do
  cp "$BRANCH/benchmark/$file" "$BASELINE/benchmark/$file"
done

# One sample of `case` for `arm`, captured whole: the analyzer finds the numbers in it.
run_one() {
  kase=$1 rep=$2 arm=$3
  (cd "$(tree_of "$arm")" && AB_MODE=sample AB_CASE="$kase" mix run --no-start benchmark/consume_skip_ab.exs) \
    >"$OUT/$kase/$rep-$arm.out" 2>&1
}

rm -rf "$OUT"
mkdir -p "$OUT"
build "$BASELINE"
build "$BRANCH"
run_case self "$REPS"
run_case ancestor "$REPS"

say "analyzing"
cd "$BRANCH"
AB_MODE=analyze AB_RESULTS="$OUT" AB_OUT="$OUT/report.json" mix run --no-start benchmark/consume_skip_ab.exs
