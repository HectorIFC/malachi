#!/usr/bin/env bash
# Paired A/Bs of the load generators themselves (issue #192), interleaved by run with an A-A control. See
# benchmark/loadtest_ab.exs for what is compared and support/paired_stats.exs for the verdict rule.
#
#   node-runtime  Node 20 (A-A) against Node 22, constant bytes, the regime the Node series is published
#                 in. Run before CI moved the published series from Node 20 to 22.
#   payload       constant bytes (A-A) against json, for each generator and batch size: what generating
#                 realistic values costs the generator. No difference is a valid result.
#
# Every run gets a fresh server (docker-compose.bench.yml, its data on a tmpfs that a long window would
# fill) pinned to SRV_CPUSET, and a generator pinned to LT_CPUSET: the Node one in a stock Node image with
# this tree mounted, the Elixir one in the bench compose's loadtest image. Only a Linux run counts; on a
# Mac that is Docker's Linux VM, which also means the numbers are relative (its architecture need not be
# the CI runner's).
#
# Usage: benchmark/loadtest_ab.sh [node-runtime|payload|all] [OUT_DIR]
#   AB_REPS       repetitions per arm (default 7; a verdict needs at least 5)
#   SRV_CPUSET    server cores (default 1,2,3)    LT_CPUSET  generator core (default 0)
#   DUR WARM      measured and warmup seconds per run (default 5 and 1, which the tmpfs holds)
#   CONNS         connections (default 32, near the published batch 10 peak)
#   NODE20_IMAGE NODE22_IMAGE  the generator images, pinned by digest
#   AB_CASES      only these cases (for example payload-elixir-b100), to rerun one in another window
#
# The server's data sits on a 1g tmpfs, and the Elixir generator at batch 100 and above writes past it in
# a 5s window (the produces then fail with {:storage, :enospc}); run those cases with DUR=2. The analysis
# refuses to judge a case with any errored sample, so a window that is too long shows up as NOT JUDGED.
set -euo pipefail

WHICH=${1:-all}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${2:-${TMPDIR:-/tmp}/loadtest_ab_results}
# Without its trailing slashes, which tab completion adds: the build log is written beside OUT as
# "$OUT.build.log", and with a slash left on that path lands inside OUT, which is deleted further down.
while [ "$OUT" != / ] && [ "${OUT%/}" != "$OUT" ]; do OUT=${OUT%/}; done
# OUT is removed with rm -rf before every run, so the root is refused outright.
if [ "$OUT" = / ]; then
  echo "refusing OUT_DIR=/: the results directory is deleted before every run" >&2
  exit 2
fi
REPS=${AB_REPS:-7}
DUR=${DUR:-5}
WARM=${WARM:-1}
CONNS=${CONNS:-32}
export SRV_CPUSET=${SRV_CPUSET:-1,2,3}
export LT_CPUSET=${LT_CPUSET:-0}
NODE20_IMAGE=${NODE20_IMAGE:-node:20-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0}
NODE22_IMAGE=${NODE22_IMAGE:-node:22-bookworm-slim@sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c}

case "$WHICH" in
  node-runtime|payload|all) ;;
  *) echo "usage: $0 [node-runtime|payload|all] [OUT_DIR]" >&2; exit 2 ;;
esac

cd "$ROOT"
. "$ROOT/benchmark/support/ab_lib.sh"
COMPOSE="docker compose -f docker-compose.bench.yml"

# The bench server's container name is fixed, so a second harness on this machine would share it. Refuse
# rather than recreate a container this script did not start.
if docker ps -a --format '{{.Names}}' | grep -qx malachi-bench; then
  echo "a container named malachi-bench already exists; it is not this run's, so stopping here" >&2
  exit 1
fi
trap '$COMPOSE down -v > /dev/null 2>&1 || true' EXIT

say "building the bench images"
# The build log sits beside OUT, which is only created further down: an OUT_DIR whose parent does not
# exist yet would otherwise fail this redirection before anything is built.
mkdir -p "$(dirname "$OUT")"
$COMPOSE build > "$OUT.build.log" 2>&1 || { tail -20 "$OUT.build.log" >&2; exit 1; }

fresh_server() {
  $COMPOSE up -d --wait --force-recreate malachi > "$OUT/up.log" 2>&1 || { cat "$OUT/up.log" >&2; return 1; }
}

network() {
  docker inspect -f '{{range $name, $net := .NetworkSettings.Networks}}{{$name}}{{end}}' malachi-bench
}

# A Node generator run in `image` against the fresh server, its JSON report on stdout.
node_run() {
  local image=$1; shift
  docker run --rm --cpuset-cpus "$LT_CPUSET" --network "$(network)" -v "$ROOT":/w -w /w \
    -e MALACHI_HOST=malachi -e MALACHI_PORT=4040 -e MALACHI_USER=admin -e MALACHI_PASS=admin123 \
    "$image" node scripts/loadtest.js --scenario produce --json --connections "$CONNS" \
    --duration "$DUR" --warmup "$WARM" --record-size 256 "$@"
}

# The Elixir generator, one scheduler for its one core.
elixir_run() {
  $COMPOSE run --rm --no-deps -e ERL_FLAGS="+S 1:1" loadtest --host malachi --scenario produce --json \
    --connections "$CONNS" --duration "$DUR" --warmup "$WARM" --record-size 256 "$@"
}

# Whether CASE is to run: every case, or only those named in AB_CASES.
wanted() { [ -z "${AB_CASES:-}" ] || [[ " $AB_CASES " == *" $1 "* ]]; }

# One sample: CASE is <experiment>-<generator>-b<batch>, ARM one of ab_lib's three.
run_one() {
  local kase=$1 rep=$2 arm=$3
  local experiment generator batch
  IFS=- read -r experiment generator batch <<< "$kase"
  batch=${batch#b}
  fresh_server

  case "$experiment:$arm" in
    node:branch) node_run "$NODE22_IMAGE" --batch "$batch" ;;
    node:*) node_run "$NODE20_IMAGE" --batch "$batch" ;;
    payload:branch) "${generator}_gen" --batch "$batch" --payload json ;;
    payload:*) "${generator}_gen" --batch "$batch" ;;
  esac > "$OUT/$kase/$rep-$arm.out" 2> "$OUT/$kase/$rep-$arm.err"
}

node_gen() { node_run "$NODE22_IMAGE" "$@"; }
elixir_gen() { elixir_run "$@"; }

rm -rf "$OUT"
mkdir -p "$OUT"
say "server cores $SRV_CPUSET, generator core $LT_CPUSET, $CONNS connections, ${WARM}s + ${DUR}s per run, $REPS reps"

if [ "$WHICH" != payload ]; then
  for batch in 10 100; do wanted "node-node-b$batch" && run_case "node-node-b$batch" "$REPS"; done
fi

if [ "$WHICH" != node-runtime ]; then
  for generator in elixir node; do
    for batch in 10 100 1000; do wanted "payload-$generator-b$batch" && run_case "payload-$generator-b$batch" "$REPS"; done
  done
fi

say "analyzing"
AB_MODE=analyze AB_RESULTS="$OUT" AB_OUT="$OUT/report.json" mix run --no-start benchmark/loadtest_ab.exs
