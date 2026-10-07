#!/usr/bin/env bash
# Batch x pipeline x shards sweep: measure Malachi produce in the SAME regime the competitors run by
# default. Every high-throughput client batches and pipelines (Kafka linger.ms=5 + max.in.flight,
# Pulsar batchingEnabled=true, Iggy batching); none run closed-loop (one in-flight request per
# connection). Our accept path is already parallel (one acceptor per scheduler + SO_REUSEPORT), so the
# lever here is purely client-side: how far do batching and pipelining lift the networked ceiling, and
# does data-plane sharding still matter once we pipeline?
#
# For each cell (shards x batch x pipeline) it brings up a FRESH 4-core server (cpuset 4-7, group commit
# on) so one cell's tmpfs data cannot corrupt the next, runs produce from the Elixir client (cpuset 0-3),
# and prints rec/s, p50, p99, and err/drop/over/recon. Latency matters here: pipelining trades latency for
# throughput, so p50/p99 rising while rec/s rises is the expected signal.
#
# Connections are kept at 64 so in-flight records (~conns*pipeline*batch) stay under the 200k overload
# valve at the swept points, keeping the throughput signal clean; a cell that shows overloaded > 0 means
# the valve capped it (visible, not hidden). The client fans out over 64 topics so sharding actually
# spreads load (a topic is pinned to one shard). The window is short (warmup 1 + duration 2) because high
# batch x pipeline produces a lot of data into the same 1g tmpfs.
#
# REPS repeats the whole grid (default 1): each repetition walks every cell once before the next one
# starts, so drift on the host over the series lands in the spread between the repetitions of each cell
# instead of in the difference between cells, and that spread is the noise floor a difference between
# cells has to clear. SCENARIO picks the
# generator scenario (default produce; --pipeline only shapes produce), PAYLOAD the record values (default
# constant; json is #192's compressible payload). OUT, when set, gets one JSON object
# per run APPENDED (JSON lines), with the cell, the repetition, the generator's own JSON and the commit of
# the tree that ran it.
#
# The server runs in a Linux container whatever the host is, and that is where the numbers come from; the
# script refuses a Docker daemon that is not Linux. On a macOS host that daemon runs in a VM: the numbers
# are then relative (one cell against another on the same VM), not absolute figures for a Linux host.
# SRV_CPUSET and LT_CPUSET default to the 8 CPU split
# (4-7 and 0-3); on a 4 CPU VM pass SRV_CPUSET=1,2,3 LT_CPUSET=0, and the BEAM schedulers follow the
# cpusets (+S n:n, n the number of pinned cores, ranges counted by scripts/bench_lib.sh) unless SRV_ERL_FLAGS or LT_ERL_FLAGS say otherwise.
#
# Usage: benchmark/docker-pipeline.sh   (override SHARDS/BATCHES/PIPELINES/CONNS/TOPICS/DUR/WARM/REPS/
#        SCENARIO/PAYLOAD/OUT/SRV_CPUSET/LT_CPUSET via env)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f docker-compose.bench.yml"
DUR="${DUR:-2}"
WARM="${WARM:-1}"
CONNS="${CONNS:-64}"
TOPICS="${TOPICS:-64}"
SHARDS="${SHARDS:-1 4}"
BATCHES="${BATCHES:-1 100}"
PIPELINES="${PIPELINES:-1 4 16}"
REPS="${REPS:-1}"
SCENARIO="${SCENARIO:-produce}"
# What the record values are: constant (default), json or random (lib/malachi/loadtest/payload.ex).
PAYLOAD="${PAYLOAD:-constant}"
OUT="${OUT:-}"
export SRV_CPUSET="${SRV_CPUSET:-4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1,2,3}"
# One scheduler per pinned core: more schedulers than cores under a cpuset only adds contention.
source scripts/bench_lib.sh
export SRV_ERL_FLAGS="${SRV_ERL_FLAGS:-$(schedulers_for "$SRV_CPUSET")}"
export LT_ERL_FLAGS="${LT_ERL_FLAGS:-$(schedulers_for "$LT_CPUSET")}"
TREE="$(tree_label)"
case "$REPS" in '' | *[!0-9]* | 0*) echo "REPS must be a positive integer, got '$REPS'" >&2; exit 2 ;; esac
[ "$(docker info --format '{{.OSType}}' 2>/dev/null)" = linux ] ||
  { echo "the Docker daemon is not Linux (or is not running); the numbers only count on Linux" >&2; exit 2; }
if [ -n "$OUT" ]; then mkdir -p "$(dirname "$OUT")" || exit 2; fi
# Any cell that could not be measured fails the whole sweep. A grid where some cells silently did not run
# is not a slower sweep, it is a sweep with holes in it, and the numbers next to those holes get compared
# against each other as if the grid were complete.
FAILED=0
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

printf "%-7s %-6s %-6s %-4s | %10s %8s %8s %8s %8s %8s\n" shards batch pipe rep "rec/s" "p50 ms" "p99 ms" dropped overload reconn
printf -- "--------------------------------------------------------------------------------------\n"

for rep in $(seq 1 "$REPS"); do
 for s in $SHARDS; do
  for b in $BATCHES; do
    for p in $PIPELINES; do
      if ! DATA_SHARDS="$s" $COMPOSE up -d --wait --force-recreate malachi >"$WORK/up.log" 2>&1; then
        echo "  server did not come up healthy (shards=$s); see below"; cat "$WORK/up.log"
        DATA_SHARDS="$s" $COMPOSE down >/dev/null 2>&1
        FAILED=1
        continue
      fi

      json=$(DATA_SHARDS="$s" $COMPOSE run --rm loadtest \
               --host malachi --scenario "$SCENARIO" --connections "$CONNS" --batch "$b" --pipeline "$p" \
               --topics "$TOPICS" --duration "$DUR" --warmup "$WARM" --record-size 256 --payload "$PAYLOAD" --json 2>/dev/null \
             | grep -E '^\{' | tail -1)

      DATA_SHARDS="$s" $COMPOSE down >/dev/null 2>&1

      if [ -z "$json" ]; then
        printf "%-7s %-6s %-6s %-4s | %s\n" "$s" "$b" "$p" "$rep" "(no json)"; FAILED=1; continue
      fi
      if [ -n "$OUT" ]; then
        jq -c --arg scenario "$SCENARIO" --arg payload "$PAYLOAD" --argjson shards "$s" --argjson batch "$b" --argjson pipeline "$p" \
          --argjson rep "$rep" --argjson conns "$CONNS" --arg srv "$SRV_CPUSET" --arg lt "$LT_CPUSET" --arg tree "$TREE" \
          '{scenario: $scenario, payload: $payload, shards: $shards, batch: $batch, pipeline: $pipeline, rep: $rep, conns: $conns,
            srv_cpuset: $srv, lt_cpuset: $lt, tree: $tree, loadtest: .}' <<< "$json" >> "$OUT"
      fi
      read -r recs p50 p99 err drop over recon < <(echo "$json" \
        | jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors,.dropped,.overloaded,.reconnects]|@tsv')
      [ "$err" != "0" ] && drop="$drop(err=$err)"
      printf "%-7s %-6s %-6s %-4s | %10s %8s %8s %8s %8s %8s\n" "$s" "$b" "$p" "$rep" "$recs" "$p50" "$p99" "$drop" "$over" "$recon"
     done
    done
  done
done

echo
if [ "$FAILED" != "0" ]; then
  echo "done, WITH CELLS THAT DID NOT RUN (see above); the grid is incomplete"
  exit 1
fi
echo "done (batch=1 pipe=1 is closed-loop; batch=100 pipe>1 is the competitor regime; compare shards 1 vs 4)"
