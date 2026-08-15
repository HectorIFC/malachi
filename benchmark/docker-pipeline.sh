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
# Usage: benchmark/docker-pipeline.sh   (override SHARDS/BATCHES/PIPELINES/CONNS/TOPICS/DUR/WARM via env)
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
export SRV_CPUSET="4,5,6,7" LT_CPUSET="0,1,2,3"

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

printf "%-7s %-6s %-6s | %10s %8s %8s %8s %8s %8s\n" shards batch pipe "rec/s" "p50 ms" "p99 ms" dropped overload reconn
printf -- "--------------------------------------------------------------------------------------\n"

for s in $SHARDS; do
  for b in $BATCHES; do
    for p in $PIPELINES; do
      if ! DATA_SHARDS="$s" $COMPOSE up -d --wait --force-recreate malachi >/tmp/dp_up.log 2>&1; then
        echo "  server did not come up healthy (shards=$s); see /tmp/dp_up.log"; cat /tmp/dp_up.log
        DATA_SHARDS="$s" $COMPOSE down >/dev/null 2>&1
        continue
      fi

      json=$(DATA_SHARDS="$s" $COMPOSE run --rm loadtest \
               --host malachi --scenario produce --connections "$CONNS" --batch "$b" --pipeline "$p" \
               --topics "$TOPICS" --duration "$DUR" --warmup "$WARM" --record-size 256 --json 2>/dev/null \
             | grep -E '^\{' | tail -1)

      DATA_SHARDS="$s" $COMPOSE down >/dev/null 2>&1

      if [ -z "$json" ]; then
        printf "%-7s %-6s %-6s | %s\n" "$s" "$b" "$p" "(no json)"; continue
      fi
      read -r recs p50 p99 err drop over recon < <(echo "$json" \
        | jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors,.dropped,.overloaded,.reconnects]|@tsv')
      [ "$err" != "0" ] && drop="$drop(err=$err)"
      printf "%-7s %-6s %-6s | %10s %8s %8s %8s %8s %8s\n" "$s" "$b" "$p" "$recs" "$p50" "$p99" "$drop" "$over" "$recon"
    done
  done
done

echo
echo "done (batch=1 pipe=1 is closed-loop; batch=100 pipe>1 is the competitor regime; compare shards 1 vs 4)"
