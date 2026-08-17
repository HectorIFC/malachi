#!/usr/bin/env bash
# Data-plane sharding sweep: measure how far N independent broker shards lift the NETWORKED throughput
# ceiling. The single serial BrokerServer mailbox is the ceiling; single_node_scale.exs already proved N
# shards scale in-VM, this asks whether routing produce by hash(topic) across N shards behind the TCP layer
# lifts the networked number too, before committing to the production sharding refactor.
#
# For each DATA_SHARDS in 1/2/4 it brings up a FRESH 4-core server (cpuset 4-7, group commit on) so one
# run's tmpfs data cannot corrupt the next, runs produce batch 100 conns 256 from the Elixir client
# (cpuset 0-3), and prints rec/s plus err/drop/over/recon. On a fresh server err/drop should be 0; a
# nonzero value means the 1g tmpfs filled (lower DUR).
#
# The client fans out over many topics (--topics), because a topic is pinned to one shard: a single-topic
# run would send all load to one shard and the other shards would sit idle, hiding any scaling.
#
# The window is short (warmup 1 + duration 2) because higher N produces more data into the same 1g tmpfs.
#
# Usage: benchmark/docker-shards.sh      (override with DUR=.. WARM=.. SHARDS="1 2 4")
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.bench.yml"
# Private scratch dir (not a predictable /tmp path a local attacker could pre-create as a symlink).
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
DUR="${DUR:-2}"
WARM="${WARM:-1}"
SHARDS="${SHARDS:-1 2 4}"
TOPICS="${TOPICS:-64}"
export SRV_CPUSET="4,5,6,7" LT_CPUSET="0,1,2,3"
FAILED=0

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

printf "%-8s | %10s %8s %8s %10s %10s\n" shards "rec/s" errors dropped overload reconn
printf -- "--------------------------------------------------------------------\n"

for n in $SHARDS; do
  # Fresh server per shard count: recreate the container so its tmpfs (RAM data disk) starts empty.
  if ! DATA_SHARDS="$n" $COMPOSE up -d --wait --force-recreate malachi >"$WORK/up.log" 2>&1; then
    echo "  server did not come up healthy (DATA_SHARDS=$n); log follows:"; cat "$WORK/up.log"
    DATA_SHARDS="$n" $COMPOSE down >/dev/null 2>&1
    FAILED=1
    continue
  fi

  json=$(DATA_SHARDS="$n" $COMPOSE run --rm loadtest \
           --host malachi --scenario produce --connections 256 --batch 100 --topics "$TOPICS" \
           --duration "$DUR" --warmup "$WARM" --record-size 256 --json 2>/dev/null \
         | grep -E '^\{' | tail -1)

  DATA_SHARDS="$n" $COMPOSE down >/dev/null 2>&1

  if [ -z "$json" ]; then
    # A caseless client must fail the run, not blend in as a blank row.
    printf "%-8s | %s\n" "$n" "(no json)"; FAILED=1; continue
  fi
  read -r recs errs drop over recon < <(echo "$json" | jq -r '[.records_per_s,.errors,.dropped,.overloaded,.reconnects]|@tsv')
  printf "%-8s | %10s %8s %8s %10s %10s\n" "$n" "$recs" "$errs" "$drop" "$over" "$recon"
done

echo
if [ "$FAILED" != "0" ]; then
  echo "done, WITH FAILED CASES (see above)"
  exit 1
fi
echo "done (rec/s should rise toward N x the 1-shard baseline until it plateaus at N=cores)"
