#!/usr/bin/env bash
# What the frontend hop costs on a network with real latency (issue #275, decision 13A). A legacy produce
# sent to any node is planned there and dispatched to the segment's primary on another node. A NorthGuard
# client talks to the brokers directly (transcript 842), picks the shard itself (885-886) and produces in a
# session with a broker (543-560); that the broker it talks to is the segment's primary is Malachi's
# reading of those lines, a deduction, since the transcript does not name it. On a
# single machine that hop costs almost nothing, which is why the August cluster measurement saw blind
# round robin match the aligned path. This harness adds latency between the containers and measures both
# arms on the same cluster.
#
# - random:  connection i goes to node i mod 3, so the connections are spread over the three nodes (the
#            any-node frontend: two produces in three take the extra hop);
# - aligned: every connection goes to the node that is the primary of the topic's only segment.
#
# One topic with one range, so "the primary" is one node and the aligned arm can be expressed with the
# legacy client as it is today (`--host <primary>`). The primary is read from a node's own metadata cache
# over Erlang distribution after the topic's segment exists. A segment that rolls gets a new id and a new
# placement, so the primary can move to another node mid series. The roll is decided by the node that
# planned each produce, on the bytes IT sent to the segment, so the aligned arm puts every byte on the
# primary's count and the random arm about a third: at delay 0 an arm moves about 90 MB/s. The nodes run
# with a 512 MiB segment (MALACHI_SEGMENT_MAX_BYTES) and short arms (WARM 1 + DUR 2), which keeps a pair
# well under it, and the active segment and its primary are read again after every arm. A pair whose
# segment moved anyway is not recorded at all, so the two arms stay paired and their order stays
# alternated: it runs again on a new cluster, up to ATTEMPTS clusters, and fails the series after that.
#
# Latency comes from `tc netem delay` on eth0 of every node, applied from a helper container that joins
# the node's network namespace with NET_ADMIN, so the Malachi image is unchanged. netem delays egress
# only: a client round trip to a node gains DELAY once, a node to node round trip twice (each side's
# egress), which is the asymmetry between the arms this measures.
#
# A fresh cluster for every pair of runs (data on each node's 1g tmpfs, which a longer series would
# fill), the two arms inside a pair in alternating order across pairs (ab, ba, ab, ...), so neither arm
# always runs first on a fresh cluster.
#
# The servers run in Linux containers, and that is where the numbers come from; the script refuses a
# Docker daemon that is not Linux. On a macOS host that daemon runs in a VM: the numbers are then
# relative (one arm against the other, on the same VM), not absolute figures for a Linux host.
#
# Knobs (env): RFS (default "1 3"), DELAYS in ms (default "0 1 5"), PAIRS per cell (default 3), CONNS
# (16), BATCH (100), DUR (2), WARM (1), ATTEMPTS (3), PAYLOAD (json), SRV_CPUSET (1,2,3), LT_CPUSET (0),
# MALACHI_SEGMENT_MAX_BYTES (536870912), OUT (JSON lines, appended; one object per run, with the commit of
# the tree that ran it).
#
# Usage: OUT=benchmark/results/275/m0-hop.jsonl benchmark/netem-hop.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.cluster.yml"

usage_error() { echo "$*" >&2; exit 2; }
require_positive() { case "$2" in '' | *[!0-9]* | 0*) usage_error "$1 must be a positive integer, got '$2'" ;; esac; }

RFS="${RFS:-1 3}"
DELAYS="${DELAYS:-0 1 5}"
PAIRS="${PAIRS:-3}"
CONNS="${CONNS:-16}"
BATCH="${BATCH:-100}"
DUR="${DUR:-2}"
WARM="${WARM:-1}"
# Fresh clusters a pair may take: a pair whose segment moved runs again from scratch (see the header).
ATTEMPTS="${ATTEMPTS:-3}"
PAYLOAD="${PAYLOAD:-json}"
OUT="${OUT:-}"
TOPIC=hop
TC_IMAGE=malachi-netem-tc:local
export SRV_CPUSET="${SRV_CPUSET:-1,2,3}" LT_CPUSET="${LT_CPUSET:-0}"
source scripts/bench_lib.sh
export SRV_ERL_FLAGS="${SRV_ERL_FLAGS:-$(schedulers_for "$SRV_CPUSET")}"
export LT_ERL_FLAGS="${LT_ERL_FLAGS:-$(schedulers_for "$LT_CPUSET")}"
# Far above what one pair writes (see the header), and within the nodes' 1g tmpfs.
export MALACHI_SEGMENT_MAX_BYTES="${MALACHI_SEGMENT_MAX_BYTES:-536870912}"
TREE="$(tree_label)"

for knob in PAIRS CONNS BATCH DUR WARM ATTEMPTS; do require_positive "$knob" "${!knob}"; done
for rf in $RFS; do case "$rf" in 1 | 2 | 3) ;; *) usage_error "RFS entries must be 1, 2 or 3, got '$rf'" ;; esac; done
for d in $DELAYS; do case "$d" in '' | *[!0-9]*) usage_error "DELAYS entries must be whole milliseconds, got '$d'" ;; esac; done
for tool in docker jq; do command -v "$tool" > /dev/null 2>&1 || usage_error "$tool is required"; done
[ "$(docker info --format '{{.OSType}}' 2> /dev/null)" = linux ] ||
  usage_error "the Docker daemon is not Linux (or is not running); the numbers only count on Linux"
if [ -n "$OUT" ]; then mkdir -p "$(dirname "$OUT")" || usage_error "cannot create the directory of OUT=$OUT"; fi
if docker ps --format '{{.Names}}' | grep -qE '^malachi-cluster-'; then
  usage_error "another malachi-cluster-* is running; the drills and cluster benchmarks share those names"
fi

FAILED=0
teardown() { RF="$1" $COMPOSE down -v > /dev/null 2>&1; }
trap 'for rf in $RFS; do teardown "$rf"; done' EXIT

wait_healthy() {
  for _ in $(seq 1 48); do
    healthy=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$healthy" = "3" ] && return 0
    sleep 5
  done
  return 1
}

apply_delay() {
  local d="$1" i
  [ "$d" = 0 ] && return 0
  for i in 1 2 3; do
    docker run --rm --net "container:malachi-cluster-$i" --cap-add NET_ADMIN "$TC_IMAGE" \
      tc qdisc add dev eth0 root netem delay "${d}ms" || return 1
  done
}

# "<segment id> <node>" of the topic's active segment and its primary, read from node 1's metadata cache.
active_segment() {
  docker exec malachi-cluster-1 elixir --sname "hopprobe$$" --cookie malachi_bench -e '
    n = :"malachi@malachi1"
    true = Node.connect(n)
    t = System.argv() |> hd()
    d = :rpc.call(n, :sys, :get_state, [Malachi.LogBroker]).broker.dsrsm
    [range] = :rpc.call(n, Malachi.Cluster.DSRSM, :ranges_of_topic, [d, t])
    [seg] = :rpc.call(n, Malachi.Cluster.DSRSM, :segments_of_range, [d, t, range.id]) |> Enum.filter(&(&1.state == :active))
    {_name, node} = hd(seg.replica_set)
    IO.puts("#{inspect(seg.id)} #{node}")
  ' "$TOPIC" 2> /dev/null | tail -1
}

loadtest() {
  RF="$1" $COMPOSE run --rm --no-deps loadtest --host "$2" --topic "$TOPIC" --topics 1 --scenario produce \
    --connections "$3" --batch "$BATCH" --duration "$4" --warmup "$5" --payload "$PAYLOAD" --json 2> /dev/null |
    grep -E '^\{' | tail -1
}

echo "building the tc helper image and the cluster images..."
printf 'FROM alpine:3.21\nRUN apk add --no-cache iproute2-tc\n' | docker build -q -t "$TC_IMAGE" - > /dev/null ||
  { echo "could not build the tc helper image"; exit 1; }
$COMPOSE build > /dev/null || { echo "build failed"; exit 1; }

printf "%-3s %-6s %-5s %-8s %-10s | %10s %8s %8s %6s\n" rf delay pair arm primary "rec/s" "p50 ms" "p99 ms" errors
printf -- "--------------------------------------------------------------------------\n"

# One pair on a fresh cluster: both arms, or nothing. Prints the pair's rows and appends them to OUT only
# when both arms ran on the segment the primary was read from. Returns 0 when recorded, 2 when the
# segment moved (the caller runs the pair again on a new cluster), 1 on any other failure.
run_pair() {
  local rf="$1" d="$2" pair="$3" before after host arms arm hosts json rows=() lines=() recs p50 p99 errs errored=0
  teardown "$rf"
  if ! RF="$rf" $COMPOSE up -d malachi1 malachi2 malachi3 > /dev/null 2>&1 || ! wait_healthy; then
    echo "rf=$rf delay=$d pair=$pair: the cluster did not come up healthy"; return 1
  fi
  # The topic and its segment are created before any delay, so setup costs are the same in both arms.
  if [ -z "$(RF="$rf" $COMPOSE run --rm --no-deps loadtest --host malachi1 --topic "$TOPIC" --topics 1 \
              --scenario produce --connections 1 --batch "$BATCH" --prepopulate "$BATCH" --duration 1 \
              --warmup 0 --json 2> /dev/null | grep -E '^\{')" ]; then
    echo "rf=$rf delay=$d pair=$pair: setup produce failed"; return 1
  fi
  sleep 2
  before="$(active_segment)"
  case "${before##* }" in
    malachi@malachi[123]) host="${before##*@}" ;;
    *) echo "rf=$rf delay=$d pair=$pair: could not read the primary (got '$before')"; return 1 ;;
  esac
  apply_delay "$d" || { echo "rf=$rf delay=$d pair=$pair: tc netem failed"; return 1; }
  if [ $((pair % 2)) = 1 ]; then arms="random aligned"; else arms="aligned random"; fi
  for arm in $arms; do
    if [ "$arm" = random ]; then hosts=malachi1,malachi2,malachi3; else hosts="$host"; fi
    json="$(loadtest "$rf" "$hosts" "$CONNS" "$DUR" "$WARM")"
    after="$(active_segment)"
    if [ "$after" != "$before" ]; then
      # The arms that did complete are shown, marked, so an error in a discarded attempt is not lost.
      [ "${#lines[@]}" = 0 ] || printf '%s (discarded)\n' "${lines[@]}"
      echo "rf=$rf delay=$d pair=$pair: the segment moved during the $arm arm ('$before' -> '$after')"
      return 2
    fi
    if [ -z "$json" ]; then
      echo "rf=$rf delay=$d pair=$pair: the $arm arm gave no result"; return 1
    fi
    read -r recs p50 p99 errs < <(jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,.errors]|@tsv' <<< "$json")
    lines+=("$(printf "%-3s %-6s %-5s %-8s %-10s | %10s %8s %8s %6s" "$rf" "${d}ms" "$pair" "$arm" "$host" "$recs" "$p50" "$p99" "$errs")")
    [ "$errs" = 0 ] || errored=1
    rows+=("$(jq -c --argjson rf "$rf" --argjson delay_ms "$d" --argjson pair "$pair" --arg arm "$arm" --arg primary "$host" \
      --argjson conns "$CONNS" --argjson batch "$BATCH" --arg payload "$PAYLOAD" --arg srv "$SRV_CPUSET" --arg lt "$LT_CPUSET" \
      --arg segment "${before% *}" --arg tree "$TREE" --argjson warm "$WARM" --argjson dur "$DUR" \
      '{rf: $rf, delay_ms: $delay_ms, pair: $pair, arm: $arm, primary: $primary, segment: $segment, conns: $conns,
        batch: $batch, payload: $payload, warmup_s: $warm, duration_s: $dur, srv_cpuset: $srv, lt_cpuset: $lt,
        tree: $tree, loadtest: .}' <<< "$json")")
  done
  printf '%s\n' "${lines[@]}"
  if [ -n "$OUT" ]; then printf '%s\n' "${rows[@]}" >> "$OUT"; fi
  # A recorded pair with produce errors keeps its rows, with their counts, and fails the series. Only a
  # recorded pair does: a discarded attempt's errors are shown above and do not count against the series.
  [ "$errored" = 0 ] || FAILED=1
  return 0
}

for rf in $RFS; do
  for d in $DELAYS; do
    for pair in $(seq 1 "$PAIRS"); do
      attempt=1
      while :; do
        run_pair "$rf" "$d" "$pair"
        status=$?
        [ "$status" = 2 ] && [ "$attempt" -lt "$ATTEMPTS" ] && { attempt=$((attempt + 1)); continue; }
        [ "$status" = 0 ] || FAILED=1
        break
      done
    done
    teardown "$rf"
  done
done

echo
if [ "$FAILED" != 0 ]; then echo "done, WITH RUNS THAT DID NOT COMPLETE (see above)"; exit 1; fi
echo "done"
