#!/usr/bin/env bash
# Does a streaming subscriber reading cold history slow down producers? (issue #275, Part 3, decision 16A)
#
# `Malachi.BrokerServer` serializes every append and, today, also performs the read for every streaming
# push inside the same loop (`push_subscriber/3` -> `Broker.read_consume/5` -> `ReplicationServer.read/4`),
# so a cold read queues every producer behind it. This harness measures produce latency on one topic with
# and without streaming subscribers reading old history from disk on another:
#
# - none:       only the producer;
# - subscriber: K streaming consumers (each its own group) reading topic `hist` from where their group
#               left off, with the widest window the server grants (10000) and the largest page (1000).
#
# Before EVERY run, in both arms, the server is restarted and the Linux page cache of the Docker host is
# dropped, so on a Linux host the history the subscribers read comes from the disk (the data lives on a
# named volume, not the tmpfs the other bench harnesses use; docker-compose.stalled.yml). On a macOS host
# the Docker host is a VM whose disk is an image file on macOS: dropping the VM's cache does not drop
# macOS's own cache of that file, so a read there is not guaranteed to be cold, and the numbers are
# relative (one arm against the other on the same VM). The cold read is only certain on a Linux host,
# such as a CI runner; no CI job runs this harness yet. The history is written once, at the start, by a plain produce (no consumer group reads it before
# the series), and has to outlast every group: K consumers at the widest window read about a million
# records per group per run on a 4 CPU Colima VM, so the default leaves room for PAIRS subscriber runs.
# A subscriber run in which a group reached the end of the history may have loaded the server for only
# part of the producer's window: after every subscriber run each group's committed position is read from
# the node, over Erlang distribution, against the topic's end, and a run where a group got there (or
# whose consumers read nothing) fails the series instead of being recorded as one. The check covers the
# consumers' whole run, which outlasts the producer's window by about 2 s, so a group that runs out only
# in that tail also fails the run: the check errs toward discarding a valid run, never toward recording a
# partial one.
#
# CPUs: the server on SRV_CPUSET (default 2,3), the producer's generator on LT_CPUSET (0), the consumers'
# generator on CONSUMER_CPUSET (1), so the consumers' decoding never competes with the producer's
# measurement. Produce latency is the generator's own (round trip of each produce).
#
# Pairs of runs, the arms in alternating order across pairs (none/subscriber, subscriber/none, ...). The
# servers run in Linux containers, and that is where the numbers come from; the script refuses a Docker
# daemon that is not Linux.
#
# Knobs (env): PAIRS (4), HISTORY records (6000000), K consumers (4), PCONNS producer connections (4),
# PBATCH (10), DUR (8), WARM (2), PAYLOAD (json), SRV_CPUSET (2,3), LT_CPUSET (0), CONSUMER_CPUSET (1),
# OUT (JSON lines, appended; one object per run, with the commit of the tree that ran it).
#
# Usage: OUT=benchmark/results/275/m0-stalled.jsonl benchmark/stalled_subscriber_ab.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
COMPOSE="docker compose -f docker-compose.bench.yml -f benchmark/docker-compose.stalled.yml"

usage_error() { echo "$*" >&2; exit 2; }
require_positive() { case "$2" in '' | *[!0-9]* | 0*) usage_error "$1 must be a positive integer, got '$2'" ;; esac; }

PAIRS="${PAIRS:-4}"
HISTORY="${HISTORY:-6000000}"
K="${K:-4}"
PCONNS="${PCONNS:-4}"
PBATCH="${PBATCH:-10}"
DUR="${DUR:-8}"
WARM="${WARM:-2}"
PAYLOAD="${PAYLOAD:-json}"
OUT="${OUT:-}"
export SRV_CPUSET="${SRV_CPUSET:-2,3}" LT_CPUSET="${LT_CPUSET:-0}" CONSUMER_CPUSET="${CONSUMER_CPUSET:-1}"
source scripts/bench_lib.sh
export SRV_ERL_FLAGS="${SRV_ERL_FLAGS:-$(schedulers_for "$SRV_CPUSET")}"
export LT_ERL_FLAGS="${LT_ERL_FLAGS:-$(schedulers_for "$LT_CPUSET")}"
export CONSUMER_ERL_FLAGS="${CONSUMER_ERL_FLAGS:-$(schedulers_for "$CONSUMER_CPUSET")}"
TREE="$(tree_label)"

for knob in PAIRS HISTORY K PCONNS PBATCH DUR WARM; do require_positive "$knob" "${!knob}"; done
for tool in docker jq; do command -v "$tool" > /dev/null 2>&1 || usage_error "$tool is required"; done
[ "$(docker info --format '{{.OSType}}' 2> /dev/null)" = linux ] ||
  usage_error "the Docker daemon is not Linux (or is not running); the numbers only count on Linux"
if [ -n "$OUT" ]; then mkdir -p "$(dirname "$OUT")" || usage_error "cannot create the directory of OUT=$OUT"; fi
docker ps --format '{{.Names}}' | grep -qx malachi-bench && usage_error "malachi-bench is already running"

WORK="$(mktemp -d)"
trap '$COMPOSE down -v > /dev/null 2>&1; rm -rf "$WORK"' EXIT
FAILED=0

wait_healthy() {
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' malachi-bench 2> /dev/null)" = healthy ] && return 0
    sleep 2
  done
  return 1
}

# The fewest records any of the K groups still has to read in topic hist, from the node's own state.
records_left() {
  docker exec malachi-bench elixir --sname "stalledprobe$$" --cookie malachi_bench -e '
    n = :"malachi@malachi"
    true = Node.connect(n)
    [k] = System.argv() |> Enum.map(&String.to_integer/1)
    ends = :rpc.call(n, :sys, :get_state, [Malachi.LogBroker]).broker.offsets
    left =
      for i <- 0..(k - 1), {range_id, last} <- ends, elem(range_id, 0) == "hist" do
        committed = :rpc.call(n, Malachi.BrokerServer, :committed_offsets, [Malachi.LogBroker, "sgrp_#{i}", "hist"])
        offset = case Map.get(committed, range_id, :start) do {_source, o} -> o; :start -> 0 end
        last - offset
      end
    IO.puts(if left == [], do: "unknown", else: Enum.min(left))
  ' "$K" 2> /dev/null | tail -1
}

cold_restart() {
  docker restart malachi-bench > /dev/null && wait_healthy &&
    docker run --rm --privileged alpine:3.21 sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}

echo "building images..."
$COMPOSE build > /dev/null || { echo "build failed"; exit 1; }
$COMPOSE down -v > /dev/null 2>&1
$COMPOSE up -d --wait malachi > /dev/null 2>&1 || { echo "the server did not come up healthy"; exit 1; }

echo "writing $HISTORY history records to topic hist..."
if [ -z "$($COMPOSE run --rm --no-deps loadtest --host malachi --topic hist --scenario produce --connections 1 \
            --batch 500 --prepopulate "$HISTORY" --duration 1 --warmup 0 --payload "$PAYLOAD" --json 2> "$WORK/prep.err" |
          grep -E '^\{')" ]; then
  echo "writing the history failed:"; tail -5 "$WORK/prep.err"; exit 1
fi
docker exec malachi-bench du -sh /data/malachi_log | sed 's/^/on disk: /'

printf "%-5s %-11s | %10s %8s %8s %8s %6s | %14s\n" pair arm "rec/s" "p50 ms" "p99 ms" "p999 ms" errors "consumer rec/s"
printf -- "------------------------------------------------------------------------------------\n"

for pair in $(seq 1 "$PAIRS"); do
  if [ $((pair % 2)) = 1 ]; then arms="none subscriber"; else arms="subscriber none"; fi
  for arm in $arms; do
    cold_restart || { echo "pair=$pair arm=$arm: cold restart failed"; FAILED=1; continue; }
    cpid=""
    : > "$WORK/consumers.out"
    if [ "$arm" = subscriber ]; then
      $COMPOSE run --rm --no-deps consumers --host malachi --topic hist --scenario stream --connections "$K" \
        --window 10000 --max 1000 --prepopulate 0 --duration $((DUR + WARM + 4)) --warmup 0 --json \
        > "$WORK/consumers.out" 2> /dev/null &
      cpid=$!
      sleep 2
    fi
    json="$($COMPOSE run --rm --no-deps loadtest --host malachi --topic live --scenario produce \
              --connections "$PCONNS" --batch "$PBATCH" --duration "$DUR" --warmup "$WARM" --payload "$PAYLOAD" \
              --json 2> /dev/null | grep -E '^\{' | tail -1)"
    [ -n "$cpid" ] && wait "$cpid"
    cjson="$(grep -E '^\{' "$WORK/consumers.out" | tail -1)"
    crecs="$(if [ -n "$cjson" ]; then jq -r '.records_per_s' <<< "$cjson"; else echo -; fi)"
    if [ -z "$json" ]; then
      printf "%-5s %-11s | %s\n" "$pair" "$arm" "(no json)"; FAILED=1; continue
    fi
    if [ "$arm" = subscriber ] && { [ "$crecs" = - ] || [ "$crecs" = 0 ]; }; then
      printf "%-5s %-11s | %s\n" "$pair" "$arm" "(the consumers read nothing: no subscriber was measured; run not recorded)"
      FAILED=1
      continue
    fi
    if [ "$arm" = subscriber ]; then
      left="$(records_left)"
      case "$left" in
        '' | unknown | *[!0-9]*)
          printf "%-5s %-11s | %s\n" "$pair" "$arm" "(could not read the groups' positions (got '$left'); run not recorded)"
          FAILED=1
          continue
          ;;
        0)
          printf "%-5s %-11s | %s\n" "$pair" "$arm" "(a group reached the end of the history during the run; run not recorded)"
          FAILED=1
          continue
          ;;
      esac
    fi
    read -r recs p50 p99 p999 errs < <(jq -r '[.records_per_s,.latency_ms.p50,.latency_ms.p99,(.latency_ms.p99_9 // "n/a"),.errors]|@tsv' <<< "$json")
    printf "%-5s %-11s | %10s %8s %8s %8s %6s | %14s\n" "$pair" "$arm" "$recs" "$p50" "$p99" "$p999" "$errs" "$crecs"
    # A run with produce errors is still recorded, with its count, and fails the series.
    [ "$errs" = 0 ] || FAILED=1
    if [ -n "$OUT" ]; then
      jq -c --argjson pair "$pair" --arg arm "$arm" --argjson k "$K" --argjson pconns "$PCONNS" --argjson pbatch "$PBATCH" \
        --arg payload "$PAYLOAD" --arg srv "$SRV_CPUSET" --argjson consumers "${cjson:-null}" --arg tree "$TREE" \
        '{pair: $pair, arm: $arm, k: $k, pconns: $pconns, pbatch: $pbatch, payload: $payload, srv_cpuset: $srv,
          tree: $tree, producer: ., consumers: $consumers}' <<< "$json" >> "$OUT"
    fi
  done
done

echo
if [ "$FAILED" != 0 ]; then echo "done, WITH RUNS THAT DID NOT COMPLETE (see above)"; exit 1; fi
echo "done"
