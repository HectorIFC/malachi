#!/usr/bin/env bash
# Ceiling load test: drive ONE generator (node|elixir) against a freshly booted Malachi server, pinning
# the server to SRV_CPUSET and the generator to LT_CPUSET so the client can never steal server CPU, and
# ramp --connections across CONNS_LADDER until throughput stops rising. The peak run is the ceiling and
# becomes the canonical $OUT json; the whole ladder is kept beside it under $RUN_DIR. A sampler reads the
# server beam's /proc CPU over each measured window so the report can say whether the SERVER (3 cores)
# saturated or the 1-core generator capped first.
#
# The methodology is identical for both generators (closed-loop, same flags); each is meant to run on its
# OWN runner so one load test never influences the other. The server is always the Malachi broker (BEAM);
# only the generator differs.
#
# Usage: GENERATOR=node|elixir OUT=/path/loadtest-node.json scripts/loadtest-ceiling.sh
# Knobs (env): SRV_CPUSET=1,2,3  LT_CPUSET=0  CONNS_LADDER="32 64 128 256"  DUR=15  WARM=3  BATCH=10
#   RSIZE=256  MALACHI_USER=admin  MALACHI_PASS=admin123  MALACHI_PORT=4040
#
# Not -e: a single failed sweep point must not abort the whole ladder; failures are handled per point.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

GENERATOR="${GENERATOR:?set GENERATOR=node|elixir}"
case "$GENERATOR" in
  node | elixir) ;;
  *) echo "GENERATOR must be node or elixir, got '$GENERATOR'"; exit 2 ;;
esac

SRV_CPUSET="${SRV_CPUSET:-1,2,3}"
LT_CPUSET="${LT_CPUSET:-0}"
CONNS_LADDER="${CONNS_LADDER:-32 64 128 256}"
DUR="${DUR:-15}"
WARM="${WARM:-3}"
BATCH="${BATCH:-10}"
RSIZE="${RSIZE:-256}"
export MALACHI_USER="${MALACHI_USER:-admin}"
export MALACHI_PASS="${MALACHI_PASS:-admin123}"
export MALACHI_PORT="${MALACHI_PORT:-4040}"
# Admission limits gate connections/auth, not the produce hot path; leaving them on would cap the sweep.
export MALACHI_RATE_LIMIT_ENABLED="${MALACHI_RATE_LIMIT_ENABLED:-false}"
export MALACHI_CONNECTION_LIMIT_ENABLED="${MALACHI_CONNECTION_LIMIT_ENABLED:-false}"

OUT="${OUT:-$ROOT/loadtest-$GENERATOR.json}"
RUN_DIR="${RUN_DIR:-${RUNNER_TEMP:-/tmp}/ceiling-$GENERATOR}"
mkdir -p "$RUN_DIR"
SERVER_LOG="$RUN_DIR/server.log"
: > "$RUN_DIR/loadtest.err"
TMP="${TMPDIR:-/tmp}"

# How many cores SRV_CPUSET names, for the "X of N cores" attribution and the server scheduler count.
SRV_BUDGET="$(echo "$SRV_CPUSET" | tr ',' '\n' | grep -c .)"
CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# A pin prefix like 'taskset -c 1,2,3' (or empty locally where taskset is absent). Kept as a scalar and
# used inline, NOT a shell function: backgrounding a function would put a subshell between $! and the
# beam, and killing that subshell leaks the server. The cpusets have no spaces, so the word-split is safe.
srv_pin=""
lt_pin=""
if command -v taskset > /dev/null 2>&1; then
  srv_pin="taskset -c $SRV_CPUSET"
  lt_pin="taskset -c $LT_CPUSET"
else
  echo "WARN: taskset not found; running UNPINNED (local smoke only, CI must pin)" >&2
fi

port_open() { (echo > "/dev/tcp/127.0.0.1/$MALACHI_PORT") 2> /dev/null; }

# utime+stime in clock ticks for a pid, or non-zero exit if /proc is unavailable (e.g. macOS).
cpu_ticks() { # cpu_ticks <pid>
  local pid="$1"
  [ -r "/proc/$pid/stat" ] || return 1
  awk '{print $14 + $15}' "/proc/$pid/stat"
}

SERVER_PID=""
BEAM_PID=""
boot_server() {
  rm -rf "$TMP/malachi_log" "$TMP/malachi_ra"
  # +S N:N so the BEAM opens no more schedulers than pinned cores, else it oversubscribes them; busy-wait
  # off for the same reason (a spinning scheduler would burn a pinned core doing nothing).
  # shellcheck disable=SC2086  # $srv_pin is a controlled 'taskset -c N' prefix (or empty); split intended
  MIX_ENV=dev ERL_AFLAGS="+S ${SRV_BUDGET}:${SRV_BUDGET} +sbwt none +sbwtdcpu none +sbwtdio none" \
    $srv_pin mix run --no-halt >> "$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    if port_open; then sleep 1.5; break; fi # extra settle so the auth/topic path is ready
    if ! kill -0 "$SERVER_PID" 2> /dev/null; then echo "server died on boot; see $SERVER_LOG"; return 1; fi
    sleep 1
  done
  port_open || { echo "server did not open :$MALACHI_PORT; see $SERVER_LOG"; return 1; }
  # taskset/mix exec straight into the beam, so $! IS the server beam: sample and kill it directly.
  BEAM_PID="$SERVER_PID"
  return 0
}
kill_server() {
  [ -n "$SERVER_PID" ] || return 0
  kill "$SERVER_PID" 2> /dev/null
  wait "$SERVER_PID" 2> /dev/null
  SERVER_PID=""; BEAM_PID=""
}
trap kill_server EXIT

run_point() { # run_point <conns> ; writes $RUN_DIR/run-<conns>.json (canonical fields + attribution)
  local n="$1"
  local out="$RUN_DIR/run-$n.json"
  local cpu_file="$RUN_DIR/cpu-$n.txt"
  rm -f "$cpu_file"

  # Fresh server per ladder point: a later N must never measure a server bloated by an earlier one.
  boot_server || return 1

  # Sample the server beam's CPU across the MEASURED window only (skip warmup, then delta over DUR). A
  # background subshell so it runs concurrently with the generator; writes cores to $cpu_file or nothing.
  (
    sleep "$WARM"
    t0="$(cpu_ticks "$BEAM_PID")" || exit 0
    sleep "$DUR"
    t1="$(cpu_ticks "$BEAM_PID")" || exit 0
    awk -v a="$t0" -v b="$t1" -v hz="$CLK_TCK" -v s="$DUR" 'BEGIN { printf "%.2f", ((b - a) / hz) / s }' \
      > "$cpu_file"
  ) &
  local sampler=$!

  if [ "$GENERATOR" = node ]; then
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    $lt_pin node scripts/loadtest.js --scenario produce --json \
      --connections "$n" --batch "$BATCH" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" > "$out" 2>> "$RUN_DIR/loadtest.err" \
      || { echo "run failed: connections=$n (see loadtest.err)" >&2; rm -f "$out"; }
  else
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    ERL_AFLAGS="+S 1:1" $lt_pin mix malachi.loadtest --scenario produce --json \
      --connections "$n" --batch "$BATCH" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" --pipeline 1 \
      --user "$MALACHI_USER" --pass "$MALACHI_PASS" > "$out" 2>> "$RUN_DIR/loadtest.err" \
      || { echo "run failed: connections=$n (see loadtest.err)" >&2; rm -f "$out"; }
  fi

  wait "$sampler" 2> /dev/null
  local cores="null"
  [ -s "$cpu_file" ] && cores="$(cat "$cpu_file")"

  kill_server

  # Stamp the attribution onto the run json (skip a point whose generator produced nothing).
  [ -f "$out" ] || return 0
  local tmp="$out.tmp"
  if jq --argjson cores "${cores:-null}" --argjson budget "$SRV_BUDGET" \
       '. + {server_cpu_cores: $cores, server_cpu_budget: $budget}' "$out" > "$tmp"; then
    mv "$tmp" "$out"
  else
    rm -f "$tmp"
  fi
}

echo "== ceiling sweep: generator=$GENERATOR ladder=[$CONNS_LADDER] srv=$SRV_CPUSET lt=$LT_CPUSET ==" >&2
for n in $CONNS_LADDER; do
  echo ">> connections=$n" >&2
  run_point "$n"
done

# The ceiling is the peak throughput across the ladder; that run becomes the canonical result.
best="$(jq -s 'map(select(.records_per_s != null)) | sort_by(.records_per_s) | last // empty' \
  "$RUN_DIR"/run-*.json 2> /dev/null)"
if [ -z "$best" ]; then
  echo "no successful runs in the sweep; see $RUN_DIR/loadtest.err" >&2
  exit 1
fi
echo "$best" > "$OUT"

peak_n="$(echo "$best" | jq -r '.connections')"
peak_rps="$(echo "$best" | jq -r '.records_per_s')"
peak_cores="$(echo "$best" | jq -r '.server_cpu_cores // "n/a"')"
echo "== peak: $peak_rps rec/s @ $peak_n connections, server $peak_cores of $SRV_BUDGET cores ==" >&2

# No silent cap: a peak at the top of the ladder means the knee may lie beyond it, so the number is a
# lower bound on the ceiling rather than the ceiling. Say so instead of quietly reporting it as the top.
top="$(echo "$CONNS_LADDER" | awk '{print $NF}')"
if [ "$peak_n" = "$top" ]; then
  echo "WARN: peak at the top of the ladder ($top); widen CONNS_LADDER to confirm the ceiling." >&2
fi
