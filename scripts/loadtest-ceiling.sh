#!/usr/bin/env bash
# Ceiling load test: drive ONE generator (node|elixir) against a freshly booted Malachi server, pinning
# the server to SRV_CPUSET and the generator to LT_CPUSET so the client can never steal server CPU, and
# ramp --connections across CONNS_LADDER until throughput stops rising. The peak run is the ceiling and
# becomes the canonical $OUT json; the whole ladder is kept beside it under $RUN_DIR. A sampler reads the
# /proc CPU of BOTH the server beam and the generator over each measured window, so the report can say
# which side saturated: the server (its ceiling was found) or the generator (the number is a lower bound).
#
# The methodology is identical for both generators (closed-loop, same flags); each is meant to run on its
# OWN runner so one load test never influences the other. The server is always the Malachi broker (BEAM);
# only the generator differs.
#
# Usage: GENERATOR=node|elixir OUT=/path/loadtest-node.json scripts/loadtest-ceiling.sh
# Knobs (env): SRV_CPUSET=1,2,3  LT_CPUSET=0  CONNS_LADDER="32 64 128 256 512"  DUR=15  WARM=3  BATCH=10
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
# 512 included because the first CI runs peaked at 256, the then-top rung, with the server short of its
# core budget: the knee lies above.
CONNS_LADDER="${CONNS_LADDER:-32 64 128 256 512}"
DUR="${DUR:-15}"
WARM="${WARM:-3}"
BATCH="${BATCH:-10}"
RSIZE="${RSIZE:-256}"
export MALACHI_USER="${MALACHI_USER:-admin}"
export MALACHI_PASS="${MALACHI_PASS:-admin123}"
export MALACHI_PORT="${MALACHI_PORT:-4040}"
# The Node generator reads MALACHI_HOST from the environment; force it to the server this script boots so
# an inherited value can never aim the load at another server while CPU sampling targets our idle local
# one. (The Elixir generator takes --host instead, passed explicitly below.)
export MALACHI_HOST="127.0.0.1"
# Admission limits gate connections/auth, not the produce hot path; forced off (not defaulted) so an
# inherited =true cannot leave a cap in place and get published as the ceiling.
export MALACHI_RATE_LIMIT_ENABLED=false
export MALACHI_CONNECTION_LIMIT_ENABLED=false

OUT="${OUT:-$ROOT/loadtest-$GENERATOR.json}"
# Create OUT's directory up front, and fail if that cannot be done: without -e a failed final write
# would otherwise still exit 0 and report a peak the run never persisted.
mkdir -p "$(dirname "$OUT")" || { echo "cannot create output directory for $OUT" >&2; exit 1; }
RUN_DIR="${RUN_DIR:-${RUNNER_TEMP:-/tmp}/ceiling-$GENERATOR}"
mkdir -p "$RUN_DIR"
SERVER_LOG="$RUN_DIR/server.log"
: > "$RUN_DIR/loadtest.err"
TMP="${TMPDIR:-/tmp}"

# How many cores a taskset cpu-list names, for the "X of N cores" attribution and the scheduler counts.
# Handles every form taskset accepts: single ids (0), ranges (1-3), and strides (0-10:2); counting
# comma tokens alone would read 1-3 as ONE core and boot the server with a third of its schedulers.
count_cpus() { # count_cpus <cpu-list>
  echo "$1" | tr ',' '\n' | awk -F'[-:]' '
    /^$/ { next }
    NF == 1 { total += 1 }
    NF == 2 { total += $2 - $1 + 1 }
    NF == 3 { total += int(($2 - $1) / $3) + 1 }
    END { print total + 0 }
  '
}
SRV_BUDGET="$(count_cpus "$SRV_CPUSET")"
LT_BUDGET="$(count_cpus "$LT_CPUSET")"
CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# The generator-side BEAM flags. +S alone is NOT enough on a pinned single core: the VM still starts 4
# dirty CPU schedulers, 10 dirty IO schedulers and aux threads by default, and schedulers busy-wait when
# idle. ~16 threads timeslicing one core, several of them spinning, produced second-long stalls in CI
# (p50 37ms, p99 1.1-2.4s, server nearly idle) and a non-monotonic ladder. So: schedulers = pinned
# cores, dirty pools shrunk to 1 (a generator does no file IO inside the window), and all busy-wait off,
# the same treatment the server gets.
GEN_ERL_AFLAGS="+S ${LT_BUDGET}:${LT_BUDGET} +SDcpu ${LT_BUDGET}:${LT_BUDGET} +SDio 1 +sbwt none +sbwtdcpu none +sbwtdio none"

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
  # Refuse a port already held (a foreign server, or one a previous point failed to release): otherwise
  # the wait loop below sees it open at once and greenlights measuring THAT server, while the fresh mix
  # run dies on the bind and CPU sampling targets our own dead child.
  if port_open; then echo "port $MALACHI_PORT is already in use; refusing to boot" >&2; return 1; fi
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
  local srv_cpu_file="$RUN_DIR/cpu-srv-$n.txt"
  local gen_cpu_file="$RUN_DIR/cpu-gen-$n.txt"
  rm -f "$srv_cpu_file" "$gen_cpu_file"

  # Fresh server per ladder point: a later N must never measure a server bloated by an earlier one.
  boot_server || return 1

  # The generator runs in the background so its pid can be sampled alongside the server's; its exit
  # status is collected by the wait below. taskset/mix/node all exec straight into the measured process,
  # so $! is the right pid for /proc on both sides.
  if [ "$GENERATOR" = node ]; then
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    $lt_pin node scripts/loadtest.js --scenario produce --json \
      --connections "$n" --batch "$BATCH" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" > "$out" 2>> "$RUN_DIR/loadtest.err" &
  else
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    ERL_AFLAGS="$GEN_ERL_AFLAGS" $lt_pin mix malachi.loadtest --scenario produce --json \
      --connections "$n" --batch "$BATCH" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" --pipeline 1 --host 127.0.0.1 \
      --user "$MALACHI_USER" --pass "$MALACHI_PASS" > "$out" 2>> "$RUN_DIR/loadtest.err" &
  fi
  local gen_pid=$!

  # Sample both sides' CPU across the MEASURED window only (skip warmup, then delta over DUR). One
  # background subshell; each side writes its cores file, or nothing where /proc is unavailable.
  (
    sleep "$WARM"
    srv_t0="$(cpu_ticks "$BEAM_PID")" || srv_t0=""
    gen_t0="$(cpu_ticks "$gen_pid")" || gen_t0=""
    sleep "$DUR"
    if [ -n "$srv_t0" ] && srv_t1="$(cpu_ticks "$BEAM_PID")"; then
      awk -v a="$srv_t0" -v b="$srv_t1" -v hz="$CLK_TCK" -v s="$DUR" \
        'BEGIN { printf "%.2f", ((b - a) / hz) / s }' > "$srv_cpu_file"
    fi
    if [ -n "$gen_t0" ] && gen_t1="$(cpu_ticks "$gen_pid")"; then
      awk -v a="$gen_t0" -v b="$gen_t1" -v hz="$CLK_TCK" -v s="$DUR" \
        'BEGIN { printf "%.2f", ((b - a) / hz) / s }' > "$gen_cpu_file"
    fi
  ) &
  local sampler=$!

  if ! wait "$gen_pid"; then
    echo "run failed: connections=$n (see loadtest.err)" >&2
    rm -f "$out"
  fi
  wait "$sampler" 2> /dev/null

  local srv_cores="null" gen_cores="null"
  [ -s "$srv_cpu_file" ] && srv_cores="$(cat "$srv_cpu_file")"
  [ -s "$gen_cpu_file" ] && gen_cores="$(cat "$gen_cpu_file")"

  kill_server

  # Stamp the attribution onto the run json (skip a point whose generator produced nothing).
  [ -f "$out" ] || return 0
  local tmp="$out.tmp"
  if jq --argjson srv "${srv_cores:-null}" --argjson srvb "$SRV_BUDGET" \
       --argjson gen "${gen_cores:-null}" --argjson genb "$LT_BUDGET" \
       '. + {server_cpu_cores: $srv, server_cpu_budget: $srvb, generator_cpu_cores: $gen, generator_cpu_budget: $genb}' \
       "$out" > "$tmp"; then
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
peak_n="$(echo "$best" | jq -r '.connections')"
peak_rps="$(echo "$best" | jq -r '.records_per_s')"
peak_srv="$(echo "$best" | jq -r '.server_cpu_cores // "n/a"')"
peak_gen="$(echo "$best" | jq -r '.generator_cpu_cores // "n/a"')"
top="$(echo "$CONNS_LADDER" | awk '{print $NF}')"

# No silent cap: a peak at the top of the ladder means the knee may lie beyond it, so the number is only
# a lower bound on the ceiling. Stamp that onto the result so the caveat travels with the number into the
# comment and the pages, not just this CI log.
limited=false
[ "$peak_n" = "$top" ] && limited=true
best="$(echo "$best" | jq --argjson limited "$limited" '. + {peak_at_ladder_limit: $limited}')"
echo "$best" > "$OUT"

echo "== peak: $peak_rps rec/s @ $peak_n connections, server $peak_srv of $SRV_BUDGET cores, generator $peak_gen of $LT_BUDGET ==" >&2
if [ "$limited" = true ]; then
  echo "WARN: peak at the top of the ladder ($top); widen CONNS_LADDER to confirm the ceiling." >&2
fi
