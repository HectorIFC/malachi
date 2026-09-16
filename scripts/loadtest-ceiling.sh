#!/usr/bin/env bash
# Ceiling load test: drive ONE generator (node|elixir) against a freshly booted Malachi server, pinning
# the server to SRV_CPUSET and the generator to LT_CPUSET so the client can never steal server CPU, and
# sweep two axes: the batch size (records per produce, so bytes per flush with group commit off) across
# BATCH_LADDER, and for each batch size the connection count across its own ladder. Each batch size's
# peak is its ceiling; the headline batch size's peak is what readers of the flat result see, and the
# whole curve is published beside it in $OUT. Every run is kept under $RUN_DIR. A sampler reads the
# /proc CPU of BOTH the server beam and the generator over each measured window, so the report can say
# which side saturated: the server (its ceiling was found) or the generator (the number is a lower bound).
#
# Why a curve: the per-flush cost of the commit path changes sign with the flush size (segment
# preallocation is 69.7% faster at 2.5KB per flush and 16.2% slower at 1MB, crossing near 170KB, see
# Malachi.Storage.Preallocation), so a ceiling at one batch size describes one slice of the surface.
#
# The methodology is identical for both generators (closed-loop, same flags); each is meant to run on its
# OWN runner so one load test never influences the other. The server is always the Malachi broker (BEAM);
# only the generator differs. What gets published (validation, run order, peak election, lower bounds,
# the JSON shape) is decided by `mix malachi.loadtest.ceiling`, which is tested; this script runs things.
#
# Usage: GENERATOR=node|elixir OUT=/path/loadtest-node.json scripts/loadtest-ceiling.sh
# Knobs (env): SRV_CPUSET=1,2,3  LT_CPUSET=0  DUR=15  WARM=3  RSIZE=256  REPS=1
#   BATCH_LADDER="10 100 512 1024 4096"  HEADLINE_BATCH=10
#   CONNS_LADDER="32 64 128 256 512" (for any batch size without its own)
#   CONNS_LADDER_<batch>="..." (that batch size's ladder; defaults below for 100, 512, 1024 and 4096)
#   MARKER_TIMEOUT=300  MALACHI_USER=admin  MALACHI_PASS=admin123  MALACHI_PORT=4040
# Exit: 0 with a headline peak, 1 without one ($OUT is still written) or on a failed step, 2 on invalid
# knobs.
#
# Not -e: a single failed sweep point must not abort the whole sweep; failures are handled per point.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

GENERATOR="${GENERATOR:?set GENERATOR=node|elixir}"
case "$GENERATOR" in
  node | elixir) ;;
  *) echo "GENERATOR must be node or elixir, got '$GENERATOR'" >&2; exit 2 ;;
esac

# BATCH was a single batch size. Ignoring it silently would publish a sweep its caller did not ask for.
if [ -n "${BATCH+set}" ]; then
  echo "BATCH was replaced by BATCH_LADDER (a list of batch sizes) and HEADLINE_BATCH; unset BATCH" >&2
  exit 2
fi

SRV_CPUSET="${SRV_CPUSET:-1,2,3}"
LT_CPUSET="${LT_CPUSET:-0}"
# Chosen on the byte sizes #83 measured the preallocation curve at: with 256B records these are 2.5KB,
# 25KB, 128KB, 256KB and 1MB of values per request, bracketing the ~170KB crossover and both extremes.
BATCH_LADDER="${BATCH_LADDER:-10 100 512 1024 4096}"
# The batch size readers of the flat result see, and the one every earlier published number describes.
HEADLINE_BATCH="${HEADLINE_BATCH:-10}"
# One run per point by default: the published sweep states it, and the A-A repeat of the headline peak
# measures how much a single run moves. Raise it for a question that needs more than one.
REPS="${REPS:-1}"
# 512 included because the first CI runs peaked at 256, the then-top rung, with the server short of its
# core budget: the knee lies above.
CONNS_LADDER="${CONNS_LADDER:-32 64 128 256 512}"
DUR="${DUR:-15}"
WARM="${WARM:-3}"
# How long a generator may take to reach its measured window. Every connection authenticates first,
# which took 25s at 512 connections on the CI runner, so this bounds a stuck connect phase rather than
# a slow one.
MARKER_TIMEOUT="${MARKER_TIMEOUT:-300}"
# The span sampled inside the measured window: one second short of DUR when DUR allows it. The window
# ends when the generator stops measuring, and a generator that has already exited by the closing read
# leaves its side unsampled.
if [ "$DUR" -ge 3 ]; then SAMPLE_S=$((DUR - 1)); else SAMPLE_S="$DUR"; fi
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
# The regime the published curve claims, forced for the same reason. With group commit off every produce
# is its own flush, which is what makes a batch size a flush size; an inherited =true would coalesce
# requests and publish flush sizes that never happened. Preallocation is forced to the application's
# default so the curve describes the configuration production runs, whatever the caller exported.
export MALACHI_GROUP_COMMIT=false
export MALACHI_SEGMENT_PREALLOC_BYTES=67108864

OUT="${OUT:-$ROOT/loadtest-$GENERATOR.json}"
# Create OUT's directory up front, and fail if that cannot be done: without -e a failed final write
# would otherwise still exit 0 and report a peak the run never persisted.
mkdir -p "$(dirname "$OUT")" || { echo "cannot create output directory for $OUT" >&2; exit 1; }
RUN_DIR="${RUN_DIR:-${RUNNER_TEMP:-/tmp}/ceiling-$GENERATOR}"
mkdir -p "$RUN_DIR"
# A reused RUN_DIR (the default is a fixed path) still holds the previous sweep's runs, and a point this
# sweep failed to measure would otherwise be read back from that one.
rm -f "$RUN_DIR"/run-*.json "$RUN_DIR"/aa-*.json "$RUN_DIR"/*.marker "$RUN_DIR"/*.cpu-*.txt "$RUN_DIR"/sweep.json
SWEEP="$RUN_DIR/sweep.json"
SERVER_LOG="$RUN_DIR/server.log"
: > "$RUN_DIR/loadtest.err"
TMP="${TMPDIR:-/tmp}"

# The connection ladder a batch size gets when CONNS_LADDER_<batch> is not set. A larger request
# saturates the server at fewer connections, so the ladders shift down as the batch grows, keeping at
# most 64MB of values in flight and the runner time on points that carry information.
default_conns_ladder() { # default_conns_ladder <batch>
  case "$1" in
    100) echo "16 32 64 128 256" ;;
    512 | 1024) echo "8 16 32 64 128" ;;
    4096) echo "4 8 16 32 64" ;;
    *) echo "$CONNS_LADDER" ;;
  esac
}

ceiling() { MIX_ENV=dev mix malachi.loadtest.ceiling "$@"; }

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

# Waits until the generator creates its measure marker, the signal that its measured window began. The
# harness cannot infer that instant from the spawn: both generators authenticate every connection and
# warm up first, and a sampler started WARM seconds after the spawn measured the server verifying
# credentials and the generator waiting on it (server 2.88 of 3 cores, generator 0.01, at 512
# connections). Gives up when the generator exits first (the caller reports that failure) or after
# MARKER_TIMEOUT, leaving the attribution unset rather than sampled over the wrong window.
wait_for_marker() { # wait_for_marker <marker> <gen_pid>
  local marker="$1" gen_pid="$2" ticks=$((MARKER_TIMEOUT * 10))
  while [ ! -e "$marker" ]; do
    if ! kill -0 "$gen_pid" 2> /dev/null; then
      # It may have written the marker between the two checks and finished already.
      [ -e "$marker" ] && return 0
      return 1
    fi
    if [ "$ticks" -le 0 ]; then
      echo "NOTE: no measured window within ${MARKER_TIMEOUT}s; CPU attribution left unset for this point" >&2
      return 1
    fi
    ticks=$((ticks - 1))
    sleep 0.1
  done
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
    if ! kill -0 "$SERVER_PID" 2> /dev/null; then echo "server died on boot; see $SERVER_LOG" >&2; return 1; fi
    sleep 1
  done
  port_open || { echo "server did not open :$MALACHI_PORT; see $SERVER_LOG" >&2; return 1; }
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

# run_point <batch> <conns> <out.json>: one generator run against a fresh server, written to <out.json>
# with the CPU attribution stamped on. Nothing is written for a run whose generator failed.
run_point() {
  local batch="$1" n="$2" out="$3"
  local base="${out%.json}"
  local srv_cpu_file="$base.cpu-srv.txt"
  local gen_cpu_file="$base.cpu-gen.txt"
  local marker="$base.marker"
  rm -f "$out" "$srv_cpu_file" "$gen_cpu_file" "$marker"

  # Fresh server per point: a later point must never measure a server bloated by an earlier one.
  boot_server || return 1

  # The generator runs in the background so its pid can be sampled alongside the server's; its exit
  # status is collected by the wait below. taskset/mix/node all exec straight into the measured process,
  # so $! is the right pid for /proc on both sides.
  # The connect strategy is pinned explicitly (not left to the generators' defaults) so the published
  # methodology is visible here: bounded connects avoid the auth storm that killed the high rungs, since
  # every connection pays an Argon2 verify on the 3-core server and hundreds at once exhaust it.
  if [ "$GENERATOR" = node ]; then
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    $lt_pin node scripts/loadtest.js --scenario produce --json \
      --connections "$n" --batch "$batch" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" \
      --connect-strategy bounded --connect-concurrency 32 \
      --measure-marker "$marker" > "$out" 2>> "$RUN_DIR/loadtest.err" < /dev/null &
  else
    # shellcheck disable=SC2086  # $lt_pin is a controlled 'taskset -c N' prefix (or empty); split intended
    ERL_AFLAGS="$GEN_ERL_AFLAGS" $lt_pin mix malachi.loadtest --scenario produce --json \
      --connections "$n" --batch "$batch" --record-size "$RSIZE" \
      --duration "$DUR" --warmup "$WARM" --pipeline 1 --host 127.0.0.1 \
      --connect-strategy bounded --connect-concurrency 32 \
      --user "$MALACHI_USER" --pass "$MALACHI_PASS" \
      --measure-marker "$marker" > "$out" 2>> "$RUN_DIR/loadtest.err" < /dev/null &
  fi
  local gen_pid=$!

  # Sample both sides' CPU across the MEASURED window only: from the generator's marker, for SAMPLE_S.
  # One background subshell; each side writes its cores file, or nothing where /proc is unavailable or
  # no measured window was signalled.
  (
    wait_for_marker "$marker" "$gen_pid" || exit 0
    srv_t0="$(cpu_ticks "$BEAM_PID")" || srv_t0=""
    gen_t0="$(cpu_ticks "$gen_pid")" || gen_t0=""
    sleep "$SAMPLE_S"
    if [ -n "$srv_t0" ] && srv_t1="$(cpu_ticks "$BEAM_PID")"; then
      awk -v a="$srv_t0" -v b="$srv_t1" -v hz="$CLK_TCK" -v s="$SAMPLE_S" \
        'BEGIN { printf "%.2f", ((b - a) / hz) / s }' > "$srv_cpu_file"
    fi
    if [ -n "$gen_t0" ] && gen_t1="$(cpu_ticks "$gen_pid")"; then
      awk -v a="$gen_t0" -v b="$gen_t1" -v hz="$CLK_TCK" -v s="$SAMPLE_S" \
        'BEGIN { printf "%.2f", ((b - a) / hz) / s }' > "$gen_cpu_file"
    fi
  ) &
  local sampler=$!

  if ! wait "$gen_pid"; then
    echo "run failed: batch=$batch connections=$n (see loadtest.err)" >&2
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

# Validate the knobs and plan the run order before booting anything. Each batch size's connection ladder
# comes from CONNS_LADDER_<batch> when that is SET (an empty value is passed on and refused, not replaced
# by the default), else from the table above. A token that is not a plain number has no such variable;
# it is passed on as it is for the planner to name.
conns_args=()
for batch in $BATCH_LADDER; do
  ladder=""
  if [[ "$batch" =~ ^[0-9]+$ ]]; then
    var="CONNS_LADDER_$batch"
    ladder="${!var-$(default_conns_ladder "$batch")}"
  fi
  conns_args+=(--conns-ladder "$batch=$ladder")
done

plan_output="$(ceiling plan --batch-ladder "$BATCH_LADDER" "${conns_args[@]}" \
  --headline-batch "$HEADLINE_BATCH" --reps "$REPS" --record-size "$RSIZE" \
  --group-commit "$MALACHI_GROUP_COMMIT" --segment-prealloc-bytes "$MALACHI_SEGMENT_PREALLOC_BYTES" \
  --out "$SWEEP")"
plan_status=$?
if [ "$plan_status" -ne 0 ]; then
  exit "$plan_status"
fi
# Only the point lines: mix may print compilation output on the same stream.
points="$(printf '%s\n' "$plan_output" | grep -E '^[0-9]+ [0-9]+ [0-9]+$')"
planned="$(printf '%s\n' "$points" | grep -c .)"
headline="$(jq -r '.headline_batch' "$SWEEP")"

echo "== ceiling sweep: generator=$GENERATOR batches=[$BATCH_LADDER] headline=$headline reps=$REPS points=$planned srv=$SRV_CPUSET lt=$LT_CPUSET ==" >&2
started=$SECONDS
index=0
while read -r batch n rep; do
  index=$((index + 1))
  echo ">> [$index/$planned] batch=$batch connections=$n repetition=$rep ($((SECONDS - started))s elapsed)" >&2
  # stdin from /dev/null: nothing a point starts may read the list this loop is reading.
  run_point "$batch" "$n" "$RUN_DIR/run-b$batch-c$n-r$rep.json" < /dev/null
done <<< "$points"

# The A-A control: the headline peak once more, on a fresh server, after everything else. How far it
# moves is the noise floor the published curve is read against.
peak_connections="$(ceiling peak --run-dir "$RUN_DIR" --sweep "$SWEEP" | grep -E '^[0-9]+$' | tail -n 1)"
if [ -n "$peak_connections" ]; then
  echo ">> A-A control: batch=$headline connections=$peak_connections ($((SECONDS - started))s elapsed)" >&2
  run_point "$headline" "$peak_connections" "$RUN_DIR/aa-b$headline-c$peak_connections.json" < /dev/null
fi

ceiling summarize --run-dir "$RUN_DIR" --sweep "$SWEEP" --out "$OUT"
status=$?

if [ "$status" -eq 0 ]; then
  jq -r '"== headline: \(.records_per_s) rec/s @ \(.connections) connections, \(.regime_label), server \(.server_cpu_cores // "n/a") of \(.server_cpu_budget) cores, generator \(.generator_cpu_cores // "n/a") of \(.generator_cpu_budget) =="' "$OUT" >&2
fi
if [ -f "$OUT" ]; then
  # No silent caps: every batch size that is only a lower bound, or has no peak at all, is named here as
  # well as in the result.
  jq -r '.curve[]? | select(.peak_at_ladder_limit == true) | "WARN: batch \(.batch) peaked at the top of its connection ladder; widen CONNS_LADDER_\(.batch) to confirm the ceiling."' "$OUT" >&2
  jq -r '.curve[]? | select(.status != "peak") | "WARN: batch \(.batch) has no peak (\(.status))."' "$OUT" >&2
fi
echo "== sweep took $((SECONDS - started))s ==" >&2
exit "$status"
