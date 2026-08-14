#!/usr/bin/env bash
# Empirical produce-throughput sweep (see the plan behind this): boots a clean dev server and drives
# scripts/loadtest.js across a closed-loop connections x batch matrix plus an open-loop pipelined check,
# writing one JSON per run and aggregating a CSV + summary table. Then runs the in-VM Elixir ceilings
# (storage_viability, throughput_1m). No server code is involved; this only orchestrates existing tools.
#
# Usage: scripts/bench-sweep.sh [out_dir]
#   out_dir defaults to a scratch path so results never pollute the repo tree.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${1:-${OUT_DIR:-/tmp/malachi-sweep}}"
RUN_DIR="$OUT_DIR/runs"
mkdir -p "$RUN_DIR"
CSV="$OUT_DIR/sweep.csv"
SERVER_LOG="$OUT_DIR/server.log"
: > "$OUT_DIR/loadtest.err"

export MALACHI_USER="${MALACHI_USER:-admin}"
export MALACHI_PASS="${MALACHI_PASS:-admin123}"
export MALACHI_PORT="${MALACHI_PORT:-4040}"
# Admission limits are per-IP and would otherwise cap the sweep (auth rate limit default 10/60s, and
# max_connections_per_ip default 100). All load comes from one host opening up to 128 connections plus
# reconnects, so both must be off. Neither touches the produce hot path (publish/subscribe are not
# rate-limited on the broker path per docs/RATE_LIMITING.md), so throughput stays comparable to the
# dashboard baseline; only connection/auth admission changes.
export MALACHI_RATE_LIMIT_ENABLED="${MALACHI_RATE_LIMIT_ENABLED:-false}"
export MALACHI_CONNECTION_LIMIT_ENABLED="${MALACHI_CONNECTION_LIMIT_ENABLED:-false}"
DUR="${DUR:-12}"          # measured seconds per run
WARM="${WARM:-2}"         # warmup seconds excluded from stats
RSIZE="${RSIZE:-256}"     # record value bytes (matches the dashboard baseline)
TMP="${TMPDIR:-/tmp}"

SERVER_PID=""

port_open() {
  node -e 'const n=require("net");const s=n.connect(process.env.MALACHI_PORT,"127.0.0.1");
    s.on("connect",()=>{s.end();process.exit(0)});s.on("error",()=>process.exit(1));
    setTimeout(()=>process.exit(1),1000)' 2>/dev/null
}

boot_server() {
  rm -rf "$TMP/malachi_log" "$TMP/malachi_ra"
  MIX_ENV=dev mix run --no-halt >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    if port_open; then sleep 1.5; return 0; fi   # extra settle so auth/topic path is ready
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then echo "server died on boot; see $SERVER_LOG"; return 1; fi
    sleep 1
  done
  echo "server did not open port $MALACHI_PORT; see $SERVER_LOG"; return 1
}

kill_server() {
  [ -n "$SERVER_PID" ] || return 0
  kill "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

trap kill_server EXIT

# run_case <mode> <batch> <conns> <label> [extra loadtest args...]
run_case() {
  local mode="$1" batch="$2" conns="$3" label="$4"; shift 4
  local out="$RUN_DIR/${label}.json"
  echo ">> $label" >&2
  if node scripts/loadtest.js --scenario produce --json \
       --record-size "$RSIZE" --duration "$DUR" --warmup "$WARM" \
       --batch "$batch" --connections "$conns" "$@" >"$out" 2>>"$OUT_DIR/loadtest.err"; then
    # stamp the params we controlled onto the JSON so the aggregator never has to guess
    node -e 'const fs=require("fs");const f=process.argv[1];const d=JSON.parse(fs.readFileSync(f));
      d._sweep={mode:process.argv[2],batch:+process.argv[3],conns:+process.argv[4],label:process.argv[5]};
      fs.writeFileSync(f,JSON.stringify(d));' "$out" "$mode" "$batch" "$conns" "$label"
  else
    echo "   run failed: $label (see loadtest.err)" >&2
    rm -f "$out"
  fi
}

# ---- baseline sanity: reproduce the dashboard cell (batch 10, 8 conns, 6s) ----
echo "== baseline sanity ==" >&2
boot_server || exit 1
DUR_SAVE="$DUR"; DUR=6
run_case closed 10 8 "sanity_b10_c8"
DUR="$DUR_SAVE"
kill_server

# ---- Phase 1: closed-loop connections x batch ----
echo "== phase 1: closed-loop sweep ==" >&2
phase1() {
  local batch="$1"; shift
  boot_server || exit 1
  for c in "$@"; do
    run_case closed "$batch" "$c" "p1_b${batch}_c${c}"
  done
  kill_server
}
phase1 1    8 16 32 64 128
phase1 10   8 16 32 64 128
phase1 100  8 16 32 64
phase1 1000 4 8 16 32

# ---- Phase 2: open-loop / pipelined (push past per-connection concurrency) ----
echo "== phase 2: open-loop pipelined ==" >&2
phase2() {
  local batch="$1"; shift
  boot_server || exit 1
  for mi in "$@"; do
    run_case open "$batch" 16 "p2_b${batch}_mi${mi}" --rate 500000 --max-inflight "$mi"
  done
  kill_server
}
phase2 100  64 128 256
phase2 1000 64 128 256

# ---- Aggregate networked runs into a CSV + table ----
echo "== aggregate ==" >&2
node -e '
  const fs=require("fs"),path=require("path");
  const dir=process.argv[1];
  const rows=[];
  for(const f of fs.readdirSync(dir).filter(x=>x.endsWith(".json")).sort()){
    const d=JSON.parse(fs.readFileSync(path.join(dir,f)));
    const s=d._sweep||{},l=d.latency_ms||{};
    rows.push({label:s.label,mode:s.mode,batch:s.batch,conns:s.conns,
      records_per_s:Math.round(d.records_per_s),ops_per_s:Math.round(d.ops_per_s),
      p50:l.p50,p99:l.p99,p99_99:l.p99_99,errors:d.errors,saturated:d.saturated??""});
  }
  const cols=["label","mode","batch","conns","records_per_s","ops_per_s","p50","p99","p99_99","errors","saturated"];
  const csv=[cols.join(",")].concat(rows.map(r=>cols.map(c=>{
    const v=r[c]; return (typeof v==="number"&&!Number.isInteger(v))?v.toFixed(2):(v??"");
  }).join(","))).join("\n");
  fs.writeFileSync(process.argv[2],csv+"\n");
  // pretty table to stdout
  const w=cols.map(c=>Math.max(c.length,...rows.map(r=>String(r[c]??"").length)));
  const line=a=>a.map((v,i)=>String(v??"").padStart(w[i])).join("  ");
  console.log(line(cols));
  for(const r of rows) console.log(line(cols.map(c=>{
    const v=r[c]; return (typeof v==="number"&&!Number.isInteger(v))?v.toFixed(2):v;
  })));
' "$RUN_DIR" "$CSV"

echo "CSV: $CSV" >&2

# ---- Phase 3: in-VM Elixir ceilings (server stopped) ----
echo "== phase 3: in-VM Elixir ceilings ==" >&2
{
  echo "----- storage_viability.exs (raw fsync/WAL ceiling) -----"
  MIX_ENV=dev mix run benchmark/storage_viability.exs 2>&1
  echo
  echo "----- throughput_1m.exs (in-VM end-to-end, batch 1000) -----"
  MIX_ENV=dev mix run benchmark/throughput_1m.exs 2>&1
} | tee "$OUT_DIR/elixir-ceilings.txt"

echo "== done: $OUT_DIR ==" >&2
