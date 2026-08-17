#!/usr/bin/env bash
# Node vs Elixir load client, side by side against one server. Boots a group-commit dev server once, then
# runs scripts/loadtest.js and `mix malachi.loadtest` at matching configs and prints a comparison table.
#
# Note: the client and server share this machine's cores, so a multi-core client (the Elixir one) competes
# with the server for CPU. To measure a client's true ceiling, run it from a separate host.
#
# Usage: benchmark/compare-clients.sh
set -uo pipefail
ulimit -n 16384 2>/dev/null || true
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

export MALACHI_USER="${MALACHI_USER:-admin}" MALACHI_PASS="${MALACHI_PASS:-admin123}" MALACHI_PORT="${MALACHI_PORT:-4040}"
export MALACHI_RATE_LIMIT_ENABLED=false MALACHI_CONNECTION_LIMIT_ENABLED=false
export MALACHI_GROUP_COMMIT=true MALACHI_GROUP_COMMIT_INTERVAL_MS="${MALACHI_GROUP_COMMIT_INTERVAL_MS:-2}"
TMP="${TMPDIR:-/tmp}"
DUR="${DUR:-8}"
# Private scratch dir (not a predictable /tmp path a local attacker could pre-create as a symlink).
WORK="$(mktemp -d)"
SERVER_LOG="$WORK/server.log"

rm -rf "$TMP/malachi_log" "$TMP/malachi_ra"
MIX_ENV=dev mix run --no-halt >"$SERVER_LOG" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null; rm -rf "$WORK"' EXIT

for _ in $(seq 1 60); do
  node -e 'const n=require("net");const s=n.connect(process.env.MALACHI_PORT||4040,"127.0.0.1");s.on("connect",()=>{s.end();process.exit(0)});s.on("error",()=>process.exit(1));setTimeout(()=>process.exit(1),1000)' 2>/dev/null && break
  kill -0 "$SERVER" 2>/dev/null || { echo "server died"; tail -15 "$SERVER_LOG"; exit 1; }
  sleep 1
done
sleep 1.5

# Extracts "rec/s p50 p99 err" from a --json report on stdin.
read_json='let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{try{const d=JSON.parse(s.split("\n").filter(l=>l.trim().startsWith("{")).pop());const l=d.latency_ms;process.stdout.write(`${String(d.records_per_s).padStart(8)}  p50=${String(l.p50).padStart(6)} p99=${String(l.p99).padStart(6)}  err=${d.errors}`)}catch(e){process.stdout.write("       -")}})'

node_run() { node scripts/loadtest.js --scenario produce --connections "$2" --batch "$1" --duration "$DUR" --record-size 256 --json 2>/dev/null | node -e "$read_json"; }
elixir_run() { MIX_ENV=dev mix malachi.loadtest --scenario produce --connections "$2" --batch "$1" --duration "$DUR" --record-size 256 --json 2>/dev/null | node -e "$read_json"; }

printf '%-22s | %-38s | %-38s\n' "config" "Node (1 core)" "Elixir (multi-core)"
printf '%s\n' "----------------------------------------------------------------------------------------------------"
for cfg in "1 256" "10 128" "10 256" "100 256"; do
  set -- $cfg
  printf '%-22s | %-38s | %-38s\n' "batch=$1 conns=$2" "$(node_run "$1" "$2")" "$(elixir_run "$1" "$2")"
done

echo
echo "done"
