#!/usr/bin/env bash
# Docker cpuset comparison: the server and the Elixir load client run in separate containers, once with
# their cores SEPARATED (server 4-7, client 0-3) and once CO-LOCATED (both 0-7), to isolate how much of the
# networked ceiling was core contention between client and server.
#
# Requires the Docker VM to have >= 8 CPUs (Docker Desktop > Settings > Resources). On macOS the numbers
# are relative (arm64 Linux VM overhead), so read the separated-vs-co-located delta, not the absolute peak.
#
# Usage: benchmark/docker-compare.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose -f docker-compose.bench.yml"
DUR="${DUR:-8}"

# Extract "rec/s ops/s p50 p99 err" from a --json report line on stdin.
READ_JSON='let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{const line=s.split("\n").find(l=>l.trim().startsWith("{"));if(!line){process.stdout.write("       (no json)");return}const d=JSON.parse(line);const l=d.latency_ms;process.stdout.write(`${String(d.records_per_s).padStart(8)} rec/s ${String(d.ops_per_s).padStart(6)} ops/s  p50=${String(l.p50).padStart(6)} p99=${String(l.p99).padStart(6)}  err=${d.errors}`)})'

matrix() { # label SRV_CPUSET LT_CPUSET
  local label="$1"
  export SRV_CPUSET="$2" LT_CPUSET="$3"

  echo "== ${label}: server cores=${2}  client cores=${3} =="
  if ! $COMPOSE up -d --wait malachi >/tmp/dc_up.log 2>&1; then
    echo "  server did not come up healthy; see /tmp/dc_up.log"; cat /tmp/dc_up.log; return 1
  fi

  for cfg in "1 256" "10 128" "10 256" "100 256"; do
    set -- $cfg
    out=$($COMPOSE run --rm loadtest \
            --host malachi --scenario produce --connections "$2" --batch "$1" \
            --duration "$DUR" --warmup 2 --record-size 256 --json 2>/dev/null | node -e "$READ_JSON")
    printf "  batch=%-4s conns=%-4s  %s\n" "$1" "$2" "$out"
  done

  $COMPOSE down >/dev/null 2>&1
}

echo "Building images (first run compiles all deps; slow)..."
$COMPOSE build || { echo "build failed"; exit 1; }

matrix "SEPARATED " "4,5,6,7" "0,1,2,3"
echo
matrix "CO-LOCATED" "0,1,2,3,4,5,6,7" "0,1,2,3,4,5,6,7"

echo
echo "done"
