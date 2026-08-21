# Shared plumbing for the chaos certification harnesses (docker-chaos-test.sh and
# docker-storage-chaos.sh): compose lifecycle, health polling, the acked-durability checker, and
# the closing invariants. Callers export their env knobs (RF, MALACHI_DATA_ROOT, cpusets, ...)
# BEFORE sourcing this file; it defines functions and the WORK scratch dir, nothing runs yet.
#
# Contract: every check calls `fail` instead of exiting, so a run always reaches its summary;
# `finish <name>` prints the PASS/FAIL banner and exits with the accumulated status.

COMPOSE="docker compose -f docker-compose.cluster.yml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAILED=0
CHAOS_HOSTS="malachi1,malachi2,malachi3"

say() { printf '\n== %s ==\n' "$1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

wait_healthy() {
  for _ in $(seq 1 48); do
    h=$(docker ps --filter "name=malachi-cluster" --filter "health=healthy" --format '{{.Names}}' | wc -l | tr -d ' ')
    [ "$h" = "3" ] && return 0
    sleep 5
  done
  return 1
}

build_images() {
  say "building images"
  $COMPOSE build >/dev/null 2>&1 || { echo "build failed"; exit 1; }
}

# Fresh cluster on fresh volumes; sets NET to the compose network name for partition events.
start_cluster() {
  say "starting 3-node RF=${RF} cluster (fresh persistent volumes)"
  $COMPOSE down -v >/dev/null 2>&1
  $COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 >"$WORK/up.log" 2>&1
  wait_healthy || { echo "cluster never converged; aborting"; cat "$WORK/up.log"; exit 1; }
  NET=$(docker inspect malachi-cluster-1 --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
  echo "cluster healthy; network: $NET"
}

# Runs the checker/topology script inside the compose network (the cluster publishes no host
# ports); the acked file and the scripts dir are mounted in from the host.
checker_run() {
  $COMPOSE run --rm -v "$WORK:/chaos" -v "$PWD/scripts:/chaos_scripts" --entrypoint sh loadtest \
    -c "cd /app && mix run --no-start /chaos_scripts/chaos_checker.exs $*"
}

# Starts the acked-durability checker in the background for $1 seconds; sets CHECKER (pid).
start_checker() {
  say "starting acked-durability checker (${1}s window)"
  checker_run "produce $CHAOS_HOSTS $CHAOS_TOPIC $1 /chaos/acked.log" >"$WORK/checker.log" 2>&1 &
  CHECKER=$!
}

# Starts background multi-topic produce traffic for $1 seconds; sets TRAFFIC (pid).
start_traffic() {
  $COMPOSE run --rm loadtest \
    --host "$CHAOS_HOSTS" --scenario produce --connections 24 --batch 10 --topics 12 \
    --duration "$1" --warmup 5 --record-size 256 --json \
    >"$WORK/traffic.log" 2>&1 &
  TRAFFIC=$!
}

# Waits for the checker window (and background traffic if any) and requires at least one ack.
close_window() {
  say "waiting for the checker window to close"
  wait "$CHECKER" 2>/dev/null
  [ -n "${TRAFFIC:-}" ] && wait "$TRAFFIC" 2>/dev/null
  acked=$(wc -l < "$WORK/acked.log" 2>/dev/null | tr -d ' ')
  echo "checker acked $acked writes through the chaos (log tail:)"
  tail -3 "$WORK/checker.log"
  [ "${acked:-0}" -gt 0 ] || fail "checker acked nothing: no availability at any point"
}

# Invariant: every acknowledged write is still readable.
verify_acked() {
  say "invariant: every acknowledged write survived"
  checker_run "verify $CHAOS_HOSTS $CHAOS_TOPIC /chaos/acked.log" >"$WORK/verify.log" 2>&1
  tail -3 "$WORK/verify.log"
  if grep -q "VERIFY OK" "$WORK/verify.log"; then
    echo "durability invariant holds"
  else
    fail "acked writes were lost (see verify output above)"
  fi
}

# Invariant: the cluster ends fully healthy.
check_convergence() {
  say "invariant: final convergence"
  wait_healthy && echo "3/3 healthy" || fail "cluster is not fully healthy at the end"
}

# Invariant: a clean produce+fetch passes after the chaos.
check_clean_produce() {
  say "invariant: clean produce+fetch after the chaos"
  post=$($COMPOSE run --rm loadtest \
          --host "$CHAOS_HOSTS" --scenario produce --connections 12 --batch 10 --topics 6 \
          --duration 3 --warmup 3 --record-size 256 --json 2>/dev/null | grep -E '^\{' | tail -1)
  post_errs=$(echo "$post" | jq -r '[.errors,.dropped]|add' 2>/dev/null)
  post_recs=$(echo "$post" | jq -r .records_per_s 2>/dev/null)
  if [ "${post_errs:-1}" = "0" ] && [ "${post_recs:-0}" -gt 0 ]; then
    echo "post-chaos produce clean: ${post_recs} rec/s, 0 errors/drops"
  else
    fail "post-chaos produce not clean: $post"
  fi
}

# Prints the certification banner named $1 and exits 0/1 by the accumulated FAILED flag. On
# failure, dumps each node's recent logs first: `down` removes the containers, and losing the
# postmortem to the teardown cost a diagnosis round once.
finish() {
  if [ "$FAILED" != "0" ]; then
    say "postmortem: node logs (last 40 lines each)"
    for c in malachi-cluster-1 malachi-cluster-2 malachi-cluster-3; do
      echo "--- $c ---"
      docker logs --tail 40 "$c" 2>&1 | grep -iE "error|warn|crash|terminat" | tail -15
    done
  fi

  $COMPOSE down >/dev/null 2>&1
  say "result"
  if [ "$FAILED" != "0" ]; then
    echo "$1 FAILED (see FAIL lines above)"
    exit 1
  fi
  echo "$1 PASSED: every invariant held through all events"
}
