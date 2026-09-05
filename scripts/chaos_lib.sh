# Shared plumbing for the chaos certification harnesses (docker-chaos-test.sh and
# docker-storage-chaos.sh): compose lifecycle, health polling, the acked-durability checker, and
# the closing invariants. Callers export their env knobs (RF, MALACHI_DATA_ROOT, cpusets, ...)
# BEFORE sourcing this file; it defines functions and the WORK scratch dir, nothing runs yet.
#
# Contract: every check calls `fail` instead of exiting, so a run always reaches its summary;
# `finish <name>` prints the PASS/FAIL banner and exits with the accumulated status.
#
# Set CHAOS_RESULT_FILE to also have `finish` write the run as JSON: what was injected, what held,
# and on which commit. That file is what the published results page renders, so a drill that is not
# read by a human still leaves a record.

COMPOSE="docker compose -f docker-compose.cluster.yml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAILED=0
CHAOS_HOSTS="malachi1,malachi2,malachi3"
EVENTS=()
FAILURES=()

say() { printf '\n== %s ==\n' "$1"; }

fail() {
  echo "FAIL: $1"
  FAILED=1
  FAILURES+=("$1")
}

# Announces an injected fault AND records it. One call site per event rather than a banner plus a
# separate bookkeeping line, so the printed run and the recorded run can never disagree.
event() {
  EVENTS+=("$1")
  say "event $1"
}

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
  ACKED_WRITES="${acked:-0}"
  echo "checker acked $acked writes through the chaos (log tail:)"
  tail -3 "$WORK/checker.log"
  [ "${acked:-0}" -gt 0 ] || fail "checker acked nothing: no availability at any point"
}

# Invariant: every acknowledged write is still readable.
verify_acked() {
  say "invariant: every acknowledged write survived"
  # Let the cluster reconverge FIRST. The invariant under test is that an acknowledged write survived,
  # not that it is visible within seconds of a rolling restart, and scanning a cluster that is still
  # coming back measures the second while claiming to measure the first: that is what made this drill
  # fail roughly one run in four with nothing wrong in the broker. Waiting cannot hide a real loss,
  # because re-replication only copies records that exist; nothing can invent one back. A cluster that
  # never converges is still reported, by check_convergence, which owns that invariant.
  wait_healthy || echo "cluster not fully healthy before the scan; verifying anyway"

  checker_run "verify $CHAOS_HOSTS $CHAOS_TOPIC /chaos/acked.log" >"$WORK/verify.log" 2>&1
  # The counts line comes before the verdict, so a fixed tail of 3 used to cut it off exactly when it
  # mattered most.
  grep -E "^(acked=|VERIFY|[0-9]+ values not visible|missing |first missing)" "$WORK/verify.log" | tail -6
  # The checker dumps the segment map on any non-OK verdict. Printed separately from the tail above so
  # a long map cannot push the verdict itself out of view, and only when there is one: this is the
  # evidence a rerun cannot recover, because by then the cluster has moved on.
  if grep -q "^SEGMENT " "$WORK/verify.log"; then
    echo "segment map at the time of the failure:"
    grep "^SEGMENT " "$WORK/verify.log"
  fi
  if grep -q "VERIFY OK" "$WORK/verify.log"; then
    echo "durability invariant holds"
  elif grep -q "VERIFY INCONCLUSIVE" "$WORK/verify.log"; then
    # The checker could not finish reading the topic, so it never established whether anything is
    # missing. Still a failed run, since the invariant went unverified, but calling it lost data would
    # be an accusation the evidence does not support.
    fail "the durability invariant could not be verified (see verify output above); this is not evidence of data loss"
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
    POST_CHAOS_RECORDS_PER_S="$post_recs"
    echo "post-chaos produce clean: ${post_recs} rec/s, 0 errors/drops"
  else
    fail "post-chaos produce not clean: $post"
  fi
}

# A JSON array of the arguments, escaped by jq rather than by hand. Event and failure text is free
# form (it carries node names, paths and error strings), and one quote in any of it would produce a
# file no parser accepts, which is a poor way for a published result to fail.
# One array element per argument, whatever the argument contains. The previous form piped the
# arguments through `jq -R`, which reads a LINE at a time, so a failure message spanning two lines
# became two failures: the recorded result overstated how many things broke and split the sentence
# describing each. `--args` passes them as arguments rather than as text to be re-split.
json_array() {
  jq -n -c '$ARGS.positional' --args ${1+"$@"}
}

# When and from what, so a recorded run stands on its own. Same shape the two load generators write,
# so one renderer covers all three. The version is read from mix.exs the way the release workflow
# reads it; git is optional (a checkout is not guaranteed) and simply leaves the field empty.
chaos_meta() {
  ref="$(git rev-parse --short HEAD 2>/dev/null || true)"
  if [ -n "$ref" ] && [ -n "$(git status --porcelain 2>/dev/null)" ]; then ref="${ref}-dirty"; fi

  jq -n \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg command "${CHAOS_COMMAND:-$0}" \
    --arg git_ref "$ref" \
    --arg git_ref_date "$(git show -s --format=%cI HEAD 2>/dev/null || true)" \
    --arg malachi_version "$(grep '@version' mix.exs | head -1 | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')" \
    --arg cpu "$(uname -m)" \
    --arg os "$(uname -s) $(uname -r)" \
    --argjson cores "$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo null)" \
    '{timestamp: $timestamp, command: $command, git_ref: $git_ref, git_ref_date: $git_ref_date,
      malachi_version: $malachi_version, hardware: {cpu: $cpu, cores: $cores, os: $os}}'
}

# The measured invariants, null where a harness does not run that check (the storage and config
# drills certify other things). null says not measured; 0 would say measured and empty.
chaos_invariants() {
  jq -n \
    --argjson acked "${ACKED_WRITES:-null}" \
    --argjson post "${POST_CHAOS_RECORDS_PER_S:-null}" \
    '{acked_writes: $acked, post_chaos_records_per_s: $post}'
}

# Writes the run as JSON when CHAOS_RESULT_FILE is set, and stays silent otherwise so an interactive
# drill behaves exactly as before.
write_result() {
  [ -n "${CHAOS_RESULT_FILE:-}" ] || return 0
  mkdir -p "$(dirname "$CHAOS_RESULT_FILE")"

  verdict=passed
  [ "$FAILED" != "0" ] && verdict=failed

  jq -n \
    --argjson meta "$(chaos_meta)" \
    --arg certification "$1" \
    --arg verdict "$verdict" \
    --argjson replication_factor "${RF:-null}" \
    --argjson events "$(json_array ${EVENTS[@]+"${EVENTS[@]}"})" \
    --argjson invariants "$(chaos_invariants)" \
    --argjson failures "$(json_array ${FAILURES[@]+"${FAILURES[@]}"})" \
    '{meta: $meta, certification: $certification, verdict: $verdict,
      replication_factor: $replication_factor, events: $events,
      invariants: $invariants, failures: $failures}' > "$CHAOS_RESULT_FILE"

  echo "result written to $CHAOS_RESULT_FILE"
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
  # Before the exit below, not after: a failed drill is exactly the run whose record is worth having.
  write_result "$1"
  if [ "$FAILED" != "0" ]; then
    echo "$1 FAILED (see FAIL lines above)"
    exit 1
  fi
  echo "$1 PASSED: every invariant held through all events"
}
