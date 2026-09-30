# Shared plumbing for the chaos certification harnesses (the docker-*-chaos*.sh drills): compose
# lifecycle, health polling, node rolls, operator tasks inside a node, the acked-durability checker,
# and the closing invariants. Callers export their env knobs (RF, MALACHI_DATA_ROOT, cpusets, ...)
# BEFORE sourcing this file; it defines functions and the WORK scratch dir, nothing runs yet.
#
# Contract: every check calls `fail` instead of exiting, so a run always reaches its summary;
# `finish <name>` prints the PASS/FAIL banner and exits with the accumulated status.
#
# Set CHAOS_RESULT_FILE to also have `finish` write the run as JSON: what was injected, what held,
# and on which commit. That file is what the published results page renders, so a drill that is not
# read by a human still leaves a record.

COMPOSE="docker compose -f docker-compose.cluster.yml"
# The scratch dir is bind-mounted into the checker container (`-v "$WORK:/chaos"`), so it must live where the
# Docker host can see it. A bare `mktemp -d` does not guarantee that: on macOS it ignores TMPDIR and picks
# /var/folders, which Docker Desktop need not share with its VM. The checker then wrote acked.log inside the
# VM, `close_window` read nothing, and the drill failed with "checker acked nothing" while every write had
# been acknowledged. Every caller cds to the repo root before sourcing this file, and the repo sits on a path
# the host shares; CHAOS_WORK_ROOT overrides it.
CHAOS_WORK_ROOT="${CHAOS_WORK_ROOT:-$PWD/tmp/chaos}"
mkdir -p "$CHAOS_WORK_ROOT" || { echo "cannot create $CHAOS_WORK_ROOT"; exit 1; }
WORK="$(mktemp -d "$CHAOS_WORK_ROOT/work.XXXXXX")" || { echo "cannot create a scratch dir under $CHAOS_WORK_ROOT"; exit 1; }
trap 'rm -rf "$WORK"' EXIT
# Where a failed check keeps what it saw. Beside WORK rather than inside it, so the EXIT trap leaves it, and
# named by time and commit so runs never overwrite each other. Created only when something is kept.
EVIDENCE_DIR="$CHAOS_WORK_ROOT/evidence/$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
EVIDENCE_KEPT=""
# Captures kept so far in this run, numbering their directories in order.
EVIDENCE_SEQ=0
# Extra `docker compose run` arguments for the next checker_run (mounts, --no-deps). Callers set and reset it.
CHECKER_RUN_ARGS=()
# Images this run built for itself (the upgrade drill's OLD and NEW), removed by `finish` once no container
# uses them. Images a compose build tagged for the project are not listed: every drill shares those.
OWN_IMAGES=()
# The image each node runs, malachi1 to malachi3, for a drill that runs them on different images (the upgrade
# drill). Empty means one build for all three, which is what every other drill starts: compose tags that
# build once per service, so the tags themselves cannot say whether the nodes run the same code.
NODE_IMAGES=()
# Set once this run has brought the cluster up, so a second start (the storage drill's phase 2) replaces its
# own cluster instead of being refused as someone else's.
OWN_CLUSTER=0
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

# The compose project and its container names are shared with every other cluster harness (the benchmark in
# benchmark/docker-cluster.sh brings up the same file), and start_cluster begins with `down -v`: starting a drill
# while another harness runs would destroy that run's cluster and volumes, and fail both. So a RUNNING cluster
# that this run did not start is refused. A stopped leftover is not refused, the `down -v` is what clears it.
# The refusal goes through `abort_run`, so a drill that names its certification still writes its result and
# removes the images it built before getting here.
#
# The host load is printed too: a drill that shares the machine with a CPU-heavy job can fail for reasons that
# are not in the broker, and the run should say so on its face.
preflight_cluster() {
  if [ "$OWN_CLUSTER" != "1" ]; then
    running=$(docker ps --filter "name=malachi-cluster-" --format '{{.Names}}' | sort | tr '\n' ' ')
    if [ -n "$running" ]; then
      echo "refusing to start: another cluster is already running (${running% })."
      echo "Another harness may be using it. If it is a leftover, remove it with: $COMPOSE down -v"
      abort_run "another cluster is already running (${running% })"
    fi
  fi
  echo "host load:$(uptime | sed 's/.*load average/ load average/')"
}

# Fresh cluster on fresh volumes; sets NET to the compose network name for partition events.
start_cluster() {
  say "starting 3-node RF=${RF} cluster (fresh persistent volumes)"
  preflight_cluster
  OWN_CLUSTER=1
  $COMPOSE down -v >/dev/null 2>&1
  $COMPOSE up -d --force-recreate malachi1 malachi2 malachi3 >"$WORK/up.log" 2>&1
  wait_healthy || { cat "$WORK/up.log"; abort_run "cluster never converged"; }
  NET=$(docker inspect malachi-cluster-1 --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
  echo "cluster healthy; network: $NET"
}

# Ends a run that cannot go on, recording $1 as its failure. A drill that named its certification in
# CHAOS_CERTIFICATION goes through `finish`, so its result is still written (a nightly job then publishes that
# the run failed rather than nothing) and the images it built are removed; any other drill just stops.
abort_run() {
  fail "$1"
  [ -n "${CHAOS_CERTIFICATION:-}" ] && finish "$CHAOS_CERTIFICATION"
  exit 1
}

# Runs the checker/topology script inside the compose network (the cluster publishes no host
# ports); the acked file and the scripts dir are mounted in from the host.
checker_run() {
  $COMPOSE run --rm ${CHECKER_RUN_ARGS[@]+"${CHECKER_RUN_ARGS[@]}"} -v "$WORK:/chaos" -v "$PWD/scripts:/chaos_scripts" --entrypoint sh loadtest \
    -c "cd /app && mix run --no-start /chaos_scripts/chaos_checker.exs $*"
}

# The named volume mounted at /data in container $1, and the image it runs.
data_volume_of() { docker inspect "$1" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}'; }
image_of() { docker inspect "$1" --format '{{.Config.Image}}'; }

# The acknowledged writes so far in this window: one line per ack in the checker's file, 0 before the first.
acked_count() { wc -l < "$WORK/acked.log" 2>/dev/null | tr -d ' '; }

# Requires the acked count to grow past $1: availability held through the step named $2. Polled for up to
# PROGRESS_TIMEOUT_S rather than read once, because a node reports healthy as soon as its HTTP listener
# answers, which can be before its broker serves: a single read right after `wait_healthy` catches that gap
# and calls it an outage. Availability measured this way is what the NorthGuard certification checks through
# every admin operation (transcript 581), and it is why no readiness endpoint stands in for it here.
require_progress() {
  waited=0
  while :; do
    now=$(acked_count)
    if [ "${now:-0}" -gt "$1" ]; then
      echo "acks kept flowing through $2 ($1 -> $now)"
      return 0
    fi
    [ "$waited" -ge "${PROGRESS_TIMEOUT_S:-60}" ] && break
    sleep 2
    waited=$((waited + 2))
  done
  fail "no produce was acknowledged through $2 (stuck at ${now:-0})"
  return 1
}

# Saves container $1's log under $WORK/logs, numbered in order, with a line in index.txt naming the step $2.
# Recreating a container (`up -d` after its image or environment changed) deletes its log with it, and the log
# of the build being replaced is the only record of what that build did. `finish` keeps the directory on a
# failed run. A log that cannot be read is reported and is not itself a failure: the container may be gone.
LOG_SEQ=0
capture_logs() {
  mkdir -p "$WORK/logs"
  LOG_SEQ=$((LOG_SEQ + 1))
  seq=$(printf '%02d' "$LOG_SEQ")
  echo "$seq $1 $2" >> "$WORK/logs/index.txt"
  # The image the container ran, so a drill that runs two builds can tell which one wrote each log.
  echo "$seq $(image_of "$1" 2>/dev/null)" >> "$WORK/logs/images.txt"
  docker logs "$1" >"$WORK/logs/$seq-$1.log" 2>&1 || echo "could not read the log of $1"
}

# Replaces compose service $1 (malachiN) with whatever the compose files now say for it, image and environment
# alike, and certifies the step named $2: the replaced container's log is kept, the cluster served while it was
# down, it is back to 3/3 healthy, and it serves again. Returns non-zero, after recording the failure, when any
# of that did not hold.
#
# The node is stopped first and only then recreated, so the outage is a window of its own that can be measured:
#   * acks grew from a count taken once the node had stopped, polled while it stays stopped: the other two, a
#     quorum, served without it. Measured across the recreate instead, an ack from before the stop or from
#     after the node came back would pass it, and a build that stops serving whenever one node is down with it;
#   * acks grew from a count taken once the node was back: the cluster still serves after the recreate;
#   * a produce through the replaced node alone was clean: the replaced node itself serves. Acks need only a
#     quorum and the checker stays on one host, so neither count above can see a node that answers its health
#     check and never serves.
# The log is read once the container has stopped, so it holds the whole life of the build being replaced. The
# node is brought back whatever the outage measure said: left down, it would take the next step's node below a
# quorum with it, and every check after the first failure would fail for that reason instead of its own.
roll_node() {
  $COMPOSE stop "$1" >/dev/null 2>&1 || { fail "compose could not stop $1 ($2)"; return 1; }
  capture_logs "malachi-cluster-${1#malachi}" "$2"
  down=$(acked_count)
  served_while_down=1
  require_progress "${down:-0}" "$2, while $1 was down" && served_while_down=0
  $COMPOSE up -d "$1" >/dev/null 2>&1 || { fail "compose could not recreate $1 ($2)"; return 1; }
  wait_healthy || { fail "the cluster did not reconverge after $2"; return 1; }
  [ "$served_while_down" = 0 ] || return 1
  rejoined=$(acked_count)
  require_progress "${rejoined:-0}" "$2, after the node rejoined" || return 1
  if serves_through "$1"; then
    echo "$1 served a clean produce of its own after $2"
  else
    fail "$1 did not serve a clean produce of its own after $2"
    return 1
  fi
}

# Runs an operator mix task inside node $1, targeting that same node. The cluster uses short names, so
# the task has to run from a VM that is itself short-named with the same cookie; a separate long-named
# container could not connect to it at all.
node_task() {
  idx=$1
  shift
  docker exec "malachi-cluster-$idx" sh -c \
    "cd /app && elixir --sname chaoscli --cookie malachi_bench -S mix $* --node malachi@malachi$idx" 2>&1
}

# Re-sharding is lease-gated and the lease is elected, so an operator has to issue it on the node that
# currently holds it; refusing elsewhere with "try another node" is the task's documented behaviour.
# Walking the three nodes is what an operator does, and it keeps a drill from failing on an election
# that simply landed somewhere other than node 1.
reshard_on_lease_holder() {
  for idx in 1 2 3; do
    out=$(node_task "$idx" "malachi.reshard --to $1")
    status=$?
    if [ $status -eq 0 ]; then
      echo "reshard ran on node $idx (the lease holder)"
      return 0
    fi
    if ! echo "$out" | grep -q "does not hold the cluster lease"; then
      echo "$out" | sed 's/^/    /'
      return 1
    fi
  done
  echo "    no node accepted the reshard; none reported holding the lease"
  return 1
}

# The ring as the cluster records it, through node 1. The query is linearizable, so ra routes it to the
# leader wherever that is. Retried, because right after a restart the ring store may need a moment to elect
# before it can answer, and an unreadable store is not the same as a changed ring.
ring_show() {
  for _ in $(seq 1 20); do
    out=$(node_task 1 "malachi.ring --show")
    if echo "$out" | grep -q "durable ring: version"; then
      echo "$out"
      return 0
    fi
    sleep 3
  done
  echo "$out"
  return 1
}

# How many vnodes the recorded ring has, or nothing when it could not be read.
ring_vnode_count() {
  ring_show | sed -n 's/.*, \([0-9][0-9]*\) vnodes .*/\1/p' | head -1
}

# One md5 line per file of the node's data on volume $1, sorted by path, read through image $2 with the volume
# mounted read-only: the log directory and the control plane's (`ra`) beside it, since a build that refuses the
# data has to leave both as they were. Fails when there is no log directory, which is no node's volume; a node
# that never started `ra` has no control-plane directory, and that is left out rather than failed. The node that
# owns the volume must be stopped for the answer to mean anything.
volume_md5() {
  docker run --rm -v "$1:/data:ro" --entrypoint sh "$2" -c \
    "cd /data && test -d malachi_log && find malachi_log \$(test -d malachi_ra && echo malachi_ra) -type f -exec md5sum {} + | sort -k 2"
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
  # The checker dumps the page trace and the segment map on any non-OK verdict. Printed separately
  # from the tail above so neither can push the verdict itself out of view, and only when there is
  # one: this is the evidence a rerun cannot recover, because by then the cluster has moved on.
  #
  # The SCAN SKIPPED line comes first because it is the finding, not the raw material: it separates a
  # scan that ran out of time (raise the budget) from a cursor that advanced past records the server
  # had not made visible (a different bug entirely), which the verdict alone could never distinguish.
  grep "^SCAN SKIPPED" "$WORK/verify.log" || true
  if grep -q "^PAGE " "$WORK/verify.log"; then
    echo "scan trace, one line per fetch:"
    grep "^PAGE " "$WORK/verify.log"
  fi
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
  elif grep -q "VERIFY FAILED: .* unreachable" "$WORK/verify.log"; then
    # Two full reads skipped the same block at the same page boundary. The values were acknowledged
    # and are not readable through the API, which is a failure in its own right and a different bug
    # from a lost write: the records may well be on disk, under a segment the scan cannot reach.
    # Checked before the generic branch so it is never reported as loss.
    fail "acknowledged writes are unreachable: the scan skipped over them (see the page trace above)"
  else
    fail "acked writes were lost (see verify output above)"
  fi
}

# Invariant: the cluster ends fully healthy, and its control plane converged. Three healthy nodes is not
# enough on its own: members whose Raft groups diverged answer health checks all the same (issue #188), so
# every group is compared across its members too.
check_convergence() {
  say "invariant: final convergence"
  if ! wait_healthy; then
    fail "cluster is not fully healthy at the end"
    return 1
  fi
  echo "3/3 healthy"
  check_control_plane all
}

# Runs the checker's control-plane mode from a VM short-named with the cluster's cookie, the only kind of
# VM a short-named cluster accepts a connection from. --no-deps: the cluster is already up, and a
# dependency check must not restart a node the drill stopped on purpose.
control_plane_run() {
  $COMPOSE run --rm --no-deps -v "$PWD/scripts:/chaos_scripts" --entrypoint sh loadtest \
    -c "cd /app && elixir --sname chaoschk --cookie malachi_bench -S mix run --no-start /chaos_scripts/chaos_checker.exs control-plane $*"
}

# Invariant: every Raft group holds the same state on every member, compared at equal applied indexes
# (ChaosChecker.ControlPlane). $1 is the projection: `all`, a digest of each whole state, which means
# something only between members on one build, so it is refused while NODE_IMAGES says the nodes run
# different images; or `topics`, the part every build keeps, for a cluster caught mid-upgrade.
check_control_plane() {
  projection="$1"
  if [ "$projection" = all ] && [ ${#NODE_IMAGES[@]} -gt 0 ]; then
    images=$(printf '%s\n' "${NODE_IMAGES[@]}" | sort -u | wc -l | tr -d ' ')
    if [ "$images" != "1" ]; then
      fail "harness error: whole control-plane states are comparable on one image only, and the nodes run $images (${NODE_IMAGES[*]})"
      return 1
    fi
  fi

  control_plane_run "malachi@malachi1,malachi@malachi2,malachi@malachi3 --project $projection" \
    >"$WORK/control-plane.log" 2>&1
  grep "^CONTROL-PLANE" "$WORK/control-plane.log" | tail -12
  if grep -q "^CONTROL-PLANE OK" "$WORK/control-plane.log"; then
    echo "every control-plane group holds the same state on every member ($projection)"
    return 0
  elif grep -q "^CONTROL-PLANE DIVERGED" "$WORK/control-plane.log"; then
    fail "the control plane diverged: members hold different states at the same index (see the MISMATCH lines above)"
  elif grep -q "^CONTROL-PLANE UNSETTLED" "$WORK/control-plane.log"; then
    fail "the control plane never settled on every member (see the PENDING lines above)"
  else
    fail "the control-plane check did not run: $(tail -3 "$WORK/control-plane.log" | tr '\n' ' ')"
  fi
  return 1
}

# A short produce through the hosts in $1 only. Prints the load test's JSON summary and succeeds when it
# counted no error and no drop and moved records. --no-deps: the loadtest service depends on every node being
# healthy, and a node a drill holds down on purpose would otherwise be started again by this very check.
produce_through() {
  post=$($COMPOSE run --rm --no-deps loadtest \
          --host "$1" --scenario produce --connections 12 --batch 10 --topics 6 \
          --duration 3 --warmup 3 --record-size 256 --json 2>/dev/null | grep -E '^\{' | tail -1)
  echo "$post"
  post_errs=$(echo "$post" | jq -r '[.errors,.dropped]|add' 2>/dev/null)
  post_recs=$(echo "$post" | jq -r .records_per_s 2>/dev/null)
  [ "${post_errs:-1}" = "0" ] && [ "${post_recs:-0}" -gt 0 ]
}

# Retries a short produce through the hosts in $1 alone until one is clean, for up to PROGRESS_TIMEOUT_S of wall
# clock. A node that answers its health check can take a few more seconds to serve, and what a step certifies
# is that availability comes back, not that it is there on the first try. Measured with SECONDS rather than by
# counting the pauses, since each try is a load test run of several seconds itself.
serves_through() {
  deadline=$((SECONDS + ${PROGRESS_TIMEOUT_S:-60}))
  while :; do
    produce_through "$1" >/dev/null && return 0
    [ "$SECONDS" -ge "$deadline" ] && return 1
    sleep 2
  done
}

# Invariant: a clean produce+fetch passes after the chaos, through the hosts in $1 (default: all three).
# Retried like any other step (serves_through): what it certifies is that availability came back.
check_clean_produce() {
  say "invariant: clean produce+fetch after the chaos"
  if serves_through "${1:-$CHAOS_HOSTS}"; then
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
    --arg evidence_dir "$EVIDENCE_KEPT" \
    --argjson details "${CHAOS_DETAILS:-null}" \
    '{meta: $meta, certification: $certification, verdict: $verdict,
      replication_factor: $replication_factor, events: $events,
      invariants: $invariants, failures: $failures,
      evidence_dir: (if $evidence_dir == "" then null else $evidence_dir end),
      details: $details}' > "$CHAOS_RESULT_FILE"

  echo "result written to $CHAOS_RESULT_FILE"
}

# Prints the certification banner named $1 and exits 0/1 by the accumulated FAILED flag. On
# failure, dumps each node's recent logs first: `down` removes the containers, and losing the
# postmortem to the teardown cost a diagnosis round once.
finish() {
  if [ "$FAILED" != "0" ]; then
    # The nodes of the compose project are this run's only once it started them: a run that stopped before (a
    # refused preflight, an image that did not build) would print another cluster's logs as its own.
    if [ "$OWN_CLUSTER" = "1" ]; then
      say "postmortem: node logs (matching lines with their following context)"
      for c in malachi-cluster-1 malachi-cluster-2 malachi-cluster-3; do
        echo "--- $c ---"
        # -A 20, and it is the whole point of this line. An Erlang crash report puts the reason and
        # the stack trace on the lines AFTER the one that matches, and not one of those lines contains
        # error, warn, crash or terminating. Filtering line by line printed the first two lines of a
        # MatchError and swallowed the trace that says where it came from, which cost a diagnosis
        # round on a failure that then did not reproduce. The wider --tail is so a trace near the end
        # of the window is not cut off before grep ever sees it.
        docker logs --tail 200 "$c" 2>&1 | grep -iE -A 20 "error|warn|crash|terminat" | tail -80
      done
    fi

    # The logs `capture_logs` saved belong to containers that no longer exist, so the scratch dir is their
    # only copy and the EXIT trap is about to remove it.
    if [ -d "$WORK/logs" ]; then
      EVIDENCE_SEQ=$((EVIDENCE_SEQ + 1))
      kept="$EVIDENCE_DIR/$(printf '%02d' "$EVIDENCE_SEQ")-node-logs"
      mkdir -p "$kept" && cp "$WORK/logs/"* "$kept/" && EVIDENCE_KEPT="$EVIDENCE_DIR" &&
        echo "saved container logs kept in $kept"
    fi
  fi

  # Only a cluster this run started: a run that ends before `start_cluster` (an image that did not build)
  # shares the compose project with whatever other run may be up, and must leave it alone.
  [ "$OWN_CLUSTER" = "1" ] && $COMPOSE down >/dev/null 2>&1
  # After the containers are gone, since an image a container still uses cannot be removed.
  if [ ${#OWN_IMAGES[@]} -gt 0 ]; then
    docker image rm "${OWN_IMAGES[@]}" >/dev/null 2>&1 || echo "could not remove the images ${OWN_IMAGES[*]}"
  fi
  say "result"
  # Before the exit below, not after: a failed drill is exactly the run whose record is worth having.
  write_result "$1"
  if [ "$FAILED" != "0" ]; then
    echo "$1 FAILED (see FAIL lines above)"
    exit 1
  fi
  echo "$1 PASSED: every invariant held through all events"
}
