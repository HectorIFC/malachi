#!/usr/bin/env bash
# Rolling-upgrade certification: the deploy the build / roll back the build operations of the NorthGuard
# certification pipeline (transcript 576-582), on the real 3-node RF=3 cluster, with the acked-durability
# checker and background traffic producing through every step (issue #196).
#
# Two images. OLD is a release, built from its own tree (git archive of OLD_REF; by default the newest release
# whose code differs from this tree's, see resolve_old_ref). NEW is the working tree with
# test/support/upgrade_canary.patch applied, so it has what no release has yet: a capability and a cluster
# flag (upgrade_canary), a metadata command at the next machine version ({:canary_note, topic, note} at NEW's
# @code_version, read from the patched tree), a
# replication cast older builds do not know, and a data format (2) its flag raises the marker to. Every node
# keeps its volume across every swap.
#
# Phase 1, roll forward under the machine version pin (MALACHI_RA_MACHINE_VERSION = OLD's version):
#   each node goes OLD -> NEW. With node 3 on NEW and nodes 1 and 2 on OLD, the drill certifies that the
#   flag is refused naming the OLD nodes (#193), that the canary command is refused (#188) and leaves the
#   topic metadata identical on all three replicas and out of node 3's own cache, and that an OLD node
#   counted the unknown cast instead of crashing (#187). With all three on NEW, and when OLD can bring its
#   vnode members back after a restart (choose_control_plane), the metadata ring is split from 4 vnodes to 5,
#   so that phase 2 has to bring up a vnode created by a split on the OLD build (#242).
# Phase 2, roll back before the flip: every node goes back to OLD, still under load. This must succeed.
# Phase 3, roll forward, finalize, flip, and a rollback that must be refused: every node goes to NEW under
#   the pin, then the pin is removed node by node, which moves the control plane to NEW's version, where the
#   canary command applies. The flag is switched on, which raises every data directory to format 2. Node 3 is
#   then started on OLD: it must exit 78 with the format marker's refusal (#189), its data directory (segment
#   files and control-plane state) must be byte for byte what it was, and the other two must keep serving.
#   Node 3 then goes back to NEW.
#
# Invariants: every write acknowledged in each phase is readable (one checker window and one topic per
# phase), every Raft group holds the same state on every member at the end of each phase, availability holds
# through every node swap (`roll_node`), no process crashed except an OLD one in a way that release is known to
# (check_no_crash, OLD_KNOWN_CRASHES), and a clean produce passes at the end.
#
# Knobs: OLD_REF (a release at 0.14.2 or later, 0.16.1 for a sharded run, which no release is before the first
# with the fix for #136); OLD_PATCH and NEW_PATCH, an extra
# patch applied to either tree, for the fails-before runs (a run with either is recorded as such in the
# result); PHASE1_WINDOW_S, PHASE2_WINDOW_S and PHASE3_WINDOW_S; UPGRADE_CANARY_PATCH, the canary patch (a
# test seam).
#
# Usage: scripts/docker-upgrade-chaos.sh
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1
export SRV_CPUSET="${SRV_CPUSET:-2,3,4,5,6,7}" LT_CPUSET="${LT_CPUSET:-0,1}"
export RF=3
export MALACHI_DATA_ROOT=/data
# Segment preallocation at the production default, as in the other drills: a restart must meet the tail of
# zeros a real deployment leaves behind.
export MALACHI_SEGMENT_PREALLOC_BYTES="${MALACHI_SEGMENT_PREALLOC_BYTES:-67108864}"
# The control plane's shape is chosen once OLD is known (choose_control_plane): sharded, and split in phase 1,
# when OLD can bring its vnode members back after a restart; unsharded otherwise.
export MALACHI_LOG_VNODES=1
SPLIT_TO=""
VNODE_RULE=""
PHASE1_WINDOW_S="${PHASE1_WINDOW_S:-480}"
PHASE2_WINDOW_S="${PHASE2_WINDOW_S:-360}"
PHASE3_WINDOW_S="${PHASE3_WINDOW_S:-900}"
UPGRADE_CANARY_PATCH="${UPGRADE_CANARY_PATCH:-test/support/upgrade_canary.patch}"
OLD_PATCH="${OLD_PATCH:-}"
NEW_PATCH="${NEW_PATCH:-}"
# The oldest release this drill can take as OLD, one per shape of the control plane (choose_control_plane).
# Unsharded: the first release with every gate the canary exercises (capabilities and cluster flags, the machine
# version pin, the format marker) and every control-plane group NEW starts, the storage policy store included:
# a NEW node starts that group, and on an OLD without it the group is never running on the OLD nodes, so the
# mixed cluster's control plane could never compare equal. Sharded: the first that brings up a vnode created
# by a split on a node that did not boot with it (#242), which a sharded phase 2 needs.
OLD_FLOOR_UNSHARDED=0.14.2
OLD_FLOOR_SHARDED=0.16.1
CHAOS_TOPIC=chaos_upgrade_p1
# Named before the library is sourced, so a run it has to abort (a cluster that never converges) still ends in
# `finish`: the result is written and the images this run built are removed.
CHAOS_CERTIFICATION="UPGRADE AND ROLLBACK CERTIFICATION"
source "$(dirname "$0")/chaos_lib.sh"
COMPOSE="$COMPOSE -f docker-compose.upgrade.yml"

# A run that cannot start is still recorded, as failed, so the nightly job publishes that it did not certify
# anything rather than nothing at all. Exit 2 tells a mistake in how the drill was called from a failed run.
usage_error() {
  echo "usage error: $1"
  fail "usage error: $1"
  write_result "$CHAOS_CERTIFICATION"
  exit 2
}

# Whether version $1 is at least $2, both X.Y.Z.
version_at_least() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    split(a, x, "."); split(b, y, ".")
    for (i = 1; i <= 3; i++) { if (x[i] + 0 > y[i] + 0) exit 0; if (x[i] + 0 < y[i] + 0) exit 1 }
    exit 0
  }'
}

absolute() { (cd "$(dirname "$1")" && echo "$(pwd)/$(basename "$1")"); }

# Applies patch $2 to the tree in $1. GIT_CEILING_DIRECTORIES stops git from finding the repository the scratch
# dir sits in, which would make it apply the patch to the checkout instead of to the tree.
apply_to() {
  patch_file=$(absolute "$2")
  (cd "$1" && GIT_CEILING_DIRECTORIES="$(dirname "$1")" git apply "$patch_file") ||
    abort_run "the $3 patch ($2) does not apply to the $4 tree"
}

# --- the two images ---

# OLD is the release an operator would be upgrading from. Every merge to main is tagged as a release, so on
# main the newest tag usually holds the very code being certified, and rolling from it would certify a release
# against itself. So: when the working tree's code (lib, config, mix.lock) is the newest release's, OLD is the
# newest release whose code differs; when it differs from the newest, as on a branch ahead of the last
# release, OLD is the newest release.
# Whether this tree's code is exactly release $1's: lib, config and mix.lock. A file under lib or config that
# git does not track yet is code too, since the NEW image is built with it.
same_code_as() {
  git diff --quiet "$1" -- lib config mix.lock && [ -z "$(git ls-files --others --exclude-standard -- lib config)" ]
}

resolve_old_ref() {
  if [ -n "${OLD_REF:-}" ]; then
    OLD_RULE="given as OLD_REF"
  else
    newest=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null)
    [ -n "$newest" ] ||
      usage_error "no release tag is reachable from HEAD; fetch the tags (fetch-depth: 0) or set OLD_REF"
    if same_code_as "$newest"; then
      # Back through the releases until one whose code differs: a merge that changed only the docs is tagged
      # too, and the release before it holds this very code.
      OLD_REF=""
      for tag in $(git tag --list 'v*' --merged "$newest" --sort=-v:refname); do
        same_code_as "$tag" || { OLD_REF=$tag; break; }
      done
      [ -n "$OLD_REF" ] ||
        usage_error "this tree is release $newest and no older release with different code is reachable; set OLD_REF"
      OLD_RULE="the newest release whose code differs from this tree, which is release $newest's"
    else
      OLD_REF=$newest
      OLD_RULE="the newest release, whose code this tree changes"
    fi
  fi
  OLD_SHA=$(git rev-parse --verify -q "$OLD_REF^{commit}") ||
    usage_error "OLD_REF=$OLD_REF names no commit here; fetch the tags (fetch-depth: 0)"
  # From here on a run that stops early still records which release it was rolling from.
  record_details
}

prepare_contexts() {
  OLD_CTX="$WORK/ctx-old"
  NEW_CTX="$WORK/ctx-new"
  mkdir -p "$OLD_CTX" "$NEW_CTX"

  git archive "$OLD_SHA" | tar -x -C "$OLD_CTX" || usage_error "could not export $OLD_REF"
  OLD_VERSION=$(grep '@version' "$OLD_CTX/mix.exs" | head -1 | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')
  OLD_PIN=$(sed -n 's/^  @code_version \([0-9][0-9]*\)$/\1/p' "$OLD_CTX/lib/malachi/cluster/machine_version.ex")
  [ -n "$OLD_PIN" ] || usage_error "cannot read the machine version of $OLD_REF"
  choose_control_plane
  record_details
  if [ -n "$SPLIT_TO" ]; then
    version_at_least "$OLD_VERSION" "$OLD_FLOOR_SHARDED" ||
      usage_error "OLD_REF=$OLD_REF is release $OLD_VERSION; a sharded run needs $OLD_FLOOR_SHARDED or later, the first release that brings up a vnode created by a split (#242)"
  else
    version_at_least "$OLD_VERSION" "$OLD_FLOOR_UNSHARDED" ||
      usage_error "OLD_REF=$OLD_REF is release $OLD_VERSION; an unsharded run needs $OLD_FLOOR_UNSHARDED or later, the first release with every gate the canary exercises and every control-plane group NEW starts"
  fi

  # The working tree as it is, uncommitted changes included, minus what git ignores and what was deleted.
  git ls-files -z --cached --others --exclude-standard |
    while IFS= read -r -d '' f; do [ -e "$f" ] && printf '%s\0' "$f"; done |
    tar --null -T - -cf - | tar -xf - -C "$NEW_CTX" || abort_run "could not copy the working tree"

  apply_to "$NEW_CTX" "$UPGRADE_CANARY_PATCH" "upgrade canary" "NEW"
  # The machine version the canary command is introduced at, which the control plane reaches once the pin
  # is removed: read from NEW's own tree, as OLD_PIN is from OLD's, so a release that raises the version
  # needs only the patch to follow it.
  NEW_VERSION=$(sed -n 's/^  @code_version \([0-9][0-9]*\)$/\1/p' "$NEW_CTX/lib/malachi/cluster/machine_version.ex")
  [ -n "$NEW_VERSION" ] || usage_error "cannot read the machine version of the canary tree"
  [ -n "$NEW_PATCH" ] && apply_to "$NEW_CTX" "$NEW_PATCH" "NEW_PATCH" "NEW"
  [ -n "$OLD_PATCH" ] && apply_to "$OLD_CTX" "$OLD_PATCH" "OLD_PATCH" "OLD"
  echo "OLD is $OLD_REF (release $OLD_VERSION, machine version $OLD_PIN: $OLD_RULE); NEW is the working tree with the canary"
}

build_upgrade_images() {
  say "building the OLD and NEW images"
  prefix="${COMPOSE_PROJECT_NAME:-$(basename "$PWD")}-upgrade"
  OLD_IMAGE="$prefix:old"
  NEW_IMAGE="$prefix:new"
  # Every node starts on OLD. Set before any compose command, because the override requires an image for every
  # node and compose reads the whole file whatever service a command names.
  for n in 1 2 3; do place "$n" "$OLD_IMAGE"; done
  # Each from its own tree and its own Dockerfile, so OLD is built the way its release was.
  docker build -q -t "$OLD_IMAGE" -f "$OLD_CTX/benchmark/Dockerfile.loadtest" "$OLD_CTX" >"$WORK/build-old.log" 2>&1 ||
    { tail -20 "$WORK/build-old.log"; abort_run "the OLD image did not build"; }
  OWN_IMAGES=("$OLD_IMAGE")
  docker build -q -t "$NEW_IMAGE" -f "$NEW_CTX/benchmark/Dockerfile.loadtest" "$NEW_CTX" >"$WORK/build-new.log" 2>&1 ||
    { tail -20 "$WORK/build-new.log"; abort_run "the NEW image did not build"; }
  OWN_IMAGES=("$OLD_IMAGE" "$NEW_IMAGE")
  # The checker and the load generator stay on the working tree without the canary: they are clients.
  $COMPOSE build loadtest >"$WORK/build-loadtest.log" 2>&1 ||
    { tail -20 "$WORK/build-loadtest.log"; abort_run "the load generator image did not build"; }
}

# A node that comes back must restart its own members of the metadata vnodes: ra does not, and releases
# before the fix for #136 have nothing that does, so a rolling restart of a sharded cluster on one of them
# loses each vnode's quorum at its second node. Phase 2 restarts every node on OLD, so a sharded run needs an
# OLD that has the fix: then the cluster runs four vnodes and phase 1 splits the ring to five, which makes
# phase 2 bring up a vnode a split created (#242) on OLD. On an older OLD the cluster runs unsharded and
# nothing is split. Chosen from OLD's own tree, printed and recorded.
choose_control_plane() {
  if grep -q "def resume_local_vnodes" "$OLD_CTX/lib/malachi/application.ex" 2>/dev/null; then
    export MALACHI_LOG_VNODES=4
    SPLIT_TO=5
    VNODE_RULE="sharded: $OLD_REF resumes its vnode members after a restart"
  else
    export MALACHI_LOG_VNODES=1
    SPLIT_TO=""
    VNODE_RULE="unsharded, no split: $OLD_REF does not resume its vnode members after a restart (#136)"
  fi
  echo "control plane: $MALACHI_LOG_VNODES vnodes ($VNODE_RULE)"
}

# Node $1 runs image $2 from its next recreate on.
place() {
  export "UPGRADE_IMAGE_$1=$2"
  NODE_IMAGES[$(($1 - 1))]="$2"
}

# Swaps node $1 to image $2 and certifies the swap (`roll_node`).
swap() {
  place "$1" "$2"
  roll_node "malachi$1" "$3"
}

# --- phases ---

ACKED_BY_PHASE="{}"
ACKED_TOTAL=0
# What is known about OLD so far; `record_details` writes whatever is set, so each starts empty.
OLD_SHA=""
OLD_VERSION=""
OLD_RULE=""
OLD_PIN=""

begin_phase() {
  PHASE=$1
  PHASE_WINDOW_S=$2
  PHASE_STARTED=$(date +%s)
  CHAOS_TOPIC="chaos_upgrade_p$1"
  rm -f "$WORK/acked.log"
  start_checker "$2"
  start_traffic "$2"
  sleep 10
}

# Closes the phase's window and checks its invariants. A phase whose steps outlasted its window measured
# availability against a checker that had stopped, so that is a failure of its own, named, rather than a
# stuck count blamed on the cluster.
end_phase() {
  elapsed=$(($(date +%s) - PHASE_STARTED))
  [ "$elapsed" -lt "$PHASE_WINDOW_S" ] ||
    fail "phase $PHASE took ${elapsed}s, past its ${PHASE_WINDOW_S}s checker window; raise PHASE${PHASE}_WINDOW_S"
  close_window
  ACKED_TOTAL=$((ACKED_TOTAL + ${ACKED_WRITES:-0}))
  ACKED_BY_PHASE=$(echo "$ACKED_BY_PHASE" | jq -c --arg p "phase$PHASE" --argjson n "${ACKED_WRITES:-0}" '.[$p] = $n')
  verify_acked
  check_convergence
}

# Evaluates Elixir expression $2 in a VM beside node $1, short-named with the cluster's cookie.
node_eval() {
  docker exec "malachi-cluster-$1" sh -c "cd /app && elixir --sname chaoseval --cookie malachi_bench -e '$2'" 2>&1
}

node_atom() { echo "String.to_atom(~s(malachi@malachi$1))"; }

# Sends the canary command {:canary_note, topic, $2} through node $1's broker and prints the reply.
canary_note() {
  node_eval "$1" "IO.inspect(:rpc.call($(node_atom "$1"), GenServer, :call, [Malachi.LogBroker, {:canary_note, ~s($CHAOS_TOPIC), $2}]))"
}

# Sends {:canary_note, topic, $2} through node $1's broker and, in the same VM and right after the reply, reads
# the note that broker's own metadata cache then holds for the topic (nil for none). The broker re-seeds its
# cache from the replicas every second, so a note that entered it is gone a couple of seconds later: only a
# read this close to the command can see it. Prints `reply: ...` and `canary_note: ...`.
canary_and_cache() {
  node_eval "$1" "n = $(node_atom "$1"); r = :rpc.call(n, GenServer, :call, [Malachi.LogBroker, {:canary_note, ~s($CHAOS_TOPIC), $2}]); b = :rpc.call(n, :sys, :get_state, [Malachi.LogBroker]).broker; t = :rpc.call(n, Malachi.Cluster.DSRSM, :get_topic, [b.dsrsm, ~s($CHAOS_TOPIC)]); IO.inspect(r, label: :reply); IO.inspect(Map.get(t || %{}, :canary_note), label: :canary_note)"
}

# The effective machine version of node $1's cluster flag store, which every control-plane machine shares.
effective_version() {
  node_eval "$1" "IO.inspect(:rpc.call($(node_atom "$1"), :ra_counters, :counters, [{Malachi.LogClusterFlags, $(node_atom "$1")}, [:effective_machine_version]]))" |
    sed -n 's/.*effective_machine_version: \([0-9][0-9]*\).*/\1/p' | tail -1
}

# How many replication casts node $1 dropped as unknown, read from its /metrics like an operator would.
unexpected_casts() {
  docker exec -i "malachi-cluster-$1" sh -s 2>/dev/null <<'EOF' | sed -n 's/^malachi_unexpected_messages_total{server="replication",kind="cast"} \([0-9][0-9]*\).*/\1/p'
token=$(wget -qO- --header 'Content-Type: application/json' \
  --post-data '{"username":"admin","password":"admin123"}' http://127.0.0.1:4041/login |
  sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
wget -qO- --header "Authorization: Bearer $token" --header 'Accept: text/plain' http://127.0.0.1:4041/metrics
EOF
}

# The format recorded in node $1's data-directory marker.
marker_format() {
  docker exec "malachi-cluster-$1" sh -c 'cat /data/malachi_log/malachi.format' 2>/dev/null |
    sed -n 's/^format=\([0-9][0-9]*\)$/\1/p'
}

# Polls command $1 (a function name and arguments) until its output is $2, up to $3 tries two seconds apart.
await_value() {
  want=$1
  tries=$2
  shift 2
  for _ in $(seq 1 "$tries"); do
    got=$("$@")
    [ "$got" = "$want" ] && return 0
    sleep 2
  done
  return 1
}

mixed_cluster_checks() {
  event "p1: mixed cluster (node 3 on NEW, nodes 1 and 2 on OLD)"

  out=$(node_task 3 "malachi.flag enable upgrade_canary")
  if echo "$out" | grep -q "do not support that flag: malachi@malachi1, malachi@malachi2"; then
    echo "the canary flag is refused while nodes 1 and 2 run OLD"
  else
    fail "the canary flag was not refused while nodes 1 and 2 run OLD: $(echo "$out" | tail -2 | tr '\n' ' ')"
  fi

  # The reply is the Raft leader's, whichever member that is: an OLD leader does not know the command at all, a
  # NEW one knows it and refuses it at the pinned version. Either refusal is correct; what proves every member
  # refused it is the comparison below, not the wording of one reply.
  out=$(canary_and_cache 3 1)
  reply=$(echo "$out" | sed -n 's/^reply: //p' | tail -1)
  note=$(echo "$out" | sed -n 's/^canary_note: //p' | tail -1)
  if echo "$reply" | grep -qF -e "{:error, {:unknown_command, {:canary_note, 3}, $OLD_PIN}}" \
    -e "{:error, {:unsupported_command, {:canary_note, 3}, $NEW_VERSION, $OLD_PIN}}"; then
    echo "the canary command is refused at machine version $OLD_PIN"
  else
    fail "the canary command was not refused at machine version $OLD_PIN: $(echo "$out" | tail -2 | tr '\n' ' ')"
  fi
  # The command the NEW member knows and the OLD ones do not must leave the topic metadata the same on all three
  # replicas, and out of the NEW node's own metadata cache, which applies what the leader answers.
  check_control_plane topics
  [ "$note" = nil ] && echo "node 3's metadata cache did not take the refused command" ||
    fail "node 3's metadata cache took the command every replica refused (canary_note=${note:-unreadable})"

  if await_value_positive unexpected_casts 1; then
    echo "OLD node 1 dropped $(unexpected_casts 1) unknown replication casts and kept serving"
  else
    fail "OLD node 1 counted no unknown replication cast: the canary never reached an OLD node"
  fi
}

# Polls "$@" until it prints a number above zero, up to 30 tries.
await_value_positive() {
  for _ in $(seq 1 30); do
    got=$("$@")
    [ "${got:-0}" -gt 0 ] 2>/dev/null && return 0
    sleep 2
  done
  return 1
}

split_ring() {
  event "p1: splitting the metadata ring from $MALACHI_LOG_VNODES to $SPLIT_TO vnodes on NEW"
  before=$(acked_count)
  reshard_on_lease_holder "$SPLIT_TO" || fail "the split did not complete"
  require_progress "$before" "the split"
  check_ring
}

check_ring() {
  count=$(ring_vnode_count)
  [ "$count" = "$SPLIT_TO" ] && echo "the ring has $SPLIT_TO vnodes" ||
    fail "expected a $SPLIT_TO-vnode ring, got '${count:-none}'"
}

refused_rollback() {
  event "p3: node 3 started on OLD after the flip must refuse its data directory"
  before=$(acked_count)
  capture_logs malachi-cluster-3 "p3 NEW node 3 before the refused rollback"
  $COMPOSE stop malachi3 >/dev/null 2>&1
  volume=$(data_volume_of malachi-cluster-3)
  volume_md5 "$volume" "$NEW_IMAGE" >"$WORK/md5-before.txt" || fail "could not read node 3's data before the rollback"

  place 3 "$OLD_IMAGE"
  export UPGRADE_RESTART_3=on-failure:3
  $COMPOSE up -d malachi3 >/dev/null 2>&1 || fail "compose could not start node 3 on OLD"

  refused=""
  for _ in $(seq 1 60); do
    state=$(docker inspect malachi-cluster-3 --format '{{.State.Status}} {{.State.ExitCode}} {{if .State.Health}}{{.State.Health.Status}}{{end}}')
    case "$state" in
      *" healthy") break ;;
      *" 78"*) refused=yes; break ;;
    esac
    sleep 2
  done
  $COMPOSE stop malachi3 >/dev/null 2>&1
  logs=$(docker logs malachi-cluster-3 2>&1)
  capture_logs malachi-cluster-3 "p3 OLD node 3, refused"

  if [ -z "$refused" ]; then
    fail "node 3 on OLD did not exit 78 on a format 2 directory (last state: $state)"
  elif ! echo "$logs" | grep -q "REFUSING TO START (exit 78): the format marker .* records format 2"; then
    fail "node 3 on OLD exited 78 without the format marker's refusal: $(echo "$logs" | grep "REFUSING" | tail -1)"
  else
    echo "node 3 on OLD refused the format 2 directory with exit 78"
  fi

  volume_md5 "$volume" "$NEW_IMAGE" >"$WORK/md5-after.txt" || fail "could not read node 3's data after the rollback"
  if [ -s "$WORK/md5-before.txt" ] && diff -q "$WORK/md5-before.txt" "$WORK/md5-after.txt" >/dev/null; then
    echo "node 3's data directory is byte for byte what it was ($(wc -l <"$WORK/md5-before.txt" | tr -d ' ') files)"
  else
    diff "$WORK/md5-before.txt" "$WORK/md5-after.txt" | head -20
    fail "node 3's data directory changed across the refused start"
  fi

  require_progress "$before" "the refused rollback of node 3 (quorum on 2/3)"
  if serves_through malachi1,malachi2; then
    echo "nodes 1 and 2 serve a clean produce while node 3 is down"
  else
    fail "nodes 1 and 2 did not serve a clean produce while node 3 was down"
  fi

  export UPGRADE_RESTART_3=on-failure
  swap 3 "$NEW_IMAGE" "p3: node 3 back on NEW"
}

# Invariant: no process crashed on a message it did not know, in any container of the run, the replaced ones
# included (their logs were saved before each recreate).
# Any crash report fails the run, in a NEW build's log and in an OLD one's, with one exception: a crash an OLD
# build is known to have, listed below with the change that fixed it. A released build cannot be fixed by
# certifying the next one, but only a crash someone has read and named is excused: a crash in an OLD log over a
# message it did not know, whatever form it takes (an unknown tag, or a known tag in a shape it cannot read),
# is what the upgrade itself causes, and fails. An excused crash is printed all the same, so it is seen.
CRASH_ANY="FunctionClauseError|GenServer .* terminating|\*\* \(EXIT\)"
# One extended regex per known crash, matched against the line after its "GenServer ... terminating" line.
OLD_KNOWN_CRASHES=(
  # A heal or retention coordinator dying on the broker's :metadata call timing out while other nodes restart.
  # Fixed by fix(cluster): heal and retention skip a pass the broker cannot answer (#196).
  'GenServer\.call\(Malachi\.LogBroker, :metadata'
)

# $1 with every crash report of an OLD_KNOWN_CRASHES kind removed: the "GenServer ... terminating" line and
# everything up to the next timestamped line.
# Closed when in doubt: with no entry it removes nothing, an entry that matches an empty line makes it fail (an
# empty regex, .* or () matches every line and would excuse every crash), and so does an entry awk cannot read,
# rather than print what it read before the error. An entry that matches only almost every line (a single space)
# is not caught: the list is reviewed like the rest of the script.
without_known_crashes() {
  if [ "${#OLD_KNOWN_CRASHES[@]}" -eq 0 ]; then
    cat "$1"
    return
  fi
  for entry in "${OLD_KNOWN_CRASHES[@]}"; do
    KNOWN_CRASH="$entry" awk 'BEGIN { exit ("" ~ ENVIRON["KNOWN_CRASH"]) }' || return 2
  done
  # Through the environment, not -v: awk reads escape sequences in a -v value, which would turn the regexes'
  # backslashes into nothing and leave them unbalanced.
  KNOWN_CRASHES=$(IFS='|'; echo "${OLD_KNOWN_CRASHES[*]}") awk '
    skipping && /^[0-9][0-9]:[0-9][0-9]:/ { skipping = 0 }
    skipping { next }
    /GenServer .* terminating/ { held = $0; if ((getline line) > 0) { if (line ~ ENVIRON["KNOWN_CRASHES"]) { skipping = 1; next } print held; print line; next } print held; next }
    { print }
  ' "$1"
}

check_no_crash() {
  say "invariant: no process crashed on a message it did not know"
  for n in 1 2 3; do capture_logs "malachi-cluster-$n" "end of the drill"; done
  crashed=0
  for f in "$WORK/logs/"*.log; do
    [ -f "$f" ] || continue
    seq=$(basename "$f" | cut -d- -f1)
    image=$(awk -v s="$seq" '$1 == s {print $2}' "$WORK/logs/images.txt" 2>/dev/null)
    if [ "$image" != "$OLD_IMAGE" ]; then
      checked=$(cat "$f")
    elif ! checked=$(without_known_crashes "$f"); then
      fail "OLD_KNOWN_CRASHES holds an entry that matches an empty line or one awk cannot read as a regular expression"
      checked=$(cat "$f")
    fi
    if echo "$checked" | grep -qE "$CRASH_ANY"; then
      echo "--- $(basename "$f") (${image:-unknown image})"
      echo "$checked" | grep -E -A 5 "$CRASH_ANY" | head -20
      crashed=1
    elif grep -qE "$CRASH_ANY" "$f"; then
      echo "--- $(basename "$f") (OLD, a crash it is known to have, listed in OLD_KNOWN_CRASHES; not failed)"
      grep -E -A 3 "$CRASH_ANY" "$f" | head -8
    fi
  done
  if [ "$crashed" = 0 ]; then
    echo "no crash report the certification counts in any of the $(grep -c . "$WORK/logs/index.txt") saved container logs"
  else
    fail "a process crashed during the upgrade (see the logs above)"
  fi
}

record_details() {
  ACKED_WRITES=$ACKED_TOTAL
  CHAOS_DETAILS=$(jq -n -c \
    --arg old_ref "$OLD_REF" \
    --arg old_rule "$OLD_RULE" \
    --arg old_sha "$OLD_SHA" \
    --arg old_version "$OLD_VERSION" \
    --argjson old_pin "${OLD_PIN:-null}" \
    --arg old_patch "$OLD_PATCH" \
    --arg new_patch "$NEW_PATCH" \
    --argjson acked_writes_by_phase "$ACKED_BY_PHASE" \
    --argjson split_to "${SPLIT_TO:-null}" \
    --argjson vnodes "$MALACHI_LOG_VNODES" \
    --arg vnode_rule "$VNODE_RULE" \
    '{old_ref: $old_ref, old_rule: $old_rule, old_sha: $old_sha, old_version: $old_version, old_machine_version: $old_pin,
      old_patch: (if $old_patch == "" then null else $old_patch end),
      new_patch: (if $new_patch == "" then null else $new_patch end),
      negative_control: ($old_patch != "" or $new_patch != ""),
      acked_writes_by_phase: $acked_writes_by_phase, vnodes: $vnodes, vnode_rule: $vnode_rule,
      vnodes_after_split: $split_to}')
}

# Before anything is built: the images are tagged per checkout, so a run refused because another one is up
# would otherwise re-tag, then remove on its way out, the images that run is using. `start_cluster` checks again.
preflight_cluster
resolve_old_ref
prepare_contexts
build_upgrade_images

export UPGRADE_RESTART_3=on-failure
export MALACHI_RA_MACHINE_VERSION="$OLD_PIN"
start_cluster

say "phase 1: roll forward under the pin"
begin_phase 1 "$PHASE1_WINDOW_S"
event "p1: rolling every node from OLD to NEW (pin $OLD_PIN)"
swap 3 "$NEW_IMAGE" "p1: node 3 to NEW"
mixed_cluster_checks
swap 2 "$NEW_IMAGE" "p1: node 2 to NEW"
swap 1 "$NEW_IMAGE" "p1: node 1 to NEW"
[ -n "$SPLIT_TO" ] && split_ring
end_phase

say "phase 2: roll back before the flip"
begin_phase 2 "$PHASE2_WINDOW_S"
event "p2: rolling every node from NEW back to OLD (pin $OLD_PIN)"
swap 3 "$OLD_IMAGE" "p2: node 3 back to OLD"
swap 2 "$OLD_IMAGE" "p2: node 2 back to OLD"
swap 1 "$OLD_IMAGE" "p2: node 1 back to OLD"
[ -n "$SPLIT_TO" ] && check_ring
end_phase

say "phase 3: roll forward, finalize, flip, refused rollback"
begin_phase 3 "$PHASE3_WINDOW_S"
event "p3: rolling every node from OLD to NEW (pin $OLD_PIN)"
swap 3 "$NEW_IMAGE" "p3: node 3 to NEW"
swap 2 "$NEW_IMAGE" "p3: node 2 to NEW"
swap 1 "$NEW_IMAGE" "p3: node 1 to NEW"

event "p3: finalizing: the pin removed node by node"
export MALACHI_RA_MACHINE_VERSION=
for n in 3 2 1; do roll_node "malachi$n" "p3: node $n without the pin"; done
if await_value "$NEW_VERSION" 30 effective_version 1; then
  echo "the control plane moved to machine version $NEW_VERSION"
else
  fail "the control plane did not move to machine version $NEW_VERSION after the pin was removed (at '$(effective_version 1)')"
fi
reply=$(canary_note 1 2)
echo "$reply" | grep -q "^:ok$" && echo "the canary command applies at machine version $NEW_VERSION" ||
  fail "the canary command did not apply at machine version $NEW_VERSION: $(echo "$reply" | tail -2 | tr '\n' ' ')"
check_control_plane all

event "p3: switching the canary flag on"
out=$(node_task 1 "malachi.flag enable upgrade_canary")
echo "$out" | grep -q "cluster flag enabled: upgrade_canary" && echo "the canary flag is on" ||
  fail "the canary flag was refused with every node on NEW: $(echo "$out" | tail -2 | tr '\n' ' ')"
for n in 1 2 3; do
  await_value 2 45 marker_format "$n" && echo "node $n's data directory is at format 2" ||
    fail "node $n's data directory did not reach format 2 after the flip (at '$(marker_format "$n")')"
done

refused_rollback
end_phase

check_no_crash
check_clean_produce
record_details
finish "UPGRADE AND ROLLBACK CERTIFICATION"
