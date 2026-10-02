#!/usr/bin/env bash
# Single-node restart drill (#273): a single node keeps what it acknowledged across a restart, and refuses
# to start rather than delete segments its control plane does not know.
#
# One container of IMAGE, in its default single-node configuration, with its data on a host directory and
# the orphan sweep sped up (a pass every 5 s, no minimum age to speak of, two sightings), so a sweep that
# would delete has every chance to within the drill. Arms, in order, all over the same data:
#
#   1. restart:            200 records produced and a group position committed; `docker restart`; every
#                          record readable again, then still readable after 30 s of sweeps, with every
#                          segment directory still on disk and the group's position intact. Before #273 a
#                          single node kept its metadata in memory: the read came back empty and the sweep
#                          deleted the directories.
#   2. recreate:           the container removed and created again with the same hostname: the same.
#   3. peers configured:   created again with MALACHI_LOG_NODES naming a second node: refuses to start
#                          (exit 78), since growing a one-member cluster in place is not supported.
#   4. another hostname:   created again under another hostname, so another node name and an empty control
#                          plane over the same segments: refuses to start (exit 78), segments untouched.
#   5. adopted:            the same with MALACHI_ADOPT_ORPHANED_LOG_DIR=true: starts, and logs what it adopted.
#
# Usage: scripts/docker-single-node-restart.sh [IMAGE]   (default malachi:test)
# Env:   SRV_CPUSET (default 1,2,3), DRILL_NAME (container name, default malachi-single-restart-<pid>),
#        DRILL_DIR (host data directory, default ./tmp/single-node-restart-<pid>; on Docker Desktop or
#        Colima it must be under a shared path such as /Users, which a mktemp -d directory is not).
#
# Only the container this script creates is ever stopped or removed.
set -euo pipefail

IMAGE="${1:-malachi:test}"
SRV_CPUSET="${SRV_CPUSET:-1,2,3}"
NAME="${DRILL_NAME:-malachi-single-restart-$$}"
DIR="${DRILL_DIR:-$PWD/tmp/single-node-restart-$$}"
HOST=drill273
OTHER_HOST=drill273b
BROKER=Malachi.LogBroker
TOPIC=orders
GROUP=g1

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  # The container wrote the directory as its own user; remove it from a container so no host permission
  # stands in the way.
  docker run --rm --entrypoint sh -v "$DIR:/d" "$IMAGE" -c 'rm -rf /d/*' >/dev/null 2>&1 || true
  rmdir "$DIR" 2>/dev/null || true
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  echo "--- container log ---" >&2
  docker logs "$NAME" 2>&1 | tail -60 >&2 || true
  exit 1
}

mkdir -p "$DIR"
chmod 777 "$DIR"

run_node() {
  local host=$1
  shift
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" --hostname "$host" --cpuset-cpus "$SRV_CPUSET" \
    -v "$DIR:/app/data" \
    -e MALACHI_LOG_DATA_DIR=/app/data/log -e MALACHI_RA_DATA_DIR=/app/data/ra \
    -e MALACHI_CONFIG_ENV=test -e MALACHI_REQUIRE_TLS=false \
    -e MALACHI_ADMIN_PASS="drill_$(openssl rand -hex 8)" \
    -e MALACHI_RETENTION_ORPHAN_SWEEP_INTERVAL_MS=5000 \
    -e MALACHI_RETENTION_ORPHAN_MIN_AGE_MS=1000 \
    -e MALACHI_RETENTION_ORPHAN_SIGHTINGS=2 \
    "$@" "$IMAGE" >/dev/null
}

rpc() { docker exec "$NAME" bin/malachi rpc "$1"; }

# Every check below reads its whole input (`grep -c`, never `grep -q`): `grep -q` exits at the first
# match, and under pipefail the SIGPIPE that leaves the writer with fails a check that matched.

wait_up() {
  for _ in $(seq 1 60); do
    if rpc "IO.puts(is_pid(Process.whereis($BROKER)))" 2>/dev/null | grep -cF -- true >/dev/null; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]; then fail "the node stopped while booting"; fi
    sleep 1
  done
  fail "the node did not come up in 60 s"
}

# Prints the number of records a fetch from the start returns. A refusal while the node learns where its
# segments end (:metadata_unavailable) is retried for up to 10 s; an empty or short SUCCESSFUL page is
# never retried, because that is the answer this drill exists to catch.
fetch_count() {
  rpc "Enum.reduce_while(1..100, nil, fn _, _ ->
         case Malachi.LogApi.fetch($BROKER, \"$TOPIC\", :start, 1000) do
           {:ok, records, _} -> {:halt, IO.puts(length(records))}
           {:error, :metadata_unavailable} -> Process.sleep(100); {:cont, nil}
           other -> {:halt, IO.puts(inspect(other))}
         end
       end)"
}

committed() { rpc "IO.inspect(Malachi.BrokerServer.committed_offsets($BROKER, \"$GROUP\", \"$TOPIC\"))"; }
segments() { docker exec "$NAME" sh -c 'ls /app/data/log' | grep -E "^$TOPIC-r[0-9]+-s[0-9]+$" | sort || true; }

expect_records() {
  local got
  got=$(fetch_count)
  [ "$got" = "200" ] || fail "$1: a fetch from the start returned $got records, expected 200"
}

expect_refusal() {
  local status
  status=$(timeout 90 docker wait "$NAME") || fail "$1: the node did not stop within 90 s"
  [ "$status" = "78" ] || fail "$1: exit status $status, expected 78"
  docker logs "$NAME" 2>&1 | grep -cF -- "$2" >/dev/null || fail "$1: the log does not say $2"
  # The segments are the same directories, untouched, read from the host side.
  [ "$(ls "$DIR/log" | grep -E "^$TOPIC-r[0-9]+-s[0-9]+$" | sort)" = "$SEGMENTS" ] || fail "$1: segment directories changed"
}

echo "== 1. restart"
run_node "$HOST"
wait_up
rpc "IO.inspect(Malachi.LogApi.create_topic($BROKER, \"$TOPIC\"))" | grep -cF -- ':ok' >/dev/null || fail "create_topic"
rpc "IO.inspect(Malachi.LogApi.produce($BROKER, \"$TOPIC\", for(i <- 1..200, do: %{\"key\" => \"k#{i}\", \"value\" => \"v#{i}\"})))" |
  grep -cF -- '{:ok, 200}' >/dev/null || fail "produce"
rpc "{:ok, _, cursor} = Malachi.LogApi.fetch($BROKER, \"$TOPIC\", :start, 50); IO.inspect(Malachi.LogApi.commit($BROKER, \"$TOPIC\", \"$GROUP\", cursor))" |
  grep -cF -- ':ok' >/dev/null || fail "commit"
COMMITTED=$(committed)
SEGMENTS=$(segments)
[ -n "$SEGMENTS" ] || fail "no segment directory after the produce"
echo "committed $COMMITTED; segments: $(echo $SEGMENTS)"

docker restart "$NAME" >/dev/null
wait_up
expect_records "after the restart"
[ "$(committed)" = "$COMMITTED" ] || fail "the group's position changed across the restart: $(committed)"
echo "sweeping for 30 s"
sleep 30
expect_records "after 30 s of sweeps"
[ "$(segments)" = "$SEGMENTS" ] || fail "segment directories changed after the sweeps: $(segments)"
echo "ok"

echo "== 2. recreate with the same hostname"
run_node "$HOST"
wait_up
expect_records "after recreating the container"
[ "$(committed)" = "$COMMITTED" ] || fail "the group's position changed across the recreate"
echo "ok"

echo "== 3. a second node configured"
run_node "$HOST" -e MALACHI_LOG_NODES="malachi@$HOST,malachi@peer273"
expect_refusal "peers configured" "REFUSING TO START"
docker logs "$NAME" 2>&1 | grep -cF -- "peer273" >/dev/null || fail "peers configured: the refusal does not name the new node"
echo "ok"

echo "== 4. another hostname"
run_node "$OTHER_HOST"
expect_refusal "another hostname" "MALACHI_ADOPT_ORPHANED_LOG_DIR"
echo "ok"

echo "== 5. another hostname, directory adopted"
run_node "$OTHER_HOST" -e MALACHI_ADOPT_ORPHANED_LOG_DIR=true
wait_up
docker logs "$NAME" 2>&1 | grep -cF -- "MALACHI_ADOPT_ORPHANED_LOG_DIR=true" >/dev/null || fail "adopted: the adoption is not logged"
echo "ok"

echo "PASS: single-node restart drill"
