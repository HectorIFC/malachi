#!/usr/bin/env bash
# Single-node restart drill (#273): a single node keeps what it acknowledged across a restart, and refuses
# to start rather than delete segments its control plane does not know.
#
# One container of IMAGE, in its default single-node configuration, with its data on a host directory and
# the orphan sweep sped up (a pass every 5 s, no minimum age to speak of, two sightings), so a sweep that
# would delete has every chance to within the drill. Arms, in order, all over the same data:
#
#   1. restart:            200 records produced, read by a group member and its position committed past
#                          them; `docker restart`; every record readable again, then still readable after
#                          30 s of sweeps, with every segment directory still on disk and the group's
#                          position intact (nothing left to read after it). Before #273 a
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
# Every read and write goes over the wire, through `Malachi.Loadtest.Client` connected to the node's own
# listener as the one user the drill seeds (MALACHI_DEFAULT_USERS), so it runs on the stream and group keys
# as any client does.
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
# One admin password for the whole run: the users live in the control plane on the data directory, so every
# container of the run seeds and logs in with the same one.
ADMIN_PASS="drill_$(openssl rand -hex 8)"

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
    -e MALACHI_DEFAULT_USERS="admin:${ADMIN_PASS}:admin" \
    -e MALACHI_RETENTION_ORPHAN_SWEEP_INTERVAL_MS=5000 \
    -e MALACHI_RETENTION_ORPHAN_MIN_AGE_MS=1000 \
    -e MALACHI_RETENTION_ORPHAN_SIGHTINGS=2 \
    "$@" "$IMAGE" >/dev/null
}

rpc() { docker exec "$NAME" bin/malachi rpc "$1"; }

# Binds `client` in an rpc snippet: a `Malachi.Loadtest.Client` connected to the node's listener, asked again
# every 100 ms for up to 10 s while the node cannot answer one yet (its cluster state still unreadable).
CONNECT="alias Malachi.Loadtest.Client
  client =
    Enum.reduce_while(1..100, nil, fn _, _ ->
      case Client.connect(host: \"127.0.0.1\", port: Malachi.TCPAcceptorPool.port(), user: \"admin\", pass: \"$ADMIN_PASS\") do
        {:ok, client} -> {:halt, client}
        {:error, _} -> Process.sleep(100); {:cont, nil}
      end
    end) || raise \"no connection to the node in 10 s\""

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

# Prints the number of records a read of the whole topic from the start returns. A refusal while the node
# learns where its segments end (metadata_unavailable) is waited out for up to 10 s by the client; an empty
# or short SUCCESSFUL read is never retried, because that is the answer this drill exists to catch.
fetch_count() {
  rpc "$CONNECT
       case Client.read(client, \"$TOPIC\", timeout_ms: 10_000) do
         {:ok, %{values: values}} -> IO.puts(length(values))
         other -> IO.puts(inspect(other))
       end"
}

# Prints how many records the group still has to read from its checkpoint: left=0 once its position is
# committed past every record, left=200 if the position were lost.
committed() {
  rpc "$CONNECT
       case Client.read(client, \"$TOPIC\", start_from: {:committed, \"$GROUP\"}, timeout_ms: 10_000) do
         {:ok, %{values: values}} -> IO.puts(\"left=#{length(values)}\")
         other -> IO.puts(inspect(other))
       end"
}

segments() { docker exec "$NAME" sh -c 'ls /app/data/log' | grep -E "^$TOPIC-r[0-9]+-s[0-9]+$" | sort || true; }

expect_records() {
  local got
  got=$(fetch_count)
  [ "$got" = "200" ] || fail "$1: a read from the start returned $got records, expected 200"
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
rpc "$CONNECT
     IO.inspect(Client.create_topic(client, \"$TOPIC\", 8))" | grep -cF -- ':ok' >/dev/null || fail "create_topic"
rpc "$CONNECT
     records = for i <- 1..200, do: Malachi.Log.Record.new(\"v#{i}\", key: \"k#{i}\")
     IO.inspect(Client.produce(client, \"$TOPIC\", records, 10_000))" |
  grep -cF -- '{:ok, 200}' >/dev/null || fail "produce"
# A member reads everything from the group's checkpoint and commits the positions it reached, then leaves:
# the checkpoint stays in the control plane.
rpc "$CONNECT
     {:ok, %{generation: generation, ranges: ranges}} = Client.join(client, \"$TOPIC\", \"$GROUP\", \"m1\")
     {:ok, %{positions: positions}} =
       Client.read(client, \"$TOPIC\", start_from: {:committed, \"$GROUP\"}, ranges: ranges, timeout_ms: 10_000)
     :ok = Client.commit(client, \"$TOPIC\", \"$GROUP\", \"m1\", generation, positions)
     IO.inspect(Client.leave(client, \"$TOPIC\", \"$GROUP\", \"m1\"))" |
  grep -cF -- ':ok' >/dev/null || fail "commit"
COMMITTED=$(committed)
[ "$COMMITTED" = "left=0" ] || fail "the group's committed position does not cover the 200 records: $COMMITTED"
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
# With an address to advertise, so the refusal is the data directory's and not the missing host's.
run_node "$HOST" -e MALACHI_LOG_NODES="malachi@$HOST,malachi@peer273" -e MALACHI_ADVERTISED_HOST="$HOST"
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
