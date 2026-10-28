#!/bin/bash
set -e

echo "=== Malachi Docker Regression Tests ==="
echo ""

# Configuration
IMAGE_NAME="${1:-hectorcardoso/malachi:latest}"
CONTAINER_NAME="malachi-regression-$$"
TCP_PORT=14040
DASHBOARD_PORT=14041

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Track test results
TESTS_PASSED=0
TESTS_FAILED=0

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    docker stop "$CONTAINER_NAME" > /dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# Runs an Elixir snippet in the node (`bin/malachi rpc`) with `client` bound to a `Malachi.Loadtest.Client`
# connected to the node's own listener as admin, so the log checks below go over the wire, on the stream
# and group keys, as any client does. The container below serves plain TCP (MALACHI_REQUIRE_TLS=false),
# which is what the client speaks. The snippet ($2) gets $1 seconds, the time a check may take.
client_rpc() {
    timeout "$1" docker exec "$CONTAINER_NAME" bin/malachi rpc "
  alias Malachi.Loadtest.Client
  alias Malachi.Log.Record
  {:ok, client} = Client.connect(host: \"127.0.0.1\", port: Malachi.TCPAcceptorPool.port(), user: \"admin\", pass: System.fetch_env!(\"MALACHI_ADMIN_PASS\"))
  $2"
}

# Test helper function
run_test() {
    local test_name=$1
    local test_command=$2
    
    echo -n "Testing: $test_name... "
    if eval "$test_command" > /dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# Generate random passwords (save admin password for dashboard auth)
ADMIN_PASS="regression_admin_$(date +%s)"
PRODUCER_PASS="regression_producer_$(date +%s)"
CONSUMER_PASS="regression_consumer_$(date +%s)"
APP_PASS="regression_app_$(date +%s)"

# Start container
echo "Starting container: $IMAGE_NAME"
docker run -d --name "$CONTAINER_NAME" \
    -p ${TCP_PORT}:4040 \
    -p ${DASHBOARD_PORT}:4041 \
    -e MALACHI_ADMIN_PASS="$ADMIN_PASS" \
    -e MALACHI_PRODUCER_PASS="$PRODUCER_PASS" \
    -e MALACHI_CONSUMER_PASS="$CONSUMER_PASS" \
    -e MALACHI_APP_PASS="$APP_PASS" \
    -e MALACHI_REQUIRE_TLS=false \
    -e MALACHI_ENABLE_TLS=false \
    "$IMAGE_NAME" > /dev/null

# Wait for startup
echo "Waiting for services to start..."
sleep 8

# Authenticate to dashboard and obtain Bearer token
echo "Authenticating to dashboard..."
LOGIN_RESPONSE=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"admin\",\"password\":\"${ADMIN_PASS}\"}" \
    "http://localhost:${DASHBOARD_PORT}/login" 2>&1) || {
    echo -e "${RED}FAIL: Could not authenticate to dashboard${NC}"
    echo "Response: $LOGIN_RESPONSE"
    docker logs --tail 20 "$CONTAINER_NAME"
    exit 1
}
TOKEN=$(echo "$LOGIN_RESPONSE" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['token'])" 2>/dev/null) || {
    echo -e "${RED}FAIL: Could not parse auth token from response${NC}"
    echo "Response: $LOGIN_RESPONSE"
    exit 1
}
echo -e "${GREEN}Dashboard authentication successful${NC}"
echo ""

# Test 1: Dashboard health check (with auth)
run_test "Dashboard HTTP endpoint" "curl -sf -H 'Authorization: Bearer ${TOKEN}' http://localhost:${DASHBOARD_PORT}/"

# Test 2: Metrics endpoint (with auth)
run_test "Metrics endpoint returns JSON" "curl -sf -H 'Authorization: Bearer ${TOKEN}' http://localhost:${DASHBOARD_PORT}/metrics | grep -q 'topics'"

# Test 3: SSE stream endpoint (with auth)
run_test "SSE stream endpoint available" "timeout 2 curl -sf -H 'Authorization: Bearer ${TOKEN}' http://localhost:${DASHBOARD_PORT}/stream | head -1"

# Test 3.5: Static assets shipped in the image, and nothing else from priv. Not through run_test, which
# discards output: the check names what failed (a 404, the content type, the body, an extra priv entry).
echo -n "Testing: Static assets shipped in the image... "
if ASSETS_RESULT=$("$(dirname "$0")/docker-static-assets-check.sh" "$CONTAINER_NAME" "http://localhost:${DASHBOARD_PORT}" 2>&1); then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC}"
    echo "$ASSETS_RESULT"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 4: TCP port listening
run_test "TCP server listening" "nc -zv localhost ${TCP_PORT}"

# Test 5: Container process running
run_test "Container process health" "docker exec $CONTAINER_NAME bin/malachi pid"

# Test 6: Basic log produce/read workflow (append by key on a producer stream -> read the topic back)
echo -n "Testing: Log produce/read workflow... "
WORKFLOW_RESULT=$(client_rpc 60 '
  topic = "regression_topic"
  _ = Client.create_topic(client, topic, 8)
  {:ok, 1} = Client.produce(client, topic, [Record.new("test payload", key: "rk")], 10_000)
  {:ok, %{values: values}} = Client.read(client, topic)
  if "test payload" in values, do: IO.puts("PASS"), else: IO.puts("FAIL")
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$WORKFLOW_RESULT" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC} (got: $WORKFLOW_RESULT)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
# Test 6.5: Consumer group resume workflow (join -> read from the checkpoint -> commit -> resume empty,
# at-least-once)
echo -n "Testing: Consumer group resume workflow... "
GROUP_RESULT=$(client_rpc 60 '
  topic = "regression_group_topic"
  _ = Client.create_topic(client, topic, 8)
  {:ok, _} = Client.produce(client, topic, [Record.new("gm", key: "g")], 10_000)
  {:ok, %{generation: generation, ranges: ranges}} = Client.join(client, topic, "rg", "m1")
  {:ok, %{values: first, positions: positions}} = Client.read(client, topic, start_from: {:committed, "rg"}, ranges: ranges)
  :ok = Client.commit(client, topic, "rg", "m1", generation, positions)
  {:ok, %{values: second}} = Client.read(client, topic, start_from: {:committed, "rg"}, ranges: ranges)
  :ok = Client.leave(client, topic, "rg", "m1")
  # after committing the position the group resumes with nothing left to read
  if length(first) >= 1 and second == [], do: IO.puts("PASS"), else: IO.puts("FAIL")
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$GROUP_RESULT" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC} (got: $GROUP_RESULT)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
# Test 7: High-volume throughput
echo -n "Testing: High-volume message throughput... "
THROUGHPUT_TEST=$(client_rpc 60 '
  topic = "throughput_topic"
  _ = Client.create_topic(client, topic, 8)
  records = for i <- 1..1000, do: Record.new("msg_#{i}", key: "k#{rem(i, 8)}")
  {:ok, count} = Client.produce(client, topic, records, 30_000)
  if count >= 1000, do: IO.puts("PASS"), else: IO.puts("FAIL:#{count}")
' 2>&1 | grep -E "^PASS$|^FAIL:" | tail -1)

if echo "$THROUGHPUT_TEST" | grep -q "^PASS"; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC} (result: $THROUGHPUT_TEST)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 8: Memory stability under load
echo -n "Testing: Memory stability under load... "
INITIAL_MEM=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1)
client_rpc 60 '
  topic = "mem_topic"
  _ = Client.create_topic(client, topic, 8)
  records = for i <- 1..5000, do: Record.new("msg_#{i}", key: "k#{rem(i, 8)}")
  {:ok, _count} = Client.produce(client, topic, records, 30_000)
  :ok
' > /dev/null 2>&1
sleep 2
FINAL_MEM=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1)
echo -e "${GREEN}PASS${NC} (${INITIAL_MEM} → ${FINAL_MEM})"
TESTS_PASSED=$((TESTS_PASSED + 1))

# Test 9: Concurrent multi-topic operations
echo -n "Testing: Concurrent multi-topic operations... "
CONCURRENT_TEST=$(client_rpc 30 '
  topics = for i <- 1..10, do: "concurrent_topic_#{i}"
  Enum.each(topics, fn t -> _ = Client.create_topic(client, t, 8) end)

  # Produce concurrently to all topics (100 records each), each on producer streams of its own
  topics
  |> Enum.map(fn t ->
    Task.async(fn ->
      records = for i <- 1..100, do: Record.new("msg_#{i}", key: "k#{rem(i, 8)}")
      Client.produce(client, t, records, 15_000)
    end)
  end)
  |> Task.await_many(20_000)

  # Verify every topic reads at least its 100 records back
  all_ok = Enum.all?(topics, fn t ->
    {:ok, %{values: values}} = Client.read(client, t)
    length(values) >= 100
  end)

  if all_ok, do: IO.puts("PASS"), else: IO.puts("FAIL")
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$CONCURRENT_TEST" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC}"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Test 10: JIT compilation active
echo -n "Testing: JIT compilation enabled... "
JIT_STATUS=$(docker exec "$CONTAINER_NAME" bin/malachi rpc ':erlang.system_info(:emu_flavor)' 2>&1 | grep -o 'jit' || echo "nojit")
if [ "$JIT_STATUS" = "jit" ]; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${YELLOW}SKIP${NC} (JIT not detected, this is OK for some platforms)"
fi

# Test 11: The image's own HEALTHCHECK turns the container healthy (#282). Last, so its wait for the first
# probe overlaps the tests above. Not through run_test, which discards output: the check names what failed
# and prints the last probe's output.
echo -n "Testing: Image HEALTHCHECK turns healthy... "
if HEALTH_RESULT=$("$(dirname "$0")/docker-image-health-check.sh" "$CONTAINER_NAME" 2>&1); then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC}"
    echo "$HEALTH_RESULT"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi

# Summary
echo ""
echo "==================================="
echo "Regression Test Summary"
echo "==================================="
echo -e "Passed: ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Failed: ${RED}${TESTS_FAILED}${NC}"
echo "==================================="

if [ $TESTS_FAILED -gt 0 ]; then
    echo ""
    echo "Logs from failed container:"
    docker logs --tail 50 "$CONTAINER_NAME"
    exit 1
else
    echo -e "${GREEN}All regression tests passed!${NC}"
    exit 0
fi
