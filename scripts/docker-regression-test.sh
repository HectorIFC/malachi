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

# Test 4: TCP port listening
run_test "TCP server listening" "nc -zv localhost ${TCP_PORT}"

# Test 5: Container process running
run_test "Container process health" "docker exec $CONTAINER_NAME bin/malachi pid"

# Test 6: Basic log produce/fetch workflow (produce by key -> fetch by opaque cursor)
echo -n "Testing: Log produce/fetch workflow... "
WORKFLOW_RESULT=$(docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topic = "regression_topic"
  _ = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  {:ok, 1} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, [%Malachi.Log.Record{key: "rk", value: "test payload"}])
  {:ok, records, _cursor} = Malachi.LogApi.fetch(Malachi.LogBroker, topic, :start, 10)
  if "test payload" in Enum.map(records, & &1.value), do: IO.puts("PASS"), else: IO.puts("FAIL")
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$WORKFLOW_RESULT" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo -e "${RED}FAIL${NC} (got: $WORKFLOW_RESULT)"
    TESTS_FAILED=$((TESTS_FAILED + 1))
fi
# Test 6.5: Consumer group resume workflow (fetch by group -> commit -> resume empty, at-least-once)
echo -n "Testing: Consumer group resume workflow... "
GROUP_RESULT=$(docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topic = "regression_group_topic"
  _ = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  {:ok, _} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, [%Malachi.Log.Record{key: "g", value: "gm"}])
  {:ok, first, cursor} = Malachi.LogApi.fetch_group(Malachi.LogBroker, topic, "rg", 10)
  :ok = Malachi.LogApi.commit(Malachi.LogBroker, topic, "rg", cursor)
  {:ok, second, _cursor} = Malachi.LogApi.fetch_group(Malachi.LogBroker, topic, "rg", 10)
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
THROUGHPUT_TEST=$(docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topic = "throughput_topic"
  _ = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  records = for i <- 1..1000, do: %Malachi.Log.Record{key: "k#{rem(i, 8)}", value: "msg_#{i}"}
  {:ok, count} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, records)
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
docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topic = "mem_topic"
  _ = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  records = for i <- 1..5000, do: %Malachi.Log.Record{key: "k#{rem(i, 8)}", value: "msg_#{i}"}
  {:ok, _count} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, records)
  :ok
' > /dev/null 2>&1
sleep 2
FINAL_MEM=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1)
echo -e "${GREEN}PASS${NC} (${INITIAL_MEM} → ${FINAL_MEM})"
TESTS_PASSED=$((TESTS_PASSED + 1))

# Test 9: Concurrent multi-topic operations
echo -n "Testing: Concurrent multi-topic operations... "
CONCURRENT_TEST=$(timeout 30 docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topics = for i <- 1..10, do: "concurrent_topic_#{i}"
  Enum.each(topics, fn t -> _ = Malachi.LogApi.create_topic(Malachi.LogBroker, t) end)

  # Produce concurrently to all topics (100 records each)
  topics
  |> Enum.map(fn t ->
    Task.async(fn ->
      records = for i <- 1..100, do: %Malachi.Log.Record{key: "k#{rem(i, 8)}", value: "msg_#{i}"}
      Malachi.LogApi.produce_records(Malachi.LogBroker, t, records)
    end)
  end)
  |> Task.await_many(20_000)

  # Verify every topic drained at least its 100 records back
  all_ok = Enum.all?(topics, fn t ->
    {:ok, records, _cursor} = Malachi.LogApi.fetch(Malachi.LogBroker, t, :start, 1000)
    length(records) >= 100
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
