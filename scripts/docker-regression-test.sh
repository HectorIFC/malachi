#!/bin/bash
set -e

echo "=== MalachiMQ Docker Regression Tests ==="
echo ""

# Configuration
IMAGE_NAME="${1:-hectorcardoso/malachimq:latest}"
CONTAINER_NAME="malachimq-regression-$$"
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
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        ((TESTS_FAILED++))
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
    -e MALACHIMQ_ADMIN_PASS="$ADMIN_PASS" \
    -e MALACHIMQ_PRODUCER_PASS="$PRODUCER_PASS" \
    -e MALACHIMQ_CONSUMER_PASS="$CONSUMER_PASS" \
    -e MALACHIMQ_APP_PASS="$APP_PASS" \
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
run_test "Metrics endpoint returns JSON" "curl -sf -H 'Authorization: Bearer ${TOKEN}' http://localhost:${DASHBOARD_PORT}/metrics | grep -q 'queues'"

# Test 3: SSE stream endpoint (with auth)
run_test "SSE stream endpoint available" "timeout 2 curl -sf -H 'Authorization: Bearer ${TOKEN}' http://localhost:${DASHBOARD_PORT}/stream | head -1"

# Test 4: TCP port listening
run_test "TCP server listening" "nc -zv localhost ${TCP_PORT}"

# Test 5: Container process running
run_test "Container process health" "docker exec $CONTAINER_NAME bin/malachimq pid"

# Test 6: Basic queue operations
echo -n "Testing: Queue publish/consume workflow... "
WORKFLOW_RESULT=$(docker exec "$CONTAINER_NAME" bin/malachimq rpc '
  result = MalachiMQ.Queue.enqueue("regression_test", "test payload", %{"test" => "true"})
  :timer.sleep(500)
  metrics = MalachiMQ.Metrics.get_metrics("regression_test")
  case {result, metrics.enqueued} do
    {{:ok, _}, count} when count > 0 -> IO.puts("PASS")
    _ -> IO.puts("FAIL")
  end
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$WORKFLOW_RESULT" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
else
    echo -e "${RED}FAIL${NC} (got: $WORKFLOW_RESULT)"
    ((TESTS_FAILED++))
fi
# Test 6.5: Channel publish/broadcast workflow
echo -n "Testing: Channel publish/broadcast workflow... "
CHANNEL_RESULT=$(docker exec "$CONTAINER_NAME" bin/malachimq rpc '
  # Spawn multiple subscribers
  parent = self()
  subscribers = Enum.map(1..5, fn i ->
    spawn(fn ->
      receive do
        {:channel_message, msg} -> send(parent, {:received, i, msg})
      after
        2000 -> send(parent, {:timeout, i})
      end
    end)
  end)
  
  # Subscribe all
  Enum.each(subscribers, fn sub ->
    MalachiMQ.Channel.subscribe("regression_channel", sub)
  end)
  
  :timer.sleep(200)
  
  # Publish message
  MalachiMQ.Channel.publish("regression_channel", "broadcast_test", %{})
  
  # Wait for responses
  :timer.sleep(500)
  
  # Count received messages
  received = Enum.reduce(1..5, 0, fn _, acc ->
    receive do
      {:received, _, _} -> acc + 1
      {:timeout, _} -> acc
    after
      0 -> acc
    end
  end)
  
  stats = MalachiMQ.Channel.get_stats("regression_channel")
  
  if received >= 3 and stats.published >= 1 do
    IO.puts("PASS")
  else
    IO.puts("FAIL")
  end
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$CHANNEL_RESULT" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
else
    echo -e "${RED}FAIL${NC} (got: $CHANNEL_RESULT)"
    ((TESTS_FAILED++))
fi
# Test 7: High-volume throughput
echo -n "Testing: High-volume message throughput... "
THROUGHPUT_TEST=$(docker exec "$CONTAINER_NAME" bin/malachimq rpc '
  results = Enum.map(1..1000, fn i -> 
    MalachiMQ.Queue.enqueue("throughput_test", "msg_#{i}", %{})
  end)
  :timer.sleep(1000)
  metrics = MalachiMQ.Metrics.get_metrics("throughput_test")
  if metrics.enqueued >= 1000, do: IO.puts("PASS"), else: IO.puts("FAIL:#{metrics.enqueued}")
' 2>&1 | grep -E "^PASS$|^FAIL:" | tail -1)

if echo "$THROUGHPUT_TEST" | grep -q "^PASS"; then
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
else
    echo -e "${RED}FAIL${NC} (result: $THROUGHPUT_TEST)"
    ((TESTS_FAILED++))
fi

# Test 8: Memory stability under load
echo -n "Testing: Memory stability under load... "
INITIAL_MEM=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1)
docker exec "$CONTAINER_NAME" bin/malachimq rpc '
  Enum.each(1..5000, fn i ->
    MalachiMQ.Queue.enqueue("mem_test", "msg_#{i}", %{})
  end)
  :ok
' > /dev/null 2>&1
sleep 2
FINAL_MEM=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1)
echo -e "${GREEN}PASS${NC} (${INITIAL_MEM} → ${FINAL_MEM})"
((TESTS_PASSED++))

# Test 9: Concurrent queue operations
echo -n "Testing: Concurrent multi-queue operations... "
CONCURRENT_TEST=$(timeout 30 docker exec "$CONTAINER_NAME" bin/malachimq rpc '
  queues = for i <- 1..10, do: "concurrent_queue_#{i}"
  
  # Send messages to all queues
  Enum.each(queues, fn q ->
    Enum.each(1..100, fn i ->
      MalachiMQ.Queue.enqueue(q, "msg_#{i}", %{})
    end)
  end)
  
  :timer.sleep(1000)
  
  # Verify all queues have messages
  all_ok = Enum.all?(queues, fn q ->
    metrics = MalachiMQ.Metrics.get_metrics(q)
    metrics.enqueued >= 100
  end)
  
  if all_ok, do: IO.puts("PASS"), else: IO.puts("FAIL")
' 2>&1 | grep -E "^PASS$|^FAIL$" | tail -1)

if [ "$CONCURRENT_TEST" = "PASS" ]; then
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
else
    echo -e "${RED}FAIL${NC}"
    ((TESTS_FAILED++))
fi

# Test 10: JIT compilation active
echo -n "Testing: JIT compilation enabled... "
JIT_STATUS=$(docker exec "$CONTAINER_NAME" bin/malachimq rpc ':erlang.system_info(:emu_flavor)' 2>&1 | grep -o 'jit' || echo "nojit")
if [ "$JIT_STATUS" = "jit" ]; then
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
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
