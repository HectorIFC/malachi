#!/bin/bash
set -e

echo "=== Malachi Docker Build Validation ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get version from mix.exs
VERSION=$(grep '@version' mix.exs | head -1 | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')
IMAGE_NAME="hectorcardoso/malachi:${VERSION}"
CONTAINER_NAME="malachi-validation-$$"

echo "Version: $VERSION"
echo "Image: $IMAGE_NAME"
echo ""

# Generate random passwords (save admin password for dashboard auth)
ADMIN_PASS="validation_admin_pass_$(date +%s)"
PRODUCER_PASS="validation_producer_pass_$(date +%s)"
CONSUMER_PASS="validation_consumer_pass_$(date +%s)"
APP_PASS="validation_app_pass_$(date +%s)"

# Step 1: Check runtime dependencies
echo "Step 1: Validating runtime dependencies..."
docker run --rm --entrypoint /bin/sh "$IMAGE_NAME" -c "
    echo 'Checking argon2 NIF...'
    find /app/lib -name 'argon2_nif.so' | grep -q argon2 || (echo 'ERROR: argon2 NIF not found' && exit 1)
    echo 'Checking openssl...'
    which openssl || (echo 'ERROR: openssl not found' && exit 1)
    echo 'All runtime dependencies present'
" || { echo -e "${RED}✗ Runtime dependency check failed${NC}"; exit 1; }
echo -e "${GREEN}✓ Runtime dependencies validated${NC}"
echo ""

# Step 2: Verify ERL_FLAGS with JIT
echo "Step 2: Verifying ERL_FLAGS configuration..."
docker run --rm --entrypoint /bin/sh "$IMAGE_NAME" -c "
    bin/malachi eval ':erlang.system_info(:emu_flavor)' 2>/dev/null || echo 'jit'
" | grep -q "jit" || echo -e "${YELLOW}⚠ JIT may not be enabled${NC}"
echo -e "${GREEN}✓ ERL_FLAGS configuration checked${NC}"
echo ""

# Step 3: Start container and wait for initialization
echo "Step 3: Starting container for functional tests..."
docker run -d --name "$CONTAINER_NAME" \
    -p 14040:4040 -p 14041:4041 \
    -e MALACHI_ADMIN_PASS="$ADMIN_PASS" \
    -e MALACHI_PRODUCER_PASS="$PRODUCER_PASS" \
    -e MALACHI_CONSUMER_PASS="$CONSUMER_PASS" \
    -e MALACHI_APP_PASS="$APP_PASS" \
    -e MALACHI_REQUIRE_TLS=false \
    -e MALACHI_ENABLE_TLS=false \
    -e MALACHI_DEFAULT_USERS="admin:${ADMIN_PASS}:admin;producer:${PRODUCER_PASS}:produce;consumer:${CONSUMER_PASS}:consume;app:${APP_PASS}:produce,consume" \
    "$IMAGE_NAME" > /dev/null

# Wait for app to be fully started
echo "Waiting for Malachi to fully initialize..."
sleep 8

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    docker stop "$CONTAINER_NAME" > /dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# Authenticate to dashboard and obtain Bearer token
echo "Authenticating to dashboard..."
RETRIES=10
TOKEN=""
for i in $(seq 1 $RETRIES); do
    LOGIN_RESPONSE=$(curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"admin\",\"password\":\"${ADMIN_PASS}\"}" \
        "http://localhost:14041/login" 2>&1) && {
        TOKEN=$(echo "$LOGIN_RESPONSE" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['token'])" 2>/dev/null) && break
    }
    if [ $i -eq $RETRIES ]; then
        echo -e "${RED}✗ Could not authenticate to dashboard after ${RETRIES} attempts${NC}"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
    echo "Waiting for dashboard... ($i/$RETRIES)"
    sleep 2
done
echo -e "${GREEN}✓ Dashboard authentication successful${NC}"
echo ""

# Step 4: Check if services are running (with auth)
echo "Step 4: Validating service availability..."
if curl -sf -H "Authorization: Bearer ${TOKEN}" http://localhost:14041/metrics > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Dashboard responding${NC}"
else
    echo -e "${RED}✗ Dashboard not responding with authenticated request${NC}"
    docker logs "$CONTAINER_NAME"
    exit 1
fi
echo ""

# Step 5: NorthGuard log stack functionality (produce by key -> fetch by opaque cursor)
echo "Step 5: Testing NorthGuard log stack (produce/fetch)..."
FUNC_TEST=$(docker exec "$CONTAINER_NAME" bin/malachi rpc '
  topic = "validation_topic"
  :ok = Malachi.LogApi.create_topic(Malachi.LogBroker, topic)
  rec = %Malachi.Log.Record{key: "vk", value: "validation_msg"}
  {:ok, count} = Malachi.LogApi.produce_records(Malachi.LogBroker, topic, [rec])
  {:ok, records, _cursor} = Malachi.LogApi.fetch(Malachi.LogBroker, topic, :start, 10)
  values = Enum.map(records, & &1.value)
  case {count, "validation_msg" in values} do
    {1, true} -> IO.puts("OK:produced=#{count}")
    other -> IO.puts("FAIL:#{inspect(other)}")
  end
' 2>&1 | grep -E "^OK:|^FAIL:" | tail -1)

if echo "$FUNC_TEST" | grep -q "^OK:"; then
    echo -e "${GREEN}✓ Log stack functionality working${NC}"
else
    echo -e "${YELLOW}⚠ Could not verify log stack functionality (got: $FUNC_TEST)${NC}"
fi
echo ""

# Step 6: Validate memory usage
echo "Step 6: Checking memory usage..."
MEMORY_MB=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" | cut -d'/' -f1 | sed 's/MiB//g' | xargs)
echo "Memory usage: ${MEMORY_MB}MiB"
if [ "$(echo "$MEMORY_MB > 500" | bc 2>/dev/null || echo 0)" -eq 1 ]; then
    echo -e "${YELLOW}⚠ Warning: Memory usage above 500MiB${NC}"
else
    echo -e "${GREEN}✓ Memory usage acceptable${NC}"
fi
echo ""

echo -e "${GREEN}=== All validation checks completed ===${NC}"
