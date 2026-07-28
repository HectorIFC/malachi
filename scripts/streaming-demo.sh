#!/bin/bash

# Demo of the Malachi log model over the binary protocol: append by key, then stream by consumer group.
# Replaces the old channel pub/sub demo (channels are gone; a topic + consumer group is the equivalent).

set -e

TOPIC="demo_$(date +%s)"

echo "🚀 Malachi log + streaming demo"
echo "==============================="
echo ""
echo "This demo shows:"
echo "  • Appending records by key to a topic (producer.js)"
echo "  • Server-push streaming to a consumer group (subscriber.js)"
echo "  • Durable, resumable position via streamAck"
echo ""

if ! nc -z localhost 4040 2>/dev/null; then
    echo "❌ Malachi is not running on port 4040"
    echo "   Start it with: mix run --no-halt"
    exit 1
fi
echo "✓ Malachi is running"
echo ""

echo "Step 1: create the topic and append 5 records"
echo "---------------------------------------------"
node scripts/producer.js "$TOPIC" 5 --create
echo ""

echo "Step 2: stream the topic (3s), while a producer appends 3 more"
echo "--------------------------------------------------------------"
node scripts/subscriber.js "$TOPIC" --group demo &
SUB_PID=$!
sleep 1
node scripts/producer.js "$TOPIC" 3 --key live
sleep 2
kill "$SUB_PID" 2>/dev/null || true
echo ""

echo "✅ Demo complete!"
echo ""
echo "Try it interactively:"
echo "  Terminal 1: node scripts/subscriber.js $TOPIC --group demo"
echo "  Terminal 2: node scripts/producer.js $TOPIC 10"
echo ""
echo "Pull instead of push (client-driven cursor, resumable group):"
echo "  node scripts/consumer.js $TOPIC --group workers --follow"
echo ""
