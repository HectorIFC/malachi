#!/bin/bash

# Run All Baselines Script
# Executes all benchmark tests sequentially and generates consolidated report

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Malachi Performance Baseline Suite"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Ensure results directory exists
mkdir -p "$PROJECT_ROOT/benchmark/results"

# Generate timestamp for this run
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CONSOLIDATED_FILE="$PROJECT_ROOT/benchmark/results/baseline_${TIMESTAMP}.json"

# Get Malachi version
VERSION=$(grep '@version "' "$PROJECT_ROOT/mix.exs" | sed 's/.*@version "\(.*\)".*/\1/')
echo "Version: $VERSION"
echo "Timestamp: $TIMESTAMP"
echo ""

# Load benchmark config, MIX_ENV=benchmark loads config/benchmark.exs (mnesia_dir, tuning params)
export MIX_ENV=benchmark
export MALACHI_LOGGER_LEVEL=warning

# Disable rate limiting and connection limiting during benchmarks
# Benchmarks measure performance under ideal conditions, not security features
export MALACHI_RATE_LIMIT_ENABLED=false
export MALACHI_CONNECTION_LIMIT_ENABLED=false

echo "Starting benchmark suite (this will take ~10 minutes)..."
echo ""

# Array to store result files
RESULT_FILES=()

# Run each benchmark sequentially
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1/5 - Throughput Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cd "$PROJECT_ROOT"
elixir --sname malachi_bench -S mix run benchmark/baseline_throughput.exs
RESULT_FILES+=("$(ls -t benchmark/results/throughput_*.json | head -1)")
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2/5 - Latency Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
elixir --sname malachi_bench -S mix run benchmark/baseline_latency.exs
RESULT_FILES+=("$(ls -t benchmark/results/latency_*.json | head -1)")
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3/5 - Memory Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
elixir --sname malachi_bench -S mix run benchmark/baseline_memory.exs
RESULT_FILES+=("$(ls -t benchmark/results/memory_*.json | head -1)")
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4/5 - Connections Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
elixir --sname malachi_bench -S mix run benchmark/baseline_connections.exs
RESULT_FILES+=("$(ls -t benchmark/results/connections_*.json | head -1)")
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5/5 - Authentication Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
elixir --sname malachi_bench -S mix run benchmark/baseline_auth.exs
RESULT_FILES+=("$(ls -t benchmark/results/auth_*.json | head -1)")
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6/6 - Edge Cases Benchmark"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
elixir --sname malachi_bench -S mix run benchmark/baseline_edge_cases.exs
RESULT_FILES+=("$(ls -t benchmark/results/edge_cases_*.json | head -1)")
echo ""

# Consolidate results
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Consolidating results..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Create consolidated JSON
cat > "$CONSOLIDATED_FILE" << EOF
{
  "benchmark": "baseline_consolidated",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "version": "$VERSION",
  "results": {
EOF

# Add each benchmark result
FIRST=true
for file in "${RESULT_FILES[@]}"; do
  if [ -f "$file" ]; then
    BENCHMARK_NAME=$(basename "$file" .json | sed 's/_[0-9]*$//')
    
    if [ "$FIRST" = true ]; then
      FIRST=false
    else
      echo "," >> "$CONSOLIDATED_FILE"
    fi
    
    echo "    \"$BENCHMARK_NAME\": " >> "$CONSOLIDATED_FILE"
    jq '.results' "$file" >> "$CONSOLIDATED_FILE"
  fi
done

# Close JSON
cat >> "$CONSOLIDATED_FILE" << EOF

  }
}
EOF

echo "Consolidated results saved to:"
echo "  $CONSOLIDATED_FILE"
echo ""

# Display summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Benchmark Suite Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Individual results:"
for file in "${RESULT_FILES[@]}"; do
  if [ -f "$file" ]; then
    echo "  - $(basename "$file")"
  fi
done
echo ""
echo "Consolidated report:"
echo "  - $(basename "$CONSOLIDATED_FILE")"
echo ""

# Check if jq is available for pretty printing
if command -v jq &> /dev/null; then
  echo "Quick Summary:"
  echo ""
  jq -r '.results | to_entries[] | "\(.key):"' "$CONSOLIDATED_FILE" 2>/dev/null || true
fi

echo ""
echo "To compare against this baseline in the future, run:"
echo "  ./benchmark/compare_baselines.exs $CONSOLIDATED_FILE <new_result_file>"
echo ""
