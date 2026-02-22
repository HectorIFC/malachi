#!/bin/bash

# Pre-commit hook: Update performance baseline
# Executes all benchmarks and stages baseline_reference.json

set -e

# Colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Pre-commit: Running performance benchmarks${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Get project root using git
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
RESULTS_DIR="$PROJECT_ROOT/benchmark/results"
REFERENCE_FILE="$RESULTS_DIR/baseline_reference.json"

# Change to project root (needed for format and benchmarks)
cd "$PROJECT_ROOT"

# --- Format ---
echo -e "${CYAN}Running mix format...${NC}"
mix format

# Re-stage any already-staged Elixir files that were reformatted by mix format
FORMATTED_FILES=$(git diff --name-only | grep -E '\.(ex|exs)$' || true)
if [ -n "$FORMATTED_FILES" ]; then
  while IFS= read -r file; do
    if git diff --cached --name-only | grep -qx "$file"; then
      git add "$file"
      echo "  Reformatted and re-staged: $file"
    fi
  done <<< "$FORMATTED_FILES"
fi
echo -e "${GREEN}✓ Code formatted${NC}"
echo ""

# Check if benchmark changes are being committed
BENCHMARK_CHANGES=$(git diff --cached --name-only | grep -E '^(lib/malachimq/|benchmark/)' || true)

if [ -z "$BENCHMARK_CHANGES" ]; then
  echo -e "${GREEN}✓ No changes in lib/malachimq/ or benchmark/ - skipping benchmark update${NC}"
  echo ""
  exit 0
fi

echo -e "${YELLOW}Changes detected in:${NC}"
echo "$BENCHMARK_CHANGES" | sed 's/^/  - /'
echo ""
echo -e "${YELLOW}Running benchmark suite (this may take ~10 minutes)...${NC}"
echo -e "${YELLOW}To skip: git commit --no-verify${NC}"
echo ""

# Run benchmarks
if ! bash benchmark/run_all_baselines.sh; then
  echo ""
  echo -e "${RED}✗ Benchmark suite failed${NC}"
  echo ""
  echo "To commit anyway:"
  echo "  git commit --no-verify"
  echo ""
  exit 1
fi

# Find latest consolidated baseline
LATEST_BASELINE=$(ls -t "$RESULTS_DIR"/baseline_*.json 2>/dev/null | grep -v "baseline_reference.json" | head -1)

if [ -z "$LATEST_BASELINE" ]; then
  echo ""
  echo -e "${RED}✗ No baseline results found${NC}"
  exit 1
fi

echo ""
echo -e "${CYAN}Updating baseline reference...${NC}"
echo ""

# Copy latest to reference
cp "$LATEST_BASELINE" "$REFERENCE_FILE"
echo "Updated: baseline_reference.json"

# Stage the file
git add "$REFERENCE_FILE"
echo -e "${GREEN}✓ Staged baseline_reference.json${NC}"

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ Benchmark update complete!${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

exit 0
