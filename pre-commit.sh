#!/bin/bash

# Pre-commit hook: format staged Elixir files.
# Runs mix format and re-stages any files it reformats.

set -e

# Colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
NC='\033[0m'

echo ""
echo -e "${CYAN}Pre-commit: mix format${NC}"
echo ""

# Get project root using git
PROJECT_ROOT="$(git rev-parse --show-toplevel)"

# Change to project root (needed for mix format)
cd "$PROJECT_ROOT"

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

exit 0
