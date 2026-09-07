#!/usr/bin/env bash
# Fails if any tracked shell entrypoint downloads something and feeds it straight to an interpreter.
#
# This exists because scripts/setup-dev.sh used to do exactly that (issue #69), and the fix is the kind a
# later "simplification" undoes by accident: the safe form is longer than the unsafe one. A comment asking
# people not to do it only works if somebody reads the comment, so CI checks instead.
#
# Two shapes are rejected:
#   1. a download whose output is piped into sh/bash/zsh/ksh/dash/ash, with or without sudo or env
#   2. process substitution feeding the same interpreters
#
# Usage: scripts/check-no-pipe-to-shell.sh [file ...]
# With no arguments it scans this repository's shell entrypoints, Makefile and Dockerfile.
set -euo pipefail

PIPED='(curl|wget)[^|]*\|[[:space:]]*(sudo([[:space:]]+-[A-Za-z]+)*[[:space:]]+)?(env[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*(ba|z|k|da|a)?sh([[:space:]]|$)'
SUBSTITUTED='(ba|z|k|da|a)?sh[[:space:]]+<\([[:space:]]*(curl|wget)'

die() {
  echo "❌ $*" >&2
  exit 1
}

targets=()

if [ "$#" -gt 0 ]; then
  # An unreadable path has to be an error: swallowing it would let a typo in the CI invocation turn this
  # check into a pass that scanned nothing.
  for candidate in "$@"; do
    if [ ! -f "$candidate" ]; then die "not a file: $candidate"; fi
    targets+=("$candidate")
  done
else
  cd "$(cd "$(dirname "$0")/.." && pwd)"
  for candidate in scripts/*.sh pre-commit.sh Makefile Dockerfile; do
    # This guard is the one file allowed to contain the pattern: it is the pattern.
    if [ -f "$candidate" ] && [ "$candidate" != "scripts/check-no-pipe-to-shell.sh" ]; then
      targets+=("$candidate")
    fi
  done
fi

if [ "${#targets[@]}" -eq 0 ]; then die "no files to scan"; fi

# grep exits 1 when it finds nothing, which is the success case here, so the pipeline cannot be allowed to
# abort the script under set -e.
findings="$(grep -nE "$PIPED|$SUBSTITUTED" "${targets[@]}" /dev/null || true)"

if [ -n "$findings" ]; then
  echo "❌ download piped into a shell interpreter:" >&2
  echo "$findings" >&2
  echo "" >&2
  echo "Download to a file, verify its checksum against a pinned release, then run it." >&2
  echo "See scripts/setup-dev.sh for the pattern this repository uses." >&2
  exit 1
fi

echo "✅ no download is piped into a shell interpreter (${#targets[@]} files scanned)"
