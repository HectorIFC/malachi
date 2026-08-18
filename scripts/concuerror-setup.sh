#!/usr/bin/env bash
# Builds the Concuerror escript into .concuerror/ (gitignored). Concuerror is not a hex package
# and is not needed to build or test Malachi: it is only used by the systematic-interleaving
# spike (scripts/concuerror.sh). Uses the rebar3 that Mix already manages (mix local.rebar) so no
# extra toolchain is required.
#
# Usage: scripts/concuerror-setup.sh
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

TARGET=.concuerror
REBAR3=$(ls -d "$HOME"/.mix/elixir/*/rebar3 2>/dev/null | tail -1 || true)

if [ -z "$REBAR3" ]; then
  echo "rebar3 not found; run: mix local.rebar --force" >&2
  exit 1
fi

if [ ! -d "$TARGET/src" ]; then
  echo "cloning Concuerror into $TARGET"
  git clone --depth 1 https://github.com/parapluu/Concuerror.git "$TARGET"
fi

echo "building the escript with $REBAR3"
(cd "$TARGET" && PATH="$(dirname "$REBAR3"):$PATH" rebar3 escriptize >/dev/null 2>&1)

BIN="$TARGET/_build/default/bin/concuerror"
[ -x "$BIN" ] || { echo "build produced no escript at $BIN" >&2; exit 1; }
echo "ready: $BIN ($("$BIN" --version 2>&1 | head -1))"
