#!/usr/bin/env bash
# Runs the Concuerror scenario (test/concuerror/) over the compiled Malachi beams, bounded so a
# spike cannot run forever. Requires scripts/concuerror-setup.sh to have built the escript.
#
# Usage: scripts/concuerror.sh [scheduling_bound] [timeout_seconds]
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)" || exit 1

BOUND="${1:-2}"
LIMIT="${2:-600}"
BIN=.concuerror/_build/default/bin/concuerror
[ -x "$BIN" ] || { echo "run scripts/concuerror-setup.sh first" >&2; exit 1; }

MIX_ENV=dev mix compile >/dev/null || exit 1
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
# The report outlives the scratch dir: an interleaving trace is the whole point of a failing run.
REPORT=.concuerror/report.txt
elixirc --ignore-module-conflict -o "$OUT" test/concuerror/replicate_race.ex >/dev/null 2>&1 ||
  { echo "scenario failed to compile" >&2; exit 1; }

# Every dependency's ebin plus Elixir's own, since Concuerror instruments what it loads. Elixir's
# ebin is asked of the VM, not derived from `which elixir` (asdf and friends install a shim there,
# and a wrong path is skipped silently, leaving the scenario module uninstrumented).
ELIXIR_EBIN=$(elixir -e 'IO.puts(:code.lib_dir(:elixir) |> Path.join("ebin"))' 2>/dev/null)
PA_ARGS=""
for dir in _build/dev/lib/*/ebin "$OUT" "$ELIXIR_EBIN"; do
  [ -d "$dir" ] && PA_ARGS="$PA_ARGS --pa $dir"
done

echo "running Concuerror (scheduling_bound=$BOUND, wall limit ${LIMIT}s)"
echo "NOTE: this currently stops on a tool limitation, not a Malachi bug. See the spike verdict"
echo "      in docs/ARCHITECTURE.md (What we do not replicate)."
# shellcheck disable=SC2086
timeout "$LIMIT" "$BIN" $PA_ARGS \
  -m Elixir.Malachi.Concuerror.ReplicateRace -t replicate_race \
  --scheduling_bound "$BOUND" --after_timeout 50 --treat_as_normal shutdown \
  --output "$REPORT" 2>&1 | grep -vE '^\[|^ *(none|[0-9]+) \|' | tail -12
status=${PIPESTATUS[0]}

if [ "$status" -eq 124 ]; then
  echo "TIMED OUT after ${LIMIT}s (state space did not close)"
  exit 124
fi

if grep -q "file_server_2" "$REPORT" 2>/dev/null; then
  echo
  echo "VERDICT: blocked by Concuerror, not by Malachi. The replication server is disk-backed by"
  echo "design, and Concuerror's file_server emulation does not implement read_file_info, so the"
  echo "exploration cannot start. Re-run this script to re-check after a Concuerror release that"
  echo "supports it, or once the storage layer gains a filesystem-free seam."
fi

echo "report: $REPORT"
exit "$status"
