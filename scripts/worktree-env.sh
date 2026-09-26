#!/usr/bin/env bash
# Gives one git worktree of this repository its own development environment, written to worktree.env at
# the worktree's root: five host ports, the dev node's data directories and node name, and a compose
# project name. The start-issue-work skill runs it for every new worktree, and every session in that
# worktree loads the file before any `mix` or `docker` command:
#
#   set -a; . ./worktree.env; set +a
#
# Usage: scripts/worktree-env.sh <issue number> <worktree directory>
#
# The ports are 20000 + 10 * <issue number>, plus 0 to 4, so a port names its worktree. They stay below
# 32768, where Linux starts handing out ephemeral ports (macOS starts at 49152): a dev node must never be
# given a port the kernel may also give a random outgoing connection. That caps the issue number at 1276,
# and a larger one is refused rather than wrapped around.
#
# This is a tool for the operator's host, not part of Malachi. Its port check (`lsof`) says whether a port
# was free at the moment the file was written; it reserves nothing, and it says nothing about the broker,
# which only ever runs, and is only ever measured, on Linux.
#
# Never in the main checkout: its compose volume is named after its directory (malachi_malachi-data), and
# a compose project name there would orphan that volume with the data inside.
set -euo pipefail

usage() {
  echo "usage: $0 <issue number> <worktree directory>" >&2
  exit 64
}

[ "$#" -eq 2 ] || usage
issue=$1
dir=$2

case "$issue" in
  '' | *[!0-9]* | 0*) echo "refusing: issue number must be a positive integer, got '$issue'" >&2; exit 64 ;;
esac

base=$((20000 + 10 * issue))
if [ $((base + 4)) -ge 32768 ]; then
  echo "refusing: issue $issue would get ports $base-$((base + 4)), inside the Linux ephemeral range" \
    "(32768 and up); the formula serves issues up to 1276" >&2
  exit 65
fi

if ! root=$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null); then
  echo "refusing: $dir is not inside a git checkout" >&2
  exit 66
fi

# A linked worktree has a git directory of its own under the common one; the main checkout's two are the
# same directory.
git_dir=$(git -C "$root" rev-parse --absolute-git-dir)
common_dir=$(cd "$root" && cd "$(git rev-parse --git-common-dir)" && pwd -P)
if [ "$(cd "$git_dir" && pwd -P)" = "$common_dir" ]; then
  echo "refusing: $root is the main checkout, not a linked worktree; it keeps no worktree.env" >&2
  exit 66
fi

command -v lsof >/dev/null 2>&1 || { echo "refusing: lsof is required to check the ports" >&2; exit 69; }

names=(MALACHI_TCP_PORT MALACHI_DASHBOARD_PORT JAEGER_UI_PORT OTLP_PORT PROMETHEUS_PORT)

# Prints one line per port some process is listening on: the variable, the port, the command and its pid.
taken_ports() {
  local i port holder
  for i in "${!names[@]}"; do
    port=$((base + i))
    holder=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -Fpc 2>/dev/null |
      awk '/^p/ { pid = substr($0, 2) } /^c/ { print substr($0, 2) " (pid " pid ")"; exit }') || true
    if [ -n "$holder" ]; then
      echo "  ${names[$i]}=$port is held by $holder"
    fi
  done
}

env_file="$root/worktree.env"
dashboard_port=$((base + 1))

if [ -e "$env_file" ]; then
  # Kept, never rewritten: a session may already be running on these values. A port held now is most
  # likely this worktree's own dev node, so it is reported, not refused.
  echo "kept existing $env_file"
  taken=$(taken_ports)
  if [ -n "$taken" ]; then
    echo "warning: some of its ports are in use right now:"
    echo "$taken"
  fi
  echo "dashboard: http://127.0.0.1:$(sed -n 's/^MALACHI_DASHBOARD_PORT=//p' "$env_file")"
  exit 0
fi

taken=$(taken_ports)
if [ -n "$taken" ]; then
  echo "refusing: ports for issue $issue are already in use; nothing was written" >&2
  echo "$taken" >&2
  exit 75
fi

tmp="$env_file.tmp.$$"
{
  echo "# Written by scripts/worktree-env.sh for issue $issue. Load with: set -a; . ./worktree.env; set +a"
  for i in "${!names[@]}"; do
    echo "${names[$i]}=$((base + i))"
  done
  echo "MALACHI_LOG_DATA_DIR=$root/tmp/data/log"
  echo "MALACHI_RA_DATA_DIR=$root/tmp/data/ra"
  echo "MALACHI_NODE=malachi_$issue@127.0.0.1"
  echo "COMPOSE_PROJECT_NAME=malachi-$issue"
} >"$tmp"
mv "$tmp" "$env_file"

echo "wrote $env_file"
echo "dashboard: http://127.0.0.1:$dashboard_port"
