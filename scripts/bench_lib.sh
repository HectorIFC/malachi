# shellcheck shell=bash
# Shared by the harnesses that pin a server and a load generator to cpusets and record what they measured
# (scripts/loadtest-ceiling.sh, benchmark/docker-pipeline.sh, benchmark/netem-hop.sh,
# benchmark/stalled_subscriber_ab.sh).

# How many cores a cpu-list names. Handles single ids (0), ranges (1-3), and strides (0-10:2): Docker's
# cpuset takes the first two forms, taskset all three. Counting comma tokens alone would read 1-3 as ONE
# core and boot the server with a third of its schedulers.
count_cpus() { # count_cpus <cpu-list>
  echo "$1" | tr ',' '\n' | awk -F'[-:]' '
    /^$/ { next }
    NF == 1 { total += 1 }
    NF == 2 { total += $2 - $1 + 1 }
    NF == 3 { total += int(($2 - $1) / $3) + 1 }
    END { print total + 0 }
  '
}

# `+S n:n` for a cpu-list: one scheduler per pinned core.
schedulers_for() { # schedulers_for <cpu-list>
  local n
  n="$(count_cpus "$1")"
  echo "+S $n:$n"
}

# The commit of the tree in the current directory, with -dirty when it has uncommitted or untracked
# changes, or `unknown` outside a git checkout. The whole status is captured before it is tested: piping
# it into `grep -q` under `set -o pipefail` loses -dirty whenever grep exits before git has written
# everything (git then dies of SIGPIPE and the pipeline fails).
tree_label() {
  local ref status
  ref="$(git rev-parse --short HEAD 2> /dev/null)" || {
    echo unknown
    return
  }
  status="$(git status --porcelain 2> /dev/null)"
  if [ -n "$status" ]; then echo "$ref-dirty"; else echo "$ref"; fi
}
