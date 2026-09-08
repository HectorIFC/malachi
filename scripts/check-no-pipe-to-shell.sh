#!/usr/bin/env bash
# Fails if any tracked shell entrypoint downloads something and feeds it straight to an interpreter.
#
# This exists because scripts/setup-dev.sh used to do exactly that (issue #69), and the fix is the kind a
# later "simplification" undoes by accident: the safe form is longer than the unsafe one. A comment asking
# people not to do it only works if somebody reads the comment, so CI checks instead.
#
# Four shapes are rejected, which is the family of "a download becomes code that runs":
#   1. the download piped into sh/bash/zsh/ksh/dash/ash, however it is spelled: with or without sudo or
#      env, with flags, with a leading path, and with intermediate stages in the pipeline
#   2. process substitution feeding an interpreter, both attached and via a stdin redirect
#   3. the same, but sourced into the current shell with `source` or `.`
#   4. the download taken as a command substitution argument, as in eval or an interpreter's -c
#
# SCOPE, stated so the next reader does not mistake it for more than it is: this catches the common
# spellings of the anti-pattern, which is what a tidy-up would reintroduce. It is not a sandbox and it
# cannot prove absence. A determined author can always write the download to a file and run that file,
# and no line-based check will see it.
#
# Usage: scripts/check-no-pipe-to-shell.sh [file ...]
# With no arguments it scans this repository's shell entrypoints, Makefile and Dockerfile.
set -euo pipefail

# Built from named parts because the whole expression is unreadable in one line, and an unreadable guard
# is one nobody notices a hole in. PATH_PREFIX is what made `curl ... | /bin/bash` slip through the first
# version: a command can be written with or without a leading path.
PATH_PREFIX='([A-Za-z0-9_.~/-]*/)?'
# Long, short and the bare `--` terminator: `env -i`, `env --ignore-environment`, `env -- bash`.
FLAGS='([[:space:]]+-[-A-Za-z0-9]*)*'
SUDO="(${PATH_PREFIX}sudo${FLAGS}[[:space:]]+)?"
ENV="(${PATH_PREFIX}env${FLAGS}[[:space:]]+)?"
ASSIGNMENTS='([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*'
SHELL_NAME="${PATH_PREFIX}"'(ba|z|k|da|a)?sh'
DOWNLOAD='(curl|wget)'
# `<(cmd)` attached to the interpreter, and `< <(cmd)` feeding its stdin.
PROCESS_SUB='([[:space:]]*<)?[[:space:]]*<\([[:space:]]*'

# `.*` rather than `[^|]*` before the pipe: an intermediate stage (`| tac | bash`) is the same hazard
# wearing a hat.
PIPED="${DOWNLOAD}"'.*\|[[:space:]]*'"${SUDO}${ENV}${ASSIGNMENTS}${SHELL_NAME}"'([[:space:]]|$)'
SOURCED='(source|\.)[[:space:]]+'"${PROCESS_SUB}${DOWNLOAD}"
# eval, or an interpreter given the download as an argument, which is how `sh -c "$(curl ...)"` reads.
# `[^;&|#]*` rather than `.*`: the substitution has to be an argument of THAT command, not of some later
# one on the same line, or `sh script.sh; echo "$(curl ...)"` would read as a finding.
SUBSTITUTION_ARG="(eval|${SHELL_NAME})${FLAGS}"'[[:space:]]+[^;&|#]*(\$\(|`)[[:space:]]*'"${DOWNLOAD}"
SUBSTITUTED="${SHELL_NAME}${PROCESS_SUB}${DOWNLOAD}"

# Known limit, stated rather than papered over: grep is line-based, so a pipeline split across lines with a
# trailing backslash is not detected. Joining continuations first would cost the line numbers that make this
# report actionable, and a reintroduction by tidy-up arrives on one line, not folded over two.

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
findings="$(grep -nE "$PIPED|$SUBSTITUTED|$SOURCED|$SUBSTITUTION_ARG" "${targets[@]}" /dev/null || true)"

if [ -n "$findings" ]; then
  echo "❌ a download reaches a shell interpreter:" >&2
  echo "$findings" >&2
  echo "" >&2
  echo "Download to a file, verify its checksum against a pinned release, then run it." >&2
  echo "See scripts/setup-dev.sh for the pattern this repository uses." >&2
  exit 1
fi

echo "✅ no download reaches a shell interpreter (${#targets[@]} files scanned)"
