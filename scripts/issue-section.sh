#!/usr/bin/env bash
# Reads one section out of a Malachi issue body, the way every skill that consumes an issue needs it:
#
#   "$(/usr/bin/git rev-parse --show-toplevel)/scripts/issue-section.sh" <section> <issue.md>
#
# branch          the branch named in the `## PR` section: the first line of the first code block after
#                 `**Branch**`, printed only when it is a name git accepts and made of [A-Za-z0-9._/-]
#                 alone (check-ref-format by itself accepts `$(id)` and backticks, which a caller would
#                 then place in a command line).
# pr-description  the first code block after `**Description**` in the `## PR` section, verbatim.
# verification    the `## Verification` section, verbatim, without its leading blank lines.
# plan            the `## Plan` section, the same way.
#
# Every scan tracks code fences FIRST and ignores every marker inside one: an issue that shows what the
# template looks like puts `## PR` and `**Branch**` inside a code block, and a `##` line inside a block
# would otherwise end the section early.
#
# Prints the section and exits 0; exits 3, printing nothing, when the section is missing or blank, or
# (branch) when the name fails the check; exits 2 on a usage error. The body is data: nothing in it is
# ever evaluated.
set -uo pipefail

usage() {
  sed -n '4p' "$0" | sed 's/^# *//' >&2
  exit 2
}

[ $# -eq 2 ] && [ -r "$2" ] || usage
section="$1"
issue="$2"

# The first code block after a bold marker inside the `## PR` section. `first_line` keeps only its first
# non-empty line (the branch); otherwise the whole block is printed.
pr_block() {
  LC_ALL=C awk -v marker="$1" -v first_line="$2" '
    /^```/ { fence = !fence; if (m && !c) { c = 1; next } else if (c) { exit } next }
    c && first_line && NF { print; exit }
    c { if (!first_line) print; next }
    fence { next }
    !p && /^## PR[[:space:]]*$/ { p = 1; next }
    !p { next }
    /^## / { exit }
    !m && index($0, "**" marker "**") == 1 { m = 1 }
  ' "$issue"
}

# A whole `## <title>` section, code blocks included, up to the next `## ` heading outside a block.
heading_section() {
  LC_ALL=C awk -v title="$1" '
    /^```/ { fence = !fence; if (v) print; next }
    fence { if (v) print; next }
    !v && $0 ~ ("^## " title "[[:space:]]*$") { v = 1; next }
    !v { next }
    /^## / { exit }
    { print }
  ' "$issue" | sed -e '/./,$!d'
}

# Written to a file rather than captured with $(...), which would drop trailing newlines: the section
# comes out byte for byte as the scan printed it.
out=$(mktemp "${TMPDIR:-/tmp}/issue-section.XXXXXX") || exit 2
trap 'rm -f "$out"' EXIT

case "$section" in
  branch)
    pr_block Branch 1 > "$out"
    grep -Eqx '[A-Za-z0-9._/-]+' "$out" && [ "$(wc -l < "$out")" -eq 1 ] &&
      git check-ref-format --branch "$(cat "$out")" > /dev/null 2>&1 || exit 3
    ;;
  pr-description) pr_block Description 0 > "$out" ;;
  verification) heading_section Verification > "$out" ;;
  plan) heading_section Plan > "$out" ;;
  *) usage ;;
esac

grep -q '[^[:space:]]' "$out" || exit 3
cat "$out"
