#!/usr/bin/env bash
# Pins down the diff a review looks at, so every reviewer in this repository reads the same bytes. The
# adversarial-review and pr-agent-review skills both call it, from the checkout they run in:
#
#   "$(/usr/bin/git rev-parse --show-toplevel)/scripts/pin-diff.sh" pin [--base <rev>] <outdir>
#   ".../scripts/pin-diff.sh" render <diff.patch>
#   ".../scripts/pin-diff.sh" split <rendered> <outdir> [max-bytes] [max-groups]
#
# pin    Everything the branch changes: its commits since it left origin/main (or since --base), what is
#        modified, and what is untracked, apart from the files the prepare-commits skill generates at the
#        root (commit_message.sh, commit_<n>.patch, commit_message.txt, commit_message_<n>.txt), dropped
#        by exact name and only while untracked: a file of that name the project committed is its own.
#        Writes <outdir>/diff.patch, <outdir>/files.txt (one path per line) and <outdir>/base.txt.
#        Exit 0 when there is something to review, 3 when there is nothing, 2 on a usage error, 1 when
#        git failed (a file it cannot read, say): the files it wrote are then incomplete, and a caller
#        stops rather than review them.
# render The patch in the hunk format PR-Agent's prompts expect (pr_agent/settings/prompt_fragments.toml,
#        diff_hunk_format): per file a "## File: 'path'" header, per hunk a numbered "__new hunk__" and an
#        "__old hunk__" only when the hunk removes something. A deleted file, a binary file, an empty
#        new file and a pure rename are one line each.
# split  The rendered diff in groups of whole files, at most max-bytes each (default 100000) and at most
#        max-groups of them (default 5), a lib/<x>.ex kept with its test/<x>_test.exs. Writes
#        <outdir>/group-<n>.txt, and <outdir>/unreviewed.txt with the files that did not fit (empty when
#        all did). A single file larger than max-bytes gets a group of its own rather than being cut.
#
# Reads nothing it is given as a command: the paths in a diff are data, and only go to files.
set -uo pipefail

# git reads these ahead of the directory it runs in, so a call from inside a git hook would otherwise
# look at another repository.
unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE

usage() {
  sed -n '5,7p' "$0" | sed 's/^# *//' >&2
  exit 2
}

# The untracked names prepare-commits writes at the root. Exact names, never a wildcard: a pattern would
# also hide a real file that happens to match, such as a commit_message.ex.
generated='^(commit_message\.sh|commit_[0-9]+\.patch|commit_message\.txt|commit_message_[0-9]+\.txt)$'

pin() {
  local base="" outdir="" base_given=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --base)
        [ $# -ge 2 ] || usage
        base="$2"
        base_given=1
        shift 2
        ;;
      -*) usage ;;
      *)
        [ -z "$outdir" ] || usage
        outdir="$1"
        shift
        ;;
    esac
  done
  [ -n "$outdir" ] || usage
  # An empty --base is what a caller passes when the merge-base it computed failed. Taking it as no
  # --base would review against origin/main instead, quietly, so it is refused.
  if [ "$base_given" = 1 ] && [ -z "$base" ]; then
    echo "pin-diff: --base is empty" >&2
    exit 2
  fi

  mkdir -p "$outdir" || exit 2
  outdir=$(cd "$outdir" && pwd) || exit 2

  local top
  top=$(git rev-parse --show-toplevel 2> /dev/null) || {
    echo "pin-diff: not inside a git work tree" >&2
    exit 2
  }
  cd "$top" || exit 2

  if [ -n "$base" ]; then
    base=$(git rev-parse --verify --quiet "${base}^{commit}") || {
      echo "pin-diff: --base is not a commit" >&2
      exit 2
    }
  elif git rev-parse --verify --quiet HEAD > /dev/null; then
    base=$(git merge-base HEAD origin/main 2> /dev/null) || base=$(git rev-parse HEAD)
  else
    # No commit yet: everything is new, compared against the empty tree.
    base=$(git hash-object -t tree /dev/null)
  fi
  printf '%s\n' "$base" > "$outdir/base.txt"

  # The same diff whatever the contributor's git config says. Each setting that could change it is
  # overridden once, and test/scripts/pin_diff_test.exs guards each: --no-color (color.ui, color.diff),
  # --src-prefix and --dst-prefix (diff.mnemonicPrefix, diff.noprefix; render strips exactly a/ and b/),
  # diff.suppressBlankEmpty (render counts blank context lines as " "), --no-ext-diff (diff.external),
  # and the cd to the top above (diff.relative). core.quotePath=false keeps non-ASCII names unescaped;
  # a name with a double quote, a backslash or a control character is still C-quoted in the headers,
  # render unquotes it, and files.txt comes from -z output, which is never quoted.
  local g=(git -c core.quotePath=false -c diff.suppressBlankEmpty=false)
  local d=(diff --no-ext-diff --no-color --src-prefix=a/ --dst-prefix=b/)

  "${g[@]}" "${d[@]}" "$base" -- > "$outdir/diff.patch" || exit 1
  "${g[@]}" diff --no-ext-diff --name-only -z "$base" -- | tr '\0' '\n' > "$outdir/files.txt"
  [ "${PIPESTATUS[0]}" -eq 0 ] || exit 1

  # git ls-files only warns about an untracked directory it cannot open, and succeeds, so its warnings are
  # kept and any of them fails the pin: otherwise the files in that directory would silently be left out.
  local name warnings="$outdir/.ls-files-warnings"
  while IFS= read -r -d '' name; do
    [[ "$name" =~ $generated ]] && continue
    printf '%s\n' "$name" >> "$outdir/files.txt"
    # --no-index exits 1 when the files differ, which here they always do.
    "${g[@]}" "${d[@]}" --no-index -- /dev/null "$name" >> "$outdir/diff.patch"
    [ $? -le 1 ] || exit 1
  done < <(git ls-files -z --others --exclude-standard 2> "$warnings")
  if [ -s "$warnings" ]; then
    cat "$warnings" >&2
    rm -f "$warnings"
    exit 1
  fi
  rm -f "$warnings"

  [ -s "$outdir/files.txt" ] || exit 3
  exit 0
}

render() {
  [ $# -eq 1 ] && [ -r "$1" ] || usage
  LC_ALL=C awk '
    function flush_file() {
      if (!infile) return
      if (!printed) {
        if (deleted) printf "## File %c%s%c was deleted\n\n", 39, name, 39
        else if (binary) printf "## File %c%s%c is binary, not shown\n\n", 39, name, 39
        else if (renamed_from != "") printf "## File %c%s%c renamed from %c%s%c, content unchanged\n\n", 39, name, 39, 39, renamed_from, 39
        else if (created) printf "## File %c%s%c added, empty\n\n", 39, name, 39
        else printf "## File %c%s%c mode changed, content unchanged\n\n", 39, name, 39
      }
      infile = 0
    }
    function header() {
      if (!printed) { printf "## File: %c%s%c\n\n", 39, name, 39; printed = 1 }
    }
    function flush_hunk(   i) {
      if (!inhunk) return
      if (!deleted) {
        header()
        print hunk_header
        print "__new hunk__"
        for (i = 1; i <= nn; i++) print newl[i]
        if (removed) {
          print "__old hunk__"
          for (i = 1; i <= no; i++) print oldl[i]
        }
        print ""
      }
      inhunk = 0
    }
    # A name git C-quoted back to the name itself: git quotes a name with a double quote, a backslash or
    # a control character, writing \a \b \t \n \v \f \r \" \\ and three-digit octal for the rest
    # (non-ASCII bytes too, when core.quotePath is on, which pin turns off).
    function unquote(p,   out, i, c, code) {
      if (p !~ /^".*"$/) return p
      p = substr(p, 2, length(p) - 2)
      out = ""
      for (i = 1; i <= length(p); i++) {
        c = substr(p, i, 1)
        if (c == "\\" && i < length(p)) {
          c = substr(p, ++i, 1)
          if (c ~ /[0-7]/) {
            code = (c + 0) * 64 + (substr(p, i + 1, 1) + 0) * 8 + (substr(p, i + 2, 1) + 0)
            i += 2
            c = sprintf("%c", code)
          }
          else if (c == "a") c = sprintf("%c", 7)
          else if (c == "b") c = sprintf("%c", 8)
          else if (c == "t") c = "\t"
          else if (c == "n") c = "\n"
          else if (c == "v") c = sprintf("%c", 11)
          else if (c == "f") c = sprintf("%c", 12)
          else if (c == "r") c = sprintf("%c", 13)
        }
        out = out c
      }
      return out
    }
    # A path from a "--- a/x" or "+++ b/x" line: the tab git appends after a name that contains a
    # space goes, the quoting goes, and then the prefix.
    function strip(p) {
      sub(/\t$/, "", p)
      p = unquote(p)
      sub(/^[ab]\//, "", p)
      return p
    }
    # In a hunk every line starts with its marker, so a content line can never look like a header.
    inhunk {
      c = substr($0, 1, 1)
      if (c == "\\") next
      if (c == " " || c == "+") { newl[++nn] = newno " " $0; newno++; nleft-- }
      if (c == " " || c == "-") { oldl[++no] = $0; oleft-- }
      if (c == "-") removed = 1
      if (nleft <= 0 && oleft <= 0) flush_hunk()
      next
    }
    /^diff --git / {
      flush_file()
      infile = 1; printed = 0; deleted = 0; binary = 0; created = 0; renamed_from = ""
      # "diff --git a/P b/P": for a file that is not renamed both halves are the same path, so the
      # second half is exactly the back half of the line, spaces in the name included.
      rest = substr($0, 12)
      name = strip(substr(rest, int(length(rest) / 2) + 2))
      next
    }
    !infile { next }
    /^deleted file mode / { deleted = 1; next }
    /^new file mode / { created = 1; next }
    /^rename from / { renamed_from = unquote(substr($0, 13)); next }
    /^rename to / { name = unquote(substr($0, 11)); next }
    /^Binary files / { binary = 1; next }
    /^--- / { if ($0 != "--- /dev/null") oldname = strip(substr($0, 5)); next }
    /^\+\+\+ / {
      if ($0 == "+++ /dev/null") { deleted = 1; name = oldname } else name = strip(substr($0, 5))
      next
    }
    /^@@ / {
      # @@ -a[,b] +c[,d] @@ context: a count left out is 1.
      split($2, o, ","); split($3, n, ",")
      oleft = (2 in o) ? o[2] + 0 : 1
      nleft = (2 in n) ? n[2] + 0 : 1
      newno = substr(n[1], 2) + 0
      hunk_header = $0; nn = 0; no = 0; removed = 0; inhunk = 1
      if (nleft <= 0 && oleft <= 0) flush_hunk()
      next
    }
    END { flush_hunk(); flush_file() }
  ' "$1"
}

split_groups() {
  [ $# -ge 2 ] && [ $# -le 4 ] && [ -r "$1" ] || usage
  local rendered="$1" outdir="$2" max_bytes="${3:-100000}" max_groups="${4:-5}"
  case "$max_bytes$max_groups" in *[!0-9]*) usage ;; esac
  [ "$max_bytes" -gt 0 ] && [ "$max_groups" -gt 0 ] || usage
  mkdir -p "$outdir/sections" || exit 2
  rm -f "$outdir"/group-*.txt "$outdir/unreviewed.txt"

  # One file per rendered file section, and an index line per section: its unit key, its position, its
  # size and its path. A lib/<x>.ex and a test/<x>_test.exs share the key <x>, so sorting by key puts a
  # module next to its test, and sorting by position keeps the diff's own order inside a unit.
  LC_ALL=C awk -v dir="$outdir/sections" '
    function close_section() {
      if (!file) return
      close(file)
      key = path
      if (key ~ /^lib\/.*\.ex$/) { sub(/^lib\//, "", key); sub(/\.ex$/, "", key) }
      else if (key ~ /^test\/.*_test\.exs$/) { sub(/^test\//, "", key); sub(/_test\.exs$/, "", key) }
      printf "%s\t%06d\t%d\t%s\n", key, idx, bytes, path
      file = ""
    }
    /^## File/ {
      close_section()
      idx++; file = sprintf("%s/%06d", dir, idx); bytes = 0
      path = $0
      sub(/^## File:? \047/, "", path); sub(/\047.*$/, "", path)
    }
    file { print > file; bytes += length($0) + 1 }
    END { close_section() }
  ' "$rendered" | LC_ALL=C sort -t "$(printf '\t')" -k1,1 -k2,2 > "$outdir/index.tsv"

  # Whole units, in key order, packed greedily: a unit that does not fit in the current group opens the
  # next one, and once max-groups are full everything left is listed as unreviewed.
  LC_ALL=C awk -F '\t' -v dir="$outdir" -v max="$max_bytes" -v groups="$max_groups" '
    function place(   i, size) {
      if (!nu) return
      size = 0
      for (i = 1; i <= nu; i++) size += ubytes[i]
      if (g == 0 || (used + size > max && used > 0)) { g++; used = 0 }
      if (g > groups) {
        for (i = 1; i <= nu; i++) print upath[i] > (dir "/unreviewed.txt")
      } else {
        for (i = 1; i <= nu; i++) {
          while ((getline line < (dir "/sections/" uidx[i])) > 0) print line > (dir "/group-" g ".txt")
          close(dir "/sections/" uidx[i])
        }
        used += size
      }
      nu = 0
    }
    $1 != key { place(); key = $1 }
    { nu++; uidx[nu] = $2; ubytes[nu] = $3; upath[nu] = $4 }
    END { place() }
  ' "$outdir/index.tsv"
  : >> "$outdir/unreviewed.txt"
  rm -rf "$outdir/sections" "$outdir/index.tsv"
}

[ $# -ge 1 ] || usage
command="$1"
shift
case "$command" in
  pin) pin "$@" ;;
  render) render "$@" ;;
  split) split_groups "$@" ;;
  *) usage ;;
esac
