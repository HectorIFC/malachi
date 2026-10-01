<!--
Upstream: pr_agent/settings/prompt_fragments.toml @ 10bbd9a4 (https://github.com/The-PR-Agent/pr-agent)
Copyright (c) 2026 The PR Agent. MIT License, full text in LICENSE-pr-agent beside this file.
Changes from upstream:
- Rendered with include_line_numbers=true and include_ai_metadata=false, the only combination this port
  uses; the Jinja conditionals are resolved.
- One item added at the end for the one-line forms scripts/pin-diff.sh render writes for a deleted,
  binary, empty, renamed or mode-only file, where upstream writes one only for a deleted file.
This text is the {{ diff_hunk_format }} slot of the other prompts.
-->
======
## File: 'src/file1.py'

@@ ... @@ def func1():
__new hunk__
11  unchanged code line0
12  unchanged code line1
13 +new code line2 added
14  unchanged code line3
__old hunk__
 unchanged code line0
 unchanged code line1
-old code line2 removed
 unchanged code line3

@@ ... @@ def func2():
__new hunk__
21  unchanged code line4
22 +new code line5 added
23  unchanged code line6

## File: 'src/file2.py'
...
======

- Each code chunk is split into separate '__new hunk__' and '__old hunk__' sections. The '__new hunk__' section
  shows the code chunk after the PR changes. The '__old hunk__' section shows the code chunk before the PR changes
  and is omitted when the chunk contains no removed code.
- Line numbers appear before the change marker in '__new hunk__' sections to help you refer to specific lines.
  These numbers are for reference only and are not part of the code. '__old hunk__' sections are not numbered.
- Change markers describe how each line differs: '+' marks added code and appears only in '__new hunk__', '-'
  marks removed code and appears only in '__old hunk__', and ' ' marks unchanged context that appears in both.
- A file the PR deletes, a binary file, an empty new file, a file only renamed and a file whose mode alone
  changed appear as a single line such as "## File 'x' was deleted", "## File 'x' is binary, not shown",
  "## File 'x' added, empty", "## File 'x' renamed from 'y', content unchanged" or
  "## File 'x' mode changed, content unchanged". Read the file itself from the checkout when you need its
  content.
