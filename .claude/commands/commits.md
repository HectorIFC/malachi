---
description: 'Prepare the commits for the current changes: messages, one patch per commit, and a script the user runs'
argument-hint: "[notes on how to split, optional]"
---

Prepare commits for the changes in this working tree. If $ARGUMENTS says how to split them or what to
leave out, follow it.

Follow the `prepare-commits` skill exactly: check for an earlier set first, account for every change,
group the changes into commits (splitting a file by hunk when it belongs to more than one), build each
commit's tree in a private index, write one patch and one message per commit, prove that the patches
replay on HEAD to the intended tree, and write an executable `commit_message.sh` that commits in order
and then deletes the patches, the messages and itself.

Ask before leaving any change out or folding an unrelated one in. Never run `git commit`, `git push` or
the script: finish by printing the command that runs it, on its own line.
