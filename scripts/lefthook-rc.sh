# Sourced by the git hooks Lefthook installs (wired up by `rc:` in lefthook.yml). Not executable: it is
# read into the hook's shell, not run.
#
# Without this, the generated hook looks for `lefthook` on PATH BEFORE the binary in this clone, so a
# contributor with some other Lefthook installed would run hooks through that one instead of the pinned,
# checksum-verified binary scripts/setup-dev.sh installed (issue #69). Measured, not assumed: the lookup
# order is visible in .git/hooks/pre-commit after `lefthook install`.
#
# Resolved at hook time rather than baked in, so the clone can be moved or copied. If the pinned binary is
# absent (someone manages Lefthook themselves and never ran the setup script), LEFTHOOK_BIN stays unset and
# Lefthook's own lookup takes over unchanged.
_lefthook_root="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -n "$_lefthook_root" ] && [ -x "$_lefthook_root/.lefthook/bin/lefthook" ]; then
  export LEFTHOOK_BIN="$_lefthook_root/.lefthook/bin/lefthook"
fi
unset _lefthook_root
