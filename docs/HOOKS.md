# Git Hooks with Lefthook

This project uses [Lefthook](https://github.com/evilmartians/lefthook) to manage git hooks. This ensures all developers have the same hooks configured automatically.

## Installation

Run the setup script after cloning the repository:

```bash
./scripts/setup-dev.sh
```

This will:
1. Download the pinned Lefthook release into `.lefthook/bin/` (gitignored) and verify its SHA-256
2. Install git hooks from `lefthook.yml`
3. Configure the pre-commit hook

No `sudo`, no package manager, and nothing written outside the working copy.

### How the install is verified

The script downloads a **pinned** release binary from GitHub and checks its SHA-256 against
`scripts/lefthook.checksums`, a byte-for-byte copy of the checksum file published with that release, before
it unpacks or runs anything. A mismatch, a missing checksum line, or a machine without `sha256sum`/`shasum`
all abort the setup rather than install something unverified.

It used to pipe a remote setup script straight into `sudo bash`, which handed anyone controlling that URL
(or holding a TLS-stripping network position) root on a contributor's laptop. That is
[issue #69](https://github.com/HectorIFC/malachi/issues/69). The safe form is longer than the unsafe one, so
CI runs `scripts/check-no-pipe-to-shell.sh` to keep it from being tidied back.

### Bumping the pinned version

Change `LEFTHOOK_VERSION` at the top of `scripts/setup-dev.sh`, then replace the checksum table wholesale:

```bash
curl -fsSL https://github.com/evilmartians/lefthook/releases/download/v<version>/lefthook_checksums.txt \
  -o scripts/lefthook.checksums
```

The vendored copy deliberately drops the `.txt` extension: `.gitignore` ignores `*.txt` repo-wide, so a file
named `lefthook_checksums.txt` would be silently untracked.

### Prefer your own Lefthook?

The script does not consult a Lefthook on `PATH`: pinning a version and then running whatever happens to be
installed would be decorative pinning. If you manage Lefthook yourself (Homebrew, `go install`, a distro
package), skip the script and run `lefthook install` from the repository root instead.

## Hooks Configuration

### Pre-commit Hook

**Location:** `pre-commit.sh` (root of repository)

**Behavior:**
- Runs `mix format`
- Re-stages any already-staged `.ex`/`.exs` files that were reformatted

**Skip the hook:**
```bash
git commit --no-verify
```

## Manual Lefthook Commands

```bash
# Install/reinstall hooks
lefthook install

# Run all hooks manually
lefthook run pre-commit

# Uninstall hooks
lefthook uninstall
```

## Configuration File

The `lefthook.yml` file in the root defines all hooks. Edit this file to modify hook behavior.

Example:
```yaml
pre-commit:
  commands:
    format:
      run: bash pre-commit.sh
      stage_fixed: true
```

## Troubleshooting

### Lefthook not found
Re-run the setup script:
```bash
./scripts/setup-dev.sh
```

It is idempotent: with the pinned version already in `.lefthook/bin/`, it re-installs the hooks without
downloading anything.

### Checksum mismatch during setup
The download did not match the pinned release, and nothing was installed. Retry once in case the transfer was
truncated; if it persists, do not work around it. Either the pinned version and `scripts/lefthook.checksums`
disagree (someone bumped one without the other) or the artifact is not what upstream published.

### Hooks not running
Reinstall hooks:
```bash
lefthook install
```

### Want to disable hooks temporarily
Use `--no-verify`:
```bash
git commit --no-verify -m "your message"
```

## Why Lefthook?

- **Versionable**: Hooks are tracked in git, not in `.git/hooks/`
- **Cross-platform**: Works on macOS, Linux, Windows
- **Fast**: Written in Go, parallel execution
- **Simple**: YAML configuration, no complex scripts
- **Team-friendly**: Everyone gets the same hooks automatically

## Links

- [Lefthook Documentation](https://github.com/evilmartians/lefthook)
- [Lefthook Install Guide](https://github.com/evilmartians/lefthook#install)
