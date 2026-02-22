# Git Hooks with Lefthook

This project uses [Lefthook](https://github.com/evilmartians/lefthook) to manage git hooks. This ensures all developers have the same hooks configured automatically.

## Installation

Run the setup script after cloning the repository:

```bash
./scripts/setup-dev.sh
```

This will:
1. Install Lefthook (if not already installed)
2. Install git hooks from `lefthook.yml`
3. Configure the pre-commit hook

## Hooks Configuration

### Pre-commit Hook

**Location:** `pre-commit.sh` (root of repository)

**Behavior:**
- Detects changes in `lib/malachimq/` or `benchmark/`
- If changes detected:
  - Runs full benchmark suite (~10 minutes)
  - Updates `baseline_reference.json`
  - Stages the updated file automatically
- If no changes in those directories: skips execution

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
    update-baseline:
      run: bash pre-commit.sh
      stage_fixed: true
```

## Troubleshooting

### Lefthook not found
Re-run the setup script:
```bash
./scripts/setup-dev.sh
```

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
