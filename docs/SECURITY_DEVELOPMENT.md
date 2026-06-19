# Security Development Guide

Guide for maintaining security practices during Malachi development.

## Running Security Audits Locally

```bash
# Check for vulnerable dependencies
mix deps.audit

# Run Sobelow security analyzer
mix sobelow --config --exit

# Check code quality
mix credo --strict

# Check for outdated dependencies
mix hex.outdated

# Check for unused dependencies
mix deps.unlock --check-unused
```

## Running Security Tests

```bash
# Run all security tests
mix test test/security_xss_test.exs test/comprehensive_security_test.exs test/attack_simulation_test.exs test/injection_attack_test.exs test/input_fuzzing_test.exs test/protocol_fuzzing_test.exs test/dependency_security_test.exs test/security_performance_regression_test.exs

# Run attack simulation tests only
mix test test/attack_simulation_test.exs

# Run fuzzing tests only
mix test test/input_fuzzing_test.exs test/protocol_fuzzing_test.exs

# Run injection prevention tests
mix test test/injection_attack_test.exs

# Run security performance regression tests
mix test test/security_performance_regression_test.exs

# Run dependency security tests
mix test test/dependency_security_test.exs

# Run TLS-related tests
mix test test/tls_config_test.exs test/tls_validator_test.exs test/tls_enforcement_test.exs test/tls_metrics_test.exs

# Run OWASP Top 10 alignment tests
mix test test/comprehensive_security_test.exs
```

## Running Performance Benchmarks

```bash
# Run all baseline benchmarks
bash benchmark/run_all_baselines.sh

# Run individual benchmarks
mix run benchmark/baseline_throughput.exs
mix run benchmark/baseline_latency.exs
mix run benchmark/baseline_memory.exs
mix run benchmark/baseline_connections.exs
mix run benchmark/baseline_auth.exs
mix run benchmark/baseline_edge_cases.exs
mix run benchmark/baseline_sustained_load.exs

# Run security-specific benchmarks
mix run benchmark/security_performance_suite.exs
mix run benchmark/tls_performance_benchmark.exs
mix run benchmark/rate_limiting_benchmark.exs
mix run benchmark/validation_benchmark.exs
mix run benchmark/dashboard_security_benchmark.exs

# Compare against reference baseline (detects regressions)
mix run benchmark/compare_baselines.exs benchmark/results/baseline_reference.json benchmark/results/baseline_LATEST.json
```

## Pre-Commit Hooks Setup

```bash
# Install pre-commit (macOS)
brew install pre-commit

# Or with pip
pip install pre-commit

# Install hooks in the repository
pre-commit install

# Run hooks manually on all files
pre-commit run --all-files
```

### What the hooks check:

- **gitleaks** - Prevents committing secrets/credentials
- **large files** - Warns on files > 500KB
- **merge conflicts** - Detects unresolved merge conflict markers
- **private keys** - Detects private key files
- **mix format** - Ensures Elixir code is formatted (pre-commit)
- **mix credo** - Runs static analysis (pre-push)

## Updating Dependencies

```bash
# Update a specific dependency
mix deps.update jason

# Update all dependencies
mix deps.update --all

# Verify no vulnerabilities
mix deps.audit

# Run tests
mix test
```

## Responding to Vulnerabilities

### If a vulnerability is found in a dependency:

1. Check severity (CVSS score)
2. Update the affected dependency: `mix deps.update <package>`
3. Run `mix deps.audit` to verify no new vulnerabilities
4. Run full test suite: `mix test`
5. Create a PR with the fix

### If a vulnerability is reported in Malachi:

1. Assess severity and impact
2. Create a fix on a private branch
3. Write tests to prevent regression
4. Release a patch version
5. Publish a GitHub Security Advisory

## PR Security Checklist

Before submitting a PR, verify:

- [ ] No hardcoded secrets or credentials
- [ ] Input validation for any new user-facing inputs
- [ ] No new dependencies with known vulnerabilities (`mix deps.audit`)
- [ ] Security tests pass (`mix test test/security_*.exs test/tls_*.exs`)
- [ ] Code passes Sobelow scan (`mix sobelow --config --exit`)
- [ ] No GPL-licensed dependencies added
- [ ] TLS configurations use secure defaults
- [ ] Rate limiting tested for new endpoints
- [ ] Backpressure behavior verified under load
- [ ] No atom creation from untrusted input
- [ ] Benchmark suite passes without regressions (< 5% degradation)

## Security Tools Reference

| Tool | Purpose | Command |
|------|---------|---------|
| mix_audit | Dependency vulnerability scanning | `mix deps.audit` |
| Sobelow | Elixir/Phoenix security analysis | `mix sobelow --config --exit` |
| Credo | Code quality and safety checks | `mix credo --strict` |
| Gitleaks | Secret detection in git history | `gitleaks detect` |
| Trivy | Container and filesystem scanning | `trivy fs .` |
| CodeQL | Advanced code analysis | GitHub Actions |
| Dependabot | Automated dependency updates | Configured in `.github/dependabot.yml` |
| Benchee | Performance benchmarking | `mix run benchmark/*.exs` |

## CI/CD Security Pipeline

The security pipeline runs automatically via GitHub Actions (`security.yml`):

- **On every PR to main:** Sobelow, dependency audit, secret scanning, Trivy filesystem scan
- **On push to main:** All of the above + Docker image scan
- **Daily at 2 AM UTC:** Full security scan (catches newly disclosed vulnerabilities)

Results are visible in:
- GitHub Actions workflow runs
- GitHub Security tab (Trivy SARIF uploads)
- PR comments (dependency review)
