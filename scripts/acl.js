#!/usr/bin/env node
'use strict';

/**
 * Malachi ACL admin CLI: manage per-topic ACLs over the binary protocol (requires the admin permission).
 *
 * ACLs live in the replicated ACL store, so a change made through any node propagates cluster-wide. An
 * operation is `produce` or `consume`; a pattern is an exact topic ("orders.eu") or a `*`-suffixed prefix
 * ("orders.*" = every topic starting with "orders."). ACLs are enforced when MALACHI_ACL_STRICT is on;
 * otherwise a user's global produce/consume permission already grants every topic and ACLs only add access.
 *
 * Usage:
 *   node acl.js list <username>                       # list a user's ACL grants
 *   node acl.js grant <username> <operation> <pattern>  # grant produce/consume on a topic or prefix
 *   node acl.js revoke <username> <operation> <pattern> # revoke a grant
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_USER, MALACHI_PASS.
 * Default credentials: admin / admin123 (needs the admin permission). Run against a TLS endpoint in prod.
 */

const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'admin', password: 'admin123' });

function usage() {
  console.log(colors.cyan('\nMalachi ACL admin CLI'));
  console.log(colors.gray('   node acl.js list <username>'));
  console.log(colors.gray('   node acl.js grant <username> <operation> <pattern>'));
  console.log(colors.gray('   node acl.js revoke <username> <operation> <pattern>\n'));
  console.log(colors.gray('   operation: produce | consume'));
  console.log(colors.gray('   pattern:   an exact topic ("orders.eu") or a *-suffixed prefix ("orders.*")'));
  console.log(colors.gray('   Auth: MALACHI_USER/MALACHI_PASS (default admin/admin123, needs admin).\n'));
}

async function connect() {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  return client;
}

async function run(cmd, rest) {
  const client = await connect();
  try {
    switch (cmd) {
      case 'list': {
        const [username] = rest;
        if (!username) return usageExit();
        const acls = await client.listAcls(username);
        if (acls.length === 0) {
          console.log(colors.gray('(no acls)'));
        } else {
          for (const acl of acls.sort((a, b) => a.operation.localeCompare(b.operation) || a.resource.localeCompare(b.resource))) {
            console.log(`${colors.bold(acl.operation)}  ${colors.gray(acl.resource)}`);
          }
        }
        break;
      }

      case 'grant': {
        const [username, operation, pattern] = rest;
        if (!username || !operation || !pattern) return usageExit();
        await client.grantAcl(username, operation, pattern);
        console.log(colors.green(`granted ${operation} on "${pattern}" to "${username}"`));
        break;
      }

      case 'revoke': {
        const [username, operation, pattern] = rest;
        if (!username || !operation || !pattern) return usageExit();
        await client.revokeAcl(username, operation, pattern);
        console.log(colors.green(`revoked ${operation} on "${pattern}" from "${username}"`));
        break;
      }

      default:
        return usageExit();
    }
  } finally {
    client.close();
  }
}

function usageExit() {
  usage();
  process.exit(1);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), []);
  if (flags.help) {
    usage();
    process.exit(0);
  }

  const [cmd, ...rest] = positional;
  if (!cmd) return usageExit();

  try {
    await run(cmd, rest);
  } catch (err) {
    fail(err, cfg);
  }
}

main();
