#!/usr/bin/env node
'use strict';

/**
 * Malachi User admin CLI: manage users over the binary protocol (requires the admin permission).
 *
 * Users live in the replicated user store, so a change made through any node propagates cluster-wide.
 *
 * Usage:
 *   node user.js list                                   # list users and their permissions (no hashes)
 *   node user.js create <username> <password>           # create a user (default perms: produce,consume)
 *   node user.js create <u> <p> --perms admin           # create with specific perms (admin|produce|consume)
 *   node user.js passwd <username> <newpassword>        # rotate a user's password
 *   node user.js delete <username>                       # remove a user (revokes it everywhere)
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_USER, MALACHI_PASS.
 * Default credentials: admin / admin123 (needs the admin permission). Passwords cross the wire in the
 * clear, as with the auth handshake, so run against a TLS endpoint in production.
 */

const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'admin', password: 'admin123' });

function usage() {
  console.log(colors.cyan('\nMalachi User admin CLI'));
  console.log(colors.gray('   node user.js list'));
  console.log(colors.gray('   node user.js create <username> <password> [--perms produce,consume]'));
  console.log(colors.gray('   node user.js passwd <username> <newpassword>'));
  console.log(colors.gray('   node user.js delete <username>\n'));
  console.log(colors.gray('   Permissions: admin | produce | consume (comma-separated).'));
  console.log(colors.gray('   Auth: MALACHI_USER/MALACHI_PASS (default admin/admin123, needs admin).\n'));
}

async function connect() {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  return client;
}

async function run(cmd, rest, flags) {
  const client = await connect();
  try {
    switch (cmd) {
      case 'list': {
        const users = await client.listUsers();
        if (users.length === 0) {
          console.log(colors.gray('(no users)'));
        } else {
          for (const u of users.sort((a, b) => a.username.localeCompare(b.username))) {
            console.log(`${colors.bold(u.username)}  ${colors.gray(`[${u.permissions.join(', ')}]`)}`);
          }
        }
        break;
      }

      case 'create': {
        const [username, password] = rest;
        if (!username || !password) return usageExit();
        const perms = String(flags.perms || 'produce,consume')
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
        await client.createUser(username, password, perms);
        console.log(colors.green(`created user "${username}" [${perms.join(', ')}]`));
        break;
      }

      case 'passwd': {
        const [username, newPassword] = rest;
        if (!username || !newPassword) return usageExit();
        await client.changePassword(username, newPassword);
        console.log(colors.green(`changed password for "${username}"`));
        break;
      }

      case 'delete': {
        const [username] = rest;
        if (!username) return usageExit();
        await client.deleteUser(username);
        console.log(colors.green(`deleted user "${username}"`));
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
  const { positional, flags } = parseArgs(process.argv.slice(2), ['perms']);
  if (flags.help) {
    usage();
    process.exit(0);
  }

  const [cmd, ...rest] = positional;
  if (!cmd) return usageExit();

  try {
    await run(cmd, rest, flags);
  } catch (err) {
    fail(err, cfg);
  }
}

main();
