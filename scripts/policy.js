#!/usr/bin/env node
'use strict';

/**
 * Malachi storage policy admin CLI over the binary protocol (requires the admin permission).
 *
 * A policy is one definition for the cluster (retention bounds and a placement spread attribute); a topic
 * is bound to a policy by name. A field left out of `define` inherits the cluster's global value, `--off`
 * turns that rule off, and 0 is a real budget (`--set retention.max_bytes=0` expires every sealed
 * segment).
 *
 * Usage:
 *   node policy.js list
 *   node policy.js define <name> [--set <field>=<value>]... [--off <field>]...
 *   node policy.js delete <name> [--force]
 *   node policy.js bind <topic> <name>
 *   node policy.js unbind <topic>
 *   node policy.js get <topic>
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_USER, MALACHI_PASS.
 * Default credentials: admin / admin123 (needs the admin permission). Run against a TLS endpoint in prod.
 */

const { MalachiClient } = require('./lib/client');
const { colors, config, fail } = require('./lib/cli');
const { POLICY_FIELDS } = require('./lib/wire');

const cfg = config({ username: 'admin', password: 'admin123' });

function usage() {
  console.log(colors.cyan('\nMalachi storage policy admin CLI'));
  console.log(colors.gray('   node policy.js list'));
  console.log(colors.gray('   node policy.js define <name> [--set <field>=<value>]... [--off <field>]...'));
  console.log(colors.gray('   node policy.js delete <name> [--force]'));
  console.log(colors.gray('   node policy.js bind <topic> <name>'));
  console.log(colors.gray('   node policy.js unbind <topic>'));
  console.log(colors.gray('   node policy.js get <topic>\n'));
  console.log(colors.gray(`   fields: ${Object.keys(POLICY_FIELDS).join(', ')}`));
  console.log(colors.gray('   Auth: MALACHI_USER/MALACHI_PASS (default admin/admin123, needs admin).\n'));
}

// argv -> { positional, fields, force }. `--set f=v` and `--off f` may repeat, in the order given.
function parse(argv) {
  const positional = [];
  const fields = [];
  let force = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '-h' || a === '--help') return { help: true };
    if (a === '--force') {
      force = true;
    } else if (a === '--off') {
      fields.push([operand(argv, i, a), null]);
      i++;
    } else if (a === '--set') {
      const assignment = operand(argv, i, a);
      i++;
      const at = assignment.indexOf('=');
      if (at <= 0) throw new Error(`--set takes <field>=<value>, got: ${assignment}`);
      const name = assignment.slice(0, at);
      fields.push([name, parseValue(name, assignment.slice(at + 1))]);
    } else if (a.startsWith('--')) {
      throw new Error(`unknown option: ${a}`);
    } else {
      positional.push(a);
    }
  }
  return { positional, fields, force };
}

// The token after an option that takes one. Missing, or another option in its place (`--off --force`
// would otherwise swallow the force flag as a field name), is refused rather than consumed.
function operand(argv, i, option) {
  const next = argv[i + 1];
  if (next === undefined || next.startsWith('--')) throw new Error(`${option} needs a value`);
  return next;
}

function parseValue(name, raw) {
  const type = POLICY_FIELDS[name] || (/^\d+$/.test(raw) ? 'bound' : 'attribute');
  if (type === 'attribute') return raw;
  if (!/^\d+$/.test(raw)) throw new Error(`invalid value in --set ${name}=${raw}`);
  const n = BigInt(raw);
  return n <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(n) : n;
}

const show = (value) => (value === null ? 'off' : String(value));

async function run(cmd, rest, fields, force) {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  try {
    switch (cmd) {
      case 'list': {
        const policies = await client.listPolicies();
        if (policies.length === 0) console.log(colors.gray('(no policies)'));
        for (const [name, policyFields] of policies) {
          const shown = policyFields.map(([f, v]) => `${f}=${show(v)}`).join(' ') || '(inherits every global value)';
          console.log(`${colors.bold(name)}\t${shown}`);
        }
        break;
      }

      case 'define': {
        const [name] = rest;
        if (!name) return usageExit();
        await client.definePolicy(name, fields);
        console.log(colors.green(`defined policy ${name}`));
        break;
      }

      case 'delete': {
        const [name] = rest;
        if (!name) return usageExit();
        await client.deletePolicy(name, { force });
        console.log(colors.green(`deleted policy ${name}`));
        break;
      }

      case 'bind': {
        const [topic, name] = rest;
        if (!topic || !name) return usageExit();
        await client.bindTopicPolicy(topic, name);
        console.log(colors.green(`bound ${topic} to policy ${name}`));
        break;
      }

      case 'unbind': {
        const [topic] = rest;
        if (!topic) return usageExit();
        await client.bindTopicPolicy(topic, null);
        console.log(colors.green(`detached ${topic} from its policy`));
        break;
      }

      case 'get': {
        const [topic] = rest;
        if (!topic) return usageExit();
        const tp = await client.getTopicPolicy(topic);
        const binding =
          tp.policy === null ? '(none)' : tp.resolution === 'unresolved' ? `${tp.policy} (undefined: this topic holds its data)` : tp.policy;
        console.log(`topic\t${tp.topic}`);
        console.log(`policy\t${binding}`);
        for (const [name, value, origin] of tp.effective) console.log(`${name}\t${show(value)}\t(${origin})`);
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
  let parsed;
  try {
    parsed = parse(process.argv.slice(2));
  } catch (err) {
    console.error(colors.red(`\nError: ${err.message}`));
    usageExit();
  }
  if (parsed.help) {
    usage();
    process.exit(0);
  }

  const [cmd, ...rest] = parsed.positional;
  if (!cmd) return usageExit();

  try {
    await run(cmd, rest, parsed.fields, parsed.force);
  } catch (err) {
    fail(err, cfg);
  }
}

main();
