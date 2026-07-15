#!/usr/bin/env node
'use strict';

/**
 * Malachi Consumer — pulls records from a topic over the binary log protocol.
 *
 * The client drives its own position with an opaque cursor: each fetch returns a batch plus the next
 * cursor to pass back. With --group, position is committed server-side so a restart resumes where it left
 * off (the old queue ack/nack is gone — commit is by cursor, per consumer group). This replaces the old
 * queue "subscribe": here the client polls; for server-push use subscriber.js.
 *
 * Usage:
 *   node consumer.js [topic]                # drain the topic once from the start
 *   node consumer.js orders --group workers # resume the "workers" group, committing after each batch
 *   node consumer.js orders --follow        # keep long-polling for new records until Ctrl-C
 *   node consumer.js orders --max 50        # up to 50 records per fetch
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: consumer / consumer123 (consume permission).
 */

const { MalachiClient, isNotOwner, isMigrating } = require('./lib/client');
const { colors, config, parseArgs, fail, sleep, withRetry } = require('./lib/cli');

const cfg = config({ username: 'consumer', password: 'consumer123' });
const FOLLOW_WAIT_MS = 5000; // server-side long-poll window while following
const NOT_OWNER_RETRY_MS = 200; // back-off before retrying a member fetch during a coordinator failover

function printRecord(rec, n) {
  const value = rec.value.toString('utf8');
  const key = rec.key === null ? colors.gray('(no key)') : rec.key;
  console.log(colors.green(`[${n}]`) + ` key=${key}  ${value}`);
}

async function run(topic, { group, member, max, follow }) {
  console.log(colors.cyan('\nMalachi Consumer'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group || '(none)'}   member: ${member || '(none)'}   follow: ${!!follow}\n`));

  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  console.log(colors.green(`Connected (authenticated as ${cfg.username})\n`));

  // In member mode the server scopes each fetch to this member's assigned ranges and drives the position
  // (we always pass {group, member} and commit). Otherwise an explicit cursor takes precedence over a
  // group resume, so we pass the group only on the first fetch and drive by cursor after.
  let cursor = null;
  let total = 0;
  let stop = false;

  const leave = async () => {
    if (member) {
      try {
        await client.leaveGroup(topic, group, member);
      } catch (_e) {
        // best-effort — the coordinator evicts on session timeout anyway
      }
    }
  };

  process.on('SIGINT', async () => {
    stop = true;
    await leave();
    client.close();
    console.log(colors.cyan(`\nStopped. consumed=${total}\n`));
    process.exit(0);
  });

  while (!stop) {
    const waitMs = follow ? FOLLOW_WAIT_MS : 0;

    const opts = member
      ? { group, member, max, waitMs }
      : cursor === null
        ? { group, max, waitMs }
        : { cursor, max, waitMs };

    let records, next;

    try {
      ({ records, cursor: next } = await client.fetch(topic, opts));
    } catch (err) {
      // a member fetch routed to a coordinator mid-failover: back off and retry (the server re-resolves)
      if (isNotOwner(err)) {
        process.stdout.write(colors.gray('~'));
        await sleep(NOT_OWNER_RETRY_MS);
        continue;
      }

      throw err;
    }

    for (const rec of records) printRecord(rec, ++total);
    cursor = next;

    // retry the commit if the topic is mid-migration (a vnode split); it must land before advancing
    if (group && records.length > 0) {
      await withRetry(() => client.commit(topic, group, cursor), isMigrating, NOT_OWNER_RETRY_MS, () =>
        process.stdout.write(colors.gray('~')),
      );
    }

    if (records.length === 0 && !follow) break;
    if (records.length === 0 && follow) process.stdout.write(colors.gray('.'));
  }

  await leave();
  client.close();
  console.log(colors.cyan(`\nDone. consumed=${total}\n`));
}

function help() {
  console.log(`
${colors.cyan('Malachi Consumer')} — pull records from a topic

${colors.yellow('Usage')}
  node consumer.js [topic] [options]

${colors.yellow('Options')}
  --group <g>     Consumer group: resume and commit position server-side
  --member <m>    Group member id: the server assigns this member a share of the topic's ranges
                  (run several with the same --group and distinct --member for parallel consumption)
  --follow        Long-poll for new records until Ctrl-C
  --max <n>       Records per fetch (default 100)
  -h, --help      Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['group', 'member', 'max']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const max = parseInt(flags.max, 10) || 100;
  const group = flags.group || null;
  const member = flags.member || null;

  if (member && !group) {
    console.error(colors.red('--member requires --group (a member belongs to a consumer group)'));
    process.exit(1);
  }

  try {
    await run(topic, { group, member, max, follow: !!flags.follow });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();

module.exports = { MalachiClient };
