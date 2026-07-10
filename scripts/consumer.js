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

const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'consumer', password: 'consumer123' });
const FOLLOW_WAIT_MS = 5000; // server-side long-poll window while following

function printRecord(rec, n) {
  const value = rec.value.toString('utf8');
  const key = rec.key === null ? colors.gray('(no key)') : rec.key;
  console.log(colors.green(`[${n}]`) + ` key=${key}  ${value}`);
}

async function run(topic, { group, max, follow }) {
  console.log(colors.cyan('\nMalachi Consumer'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group || '(none)'}   follow: ${!!follow}\n`));

  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  console.log(colors.green(`Connected (authenticated as ${cfg.username})\n`));

  // An explicit cursor takes precedence over a group resume, so only pass a group on the first fetch
  // (cursor null) and drive by cursor afterwards.
  let cursor = null;
  let total = 0;
  let stop = false;
  process.on('SIGINT', () => {
    stop = true;
    client.close();
    console.log(colors.cyan(`\nStopped. consumed=${total}\n`));
    process.exit(0);
  });

  while (!stop) {
    const waitMs = follow ? FOLLOW_WAIT_MS : 0;
    const opts = cursor === null ? { group, max, waitMs } : { cursor, max, waitMs };
    const { records, cursor: next } = await client.fetch(topic, opts);

    for (const rec of records) printRecord(rec, ++total);
    cursor = next;

    if (group && records.length > 0) await client.commit(topic, group, cursor);

    if (records.length === 0 && !follow) break;
    if (records.length === 0 && follow) process.stdout.write(colors.gray('.'));
  }

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
  --follow        Long-poll for new records until Ctrl-C
  --max <n>       Records per fetch (default 100)
  -h, --help      Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['group', 'max']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const max = parseInt(flags.max, 10) || 100;

  try {
    await run(topic, { group: flags.group || null, max, follow: !!flags.follow });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();

module.exports = { MalachiClient };
