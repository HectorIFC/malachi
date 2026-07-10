#!/usr/bin/env node
'use strict';

/**
 * Malachi Subscriber — server-push streaming over the binary log protocol.
 *
 * subscribe opens a push stream for a consumer group, flow-controlled by a credit window: the server
 * pushes up to `window` in-flight records, and each streamAck durably commits the group's position and
 * returns that many records of credit. This replaces the old channel pub/sub — channels are gone; a
 * topic + consumer group with server-push is the real-time equivalent.
 *
 * Usage:
 *   node subscriber.js [topic]                    # stream <topic> as group "stream"
 *   node subscriber.js orders --group live        # named consumer group (shared, resumable)
 *   node subscriber.js orders --window 500        # allow up to 500 in-flight records
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: consumer / consumer123 (consume permission).
 */

const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'consumer', password: 'consumer123' });

async function run(topic, { group, window, max }) {
  console.log(colors.cyan('\nMalachi Subscriber (streaming)'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group}   window: ${window}`));
  console.log(colors.yellow('   Press Ctrl-C to stop\n'));

  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  console.log(colors.green(`Connected (authenticated as ${cfg.username}), streaming...\n`));

  let total = 0;
  await client.subscribe(topic, {
    group,
    window,
    max,
    onRecords: ({ records, cursor }) => {
      for (const rec of records) {
        const key = rec.key === null ? colors.gray('(no key)') : rec.key;
        console.log(colors.green(`[${++total}]`) + ` key=${key}  ${rec.value.toString('utf8')}`);
      }
      // Release the credit we just consumed and durably commit the group's position.
      if (records.length > 0) client.streamAck(topic, group, cursor, records.length);
    },
    onError: (err) => {
      console.error(colors.red(`\nStream error: ${err.message}`));
      client.close();
      process.exit(1);
    },
  });

  process.on('SIGINT', () => {
    client.close();
    console.log(colors.cyan(`\nStopped. received=${total}\n`));
    process.exit(0);
  });
}

function help() {
  console.log(`
${colors.cyan('Malachi Subscriber')} — server-push streaming from a topic

${colors.yellow('Usage')}
  node subscriber.js [topic] [options]

${colors.yellow('Options')}
  --group <g>     Consumer group (default: stream)
  --window <n>    Max in-flight (uncommitted) records (default 100)
  --max <n>       Max records per push (default 100)
  -h, --help      Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['group', 'window', 'max']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const group = flags.group || 'stream';
  const window = parseInt(flags.window, 10) || 100;
  const max = parseInt(flags.max, 10) || 100;

  try {
    await run(topic, { group, window, max });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();

module.exports = { MalachiClient };
