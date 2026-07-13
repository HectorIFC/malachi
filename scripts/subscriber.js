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
 *   node subscriber.js orders --group live --member m1   # server-scoped stream (a share of the ranges)
 *
 * With --member the server scopes the push stream to this member's assigned ranges (run several with the
 * same --group and distinct --member for parallel streaming). A member stream_ack also heartbeats the
 * coordinator; while idle (no records to ack) a periodic empty ack keeps the membership alive.
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: consumer / consumer123 (consume permission).
 */

const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'consumer', password: 'consumer123' });
const DEFAULT_HEARTBEAT_MS = 10_000; // < the coordinator's 30s session timeout (safe margin)

async function run(topic, { group, member, window, max, heartbeatMs }) {
  console.log(colors.cyan('\nMalachi Subscriber (streaming)'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group}   member: ${member || '(none)'}   window: ${window}`));
  console.log(colors.yellow('   Press Ctrl-C to stop\n'));

  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  console.log(colors.green(`Connected (authenticated as ${cfg.username}), streaming...\n`));

  let total = 0;
  let lastAck = Date.now();

  // A member ack doubles as a coordinator heartbeat + range refresh. Acking real records resets the
  // timer; while idle, a periodic empty ack (null cursor, 0 credit) keeps the membership from being
  // evicted on session timeout — closing the idle-member liveness gap.
  const ack = (cursor, count) => {
    client.streamAck(topic, group, member, cursor, count);
    lastAck = Date.now();
  };

  let heartbeat = null;
  if (member) {
    heartbeat = setInterval(() => {
      if (Date.now() - lastAck >= heartbeatMs) ack(null, 0);
    }, heartbeatMs);
    if (heartbeat.unref) heartbeat.unref();
  }

  await client.subscribe(topic, {
    group,
    member,
    window,
    max,
    onRecords: ({ records, cursor }) => {
      for (const rec of records) {
        const key = rec.key === null ? colors.gray('(no key)') : rec.key;
        console.log(colors.green(`[${++total}]`) + ` key=${key}  ${rec.value.toString('utf8')}`);
      }
      // Release the credit we just consumed and durably commit the group's position.
      if (records.length > 0) ack(cursor, records.length);
    },
    onError: (err) => {
      console.error(colors.red(`\nStream error: ${err.message}`));
      if (heartbeat) clearInterval(heartbeat);
      client.close();
      process.exit(1);
    },
  });

  process.on('SIGINT', async () => {
    if (heartbeat) clearInterval(heartbeat);
    // A member leaves the group for a fast rebalance (else the coordinator evicts it on session timeout).
    if (member) {
      try {
        await client.leaveGroup(topic, group, member);
      } catch (_e) {
        // best-effort
      }
    }
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
  --group <g>       Consumer group (default: stream)
  --member <m>      Group member id: scope the stream to this member's share of the ranges
                    (run several with the same --group and distinct --member for parallel streaming)
  --window <n>      Max in-flight (uncommitted) records (default 100)
  --max <n>         Max records per push (default 100)
  --heartbeat <ms>  Idle member-ack interval, member mode only (default 10000)
  -h, --help        Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['group', 'member', 'window', 'max', 'heartbeat']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const group = flags.group || 'stream';
  const member = flags.member || null;
  const window = parseInt(flags.window, 10) || 100;
  const max = parseInt(flags.max, 10) || 100;
  const heartbeatMs = parseInt(flags.heartbeat, 10) || DEFAULT_HEARTBEAT_MS;

  try {
    await run(topic, { group, member, window, max, heartbeatMs });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();

module.exports = { MalachiClient };
