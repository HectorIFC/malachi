#!/usr/bin/env node
'use strict';

/**
 * Malachi Subscriber: server-push streaming on consume streams (./lib/consumer.js): one stream per range,
 * opened on the broker that leads it, which pushes pages within a credit window of --window records; each
 * page read is acked, which returns its credit.
 *
 * Without --group it reads every range from the start, its position held only in this process. With
 * --group it is a member of that consumer group (./lib/group_member.js): the group hands it a share of the
 * topic's ranges, it opens each from the group's checkpoint and commits its position after every page, and
 * it follows every rebalance, so several subscribers with the same --group split the topic between them.
 *
 * Usage:
 *   node subscriber.js [topic]                    # stream every range of <topic>
 *   node subscriber.js orders --group live        # as a member of "live" (shared, resumable)
 *   node subscriber.js orders --window 500        # up to 500 records pushed and unread
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: consumer / consumer123 (consume permission).
 */

const { Router } = require('./lib/router');
const { Consumer } = require('./lib/consumer');
const { colors, config, connOptions, parseArgs, fail, groupMemberOf, joinGroupAs } = require('./lib/cli');
const readLanes = require('./lib/read_lanes');

const cfg = config({ username: 'consumer', password: 'consumer123' });

async function run(topic, { group, member, window, max }) {
  console.log(colors.cyan('\nMalachi Subscriber (streaming)'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group || '(none)'}   member: ${member || '(none)'}   window: ${window}`));
  console.log(colors.yellow('   Press Ctrl-C to stop\n'));

  let total = 0;
  let groupMember = null;
  const fatal = (reason) => {
    console.error(colors.red(`\nStream error: ${reason}`));
    process.exit(1);
  };

  const router = new Router({ host: cfg.host, port: cfg.port });
  const consumer = new Consumer(
    router,
    connOptions(cfg, 30000),
    {
      window,
      max,
      decode: true,
      onRefusal: 'stop',
      startFrom: group ? { committed: group } : 'earliest',
      onEvent: (event) => {
        if (event.kind === 'failed') return fatal(event.reason);
        for (const rec of event.records) {
          const key = rec.key === null ? colors.gray('(no key)') : rec.key;
          console.log(colors.green(`[${++total}]`) + ` key=${key}  ${rec.value.toString('utf8')}`);
        }
        if (groupMember && event.records.length > 0) {
          const [, range] = readLanes.splitKey(event.key);
          groupMember.commit([{ range, position: event.next }]).catch(() => {});
        }
      },
    },
  );

  // a subscriber streams until it is stopped: its join waits for the cluster as long as it takes
  if (group) groupMember = await joinGroupAs({ cfg, router, topic, group, member, reader: consumer, waitMs: Infinity });

  const subscribed = await consumer.subscribe(topic, 30000);
  if (subscribed.error) throw new Error(subscribed.error);
  console.log(colors.green(`Connected (authenticated as ${cfg.username}), streaming...\n`));

  process.on('SIGINT', async () => {
    // a member leaves at once, so its ranges go to the others without waiting out its session
    if (groupMember) await groupMember.leave();
    await consumer.close();
    console.log(colors.cyan(`\nStopped. received=${total}\n`));
    process.exit(0);
  });
}

function help() {
  console.log(`
${colors.cyan('Malachi Subscriber')}, server-push streaming from a topic

${colors.yellow('Usage')}
  node subscriber.js [topic] [options]

${colors.yellow('Options')}
  --group <g>       Consumer group: stream this member's share of the ranges, from the group's
                    checkpoint, committing after each page (run several with the same --group)
  --member <m>      The member id in the group (default: <host>-<pid>)
  --window <n>      Max records pushed and unread (default 100)
  --max <n>         Max records per push (default 100)
  -h, --help        Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['group', 'member', 'window', 'max']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const { group, member } = groupMemberOf(flags);
  const window = parseInt(flags.window, 10) || 100;
  const max = parseInt(flags.max, 10) || 100;

  try {
    await run(topic, { group, member, window, max });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();
