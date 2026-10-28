#!/usr/bin/env node
'use strict';

/**
 * Malachi Consumer: pulls records from a topic with fetch_range (./lib/fetcher.js), one range at a time on
 * the broker that leads it. For server-push use subscriber.js.
 *
 * Without --group it reads every range from the start, its position held only in this process. With
 * --group it is a member of that consumer group (./lib/group_member.js): the group hands it a share of the
 * topic's ranges, it starts each from the group's checkpoint and commits its position after every page,
 * and it follows every rebalance, so several members with the same --group split the topic between them
 * and a member that restarts resumes where the group left off.
 *
 * Usage:
 *   node consumer.js [topic]                # read the topic once from the start
 *   node consumer.js orders --group workers # as a member of "workers", committing after each page
 *   node consumer.js orders --follow        # keep reading new records until Ctrl-C (the broker long-polls)
 *   node consumer.js orders --max 50        # up to 50 records per fetch
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: consumer / consumer123 (consume permission).
 */

const { Router } = require('./lib/router');
const { Fetcher } = require('./lib/fetcher');
const { colors, config, connOptions, parseArgs, fail, sleep, groupMemberOf, joinGroupAs } = require('./lib/cli');

const cfg = config({ username: 'consumer', password: 'consumer123' });
// How long one pass over the ranges may be held while following: split across them by the Fetcher, at
// least 200 ms a range, so past 25 ranges a pass is held 200 ms a range.
const FOLLOW_WAIT_MS = 5000;
// How long to wait before the next fetch after one that served no page: routes held back, a dropped
// connection, no broker that took one, or (following) no range the group handed this member.
const RETRY_MS = 200;
// Answers that mean no broker took the fetch (the range's broker, or every one the routes are read from):
// waited out, as a broker that restarts comes back, and so is a group join no broker answers. A follower
// waits as long as it takes; a run that reads once gives up once UNREACHABLE_WAIT_MS passed since the first
// of those was asked, waiting then for the attempt still on its way (./client.js bounds each connect, and
// then its authentication, at 15 s; a request at its timeout).
const UNREACHABLE = new Set(['connect_failed', 'metadata_unavailable']);
const UNREACHABLE_WAIT_MS = 10000;

function printRecord(rec, n) {
  const value = rec.value.toString('utf8');
  const key = rec.key === null ? colors.gray('(no key)') : rec.key;
  console.log(colors.green(`[${n}]`) + ` key=${key}  ${value}`);
}

async function run(topic, { group, member, max, follow }) {
  console.log(colors.cyan('\nMalachi Consumer'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   topic: ${topic}   group: ${group || '(none)'}   member: ${member || '(none)'}   follow: ${!!follow}\n`));

  const router = new Router({ host: cfg.host, port: cfg.port });
  const fetcher = new Fetcher(router, connOptions(cfg, FOLLOW_WAIT_MS + 30000), {
    max,
    atTail: 'wait',
    waitMs: follow ? FOLLOW_WAIT_MS : 0,
    startFrom: group ? { committed: group } : 'earliest',
  });
  // the join waits for the cluster as the reading does
  const unreachableWaitMs = follow ? Infinity : UNREACHABLE_WAIT_MS;
  const groupMember = group ? await joinGroupAs({ cfg, router, topic, group, member, reader: fetcher, waitMs: unreachableWaitMs }) : null;
  console.log(colors.green(`Connected (authenticated as ${cfg.username})\n`));

  let total = 0;
  let stop = false;
  const finish = async () => {
    if (groupMember) await groupMember.leave();
    await fetcher.close();
  };

  process.on('SIGINT', async () => {
    stop = true;
    await finish();
    console.log(colors.cyan(`\nStopped. consumed=${total}\n`));
    process.exit(0);
  });

  // Once every range read in a row brought nothing, the topic is read through (unless following).
  let emptyInARow = 0;
  let unreachableSince = null; // when the first of the fetches in a row that no broker took was asked
  while (!stop) {
    const asked = Date.now();
    const answer = await fetcher.fetchPage(topic);
    if (answer.kind === 'error' && UNREACHABLE.has(answer.reason)) {
      unreachableSince ??= asked;
      if (Date.now() - unreachableSince >= unreachableWaitMs) throw new Error(answer.reason);
      await sleep(RETRY_MS);
      continue;
    }
    if (answer.kind === 'error') throw new Error(answer.reason);
    unreachableSince = null;
    // reading once, a member the group handed nothing has nothing to read
    if (answer.kind === 'unassigned' && !follow) {
      console.log(colors.gray('   the group handed this member no range'));
      break;
    }
    // a page of a range that left the assignment while it was fetched: dropped, the next one asked at once
    if (answer.kind === 'revoked') continue;
    if (answer.kind !== 'page') {
      await sleep(answer.kind === 'held' ? answer.ms : RETRY_MS);
      continue;
    }
    const { page } = answer;
    for (const rec of page.records) printRecord(rec, ++total);
    if (page.records.length > 0) {
      emptyInARow = 0;
      if (groupMember) await groupMember.commit([{ range: page.range, position: page.to }]);
    } else {
      emptyInARow += 1;
      const ranges = groupMember ? groupMember.assignment.ranges.length : page.active.length;
      if (!follow && emptyInARow >= ranges) break;
      if (follow && emptyInARow >= ranges) process.stdout.write(colors.gray('.'));
    }
  }

  await finish();
  console.log(colors.cyan(`\nDone. consumed=${total}\n`));
}

function help() {
  console.log(`
${colors.cyan('Malachi Consumer')}, pull records from a topic

${colors.yellow('Usage')}
  node consumer.js [topic] [options]

${colors.yellow('Options')}
  --group <g>     Consumer group: read this member's share of the ranges, from the group's checkpoint,
                  committing after each page (run several with the same --group to split the topic)
  --member <m>    The member id in the group (default: <host>-<pid>)
  --follow        Keep reading new records until Ctrl-C
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
  const { group, member } = groupMemberOf(flags);

  try {
    await run(topic, { group, member, max, follow: !!flags.follow });
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();
