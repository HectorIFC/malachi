#!/usr/bin/env node
'use strict';

/**
 * Malachi Producer: appends records to a topic on producer streams (./lib/producer.js).
 *
 * Each record goes to the range its key hashes into, on a stream opened at the broker that leads that
 * range; position is never chosen by the client. A record without --key gets key-<n>.
 *
 * Usage:
 *   node producer.js [topic] [count]        # append <count> records (default 10) to <topic>
 *   node producer.js orders 100 --key user  # all records share key "user" (same range, ordered)
 *   node producer.js orders --create        # create the topic first, then append
 *   node producer.js orders --continuous    # append 1 record/second until Ctrl-C
 *
 * Environment: MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS.
 * Default credentials: producer / producer123 (produce permission; also allows create-topic).
 */

const { MalachiClient } = require('./lib/client');
const { Router } = require('./lib/router');
const { Producer } = require('./lib/producer');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'producer', password: 'producer123' });
// How long the appends handed over may take to be answered before the run gives up on them.
const ANSWER_TIMEOUT_MS = 30000;

function record(i, key) {
  return {
    key: key || `key-${i}`,
    value: JSON.stringify({ id: i, message: `record #${i}`, timestamp: new Date().toISOString() }),
    headers: { 'x-source': 'nodejs-producer', 'x-seq': String(i) },
  };
}

async function createTopic(topic) {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  try {
    await client.createTopic(topic);
  } finally {
    client.close();
  }
}

function newProducer() {
  return new Producer(
    new Router({ host: cfg.host, port: cfg.port }),
    { user: cfg.username, pass: cfg.password, timeout: ANSWER_TIMEOUT_MS },
    { maxInflight: 16, label: 'malachi-producer-cli' },
  );
}

// Every failed append, its reason with how many records it carried.
function failures(events) {
  const reasons = {};
  for (const e of events) if (e.kind === 'failed') reasons[e.reason] = (reasons[e.reason] || 0) + e.entries.length;
  return reasons;
}

async function produceBatch(topic, count, key, create) {
  console.log(colors.cyan('\nMalachi Producer'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   user:  ${cfg.username}`));
  console.log(colors.gray(`   topic: ${topic}`));
  console.log(colors.gray(`   count: ${count}${key ? `   key: ${key}` : ''}\n`));

  if (create) {
    await createTopic(topic);
    console.log(colors.green(`Topic "${topic}" created\n`));
  }

  const producer = newProducer();
  const start = Date.now();
  try {
    await producer.produce(
      topic,
      Array.from({ length: count }, (_, i) => ({ tag: i + 1, record: record(i + 1, key) })),
    );
    const outcome = await producer.wait(ANSWER_TIMEOUT_MS);
    const events = producer.takeEvents();
    const appended = events.filter((e) => e.kind === 'acked').reduce((acc, e) => acc + e.entries.length, 0);
    const duration = Math.max(1, Date.now() - start);
    console.log(colors.green(`Appended ${appended} record(s)`));
    console.log(colors.gray(`   time: ${duration}ms   rate: ${Math.round((appended / duration) * 1000)} rec/s\n`));
    const refused = failures(events);
    if (Object.keys(refused).length > 0 || outcome === 'timeout') {
      const why = outcome === 'timeout' ? { ...refused, unanswered: count - appended } : refused;
      throw new Error(`not every record was appended: ${JSON.stringify(why)}`);
    }
  } finally {
    await producer.close();
  }
}

async function produceContinuous(topic, key, create) {
  console.log(colors.cyan('\nMalachi Producer (continuous)'));
  console.log(colors.gray(`   topic: ${topic}   user: ${cfg.username}`));
  console.log(colors.yellow('   Press Ctrl-C to stop\n'));

  if (create) await createTopic(topic);
  let appended = 0;
  let errors = 0;
  const producer = new Producer(
    new Router({ host: cfg.host, port: cfg.port }),
    { user: cfg.username, pass: cfg.password, timeout: ANSWER_TIMEOUT_MS },
    {
      label: 'malachi-producer-cli',
      onEvent: (e) => {
        for (const { tag } of e.entries) {
          if (e.kind === 'acked') {
            appended += 1;
            console.log(colors.green(`✓ [${tag}]`) + colors.gray(`  ${new Date().toISOString()}`));
          } else {
            errors += 1;
            console.log(colors.red(`✗ [${tag}] ${e.reason}`));
          }
        }
      },
    },
  );

  let i = 0;
  const interval = setInterval(() => {
    i += 1;
    producer.produce(topic, [{ tag: i, record: record(i, key) }]).catch((err) => console.log(colors.red(`✗ [${i}] ${err.message}`)));
  }, 1000);

  process.on('SIGINT', async () => {
    clearInterval(interval);
    await producer.wait(5000, (p) => !p.inFlight());
    await producer.close();
    console.log(colors.cyan(`\nStopped. appended=${appended} errors=${errors}\n`));
    process.exit(0);
  });
}

function help() {
  console.log(`
${colors.cyan('Malachi Producer')}, append records to a topic

${colors.yellow('Usage')}
  node producer.js [topic] [count] [options]

${colors.yellow('Options')}
  --key <k>       Use a fixed key for every record (default: key-<n>)
  --create        Create the topic before appending
  --continuous    Append 1 record/second until Ctrl-C
  -h, --help      Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_TOPIC, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2), ['key']);
  if (flags.help) return help();

  const topic = positional[0] || cfg.topic;
  const count = parseInt(positional[1], 10) || 10;

  try {
    if (flags.continuous) {
      await produceContinuous(topic, flags.key, flags.create);
    } else {
      await produceBatch(topic, count, flags.key, flags.create);
    }
  } catch (err) {
    fail(err, cfg);
  }
}

if (require.main === module) main();
