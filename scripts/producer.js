#!/usr/bin/env node
'use strict';

/**
 * Malachi Producer — appends records to a topic over the binary log protocol.
 *
 * Records are addressed by an optional key (the server routes the key to a range); position is never
 * chosen by the client. This replaces the old queue "publish" — there is no queue, only an append log.
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
const { colors, config, parseArgs, fail } = require('./lib/cli');

const cfg = config({ username: 'producer', password: 'producer123' });

function record(i, key) {
  return {
    key: key || `key-${i}`,
    value: JSON.stringify({ id: i, message: `record #${i}`, timestamp: new Date().toISOString() }),
    headers: { 'x-source': 'nodejs-producer', 'x-seq': String(i) },
  };
}

async function connect() {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port });
  await client.connect(cfg.username, cfg.password);
  return client;
}

async function produceBatch(topic, count, key, create) {
  console.log(colors.cyan('\nMalachi Producer'));
  console.log(colors.gray(`   host:  ${cfg.host}:${cfg.port}`));
  console.log(colors.gray(`   user:  ${cfg.username}`));
  console.log(colors.gray(`   topic: ${topic}`));
  console.log(colors.gray(`   count: ${count}${key ? `   key: ${key}` : ''}\n`));

  const client = await connect();
  console.log(colors.green(`Connected (authenticated as ${cfg.username})\n`));

  if (create) {
    await client.createTopic(topic);
    console.log(colors.green(`Topic "${topic}" created\n`));
  }

  const start = Date.now();
  const records = Array.from({ length: count }, (_, i) => record(i + 1, key));
  const appended = await client.produce(topic, records);
  const duration = Date.now() - start;

  client.close();
  console.log(colors.green(`Appended ${appended} record(s)`));
  console.log(colors.gray(`   time: ${duration}ms   rate: ${Math.round((appended / duration) * 1000)} rec/s\n`));
}

async function produceContinuous(topic, key, create) {
  console.log(colors.cyan('\nMalachi Producer (continuous)'));
  console.log(colors.gray(`   topic: ${topic}   user: ${cfg.username}`));
  console.log(colors.yellow('   Press Ctrl-C to stop\n'));

  const client = await connect();
  if (create) await client.createTopic(topic);
  console.log(colors.green(`Connected (authenticated as ${cfg.username})\n`));

  let i = 0;
  let errors = 0;
  const interval = setInterval(async () => {
    i += 1;
    try {
      await client.produce(topic, [record(i, key)]);
      console.log(colors.green(`✓ [${i}]`) + colors.gray(`  ${new Date().toISOString()}`));
    } catch (err) {
      errors += 1;
      console.log(colors.red(`✗ [${i}] ${err.message}`));
    }
  }, 1000);

  process.on('SIGINT', () => {
    clearInterval(interval);
    client.close();
    console.log(colors.cyan(`\nStopped. appended=${i - errors} errors=${errors}\n`));
    process.exit(0);
  });
}

function help() {
  console.log(`
${colors.cyan('Malachi Producer')} — append records to a topic

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

module.exports = { MalachiClient };
