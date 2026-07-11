#!/usr/bin/env node
'use strict';

/**
 * Malachi load-test harness — a closed-loop load generator over the binary log protocol.
 *
 * N connections each run a scenario in a tight `op -> await` loop until the deadline, saturating the
 * server; the harness reports throughput (ops/s, records/s, MB/s) and latency percentiles. This is the
 * external, end-to-end view (real TCP + serialization + auth), reusing the reference client in ./lib.
 *
 * The op (`runOp`) is deliberately separate from the driver (`closedLoop`) so an open-loop driver
 * (fixed arrival rate) can reuse the same ops later.
 *
 * Usage:
 *   node loadtest.js --scenario produce --connections 20 --duration 10
 *   node loadtest.js --scenario fetch --prepopulate 50000 --max 200
 *   node loadtest.js --scenario stream --connections 4 --window 500
 *   node loadtest.js --scenario mixed --connections 20 --record-size 512 --keys 1000
 *
 * Common flags: --topic, --batch, --record-size, --keys, --max, --window, --prepopulate, --warmup,
 *   --samples (reservoir size), --json. Default credentials: app / app123 (produce + consume).
 */

const { performance } = require('perf_hooks');
const { MalachiClient } = require('./lib/client');
const { colors, config, parseArgs, fail } = require('./lib/cli');

const SCENARIOS = ['produce', 'fetch', 'stream', 'mixed'];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Exact count/sum/min/max plus a bounded reservoir for percentiles: the reservoir keeps memory flat on
// long runs, while min/max stay exact (a reservoir would clip the tail). Latency is in milliseconds.
class Stats {
  constructor(maxSamples) {
    this.count = 0;
    this.sum = 0;
    this.min = Infinity;
    this.max = -Infinity;
    this.errors = 0;
    this.records = 0;
    this.bytes = 0;
    this.samples = [];
    this.maxSamples = maxSamples;
    this.seen = 0;
  }

  record(latencyMs, records, bytes) {
    this.count += 1;
    this.sum += latencyMs;
    this.records += records;
    this.bytes += bytes;
    if (latencyMs < this.min) this.min = latencyMs;
    if (latencyMs > this.max) this.max = latencyMs;

    // Reservoir sampling (Vitter's Algorithm R): every observed latency has an equal chance of being kept.
    this.seen += 1;
    if (this.samples.length < this.maxSamples) {
      this.samples.push(latencyMs);
    } else {
      const j = Math.floor(Math.random() * this.seen);
      if (j < this.maxSamples) this.samples[j] = latencyMs;
    }
  }

  error() {
    this.errors += 1;
  }

  percentiles(ps) {
    const sorted = this.samples.slice().sort((a, b) => a - b);
    const at = (p) => {
      if (sorted.length === 0) return 0;
      const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
      return sorted[Math.max(0, idx)];
    };
    return Object.fromEntries(ps.map((p) => [p, at(p)]));
  }

  mean() {
    return this.count === 0 ? 0 : this.sum / this.count;
  }
}

const cfg = config({ username: 'app', password: 'app123' });

function connect() {
  const client = new MalachiClient({ host: cfg.host, port: cfg.port, timeout: 30000 });
  return client.connect(cfg.username, cfg.password);
}

// A reusable value buffer of the requested size (the codec copies it, so sharing across records is safe).
function makeValue(size) {
  return Buffer.alloc(size, 0x61); // 'a'
}

function recordBytes(records) {
  return records.reduce((acc, r) => acc + r.value.length, 0);
}

// ---- ops (one unit of work; return { records, bytes }) ----

function produceOp(opts) {
  const value = makeValue(opts.recordSize);
  let seq = 0;
  return async (client) => {
    const batch = Array.from({ length: opts.batch }, () => {
      seq += 1;
      return { key: `key-${seq % opts.keys}`, value };
    });
    const n = await client.produce(opts.topic, batch);
    return { records: n, bytes: value.length * batch.length };
  };
}

function fetchOp(opts) {
  return async (client, ctx) => {
    const { records, cursor } = await client.fetch(opts.topic, { cursor: ctx.cursor, max: opts.max });
    // Drained: rewind to the start so the worker keeps generating fetch load against the backlog.
    ctx.cursor = records.length === 0 ? null : cursor;
    return { records: records.length, bytes: recordBytes(records) };
  };
}

// ---- drivers ----

// Closed-loop: each client loops its op until the deadline. Latency is measured per op.
async function closedLoop(clients, makeOpFor, deadline, stats) {
  await Promise.all(
    clients.map(async (client, index) => {
      const op = makeOpFor(index);
      const ctx = { cursor: null };
      while (performance.now() < deadline) {
        const t0 = performance.now();
        try {
          const { records, bytes } = await op(client, ctx);
          stats.record(performance.now() - t0, records, bytes);
        } catch (err) {
          stats.error();
          if (err.message === 'not connected' || /closed/.test(err.message)) break;
          // Back off briefly so a persistent error (e.g. a permission or topic issue) throttles instead
          // of pegging a core and inflating the error count with a tight retry spin.
          await sleep(5);
        }
      }
    })
  );
}

// Streaming is server-push, not request/await: subscribe and count pushed records for the duration.
// Latency per push is not comparable to a round-trip, so the stream report is throughput-only.
let streamRun = 0;
async function streamDriver(clients, opts, durationMs, stats) {
  // A fresh group per invocation so each run reads the backlog from the start: a warmup pass commits (acks)
  // its way to the end of the backlog, so sharing a group with the measured run would leave it nothing.
  const run = streamRun++;
  await Promise.all(
    clients.map(
      (client, index) =>
        new Promise((resolve) => {
          const group = `loadtest-${run}-${index}`;
          const timer = setTimeout(resolve, durationMs);
          const stop = () => {
            clearTimeout(timer);
            resolve();
          };
          client
            .subscribe(opts.topic, {
              group,
              window: opts.window,
              max: opts.max,
              onRecords: ({ records, cursor }) => {
                if (records.length === 0) return;
                stats.record(0, records.length, recordBytes(records));
                client.streamAck(opts.topic, group, cursor, records.length);
              },
              onError: () => {
                stats.error();
                stop();
              },
            })
            .catch(() => {
              stats.error();
              stop();
            });
        })
    )
  );
}

// Appends `count` records up front so fetch/stream/mixed have a backlog to read.
async function prepopulate(topic, count, recordSize, keys) {
  if (count <= 0) return;
  const client = await connect();
  const value = makeValue(recordSize);
  const CHUNK = 1000;
  let done = 0;
  while (done < count) {
    const n = Math.min(CHUNK, count - done);
    const batch = Array.from({ length: n }, (_, i) => ({ key: `key-${(done + i) % keys}`, value }));
    await client.produce(topic, batch);
    done += n;
  }
  client.close();
  console.log(colors.gray(`   prepopulated ${done} records`));
}

// ---- reporting ----

function report(scenario, opts, elapsedMs, stats) {
  const secs = elapsedMs / 1000;
  const p = stats.percentiles([50, 90, 95, 99]);
  const streaming = scenario === 'stream';

  if (opts.json) {
    console.log(
      JSON.stringify(
        {
          scenario,
          connections: opts.connections,
          duration_s: Number(secs.toFixed(3)),
          topic: opts.topic,
          operations: stats.count,
          ops_per_s: Math.round(stats.count / secs),
          records: stats.records,
          records_per_s: Math.round(stats.records / secs),
          bytes: stats.bytes,
          mb_per_s: Number((stats.bytes / 1e6 / secs).toFixed(3)),
          errors: stats.errors,
          latency_ms: streaming
            ? null
            : {
                min: round(stats.min),
                mean: round(stats.mean()),
                p50: round(p[50]),
                p90: round(p[90]),
                p95: round(p[95]),
                p99: round(p[99]),
                max: round(stats.max),
                samples: stats.samples.length,
              },
        },
        null,
        2
      )
    );
    return;
  }

  console.log(colors.cyan('\nMalachi load test'));
  console.log(colors.gray(`   scenario:    ${scenario}`));
  console.log(colors.gray(`   connections: ${opts.connections}`));
  console.log(colors.gray(`   duration:    ${secs.toFixed(1)}s`));
  console.log(colors.gray(`   topic:       ${opts.topic}\n`));

  console.log(colors.bold('Throughput'));
  console.log(`   operations:  ${stats.count}  (${colors.green(Math.round(stats.count / secs))} ops/s)`);
  console.log(`   records:     ${stats.records}  (${colors.green(Math.round(stats.records / secs))} rec/s)`);
  console.log(`   data:        ${(stats.bytes / 1e6).toFixed(1)} MB  (${colors.green((stats.bytes / 1e6 / secs).toFixed(2))} MB/s)`);
  console.log(`   errors:      ${stats.errors > 0 ? colors.red(stats.errors) : 0}\n`);

  if (!streaming) {
    console.log(colors.bold('Latency (ms)'));
    console.log(
      `   min ${round(stats.min)}  mean ${round(stats.mean())}  p50 ${round(p[50])}  ` +
        `p90 ${round(p[90])}  p95 ${round(p[95])}  p99 ${round(p[99])}  max ${round(stats.max)}` +
        colors.gray(`   (${stats.samples.length} samples)`)
    );
  } else {
    console.log(colors.gray('Latency: n/a for server-push streaming (throughput-only)'));
  }
  console.log('');
}

function round(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}

function help() {
  console.log(`
${colors.cyan('Malachi load test')} — closed-loop load generator

${colors.yellow('Usage')}
  node loadtest.js --scenario <produce|fetch|stream|mixed> [options]

${colors.yellow('Options')}
  --scenario <s>     produce | fetch | stream | mixed (default produce)
  --connections <n>  Concurrent connections/workers (default 10)
  --duration <s>     Test duration in seconds (default 10)
  --topic <t>        Topic (default loadtest_<timestamp>, auto-created)
  --batch <n>        Records per produce op (default 1)
  --record-size <b>  Value size in bytes (default 128)
  --keys <n>         Key cardinality (default 1000)
  --max <n>          Max records per fetch/push (default 100)
  --window <n>       Streaming credit window (default 100)
  --prepopulate <n>  Records to append before fetch/stream/mixed (default 10000 for those)
  --warmup <s>       Warmup seconds excluded from stats (default 0)
  --samples <n>      Latency reservoir size (default 100000)
  --json             Emit the report as JSON
  -h, --help         Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  const valueFlags = [
    'scenario', 'connections', 'duration', 'topic', 'batch', 'record-size',
    'keys', 'max', 'window', 'prepopulate', 'warmup', 'samples',
  ];
  const { flags } = parseArgs(process.argv.slice(2), valueFlags);
  if (flags.help) return help();

  const scenario = flags.scenario || 'produce';
  if (!SCENARIOS.includes(scenario)) {
    console.error(colors.red(`Unknown scenario "${scenario}" (expected: ${SCENARIOS.join(', ')})`));
    process.exit(1);
  }

  const needsBacklog = scenario === 'fetch' || scenario === 'stream' || scenario === 'mixed';
  const opts = {
    connections: int(flags.connections, 10),
    duration: int(flags.duration, 10),
    warmup: int(flags.warmup, 0),
    topic: flags.topic || `loadtest_${Date.now()}`,
    batch: int(flags.batch, 1),
    recordSize: int(flags['record-size'], 128),
    keys: int(flags.keys, 1000),
    max: int(flags.max, 100),
    window: int(flags.window, 100),
    prepopulate: int(flags.prepopulate, needsBacklog ? 10000 : 0),
    samples: int(flags.samples, 100000),
    json: !!flags.json,
  };

  if (!opts.json) {
    console.log(colors.cyan(`\nStarting load test: ${scenario} (${opts.connections} connections, ${opts.duration}s)`));
    console.log(colors.gray(`   host: ${cfg.host}:${cfg.port}   topic: ${opts.topic}`));
  }

  let clients = [];
  try {
    // create the topic (idempotently) via a temporary produce-capable connection
    const admin = await connect();
    await admin.createTopic(opts.topic).catch(() => {});
    admin.close();

    await prepopulate(opts.topic, opts.prepopulate, opts.recordSize, opts.keys);

    clients = await Promise.all(Array.from({ length: opts.connections }, connect));

    if (opts.warmup > 0) {
      const warmStats = new Stats(opts.samples);
      await runScenario(scenario, clients, opts, opts.warmup * 1000, warmStats);
      if (!opts.json) console.log(colors.gray(`   warmup done (${warmStats.count} ops discarded)`));
      // Reconnect: the streaming subscription can only be ended by closing the socket (there is no
      // unsubscribe frame), so reuse across warmup+measure would double-subscribe. Fresh connections also
      // reset any TCP/GC warmup state for the measured run.
      clients.forEach((c) => c.close());
      clients = await Promise.all(Array.from({ length: opts.connections }, connect));
    }

    const stats = new Stats(opts.samples);
    const start = performance.now();
    await runScenario(scenario, clients, opts, opts.duration * 1000, stats);
    const elapsed = performance.now() - start;

    report(scenario, opts, elapsed, stats);
  } catch (err) {
    fail(err, cfg);
  } finally {
    clients.forEach((c) => c.close());
  }
}

// Dispatches to the right driver for `durationMs`, recording into `stats`.
function runScenario(scenario, clients, opts, durationMs, stats) {
  if (scenario === 'stream') {
    return streamDriver(clients, opts, durationMs, stats);
  }
  const deadline = performance.now() + durationMs;
  if (scenario === 'produce') {
    const op = produceOp(opts);
    return closedLoop(clients, () => op, deadline, stats);
  }
  if (scenario === 'fetch') {
    const op = fetchOp(opts);
    return closedLoop(clients, () => op, deadline, stats);
  }
  // mixed: even-indexed workers produce, odd-indexed workers fetch
  const produce = produceOp(opts);
  const fetch = fetchOp(opts);
  return closedLoop(clients, (index) => (index % 2 === 0 ? produce : fetch), deadline, stats);
}

function int(v, fallback) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

if (require.main === module) main();

module.exports = { Stats };
