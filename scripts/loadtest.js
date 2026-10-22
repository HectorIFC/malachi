#!/usr/bin/env node
'use strict';

/**
 * Malachi load-test harness: a load generator over the binary log protocol, in two modes.
 *
 * Closed-loop (default): N connections each run a scenario in a tight `op -> await` loop, saturating the
 * server, to find the ceiling and the latency at saturation. Open-loop (`--rate <rps>`): requests are
 * fired at a fixed arrival rate regardless of prior completions, and latency is measured from each
 * request's *scheduled* time: correcting coordinated omission, so a stall shows up as high latency on the
 * requests that queued behind it. Both reuse the same ops and report throughput (ops/s, records/s, MB/s)
 * plus latency percentiles. This is the external, end-to-end view (real TCP + serialization + auth),
 * reusing the reference client in ./lib.
 *
 * Producing goes through producer streams (./lib/producer.js), as the Elixir generator's does: each
 * connection is a routing producer that opens one stream per range on the broker leading it, sends its
 * appends within the window the broker grants (--pipeline at most), and lingers per --linger-ms and
 * --linger-bytes. In a closed loop an op is one acknowledged append, timed from its send, as in the Elixir
 * generator: one batch with no lingering on a one-range topic, several when lingering joins them, part of
 * one when the topic has several ranges. In the open loop an op is one hand-over of --batch records, timed
 * from its scheduled time to the acknowledgement of its last record.
 *
 * The ops (`produceOp`/`fetchOp`) are deliberately separate from the drivers (`closedLoop`/`openLoop`) so
 * both drive the same unit of work.
 *
 * Usage:
 *   node loadtest.js --scenario produce --connections 20 --duration 10
 *   node loadtest.js --scenario fetch --prepopulate 50000 --max 200
 *   node loadtest.js --scenario stream --connections 4 --window 500
 *   node loadtest.js --scenario mixed --connections 20 --record-size 512 --keys 1000
 *
 * Common flags: --topic, --batch, --record-size, --keys, --max, --window, --prepopulate, --warmup,
 *   --payload, --payload-seed, --json (emits reproduce metadata). Default credentials: app / app123
 *   (produce + consume).
 */

const { performance } = require('perf_hooks');
const { MalachiClient, isMigrating, isNotOwner, isTransport } = require('./lib/client');
const { Router } = require('./lib/router');
const { Producer, recordBytes: framedRecordBytes } = require('./lib/producer');
const { colors, config, parseArgs, fail, withRetry } = require('./lib/cli');
const payload = require('./lib/payload');
const os = require('os');
const path = require('path');
const fs = require('fs');
const { execFileSync } = require('child_process');

// Every enum flag, its values and its default, mirrored name for name by @enums in lib/malachi/loadtest.ex.
// connect-strategy is how connections are opened: each one pays a server-side credential verification, so
// opening hundreds at once is a self-inflicted auth storm; bounded caps in-flight connects, stagger delays
// each start, all-at-once is the storm. payload is what the record values are (scripts/lib/payload.js).
const ENUM_FLAGS = {
  scenario: { values: ['produce', 'fetch', 'stream', 'mixed'], fallback: 'produce' },
  'connect-strategy': { values: ['bounded', 'stagger', 'all-at-once'], fallback: 'bounded' },
  payload: { values: payload.MODES, fallback: 'constant' },
};

// A knob only some values of an enum flag read. Passing it with another value is an error, not a silent
// no-op: the run would otherwise claim a setting it never applied. Mirrored by @knobs in the Elixir generator.
const KNOB_REQUIRES = {
  'connect-concurrency': { flag: 'connect-strategy', values: ['bounded'], readers: 'the bounded connect strategy' },
  'connect-stagger-ms': { flag: 'connect-strategy', values: ['stagger'], readers: 'the stagger connect strategy' },
  'payload-seed': { flag: 'payload', values: ['json', 'random'], readers: 'the json and random payloads' },
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// How long the appends in flight when a run ends are waited for, as the Elixir generator waits (@drain_ms).
const DRAIN_MS = 15000;
// How long setting a run up may wait on the server at one step, as the Elixir generator's setup_timeout_ms.
const SETUP_TIMEOUT_MS = 30000;

// A dropped stream connection reconnects with a short backoff, capped so a server that is down does not
// spin the worker. The same values as @max_reconnect_tries and @reconnect_backoff_ms in the Elixir generator.
const MAX_RECONNECT_TRIES = 10;
const RECONNECT_BACKOFF_MS = 20;

// A compact HDR-style log-linear histogram for accurate tail percentiles (P99.9 / P99.99) with bounded
// memory and no sampling error, unlike a reservoir (which under-represents the deep tail). Values are
// recorded as integer microseconds. The lowest region (< subBucketCount us) is linear with 1us resolution;
// above it, each power-of-two octave is split into subBucketCount linear slots, giving ~(1/subBucketCount)
// relative precision everywhere. Buckets are stored sparsely in a Map, so only touched buckets cost memory.
class Histogram {
  constructor(subBucketBits = 7) {
    this.subBucketBits = subBucketBits;
    this.subBucketCount = 1 << subBucketBits; // 128 slots per octave => ~0.8% relative precision
    this.counts = new Map(); // bucketIndex -> count
    this.total = 0;
  }

  // The bucket index a value falls in. Below subBucketCount: linear (index === value). Above: the octave
  // (floor(log2 v)) times subBucketCount, plus the linear slot within it. slotWidth = octave/subBucketCount
  // is always an integer power of two >= 1, so the arithmetic stays exact.
  _index(v) {
    if (v < this.subBucketCount) return v;
    const magnitude = 31 - Math.clz32(v); // floor(log2 v); v assumed < 2^31 us (~35 min)
    const octave = 1 << magnitude;
    const slotWidth = octave >>> this.subBucketBits;
    const sub = ((v - octave) / slotWidth) | 0;
    return this.subBucketCount + (magnitude - this.subBucketBits) * this.subBucketCount + sub;
  }

  // The representative value (bucket midpoint, in us) for a bucket index, used for percentile output.
  _value(index) {
    if (index < this.subBucketCount) return index;
    const rel = index - this.subBucketCount;
    const magnitude = this.subBucketBits + Math.floor(rel / this.subBucketCount);
    const sub = rel % this.subBucketCount;
    const octave = 1 << magnitude;
    const slotWidth = octave >>> this.subBucketBits;
    return octave + sub * slotWidth + slotWidth / 2;
  }

  record(us) {
    const v = us < 1 ? 1 : us; // 1us floor (a sub-us latency still counts as the smallest bucket)
    const i = this._index(v);
    this.counts.set(i, (this.counts.get(i) || 0) + 1);
    this.total += 1;
  }

  // Nearest-rank percentiles (matching the previous ceil-based rank), returned as { p: valueUs }.
  percentiles(ps) {
    const out = {};
    if (this.total === 0) {
      for (const p of ps) out[p] = 0;
      return out;
    }
    const keys = [...this.counts.keys()].sort((a, b) => a - b);
    const sortedPs = [...ps].sort((a, b) => a - b);
    const targets = sortedPs.map((p) => Math.max(1, Math.ceil((p / 100) * this.total)));
    let cum = 0;
    let ti = 0;
    for (const k of keys) {
      cum += this.counts.get(k);
      while (ti < sortedPs.length && cum >= targets[ti]) {
        out[sortedPs[ti]] = this._value(k);
        ti += 1;
      }
      if (ti >= sortedPs.length) break;
    }
    while (ti < sortedPs.length) {
      out[sortedPs[ti]] = this._value(keys[keys.length - 1]);
      ti += 1;
    }
    return out;
  }
}

// Latency stats: exact count/min/max, Welford's online variance for a numerically stable stddev, and the
// HDR-style histogram above for accurate percentiles (including the deep tail). Latency is in milliseconds;
// the histogram records microseconds so sub-millisecond tails keep their resolution.
class Stats {
  constructor() {
    this.count = 0;
    this.mean_ = 0; // Welford running mean (ms)
    this.m2 = 0; // Welford sum of squared deltas
    this.min = Infinity;
    this.max = -Infinity;
    this.errors = 0;
    // errors by reason: the server's reason for a refusal, or the client's message for a local failure
    // (this generator counts transport failures as errors too). Always adds up to `errors`. Mirrors the
    // Elixir generator's `error_reasons`.
    this.errorReasons = {};
    this.records = 0;
    this.bytes = 0;
    this.hist = new Histogram();
    this.saturated = false; // set by openLoop when in-flight hits the cap (server can't sustain the rate)
    // Stream connections dropped, and the reconnects that followed, counted as the Elixir generator counts
    // them but only within the measured window: a warmup's drops are discarded with the rest of its Stats,
    // where the Elixir generator counts drops in any window. Only the stream driver counts these; the other
    // drivers still count a transport failure in `errors`.
    this.dropped = 0;
    this.reconnects = 0;
    // appends the broker shed, apart from the errors, as the Elixir generator counts them
    this.overloaded = 0;
    this.rateLimited = 0;
    // what the routing producers did over the whole run, added once at its end (`finishProducers`)
    this.moved = 0;
    this.redirects = 0;
    this.routeRefreshes = 0;
  }

  record(latencyMs, records, bytes) {
    this.count += 1;
    const delta = latencyMs - this.mean_;
    this.mean_ += delta / this.count;
    this.m2 += delta * (latencyMs - this.mean_);
    this.records += records;
    this.bytes += bytes;
    if (latencyMs < this.min) this.min = latencyMs;
    if (latencyMs > this.max) this.max = latencyMs;
    this.hist.record(Math.round(latencyMs * 1000)); // ms -> us
  }

  drop() {
    this.dropped += 1;
  }

  reconnect() {
    this.reconnects += 1;
  }

  // A refused append, counted under its reason: the two sheds apart, any other reason as an error. An append
  // in flight when its connection dropped is in `dropped` (the producer's count), not here.
  refusal(reason) {
    if (reason === 'connection_lost') return;
    if (reason === 'overloaded') this.overloaded += 1;
    else if (reason === 'rate_limited') this.rateLimited += 1;
    else this.error(new Error(reason));
  }

  error(err) {
    this.errors += 1;
    const reason = err && err.message ? err.message : 'unknown';
    this.errorReasons[reason] = (this.errorReasons[reason] || 0) + 1;
  }

  // { p: latencyMs } for each requested percentile.
  percentiles(ps) {
    const us = this.hist.percentiles(ps);
    const out = {};
    for (const p of ps) out[p] = us[p] / 1000; // us -> ms
    return out;
  }

  mean() {
    return this.mean_;
  }

  stddev() {
    return this.count > 1 ? Math.sqrt(this.m2 / this.count) : 0;
  }
}

const cfg = config({ username: 'app', password: 'app123' });

function newClient() {
  return new MalachiClient({ host: cfg.host, port: cfg.port, timeout: 30000 });
}

function connect() {
  return newClient().connect(cfg.username, cfg.password);
}

// One worker's routing producer, its streams opened on every range of the topic before the run, as the
// Elixir generator's are (inside the connect ramp, so the measured window pays none of it). Its events go
// to whatever phase is current (`phase`), so one producer serves the warmup and the measured window.
async function connectProducer(index, opts) {
  const producer = new Producer(
    new Router({ host: cfg.host, port: cfg.port }),
    { user: cfg.username, pass: cfg.password, timeout: 30000 },
    {
      maxInflight: opts.pipeline,
      lingerMs: opts.lingerMs,
      lingerBytes: opts.lingerBytes,
      label: `malachi-loadtest-${index}`,
      onEvent: (event) => phase.event(event),
    },
  );
  const prepared = await producer.prepare(opts.topic, SETUP_TIMEOUT_MS);
  if (prepared.error) {
    await producer.close();
    throw new Error(`could not open the producer streams of ${opts.topic}: ${prepared.error}`);
  }
  return producer;
}

// The events of every producer, recorded into the current phase's Stats; replaced at each phase. A closed
// loop records each acknowledged append as one op; the open loop finishes a hand-over on the
// acknowledgement of its last record (`handOver`).
const phase = { event: () => {} };

function closedLoopPhase(stats) {
  return (event) => {
    if (event.kind === 'acked') {
      const records = event.entries.length;
      stats.record(event.latencyUs / 1000, records, event.entries.reduce((acc, e) => acc + e.bytes, 0));
    } else {
      stats.refusal(event.reason);
    }
  };
}

// The records of one hand-over, each tagged with it, so the open loop can tell when all of them were
// answered, whichever appends carried them.
function handOver(records, intended) {
  const unit = { intended, pending: records.length, failed: null, records: records.length, bytes: recordBytes(records) };
  return { unit, entries: records.map((record) => ({ tag: unit, record, bytes: record.value.length })) };
}

// Nothing more is sent, the appends in flight are waited for (until `drainEnd`, DRAIN_MS after the measured
// window ended, which an open loop's own drain counts from too) and recorded, and what routing did over the
// whole run counts, as a dropped connection does: an event, not a rate.
async function finishProducers(producers, stats, drainEnd) {
  await Promise.all(
    producers.map(async (producer) => {
      await producer.stopSending();
      await producer.wait(Math.max(0, drainEnd - performance.now()), (p) => !p.inFlight());
      const routing = producer.statsNow();
      stats.moved += routing.moved;
      stats.redirects += routing.redirects;
      stats.routeRefreshes += routing.refreshes;
      stats.dropped += routing.dropped;
    }),
  );
}

// Opens `count` connections under opts.connectStrategy (see CONNECT_STRATEGIES). bounded is a pool of
// K async workers pulling the next index: at most K connects (and their auth round-trips) in flight,
// which keeps each auth's individual latency near K / server_verify_rate instead of the whole queue's
// drain time. stagger delays connection i's START by i * connectStaggerMs and lets completions overlap.
// `connectOne(i)` makes worker i: a plain connection, or a routing producer (`connectProducer`).
async function makeClients(count, opts, connectOne = () => connect()) {
  if (opts.connectStrategy === 'all-at-once') {
    return Promise.all(Array.from({ length: count }, (_, i) => connectOne(i)));
  }
  if (opts.connectStrategy === 'stagger') {
    return Promise.all(
      Array.from({ length: count }, async (_, i) => {
        await sleep(i * opts.connectStaggerMs);
        return connectOne(i);
      })
    );
  }
  const clients = new Array(count);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(opts.connectConcurrency, count) }, async () => {
      while (next < count) {
        const i = next++;
        clients[i] = await connectOne(i);
      }
    })
  );
  return clients;
}

// Worker i of `scenario`: a routing producer where it produces (produce, and even indexes of mixed), a plain
// connection where it reads.
const producesAt = (scenario, i) => scenario === 'produce' || (scenario === 'mixed' && i % 2 === 0);

// A reusable value buffer of the requested size (the codec copies it, so sharing across records is safe).
function makeValue(size) {
  return Buffer.alloc(size, 0x61); // 'a'
}

// The json and random values, generated once before any connection opens (see scripts/lib/payload.js): a
// whole number of batches of distinct values, shared by every connection. null for constant bytes, which
// keep the single shared value they always had.
function makePool(opts) {
  if (opts.payload === 'constant') return null;
  return payload.values(
    opts.payload,
    opts.payloadSeed,
    opts.recordSize,
    payload.poolSize(opts.recordSize, opts.batch, opts.connections, opts.pipeline),
  );
}

// The records of pool batch `n`, keyed by their position in the pool, as the Elixir generator keys them.
function poolBatch(opts, n) {
  return Array.from({ length: opts.batch }, (_, j) => {
    const pos = n * opts.batch + j;
    return { key: `k${pos % opts.keys}`, value: opts.pool[pos] };
  });
}

function recordBytes(records) {
  return records.reduce((acc, r) => acc + r.value.length, 0);
}

// ---- ops (one unit of work; return { records, bytes }) ----

// During a vnode split the topic is briefly fenced (:migrating) or routed to a stale owner (:not_owner);
// both are transient (the fence lifts and the ring settles), so retry them out of the measured op rather
// than counting a reshard blip as a benchmark error, matching what producer.js/consumer.js do.
const transient = (err) => isMigrating(err) || isNotOwner(err);

// The next batch of records a producing worker hands over. Constant bytes: one value for every record, as
// every published series was measured, each connection's keys from k0 on, as the Elixir generator's. A
// pool: the next batch on every op, from connection `index`'s own starting batch, as the Elixir generator
// walks it.
function produceOp(opts, index) {
  if (opts.pool === null) {
    const value = makeValue(opts.recordSize);
    let seq = 0;
    return () =>
      Array.from({ length: opts.batch }, () => {
        const key = `k${(index * opts.batch + seq) % opts.keys}`;
        seq += 1;
        return { key, value };
      });
  }
  const batches = opts.pool.length / opts.batch;
  let cursor = payload.startBatch(index, batches, opts.connections);
  return () => {
    const batch = poolBatch(opts, cursor);
    cursor = (cursor + 1) % batches;
    return batch;
  };
}

function fetchOp(opts) {
  return async (client, ctx) => {
    const { records, cursor } = await withRetry(
      () => client.fetch(opts.topic, { cursor: ctx.cursor, max: opts.max }),
      transient,
    );
    // Drained: rewind to the start so the worker keeps generating fetch load against the backlog.
    ctx.cursor = records.length === 0 ? null : cursor;
    return { records: records.length, bytes: recordBytes(records) };
  };
}

// ---- drivers ----

// Closed-loop: each client loops its op until the deadline. Latency is measured per op. A producing worker
// hands over a batch whenever its producer has room and otherwise waits for room, so its window, not this
// loop, sets the pace; each acknowledged append is one op, timed from its send (`closedLoopPhase`).
async function closedLoop(clients, makeOpFor, deadline, stats, opts) {
  phase.event = closedLoopPhase(stats);
  await Promise.all(
    clients.map(async (client, index) => {
      const op = makeOpFor(index);
      if (client instanceof Producer) {
        while (performance.now() < deadline) {
          if (client.room()) {
            await client.produce(opts.topic, handOver(op(), null).entries);
          } else {
            await client.wait(Math.max(1, deadline - performance.now()), (p) => p.room());
          }
        }
        return;
      }
      const ctx = { cursor: null };
      while (performance.now() < deadline) {
        const t0 = performance.now();
        try {
          const { records, bytes } = await op(client, ctx);
          stats.record(performance.now() - t0, records, bytes);
        } catch (err) {
          stats.error(err);
          if (err.message === 'not connected' || /closed/.test(err.message)) break;
          // Back off briefly so a persistent error (e.g. a permission or topic issue) throttles instead
          // of pegging a core and inflating the error count with a tight retry spin.
          await sleep(5);
        }
      }
    })
  );
}

// Open-loop: fire requests at a fixed arrival rate (opts.rate) regardless of prior completions, and
// measure latency from each request's *scheduled* time: not its actual send - so a server stall shows up
// as high latency on the requests that piled up behind it (coordinated-omission correction). Requests are
// spread round-robin over the connection pool, which multiplexes them by correlation id. When in-flight
// hits opts.maxInflight the server can't sustain the rate, so we stop piling on and flag saturation.
async function openLoop(clients, makeOpFor, opts, durationMs, stats) {
  // A hand-over is answered once every one of its records is: acknowledged, timed from its scheduled time;
  // or failed, counted once under the first refusal.
  phase.event = (event) => {
    for (const entry of event.entries) {
      const unit = entry.tag;
      if (event.kind === 'failed' && unit.failed === null) unit.failed = event.reason;
      unit.pending -= 1;
      if (unit.pending === 0) {
        if (unit.failed === null) stats.record(performance.now() - unit.intended, unit.records, unit.bytes);
        else stats.refusal(unit.failed);
        unit.done();
      }
    }
  };
  const gapMs = 1000 / opts.rate;
  const start = performance.now();
  const deadline = start + durationMs;
  let scheduled = start;
  let issued = 0;
  let inflight = 0;

  await new Promise((resolve) => {
    const pump = () => {
      let now = performance.now();
      while (scheduled <= now && now < deadline) {
        if (inflight >= opts.maxInflight) {
          stats.saturated = true;
          break;
        }
        const index = issued++;
        // the op of the worker that takes the request: in mixed, a producer produces and a reader fetches,
        // whatever the request's own parity (with an odd worker count the two would part)
        const worker = index % clients.length;
        const client = clients[worker];
        const op = makeOpFor(worker);
        const intended = scheduled;
        scheduled += gapMs;
        inflight += 1;
        if (client instanceof Producer) {
          // handed over now; answered (`phase.event`) once its records are
          const { unit, entries } = handOver(op(), intended);
          unit.done = () => {
            inflight -= 1;
          };
          client.produce(opts.topic, entries).catch((err) => {
            stats.error(err);
            unit.done();
          });
        } else {
          // fetch is stateful; a fresh ctx per request makes each fetch read from the start (stateless load).
          op(client, { cursor: null })
            .then(({ records, bytes }) => stats.record(performance.now() - intended, records, bytes))
            .catch((err) => stats.error(err))
            .finally(() => {
              inflight -= 1;
            });
        }
        now = performance.now();
      }
      if (performance.now() < deadline && !stats.saturated) {
        // sleep until the next scheduled send, but wake at least every 5ms to re-check the deadline
        setTimeout(pump, Math.max(0, Math.min(scheduled - performance.now(), 5)));
      } else {
        resolve();
      }
    };
    pump();
  });

  // Drain outstanding requests so their coordinated-omission-corrected latencies land in the stats, for as
  // long as the closed loop waits for its appends in flight (`finishProducers`): a broker that stops
  // answering without closing its connections does not hold the run forever.
  const drainEnd = deadline + DRAIN_MS;
  while (inflight > 0 && performance.now() < drainEnd) await sleep(5);
}

// Streaming is server-push, not request/await: subscribe and count pushed records for the duration.
// Latency per push is not comparable to a round-trip, so the stream report is throughput-only.
let streamRun = 0;
async function streamDriver(clients, opts, durationMs, stats) {
  // A fresh group per invocation so each run reads the backlog from the start: a warmup pass commits (acks)
  // its way to the end of the backlog, so sharing a group with the measured run would leave it nothing.
  const run = streamRun++;
  const deadline = performance.now() + durationMs;
  await Promise.all(
    clients.map((_, index) => streamWorker(clients, index, `loadtest-${run}-${index}`, opts, deadline, stats))
  );
}

// One stream connection until the deadline, mirroring stream_loop/4 and resubscribe/4 in the Elixir
// generator. A subscription the server refused ends the worker, counted in `errors` under its reason. A
// transport failure, whether the subscribe could not be sent or the connection dropped mid-stream, counts
// a drop and resubscribes on a fresh connection, which replaces the dead one in `clients` so the caller's
// final close reaches it.
async function streamWorker(clients, index, group, opts, deadline, stats) {
  for (;;) {
    const outcome = await subscribeUntil(clients[index], group, opts, deadline, stats);
    if (outcome !== 'dropped') return;
    const client = await afterDrop(stats, deadline);
    if (client === null) return;
    clients[index].close();
    clients[index] = client;
  }
}

// Subscribes and resolves with how the subscription ended: 'deadline', 'refused' or 'dropped'. Whatever
// happens after the first of these is ignored, pushes included, so nothing is counted past the deadline
// even while the subscription stays open until the run closes its clients.
function subscribeUntil(client, group, opts, deadline, stats) {
  return new Promise((resolve) => {
    let settled = false;
    const end = (outcome) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(outcome);
    };
    const failed = (err) => {
      if (settled) return;
      if (isTransport(err)) return end('dropped');
      stats.error(err);
      return end('refused');
    };
    const timer = setTimeout(() => end('deadline'), Math.max(0, deadline - performance.now()));

    client
      .subscribe(opts.topic, {
        group,
        window: opts.window,
        max: opts.max,
        onRecords: ({ records, cursor }) => {
          // The workers' deadline timers are armed at different moments and can fire about a millisecond
          // apart, so a push can reach a worker that has already settled while another has not. The guard
          // keeps that push out of the count. No test pins it: it needs a push landing inside that
          // millisecond, which no stub can place deterministically.
          if (settled || records.length === 0) return;
          stats.record(0, records.length, recordBytes(records));
          client.streamAck(opts.topic, group, null, cursor, records.length);
        },
        onError: failed,
      })
      .catch(failed);
  });
}

// A stream connection dropped: count it and reconnect, mirroring after_drop/1 in the Elixir generator.
// Resolves with a fresh authenticated client, or null once the retry cap or the deadline is reached.
async function afterDrop(stats, deadline) {
  stats.drop();
  for (let tries = 0; tries < MAX_RECONNECT_TRIES && performance.now() < deadline; tries++) {
    await sleep(RECONNECT_BACKOFF_MS);
    const client = await connectBefore(deadline);
    if (client !== null) {
      stats.reconnect();
      return client;
    }
  }
  return null;
}

// Connects and authenticates, resolving with the client, or with null when that fails or the deadline comes
// first. A connect still in flight at the deadline is destroyed rather than awaited: the socket has no
// connect timeout and the authentication waits up to 30s, and the rates divide by the time the run took,
// so waiting on it would stretch the window past --duration. A connect that fails is destroyed too: an
// authentication that was refused or timed out can leave its socket open, and since the run ends without
// process.exit, an open socket nobody closes would keep the process alive after the report.
function connectBefore(deadline) {
  const client = newClient();
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      client.destroy();
      resolve(null);
    }, Math.max(0, deadline - performance.now()));
    client.connect(cfg.username, cfg.password).then(
      () => {
        clearTimeout(timer);
        resolve(client);
      },
      () => {
        clearTimeout(timer);
        client.destroy();
        resolve(null);
      }
    );
  });
}

// Appends `count` records up front so fetch/stream/mixed have a backlog to read, from the pool when there
// is one, walking it from its start, through one producer with 16 appends in flight, as the Elixir
// generator prepopulates. A refusal fails the run before it starts.
async function prepopulate(opts) {
  const { topic, prepopulate: count, recordSize, keys, pool } = opts;
  if (count <= 0) return;
  const failures = [];
  const producer = new Producer(
    new Router({ host: cfg.host, port: cfg.port }),
    { user: cfg.username, pass: cfg.password, timeout: 30000 },
    { maxInflight: 16, label: 'malachi-loadtest-prepopulate', onEvent: (e) => e.kind === 'failed' && failures.push(e.reason) },
  );
  try {
    const value = makeValue(recordSize);
    const CHUNK = 1000;
    let done = 0;
    while (done < count && failures.length === 0) {
      const n = Math.min(CHUNK, count - done);
      const batch = Array.from({ length: n }, (_, i) => ({
        key: `k${(done + i) % keys}`,
        value: pool === null ? value : pool[(done + i) % pool.length],
      }));
      await producer.produce(topic, handOver(batch, null).entries);
      // a stall fails the setup after one wait, not after one wait per chunk
      if ((await producer.wait(SETUP_TIMEOUT_MS, (p) => p.room())) === 'timeout') failures.push('prepopulate_timeout');
      done += n;
    }
    if (failures.length === 0 && (await producer.wait(SETUP_TIMEOUT_MS)) === 'timeout') failures.push('prepopulate_timeout');
    if (failures.length > 0) throw new Error(`could not prepopulate ${topic}: ${failures[0]}`);
    // a JSON report is the whole of stdout
    if (!opts.json) console.log(colors.gray(`   prepopulated ${done} records`));
  } finally {
    await producer.close();
  }
}

// ---- reproduce metadata ----

const REPO_ROOT = path.join(__dirname, '..');

// execFileSync (no shell) with a fixed argument array: git output never flows through a shell.
function git(args) {
  try {
    return execFileSync('git', args, { cwd: REPO_ROOT, stdio: ['ignore', 'pipe', 'ignore'] })
      .toString()
      .trim();
  } catch {
    return null;
  }
}

function malachiVersion() {
  try {
    const mix = fs.readFileSync(path.join(REPO_ROOT, 'mix.exs'), 'utf8');
    const m = mix.match(/@version\s+"([^"]+)"/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

// Everything needed to reproduce a run: the exact command, the source revision, and the machine. Mirrors
// the Server/Client Information the Iggy dashboard captures, so a stored result stands on its own.
function buildMeta() {
  const ref = git(['rev-parse', '--short', 'HEAD']);
  const dirty = (git(['status', '--porcelain']) || '').length > 0;
  const cpus = os.cpus();
  return {
    timestamp: new Date().toISOString(),
    command: [path.relative(REPO_ROOT, process.argv[1]), ...recordedArgs(process.argv.slice(2))].join(' '),
    git_ref: ref ? (dirty ? `${ref}-dirty` : ref) : null,
    git_ref_date: git(['show', '-s', '--format=%cI', 'HEAD']),
    malachi_version: malachiVersion(),
    hardware: {
      cpu: cpus[0] ? cpus[0].model : null,
      cores: cpus.length,
      memory_bytes: os.totalmem(),
      os: `${os.platform()} ${os.release()}`,
    },
  };
}

// Why the marker path cannot be used, or null when it can. A marker that already exists is overwritten
// when the measured window opens, so it has to be a writable regular file; a directory or a file owned
// by someone else passes a check of the parent alone and then fails inside writeFileSync, after every
// connection has authenticated. Mirrors `check_marker!` in lib/malachi/loadtest.ex.
function markerProblem(marker) {
  let stat = null;
  try {
    stat = fs.statSync(marker);
  } catch {
    stat = null;
  }

  if (stat) {
    if (!stat.isFile()) return `"${marker}" exists and is not a regular file`;
    try {
      fs.accessSync(marker, fs.constants.W_OK);
    } catch {
      return `"${marker}" exists and is not writable`;
    }
    return null;
  }

  const dir = path.dirname(marker);
  try {
    if (!fs.statSync(dir).isDirectory()) throw new Error('not a directory');
    fs.accessSync(dir, fs.constants.W_OK);
  } catch {
    return `directory "${dir}" does not exist or is not writable`;
  }
  return null;
}

// The arguments a reproduction needs: everything typed except --measure-marker and its value. The marker
// is plumbing for the harness that launched the run, and its path names a directory on that machine, so
// a published command carrying it would refuse to start anywhere else. Mirrors the Elixir generator,
// which leaves it out of its rebuilt command for the same reason.
function recordedArgs(args) {
  const out = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--measure-marker') {
      i += 1;
      continue;
    }
    out.push(args[i]);
  }
  return out;
}

// Offline check that the histogram's percentiles (including the deep tail) match a brute-force sorted
// reference within one bucket's precision. Runs without a server: `node loadtest.js --self-test`.
function selfTest() {
  try {
    console.log(`  payload: ${require('./lib/payload.selftest').run()} checks OK`);
  } catch (err) {
    console.log(`  payload: FAIL (${err.message})`);
    console.log('\nself-test FAILED');
    process.exit(1);
  }
  const h = new Histogram();
  const ref = [];
  let seed = 123456789;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  // Heavy-tailed: most latencies small, rare spikes into the hundreds of ms, so P99.9/P99.99 are exercised.
  const N = 2_000_000;
  for (let i = 0; i < N; i++) {
    const us = Math.max(1, Math.round(200 + Math.pow(rand(), 8) * 500_000));
    h.record(us);
    ref.push(us);
  }
  ref.sort((a, b) => a - b);
  const nearestRank = (p) => ref[Math.min(ref.length - 1, Math.ceil((p / 100) * ref.length) - 1)];

  const ps = [50, 90, 95, 99, 99.9, 99.99];
  const got = h.percentiles(ps);
  let ok = true;
  for (const p of ps) {
    const expected = nearestRank(p);
    const actual = got[p];
    const tol = expected / h.subBucketCount + 2; // one bucket width at that value, plus the 1us floor
    const pass = Math.abs(actual - expected) <= tol;
    ok = ok && pass;
    console.log(
      `  P${p}: expected ~${expected}us  got ${actual}us  (tol +-${Math.round(tol)}us)  ${pass ? 'OK' : 'FAIL'}`
    );
  }
  const mono = ps.every((p, i) => i === 0 || got[p] >= got[ps[i - 1]]);
  console.log(`  monotonic: ${mono ? 'OK' : 'FAIL'}`);
  ok = ok && mono;
  console.log(ok ? '\nself-test PASSED' : '\nself-test FAILED');
  process.exit(ok ? 0 : 1);
}

// ---- reporting ----

function report(scenario, opts, elapsedMs, stats) {
  const secs = elapsedMs / 1000;
  const p = stats.percentiles([50, 90, 95, 99, 99.9, 99.99]);
  const streaming = scenario === 'stream';
  const openLoopMode = opts.rate > 0 && !streaming;

  if (opts.json) {
    console.log(
      JSON.stringify(
        {
          meta: buildMeta(),
          scenario,
          mode: openLoopMode ? 'open-loop' : streaming ? 'stream' : 'closed-loop',
          connections: opts.connections,
          ...regimeFields(opts),
          duration_s: Number(secs.toFixed(3)),
          topic: opts.topic,
          target_rate_per_s: openLoopMode ? opts.rate : null,
          saturated: openLoopMode ? stats.saturated : null,
          ops: stats.count,
          ops_per_s: Math.round(stats.count / secs),
          records: stats.records,
          records_per_s: Math.round(stats.records / secs),
          bytes: stats.bytes,
          mb_per_s: Number((stats.bytes / 1e6 / secs).toFixed(3)),
          errors: stats.errors,
          error_reasons: stats.errorReasons,
          // Counted where this generator measures every lost connection: the stream driver's, and the
          // producers' of a produce run. A fetch, and the fetch half of a mixed run, still count a transport
          // failure in `errors`, so a number there would claim a zero it never measured.
          dropped: scenario === 'fetch' || scenario === 'mixed' ? null : stats.dropped,
          reconnects: streaming ? stats.reconnects : null,
          overloaded: stats.overloaded,
          rate_limited: stats.rateLimited,
          moved: stats.moved,
          redirects: stats.redirects,
          route_refreshes: stats.routeRefreshes,
          latency_ms: streaming
            ? null
            : {
                coordinated_omission_corrected: openLoopMode,
                min: round(stats.min),
                mean: round(stats.mean()),
                stddev: round(stats.stddev()),
                p50: round(p[50]),
                p90: round(p[90]),
                p95: round(p[95]),
                p99: round(p[99]),
                p99_9: round(p[99.9]),
                p99_99: round(p[99.99]),
                max: round(stats.max),
                count: stats.count,
              },
        },
        null,
        2
      )
    );
    return;
  }

  const mode = openLoopMode ? `open-loop @ ${opts.rate} rps` : streaming ? 'stream' : 'closed-loop';
  console.log(colors.cyan('\nMalachi load test'));
  console.log(colors.gray(`   scenario:    ${scenario}`));
  console.log(colors.gray(`   mode:        ${mode}`));
  console.log(colors.gray(`   connections: ${opts.connections}`));
  console.log(colors.gray(`   duration:    ${secs.toFixed(1)}s`));
  console.log(colors.gray(`   topic:       ${opts.topic}\n`));

  console.log(colors.bold('Throughput'));
  const achieved = Math.round(stats.count / secs);
  const opsLabel = openLoopMode ? `${achieved} ops/s of ${opts.rate} target` : `${achieved} ops/s`;
  console.log(`   operations:  ${stats.count}  (${colors.green(opsLabel)})`);
  console.log(`   records:     ${stats.records}  (${colors.green(Math.round(stats.records / secs))} rec/s)`);
  console.log(`   data:        ${(stats.bytes / 1e6).toFixed(1)} MB  (${colors.green((stats.bytes / 1e6 / secs).toFixed(2))} MB/s)`);
  console.log(`   errors:      ${stats.errors > 0 ? colors.red(stats.errors) : 0}`);
  const reasons = Object.entries(stats.errorReasons)
    .sort(([a, x], [b, y]) => y - x || a.localeCompare(b))
    .map(([reason, n]) => `${reason}=${n}`);
  if (reasons.length > 0) console.log(`   reasons:     ${colors.red(reasons.join('  '))}`);
  if (streaming) console.log(`   dropped:     ${stats.dropped}  (reconnects ${stats.reconnects})`);
  if (openLoopMode && stats.saturated) {
    console.log(colors.red(`   saturated:   server could not sustain ${opts.rate} rps (in-flight hit ${opts.maxInflight})`));
  }
  console.log('');

  if (!streaming) {
    console.log(colors.bold('Latency (ms)') + (openLoopMode ? colors.gray(', coordinated-omission-corrected') : ''));
    console.log(
      `   min ${round(stats.min)}  mean ${round(stats.mean())}  stddev ${round(stats.stddev())}  max ${round(stats.max)}` +
        colors.gray(`   (${stats.count} ops)`)
    );
    console.log(
      `   p50 ${round(p[50])}  p90 ${round(p[90])}  p95 ${round(p[95])}  ` +
        `p99 ${round(p[99])}  p99.9 ${round(p[99.9])}  p99.99 ${round(p[99.99])}`
    );
  } else {
    console.log(colors.gray('Latency: n/a for server-push streaming (throughput-only)'));
  }
  console.log('');
}

// The regime the throughput describes, as fields of their own rather than only inside meta.command, whose
// syntax differs between the two generators. Mirrored field for field by regime_fields/1 in the Elixir
// generator and checked by the regime parity test; a new mirrored flag adds its line here.
function regimeFields(opts) {
  return {
    batch: opts.batch,
    record_size: opts.recordSize,
    pipeline: opts.pipeline,
    payload: opts.payload,
    payload_seed: opts.payloadSeed,
    payload_pool_values: opts.pool ? opts.pool.length : null,
    linger_ms: opts.lingerMs,
    linger_bytes: opts.lingerBytes,
  };
}

function round(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}

function help() {
  console.log(`
${colors.cyan('Malachi load test')}: closed-loop (default) or open-loop (--rate) load generator

${colors.yellow('Usage')}
  node loadtest.js --scenario <produce|fetch|stream|mixed> [options]

${colors.yellow('Options')}
  --scenario <s>     produce | fetch | stream | mixed (default produce)
  --rate <rps>       Open-loop: fixed arrival rate (coordinated-omission-corrected latency).
                     Omit or 0 = closed-loop. Not applicable to stream.
  --max-inflight <n> Open-loop in-flight cap; hitting it flags saturation (default 100000)
  --connections <n>  Concurrent connections/workers (default 10)
  --connect-strategy <s>     How connections open: bounded | stagger | all-at-once (default bounded).
                             Each connection pays a server-side credential check, so hundreds at once
                             is an auth storm; bounded caps in-flight connects.
  --connect-concurrency <n>  bounded only: max connects in flight (default 32)
  --connect-stagger-ms <ms>  stagger only: delay between connection starts (default 100)
  --duration <s>     Test duration in seconds (default 10)
  --topic <t>        Topic (default loadtest_<timestamp>, auto-created)
  --batch <n>        Records per produce op (default 1)
  --pipeline <n>     Appends a producer stream keeps in flight at most, under the window the broker
                     grants (default 1)
  --linger-ms <ms>   How long a range's records may wait to join one append (default 0: send at once)
  --linger-bytes <b> How many bytes of records one append takes at most, unless one batch alone is more
                     (an append never splits a batch; default one batch's worth)
  --record-size <b>  Value size in bytes (default 256)
  --keys <n>         Key cardinality (default 1000)
  --max <n>          Max records per fetch/push (default 100)
  --window <n>       Streaming credit window (default 100)
  --prepopulate <n>  Records to append before fetch/stream/mixed (default 10000 for those)
  --warmup <s>       Warmup seconds excluded from stats (default 0)
  --payload <p>      What the values are: constant | json | random (default constant). constant is one
                     repeated byte, as every published series so far; json is a seeded, event-like JSON
                     document of exactly --record-size bytes (at least ${payload.minJsonSize()}); random
                     is seeded noise, the incompressible control. Values are generated before the run.
  --payload-seed <n> json and random only: the seed, 0 to 4294967295 (default ${payload.DEFAULT_SEED})
  --samples <n>      Deprecated, ignored (percentiles now use an exact histogram, not a sample)
  --json             Emit the report as JSON (with a reproduce-metadata block)
  --measure-marker <path>  Create this (empty) file when the measured window begins, after every
                     connection authenticated and the warmup ended, so a harness can sample CPU over
                     that window alone. Its directory must exist. Not recorded in the JSON command.
  --self-test        Validate the latency histogram offline (no server) and exit
  -h, --help         Show this help

${colors.yellow('Environment')}
  MALACHI_HOST, MALACHI_PORT, MALACHI_USER, MALACHI_PASS
`);
}

async function main() {
  if (process.argv.includes('--self-test')) return selfTest();

  const valueFlags = [
    'scenario', 'connections', 'duration', 'topic', 'batch', 'record-size',
    'keys', 'max', 'window', 'prepopulate', 'warmup', 'samples', 'rate', 'max-inflight',
    'connect-strategy', 'connect-concurrency', 'connect-stagger-ms', 'measure-marker', 'payload', 'payload-seed',
    'pipeline', 'linger-ms', 'linger-bytes',
  ];
  const { flags } = parseArgs(process.argv.slice(2), valueFlags);
  if (flags.help) return help();

  // Every enum and every knob is checked before any connection opens, against the tables at the top.
  const enums = {};
  for (const [flag, { values, fallback }] of Object.entries(ENUM_FLAGS)) {
    const value = flags[flag] === undefined ? fallback : flags[flag];
    if (!values.includes(value)) {
      console.error(colors.red(`Unknown ${flag} "${value}" (expected: ${values.join(', ')})`));
      process.exit(1);
    }
    enums[flag] = value;
  }
  for (const [knob, { flag, values, readers }] of Object.entries(KNOB_REQUIRES)) {
    if (flags[knob] !== undefined && !values.includes(enums[flag])) {
      console.error(colors.red(`--${knob} only applies to ${readers}`));
      process.exit(1);
    }
  }
  const scenario = enums.scenario;
  const connectStrategy = enums['connect-strategy'];

  // Checked before any connection opens, as the Elixir generator does: discovering after hundreds of
  // authentications that the marker cannot be written would lose the run, and creating it early to
  // prove it would fire the signal before the window it marks. hasOwnProperty, not truthiness, because
  // a trailing `--measure-marker` with no value parses as a present key holding undefined.
  const measureMarker = flags['measure-marker'];
  if (Object.prototype.hasOwnProperty.call(flags, 'measure-marker')) {
    if (typeof measureMarker !== 'string' || measureMarker === '') {
      console.error(colors.red('--measure-marker needs a file path'));
      process.exit(1);
    }
    const problem = markerProblem(measureMarker);
    if (problem) {
      console.error(colors.red(`--measure-marker ${problem}`));
      process.exit(1);
    }
  }

  // A seed is a 32-bit word, the generator's state size; constant bytes have none. Validated here rather
  // than through int(), which would quietly fall back to the default on a typo.
  let payloadSeed = null;
  if (enums.payload !== 'constant') {
    const raw = flags['payload-seed'] === undefined ? String(payload.DEFAULT_SEED) : flags['payload-seed'];
    payloadSeed = /^\d+$/.test(raw) ? Number(raw) : NaN;
    if (!(payloadSeed <= 0xffffffff)) {
      console.error(colors.red(`--payload-seed must be an integer from 0 to 4294967295, got ${raw}`));
      process.exit(1);
    }
  }

  const needsBacklog = scenario === 'fetch' || scenario === 'stream' || scenario === 'mixed';
  const opts = {
    connections: int(flags.connections, 10),
    duration: int(flags.duration, 10),
    warmup: int(flags.warmup, 0),
    topic: flags.topic || `loadtest_${Date.now()}`,
    batch: int(flags.batch, 1),
    recordSize: int(flags['record-size'], 256),
    keys: int(flags.keys, 1000),
    max: int(flags.max, 100),
    window: int(flags.window, 100),
    prepopulate: int(flags.prepopulate, needsBacklog ? 10000 : 0),
    samples: int(flags.samples, 100000),
    rate: int(flags.rate, 0),
    maxInflight: int(flags['max-inflight'], 100000),
    connectStrategy,
    connectConcurrency: int(flags['connect-concurrency'], 32),
    connectStaggerMs: int(flags['connect-stagger-ms'], 100),
    json: !!flags.json,
    measureMarker: measureMarker || null,
    payload: enums.payload,
    payloadSeed,
    pipeline: int(flags.pipeline, 1),
    lingerMs: int(flags['linger-ms'], 0),
  };
  // One batch's worth unless given, counted as the producer counts it, with the longest key the run uses: an
  // append never splits a hand-over, so with one range a batch is one append.
  opts.lingerBytes = int(flags['linger-bytes'], opts.batch * framedRecordBytes(opts.recordSize, Buffer.byteLength(`k${opts.keys - 1}`)));
  if (opts.pipeline < 1 || opts.lingerBytes < 1) {
    console.error(colors.red('--pipeline and --linger-bytes must be positive integers'));
    process.exit(1);
  }

  // A json size too small for the document is refused before any connection opens.
  if (opts.payload === 'json') {
    try {
      payload.checkJsonSize(opts.recordSize);
    } catch (err) {
      console.error(colors.red(err.message));
      process.exit(1);
    }
  }

  if (scenario === 'stream' && opts.rate > 0) {
    console.error(colors.yellow('note: --rate does not apply to the stream scenario (server-push); ignoring it'));
    opts.rate = 0;
  }

  if (!opts.json) {
    const mode = opts.rate > 0 ? `open-loop @ ${opts.rate} rps` : 'closed-loop';
    console.log(colors.cyan(`\nStarting load test: ${scenario} (${mode}, ${opts.connections} connections, ${opts.duration}s)`));
    console.log(colors.gray(`   host: ${cfg.host}:${cfg.port}   topic: ${opts.topic}`));
  }

  let clients = [];
  try {
    // Create the topic via a temporary produce-capable connection. One that already exists is fine, so a
    // rerun works; any other refusal fails here, naming its cause, rather than as a run full of errors.
    const admin = await connect();
    try {
      await admin.createTopic(opts.topic, 8);
    } catch (err) {
      if (err.message !== 'already_exists') throw new Error(`could not create topic ${opts.topic}: ${err.message}`);
    } finally {
      admin.close();
    }

    // Generated before any connection opens, so nothing inside the measured window pays for it. Only a
    // run that produces needs it, as in the Elixir generator.
    const produces = scenario === 'produce' || scenario === 'mixed' || opts.prepopulate > 0;
    opts.pool = produces ? makePool(opts) : null;
    // One op per connection for the whole run, warmup and measured window alike, so each connection's
    // cursor carries on across them the way the Elixir worker's does (see scenarioOps).
    opts.poolOps = new Map();

    await prepopulate(opts);

    const connectWorker = (i) => (producesAt(scenario, i) ? connectProducer(i, opts) : connect());
    clients = await makeClients(opts.connections, opts, connectWorker);

    if (opts.warmup > 0) {
      const warmStats = new Stats();
      await runScenario(scenario, clients, opts, opts.warmup * 1000, warmStats);
      if (!opts.json) console.log(colors.gray(`   warmup done (${warmStats.count} ops discarded)`));
      // Only the stream scenario reconnects here: a subscription can only be ended by closing its socket
      // (there is no unsubscribe frame), so reusing it across warmup and measure would double-subscribe.
      // Every other scenario keeps its warmed connections, as the Elixir generator always has.
      // Reconnecting them all ran a second authentication storm inside every ceiling point (93s at 512
      // connections on the CI runner) and measured a colder client than the Elixir generator did, so
      // published Node numbers move with this change.
      if (scenario === 'stream') {
        clients.forEach((c) => c.close());
        clients = await makeClients(opts.connections, opts);
      }
    }

    const stats = new Stats();
    // The measured window begins here, after every connection authenticated and the warmup ended, which
    // no caller can infer from the spawn time (see --measure-marker in help()).
    if (opts.measureMarker) fs.writeFileSync(opts.measureMarker, '');
    const start = performance.now();
    await runScenario(scenario, clients, opts, opts.duration * 1000, stats);
    const elapsed = performance.now() - start;
    const windowEnd = start + opts.duration * 1000;
    await finishProducers(clients.filter((c) => c instanceof Producer), stats, windowEnd + DRAIN_MS);

    report(scenario, opts, elapsed, stats);
  } catch (err) {
    fail(err, cfg);
  } finally {
    await Promise.all(clients.map((c) => c.close()));
  }
}

// Builds the per-worker/per-request op selector for a scenario (shared by both drivers).
// mixed: even indexes produce, odd indexes fetch.
function scenarioOps(scenario, opts) {
  // Constant bytes keep the one op every connection shared, its key sequence included, as the published
  // series were measured. A pool gets an op per connection, each walking it from its own batch; the closed
  // loop asks once per connection and the open loop once per request, so the ops are cached, in
  // opts.poolOps, which outlives this call: the warmup and the measured window each make their own
  // selector, and a cache of their own would send the measured window back to the start of the pool.
  const shared = opts.pool === null ? produceOp(opts, 0) : null;
  const produceOps = opts.poolOps;
  const produceFor = (index) => {
    if (shared !== null) return shared;
    const key = index % opts.connections;
    if (!produceOps.has(key)) produceOps.set(key, produceOp(opts, key));
    return produceOps.get(key);
  };
  if (scenario === 'produce') return produceFor;
  if (scenario === 'fetch') {
    const op = fetchOp(opts);
    return () => op;
  }
  const fetch = fetchOp(opts);
  return (index) => (index % 2 === 0 ? produceFor(index) : fetch);
}

// Dispatches to the right driver for `durationMs`, recording into `stats`. --rate selects open-loop
// (stream is push-only, so --rate does not apply and it always uses the stream driver).
function runScenario(scenario, clients, opts, durationMs, stats) {
  if (scenario === 'stream') {
    return streamDriver(clients, opts, durationMs, stats);
  }
  const makeOpFor = scenarioOps(scenario, opts);
  if (opts.rate > 0) {
    return openLoop(clients, makeOpFor, opts, durationMs, stats);
  }
  return closedLoop(clients, makeOpFor, performance.now() + durationMs, stats, opts);
}

function int(v, fallback) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

if (require.main === module) main();

module.exports = { Stats };
