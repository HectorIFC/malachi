'use strict';

/**
 * The record values the load generators send, mirrored value for value by `Malachi.Loadtest.Payload`
 * (lib/malachi/loadtest/payload.ex), whose moduledoc is the specification: the same mode, seed and size
 * give byte-identical values in both generators, which the golden vectors in
 * test/support/fixtures/loadtest/payload_vectors.json pin.
 *
 *   constant - the byte 'a' repeated (the Elixir generator repeats 'x'; both are what the published
 *              series were measured with, and constant bytes compress to almost nothing)
 *   json     - a seeded, event-like JSON document of exactly the record size
 *   random   - seeded uniform bytes, the incompressible control
 *
 * The generator is xoshiro128** (Blackman and Vigna), seeded with SplitMix64. Every step is kept to 32
 * bits with Math.imul and `>>> 0`; BigInt appears only in the seeding, once per run.
 */

const zlib = require('zlib');

const MODES = ['constant', 'json', 'random'];
const DEFAULT_SEED = 1;

const POOL_TARGET_BYTES = 8 * 1024 * 1024;
const POOL_MAX_VALUES = 65536;

const TS_BASE = 1700000000000;
const TYPES = ['order.created', 'order.paid', 'order.shipped', 'order.cancelled', 'cart.updated', 'user.signup', 'user.login', 'page.view'];
const REGIONS = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'sa-east'];
const STATUSES = ['ok', 'pending', 'failed', 'retry'];
const WORDS = [
  'the', 'a', 'an', 'to', 'of', 'and', 'in', 'on', 'for', 'with', 'from', 'by', 'at', 'as', 'is', 'was', 'are', 'be',
  'been', 'has', 'have', 'had', 'not', 'but', 'or', 'if', 'then', 'when', 'this', 'that', 'order', 'payment', 'item',
  'cart', 'user', 'account', 'session', 'request', 'service', 'client', 'server', 'queue', 'stream', 'event', 'record',
  'batch', 'retry', 'timeout', 'error', 'update', 'create', 'delete', 'shipped', 'pending', 'checkout', 'price', 'total',
  'amount', 'region', 'warehouse', 'status', 'delivery', 'customer', 'refund',
];

if (WORDS.length !== 64) throw new Error(`the vocabulary must hold 64 words, it holds ${WORDS.length}`);

const MASK64 = (1n << 64n) - 1n;

// The xoshiro128** state for a 32-bit seed: two SplitMix64 outputs, each split into its low and high words.
function seed(value) {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
    throw new RangeError(`a payload seed must be an integer from 0 to 4294967295, got ${value}`);
  }
  let x = BigInt(value);
  const splitmix64 = () => {
    x = (x + 0x9e3779b97f4a7c15n) & MASK64;
    let z = x;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK64;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK64;
    return z ^ (z >> 31n);
  };
  const a = splitmix64();
  const b = splitmix64();
  const lo = (v) => Number(v & 0xffffffffn);
  const hi = (v) => Number(v >> 32n);
  return [lo(a), hi(a), lo(b), hi(b)];
}

const rotl = (x, k) => ((x << k) | (x >>> (32 - k))) >>> 0;

// The next 32-bit output; advances `s` in place.
function next(s) {
  const result = Math.imul(rotl(Math.imul(s[1], 5) >>> 0, 7), 9) >>> 0;
  const t = (s[1] << 9) >>> 0;
  s[2] = (s[2] ^ s[0]) >>> 0;
  s[3] = (s[3] ^ s[1]) >>> 0;
  s[1] = (s[1] ^ s[2]) >>> 0;
  s[0] = (s[0] ^ s[3]) >>> 0;
  s[2] = (s[2] ^ t) >>> 0;
  s[3] = rotl(s[3], 11);
  return result;
}

// A draw in 0..n-1.
const uniform = (s, n) => next(s) % n;

function skeleton(id, ts, type, user, region, status, amount) {
  return `{"id":"${id}","ts":${ts},"type":"${type}","user":"${user}","region":"${region}",` +
    `"status":"${status}","amount":${amount},"msg":""}`;
}

// The smallest record size a json value fits in: every field at its widest and an empty msg.
function minJsonSize() {
  const widest = (list) => Math.max(...list.map((w) => w.length));
  return skeleton('f'.repeat(16), TS_BASE + 9999999, 't'.repeat(widest(TYPES)), 'u0000',
    'r'.repeat(widest(REGIONS)), 's'.repeat(widest(STATUSES)), 99999).length;
}

function checkJsonSize(recordSize) {
  const min = minJsonSize();
  if (recordSize < min) throw new RangeError(`json payload needs --record-size >= ${min}, got ${recordSize}`);
}

const hex8 = (x) => x.toString(16).padStart(8, '0');

function jsonValue(s, index, size) {
  const hi = next(s);
  const lo = next(s);
  const jitter = uniform(s, 7);
  const type = TYPES[uniform(s, TYPES.length)];
  const user = `u${String(uniform(s, 10000)).padStart(4, '0')}`;
  const region = REGIONS[uniform(s, REGIONS.length)];
  const status = STATUSES[uniform(s, STATUSES.length)];
  const amount = 100 + uniform(s, 99900);
  const head = skeleton(hex8(hi) + hex8(lo), TS_BASE + index * 7 + jitter, type, user, region, status, amount);

  // Words separated by spaces until the room is covered, then cut to exactly the room.
  const room = size - head.length;
  let msg = '';
  while (msg.length < room) {
    const word = WORDS[uniform(s, WORDS.length)];
    msg = msg === '' ? word : `${msg} ${word}`;
  }
  return Buffer.from(head.slice(0, -2) + msg.slice(0, room) + '"}', 'latin1');
}

// Four bytes per draw, big-endian, the last draw cut to fit.
function randomValue(s, size) {
  const out = Buffer.alloc(Math.ceil(size / 4) * 4);
  for (let i = 0; i < out.length; i += 4) out.writeUInt32BE(next(s), i);
  return out.subarray(0, size);
}

// The first `count` values of `mode` for `seedValue` at `recordSize` bytes, in pool order. constant ignores
// the seed. Throws a RangeError for a json size below minJsonSize().
function values(mode, seedValue, recordSize, count) {
  if (mode === 'constant') {
    const value = Buffer.alloc(recordSize, 0x61); // 'a'
    return Array.from({ length: count }, () => value);
  }
  if (mode === 'json') checkJsonSize(recordSize);
  const s = seed(seedValue);
  const out = new Array(count);
  for (let i = 0; i < count; i++) out[i] = mode === 'json' ? jsonValue(s, i, recordSize) : randomValue(s, recordSize);
  return out;
}

// How many values the pool holds: min(ceil(8MiB / recordSize), 65536), rounded up to whole batches, and at
// least `pipeline` batches per connection, so every connection's first burst of `pipeline` produces is its
// own. The argument mirrors pool_size/4 in Elixir. This generator passes 1, which holds for its closed loop,
// one produce in flight per connection. The open loop (--rate) keeps as many in flight as the rate needs,
// on any connection, so there a connection can run into its neighbour's batches from the start.
function poolSize(recordSize, batch, connections, pipeline) {
  const wanted = Math.min(Math.ceil(POOL_TARGET_BYTES / recordSize), POOL_MAX_VALUES);
  const batches = Math.max(Math.ceil(wanted / batch), connections * pipeline);
  return batches * batch;
}

// The batch connection `index` starts at, out of `batches`, when `connections` share the pool.
function startBatch(index, batches, connections) {
  return (index * Math.max(1, Math.floor(batches / connections))) % batches;
}

// Whether this runtime's zlib has zstd (Node 22.15 or later), which the compression checks need.
function hasZstd() {
  return typeof zlib.zstdCompressSync === 'function';
}

module.exports = {
  MODES, DEFAULT_SEED, seed, next, values, minJsonSize, checkJsonSize, poolSize, startBatch, hasZstd,
};
