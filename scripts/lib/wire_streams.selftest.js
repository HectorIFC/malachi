'use strict';

/**
 * The stream, consume, group and routing codecs (keys 24 to 34) and the keyspace hash against the golden
 * frames and vectors the Elixir side is tested with (test/support/fixtures/wire/{route,stream,consume,
 * group}_frames.json and keyspace_vectors.json). No server needed. ExUnit runs it
 * (test/scripts/wire_streams_js_test.exs), so a byte the two clients disagree on fails the Elixir build too.
 *
 *   node scripts/lib/wire_streams.selftest.js                 checks every frame and vector
 *   node scripts/lib/wire_streams.selftest.js --encode-zstd   prints a zstd batch Node compressed, as hex
 *   node scripts/lib/wire_streams.selftest.js --decode HEX    prints the records of a batch, as JSON
 *
 * The last two let the Elixir test check zstd across the runtimes: each side's compressor writes bytes
 * of its own, so a zstd batch has no golden bytes, only golden records.
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const wire = require('./wire');
const keyspace = require('./keyspace');

const fixtureDir = path.join(__dirname, '../../test/support/fixtures/wire');
const read = (name) => JSON.parse(fs.readFileSync(path.join(fixtureDir, name), 'utf8'));

// The records of an encoded batch as the fixtures write them.
function batchJson(batch, layout) {
  const codec = ['none', 'zstd'][batch[0]];
  const entries = wire.decodeBatch(batch, { maxInflatedBytes: 16 * 1024 * 1024, layout });
  const recordJson = (rec) => ({
    key: rec.key,
    value: rec.value.toString('utf8'),
    timestamp: Number(rec.timestamp),
    headers: Object.entries(rec.headers),
  });
  const records = entries.map((e) =>
    layout === 'plain'
      ? { tombstone: e.tombstone, record: recordJson(e.record) }
      : { position: e.position, tombstone: e.tombstone, record: recordJson(e.record) },
  );
  return { codec, layout, records };
}

// A record of the fixtures as encodeRecord takes it.
function record(r) {
  return { key: r.key, value: r.value, timestamp: r.timestamp, headers: Object.fromEntries(r.headers) };
}

function batch(b) {
  const entries = b.records.map((e) => ({ ...e, record: record(e.record) }));
  return wire.encodeBatch(entries, b.codec, b.layout);
}

// A page as the fixtures write it. Its batch is also encoded back from that JSON and compared byte for
// byte, so the Node encoder of a positioned batch is held to the golden bytes too.
function pageJson(page) {
  const { batch: encoded, ...rest } = page;
  const json = batchJson(encoded, 'positioned');
  assert.strictEqual(batch(json).toString('hex'), encoded.toString('hex'), 'a positioned batch encodes back');
  return { ...rest, batch: json };
}

// Requests the client sends are encoded and compared byte for byte; what the server sends is decoded and
// compared value for value.
const requests = {
  cluster_state_req: () => wire.encodeClusterStateReq(),
  topic_routes_req: (v) => wire.encodeTopicRoutesReq(v),
  open_stream_req: (v) => wire.encodeOpenStreamReq(v),
  append_req: (v) => wire.encodeAppendReq(v.stream_id, v.sequence, batch(v.batch)),
  close_stream_req: (v) => wire.encodeCloseStreamReq(v),
  open_consume_req: (v) => wire.encodeOpenConsumeReq(v),
  consume_ack_req: (v) => wire.encodeConsumeAckReq(v),
  fetch_range_req: (v) => wire.encodeFetchRangeReq(v),
  join_group_req: (v) => wire.encodeJoinGroupReq(v),
  group_heartbeat_req: (v) => wire.encodeGroupHeartbeatReq(v),
  commit_offsets_req: (v) => wire.encodeCommitOffsetsReq(v),
};

const responses = {
  cluster_state_resp: (b) => wire.decodeClusterStateResp(b),
  topic_routes_resp: (b) => wire.decodeTopicRoutesResp(b),
  open_stream_resp: (b) => wire.decodeOpenStreamResp(b),
  open_consume_resp: (b) => wire.decodeOpenConsumeResp(b),
  page: (b) => pageJson(wire.decodePage(b)),
  push: (b) => {
    const [kind, push] = wire.decodePush(b);
    return [kind, kind === 'records' ? pageJson(push) : push];
  },
  assignment_resp: (b) => wire.decodeAssignmentResp(b),
};

const RECORDS = [
  { tombstone: false, record: { key: 'k1', value: 'v'.repeat(200), timestamp: 1767225600000, headers: {} } },
  { tombstone: true, record: { key: null, value: 'w'.repeat(200), timestamp: 1767225600001, headers: { trace: 'abc' } } },
];

function main(args) {
  if (args[0] === '--encode-zstd') {
    console.log(wire.encodeBatch(RECORDS, 'zstd').toString('hex'));
    return;
  }
  if (args[0] === '--decode') {
    console.log(JSON.stringify(batchJson(Buffer.from(args[1], 'hex'), 'plain')));
    return;
  }

  let frames = 0;
  for (const fixture of ['route_frames.json', 'stream_frames.json', 'consume_frames.json', 'group_frames.json']) {
    for (const c of read(fixture).cases) {
      const bytes = Buffer.from(c.hex, 'hex');
      if (requests[c.codec]) {
        assert.strictEqual(requests[c.codec](c.value).toString('hex'), c.hex, `${c.codec}: ${c.name}`);
      } else if (responses[c.codec]) {
        assert.deepStrictEqual(responses[c.codec](bytes), c.value, `${c.codec}: ${c.name}`);
      } else {
        throw new Error(`no Node codec for ${c.codec}`);
      }
      frames++;
    }
  }

  const vectors = read('keyspace_vectors.json');
  let positions = 0;
  for (const c of vectors.cases) {
    assert.strictEqual(keyspace.phash2(c.key), c.hash, `phash2(${JSON.stringify(c.key)})`);
    vectors.sizes.forEach((size, i) => {
      assert.strictEqual(keyspace.positionOf(c.key, size), c.positions[i], `positionOf(${JSON.stringify(c.key)}, ${size})`);
      positions++;
    });
  }

  // A batch is refused past the cap the reader gives, before anything is inflated, and a malformed answer
  // is refused rather than half read.
  const big = wire.encodeBatch(RECORDS, 'zstd');
  assert.throws(() => wire.decodeBatch(big, { maxInflatedBytes: 10 }), /batch_too_large/);
  assert.deepStrictEqual(wire.decodeBatch(wire.encodeBatch([], 'zstd'), { maxInflatedBytes: 0 }), []);
  const u32 = (n) => {
    const b = Buffer.alloc(4);
    b.writeUInt32BE(n);
    return b;
  };
  const lying = (inflatedSize, payload) => Buffer.concat([Buffer.from([1, 0, 0, 0, 1]), u32(inflatedSize), u32(payload.length), payload]);
  const tenZeros = zlib.zstdCompressSync(Buffer.alloc(10));
  assert.throws(() => wire.decodeBatch(lying(5, tenZeros), { maxInflatedBytes: 100 }), /batch_size_mismatch/);
  assert.throws(() => wire.decodeBatch(lying(20, tenZeros), { maxInflatedBytes: 100 }), /batch_size_mismatch/);
  assert.throws(() => wire.decodeBatch(lying(0, tenZeros.subarray(0, 8)), { maxInflatedBytes: 100 }), /malformed_batch/);
  assert.throws(() => wire.decodeBatch(lying(10, Buffer.from('garbage!garbage!')), { maxInflatedBytes: 100 }), /malformed_batch/);
  assert.throws(() => wire.encodeBatch([], 'lz4'), /unknown/);
  // A plain batch of one entry with its flags byte (the first byte after the 13-byte header) set to 2.
  const reserved = Buffer.from(wire.encodeBatch([RECORDS[0]], 'none'));
  reserved[13] = 2;
  assert.throws(() => wire.decodeBatch(reserved, { maxInflatedBytes: 1000 }), /reserved flags 2/);
  const consume = { topic: 't', range: 0, routes_version: 0, window: 1, max: 1, max_bytes: 1, accept: ['none'] };
  assert.throws(() => wire.encodeOpenConsumeReq({ ...consume, start: 'newest' }), /unknown start/);
  // A window is an integer from 1 to 2^32 - 1, checked before encoding: writeUInt32BE turns its argument
  // into a number without a word (undefined, NaN, null or '0' as 0, '5' as 5, a fraction truncated) and
  // throws only outside 0 to 2^32 - 1.
  const stream = { topic: 't', range: 0, routes_version: 0, codec: 'none', producer_id: null, label: null };
  for (const bad of [0, -1, 0.5, NaN, undefined, '0', '5', null, 2 ** 32]) {
    assert.throws(() => wire.encodeOpenConsumeReq({ ...consume, start: 'earliest', window: bad }), /from 1 to 4294967295/);
    assert.throws(() => wire.encodeOpenStreamReq({ ...stream, window_appends: bad, window_bytes: 1 }), /from 1 to 4294967295/);
    assert.throws(() => wire.encodeOpenStreamReq({ ...stream, window_appends: 1, window_bytes: bad }), /from 1 to 4294967295/);
  }
  const routes = read('route_frames.json').cases.find((c) => c.codec === 'topic_routes_resp');
  assert.throws(() => wire.decodeTopicRoutesResp(Buffer.concat([Buffer.from(routes.hex, 'hex'), Buffer.from([0])])), /trailing bytes/);
  assert.throws(() => keyspace.positionOf('k', 0), /out of range/);

  console.log(`wire_streams.selftest.js passed ${frames} golden frames and ${positions} keyspace positions`);
}

main(process.argv.slice(2));
