'use strict';

/**
 * The payload generator against the golden vectors the Elixir one is tested with
 * (test/support/fixtures/loadtest/payload_vectors.json), and the json, random and constant modes against
 * the compression band that fixture states. Run: `node scripts/lib/payload.selftest.js`. No server needed.
 * ExUnit runs it (test/scripts/payload_js_test.exs), and `node scripts/loadtest.js --self-test` runs it too.
 *
 * The band needs zstd in this runtime's zlib (Node 22.15 or later); without it this exits 1 saying so,
 * rather than passing on the vectors alone.
 *
 * `--digest <mode> <seed> <record_size> <count>` prints the SHA-256 of those values instead, and
 * `--constants` prints the sizes the pool rules give, both for that test to compare with the Elixir side
 * live.
 */

const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const payload = require('./payload');

const digest = (values) => {
  const hash = crypto.createHash('sha256');
  for (const value of values) hash.update(value);
  return hash.digest('hex');
};

// Exported for loadtest.js --self-test, which runs this before its own checks.
function run(fixture = path.join(__dirname, '../../test/support/fixtures/loadtest/payload_vectors.json')) {
  const { prng, digests, ratio_band: band } = JSON.parse(fs.readFileSync(fixture, 'utf8'));
  let checked = 0;

  for (const [seedText, expected] of Object.entries(prng)) {
    const state = payload.seed(Number(seedText));
    assert.deepStrictEqual(Array.from({ length: expected.length }, () => payload.next(state)), expected, `prng seed ${seedText}`);
    checked++;
  }

  for (const c of digests) {
    assert.strictEqual(digest(payload.values(c.mode, c.seed, c.record_size, c.count)), c.sha256,
      `${c.mode} seed ${c.seed} size ${c.record_size}`);
    checked++;
  }

  // A json value is exactly the record size, parses, and a size that cannot hold one is refused.
  const min = payload.minJsonSize();
  for (const value of payload.values('json', 7, min, 50)) {
    assert.strictEqual(value.length, min);
    JSON.parse(value.toString('latin1'));
  }
  assert.throws(() => payload.values('json', 1, min - 1, 1), new RegExp(`needs --record-size >= ${min}`));

  if (!payload.hasZstd()) {
    throw new Error(`this Node (${process.version}) has no zstd in zlib; the compression band needs a Node with zlib zstd (22.15 or later)`);
  }

  const ratio = (values, perBlock) => {
    let raw = 0;
    let packed = 0;
    for (let i = 0; i + perBlock <= values.length; i += perBlock) {
      const block = Buffer.concat(values.slice(i, i + perBlock));
      raw += block.length;
      packed += zlib.zstdCompressSync(block, { params: { [zlib.constants.ZSTD_c_compressionLevel]: band.level } }).length;
    }
    return raw / packed;
  };

  const blocks = Object.keys(band.json_reference).map(Number).sort((a, b) => a - b);
  const count = blocks[blocks.length - 1] * 2;
  const json = payload.values('json', band.seed, band.record_size, count);
  const random = payload.values('random', band.seed, band.record_size, count);
  const constant = payload.values('constant', band.seed, band.record_size, count);

  let previous = 0;
  for (const perBlock of blocks) {
    const reference = band.json_reference[String(perBlock)];
    const got = ratio(json, perBlock);
    assert.ok(Math.abs(got - reference) <= reference * band.json_tolerance,
      `json at ${perBlock} per block compresses ${got.toFixed(2)}x, outside ${reference}x +-${band.json_tolerance * 100}%`);
    assert.ok(got > previous, `json at ${perBlock} per block (${got.toFixed(2)}x) does not compress better than fewer`);
    previous = got;

    const noise = ratio(random, perBlock);
    assert.ok(noise <= band.random_max, `random at ${perBlock} per block compresses ${noise.toFixed(2)}x`);
    if (perBlock >= 10) assert.ok(noise >= band.random_min_from_10, `random at ${perBlock} per block: ${noise.toFixed(2)}x`);

    const trivial = ratio(constant, perBlock);
    assert.ok(trivial >= band.constant_min, `constant at ${perBlock} per block is only ${trivial.toFixed(2)}x`);
    checked++;
  }

  return checked;
}

function constants() {
  const grid = [[1, 1, 1, 1], [16, 10, 1, 1], [128, 10, 4, 1], [149, 1000, 1, 1], [256, 10, 512, 1], [256, 4096, 64, 1],
    [256, 1024, 128, 1], [1024, 100, 32, 1], [1048576, 3, 1, 1], [256, 4096, 64, 32], [256, 10, 128, 64], [1048576, 1, 2, 8]];
  return {
    min_json_size: payload.minJsonSize(),
    default_seed: payload.DEFAULT_SEED,
    modes: payload.MODES,
    pool_size: Object.fromEntries(
      grid.map(([size, batch, conns, pipe]) => [`${size}x${batch}x${conns}x${pipe}`, payload.poolSize(size, batch, conns, pipe)]),
    ),
    start_batch: [[0, 100, 8], [3, 100, 8], [7, 100, 8], [5, 3, 10], [9, 1, 4]].map(([i, b, c]) => payload.startBatch(i, b, c)),
  };
}

if (require.main === module) {
  const [flag, ...args] = process.argv.slice(2);
  if (flag === '--digest') {
    const [mode, seed, size, count] = args;
    console.log(digest(payload.values(mode, Number(seed), Number(size), Number(count))));
  } else if (flag === '--constants') {
    console.log(JSON.stringify(constants()));
  } else {
    try {
      console.log(`payload.selftest.js passed ${run(flag)} checks`);
    } catch (err) {
      console.error(`payload.selftest.js FAILED: ${err.message}`);
      process.exit(1);
    }
  }
}

module.exports = { run };
