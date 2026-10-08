'use strict';

/**
 * Where a key falls in a topic's keyspace, exactly as the broker computes it
 * (`Malachi.Keyspace.position_of/2`, which is `:erlang.phash2(key, size)`), so a client can route a record
 * to the range that owns it, as a NorthGuard client picks the shard itself (transcript 885-886).
 *
 * `phash2` of a binary is Bob Jenkins' block hash seeded with HCONST_13 (erts make_hash2, OTP 28,
 * erts/emulator/beam/erl_term_hashing.c); of the atom `nil` (a record with no key) it is the atom table's
 * hash of "nil", a constant. The positions are checked against vectors the broker itself produced
 * (test/support/fixtures/wire/keyspace_vectors.json), from Elixir and from Node.
 */

const HCONST = 0x9e3779b9;
const HCONST_13 = 0x08d12e65;
// :erlang.phash2(nil, 4294967296), the hash of a record with no key
const NIL_HASH = 29948;

// Bob Jenkins' mix, on unsigned 32-bit words.
function mix(a, b, c) {
  a = (a - b - c) >>> 0; a = (a ^ (c >>> 13)) >>> 0;
  b = (b - c - a) >>> 0; b = (b ^ (a << 8)) >>> 0;
  c = (c - a - b) >>> 0; c = (c ^ (b >>> 13)) >>> 0;
  a = (a - b - c) >>> 0; a = (a ^ (c >>> 12)) >>> 0;
  b = (b - c - a) >>> 0; b = (b ^ (a << 16)) >>> 0;
  c = (c - a - b) >>> 0; c = (c ^ (b >>> 5)) >>> 0;
  a = (a - b - c) >>> 0; a = (a ^ (c >>> 3)) >>> 0;
  b = (b - c - a) >>> 0; b = (b ^ (a << 10)) >>> 0;
  c = (c - a - b) >>> 0; c = (c ^ (b >>> 15)) >>> 0;
  return [a, b, c];
}

function word(k, i) {
  return (k[i] | (k[i + 1] << 8) | (k[i + 2] << 16) | (k[i + 3] << 24)) >>> 0;
}

// erts block_hash: 12 bytes per round, then the tail folded into a, b and c (the low byte of c holds the
// length), then one last mix.
function blockHash(bytes, initval) {
  let a = HCONST;
  let b = HCONST;
  let c = initval >>> 0;
  let i = 0;
  const len = bytes.length;
  while (len - i >= 12) {
    a = (a + word(bytes, i)) >>> 0;
    b = (b + word(bytes, i + 4)) >>> 0;
    c = (c + word(bytes, i + 8)) >>> 0;
    [a, b, c] = mix(a, b, c);
    i += 12;
  }
  const tail = len - i;
  c = (c + len) >>> 0;
  if (tail >= 11) c = (c + (bytes[i + 10] << 24)) >>> 0;
  if (tail >= 10) c = (c + (bytes[i + 9] << 16)) >>> 0;
  if (tail >= 9) c = (c + (bytes[i + 8] << 8)) >>> 0;
  if (tail >= 8) b = (b + (bytes[i + 7] << 24)) >>> 0;
  if (tail >= 7) b = (b + (bytes[i + 6] << 16)) >>> 0;
  if (tail >= 6) b = (b + (bytes[i + 5] << 8)) >>> 0;
  if (tail >= 5) b = (b + bytes[i + 4]) >>> 0;
  if (tail >= 4) a = (a + (bytes[i + 3] << 24)) >>> 0;
  if (tail >= 3) a = (a + (bytes[i + 2] << 16)) >>> 0;
  if (tail >= 2) a = (a + (bytes[i + 1] << 8)) >>> 0;
  if (tail >= 1) a = (a + bytes[i]) >>> 0;
  [a, b, c] = mix(a, b, c);
  return c;
}

// The 32-bit phash2 of a key: a string (UTF-8), a Buffer, or null for a record with no key.
function phash2(key) {
  if (key === null || key === undefined) return NIL_HASH;
  const bytes = Buffer.isBuffer(key) ? key : Buffer.from(key, 'utf8');
  return bytes.length === 0 ? HCONST_13 : blockHash(bytes, HCONST_13);
}

// The position of `key` in a keyspace of `size` positions (1 to 2^32), as `:erlang.phash2(key, size)`.
function positionOf(key, size) {
  if (!Number.isInteger(size) || size < 1 || size > 2 ** 32) throw new Error(`keyspace size out of range: ${size}`);
  return phash2(key) % size;
}

module.exports = { phash2, positionOf };
