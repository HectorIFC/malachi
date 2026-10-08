'use strict';

/**
 * The Malachi binary wire protocol, in Node.js: a faithful port of lib/malachi/wire.ex.
 *
 * Every message is a length-prefixed frame:
 *
 *   Frame:     <len:u32> <body>
 *   Request:   <api_key:u16> <correlation_id:u32> <payload>
 *   Response:  <correlation_id:u32> <error_code:u16> <payload>   // 0 = ok, 1 = error (reason string)
 *
 * A string is length-prefixed with a presence flag (0 => null, 1 => <len:u32><bytes>), so a record on
 * the wire carries no offset: position travels only in the opaque cursor.
 */

const API = {
  auth: 0,
  createTopic: 1,
  produce: 2,
  fetch: 3,
  commit: 4,
  subscribe: 5,
  streamAck: 6,
  leaveGroup: 7,
  // admin user management (require the admin permission)
  createUser: 8,
  deleteUser: 9,
  changePassword: 10,
  listUsers: 11,
  // admin per-topic ACL management (require the admin permission)
  grantAcl: 14,
  revokeAcl: 15,
  listAcls: 16,
  // admin storage policies (require the admin permission, #194)
  definePolicy: 17,
  deletePolicy: 18,
  listPolicies: 19,
  bindTopicPolicy: 20,
  getTopicPolicy: 21,
  // admin console roles (require the admin permission, #228)
  setRole: 22,
  listUsersWithRoles: 23,
  // streams, routing and groups (#275, see "Streams, routing and groups" in lib/malachi/wire.ex)
  clusterState: 24,
  topicRoutes: 25,
  openStream: 26,
  append: 27,
  closeStream: 28,
  openConsume: 29,
  consumeAck: 30,
  fetchRange: 31,
  joinGroup: 32,
  groupHeartbeat: 33,
  commitOffsets: 34,
};

const OK = 0;
const ERROR = 1;

// ---- primitives ----

function u16(n) {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16BE(n, 0);
  return b;
}

function u32(n) {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32BE(n, 0);
  return b;
}

function u64(n) {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64BE(BigInt(n), 0);
  return b;
}

// length-prefixed string with a presence flag; `null`/`undefined` encode as absent (flag 0).
function putStr(s) {
  if (s === null || s === undefined) return Buffer.from([0]);
  const bytes = Buffer.isBuffer(s) ? s : Buffer.from(s, 'utf8');
  return Buffer.concat([Buffer.from([1]), u32(bytes.length), bytes]);
}

// A cursor over a Buffer, reading the same shapes the Elixir codec writes.
class Reader {
  constructor(buffer) {
    this.buf = buffer;
    this.pos = 0;
  }
  u16() {
    const v = this.buf.readUInt16BE(this.pos);
    this.pos += 2;
    return v;
  }
  u32() {
    const v = this.buf.readUInt32BE(this.pos);
    this.pos += 4;
    return v;
  }
  u64() {
    const v = this.buf.readBigUInt64BE(this.pos);
    this.pos += 8;
    return v;
  }
  u8() {
    const v = this.buf.readUInt8(this.pos);
    this.pos += 1;
    return v;
  }
  // a u64 as a Number when it is exact as one, a BigInt past 2^53
  num64() {
    const v = this.u64();
    return v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
  }
  // a counted list: <count:u16> item*
  list(take) {
    const count = this.u16();
    const items = [];
    for (let i = 0; i < count; i++) items.push(take(this));
    return items;
  }
  bytes(len) {
    const v = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return v;
  }
  // presence-flagged string: returns null or a utf8 string
  str() {
    const flag = this.buf.readUInt8(this.pos);
    this.pos += 1;
    if (flag === 0) return null;
    const len = this.u32();
    return this.bytes(len).toString('utf8');
  }
}

// ---- framing / envelope ----

function encodeFrame(body) {
  return Buffer.concat([u32(body.length), body]);
}

// The largest frame this client will accept from the server, mirroring the server's own max_frame_size
// (config/config.exs). Without a cap, a hostile or MITM server can declare a 4 GiB length and dribble
// bytes, and the client's buffer grows toward it until the process runs out of memory. The server bounds
// the client's input the same way (frame_too_large in lib/malachi/wire.ex); this bounds the server's.
const MAX_FRAME_BYTES = 16 * 1024 * 1024;

// Peels one frame off a buffer: { body, rest }, null if the whole frame is not present yet, or throws on a
// declared length past the cap (which no honest server would send).
function decodeFrame(buffer) {
  if (buffer.length < 4) return null;
  const len = buffer.readUInt32BE(0);
  if (len > MAX_FRAME_BYTES) throw new Error(`frame_too_large: ${len} > ${MAX_FRAME_BYTES}`);
  if (buffer.length < 4 + len) return null;
  return { body: buffer.subarray(4, 4 + len), rest: buffer.subarray(4 + len) };
}

function encodeRequest(apiKey, correlationId, payload) {
  return encodeFrame(Buffer.concat([u16(apiKey), u32(correlationId), payload]));
}

function decodeResponse(body) {
  const r = new Reader(body);
  const correlationId = r.u32();
  const errorCode = r.u16();
  return { correlationId, errorCode, payload: body.subarray(r.pos) };
}

// ---- operation payloads ----

function encodeAuthReq(username, password) {
  return Buffer.concat([putStr(username), putStr(password)]);
}

// auth response / an error response both carry a single string (token, or the error reason).
function decodeString(payload) {
  return new Reader(payload).str();
}

function encodeCreateTopicReq(topic, keyspaceBits) {
  return Buffer.concat([putStr(topic), Buffer.from([keyspaceBits])]);
}

// records: [{ key?, value, headers?: {k:v}, timestamp? }]
function encodeRecord(record) {
  const value = Buffer.isBuffer(record.value) ? record.value : Buffer.from(String(record.value), 'utf8');
  const ts = record.timestamp !== undefined ? record.timestamp : Date.now();
  const headers = record.headers || {};
  const entries = Object.entries(headers);
  const headerBody = Buffer.concat(entries.map(([k, v]) => Buffer.concat([putStr(k), putStr(String(v))])));
  return Buffer.concat([
    putStr(record.key ?? null),
    u32(value.length),
    value,
    u64(ts),
    u32(entries.length),
    headerBody,
  ]);
}

function encodeProduceReq(topic, records) {
  const body = Buffer.concat(records.map(encodeRecord));
  return Buffer.concat([putStr(topic), u32(records.length), body]);
}

function decodeRecord(r) {
  const key = r.str();
  const valueLen = r.u32();
  const value = r.bytes(valueLen);
  const timestamp = r.u64();
  const headerCount = r.u32();
  const headers = {};
  for (let i = 0; i < headerCount; i++) {
    const k = r.str();
    headers[k] = r.str();
  }
  return { key, value, timestamp, headers };
}

function decodeFetchResp(payload) {
  const r = new Reader(payload);
  const count = r.u32();
  const records = [];
  for (let i = 0; i < count; i++) records.push(decodeRecord(r));
  const cursor = r.str();
  return { records, cursor };
}

// member is an optional consumer-group member id (null = whole-group / single consumer); with it set the
// server scopes the fetch to the member's ranges and returns records + an opaque cursor (no range ids).
function encodeFetchReq(topic, cursor, group, member, max, waitMs) {
  return Buffer.concat([putStr(topic), putStr(cursor), putStr(group), putStr(member), u32(max), u32(waitMs)]);
}

function encodeLeaveGroupReq(topic, group, member) {
  return Buffer.concat([putStr(topic), putStr(group), putStr(member)]);
}

function encodeCommitReq(topic, group, cursor) {
  return Buffer.concat([putStr(topic), putStr(group), putStr(cursor)]);
}

// member is an optional consumer-group member id (null = whole-group subscription); with it set the
// server scopes the push stream to the member's ranges (opaque: the push is still records + cursor).
function encodeSubscribeReq(topic, group, member, window, max) {
  return Buffer.concat([putStr(topic), putStr(group), putStr(member), u32(window), u32(max)]);
}

// A member stream_ack doubles as a coordinator heartbeat + range refresh (an empty ack = a heartbeat).
function encodeStreamAckReq(topic, group, member, cursor, count) {
  return Buffer.concat([putStr(topic), putStr(group), putStr(member), putStr(cursor), u32(count)]);
}

// ---- admin user management (permissions are byte strings: "admin"/"produce"/"consume") ----

// permission list: <count::u32, putStr(perm)*>. `perms` is an array of strings.
function putPerms(perms) {
  const body = Buffer.concat(perms.map((p) => putStr(String(p))));
  return Buffer.concat([u32(perms.length), body]);
}

function encodeCreateUserReq(username, password, permissions) {
  return Buffer.concat([putStr(username), putStr(password), putPerms(permissions)]);
}

function encodeDeleteUserReq(username) {
  return putStr(username);
}

function encodeChangePasswordReq(username, newPassword) {
  return Buffer.concat([putStr(username), putStr(newPassword)]);
}

// list_users response: <count::u32, (putStr(username), <count::u32, putStr(perm)*>)*>, no hashes.
function decodeListUsersResp(payload) {
  const r = new Reader(payload);
  const count = r.u32();
  const users = [];
  for (let i = 0; i < count; i++) {
    const username = r.str();
    const permCount = r.u32();
    const permissions = [];
    for (let j = 0; j < permCount; j++) permissions.push(r.str());
    users.push({ username, permissions });
  }
  return users;
}

// ---- admin console roles (#228). A role is "viewer"/"editor"/"admin", or null for no role. ----

function encodeSetRoleReq(username, role) {
  return Buffer.concat([putStr(username), putStr(role ?? null)]);
}

// list_users_with_roles response: <count::u32, (putStr(username), <count::u32, putStr(perm)*>, putStr(role))*>.
function decodeListUsersWithRolesResp(payload) {
  const r = new Reader(payload);
  const count = r.u32();
  const users = [];
  for (let i = 0; i < count; i++) {
    const username = r.str();
    const permCount = r.u32();
    const permissions = [];
    for (let j = 0; j < permCount; j++) permissions.push(r.str());
    users.push({ username, permissions, role: r.str() });
  }
  expectEnd(r);
  return users;
}

// admin per-topic ACL management. operation is "produce"/"consume"; pattern is a topic or a *-suffixed prefix.
// grant and revoke share the request shape.
function encodeAclReq(username, operation, pattern) {
  return Buffer.concat([putStr(username), putStr(operation), putStr(pattern)]);
}

function encodeListAclsReq(username) {
  return putStr(username);
}

// list_acls response: <count::u32, (putStr(operation), putStr(resource))*>.
function decodeListAclsResp(payload) {
  const r = new Reader(payload);
  const count = r.u32();
  const acls = [];
  for (let i = 0; i < count; i++) {
    const operation = r.str();
    const resource = r.str();
    acls.push({ operation, resource });
  }
  return acls;
}

// ---- admin storage policies ----
//
// A policy travels as a counted list of self-describing fields: <count:u16> then, per field,
// putStr(name) and a value tagged with its own type:
//
//   <0:u8>                    null (the rule is off)
//   <1:u8><integer:u64>       a non-negative integer bound
//   <2:u8><len:u32><bytes>    a string
//
// A field left out inherits the cluster's global value, so "inherit", "off" (null) and 0 stay distinct.
// Fields are [name, value] pairs, in the order given. The server refuses an unknown field by name.

// The settable fields and how the CLI parses each one: a mirror of Malachi.Cluster.Policy.fields/0, kept
// in step by test/scripts/policy_js_test.exs. A field this table lacks is still sent (as an integer when it
// is all digits, else as a string) and the server answers it by name.
const POLICY_FIELDS = {
  'retention.max_age_ms': 'bound',
  'retention.max_bytes': 'bound',
  spread_by: 'attribute',
};

const U64_MAX = 2n ** 64n - 1n;
const RESOLUTIONS = ['none', 'resolved', 'unresolved'];
const ORIGINS = ['global', 'policy', 'unresolved_backstop'];

function putValue(value) {
  if (value === null || value === undefined) return Buffer.from([0]);
  if (typeof value === 'string') {
    const bytes = Buffer.from(value, 'utf8');
    return Buffer.concat([Buffer.from([2]), u32(bytes.length), bytes]);
  }
  // A number is taken only while it is exact: past 2^53 it has already been rounded, and sending it
  // would set a different bound than the caller wrote. Larger bounds travel as a BigInt, up to the
  // 2^64 - 1 the wire carries.
  if (
    (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) ||
    (typeof value === 'bigint' && value >= 0n && value <= U64_MAX)
  ) {
    return Buffer.concat([Buffer.from([1]), u64(value)]);
  }
  throw new Error(`a policy value is null, a non-negative integer or a string, got: ${value}`);
}

function putFields(fields) {
  return Buffer.concat([u16(fields.length), ...fields.map(([name, value]) => Buffer.concat([putStr(name), putValue(value)]))]);
}

// Integers past 2^53 come back as BigInt, so a large byte budget is never rounded.
function readValue(r) {
  const tag = r.bytes(1).readUInt8(0);
  if (tag === 0) return null;
  if (tag === 1) {
    const v = r.u64();
    return v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
  }
  if (tag === 2) return r.bytes(r.u32()).toString('utf8');
  throw new Error(`unknown policy value tag ${tag}`);
}

function readFields(r) {
  const count = r.u16();
  const fields = [];
  for (let i = 0; i < count; i++) fields.push([r.str(), readValue(r)]);
  return fields;
}

function expectEnd(r) {
  if (r.pos !== r.buf.length) throw new Error(`${r.buf.length - r.pos} trailing bytes`);
}

function codeOf(table, code) {
  if (code >= table.length) throw new Error(`unknown code ${code}`);
  return table[code];
}

function encodeDefinePolicyReq(name, fields) {
  return Buffer.concat([putStr(name), putFields(fields)]);
}

function encodeDeletePolicyReq(name, force) {
  return Buffer.concat([putStr(name), Buffer.from([force ? 1 : 0])]);
}

function encodeListPoliciesReq() {
  return Buffer.alloc(0);
}

// list_policies response: <count:u32> then (putStr(name), fields)*. Returns [[name, fields], ...].
function decodeListPoliciesResp(payload) {
  const r = new Reader(payload);
  const count = r.u32();
  const policies = [];
  for (let i = 0; i < count; i++) policies.push([r.str(), readFields(r)]);
  expectEnd(r);
  return policies;
}

// A null name detaches the topic from its policy.
function encodeBindTopicPolicyReq(topic, name) {
  return Buffer.concat([putStr(topic), putStr(name)]);
}

function encodeGetTopicPolicyReq(topic) {
  return putStr(topic);
}

// get_topic_policy response: putStr(topic), putStr(policy), resolution:u8, has_definition:u8, [fields],
// <count:u16> then (putStr(name), value, origin:u8)*.
function decodeTopicPolicyResp(payload) {
  const r = new Reader(payload);
  const topic = r.str();
  const policy = r.str();
  const resolution = codeOf(RESOLUTIONS, r.bytes(1).readUInt8(0));
  const hasDefinition = r.bytes(1).readUInt8(0);
  if (hasDefinition > 1) throw new Error(`bad definition flag ${hasDefinition}`);
  const definition = hasDefinition === 1 ? readFields(r) : null;
  const count = r.u16();
  const effective = [];
  for (let i = 0; i < count; i++) {
    const name = r.str();
    const value = readValue(r);
    effective.push([name, value, codeOf(ORIGINS, r.bytes(1).readUInt8(0))]);
  }
  expectEnd(r);
  return { topic, policy, resolution, definition, effective };
}

// ---- streams, routing and groups (keys 24 to 34, see lib/malachi/wire.ex) ----

const zlib = require('zlib');

const BROKER_STATUSES = ['alive', 'suspect', 'dead'];
const RANGE_STATES = ['active', 'sealed'];
const CODECS = ['none', 'zstd'];
const PUSH_KINDS = ['append_ack', 'moved', 'records'];

function u8(n) {
  return Buffer.from([n]);
}

function codeIn(table, name) {
  const code = table.indexOf(name);
  if (code < 0) throw new Error(`unknown name ${name}`);
  return code;
}

// A counted list: <count:u16> item*
function putList(items, put) {
  return Buffer.concat([u16(items.length), ...items.map(put)]);
}

// A position is [sourceIndex, offset]: the source in a range's history (an ancestor, or the range itself).
function putPosition([source, offset]) {
  return Buffer.concat([u16(source), u64(offset)]);
}

function readPosition(r) {
  return [r.u16(), r.num64()];
}

// Where a consume starts: 'earliest', 'latest', { position: [source, offset] } or { committed: group }.
function putStart(start) {
  if (start === 'earliest') return u8(0);
  if (start === 'latest') return u8(1);
  if (start && typeof start === 'object' && start.position) return Buffer.concat([u8(2), putPosition(start.position)]);
  if (start && typeof start === 'object' && 'committed' in start) return Buffer.concat([u8(3), putStr(start.committed)]);
  throw new Error(`unknown start ${JSON.stringify(start)}`);
}

// The codecs a reader accepts, as a bitmask (bit n for codec code n).
function putAccept(codecs) {
  return u8(codecs.reduce((mask, codec) => mask | (1 << codeIn(CODECS, codec)), 0));
}

function readRouteSegment(r) {
  if (r.u8() === 0) return null;
  return { segment: r.u32(), primary: r.str() };
}

function encodeClusterStateReq() {
  return Buffer.alloc(0);
}

function decodeClusterStateResp(payload) {
  const r = new Reader(payload);
  const version = r.num64();
  const streamsEnabled = r.u8() === 1;
  const brokers = r.list((x) => ({ id: x.str(), host: x.str(), port: x.u16(), status: codeOf(BROKER_STATUSES, x.u8()) }));
  const vnodes = r.list((x) => x.u32());
  expectEnd(r);
  return { version, streams_enabled: streamsEnabled, brokers, vnodes };
}

function encodeTopicRoutesReq(topic) {
  return putStr(topic);
}

function decodeTopicRoutesResp(payload) {
  const r = new Reader(payload);
  const topic = r.str();
  const version = r.num64();
  const keyspaceBits = r.u8();
  const ranges = r.list((x) => ({
    range: x.u32(),
    key_start: x.num64(),
    key_end: x.num64(),
    state: codeOf(RANGE_STATES, x.u8()),
    segment: readRouteSegment(x),
  }));
  expectEnd(r);
  return { topic, version, keyspace_bits: keyspaceBits, ranges };
}

// The window a stream opens with: an integer from 1 to 2^32 - 1 in each count, as Malachi.Wire requires.
// Checked before encoding, because writeUInt32BE turns its argument into a number without a word: undefined,
// NaN, null or a non-numeric string as 0, a numeric string as its value, a fraction truncated. It throws
// only outside 0 to 2^32 - 1.
function openWindow(counts) {
  if (!counts.every((count) => Number.isInteger(count) && count >= 1 && count <= 0xffffffff)) {
    throw new Error(`a stream opens with a window from 1 to 4294967295, not ${JSON.stringify(counts)}`);
  }
}

function encodeOpenStreamReq(req) {
  openWindow([req.window_appends, req.window_bytes]);
  return Buffer.concat([
    putStr(req.topic),
    u32(req.range),
    u64(req.routes_version),
    u8(codeIn(CODECS, req.codec)),
    u32(req.window_appends),
    u32(req.window_bytes),
    putStr(req.producer_id),
    putStr(req.label),
  ]);
}

function decodeOpenStreamResp(payload) {
  const r = new Reader(payload);
  const resp = {
    stream_id: r.u32(),
    segment: r.u32(),
    window_appends: r.u32(),
    window_bytes: r.u32(),
    routes_version: r.num64(),
  };
  expectEnd(r);
  return resp;
}

// `batch` is a Buffer from encodeBatch, encoded once by the sender.
function encodeAppendReq(streamId, sequence, batch) {
  return Buffer.concat([u32(streamId), u64(sequence), batch]);
}

function encodeCloseStreamReq(streamId) {
  return u32(streamId);
}

function encodeOpenConsumeReq(req) {
  openWindow([req.window]);
  return Buffer.concat([
    putStr(req.topic),
    u32(req.range),
    u64(req.routes_version),
    putStart(req.start),
    u32(req.window),
    u32(req.max),
    u32(req.max_bytes),
    putAccept(req.accept),
  ]);
}

function decodeOpenConsumeResp(payload) {
  const r = new Reader(payload);
  const resp = { stream_id: r.u32(), position: readPosition(r) };
  expectEnd(r);
  return resp;
}

function encodeConsumeAckReq(req) {
  return Buffer.concat([u32(req.stream_id), putPosition(req.position), u32(req.window)]);
}

function encodeFetchRangeReq(req) {
  return Buffer.concat([
    putStr(req.topic),
    u32(req.range),
    u64(req.routes_version),
    putStart(req.start),
    u32(req.max),
    u32(req.max_bytes),
    u32(req.wait_ms),
    putAccept(req.accept),
  ]);
}

// A page (fetch_range's answer, and the body of a records push): the batch stays encoded, for decodeBatch.
function readPage(r) {
  const page = {
    next: readPosition(r),
    skip: r.u32(),
    backlog: r.num64(),
    expired: r.num64(),
    expired_exact: r.u8() === 1,
  };
  page.batch = readBatch(r);
  return page;
}

function decodePage(payload) {
  const r = new Reader(payload);
  const page = readPage(r);
  expectEnd(r);
  return page;
}

function decodePush(payload) {
  const r = new Reader(payload);
  const kind = codeOf(PUSH_KINDS, r.u8());
  const streamId = r.u32();
  let push;
  if (kind === 'append_ack') {
    push = {
      stream_id: streamId,
      acked_sequence: r.num64(),
      window_appends: r.u32(),
      window_bytes: r.u32(),
      errors: r.list((x) => ({ sequence: x.num64(), reason: x.str() })),
    };
  } else if (kind === 'moved') {
    push = {
      stream_id: streamId,
      reason: r.str(),
      routes_version: r.num64(),
      targets: r.list((x) => ({ range: x.u32(), segment: readRouteSegment(x) })),
    };
  } else {
    push = { stream_id: streamId, ...readPage(r) };
  }
  expectEnd(r);
  return [kind, push];
}

function encodeJoinGroupReq(req) {
  return Buffer.concat([putStr(req.topic), putStr(req.group), putStr(req.member)]);
}

function encodeGroupHeartbeatReq(req) {
  return Buffer.concat([encodeJoinGroupReq(req), u64(req.generation)]);
}

// join_group and group_heartbeat both answer with the member's assignment.
function decodeAssignmentResp(payload) {
  const r = new Reader(payload);
  const resp = { generation: r.num64(), session_ms: r.u32(), ranges: r.list((x) => x.u32()) };
  expectEnd(r);
  return resp;
}

function encodeCommitOffsetsReq(req) {
  const positions = putList(req.positions, (p) => Buffer.concat([u32(p.range), putPosition(p.position)]));
  return Buffer.concat([encodeJoinGroupReq(req), u64(req.generation), positions]);
}

// ---- batches (see Malachi.Wire.Batch) ----

// The smallest whole zstd frame: magic number (4), frame header descriptor (1), window descriptor or
// content size (1), block header (3).
const MIN_ZSTD_FRAME = 9;

// A record behind its flags byte (bit 0: a tombstone).
function flagged(entry) {
  return Buffer.concat([u8(entry.tombstone ? 1 : 0), encodeRecord(entry.record)]);
}

// A batch of { record, tombstone } entries for an append, or of { position, record, tombstone } ones as a
// consume carries them (layout 'positioned').
function encodeBatch(entries, codec, layout = 'plain') {
  const encoded = entries.map((entry) =>
    layout === 'plain' ? flagged(entry) : Buffer.concat([putPosition(entry.position), flagged(entry)]),
  );
  const inflated = Buffer.concat(encoded);
  const payload = codec === 'zstd' ? zlib.zstdCompressSync(inflated) : inflated;
  return Buffer.concat([u8(codeIn(CODECS, codec)), u32(entries.length), u32(inflated.length), u32(payload.length), payload]);
}

function readBatch(r) {
  const start = r.pos;
  r.u8();
  r.u32();
  r.u32();
  const size = r.u32();
  r.bytes(size);
  return r.buf.subarray(start, r.pos);
}

// A zstd payload inflated no further than its declared size, naming its errors as Malachi.Wire.Batch does.
// zlib reads only the first frame and ignores any bytes after it, which the server refuses; the server
// always compresses a batch into one frame, so a client reading its answers is only more lenient.
function inflateZstd(payload, inflatedSize) {
  try {
    // zlib refuses a maxOutputLength of 0, which an empty batch declares; any output past the declaration
    // makes the check after this refuse the batch all the same.
    return zlib.zstdDecompressSync(payload, { maxOutputLength: Math.max(inflatedSize, 1) });
  } catch (err) {
    if (err.code === 'ERR_BUFFER_TOO_LARGE') throw new Error(`batch_size_mismatch: inflates past ${inflatedSize}`);
    throw new Error(`malformed_batch: ${err.message}`);
  }
}

// The records of a batch, inflated no further than `maxInflatedBytes`.
function decodeBatch(batch, { maxInflatedBytes, layout = 'plain' }) {
  const r = new Reader(batch);
  const codec = codeOf(CODECS, r.u8());
  const count = r.u32();
  const inflatedSize = r.u32();
  const payload = r.bytes(r.u32());
  expectEnd(r);
  if (inflatedSize > maxInflatedBytes) throw new Error(`batch_too_large: ${inflatedSize} > ${maxInflatedBytes}`);
  if (codec === 'zstd' && payload.length < MIN_ZSTD_FRAME) throw new Error(`malformed_batch: ${payload.length} bytes`);
  const inflated = codec === 'zstd' ? inflateZstd(payload, inflatedSize) : payload;
  if (inflated.length !== inflatedSize) throw new Error(`batch_size_mismatch: ${inflated.length} != ${inflatedSize}`);
  const records = new Reader(inflated);
  const entries = [];
  for (let i = 0; i < count; i++) {
    const position = layout === 'plain' ? null : readPosition(records);
    const flags = records.u8();
    // Only bit 0 (a tombstone) is defined; a reserved bit is refused, as the server does.
    if (flags > 1) throw new Error(`malformed_batch: reserved flags ${flags}`);
    const tombstone = flags === 1;
    const record = decodeRecord(records);
    entries.push(layout === 'plain' ? { record, tombstone } : { position, record, tombstone });
  }
  expectEnd(records);
  return entries;
}

module.exports = {
  API,
  OK,
  ERROR,
  MAX_FRAME_BYTES,
  encodeFrame,
  decodeFrame,
  encodeRequest,
  decodeResponse,
  encodeAuthReq,
  decodeString,
  encodeCreateTopicReq,
  encodeProduceReq,
  decodeFetchResp,
  encodeFetchReq,
  encodeLeaveGroupReq,
  encodeCommitReq,
  encodeSubscribeReq,
  encodeStreamAckReq,
  encodeCreateUserReq,
  encodeDeleteUserReq,
  encodeChangePasswordReq,
  decodeListUsersResp,
  encodeSetRoleReq,
  decodeListUsersWithRolesResp,
  encodeAclReq,
  encodeListAclsReq,
  decodeListAclsResp,
  POLICY_FIELDS,
  encodeDefinePolicyReq,
  encodeDeletePolicyReq,
  encodeListPoliciesReq,
  decodeListPoliciesResp,
  encodeBindTopicPolicyReq,
  encodeGetTopicPolicyReq,
  decodeTopicPolicyResp,
  encodeClusterStateReq,
  decodeClusterStateResp,
  encodeTopicRoutesReq,
  decodeTopicRoutesResp,
  encodeOpenStreamReq,
  decodeOpenStreamResp,
  encodeAppendReq,
  encodeCloseStreamReq,
  encodeOpenConsumeReq,
  decodeOpenConsumeResp,
  encodeConsumeAckReq,
  encodeFetchRangeReq,
  decodePage,
  decodePush,
  encodeJoinGroupReq,
  encodeGroupHeartbeatReq,
  decodeAssignmentResp,
  encodeCommitOffsetsReq,
  encodeBatch,
  decodeBatch,
};

// Self-test: `node scripts/lib/wire.js`. No server needed. Guards the frame-length cap against
// regression, the same way loadtest.js self-tests its histogram.
if (require.main === module) {
  const assert = require('assert');

  // A declared length past the cap throws before any buffering, so the client cannot be driven to OOM by
  // a server-controlled length. The buffer here is tiny; the point is the length field, not the payload.
  const oversized = Buffer.alloc(4);
  oversized.writeUInt32BE(MAX_FRAME_BYTES + 1, 0);
  assert.throws(() => decodeFrame(oversized), /frame_too_large/, 'oversized frame must be rejected');

  // A length exactly at the cap, still incomplete, returns null (wait for more) rather than throwing.
  const atCap = Buffer.alloc(4);
  atCap.writeUInt32BE(MAX_FRAME_BYTES, 0);
  assert.strictEqual(decodeFrame(atCap), null, 'a frame at the cap is accepted (pending more bytes)');

  // A normal frame still round-trips.
  const body = Buffer.from('hello');
  const framed = encodeFrame(body);
  const decoded = decodeFrame(framed);
  assert.ok(decoded && decoded.body.equals(body), 'a normal frame decodes unchanged');

  console.log('wire.js self-test passed');
}
