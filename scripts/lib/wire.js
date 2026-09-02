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
  encodeAclReq,
  encodeListAclsReq,
  decodeListAclsResp,
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
