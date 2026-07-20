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

// Peels one frame off a buffer: { body, rest } or null if the whole frame is not present yet.
function decodeFrame(buffer) {
  if (buffer.length < 4) return null;
  const len = buffer.readUInt32BE(0);
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
