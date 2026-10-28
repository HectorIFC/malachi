'use strict';

/**
 * A broker for the Node client's tests, in their own process: it answers `create_topic`, `cluster_state`,
 * `topic_routes`, `open_stream`, `append`, `open_consume`, `consume_ack`, `close_stream`, `fetch_range` and
 * the group keys (`join_group`, `group_heartbeat`, `commit_offsets`, `leave_group`) as the server does, by a
 * policy the test sets, and lets the test push what a server would (an `append_ack`, a `moved`,
 * an error frame under a stream's correlation id). What is appended is kept per range, in one source, and
 * read back by `fetch_range` and the consume streams. The Node twin of test/support/stream_stub.ex. Never
 * used outside tests.
 */

const net = require('net');
const wire = require('../wire');

const BROKER_STATUSES = ['alive', 'suspect', 'dead'];
const RANGE_STATES = ['active', 'sealed'];
const PUSH_KINDS = { append_ack: 0, moved: 1, records: 2 };

const u8 = (n) => Buffer.from([n]);
function u16(n) {
  const b = Buffer.alloc(2);
  b.writeUInt16BE(n);
  return b;
}
function u32(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n);
  return b;
}
function u64(n) {
  const b = Buffer.alloc(8);
  b.writeBigUInt64BE(BigInt(n));
  return b;
}
function str(s) {
  if (s === null || s === undefined) return u8(0);
  const bytes = Buffer.from(s, 'utf8');
  return Buffer.concat([u8(1), u32(bytes.length), bytes]);
}
const list = (items, put) => Buffer.concat([u16(items.length), ...items.map(put)]);
const routeSegment = (seg) => (seg === null ? u8(0) : Buffer.concat([u8(1), u32(seg.segment), str(seg.primary)]));

// A cursor over a request payload.
class Reader {
  constructor(buf) {
    this.buf = buf;
    this.pos = 0;
  }
  u8() {
    return this.buf.readUInt8(this.pos++);
  }
  u32() {
    const v = this.buf.readUInt32BE(this.pos);
    this.pos += 4;
    return v;
  }
  u16() {
    const v = this.buf.readUInt16BE(this.pos);
    this.pos += 2;
    return v;
  }
  u64() {
    const v = Number(this.buf.readBigUInt64BE(this.pos));
    this.pos += 8;
    return v;
  }
  str() {
    if (this.u8() === 0) return null;
    const len = this.u32();
    const s = this.buf.subarray(this.pos, this.pos + len).toString('utf8');
    this.pos += len;
    return s;
  }
  rest() {
    return this.buf.subarray(this.pos);
  }
}

function responseFrame(corr, code, payload) {
  return wire.encodeFrame(Buffer.concat([u32(corr), u16(code), payload]));
}

class FakeBroker {
  constructor() {
    this.sockets = [];
    this.streams = new Map(); // stream id -> { id, corr, socket, topic, range, expected, label }
    this.nextStreamId = 1;
    this.routes = new Map(); // topic -> { version, bits, ranges }
    this.opens = []; // every open asked: { topic, range, routes_version, window_appends, label }
    this.appends = []; // every append taken: { stream, sequence, values }
    this.unknownStream = 0; // appends answered unknown_stream: to a stream that moved, or one never opened
    this.log = new Map(); // `${topic}/${range}` -> the values appended there, in order
    this.consumes = []; // every consume opened: { id, topic, range, start }
    this.acks = []; // every consume_ack: { stream, position, window }
    // what an open_consume is answered: 'ok', { refuse: reason }, 'silent', { delayMs } (answered that many
    // ms later), or { dropAfterMs } (answered, its first page pushed, and the connection closed that many ms
    // later, once)
    this.consumePolicy = () => 'ok';
    // what an open is answered: 'ok', { refuse: reason }, 'silent', or 'moved' (answered, and told it moved
    // in the same write, as a seal right behind the open would)
    this.openPolicy = () => 'ok';
    // what an append is answered: 'ack' (an append_ack passing it), 'silent', or { refuse: reason }
    this.appendPolicy = () => 'ack';
    this.window = { appends: 64, bytes: 16777216 };
    this.extraBrokers = []; // further brokers the cluster state lists
    this.clusterStateAnswer = null; // a [code, payload] to answer cluster_state with instead
    this.authDelayMs = 0; // how long an authentication waits for its answer
    this.closes = []; // every close_stream: the stream id
    // the group every join and heartbeat is answered with the assignment of, and every group call asked:
    // { call: 'join' | 'heartbeat' | 'commit' | 'leave', member, generation, positions }
    this.group = { generation: 1, session_ms: 300, ranges: [0] };
    this.groupCalls = [];
    // what a group call is answered, by the call and the connection it came on: 'ok', { refuse: reason },
    // 'silent', 'drop' (its connection closed), or with { delayMs } either answer that many ms later
    this.groupPolicy = () => 'ok';
  }

  // Listens on `port`, or on any free one: a test that stops a broker starts it again on its port.
  async start(port = 0) {
    this.server = net.createServer((socket) => this._accept(socket));
    await new Promise((resolve) => this.server.listen(port, '127.0.0.1', resolve));
    this.port = this.server.address().port;
    this.address = { host: '127.0.0.1', port: this.port };
    this.id = `fake${this.port}@127.0.0.1`;
    return this;
  }

  stop() {
    for (const s of this.sockets) s.destroy();
    this.server.close();
  }

  // Closes every connection, as a broker that went down does.
  dropConnections() {
    for (const s of this.sockets) s.destroy();
    this.sockets = [];
  }

  // `topic`'s routes: `ranges` as [range, keyStart, keyEnd, primary] (primary an id, `self` for this broker,
  // or null), every one active, the version bumped on every change.
  setRoutes(topic, ranges, bits = 4) {
    const version = (this.routes.get(topic)?.version || 0) + 1;
    this.routes.set(topic, {
      version,
      bits,
      ranges: ranges.map(([range, keyStart, keyEnd, primary]) => ({
        range,
        keyStart,
        keyEnd,
        primary: primary === 'self' ? this.id : primary,
      })),
    });
  }

  // Pushes an `append_ack` to stream `id`: `acked` the sequence below which every append was answered.
  ack(id, acked, errors = []) {
    const s = this.streams.get(id);
    const payload = Buffer.concat([
      u8(PUSH_KINDS.append_ack),
      u32(id),
      u64(acked),
      u32(this.window.appends),
      u32(this.window.bytes),
      list(errors, (e) => Buffer.concat([u64(e.sequence), str(e.reason)])),
    ]);
    s.socket.write(responseFrame(s.corr, wire.OK, payload));
  }

  // Pushes a `moved` to stream `id`, which takes no more appends from then on.
  moved(id, reason = 'sealed') {
    const s = this.streams.get(id);
    s.moved = true;
    const routes = this.routes.get(s.topic);
    const payload = Buffer.concat([u8(PUSH_KINDS.moved), u32(id), str(reason), u64(routes ? routes.version : 0), list([], () => null)]);
    s.socket.write(responseFrame(s.corr, wire.OK, payload));
  }

  // Answers one frame of stream `id` with an error frame under its correlation id.
  refuse(id, reason) {
    const s = this.streams.get(id);
    s.socket.write(responseFrame(s.corr, wire.ERROR, str(reason)));
  }

  _accept(socket) {
    this.sockets.push(socket);
    let buffer = Buffer.alloc(0);
    socket.on('error', () => {});
    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      let frame;
      while ((frame = wire.decodeFrame(buffer)) !== null) {
        buffer = frame.rest;
        this._request(socket, frame.body.readUInt16BE(0), frame.body.readUInt32BE(2), frame.body.subarray(6));
      }
    });
  }

  _request(socket, apiKey, corr, payload) {
    const reply = (code, body) => socket.write(responseFrame(corr, code, body));
    reply.socket = socket;
    if (apiKey === wire.API.auth) {
      if (this.authDelayMs > 0) return setTimeout(() => reply(wire.OK, str('token')), this.authDelayMs);
      return reply(wire.OK, str('token'));
    }
    if (apiKey === wire.API.clusterState) {
      if (this.clusterStateAnswer) return reply(...this.clusterStateAnswer);
      const brokers = [{ id: this.id, host: '127.0.0.1', port: this.port, status: 'alive' }, ...this.extraBrokers];
      return reply(
        wire.OK,
        Buffer.concat([
          u64(1),
          u8(1),
          list(brokers, (b) => Buffer.concat([str(b.id), str(b.host), u16(b.port), u8(BROKER_STATUSES.indexOf(b.status))])),
          list([0], (v) => u32(v)),
        ]),
      );
    }
    if (apiKey === wire.API.topicRoutes) {
      const topic = new Reader(payload).str();
      const routes = this.routes.get(topic);
      if (!routes) return reply(wire.ERROR, str('no_such_topic'));
      return reply(
        wire.OK,
        Buffer.concat([
          str(topic),
          u64(routes.version),
          u8(routes.bits),
          list(routes.ranges, (r) =>
            Buffer.concat([
              u32(r.range),
              u64(r.keyStart),
              u64(r.keyEnd),
              u8(RANGE_STATES.indexOf('active')),
              routeSegment(r.primary === null ? null : { segment: 1, primary: r.primary }),
            ]),
          ),
        ]),
      );
    }
    if (apiKey === wire.API.createTopic) {
      const r = new Reader(payload);
      const topic = r.str();
      const bits = r.u8();
      if (this.routes.has(topic)) return reply(wire.ERROR, str('already_exists'));
      this.setRoutes(topic, [[0, 0, 2 ** bits, 'self']], bits);
      return reply(wire.OK, Buffer.alloc(0));
    }
    if (apiKey === wire.API.openStream) return this._open(socket, corr, payload, reply);
    if (apiKey === wire.API.openConsume) return this._openConsume(socket, corr, payload, reply);
    if (apiKey === wire.API.fetchRange) return this._fetchRange(payload, reply);
    if (apiKey === wire.API.consumeAck) {
      const r = new Reader(payload);
      const stream = r.u32();
      const position = [r.buf.readUInt16BE(r.pos), Number(r.buf.readBigUInt64BE(r.pos + 2))];
      r.pos += 10;
      this.acks.push({ stream, position, window: r.u32() });
      return;
    }
    if (apiKey === wire.API.append) return this._append(corr, payload, reply);
    if (apiKey === wire.API.closeStream) {
      this.closes.push(new Reader(payload).u32());
      return reply(wire.OK, Buffer.alloc(0));
    }
    const groupCall = GROUP_CALLS[apiKey];
    if (groupCall) return this._group(groupCall, payload, reply);
    return reply(wire.ERROR, str('unsupported'));
  }

  _open(socket, corr, payload, reply) {
    const r = new Reader(payload);
    const req = { topic: r.str(), range: r.u32(), routes_version: r.u64() };
    r.u8();
    req.window_appends = r.u32();
    r.u32();
    r.str();
    req.label = r.str();
    this.opens.push(req);
    const policy = this.openPolicy(req);
    if (policy === 'silent') return;
    if (policy.refuse) return reply(wire.ERROR, str(policy.refuse));
    const id = this.nextStreamId++;
    this.streams.set(id, { id, corr, socket, topic: req.topic, range: req.range, expected: 0, moved: false });
    const answer = responseFrame(
      corr,
      wire.OK,
      Buffer.concat([u32(id), u32(1), u32(Math.min(req.window_appends, this.window.appends)), u32(this.window.bytes), u64(req.routes_version)]),
    );
    if (policy !== 'moved') return socket.write(answer);
    // one write, so the client reads the answer and the move together
    const write = socket.write.bind(socket);
    let moved = null;
    socket.write = (frame) => (moved = frame);
    this.moved(id);
    socket.write = write;
    socket.write(Buffer.concat([answer, moved]));
  }

  _append(corr, payload, reply) {
    const r = new Reader(payload);
    const id = r.u32();
    const sequence = r.u64();
    const s = this.streams.get(id);
    if (!s || s.moved || s.corr !== corr) {
      this.unknownStream += 1;
      return reply(wire.ERROR, str('unknown_stream'));
    }
    const values = wire.decodeBatch(r.rest(), { maxInflatedBytes: 1 << 24 }).map((e) => e.record.value.toString('utf8'));
    this.appends.push({ stream: id, sequence, values });
    const logKey = `${s.topic}/${s.range}`;
    this.log.set(logKey, [...(this.log.get(logKey) || []), ...values]);
    const policy = this.appendPolicy({ stream: id, sequence, values });
    if (policy === 'silent') return;
    if (policy.refuse) return reply(wire.ERROR, str(policy.refuse));
    s.expected = Math.max(s.expected, sequence + 1);
    this.ack(id, s.expected);
  }
}

const GROUP_CALLS = {
  [wire.API.joinGroup]: 'join',
  [wire.API.groupHeartbeat]: 'heartbeat',
  [wire.API.commitOffsets]: 'commit',
  [wire.API.leaveGroup]: 'leave',
};

// A group call, recorded, and answered by `groupPolicy`: a join or a heartbeat with `group`'s assignment.
FakeBroker.prototype._group = function (call, payload, reply) {
  const r = new Reader(payload);
  const req = { call, topic: r.str(), group: r.str(), member: r.str() };
  if (call === 'heartbeat' || call === 'commit') req.generation = r.u64();
  if (call === 'commit') {
    const count = r.u16();
    req.positions = Array.from({ length: count }, () => ({ range: r.u32(), position: [r.u16(), r.u64()] }));
  }
  this.groupCalls.push(req);
  const policy = this.groupPolicy(req, reply.socket);
  if (policy === 'silent') return;
  if (policy === 'drop') return reply.socket.destroy();
  const answer = () => {
    if (policy.refuse) return reply(wire.ERROR, str(policy.refuse));
    if (call === 'commit' || call === 'leave') return reply(wire.OK, Buffer.alloc(0));
    const { generation, session_ms, ranges } = this.group;
    reply(wire.OK, Buffer.concat([u64(generation), u32(session_ms), list(ranges, (range) => u32(range))]));
  };
  if (policy.delayMs) return setTimeout(answer, policy.delayMs);
  answer();
};

// A start as Malachi.Wire encodes it: 0 earliest, 1 latest, 2 a position (source u16, offset u64), 3 a
// group's checkpoint (read here as earliest: this broker keeps no checkpoints).
function readStart(r) {
  const kind = r.u8();
  if (kind === 0) return 'earliest';
  if (kind === 1) return 'latest';
  if (kind === 3) return { committed: r.str() };
  const position = [r.buf.readUInt16BE(r.pos), Number(r.buf.readBigUInt64BE(r.pos + 2))];
  r.pos += 10;
  return { position };
}

// A page of `values` from `offset` on, in the one source this broker keeps.
function page(values, offset, max) {
  const taken = values.slice(offset, offset + max);
  const entries = taken.map((value, i) => ({ position: [0, offset + i], tombstone: false, record: { key: null, value, timestamp: 1 } }));
  const next = Buffer.concat([u16(0), u64(offset + taken.length)]);
  return Buffer.concat([next, u32(0), u64(values.length - offset - taken.length), u64(0), u8(0), wire.encodeBatch(entries, 'none', 'positioned')]);
}

const offsetOf = (start, values) => (start === 'latest' ? values.length : start.position ? start.position[1] : 0);

FakeBroker.prototype._openConsume = function (socket, corr, payload, reply) {
  const r = new Reader(payload);
  const topic = r.str();
  const range = r.u32();
  r.u64();
  const start = readStart(r);
  r.u32();
  const max = r.u32();
  const id = this.nextStreamId++;
  this.consumes.push({ id, topic, range, start });
  const policy = this.consumePolicy({ topic, range, start });
  if (policy === 'silent') return;
  if (policy.refuse) return reply(wire.ERROR, str(policy.refuse));
  const open = () => this._consumeOpened(socket, corr, { id, topic, range, start, max }, policy);
  // answered later, as a broker slow to open one is
  if (policy.delayMs !== undefined) return setTimeout(open, policy.delayMs);
  open();
};

FakeBroker.prototype._consumeOpened = function (socket, corr, { id, topic, range, start, max }, policy) {
  // kept as a stream, so `moved` and `refuse` reach it
  this.streams.set(id, { id, corr, socket, topic, range, expected: 0, moved: false, consume: true });
  const values = this.log.get(`${topic}/${range}`) || [];
  const offset = offsetOf(start, values);
  // the answer and its first page in one write, so the client reads them together, as it can from a server
  const answer = responseFrame(corr, wire.OK, Buffer.concat([u32(id), u16(0), u64(offset)]));
  const first = offset < values.length ? [responseFrame(corr, wire.OK, Buffer.concat([u8(PUSH_KINDS.records), u32(id), page(values, offset, max)]))] : [];
  socket.write(Buffer.concat([answer, ...first]));
  if (policy.dropAfterMs !== undefined) {
    this.consumePolicy = () => 'ok';
    setTimeout(() => socket.destroy(), policy.dropAfterMs);
  }
};

// Pushes a records page to consume stream `id`: the range's values from `offset`, at most `max`, the first
// `skip` of them ones the reader already had.
FakeBroker.prototype.pushRecords = function (id, offset, max = 1000, skip = 0) {
  const s = this.streams.get(id);
  const values = this.log.get(`${s.topic}/${s.range}`) || [];
  const body = page(values, offset, max);
  // the skip field sits after the next position (u16 + u64)
  body.writeUInt32BE(skip, 10);
  s.socket.write(responseFrame(s.corr, wire.OK, Buffer.concat([u8(PUSH_KINDS.records), u32(id), body])));
};

// Appends `values` to `topic`'s `range` directly, as records another producer wrote.
FakeBroker.prototype.write = function (topic, range, values) {
  const key = `${topic}/${range}`;
  this.log.set(key, [...(this.log.get(key) || []), ...values]);
};

FakeBroker.prototype._fetchRange = function (payload, reply) {
  const r = new Reader(payload);
  const topic = r.str();
  const range = r.u32();
  r.u64();
  const start = readStart(r);
  const max = r.u32();
  r.u32();
  const waitMs = r.u32();
  const values = this.log.get(`${topic}/${range}`) || [];
  this.fetches = [...(this.fetches || []), { topic, range, start, wait_ms: waitMs }];
  const policy = (this.fetchPolicy || (() => 'ok'))({ topic, range, start });
  if (policy.refuse) return reply(wire.ERROR, str(policy.refuse));
  if (policy === 'drop') return reply.socket.destroy();
  if (policy === 'silent') return;
  const body = page(values, Math.min(offsetOf(start, values), values.length), max);
  // a page whose first `skip` records the reader already had
  if (policy.skip) body.writeUInt32BE(policy.skip, 10);
  // a page naming another position next, as one read from an ancestor's history names the range's own records
  if (policy.next) {
    body.writeUInt16BE(policy.next[0], 0);
    body.writeBigUInt64BE(BigInt(policy.next[1]), 2);
  }
  if (policy.delayMs) return setTimeout(() => reply(wire.OK, body), policy.delayMs);
  // nothing to read: held for the wait asked (not woken by a write, which a test makes before it fetches)
  if (offsetOf(start, values) >= values.length && waitMs > 0) return setTimeout(() => reply(wire.OK, body), waitMs);
  reply(wire.OK, body);
};

module.exports = { FakeBroker };
