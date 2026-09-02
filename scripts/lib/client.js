'use strict';

/**
 * A reference Malachi client over the binary wire protocol (see ./wire.js).
 *
 * One TCP connection multiplexes many in-flight requests by correlation id, so `produce`, `fetch`,
 * `commit` and friends can be issued concurrently. A subscription is different: after `subscribe`
 * resolves, the server keeps pushing records tagged with that same correlation id, so those frames are
 * routed to the subscription's callback instead of resolving a one-shot request.
 *
 * The log model in three calls:
 *   - produce(topic, records)  -> append by key; the server picks the range
 *   - fetch(topic, {cursor})   -> pull a batch + an opaque next cursor (position lives only in the cursor)
 *   - subscribe(topic, {...})  -> server-push stream, flow-controlled by a credit window + streamAck
 */

const net = require('net');
const wire = require('./wire');

const DEFAULT_PORT = 4040;
const DEFAULT_HOST = 'localhost';
const DEFAULT_TIMEOUT = 5000;

class MalachiError extends Error {
  constructor(reason) {
    super(reason);
    this.name = 'MalachiError';
  }
}

// The server answers :not_owner when a member request is routed to a coordinator that no longer leads the
// topic's vnode (a transient failover window). It is retryable: the server re-resolves the current leader
// on the next attempt, so the member CLIs back off and retry rather than failing.
function isNotOwner(err) {
  return err instanceof MalachiError && err.message === 'not_owner';
}

// The server answers :migrating on a metadata write (produce that rolls a segment, or a commit) to a topic
// whose vnode is being split away: the topic is fenced during the copy. It is transient (the fence lifts
// once the split completes and the ring moves), so the CLIs back off and retry against the new location.
function isMigrating(err) {
  return err instanceof MalachiError && err.message === 'migrating';
}

class MalachiClient {
  constructor(options = {}) {
    this.host = options.host || process.env.MALACHI_HOST || DEFAULT_HOST;
    this.port = options.port || parseInt(process.env.MALACHI_PORT, 10) || DEFAULT_PORT;
    this.timeout = options.timeout || DEFAULT_TIMEOUT;

    this.socket = null;
    this.token = null;
    this.buffer = Buffer.alloc(0);
    this.nextCorrelationId = 1;
    this.pending = new Map(); // correlationId -> { resolve, reject, timer }
    this.subscriptions = new Map(); // correlationId -> { onRecords, onError }
    this.closed = false;
  }

  // Opens the socket and authenticates; resolves once a token is held.
  async connect(username, password) {
    await this._openSocket();
    if (username !== undefined) await this.authenticate(username, password);
    return this;
  }

  _openSocket() {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(this.port, this.host);
      socket.once('connect', () => {
        socket.on('data', (chunk) => this._onData(chunk));
        socket.on('close', () => this._onClose());
        // Swap the connect-phase reject for a persistent error handler: after connect, an ECONNRESET/EPIPE
        // on a socket with no 'error' listener is rethrown by Node as an uncaught exception, killing the CLI.
        socket.removeListener('error', reject);
        socket.on('error', (err) => this._onError(err));
        resolve();
      });
      socket.once('error', reject);
      this.socket = socket;
    });
  }

  _onClose() {
    this._failAll(new MalachiError('connection closed by server'));
  }

  // A post-connect socket error surfaces the real reason to in-flight work. The 'close' that always follows
  // then runs _onClose, which is a no-op because _failAll already drained the maps.
  _onError(err) {
    this._failAll(new MalachiError(err.message));
  }

  // Fails every pending request and notifies every subscription, then clears both maps. Idempotent: a second
  // call (error followed by close) finds the maps empty. A deliberate close() sets `closed` and skips this.
  _failAll(err) {
    if (this.closed) return;
    for (const { reject, timer } of this.pending.values()) {
      clearTimeout(timer);
      reject(err);
    }
    this.pending.clear();
    for (const sub of this.subscriptions.values()) {
      if (sub.onError) sub.onError(err);
    }
    this.subscriptions.clear();
  }

  _onData(chunk) {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    let frame;
    try {
      while ((frame = wire.decodeFrame(this.buffer)) !== null) {
        this.buffer = frame.rest;
        this._dispatch(wire.decodeResponse(frame.body));
      }
    } catch (err) {
      // A frame past the length cap, or an otherwise malformed one, is unrecoverable mid-stream: the buffer
      // can no longer be resynced. Drop the connection instead of buffering a hostile server, and free the
      // bytes held so far. `destroy` fires 'close' -> _onClose -> _failAll, which is idempotent.
      this.buffer = Buffer.alloc(0);
      if (this.socket) this.socket.destroy();
      this._failAll(new MalachiError(err.message));
    }
  }

  _dispatch({ correlationId, errorCode, payload }) {
    const sub = this.subscriptions.get(correlationId);
    if (sub) {
      if (errorCode === wire.OK) {
        sub.onRecords(wire.decodeFetchResp(payload));
      } else {
        // subscribe was rejected (e.g. permission denied): the server never enters push mode for this
        // corr_id, so no more frames arrive: drop the subscription and surface the reason.
        this.subscriptions.delete(correlationId);
        if (sub.onError) sub.onError(new MalachiError(wire.decodeString(payload)));
      }
      return;
    }

    const waiter = this.pending.get(correlationId);
    if (!waiter) return; // late/duplicate frame for a settled request, ignore
    this.pending.delete(correlationId);
    clearTimeout(waiter.timer);
    if (errorCode === wire.OK) {
      waiter.resolve(payload);
    } else {
      waiter.reject(new MalachiError(wire.decodeString(payload)));
    }
  }

  // Sends one request and resolves with the ok payload (or rejects with the server's reason).
  _request(apiKey, payload, timeout = this.timeout) {
    if (!this.socket || this.closed) return Promise.reject(new MalachiError('not connected'));
    const correlationId = this._nextId();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new MalachiError(`request timed out after ${timeout}ms`));
      }, timeout);
      this.pending.set(correlationId, { resolve, reject, timer });
      this.socket.write(wire.encodeRequest(apiKey, correlationId, payload));
    });
  }

  _nextId() {
    // wrap within u32 so a long-lived connection never overflows the correlation-id field
    const id = this.nextCorrelationId;
    this.nextCorrelationId = id >= 0xffffffff ? 1 : id + 1;
    return id;
  }

  // ---- operations ----

  async authenticate(username, password) {
    const payload = await this._request(wire.API.auth, wire.encodeAuthReq(username, password));
    this.token = wire.decodeString(payload);
    return this.token;
  }

  async createTopic(topic, keyspaceBits = 16) {
    await this._request(wire.API.createTopic, wire.encodeCreateTopicReq(topic, keyspaceBits));
    return true;
  }

  // records: [{ key?, value, headers?, timestamp? }]. Resolves with the server-reported appended count.
  async produce(topic, records) {
    const body = await this._request(wire.API.produce, wire.encodeProduceReq(topic, records));
    return body.length >= 4 ? body.readUInt32BE(0) : records.length;
  }

  // Pulls one batch. cursor: null/undefined = start; group resumes a committed position when cursor is null.
  // waitMs > 0 long-polls server-side, so the request timeout is extended past it.
  // With `member` (a consumer-group member id), the server scopes the fetch to the member's assigned
  // ranges: the client still only sees records + an opaque cursor. Without it, whole-group / single fetch.
  async fetch(topic, { cursor = null, group = null, member = null, max = 100, waitMs = 0 } = {}) {
    const payload = wire.encodeFetchReq(topic, cursor, group, member, max, waitMs);
    const timeout = Math.max(this.timeout, waitMs + this.timeout);
    const body = await this._request(wire.API.fetch, payload, timeout);
    return wire.decodeFetchResp(body); // { records, cursor }
  }

  // Leaves a consumer group for a fast rebalance on a clean shutdown (else the server evicts on timeout).
  async leaveGroup(topic, group, member) {
    await this._request(wire.API.leaveGroup, wire.encodeLeaveGroupReq(topic, group, member));
    return true;
  }

  async commit(topic, group, cursor) {
    await this._request(wire.API.commit, wire.encodeCommitReq(topic, group, cursor));
    return true;
  }

  // Opens a server-push stream. onRecords({ records, cursor }) fires per push; the caller must streamAck
  // to release credit and durably commit. Resolves once the subscription is registered.
  // With `member` (a consumer-group member id) the server scopes the push stream to the member's ranges;
  // the client still sees only records + an opaque cursor. Without it, a whole-group subscription.
  async subscribe(topic, { group, member = null, window = 100, max = 100, onRecords, onError } = {}) {
    if (typeof onRecords !== 'function') throw new TypeError('subscribe requires an onRecords callback');
    if (!this.socket || this.closed) throw new MalachiError('not connected');
    const correlationId = this._nextId();
    this.subscriptions.set(correlationId, { onRecords, onError });
    const payload = wire.encodeSubscribeReq(topic, group, member, window, max);
    this.socket.write(wire.encodeRequest(wire.API.subscribe, correlationId, payload));
    return correlationId;
  }

  // Fire-and-forget: commits `group` at `cursor` and returns `count` records of window credit. With
  // `member` set it also heartbeats the coordinator and refreshes the member's ranges; an ack with a
  // null cursor and count 0 is a pure heartbeat that closes the idle-member liveness gap.
  streamAck(topic, group, member, cursor, count) {
    if (!this.socket || this.closed) throw new MalachiError('not connected');
    const payload = wire.encodeStreamAckReq(topic, group, member, cursor, count);
    this.socket.write(wire.encodeRequest(wire.API.streamAck, this._nextId(), payload));
  }

  // --- admin user management (require the admin permission; the server routes to the replicated store) ---

  // permissions: an array like ['produce', 'consume'] (allowed: 'admin', 'produce', 'consume').
  async createUser(username, password, permissions) {
    await this._request(wire.API.createUser, wire.encodeCreateUserReq(username, password, permissions));
    return true;
  }

  async deleteUser(username) {
    await this._request(wire.API.deleteUser, wire.encodeDeleteUserReq(username));
    return true;
  }

  async changePassword(username, newPassword) {
    await this._request(wire.API.changePassword, wire.encodeChangePasswordReq(username, newPassword));
    return true;
  }

  // Resolves with `[{ username, permissions: [...] }]`: no password hashes ever leave the server.
  async listUsers() {
    const body = await this._request(wire.API.listUsers, Buffer.alloc(0));
    return wire.decodeListUsersResp(body);
  }

  // per-topic ACL management. operation: 'produce' | 'consume'; pattern: a topic or a *-suffixed prefix.
  async grantAcl(username, operation, pattern) {
    await this._request(wire.API.grantAcl, wire.encodeAclReq(username, operation, pattern));
    return true;
  }

  async revokeAcl(username, operation, pattern) {
    await this._request(wire.API.revokeAcl, wire.encodeAclReq(username, operation, pattern));
    return true;
  }

  // Resolves with `[{ operation, resource }]` for the user.
  async listAcls(username) {
    const body = await this._request(wire.API.listAcls, wire.encodeListAclsReq(username));
    return wire.decodeListAclsResp(body);
  }

  close() {
    this.closed = true;
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.end();
      this.socket = null;
    }
  }
}

module.exports = { MalachiClient, MalachiError, isNotOwner, isMigrating };
