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
 *
 * The stream keys (24 to 34, see "Streams, routing and groups" in lib/malachi/wire.ex) work the same way:
 * `openStream` sends an open (`open_stream` or `open_consume`), and once it is answered every later frame
 * under its correlation id belongs to the stream: a push (`append_ack`, `moved`, `records`) or an error
 * frame that refuses one frame the stream sent (`sendOnStream`). A routing client (./router.js,
 * ./links.js) keeps one of these per broker.
 */

const net = require('net');
const wire = require('./wire');

const DEFAULT_PORT = 4040;
const DEFAULT_HOST = 'localhost';
const DEFAULT_TIMEOUT = 5000;
// How long a connect may take, as the Elixir load generator bounds it (Malachi.Loadtest.Conn): a host that
// drops the handshake is a connect that failed, not one the operating system retries for a minute or two.
const DEFAULT_CONNECT_TIMEOUT = 15000;

class MalachiError extends Error {
  constructor(reason) {
    super(reason);
    this.name = 'MalachiError';
  }
}

// A failure of the connection itself (closed, reset, unreadable, or never there), as opposed to a refusal
// the server answered with. Flagged so a caller can tell a dropped connection from a server error without
// matching on messages: the load generator counts the first as a drop and reconnects, the second as an
// error under its reason.
function transportError(reason) {
  const err = new MalachiError(reason);
  err.transport = true;
  return err;
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

function isTransport(err) {
  return err instanceof MalachiError && err.transport === true;
}

class MalachiClient {
  constructor(options = {}) {
    this.host = options.host || process.env.MALACHI_HOST || DEFAULT_HOST;
    this.port = options.port || parseInt(process.env.MALACHI_PORT, 10) || DEFAULT_PORT;
    this.timeout = options.timeout || DEFAULT_TIMEOUT;
    this.connectTimeout = options.connectTimeout || DEFAULT_CONNECT_TIMEOUT;
    // how the socket is made: net.createConnection, or a test's stand-in
    this.createConnection = options.createConnection || net.createConnection;

    this.socket = null;
    this.token = null;
    this.buffer = Buffer.alloc(0);
    this.nextCorrelationId = 1;
    this.pending = new Map(); // correlationId -> { resolve, reject, timer }
    this.subscriptions = new Map(); // correlationId -> { onRecords, onError }
    this.streams = new Map(); // correlationId -> { onPush, onRefusal, onClosed }
    this.opening = new Set(); // correlation ids of streams whose open is not answered yet
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
      const socket = this.createConnection(this.port, this.host);
      const timer = setTimeout(() => {
        socket.destroy();
        reject(transportError(`connect timed out after ${this.connectTimeout}ms`));
      }, this.connectTimeout);
      socket.once('connect', () => {
        clearTimeout(timer);
        socket.on('data', (chunk) => this._onData(chunk));
        socket.on('close', () => this._onClose());
        // Swap the connect-phase reject for a persistent error handler: after connect, an ECONNRESET/EPIPE
        // on a socket with no 'error' listener is rethrown by Node as an uncaught exception, killing the CLI.
        socket.removeListener('error', reject);
        socket.on('error', (err) => this._onError(err));
        resolve();
      });
      socket.once('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
      this.socket = socket;
    });
  }

  // Clear the socket reference as well, so a request or subscribe issued after the close rejects as
  // 'not connected' instead of writing to a destroyed socket and waiting on an answer that never comes.
  _onClose() {
    this.socket = null;
    this._failAll(transportError('connection closed by server'));
  }

  // A post-connect socket error surfaces the real reason to in-flight work. The 'close' that always follows
  // then runs _onClose, which is a no-op because _failAll already drained the maps.
  _onError(err) {
    this._failAll(transportError(err.message));
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
    // a stream still opening learns of the loss from its open's rejection (the pending loop above), not here
    const streams = [...this.streams].filter(([id]) => !this.opening.has(id)).map(([, stream]) => stream);
    this.streams.clear();
    this.opening.clear();
    for (const stream of streams) {
      if (stream.onClosed) stream.onClosed(err);
    }
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
      // bytes held so far. Clear the socket reference first so a request issued after this rejects as
      // 'not connected' (the guard in `_request`) rather than writing to a destroyed socket; leaving
      // `closed` false keeps the client reconnectable. `destroy` fires 'close' -> _onClose -> _failAll,
      // which is idempotent.
      this.buffer = Buffer.alloc(0);
      const socket = this.socket;
      this.socket = null;
      if (socket) socket.destroy();
      this._failAll(transportError(err.message));
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
    if (!waiter) {
      this._streamFrame(correlationId, errorCode, payload);
      return;
    }
    this.pending.delete(correlationId);
    clearTimeout(waiter.timer);
    if (errorCode === wire.OK) {
      waiter.resolve(payload);
    } else {
      waiter.reject(new MalachiError(wire.decodeString(payload)));
    }
  }

  // A frame under the correlation id of an open stream, after its open was answered: a push, or an error
  // frame refusing one frame the stream sent. Any other is late or duplicate, and ignored.
  _streamFrame(correlationId, errorCode, payload) {
    const stream = this.streams.get(correlationId);
    if (!stream) return;
    if (errorCode === wire.OK) {
      const [kind, push] = wire.decodePush(payload);
      stream.onPush(kind, push);
    } else {
      stream.onRefusal(wire.decodeString(payload));
    }
  }

  // Sends one request and resolves with the ok payload (or rejects with the server's reason).
  _request(apiKey, payload, timeout = this.timeout) {
    if (!this.socket || this.closed) return Promise.reject(transportError('not connected'));
    return this._requestAs(this._nextId(), apiKey, payload, timeout);
  }

  _requestAs(correlationId, apiKey, payload, timeout) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(correlationId);
        const err = new MalachiError(`request timed out after ${timeout}ms`);
        err.timeout = true; // no answer, as opposed to a refusal: a routing client replaces the connection
        reject(err);
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
    if (!this.socket || this.closed) throw transportError('not connected');
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
    if (!this.socket || this.closed) throw transportError('not connected');
    const payload = wire.encodeStreamAckReq(topic, group, member, cursor, count);
    this.socket.write(wire.encodeRequest(wire.API.streamAck, this._nextId(), payload));
  }

  // ---- streams and routing (keys 24 to 34) ----

  // The minimal cluster state any node answers (`cluster_state`, key 24).
  async clusterState() {
    return wire.decodeClusterStateResp(await this._request(wire.API.clusterState, wire.encodeClusterStateReq()));
  }

  // `topic`'s routes (`topic_routes`, key 25): its version, keyspace bits and ranges with their primaries.
  async topicRoutes(topic) {
    return wire.decodeTopicRoutesResp(await this._request(wire.API.topicRoutes, wire.encodeTopicRoutesReq(topic)));
  }

  // One page of one range (`fetch_range`, key 31): `req` as encodeFetchRangeReq takes it. Resolves with the
  // decoded page (its batch still encoded), or rejects with the refusal.
  async fetchRange(req) {
    return wire.decodePage(await this._request(wire.API.fetchRange, wire.encodeFetchRangeReq(req)));
  }

  // Opens a stream (`open_stream` or `open_consume`, encoded by the caller) and resolves with
  // `{ correlationId, payload }`, the open's ok answer still encoded, or rejects with the refusal (a loss of
  // the connection before the answer included). From the answer on, the stream's frames reach `handlers`:
  // `onPush(kind, push)` for a push, `onRefusal(reason)` for an error frame refusing one frame the stream
  // sent, and `onClosed(err)` once if the connection drops. A deliberate `close()` calls none of them.
  // The handlers are held from the open on, so a push the server sends right behind the answer is not lost.
  async openStream(apiKey, payload, handlers, timeout = this.timeout) {
    if (!this.socket || this.closed) throw transportError('not connected');
    const correlationId = this._nextId();
    this.streams.set(correlationId, handlers);
    this.opening.add(correlationId);
    try {
      const answer = await this._requestAs(correlationId, apiKey, payload, timeout);
      this.opening.delete(correlationId);
      return { correlationId, payload: answer };
    } catch (err) {
      this.opening.delete(correlationId);
      this.streams.delete(correlationId);
      throw err;
    }
  }

  // Sends one frame on an open stream (an `append`, a `consume_ack`), under the stream's correlation id: its
  // answer, if any, comes to the stream's handlers.
  sendOnStream(apiKey, correlationId, payload) {
    if (!this.socket || this.closed) throw transportError('not connected');
    this.socket.write(wire.encodeRequest(apiKey, correlationId, payload));
  }

  // Stops routing frames to a stream's handlers (one the server ended, or the caller is done with).
  forgetStream(correlationId) {
    this.streams.delete(correlationId);
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

  // Resolves with `[{ username, permissions: [...], role }]`, `role` null when the user has no console
  // role: no password hashes ever leave the server.
  async listUsers() {
    const body = await this._request(wire.API.listUsersWithRoles, Buffer.alloc(0));
    return wire.decodeListUsersWithRolesResp(body);
  }

  // Sets a user's console role ('viewer' | 'editor' | 'admin'), or removes it with null.
  async setRole(username, role) {
    await this._request(wire.API.setRole, wire.encodeSetRoleReq(username, role));
    return true;
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

  // Storage policies. `fields` are [name, value] pairs: a value is null (the rule is off), a
  // non-negative integer or a string; a field left out inherits the cluster's global value.
  async definePolicy(name, fields) {
    await this._request(wire.API.definePolicy, wire.encodeDefinePolicyReq(name, fields));
    return true;
  }

  // Refused with "policy_in_use: <topics>" while a topic is bound to it, unless `force` is set.
  async deletePolicy(name, { force = false } = {}) {
    await this._request(wire.API.deletePolicy, wire.encodeDeletePolicyReq(name, force));
    return true;
  }

  // Resolves with `[[name, fields], ...]`, sorted by name.
  async listPolicies() {
    const body = await this._request(wire.API.listPolicies, wire.encodeListPoliciesReq());
    return wire.decodeListPoliciesResp(body);
  }

  // A null name detaches the topic. Refused with "no_such_policy" for a name nothing defines.
  async bindTopicPolicy(topic, name) {
    await this._request(wire.API.bindTopicPolicy, wire.encodeBindTopicPolicyReq(topic, name));
    return true;
  }

  // Resolves with `{ topic, policy, resolution, definition, effective: [[name, value, origin], ...] }`.
  async getTopicPolicy(topic) {
    const body = await this._request(wire.API.getTopicPolicy, wire.encodeGetTopicPolicyReq(topic));
    return wire.decodeTopicPolicyResp(body);
  }

  // Tears the connection down at once, where close() ends it gracefully: a connect or authentication still
  // in flight is abandoned, and a pending request rejects (the 'close' that follows runs _onClose) instead
  // of holding its timer, and with it the process, until it times out.
  destroy() {
    if (this.socket) this.socket.destroy();
  }

  // Ends the connection gracefully. A request still pending is rejected now, its timer cleared, so nothing
  // waits on an answer that cannot come and no timer keeps the process alive after its work is done.
  close() {
    const pending = [...this.pending.values()];
    this.pending.clear();
    this.closed = true;
    for (const { reject, timer } of pending) {
      clearTimeout(timer);
      reject(transportError('connection closed'));
    }
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.end();
      this.socket = null;
    }
  }
}

module.exports = { MalachiClient, MalachiError, isNotOwner, isMigrating, isTransport };
