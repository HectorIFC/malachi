'use strict';

/**
 * A consuming client as NorthGuard's (meetup transcript, 562 to 569), and as the Elixir load generator's
 * Malachi.Loadtest.Consumer is: one consume stream per range (keys 29 and 30), opened where the range is
 * read, pushing `records` pages with the position of every record, and a read ack after each page that
 * names the position read and returns the credit, so the broker keeps at most `window` records pushed and
 * unread. It reads every active range of a topic, from the start of each range's history.
 *
 * Its events are queued (`takeEvents`) and, with an `onEvent` option, handed to it as they come:
 *
 *   - `{ kind: 'records', key, count, next }`: a page of `count` records new to this consumer (a page's
 *     leading records it already had, `skip`, are not counted), `next` where the range's next page starts,
 *     and with the `decode` option `records`, those records whole (a load generator counts, a CLI prints);
 *   - `{ kind: 'failed', key, reason }`: an open, or a stream, the broker refused for a reason that is not a
 *     redirect; the range opens again after a backoff, or, with `onRefusal: 'stop'`, is not read again, nor
 *     is a range a split makes of it, or a merge back into exactly its block.
 *
 * When a range moves on, its stream is answered `moved` and closed, and the consumer opens the ranges the
 * routes now name, each from where its nearest retired ancestor was read, or from the start of its history
 * (./read_lanes.js), so a record is read at least once and never lost. Opens refused `moved`,
 * `stale_routes`, `not_owner` or `migrating` are redirects, not failures; routes that name a range's primary
 * with no address are a redirect too, read again only once a backoff ends. Connections, routes and their
 * backoffs go through ./links.js. Every change of state runs on one chain, in order, as ./producer.js does.
 */

const wire = require('./wire');
const { Links, isRedirect, backoffUntil, monoMs } = require('./links');
const readLanes = require('./read_lanes');

const MAX_BYTES = 1048576;
// How long an open may wait for its answer.
const OPEN_TIMEOUT_MS = 30000;

const addressKey = ({ host, port }) => `${host}:${port}`;

class Consumer {
  // A consumer routing by `router` (./router.js) and connecting with `connOpts` ({ user, pass, timeout }).
  // Options: `window`, the records a stream keeps pushed and unread at most (100); `max`, the records per
  // page at most (100); `onRefusal`, what a failed range does next: 'retry' (the default) opens it again
  // after a backoff, 'stop' leaves it unread from then on; `onEvent`, called with every event; `decode`,
  // whether a page's records come whole in its event; `startFrom`, where a range with no ancestor read
  // starts ('earliest', or a group member's `{ committed: group }`).
  constructor(router, connOpts, opts = {}) {
    const onRefusal = opts.onRefusal ?? 'retry';
    if (onRefusal !== 'retry' && onRefusal !== 'stop') throw new Error(`onRefusal must be 'retry' or 'stop', got ${onRefusal}`);
    for (const name of ['window', 'max']) {
      const n = opts[name] ?? 100;
      if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer, got ${n}`);
    }
    this.links = new Links(router, connOpts);
    this.router = router;
    this.window = opts.window ?? 100;
    this.max = opts.max ?? 100;
    this.onRefusal = onRefusal;
    this.onEvent = opts.onEvent || null;
    this.decode = opts.decode === true;
    this.startFrom = opts.startFrom ?? 'earliest';
    this.assigned = null; // the ranges a group member was handed, or null to read every one
    this.topics = [];
    this.lanes = new Map();
    this.retired = new Map(); // the lanes of ranges the routes retired, kept for where the ranges made of them open
    this.streams = new Set();
    this.opening = 0;
    this.stats = { moved: 0, dropped: 0 };
    this.events = [];
    this.droppedClients = new WeakSet();
    this.chain = Promise.resolve();
    this.timer = null;
    this.waiters = [];
    this.closed = false;
  }

  // Connects, authenticated, to the broker of every active range of `topic`, opening no stream: what a
  // reader does before it starts, so it pays no connect later and reads nothing early.
  prepare(topic) {
    return this._run(() => this.links.connectRanges(topic));
  }

  // Starts reading `topic`, waiting for nothing: an open goes out on every active range the routes name.
  start(topic) {
    return this._run(() => {
      if (!this.topics.includes(topic)) this.topics.push(topic);
      return this._flush(monoMs());
    });
  }

  // Starts reading `topic` and waits until a stream is open on every active range, within `timeoutMs`.
  // Resolves `{ ok: true }`, or `{ error }` with the refusal that kept a range closed, or 'open_timeout'.
  async subscribe(topic, timeoutMs) {
    const deadline = monoMs() + timeoutMs;
    let lastRefusal = null;
    const hear = (event) => event.kind === 'failed' && (lastRefusal = event.reason);
    this.listeners = new Set([...(this.listeners || []), hear]);
    try {
      await this.start(topic);
      for (;;) {
        const routed = await this._run(() => this.links.ensureRoutes(topic, monoMs()));
        if (routed.error) return routed;
        await this._run(() => this._flush(monoMs()));
        if (this._allOpen(topic)) return { ok: true };
        // a range that will not be read again does not open either
        const stopped = [...this.lanes].find(([key, lane]) => key.startsWith(`${topic}\u0000`) && lane.stream === 'stopped');
        // the refusal that stopped it, heard now or before this call
        if (stopped) return { error: lastRefusal || stopped[1].refusal || 'stopped' };
        if (monoMs() >= deadline) return { error: lastRefusal || 'open_timeout' };
        await this.wait(Math.max(0, deadline - monoMs()), (c) => c.opening === 0);
        const backoffs = [...this.lanes].filter(([key]) => key.startsWith(`${topic}\u0000`)).map(([, l]) => l.retryAt).filter((at) => typeof at === 'number');
        if (backoffs.length > 0) await sleep(Math.max(0, Math.min(Math.max(...backoffs), deadline) - monoMs()));
      }
    } finally {
      this.listeners.delete(hear);
    }
  }

  _allOpen(topic) {
    const routes = this.router.routes(topic);
    // the ranges it reads: a group member's assignment, or every one
    const read = routes === null ? [] : readLanes.assignedRanges(this.assigned, routes.ranges);
    return routes !== null && read.every((r) => this.lanes.get(readLanes.laneKey(topic, r.range))?.stream?.open);
  }

  // The events so far, emptied.
  takeEvents() {
    const events = this.events;
    this.events = [];
    return events;
  }

  // Resolves with 'idle' once `until(consumer)` holds, or with 'timeout' after `timeoutMs`.
  wait(timeoutMs, until) {
    return this.chain.then(
      () =>
        new Promise((resolve) => {
          if (until(this)) return resolve('idle');
          const waiter = { until, resolve, timer: null };
          waiter.timer = setTimeout(() => {
            this.waiters = this.waiters.filter((w) => w !== waiter);
            resolve('timeout');
          }, timeoutMs);
          this.waiters.push(waiter);
        }),
    );
  }

  // Reads only `ranges` from now on (a group member's assignment), or every range with null. At once, not
  // behind the frames already queued: a page of a range that left is dropped unread, so no record of a range
  // the group handed on is reported after the member heard of it. A range that leaves has its stream closed,
  // or closed when its open is answered, and forgets where it was read: handed back later, it starts from the
  // group's checkpoint again. Ranges handed over open on the flush this queues.
  setAssignment(ranges) {
    this.assigned = readLanes.assignmentOf(ranges);
    for (const [key, lane] of [...this.lanes]) {
      if (readLanes.assigned(this.assigned, key)) continue;
      if (lane.stream && typeof lane.stream === 'object' && lane.stream.open) this._closeStream(lane.stream.open);
      // a stream still opening sees its lane gone when it is answered, and closes then
      lane.stream = 'left';
      this.lanes.delete(key);
    }
    return this._run(() => this._flush(monoMs()));
  }

  // Closes `stream` at its broker and forgets it: nothing it pushes later is read.
  _closeStream(stream) {
    this.streams.delete(stream);
    stream.client.forgetStream(stream.correlationId);
    stream.client.closeStream(stream.id).catch(() => {});
  }

  // What routing did so far.
  statsNow() {
    return { ...this.stats, refreshes: this.links.stats.refreshes, redirects: this.links.stats.redirects, connect_failures: this.links.stats.connect_failures };
  }

  // Closes every connection; the brokers drop its streams with them.
  async close() {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    await this.links.closeAll();
  }

  // ---- the chain ----

  _run(fn) {
    const step = this.chain.then(fn);
    this.chain = step.then(
      () => this._settled(),
      () => this._settled(),
    );
    return step;
  }

  _settled() {
    const ready = this.waiters.filter((w) => w.until(this));
    this.waiters = this.waiters.filter((w) => !ready.includes(w));
    for (const w of ready) {
      clearTimeout(w.timer);
      w.resolve('idle');
    }
    this._schedule();
  }

  // A range to open again once its backoff ends, or the routes of a topic read once theirs does.
  nextTimeout(now) {
    const deadlines = [...this.lanes.values()].filter((l) => l.stream === null && typeof l.retryAt === 'number').map((l) => l.retryAt);
    for (const topic of this.topics) {
      const backoff = this.links.unroutable.get(topic);
      if (backoff) deadlines.push(backoff.until);
    }
    return deadlines.length === 0 ? null : Math.max(0, Math.min(...deadlines) - now);
  }

  _schedule() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.closed) return;
    const wait = this.nextTimeout(monoMs());
    if (wait === null) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this._run(() => this._flush(monoMs()));
    }, wait);
  }

  _emit(events) {
    for (const event of events) {
      this.events.push(event);
      if (this.onEvent) this.onEvent(event);
      for (const listener of this.listeners || []) listener(event);
    }
  }

  // ---- lanes: one per range read ----

  // Every active range of every topic read has a lane; a lane whose range the routes no longer hold, and
  // whose stream is gone, hands its position to the ranges made of it and goes.
  async _flush(now) {
    if (this.closed) return;
    for (const topic of this.topics) {
      const routed = await this.links.ensureRoutes(topic, now);
      if (routed.error) {
        // routes that cannot be read hold every range waiting to open until their backoff ends
        const { until } = this.links.unroutable.get(topic);
        for (const [key, lane] of this.lanes) if (key.startsWith(`${topic}\u0000`) && lane.stream === null) lane.retryAt = until;
        continue;
      }
      const ranges = readLanes.assignedRanges(this.assigned, this.router.routes(topic).ranges);
      const fresh = (block, start) => newLane(block, start === 'earliest' ? this.startFrom : start);
      // a lane whose stream is still open keeps reading its range until it is told it moved
      readLanes.reconcile(this.lanes, this.retired, topic, ranges, (lane) => lane.stream === null || lane.stream === 'stopped', fresh);
      for (const r of ranges) {
        const key = readLanes.laneKey(topic, r.range);
        const lane = this.lanes.get(key);
        // gone from the assignment while an earlier range opened
        if (!lane || lane.stream !== null) continue;
        if (typeof lane.retryAt === 'number' && lane.retryAt > now) continue;
        lane.retryAt = null;
        await this._open(topic, r.range, key, lane, now);
      }
    }
  }

  async _open(topic, range, key, lane, now) {
    const where = this.router.addressOf(topic, range);
    if (where.error === 'unadvertised') return this.links.unadvertised(topic, lane, now);
    if (where.error) {
      lane.retryAt = backoffUntil(now);
      this.router.forget(topic);
      return;
    }
    // null too once closed (./links.js connects to nothing after closeAll), so a flush still on its way
    // when the consumer closed opens nothing
    const client = await this.links.connection(where.address);
    // gone from the assignment while its broker was connected to
    if (lane.stream !== null) return;
    if (!client) {
      lane.retryAt = backoffUntil(now);
      this.router.forget(topic);
      return;
    }
    const opening = { opening: true, address: where.address, pending: true };
    lane.stream = opening;
    this.opening += 1;
    const stream = { key, client, address: where.address, correlationId: null, id: null, early: null };
    const handlers = {
      onPush: (kind, push) => this._run(() => this._push(stream, kind, push)),
      onRefusal: (reason) => this._run(() => this._refused(stream, reason)),
      onClosed: () => this._run(() => this._dropped(where.address, client, monoMs())),
    };
    const req = {
      topic,
      range,
      routes_version: this.router.version(topic),
      start: lane.start,
      window: this.window,
      max: this.max,
      max_bytes: MAX_BYTES,
      accept: ['none', 'zstd'],
    };
    client.openStream(wire.API.openConsume, wire.encodeOpenConsumeReq(req), handlers, OPEN_TIMEOUT_MS).then(
      ({ correlationId, payload }) => this._run(() => this._opened(lane, opening, stream, correlationId, payload)),
      (err) => this._run(() => this._openFailed(topic, key, lane, opening, client, err)),
    );
  }

  _settleOpening(opening) {
    if (!opening.pending) return;
    opening.pending = false;
    this.opening -= 1;
  }

  // ---- frames from a broker ----

  async _opened(lane, opening, stream, correlationId, payload) {
    this._settleOpening(opening);
    stream.correlationId = correlationId;
    stream.id = wire.decodeOpenConsumeResp(payload).stream_id;
    // a range that left the assignment while it opened: its stream is not read
    if (lane.stream !== opening) return this._closeStream(stream);
    lane.stream = { open: stream };
    lane.redirects = 0;
    this.streams.add(stream);
    // a page or a move that came in the same read as the answer, in order
    const early = stream.early || [];
    stream.early = null;
    for (const frame of early) await frame();
  }

  async _openFailed(topic, key, lane, opening, client, err) {
    this._settleOpening(opening);
    if (lane.stream !== opening) return;
    lane.stream = null;
    const now = monoMs();
    if (err.transport || err.timeout) {
      lane.retryAt = backoffUntil(now);
      this.router.forget(topic);
      return this._dropped(opening.address, client, now);
    }
    if (isRedirect(err.message)) {
      this.links.redirected(topic, lane, now);
      return this._flush(now);
    }
    this._failed(key, lane, err.message, now);
  }

  // A range that failed for a reason that is not a redirect: reported, and opened again after a backoff, or
  // with `onRefusal: 'stop'` never again.
  _failed(key, lane, reason, now) {
    if (this.onRefusal === 'stop') {
      lane.stream = 'stopped';
      lane.start = 'stopped';
      lane.refusal = reason;
    } else {
      lane.stream = null;
      lane.retryAt = backoffUntil(now);
    }
    this._emit([{ kind: 'failed', key, reason }]);
  }

  _early(stream, frame) {
    if (stream.id !== null) return false;
    (stream.early ||= []).push(frame);
    return true;
  }

  _push(stream, kind, push) {
    if (this._early(stream, () => this._push(stream, kind, push))) return;
    if (!this.streams.has(stream)) return;
    const lane = this.lanes.get(stream.key);
    if (kind === 'records') {
      // counted, its end acked at once (the credit it took comes back, the window as it is), and the range's
      // position moved past it
      const count = Math.max(0, wire.batchHeader(push.batch).count - push.skip);
      try {
        stream.client.sendOnStream(wire.API.consumeAck, stream.correlationId, wire.encodeConsumeAckReq({ stream_id: stream.id, position: push.next, window: 0 }));
      } catch (_gone) {
        // the connection is gone: its close reaches `_dropped`
      }
      lane.start = { position: push.next };
      const event = { kind: 'records', key: stream.key, count, next: push.next };
      if (this.decode) {
        const entries = wire.decodeBatch(push.batch, { maxInflatedBytes: 16777216, layout: 'positioned' });
        event.records = entries.slice(push.skip).map((e) => e.record);
      }
      this._emit([event]);
      return;
    }
    if (kind === 'moved') {
      // the range moved on: its stream is closed; the routes are read again, and the ranges they name opened
      this.streams.delete(stream);
      this.stats.moved += 1;
      this.router.forget(readLanes.splitKey(stream.key)[0]);
      lane.stream = null;
      return this._flush(monoMs());
    }
  }

  // An error frame under a stream's correlation id: the broker closed the stream with that refusal. The range
  // fails, keeping where it read.
  _refused(stream, reason) {
    if (this._early(stream, () => this._refused(stream, reason))) return;
    if (!this.streams.has(stream)) return;
    this.streams.delete(stream);
    this._failed(stream.key, this.lanes.get(stream.key), reason, monoMs());
  }

  // A connection dropped: its streams and opens are gone, and the ranges that read through it open again,
  // from where they read, once a backoff ends and the routes are read again.
  async _dropped(address, client, now) {
    if (this.droppedClients.has(client)) return;
    this.droppedClients.add(client);
    for (const s of [...this.streams]) if (addressKey(s.address) === addressKey(address)) this.streams.delete(s);
    const topics = new Set();
    for (const [key, lane] of this.lanes) {
      const on = lane.stream && typeof lane.stream === 'object' ? (lane.stream.open ? lane.stream.open.address : lane.stream.address) : null;
      if (on && addressKey(on) === addressKey(address)) {
        if (lane.stream.opening) this._settleOpening(lane.stream);
        lane.stream = null;
        lane.retryAt = backoffUntil(now);
        topics.add(readLanes.splitKey(key)[0]);
      }
    }
    await this.links.dropConn(address);
    this.stats.dropped += 1;
    for (const topic of topics) this.router.forget(topic);
  }
}

// A range a split made of a stopped one is stopped too: its start is the parent's, 'stopped'.
function newLane(block, start) {
  return { block, start, stream: start === 'stopped' ? 'stopped' : null, retryAt: null, redirects: 0 };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = { Consumer };
