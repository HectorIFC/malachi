'use strict';

/**
 * A producing client as NorthGuard's (meetup transcript, 551 to 560 and 636 to 638), and as the Elixir load
 * generator's Malachi.Loadtest.Producer is: records go to the range their key hashes into, each range
 * through one producer stream (keys 26 to 28) opened on the broker that leads the range's active segment,
 * one connection per broker (./links.js), and every append sent within the window the broker grants and
 * answered by an `append_ack` push.
 *
 * Every record is handed over with a tag of the caller's (`{ tag, record }`), and comes back in an event:
 *
 *   - `{ kind: 'acked', entries, latencyUs }`: one append's records, acknowledged `latencyUs` after it was
 *     sent (time spent lingering is not in it);
 *   - `{ kind: 'failed', entries, reason }`: one append's records, refused with `reason` as the broker
 *     named it, or 'connection_lost' for an append in flight when its connection dropped, which may or may
 *     not have landed.
 *
 * Events are queued (`takeEvents`) and, with an `onEvent` option, handed to it as well. A record is never
 * retried here: the caller sees every outcome and decides. What is retried is routing. A stream told
 * `moved` takes no more appends; the appends it had in flight are still answered on it, and the records
 * not yet sent go to the stream the routes now name. An open refused `moved`, `stale_routes`, `not_owner`
 * or `migrating` reads the routes again and opens where they say, backing off once it has been redirected
 * 5 times in a row; routes that name a range's primary with no address are a redirect too, read again
 * after a backoff, and the range's records wait in its buffer meanwhile rather than fail.
 *
 * Lingering, per range: each `produce` call's share of a range waits in the range's buffer as one unit,
 * until the buffer holds `lingerBytes` (counted by `recordBytes`) or its oldest unit has waited `lingerMs`;
 * then as many whole units as fit in `lingerBytes` go out as one append, never splitting one (a unit larger
 * than `lingerBytes` goes alone). `lingerMs: 0` sends as soon as the window has room. `room()` is false
 * while some buffer holds `lingerBytes`, which is how a closed loop is held back by the window rather than
 * buffering without bound.
 *
 * Every change of state runs on one chain (`_run`), in order, as the Elixir producer's do in its process:
 * a hand-over, a broker's answer, a timer.
 */

const wire = require('./wire');
const { Links, isRedirect, backoffUntil, monoMs } = require('./links');

// The inflated bytes a stream asks the broker for; the broker caps it (MALACHI_STREAM_MAX_WINDOW_BYTES).
const WINDOW_BYTES = 16777216;
// How long an open may wait for its answer: opening a segment preallocates it, which can take seconds.
const OPEN_TIMEOUT_MS = 30000;
// The errors with which a broker ends a stream at the sequence the producer broke it at.
const SEQUENCE_BREAKS = new Set(['sequence_gap', 'sequence_repeat']);

const monoUs = () => Number(process.hrtime.bigint() / 1000n);
const laneKey = (topic, range) => `${topic}\u0000${range}`;
const addressKey = ({ host, port }) => `${host}:${port}`;

// The bytes a record with a `keyBytes` long key and no headers takes in a batch, as the broker counts an
// append against its window: its value and key, and 22 bytes of framing. What a buffer holds is counted
// the same way, so lingering and the window agree; a caller sizes `lingerBytes` with it.
const recordBytes = (valueBytes, keyBytes) => valueBytes + keyBytes + 22;

const strBytes = (s) => (s === null || s === undefined ? 1 : 5 + Buffer.byteLength(String(s), 'utf8'));
const valueBytes = (v) => (Buffer.isBuffer(v) ? v.length : Buffer.byteLength(String(v), 'utf8'));

// Exactly what the record takes in a plain batch: a tombstone flag (1), the key as a present-string, the
// value's length (4) and bytes, the timestamp (8), the header count (4) and each header's key and value.
function entryBytes({ record }) {
  const headers = Object.entries(record.headers || {});
  const headerBytes = headers.reduce((acc, [k, v]) => acc + strBytes(k) + strBytes(v), 0);
  return 1 + strBytes(record.key) + 4 + valueBytes(record.value) + 8 + 4 + headerBytes;
}

const entriesBytes = (entries) => entries.reduce((acc, e) => acc + entryBytes(e), 0);

function newLane(topic, range) {
  // `retryAt` is null, or the monotonic instant a backoff ends
  return { topic, range, buffer: [], bufferBytes: 0, since: 0, stream: null, retryAt: null, redirects: 0 };
}

// The oldest hand-overs, as many whole ones as fit in `limit` bytes (at least one, however large).
function takeChunk(buffer, limit) {
  let n = 0;
  let bytes = 0;
  while (n < buffer.length && (n === 0 || bytes + buffer[n].bytes <= limit)) {
    bytes += buffer[n].bytes;
    n += 1;
  }
  return { units: n, entries: buffer.slice(0, n).flatMap((u) => u.entries), bytes };
}

class Producer {
  // A producer routing by `router` (./router.js) and connecting with `connOpts` ({ user, pass, timeout }).
  // Options: `maxInflight`, the appends a stream keeps in flight at most, further capped by the window the
  // broker grants (1); `lingerMs` (0) and `lingerBytes` (1); `label`, how the operator interfaces name this
  // client; `onEvent`, called with every event as it comes.
  constructor(router, connOpts, opts = {}) {
    this.links = new Links(router, connOpts);
    this.router = router;
    this.maxInflight = positive(opts.maxInflight ?? 1, 'maxInflight');
    this.lingerMs = nonNegative(opts.lingerMs ?? 0, 'lingerMs');
    this.lingerBytes = positive(opts.lingerBytes ?? 1, 'lingerBytes');
    this.label = opts.label || 'malachi-loadtest';
    this.onEvent = opts.onEvent || null;
    this.stopped = false;
    this.lanes = new Map(); // lane key -> lane
    this.streams = new Set(); // every stream still answering: open, or retired with appends in flight
    this.opening = 0; // opens sent and not answered
    this.stats = { moved: 0, dropped: 0 };
    this.droppedClients = new WeakSet(); // connections already counted as dropped: one drop, however many streams
    this.events = [];
    this.listeners = new Set(); // called with every event too: `prepare` hears its own round's
    this.chain = Promise.resolve();
    this.timer = null;
    this.waiters = [];
  }

  // ---- the caller's side ----

  // Hands `entries` ({ tag, record }) to their ranges' buffers and sends what can go now. Resolves once they
  // are buffered or sent (or failed: records that cannot be routed, or appends a send found the connection
  // gone under).
  produce(topic, entries) {
    return this._run(async () => {
      if (entries.length > 0) await this._route(topic, entries, monoMs());
      await this._flush(monoMs());
    });
  }

  // The events so far, emptied.
  takeEvents() {
    const events = this.events;
    this.events = [];
    return events;
  }

  // Whether the caller may hand over more records: false while some range's buffer is full.
  room() {
    for (const lane of this.lanes.values()) if (lane.bufferBytes >= this.lingerBytes) return false;
    return true;
  }

  // Whether every record handed over has had its outcome: nothing buffered, opening or in flight.
  idle() {
    if (this.opening > 0) return false;
    for (const lane of this.lanes.values()) if (lane.bufferBytes > 0) return false;
    return !this.inFlight();
  }

  // Whether an append is still waiting for its answer.
  inFlight() {
    for (const stream of this.streams) if (stream.inflight.size > 0) return true;
    return false;
  }

  // Resolves with 'idle' once `until(producer)` holds (`idle` unless given; `room` for a closed loop), or
  // with 'timeout' after `timeoutMs`.
  wait(timeoutMs, until = (p) => p.idle()) {
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

  // Opens a stream on every active range of `topic` before any record is handed over, within `timeoutMs`:
  // a client that opens up front pays none of it later, and a range never written gets its segment placed.
  // A range whose open is redirected is opened again where the routes then say. Resolves `{ ok: true }`, or
  // `{ error }` with the refusal that kept a range closed, or 'open_timeout'.
  async prepare(topic, timeoutMs) {
    const deadline = monoMs() + timeoutMs;
    let lastRefusal = null;
    for (;;) {
      const now = monoMs();
      const routed = await this._run(() => this.links.ensureRoutes(topic, now));
      if (routed.error) return routed;
      const closed = this.router
        .routes(topic)
        .ranges.map((r) => laneKey(topic, r.range))
        .filter((key) => !(this.lanes.get(key)?.stream?.open));
      if (closed.length === 0) return { ok: true };
      if (now >= deadline) return { error: lastRefusal || 'open_timeout' };

      // the refusals of this round only, as the Elixir producer reads them: not an event queued before
      // the call, nor one an earlier round already weighed
      const refusals = [];
      const hear = (event) => refusals.push(event);
      this.listeners.add(hear);
      await this._run(async () => {
        for (const key of closed) {
          const [t, range] = splitKey(key);
          const lane = this.lanes.get(key) || newLane(t, range);
          if (lane.stream === null) {
            lane.retryAt = null;
            this.lanes.set(key, lane);
            refusals.push(...(await this._open(lane, monoMs())));
          }
        }
      });
      await this.wait(Math.max(0, deadline - monoMs()), (p) => p.opening === 0);
      this.listeners.delete(hear);
      const failed = refusals.find((e) => e.kind === 'failed');
      if (failed) lastRefusal = failed.reason;
      const backoffs = closed.map((key) => this.lanes.get(key)?.retryAt).filter((at) => typeof at === 'number');
      if (backoffs.length > 0) await sleep(Math.max(0, Math.min(Math.max(...backoffs), deadline) - monoMs()));
    }
  }

  // Drops every record not yet sent (buffered, or waiting on an open), with no event for them, and sends
  // nothing from now on: for a caller done handing records over that wants only the outcomes of the appends
  // already in flight. An append answered `unknown_stream` after this never reached the broker and is
  // dropped the same way, rather than sent again.
  stopSending() {
    return this._run(() => {
      for (const lane of this.lanes.values()) {
        lane.buffer = [];
        lane.bufferBytes = 0;
      }
      this.stopped = true;
    });
  }

  // What routing did so far.
  statsNow() {
    return { ...this.stats, refreshes: this.links.stats.refreshes, redirects: this.links.stats.redirects, connect_failures: this.links.stats.connect_failures };
  }

  // Closes every connection; the brokers drop its streams with them.
  async close() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.stopped = true;
    await this.links.closeAll();
  }

  // ---- the chain ----

  // Runs `fn` after everything queued before it, then wakes the waiters and sets the timer for what waits
  // on time. Resolves with what `fn` resolved with.
  _run(fn) {
    const step = this.chain.then(fn);
    this.chain = step.then(
      () => this._settled(),
      (err) => {
        this._settled();
        throw err;
      },
    );
    this.chain = this.chain.catch(() => {});
    return step;
  }

  _settled() {
    this._wake();
    this._schedule();
  }

  _emit(events) {
    for (const event of events) {
      this.events.push(event);
      if (this.onEvent) this.onEvent(event);
      for (const listener of this.listeners) listener(event);
    }
  }

  _wake() {
    const ready = this.waiters.filter((w) => w.until(this));
    this.waiters = this.waiters.filter((w) => !ready.includes(w));
    for (const w of ready) {
      clearTimeout(w.timer);
      w.resolve('idle');
    }
  }

  // Milliseconds from `now` until a flush has something to send: the oldest lingering record's deadline,
  // or the end of a range's backoff; null when nothing waits on time.
  nextTimeout(now) {
    let earliest = null;
    for (const lane of this.lanes.values()) {
      const deadline = this._laneDeadline(lane);
      if (deadline !== null && (earliest === null || deadline < earliest)) earliest = deadline;
    }
    return earliest === null ? null : Math.max(0, earliest - now);
  }

  _laneDeadline(lane) {
    if (lane.bufferBytes === 0) return null;
    if (typeof lane.retryAt === 'number') return lane.retryAt;
    // lingering only holds a lane whose window has room: one the window holds waits for an ack, not a clock
    if (this.lingerMs > 0 && lane.stream && lane.stream.open) {
      const stream = lane.stream.open;
      const { bytes } = takeChunk(lane.buffer, this.lingerBytes);
      if (this._windowRoom(stream) && !overWindow(stream, bytes)) return lane.since + this.lingerMs;
    }
    return null;
  }

  _schedule() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.stopped) return;
    const wait = this.nextTimeout(monoMs());
    if (wait === null) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this._run(() => this._flush(monoMs()));
    }, wait);
  }

  // ---- routing records to ranges ----

  async _route(topic, entries, now) {
    const routed = await this.links.ensureRoutes(topic, now);
    if (routed.error) return this._emit([{ kind: 'failed', entries, reason: routed.error }]);
    await this._routeKnown(topic, entries, now, true, 'back');
  }

  // Each entry into the buffer of the range its key hashes into. Routes caught mid-split may have no range
  // for a position: they are read again once, and an entry still without a range then fails.
  async _routeKnown(topic, entries, now, retry, where) {
    const routed = new Map();
    const unrouted = [];
    for (const entry of entries) {
      const { range, error } = this.router.rangeOf(topic, entry.record.key ?? null);
      if (error) unrouted.push(entry);
      else routed.set(range, [...(routed.get(range) || []), entry]);
    }
    for (const [range, ranged] of routed) this._buffer(topic, range, ranged, now, where);
    if (unrouted.length === 0) return;
    if (!retry) return this._emit([{ kind: 'failed', entries: unrouted, reason: 'no_range' }]);
    const read = await this.links.refresh(topic);
    if (read.error) return this._emit([{ kind: 'failed', entries: unrouted, reason: read.error }]);
    await this._routeKnown(topic, unrouted, now, false, where);
  }

  // A range's buffer holds hand-overs whole: each `produce` call's share of the range is one unit, so an
  // append never splits one.
  _buffer(topic, range, entries, now, where) {
    const key = laneKey(topic, range);
    const lane = this.lanes.get(key) || newLane(topic, range);
    const unit = { entries, bytes: entriesBytes(entries) };
    if (lane.bufferBytes === 0) lane.since = now;
    if (where === 'front') lane.buffer.unshift(unit);
    else lane.buffer.push(unit);
    lane.bufferBytes += unit.bytes;
    this.lanes.set(key, lane);
  }

  // ---- sending ----

  // A range the routes no longer hold goes first: its records are rerouted in front of what the ranges made
  // of it hold before any of them sends.
  async _flush(now) {
    if (this.stopped) return;
    const keys = [...this.lanes.keys()].sort((a, b) => this._retired(b) - this._retired(a));
    for (const key of keys) {
      // a lane retired while the flush ran is skipped: its records went to the lanes the routes name now
      const lane = this.lanes.get(key);
      if (lane) await this._flushLane(lane, now);
    }
  }

  async _flushLane(lane, now) {
    if (lane.bufferBytes === 0) return;
    // a range a split or merge retired opens nothing: its records go where the routes send them now
    if (lane.stream === null && this._retired(laneKey(lane.topic, lane.range))) return this._reroute(lane, now);
    // nothing of the topic goes out while it changes shape; it resumes on a message, not a clock
    if (this._held(lane.topic)) {
      lane.retryAt = null;
      return;
    }
    if (typeof lane.retryAt === 'number' && lane.retryAt > now) return;
    if (lane.stream === null) {
      lane.retryAt = null;
      this._emit(await this._open(lane, now));
      return;
    }
    if (lane.stream.opening) return;
    this._sendAppends(lane, now);
  }

  // Opens `lane`'s stream; resolves with the events it caused at once (none, unless its records failed).
  async _open(lane, now) {
    const { topic, range } = lane;
    const where = this.router.addressOf(topic, range);
    if (where.error === 'unknown_topic') {
      const routed = await this.links.ensureRoutes(topic, now);
      return routed.error ? this._failBuffer(lane, routed.error, now) : this._open(lane, now);
    }
    // the range was retired (split or merged) since its records were buffered
    if (where.error === 'unknown_range') {
      await this._reroute(lane, now);
      return [];
    }
    // routes that name no address for the primary are behind the cluster's, as a redirect says
    if (where.error === 'unadvertised') {
      this.links.unadvertised(topic, lane, now);
      return [];
    }
    return this._openAt(lane, where.address, now);
  }

  async _openAt(lane, address, now) {
    const client = await this.links.connection(address);
    if (!client) {
      // a broker that does not take connections may be gone: the routes are read again before the next open
      lane.retryAt = backoffUntil(now);
      this.links.router.forget(lane.topic);
      return [];
    }
    const opening = { opening: true, address, pending: true };
    lane.stream = opening;
    this.opening += 1;
    const stream = { lane: laneKey(lane.topic, lane.range), client, address, correlationId: null, id: null };
    const req = {
      topic: lane.topic,
      range: lane.range,
      routes_version: this.router.version(lane.topic),
      codec: 'none',
      window_appends: this.maxInflight,
      window_bytes: WINDOW_BYTES,
      producer_id: null,
      label: this.label,
    };
    const handlers = {
      onPush: (kind, push) => this._run(() => this._push(stream, kind, push)),
      onRefusal: (reason) => this._run(() => this._refused(stream, reason)),
      onClosed: () => this._run(() => this._dropped(address, client, monoMs())),
    };
    // an open the connection cannot take rejects like one it lost: `_openFailed` backs the range off
    client.openStream(wire.API.openStream, wire.encodeOpenStreamReq(req), handlers, OPEN_TIMEOUT_MS).then(
      ({ correlationId, payload }) => this._run(() => this._opened(lane, opening, stream, correlationId, payload)),
      (err) => this._run(() => this._openFailed(lane, opening, client, err)),
    );
    return [];
  }

  _sendAppends(lane, now) {
    const stream = lane.stream.open;
    while (lane.bufferBytes > 0 && this._windowRoom(stream) && this._due(lane, now)) {
      const chunk = takeChunk(lane.buffer, this.lingerBytes);
      // the estimate first, so a lane the byte window holds does not encode a batch only to drop it
      if (overWindow(stream, chunk.bytes)) return;
      const batch = wire.encodeBatch(chunk.entries.map((e) => ({ tombstone: false, record: e.record })), 'none');
      const size = wire.batchHeader(batch).inflated_size;
      if (overWindow(stream, size)) return;
      lane.buffer = lane.buffer.slice(chunk.units);
      lane.bufferBytes -= chunk.bytes;
      const seq = stream.nextSeq;
      try {
        stream.client.sendOnStream(wire.API.append, stream.correlationId, wire.encodeAppendReq(stream.id, seq, batch));
      } catch (_gone) {
        // the chunk never left: back to the front of the buffer, for the stream opened after the drop
        this._unsend(lane, chunk.entries);
        return this._run(() => this._dropped(stream.address, stream.client, now));
      }
      stream.nextSeq = seq + 1;
      stream.inflight.set(seq, { entries: chunk.entries, size, sentAt: monoUs() });
      stream.inflightBytes += size;
      if (lane.bufferBytes > 0) lane.since = now;
    }
  }

  // Room for one more append in flight: under the window the broker granted, and under the pipeline.
  _windowRoom(stream) {
    return stream.inflight.size < Math.min(stream.windowAppends, this.maxInflight);
  }

  _due(lane, now) {
    return this.lingerMs === 0 || lane.bufferBytes >= this.lingerBytes || now - lane.since >= this.lingerMs;
  }

  _failBuffer(lane, reason, now) {
    const entries = lane.buffer.flatMap((u) => u.entries);
    lane.buffer = [];
    lane.bufferBytes = 0;
    lane.stream = null;
    lane.retryAt = backoffUntil(now);
    return [{ kind: 'failed', entries, reason }];
  }

  async _reroute(lane, now) {
    const entries = lane.buffer.flatMap((u) => u.entries);
    this.lanes.delete(laneKey(lane.topic, lane.range));
    // the records are older than anything the ranges they go to hold: in front of it
    const routed = await this.links.ensureRoutes(lane.topic, now);
    if (routed.error) return this._emit([{ kind: 'failed', entries, reason: routed.error }]);
    await this._routeKnown(lane.topic, entries, now, true, 'front');
    await this._flush(now);
  }

  // ---- answers from a broker ----

  // An open answered or failed: it no longer counts as opening, once, whichever of its answer and a drop of
  // its connection came first.
  _settleOpening(opening) {
    if (!opening.pending) return;
    opening.pending = false;
    this.opening -= 1;
  }

  async _opened(lane, opening, stream, correlationId, payload) {
    this._settleOpening(opening);
    const resp = wire.decodeOpenStreamResp(payload);
    Object.assign(stream, {
      correlationId,
      id: resp.stream_id,
      nextSeq: 0,
      inflight: new Map(),
      inflightBytes: 0,
      windowAppends: resp.window_appends,
      windowBytes: resp.window_bytes,
      open: true,
      // appends answered `unknown_stream`, not yet told apart from the ones still to be acknowledged
      rejected: 0,
    });
    // the lane moved on meanwhile (a drop, a stop): the stream is still the broker's, so it is kept to answer
    // nothing and closed with its connection
    if (lane.stream !== opening) return;
    lane.stream = { open: stream };
    lane.redirects = 0;
    this.streams.add(stream);
    // each in turn, its own flush included, so no two flushes run at once
    const early = stream.early || [];
    stream.early = null;
    for (const frame of early) await frame();
    return this._flush(monoMs());
  }

  async _openFailed(lane, opening, client, err) {
    this._settleOpening(opening);
    if (lane.stream !== opening) return;
    lane.stream = null;
    const now = monoMs();
    // the connection is gone (or the open went unanswered): the range waits out a backoff and reads its
    // routes again
    if (err.transport || err.timeout) {
      lane.retryAt = backoffUntil(now);
      this.links.router.forget(lane.topic);
      return this._dropped(opening.address, client, now);
    }
    if (isRedirect(err.message)) {
      this.links.redirected(lane.topic, lane, now);
      return this._flush(now);
    }
    this._emit(this._failBuffer(lane, err.message, now));
  }

  // A push or refusal that came in the same read as its open's answer reaches the chain before the answer
  // does (the answer resolves a promise, the push is called at once): it is held for `_opened`, which
  // takes it right after the answer, in order, as the Elixir producer takes the frames of one read.
  _early(stream, frame) {
    if (stream.id !== null) return false;
    (stream.early ||= []).push(frame);
    return true;
  }

  _push(stream, kind, push) {
    if (this._early(stream, () => this._push(stream, kind, push))) return;
    if (!this.streams.has(stream)) return;
    if (kind === 'append_ack') return this._acked(stream, push);
    if (kind === 'moved') {
      // the range moved on: no more appends on this stream, and the records still buffered go where the
      // routes read next say; its appends in flight are still answered on it
      this.stats.moved += 1;
      this.links.router.forget(splitKey(stream.lane)[0]);
      this._retire(stream);
      return this._flush(monoMs());
    }
  }

  // An error frame under a stream's correlation id answers one append the connection did not take.
  //
  // `unknown_stream`: the broker stopped the stream for good (a move, or a sequence it refused), and every
  // append that reaches it after that is answered so. Those appends never reached the broker, and they are
  // always the last ones sent; which ones they are is known only once every append on the stream is
  // answered: then the ones still in flight are exactly the refused ones (`_settle`). Until then the topic
  // sends nothing (`_held`), so they go out again before anything handed over after them.
  //
  // Any other refusal uses up no sequence and does not stop the stream, so it names no append the client
  // can tell apart: the stream is retired and every append it has in flight fails with the reason.
  _refused(stream, reason) {
    if (this._early(stream, () => this._refused(stream, reason))) return;
    if (!this.streams.has(stream) || stream.inflight.size === 0) return;
    this.links.router.forget(splitKey(stream.lane)[0]);
    if (reason === 'unknown_stream') {
      stream.rejected += 1;
      this._retire(stream);
      this._settle(stream);
    } else {
      const failed = [...stream.inflight.entries()].sort(([a], [b]) => a - b).map(([, sent]) => ({ kind: 'failed', entries: sent.entries, reason }));
      stream.inflight.clear();
      stream.inflightBytes = 0;
      this._retire(stream);
      this._emit(failed);
    }
    return this._flush(monoMs());
  }

  // Every append below `acked_sequence` has been answered: the ones named in `errors` failed with their
  // reason, the rest landed. A sequence break ends the stream (lib/malachi/wire.ex): a gap names an append
  // that failed now, at or above `acked_sequence`; a repeat names a second copy, and the original's own
  // answer still comes, so it fails nothing.
  _acked(stream, ack) {
    const ends = ack.errors.some((e) => SEQUENCE_BREAKS.has(e.reason));
    const errors = new Map(ack.errors.filter((e) => e.reason !== 'sequence_repeat').map((e) => [e.sequence, e.reason]));
    const nowUs = monoUs();
    const gone = [...stream.inflight.entries()]
      .filter(([seq]) => seq < ack.acked_sequence || errors.has(seq))
      .sort(([a], [b]) => a - b);
    const events = gone.map(([seq, sent]) =>
      errors.has(seq)
        ? { kind: 'failed', entries: sent.entries, reason: errors.get(seq) }
        : { kind: 'acked', entries: sent.entries, latencyUs: nowUs - sent.sentAt },
    );
    for (const [seq, sent] of gone) {
      stream.inflight.delete(seq);
      stream.inflightBytes -= sent.size;
    }
    stream.windowAppends = ack.window_appends;
    stream.windowBytes = ack.window_bytes;
    // the range opens a new stream once this one drains (`_settle`); its routes have not changed
    if (ends) this._retire(stream);
    else if (!stream.open && stream.inflight.size === 0) this.streams.delete(stream);
    this._settle(stream);
    this._emit(events);
    return this._flush(monoMs());
  }

  // Once a stopped stream's refusals account for every append it still has in flight, those appends are the
  // refused ones: their records go back to the front of the range's buffer, in the order they were sent, and
  // the stream is dropped, which lets the range open its next one.
  _settle(stream) {
    if (stream.open || stream.rejected === 0 || stream.inflight.size !== stream.rejected) return;
    this.streams.delete(stream);
    if (this.stopped) return;
    const [topic, range] = splitKey(stream.lane);
    const lane = this.lanes.get(stream.lane) || newLane(topic, range);
    this.lanes.set(stream.lane, lane);
    for (const [, sent] of [...stream.inflight.entries()].sort(([a], [b]) => b - a)) this._unsend(lane, sent.entries);
  }

  // Whether `topic` is changing shape, so none of its ranges opens a stream or sends an append: a stream
  // retired on it still has appends in flight (its refused appends go out again before anything handed over
  // after them), or the routes no longer hold a range that still has a stream, open or opening (a split or
  // merge whose move has not reached that stream yet). Either ends on a message, which flushes again.
  _held(topic) {
    for (const stream of this.streams) if (!stream.open && splitKey(stream.lane)[0] === topic && stream.inflight.size > 0) return true;
    for (const [key, lane] of this.lanes) if (lane.topic === topic && lane.stream !== null && this._retired(key)) return true;
    return false;
  }

  // Whether the last routes read no longer hold `key`'s range (a split or merge retired it), judged by the
  // last ones read even after they are forgotten: within one life of the topic a retired range never comes
  // back.
  _retired(key) {
    const [topic, range] = splitKey(key);
    const last = this.router.lastReadOf(topic);
    return last !== null && !last.ranges.some((r) => r.range === range);
  }

  // Puts `entries`, which never left, back at the front of `lane`'s buffer.
  _unsend(lane, entries) {
    const unit = { entries, bytes: entriesBytes(entries) };
    lane.buffer.unshift(unit);
    lane.bufferBytes += unit.bytes;
  }

  _retire(stream) {
    const lane = this.lanes.get(stream.lane);
    if (lane && lane.stream && lane.stream.open === stream) lane.stream = null;
    stream.open = false;
    if (stream.inflight.size === 0) this.streams.delete(stream);
  }

  // A connection to a broker dropped: its appends in flight may or may not have landed, its streams and
  // opens are gone, and the ranges that streamed through it read their routes again before opening.
  async _dropped(address, client, now) {
    if (this.droppedClients.has(client)) return;
    this.droppedClients.add(client);
    const lost = [...this.streams].filter((s) => addressKey(s.address) === addressKey(address));
    const events = lost.flatMap((s) =>
      [...s.inflight.entries()].sort(([a], [b]) => a - b).map(([, sent]) => ({ kind: 'failed', entries: sent.entries, reason: 'connection_lost' })),
    );
    for (const s of lost) this.streams.delete(s);
    const topics = new Set();
    for (const lane of this.lanes.values()) {
      const on = lane.stream && (lane.stream.open ? lane.stream.open.address : lane.stream.address);
      if (on && addressKey(on) === addressKey(address)) {
        if (lane.stream.opening) this._settleOpening(lane.stream);
        lane.stream = null;
        lane.retryAt = backoffUntil(now);
        topics.add(lane.topic);
      }
    }
    await this.links.dropConn(address);
    this.stats.dropped += 1;
    for (const topic of topics) this.links.router.forget(topic);
    this._emit(events);
    // the drop may have ended a topic's hold (a retired stream draining on that connection): what waited
    // behind it goes now, as the Elixir producer flushes after a drop
    await this._flush(now);
  }
}

const overWindow = (stream, size) => stream.inflight.size > 0 && stream.inflightBytes + size > stream.windowBytes;

function splitKey(key) {
  const at = key.lastIndexOf('\u0000');
  return [key.slice(0, at), Number(key.slice(at + 1))];
}

function positive(n, name) {
  if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer, got ${n}`);
  return n;
}

function nonNegative(n, name) {
  if (!Number.isInteger(n) || n < 0) throw new Error(`${name} must be a non-negative integer, got ${n}`);
  return n;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = { Producer, recordBytes, entryBytes };
