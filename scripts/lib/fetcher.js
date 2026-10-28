'use strict';

/**
 * A fetching client, as the Elixir load generator's Malachi.Loadtest.Fetcher is: one `fetch_range` (key 31)
 * at a time, a closed loop, over every active range of a topic in turn, each request to the broker the
 * routes say serves the range. Where a range's next fetch starts moves past each page it reads. What a page
 * that brings nothing new does is the `atTail` option: 'restart' (the default, the load generator's) sends
 * that range back to the start of its history, so a reader keeps pulling a backlog; 'wait' keeps it where it
 * is, so a reader that wants every record once asks again from the end it reached.
 *
 * Fetches asked at once are answered one at a time, in order: a fetcher is one closed loop, and an open loop
 * that asks faster than it is answered waits in this queue, which is what its latency then shows.
 * Connections, routes and their backoffs go through ./links.js, lanes and what a split hands its children
 * through ./read_lanes.js. A fetch is answered as one of:
 *
 *   - `{ kind: 'ok', count, latencyUs }` from `fetch`: a page, with `count` records new to this reader (a
 *     page's leading records it already had, `skip`, are not counted), and the round trip of its
 *     `fetch_range` alone: reading the routes or connecting before it is preparation, timed by nobody;
 *   - `{ kind: 'page', page }` from `fetchPage`: the same page with its records' values;
 *   - `{ kind: 'error', reason }`: a refusal that is not a redirect, routes that could not be read, or a
 *     broker that took no connection ('connect_failed': it is connected to again on a later fetch);
 *   - `{ kind: 'held', ms }`: the topic's routes are held back, for `ms` more milliseconds, after a read that
 *     failed or that named no address for a primary; nothing was asked of anyone;
 *   - `{ kind: 'redirected' }`: the routes it went by were behind the cluster's, read again for the next
 *     fetch, which is not an op;
 *   - `{ kind: 'dropped' }`: the connection failed; the range's broker is connected to again later;
 *   - `{ kind: 'unassigned' }`: a group member's assignment holds none of the topic's ranges;
 *   - `{ kind: 'revoked' }`: the range left the assignment while its fetch was out; the page is dropped
 *     unread, and nothing moves where the range is read from.
 */

const wire = require('./wire');
const { Links, isRedirect, backoffUntil, monoMs } = require('./links');
const { MalachiError } = require('./client');
const readLanes = require('./read_lanes');

const MAX_BYTES = 1048576;
// What a page's batch may inflate to: the broker sends at least one whole batch per page, so a page can pass
// MAX_BYTES; the cap is the broker's own for a batch (MALACHI_MAX_INFLATED_BATCH_BYTES).
const MAX_INFLATED_BYTES = 16777216;
// The least a broker is asked to hold a fetch, whatever the share of `waitMs` a range gets: below it a
// follower with many idle ranges asks more often than it learns anything.
const MIN_WAIT_MS = 200;

const monoUs = () => Number(process.hrtime.bigint() / 1000n);

class Fetcher {
  // A fetcher routing by `router` (./router.js) and connecting with `connOpts` ({ user, pass, timeout }).
  // Options: `max`, records per page at most (100); `atTail`, 'restart' (the default) or 'wait'; `waitMs`,
  // how long one pass over the ranges read may be held by brokers that find nothing new (0, the load
  // generator's; a follower long-polls), split across them, at least MIN_WAIT_MS a fetch (or `waitMs` when
  // lower): a record on any range waits behind the holds on the others for about `waitMs` at most, or
  // MIN_WAIT_MS a range past `waitMs / MIN_WAIT_MS` of them (25 ranges at 5000 ms); `startFrom`,
  // where a range with no ancestor read starts ('earliest', or a group member's `{ committed: group }`).
  constructor(router, connOpts, opts = {}) {
    const atTail = opts.atTail ?? 'restart';
    if (atTail !== 'restart' && atTail !== 'wait') throw new Error(`atTail must be 'restart' or 'wait', got ${atTail}`);
    const max = opts.max ?? 100;
    if (!Number.isInteger(max) || max < 1) throw new Error(`max must be a positive integer, got ${max}`);
    this.links = new Links(router, connOpts);
    this.router = router;
    this.max = max;
    this.atTail = atTail;
    this.lanes = new Map();
    this.retired = new Map();
    this.turn = 0;
    this.waitMs = opts.waitMs ?? 0;
    this.startFrom = opts.startFrom ?? 'earliest';
    this.assigned = null; // the ranges a group member was handed, or null to read every one
    this.stats = { moved: 0, dropped: 0 };
    this.chain = Promise.resolve();
  }

  // Connects to the broker of every active range of `topic` before the first fetch, so a reader that
  // connects up front pays none of it later. `{ ok: true }`, or `{ error }` with the refusal.
  prepare(topic) {
    return this._serial(async () => {
      const connected = await this.links.connectRanges(topic);
      if (connected.error) return connected;
      this._reconcile(topic);
      return { ok: true };
    });
  }

  // One fetch of the next range of `topic`, in turn, a page as a count (counted, not decoded: a load
  // generator measures the broker, not its own parsing of what came back).
  // `now` is read when the fetch runs, not when it was asked, unless the caller sets it: a fetch that waited
  // behind others goes by the clock it runs at.
  fetch(topic, now) {
    return this._serial(async () => {
      const answer = this._kept(await this._next(topic, now ?? monoMs()));
      if (answer.kind !== 'fetched') return answer;
      const { key, lane, page, latencyUs } = answer;
      const count = Math.max(0, wire.batchHeader(page.batch).count - page.skip);
      this._advance(key, lane, page, count > 0);
      return { kind: 'ok', count, latencyUs };
    });
  }

  // One fetch of the next range of `topic`, in turn, as `fetch`, a page with its values: the range it came
  // from (`range`, and `block` in a keyspace of `size`), the topic's active ranges by the routes it was
  // fetched by (`active`), where it was asked from (`from`) and where the range's next page starts (`to`).
  fetchPage(topic, now) {
    return this._serial(async () => {
      const answer = this._kept(await this._next(topic, now ?? monoMs()));
      if (answer.kind !== 'fetched') return answer;
      const { key, lane, shape, page } = answer;
      const entries = wire.decodeBatch(page.batch, { maxInflatedBytes: MAX_INFLATED_BYTES, layout: 'positioned' });
      const records = entries.slice(page.skip).map((e) => e.record);
      const values = records.map((r) => r.value);
      const [, range] = readLanes.splitKey(key);
      const fetched = { range, active: shape.active, block: lane.block, size: shape.size, from: lane.start, to: page.next, values, records };
      this._advance(key, lane, page, values.length > 0);
      return { kind: 'page', page: fetched };
    });
  }

  // Reads only `ranges` from now on (a group member's assignment), or every range with null. At once, not
  // behind the fetch out: a page of a range that left meanwhile is dropped (`revoked`), so no record of a
  // range the group handed on is read here after the member heard of it. A range that leaves forgets where
  // it was read: handed back later, it starts from the group's checkpoint again.
  setAssignment(ranges) {
    this.assigned = readLanes.assignmentOf(ranges);
    for (const key of [...this.lanes.keys()]) if (!readLanes.assigned(this.assigned, key)) this.lanes.delete(key);
  }

  // What routing did so far.
  statsNow() {
    return { ...this.stats, refreshes: this.links.stats.refreshes, redirects: this.links.stats.redirects, connect_failures: this.links.stats.connect_failures };
  }

  // Closes every connection; a fetch still queued then answers `{ kind: 'closed' }` and opens nothing.
  close() {
    this.closed = true;
    return this.links.closeAll();
  }

  _serial(fn) {
    const step = this.chain.then(fn);
    this.chain = step.catch(() => {});
    return step;
  }

  async _next(topic, now) {
    if (this.closed) return { kind: 'closed' };
    const backoff = this.links.unroutable.get(topic);
    if (!this.router.routes(topic) && backoff && backoff.until > now) return { kind: 'held', ms: backoff.until - now };
    const routed = await this.links.ensureRoutes(topic, now);
    if (routed.error) return { kind: 'error', reason: routed.error };
    this._reconcile(topic);
    const ranges = this._readable(topic);
    if (ranges.length === 0) return { kind: 'unassigned' };
    const { range } = ranges[this.turn % ranges.length];
    this.turn += 1;
    return this._fetchRange(topic, range, now, this._waitFor(ranges.length));
  }

  // How long one fetch may be held, with `readable` ranges to go round.
  _waitFor(readable) {
    if (this.waitMs === 0) return 0;
    return Math.max(Math.min(this.waitMs, MIN_WAIT_MS), Math.floor(this.waitMs / readable));
  }

  // A fetched page of a range the assignment still holds, or `revoked`.
  _kept(answer) {
    return answer.kind === 'fetched' && !readLanes.assigned(this.assigned, answer.key) ? { kind: 'revoked' } : answer;
  }

  _readable(topic) {
    return readLanes.assignedRanges(this.assigned, this.router.routes(topic).ranges);
  }

  _reconcile(topic) {
    const fresh = (start) => (start === 'earliest' ? this.startFrom : start);
    readLanes.reconcile(this.lanes, this.retired, topic, this._readable(topic), () => true, (block, start) => ({ block, start: fresh(start) }));
  }

  async _fetchRange(topic, range, now, waitMs) {
    const where = this.router.addressOf(topic, range);
    // routes that name no address for the primary are behind the cluster's: read again, but not before a
    // backoff, so a fetcher with only that range to fetch waits rather than spin on reads
    if (where.error === 'unadvertised') {
      this.links.bump('redirects');
      this.router.forget(topic);
      this.links.unroutable.set(topic, { until: backoffUntil(now), reason: 'unadvertised' });
      return { kind: 'redirected' };
    }
    const client = await this.links.connection(where.address);
    if (!client) {
      this.router.forget(topic);
      return { kind: 'error', reason: 'connect_failed' };
    }
    const key = readLanes.laneKey(topic, range);
    // the range left the assignment while its broker was connected to
    if (!readLanes.assigned(this.assigned, key)) return { kind: 'revoked' };
    const lane = this.lanes.get(key);
    const req = {
      topic,
      range,
      routes_version: this.router.version(topic),
      start: lane.start,
      max: this.max,
      max_bytes: MAX_BYTES,
      wait_ms: waitMs,
      accept: ['none', 'zstd'],
    };
    const t0 = monoUs();
    try {
      const page = await client.fetchRange(req);
      const latencyUs = monoUs() - t0;
      const { size, ranges } = this.router.routes(topic);
      return { kind: 'fetched', key, lane, shape: { size, active: ranges.map((r) => r.range) }, page, latencyUs };
    } catch (err) {
      if (err instanceof MalachiError && !err.transport && !err.timeout) {
        if (!isRedirect(err.message)) return { kind: 'error', reason: err.message };
        this.links.bump('redirects');
        this.router.forget(topic);
        return { kind: 'redirected' };
      }
      await this.links.dropConn(where.address);
      this.stats.dropped += 1;
      this.router.forget(topic);
      return { kind: 'dropped' };
    }
  }

  // Where a range's next fetch starts after a page: past it, or, when it brought nothing new, back at the
  // start of its history ('restart') or where the page says the next one starts ('wait'). An empty page can
  // still move on: from a position in an ancestor's history that held nothing, it names the range's own
  // records next, and a fetch from there is one the broker can hold (`waitMs`) until something comes.
  _advance(key, lane, page, broughtAny) {
    if (broughtAny || this.atTail === 'wait') lane.start = { position: page.next };
    else lane.start = 'earliest';
    this.lanes.set(key, lane);
  }
}

module.exports = { Fetcher };
