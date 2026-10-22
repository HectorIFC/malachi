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
 *   - `{ kind: 'dropped' }`: the connection failed; the range's broker is connected to again later.
 */

const wire = require('./wire');
const { Links, isRedirect, backoffUntil, monoMs } = require('./links');
const { MalachiError } = require('./client');
const readLanes = require('./read_lanes');

const MAX_BYTES = 1048576;
// What a page's batch may inflate to: the broker sends at least one whole batch per page, so a page can pass
// MAX_BYTES; the cap is the broker's own for a batch (MALACHI_MAX_INFLATED_BATCH_BYTES).
const MAX_INFLATED_BYTES = 16777216;

const monoUs = () => Number(process.hrtime.bigint() / 1000n);

class Fetcher {
  // A fetcher routing by `router` (./router.js) and connecting with `connOpts` ({ user, pass, timeout }).
  // Options: `max`, records per page at most (100), and `atTail`, 'restart' (the default) or 'wait'.
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
      const answer = await this._next(topic, now ?? monoMs());
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
      const answer = await this._next(topic, now ?? monoMs());
      if (answer.kind !== 'fetched') return answer;
      const { key, lane, shape, page } = answer;
      const entries = wire.decodeBatch(page.batch, { maxInflatedBytes: MAX_INFLATED_BYTES, layout: 'positioned' });
      const values = entries.slice(page.skip).map((e) => e.record.value);
      const [, range] = readLanes.splitKey(key);
      const fetched = { range, active: shape.active, block: lane.block, size: shape.size, from: lane.start, to: page.next, values };
      this._advance(key, lane, page, values.length > 0);
      return { kind: 'page', page: fetched };
    });
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
    const { ranges } = this.router.routes(topic);
    const { range } = ranges[this.turn % ranges.length];
    this.turn += 1;
    return this._fetchRange(topic, range, now);
  }

  _reconcile(topic) {
    readLanes.reconcile(this.lanes, this.retired, topic, this.router.routes(topic).ranges, () => true, (block, start) => ({ block, start }));
  }

  async _fetchRange(topic, range, now) {
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
    const lane = this.lanes.get(key);
    const req = {
      topic,
      range,
      routes_version: this.router.version(topic),
      start: lane.start,
      max: this.max,
      max_bytes: MAX_BYTES,
      wait_ms: 0,
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
  // start of its history ('restart') or where it is ('wait').
  _advance(key, lane, page, broughtAny) {
    if (broughtAny) lane.start = { position: page.next };
    else if (this.atTail === 'restart') lane.start = 'earliest';
    this.lanes.set(key, lane);
  }
}

module.exports = { Fetcher };
