'use strict';

/**
 * What every routing client keeps between a topic's routes and the brokers that serve them, as the Elixir
 * load generator's Malachi.Loadtest.Links does: one connection per broker address, a metadata connection
 * opened on first use, the routes read again when they were forgotten, with a per-topic backoff once a read
 * fails, and counts of what routing did (`stats`), for a routing client to hold and route through.
 *
 * Connections are ./client.js MalachiClients, authenticated with `connOpts` ({ user, pass, timeout });
 * host and port come from the routes (./router.js).
 */

const { MalachiClient, MalachiError } = require('./client');

// How long a backoff lasts: a topic whose routes could not be read, a range whose open failed.
const RETRY_MS = 200;
// Redirects in a row after which a range backs off before opening again, and the most it waits.
const REDIRECTS_BEFORE_BACKOFF = 5;
const MAX_BACKOFF_MS = 1000;

const REDIRECTS = new Set(['moved', 'stale_routes', 'not_owner', 'migrating']);

// Whether `reason`, a refusal, means the routes the call went by are behind the cluster's. Every client
// reads its routes again on one, rather than count it as an error.
const isRedirect = (reason) => REDIRECTS.has(reason);

// When a range redirected `redirects` times in a row opens again: at once (null) for the first four, then
// after a backoff that grows by 50 ms a redirect up to a second, counted from `now`.
function redirectRetryAt(redirects, now) {
  if (redirects < REDIRECTS_BEFORE_BACKOFF) return null;
  return now + Math.min(MAX_BACKOFF_MS, 50 * (redirects - REDIRECTS_BEFORE_BACKOFF + 1));
}

const monoMs = () => Number(process.hrtime.bigint() / 1000000n);

// When a fixed backoff ends, counted from when the call gave up, not from when it was asked: a call that
// waited (a node that does not answer costs the connect timeout per address) would otherwise leave one
// already over. `now` is the caller's instant, which a test may set ahead of the clock.
const backoffUntil = (now) => Math.max(now, monoMs()) + RETRY_MS;

const addressKey = ({ host, port }) => `${host}:${port}`;

class Links {
  constructor(router, connOpts = {}) {
    this.router = router;
    this.connOpts = connOpts;
    this.conns = new Map(); // address key -> Promise<MalachiClient | null>
    this.control = null; // the metadata connection, or null
    this.controlOpening = null; // the metadata connect in flight, shared by the reads that wait on it
    this.unroutable = new Map(); // topic -> { until, reason } while its routes back off
    this.stats = { refreshes: 0, connect_failures: 0, redirects: 0 };
  }

  bump(stat) {
    this.stats[stat] = (this.stats[stat] || 0) + 1;
  }

  // A new authenticated connection to `address`, destroyed again if it or its authentication fails;
  // null then.
  async connect({ host, port }) {
    const client = new MalachiClient({ host, port, timeout: this.connOpts.timeout });
    try {
      await client.connect(this.connOpts.user, this.connOpts.pass);
      return client;
    } catch (_refused) {
      client.destroy();
      return null;
    }
  }

  // The connection to `address`, opened and authenticated on first use and again once the one held was
  // closed: the client, or null with `connect_failures` counted. Callers asking at once share one connect,
  // and its failure too: each gets null, and it counts once.
  async connection(address) {
    const key = addressKey(address);
    const held = this.conns.get(key);
    if (held) {
      const client = await held;
      if (!client || client.socket) return client;
      if (this.conns.get(key) === held) this.conns.delete(key);
      return this.connection(address);
    }

    const opening = this.connect(address);
    this.conns.set(key, opening);
    const client = await opening;
    if (!client) {
      if (this.conns.get(key) === opening) this.conns.delete(key);
      this.bump('connect_failures');
    }
    return client;
  }

  // Connects, authenticated, to the broker of every active range of `topic`, opening nothing on them: what a
  // reader does before it starts, so its reading pays no connect later. `{ ok: true }`, or `{ error }` with
  // the refusal the routes got, 'connect_failed' or 'unadvertised'.
  async connectRanges(topic) {
    const routed = await this.ensureRoutes(topic, monoMs());
    if (routed.error) return routed;
    for (const { range } of this.router.routes(topic).ranges) {
      const { address, error } = this.router.addressOf(topic, range);
      if (error) return { error };
      if (!(await this.connection(address))) return { error: 'connect_failed' };
    }
    return { ok: true };
  }

  // Closes and forgets the connection to `address`, if there is one.
  async dropConn(address) {
    const key = addressKey(address);
    const held = this.conns.get(key);
    if (!held) return;
    this.conns.delete(key);
    const client = await held;
    if (client) client.destroy();
  }

  // Closes every connection, the metadata one included.
  async closeAll() {
    const held = [...this.conns.values()];
    this.conns.clear();
    for (const client of await Promise.all(held)) if (client) client.close();
    if (this.control) this.control.close();
    this.control = null;
  }

  // The routes of `topic`, read again if they were forgotten: `{ ok: true }`, or `{ error }`. A topic whose
  // routes could not be read is not asked again until its backoff ends, whichever path asks: until then the
  // answer is the refusal the routes last got, with no round trip.
  async ensureRoutes(topic, now) {
    if (this.router.routes(topic)) return { ok: true };
    const backoff = this.unroutable.get(topic);
    if (backoff && backoff.until > now) return { error: backoff.reason };

    const read = await this.refresh(topic);
    if (read.error) this.unroutable.set(topic, { until: backoffUntil(now), reason: read.error });
    else this.unroutable.delete(topic);
    return read;
  }

  // Reads the cluster state and `topic`'s routes over the metadata connection, opened on first use to the
  // bootstrap address or, should that not answer, to each broker the cluster state lists in turn. Not held
  // back by the topic's backoff: `ensureRoutes` is what honours it.
  async refresh(topic) {
    this.bump('refreshes');
    return this._readRoutes(topic, this.control !== null);
  }

  // A metadata connection kept from an earlier read may have been closed by the server since (an idle one
  // is): one that fails is replaced and the read made again, once.
  async _readRoutes(topic, reused) {
    const control = await this._control();
    if (!control) return { error: 'metadata_unavailable' };
    try {
      await this.router.refresh(control, topic);
      return { ok: true };
    } catch (err) {
      // a refusal is the server's answer; a dropped or silent connection is replaced
      if (err instanceof MalachiError && !err.transport && !err.timeout) return { error: err.message };
      control.destroy();
      this.control = null;
      return reused ? this._readRoutes(topic, false) : { error: 'metadata_unavailable' };
    }
  }

  // The metadata connection, opened when there is none (or the one held was closed). Reads that ask at once
  // share one connect, so no connection is opened that `closeAll` would not close.
  async _control() {
    if (this.control && this.control.socket) return this.control;
    this.control = null;
    if (!this.controlOpening) {
      this.controlOpening = this._openControl().finally(() => (this.controlOpening = null));
    }
    return this.controlOpening;
  }

  async _openControl() {
    for (const address of this._controlAddresses()) {
      const client = await this.connect(address);
      if (client) {
        this.control = client;
        return client;
      }
    }
    return null;
  }

  _controlAddresses() {
    const seen = new Set();
    const all = [this.router.bootstrap];
    for (const b of this.router.brokers.values()) if (typeof b.host === 'string' && b.port > 0) all.push({ host: b.host, port: b.port });
    return all.filter((a) => !seen.has(addressKey(a)) && seen.add(addressKey(a)));
  }

  // A range whose open the broker refused as a redirect (`isRedirect`): the routes the open went by are
  // behind the cluster's. Not a failure: counted in `redirects`, the topic's routes forgotten so the next
  // look reads them again, and `lane` (with `stream`, `redirects` and `retryAt`) due again at `now` for the
  // first four in a row, after a growing backoff from the fifth (`redirectRetryAt`).
  redirected(topic, lane, now) {
    this._redirect(topic, lane, (redirects) => redirectRetryAt(redirects, now) ?? now);
  }

  // `redirected` for routes that name a range's primary with no address: the same redirect, but never due
  // before a fixed backoff, not even the first in a row. An address takes gossip rounds to spread, so
  // reading the routes again at once would only add metadata reads to the cluster being measured.
  unadvertised(topic, lane, now) {
    this._redirect(topic, lane, (redirects) => Math.max(redirectRetryAt(redirects, now) ?? now, backoffUntil(now)));
  }

  _redirect(topic, lane, retryAt) {
    lane.stream = null;
    lane.redirects += 1;
    lane.retryAt = retryAt(lane.redirects);
    this.bump('redirects');
    this.router.forget(topic);
  }
}

module.exports = { Links, isRedirect, redirectRetryAt, backoffUntil, monoMs, RETRY_MS };
