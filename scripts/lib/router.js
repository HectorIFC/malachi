'use strict';

/**
 * Where a record goes, as a NorthGuard client knows it (meetup transcript, 609 to 613 and 885), and as the
 * Elixir load generator's Malachi.Loadtest.Router does: the minimal cluster state any node answers
 * (`cluster_state`, key 24), and per topic its routes (`topic_routes`, key 25), cached here and read again
 * only when they may have changed, never on a timer: the cluster said so (a `moved`, `stale_routes`,
 * `not_owner` or `migrating` answer), a connection to a primary failed, or the routes name a primary with
 * no address (./links.js forgets them then).
 *
 * A record's key is hashed into the topic's keyspace exactly as the server does (./keyspace.js), the active
 * range holding that position is the range it goes to, and the broker leading that range's active segment
 * is where the range's stream is opened. A range with no active segment (`unassigned`, before its first
 * record or after a failover sealed its segment) is opened at a live broker, which places the segment and
 * answers `moved` towards its primary (`addressOf`).
 *
 * Pure but for `bootstrap` and `refresh`, which ask over a connected ./client.js MalachiClient.
 */

const keyspace = require('./keyspace');
const { MalachiError } = require('./client');

class Router {
  // A router that knows only the address it bootstraps from, `{ host, port }`.
  constructor(bootstrap) {
    if (typeof bootstrap.host !== 'string' || !Number.isInteger(bootstrap.port)) {
      throw new Error(`a router bootstraps from { host, port }, not ${JSON.stringify(bootstrap)}`);
    }
    this.bootstrap = { host: bootstrap.host, port: bootstrap.port };
    this.brokers = new Map(); // id -> { host, port, status }
    this.streamsEnabled = false;
    this.topics = new Map(); // topic -> routes, dropped by forget so the next record reads them again
    this.lastRead = new Map(); // topic -> the last routes read, kept by forget
  }

  // Reads the cluster state through `client`, connected and authenticated to the bootstrap address.
  async bootstrapFrom(client) {
    this.putClusterState(await clusterStateOf(client));
    return this;
  }

  // Reads the cluster state and `topic`'s routes again through `client`: after a move, or a connection to a
  // primary that failed, either may have changed. Rejects with the server's refusal (the topic does not
  // exist, the stream keys are off) as it answered, and with `malformed_cluster_state` for an answer that
  // is not a cluster state, as the Elixir router names it.
  async refresh(client, topic) {
    this.putClusterState(await clusterStateOf(client));
    this.putRoutes(await client.topicRoutes(topic));
    return this;
  }

  // Takes a decoded `cluster_state` answer.
  putClusterState({ brokers, streams_enabled: streamsEnabled }) {
    this.brokers = new Map(brokers.map((b) => [b.id, { host: b.host, port: b.port, status: b.status }]));
    this.streamsEnabled = streamsEnabled;
  }

  // Takes a decoded `topic_routes` answer: only the active ranges route records.
  putRoutes({ topic, version, keyspace_bits: bits, ranges }) {
    const active = ranges
      .filter((r) => r.state === 'active')
      .map((r) => ({ range: r.range, keyStart: r.key_start, keyEnd: r.key_end, primary: r.segment ? r.segment.primary : null }));
    const routes = { version, size: 2 ** bits, ranges: active };
    this.topics.set(topic, routes);
    this.lastRead.set(topic, routes);
  }

  // Forgets `topic`'s routes, so the next record for it reads them again.
  forget(topic) {
    this.topics.delete(topic);
  }

  // `topic`'s routes, or null when they have not been read (or were forgotten).
  routes(topic) {
    return this.topics.get(topic) || null;
  }

  // The last routes read for `topic`, kept when `forget` drops them, or null before any read: for telling
  // that a range was retired, never for routing a record.
  lastReadOf(topic) {
    return this.lastRead.get(topic) || null;
  }

  // The routes version of `topic` a stream is opened against, or null before they are read.
  version(topic) {
    const routes = this.routes(topic);
    return routes ? routes.version : null;
  }

  // The range of `topic` a record keyed `key` goes to: `{ range }`, or `{ error: 'unknown_topic' }` before
  // the topic's routes are read, `{ error: 'no_range' }` when no active range covers the key's position,
  // which only routes caught mid-split can say and reading them again resolves.
  rangeOf(topic, key) {
    const routes = this.routes(topic);
    if (!routes) return { error: 'unknown_topic' };
    const position = keyspace.positionOf(key, routes.size);
    const found = routes.ranges.find((r) => position >= r.keyStart && position < r.keyEnd);
    return found ? { range: found.range } : { error: 'no_range' };
  }

  // The address to open `topic`'s `range` stream at: `{ address: { host, port } }`, its primary's advertised
  // address. A range with no active segment is opened at a live broker that advertises an address, picked
  // by the range (`phash2("<topic>/<range>", n)` over the live brokers sorted by id, as the Elixir router
  // picks) so a topic's ranges spread over the cluster; the bootstrap when the cluster is one broker or
  // lists none alive. A primary with no address is reachable only as the one broker of a single node
  // cluster, the bootstrap itself; otherwise, or for a primary the state does not list,
  // `{ error: 'unadvertised' }`. `{ error: 'unknown_topic' }` and `{ error: 'unknown_range' }` as they say.
  addressOf(topic, range) {
    const routes = this.routes(topic);
    if (!routes) return { error: 'unknown_topic' };
    const found = routes.ranges.find((r) => r.range === range);
    if (!found) return { error: 'unknown_range' };
    if (found.primary === null) return { address: this._unassignedAddress(topic, range) };
    return this._brokerAddress(found.primary);
  }

  _unassignedAddress(topic, range) {
    if (this.brokers.size <= 1) return this.bootstrap;
    const live = [...this.brokers.entries()]
      .sort(([a], [b]) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8')))
      .filter(([, b]) => b.status === 'alive' && advertised(b))
      .map(([, b]) => ({ host: b.host, port: b.port }));
    return live.length === 0 ? this.bootstrap : live[keyspace.positionOf(`${topic}/${range}`, live.length)];
  }

  _brokerAddress(primary) {
    const broker = this.brokers.get(primary);
    if (broker && advertised(broker)) return { address: { host: broker.host, port: broker.port } };
    if (broker && this.brokers.size === 1) return { address: this.bootstrap };
    return { error: 'unadvertised' };
  }
}

// An answer the cluster state codec cannot read is the server's answer, a refusal, not a connection that
// failed: a node that does not speak the key and answers every key with an empty ok, say.
async function clusterStateOf(client) {
  try {
    return await client.clusterState();
  } catch (err) {
    if (err instanceof MalachiError) throw err;
    throw new MalachiError('malformed_cluster_state');
  }
}

// A broker that advertises no address answers port 0 (lib/malachi/routing.ex), as the Elixir router reads it.
const advertised = (broker) => typeof broker.host === 'string' && broker.port > 0;

module.exports = { Router };
