'use strict';

/**
 * The routing client (./router.js, ./links.js) and the stream plumbing of ./client.js. ExUnit runs it
 * (test/scripts/routing_js_test.exs):
 *
 *   node scripts/lib/routing.selftest.js                  the cases that need no server
 *   node scripts/lib/routing.selftest.js --addresses JSON  where each [topic, range] of JSON.queries is
 *                                                          opened, given JSON.cluster_state and JSON.routes
 *                                                          (decoded answers), as JSON, for the Elixir test to
 *                                                          compare with Malachi.Loadtest.Router
 *   node scripts/lib/routing.selftest.js --live TOPIC      against MALACHI_HOST/PORT as MALACHI_USER/PASS:
 *                                                          reads TOPIC's routes, connects to its ranges and
 *                                                          streams on one, printing what it saw as JSON
 */

const assert = require('assert');
const fs = require('fs');
const net = require('net');
const path = require('path');
const { EventEmitter } = require('events');
const wire = require('./wire');
const { MalachiClient } = require('./client');
const { Router } = require('./router');
const { Links, isRedirect, redirectRetryAt } = require('./links');

const broker = (id, host, port, status = 'alive') => ({ id, host, port, status });
const range = (n, start, end, primary) => ({
  range: n,
  key_start: start,
  key_end: end,
  state: 'active',
  segment: primary === null ? null : { segment: 1, primary },
});

function routerWith(brokers, ranges, bits = 4) {
  const router = new Router({ host: 'boot', port: 4040 });
  router.putClusterState({ brokers, streams_enabled: true });
  router.putRoutes({ topic: 't', version: 7, keyspace_bits: bits, ranges });
  return router;
}

// A server in this process: answers each request frame with `answer(apiKey, payload, socket)`, which returns
// `[errorCode, payload]` to reply, or null to say nothing. Authentication is always answered with a token.
// Resolves with { port, accepted (the connections it took), sockets, close }.
function fakeServer(answer) {
  const sockets = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let buffer = Buffer.alloc(0);
    socket.on('error', () => {});
    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      let frame;
      while ((frame = wire.decodeFrame(buffer)) !== null) {
        buffer = frame.rest;
        const apiKey = frame.body.readUInt16BE(0);
        const corr = frame.body.readUInt32BE(2);
        const payload = frame.body.subarray(6);
        const reply = apiKey === wire.API.auth ? [wire.OK, str('token')] : answer(apiKey, payload, socket, corr);
        if (reply) socket.write(responseFrame(corr, ...reply));
      }
    });
  });
  return new Promise((resolve) =>
    server.listen(0, '127.0.0.1', () =>
      resolve({
        port: server.address().port,
        sockets,
        close: () => {
          for (const socket of sockets) socket.destroy();
          server.close();
        },
      }),
    ),
  );
}

// A string as Malachi.Wire writes it: a presence byte, a u32 length, the bytes.
function str(text) {
  const bytes = Buffer.from(text, 'utf8');
  const len = Buffer.alloc(4);
  len.writeUInt32BE(bytes.length);
  return Buffer.concat([Buffer.from([1]), len, bytes]);
}

function responseFrame(corr, code, payload) {
  const head = Buffer.alloc(6);
  head.writeUInt32BE(corr, 0);
  head.writeUInt16BE(code, 4);
  return wire.encodeFrame(Buffer.concat([head, payload]));
}

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));

// Links whose connect is a stand-in: `outcome(address)` says what each connect to an address gives, a
// client-like object or null; `attempts` counts them per address.
function stubLinks(outcome, router = routerWith([broker('a@h', '10.0.0.1', 4040)], [range(1, 0, 16, 'a@h')])) {
  const links = new Links(router);
  links.attempts = new Map();
  links.connect = async (address) => {
    const key = `${address.host}:${address.port}`;
    links.attempts.set(key, (links.attempts.get(key) || 0) + 1);
    await tick(10);
    return outcome(address);
  };
  return links;
}

const fakeClient = () => ({ socket: {}, destroyed: 0, closed: 0, destroy() { this.destroyed++; this.socket = null; }, close() { this.closed++; } });

async function selfTest() {
  const cases = [];
  const check = async (name, fn) => {
    await fn();
    cases.push(name);
  };

  await check('only active ranges route, each by its block of the keyspace', () => {
    const sealed = { ...range(9, 0, 16, 'a@h'), state: 'sealed' };
    const router = routerWith([broker('a@h', '10.0.0.1', 4040)], [sealed, range(1, 0, 8, 'a@h'), range(2, 8, 16, 'a@h')]);
    assert.deepStrictEqual(router.routes('t').ranges.map((r) => r.range), [1, 2]);
    assert.strictEqual(router.routes('t').size, 16);
    assert.strictEqual(router.version('t'), 7);
    // the broker's own positions (keyspace_vectors.json): a key in the lower half of 16 and one in the upper
    const vectors = JSON.parse(fs.readFileSync(path.join(__dirname, '../../test/support/fixtures/wire/keyspace_vectors.json'), 'utf8'));
    const at16 = vectors.sizes.indexOf(16);
    const keyed = vectors.cases.filter((c) => typeof c.key === 'string');
    const low = keyed.find((c) => c.positions[at16] < 8);
    const high = keyed.find((c) => c.positions[at16] >= 8);
    assert.deepStrictEqual(router.rangeOf('t', low.key), { range: 1 });
    assert.deepStrictEqual(router.rangeOf('t', high.key), { range: 2 });
  });

  await check('unknown topic, no range, unknown range', () => {
    const router = routerWith([broker('a@h', '10.0.0.1', 4040)], [range(1, 0, 8, 'a@h')]);
    assert.deepStrictEqual(router.rangeOf('nope', 'k'), { error: 'unknown_topic' });
    assert.deepStrictEqual(router.addressOf('nope', 1), { error: 'unknown_topic' });
    assert.deepStrictEqual(router.addressOf('t', 5), { error: 'unknown_range' });
    const uncovered = [...Array(64).keys()].map((i) => `k${i}`).find((k) => router.rangeOf('t', k).error);
    assert.deepStrictEqual(router.rangeOf('t', uncovered), { error: 'no_range' });
  });

  await check('forget drops the routes but keeps the last read', () => {
    const router = routerWith([broker('a@h', '10.0.0.1', 4040)], [range(1, 0, 16, 'a@h')]);
    router.forget('t');
    assert.strictEqual(router.routes('t'), null);
    assert.strictEqual(router.version('t'), null);
    assert.strictEqual(router.lastReadOf('t').ranges[0].range, 1);
  });

  await check('a primary is reached at its advertised address', () => {
    const router = routerWith([broker('a@h', '10.0.0.1', 4040), broker('b@h', '10.0.0.2', 5050)], [range(1, 0, 16, 'b@h')]);
    assert.deepStrictEqual(router.addressOf('t', 1), { address: { host: '10.0.0.2', port: 5050 } });
  });

  await check('a primary advertising nothing is the bootstrap only as the one broker', () => {
    const single = routerWith([broker('a@h', '', 0)], [range(1, 0, 16, 'a@h')]);
    assert.deepStrictEqual(single.addressOf('t', 1), { address: { host: 'boot', port: 4040 } });
    const several = routerWith([broker('a@h', '', 0), broker('b@h', '10.0.0.3', 4040)], [range(1, 0, 16, 'a@h')]);
    assert.deepStrictEqual(several.addressOf('t', 1), { error: 'unadvertised' });
    const unlisted = routerWith([broker('b@h', '10.0.0.3', 4040), broker('c@h', '10.0.0.4', 4040)], [range(1, 0, 16, 'a@h')]);
    assert.deepStrictEqual(unlisted.addressOf('t', 1), { error: 'unadvertised' });
  });

  await check('a range with no primary opens at a live advertised broker, spread by range, else the bootstrap', () => {
    const brokers = [
      broker('a@h', '10.0.0.1', 4040, 'dead'),
      broker('b@h', '10.0.0.2', 4040, 'suspect'),
      broker('c@h', '10.0.0.3', 4040),
      broker('d@h', '10.0.0.4', 4040),
    ];
    const ranges = [...Array(16).keys()].map((n) => range(n, n, n + 1, null));
    const router = routerWith(brokers, ranges);
    const hosts = ranges.map((r) => router.addressOf('t', r.range).address.host);
    assert.deepStrictEqual([...new Set(hosts)].sort(), ['10.0.0.3', '10.0.0.4']);
    const down = routerWith([broker('a@h', '10.0.0.9', 4040, 'dead'), broker('b@h', '10.0.0.9', 4040, 'dead')], [range(0, 0, 16, null)]);
    assert.deepStrictEqual(down.addressOf('t', 0), { address: { host: 'boot', port: 4040 } });
    const one = routerWith([broker('a@h', '10.0.0.2', 5050)], [range(0, 0, 16, null)]);
    assert.deepStrictEqual(one.addressOf('t', 0), { address: { host: 'boot', port: 4040 } });
  });

  await check('redirects: which refusals, and when a redirected range opens again', () => {
    for (const reason of ['moved', 'stale_routes', 'not_owner', 'migrating']) assert.ok(isRedirect(reason), reason);
    assert.ok(!isRedirect('permission_denied'));
    assert.strictEqual(redirectRetryAt(4, 1000), null);
    assert.strictEqual(redirectRetryAt(5, 1000), 1050);
    assert.strictEqual(redirectRetryAt(6, 1000), 1100);
    assert.strictEqual(redirectRetryAt(100, 1000), 2000);

    const links = new Links(routerWith([broker('a@h', '10.0.0.1', 4040)], [range(1, 0, 16, 'a@h')]));
    const lane = { stream: 'open', redirects: 0, retryAt: null };
    links.redirected('t', lane, 1000);
    assert.deepStrictEqual(lane, { stream: null, redirects: 1, retryAt: 1000 });
    assert.strictEqual(links.router.routes('t'), null, 'the routes are read again');
    assert.strictEqual(links.stats.redirects, 1);
    lane.redirects = 4;
    links.redirected('t', lane, 1000);
    assert.strictEqual(lane.retryAt, 1050);
    const fresh = { stream: null, redirects: 0, retryAt: null };
    links.unadvertised('t', fresh, 0);
    assert.ok(fresh.retryAt >= 200, 'an unadvertised primary waits a fixed backoff even the first time');
  });

  await check('the metadata connection falls back to every advertised broker, the bootstrap first, once each', () => {
    const router = routerWith(
      [broker('a@h', '10.0.0.1', 4040), broker('b@h', '', 0), broker('c@h', 'boot', 4040)],
      [range(1, 0, 16, 'a@h')],
    );
    assert.deepStrictEqual(new Links(router)._controlAddresses(), [
      { host: 'boot', port: 4040 },
      { host: '10.0.0.1', port: 4040 },
    ]);
  });

  await check('callers asking at once share one connect, and its failure: one null each, counted once', async () => {
    const links = stubLinks(() => null);
    const answers = await Promise.all([1, 2, 3, 4].map(() => links.connection({ host: 'x', port: 1 })));
    assert.deepStrictEqual(answers, [null, null, null, null]);
    assert.strictEqual(links.attempts.get('x:1'), 1);
    assert.strictEqual(links.stats.connect_failures, 1);
  });

  await check('a connection held is reused, reopened once it closed, and dropConn closes and forgets it', async () => {
    const links = stubLinks(() => fakeClient());
    const first = await links.connection({ host: 'x', port: 1 });
    assert.strictEqual(await links.connection({ host: 'x', port: 1 }), first);
    first.socket = null;
    const second = await links.connection({ host: 'x', port: 1 });
    assert.notStrictEqual(second, first);
    assert.strictEqual(links.attempts.get('x:1'), 2);
    await links.dropConn({ host: 'x', port: 1 });
    assert.strictEqual(second.destroyed, 1);
    assert.strictEqual(links.conns.size, 0);
  });

  await check('the metadata connection skips a bootstrap that refuses, and reads asking at once share it', async () => {
    const router = routerWith([broker('a@h', '10.0.0.1', 4040)], [range(1, 0, 16, 'a@h')]);
    const links = stubLinks((address) => (address.host === 'boot' ? null : fakeClient()), router);
    const [one, two] = await Promise.all([links._control(), links._control()]);
    assert.strictEqual(one, two);
    assert.strictEqual(links.control, one);
    assert.strictEqual(links.attempts.get('boot:4040'), 1);
    assert.strictEqual(links.attempts.get('10.0.0.1:4040'), 1);
    await links.closeAll();
    assert.strictEqual(one.closed, 1);
  });

  await check('closed links connect to nothing more, metadata included', async () => {
    const links = stubLinks(() => fakeClient());
    await links.closeAll();
    assert.strictEqual(await links.connection({ host: 'x', port: 1 }), null);
    assert.strictEqual(await links._control(), null);
    assert.strictEqual(links.attempts.size, 0, 'no connect was tried');
    assert.strictEqual(links.stats.connect_failures, 0, 'and none counted as failed');
  });

  await check('a metadata connect that finishes after closeAll is closed, not kept', async () => {
    const router = routerWith([], []);
    router.refresh = async () => {};
    const links = new Links(router);
    let made = null;
    links.connect = async () => {
      await tick(50);
      made = fakeClient();
      return made;
    };
    const read = links.refresh('t');
    await tick(10);
    await links.closeAll();
    assert.deepStrictEqual(await read, { error: 'metadata_unavailable' });
    assert.strictEqual(links.control, null);
    assert.strictEqual(made.closed, 1, 'the connection that came after the close was closed');
  });

  await check('a topic whose routes failed is asked again once its backoff ends, not before', async () => {
    const links = new Links(routerWith([], []));
    let reads = 0;
    links.refresh = async () => {
      reads++;
      return { error: 'no_such_topic' };
    };
    const now = 1e15; // ahead of the clock, as a caller's instant may be
    assert.deepStrictEqual(await links.ensureRoutes('gone', now), { error: 'no_such_topic' });
    assert.deepStrictEqual(await links.ensureRoutes('gone', now + 199), { error: 'no_such_topic' });
    assert.strictEqual(reads, 1);
    await links.ensureRoutes('gone', now + 200);
    assert.strictEqual(reads, 2);
  });

  await check('an unreadable cluster state is a refusal: malformed_cluster_state, and the connection kept', async () => {
    const server = await fakeServer((apiKey) => (apiKey === wire.API.clusterState ? [wire.OK, Buffer.alloc(0)] : null));
    try {
      const links = new Links(new Router({ host: '127.0.0.1', port: server.port }), { user: 'u', pass: 'p' });
      assert.deepStrictEqual(await links.refresh('t'), { error: 'malformed_cluster_state' });
      assert.ok(links.control && links.control.socket, 'the metadata connection is kept');
      assert.strictEqual(server.sockets.length, 1, 'and not opened again');
      await links.closeAll();
    } finally {
      server.close();
    }
  });

  await check('a connect that never completes fails at its bound, as a failed connection', async () => {
    const socket = Object.assign(new EventEmitter(), { destroyed: false, destroy() { this.destroyed = true; } });
    const client = new MalachiClient({ host: 'h', port: 1, connectTimeout: 50, createConnection: () => socket });
    const err = await client.connect().then(() => null, (e) => e);
    assert.ok(err && err.transport, 'a transport failure');
    assert.match(err.message, /connect timed out after 50ms/);
    assert.ok(socket.destroyed);
  });

  await check('a request nobody answers fails flagged as a timeout, not a refusal', async () => {
    const server = await fakeServer(() => null);
    try {
      const client = await new MalachiClient({ host: '127.0.0.1', port: server.port, timeout: 100 }).connect('u', 'p');
      const err = await client.clusterState().then(() => null, (e) => e);
      assert.ok(err && err.timeout === true && !err.transport);
      client.close();
    } finally {
      server.close();
    }
  });

  await check('a deliberate close rejects what is pending at once and leaves no timer behind', async () => {
    const server = await fakeServer(() => null);
    try {
      const client = await new MalachiClient({ host: '127.0.0.1', port: server.port, timeout: 60000 }).connect('u', 'p');
      const pending = client.clusterState().then(() => null, (e) => e);
      await tick();
      const started = Date.now();
      client.close();
      const err = await pending;
      assert.ok(err && err.transport, 'rejected as a closed connection');
      assert.ok(Date.now() - started < 1000, 'at once, not at its 60 s timeout');
      assert.strictEqual(client.pending.size, 0);
    } finally {
      server.close();
    }
  });

  await check('a stream gets its frames until forgotten, onClosed once on a drop, and none while opening', async () => {
    let dropOnOpen = false;
    const server = await fakeServer((apiKey, _payload, socket) => {
      if (apiKey !== wire.API.openStream) return null;
      if (dropOnOpen) {
        socket.destroy();
        return null;
      }
      return [wire.OK, Buffer.alloc(0)];
    });
    try {
      const client = await new MalachiClient({ host: '127.0.0.1', port: server.port }).connect('u', 'p');
      const seen = { a: [], b: [] };
      const handlers = (name) => ({
        onPush: () => seen[name].push('push'),
        onRefusal: (reason) => seen[name].push(reason),
        onClosed: () => seen[name].push('closed'),
      });
      const a = await client.openStream(wire.API.openStream, Buffer.alloc(0), handlers('a'));
      const b = await client.openStream(wire.API.openStream, Buffer.alloc(0), handlers('b'));
      const [socket] = server.sockets;
      socket.write(responseFrame(a.correlationId, wire.ERROR, str('unknown_stream')));
      await tick();
      client.forgetStream(a.correlationId);
      socket.write(responseFrame(a.correlationId, wire.ERROR, str('after_forget')));
      await tick();
      // the connection drops with b open and c still opening
      dropOnOpen = true;
      const c = client.openStream(wire.API.openStream, Buffer.alloc(0), handlers('c')).then(() => 'opened', (e) => e.message);
      seen.c = [];
      assert.strictEqual(await c, 'connection closed by server');
      await tick();
      assert.deepStrictEqual(seen, { a: ['unknown_stream'], b: ['closed'], c: [] });
    } finally {
      server.close();
    }
  });

  console.log(`passed ${cases.length} routing cases`);
}

function addresses(json) {
  const input = JSON.parse(json);
  const router = new Router(input.bootstrap);
  router.putClusterState(input.cluster_state);
  for (const routes of input.routes) router.putRoutes(routes);
  const answers = input.queries.map(([topic, n]) => router.addressOf(topic, n));
  console.log(JSON.stringify(answers));
}

// Against a real broker: the routes, the connections, and one producer stream whose pushes and refusals
// must reach its handlers through the correlation id it was opened under.
async function live(topic) {
  const bootstrap = { host: process.env.MALACHI_HOST || '127.0.0.1', port: parseInt(process.env.MALACHI_PORT, 10) };
  const links = new Links(new Router(bootstrap), {
    user: process.env.MALACHI_USER,
    pass: process.env.MALACHI_PASS,
    timeout: 5000,
  });
  const out = { routed: await links.ensureRoutes(topic, 0) };
  out.ranges = links.router.routes(topic).ranges.map((r) => r.range);
  out.connected = await links.connectRanges(topic);

  const routes = links.router.routes(topic);
  const first = routes.ranges[0];
  const { address } = links.router.addressOf(topic, first.range);
  const client = await links.connection(address);
  out.sharedConnection = (await links.connection(address)) === client;

  const pushes = [];
  const refusals = [];
  let pushed = () => {};
  const handlers = {
    onPush: (kind, push) => {
      pushes.push([kind, push]);
      pushed();
    },
    onRefusal: (reason) => {
      refusals.push(reason);
      pushed();
    },
    onClosed: () => {},
  };
  const open = (rangeN, version) =>
    client.openStream(
      wire.API.openStream,
      wire.encodeOpenStreamReq({
        topic,
        range: rangeN,
        routes_version: version,
        codec: 'none',
        window_appends: 4,
        window_bytes: 1 << 20,
        producer_id: null,
        label: 'routing-selftest',
      }),
      handlers,
    );
  let opened = await open(first.range, routes.version).catch((err) => err);
  // an unassigned range answers moved toward the primary it placed; the routes then name it
  if (opened instanceof Error) {
    out.openRefusal = opened.message;
    await links.refresh(topic);
    opened = await open(first.range, links.router.version(topic));
  }
  const stream = wire.decodeOpenStreamResp(opened.payload);
  // a push or refusal that never reaches the handlers fails the run instead of hanging it
  const until = () =>
    new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no push or refusal reached the stream in 5 s')), 5000);
      pushed = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  const batch = wire.encodeBatch([{ tombstone: false, record: { key: 'k', value: 'v', timestamp: 1, headers: {} } }], 'none');

  let next = until();
  client.sendOnStream(wire.API.append, opened.correlationId, wire.encodeAppendReq(stream.stream_id, 0, batch));
  await next;
  // a sequence the stream did not expect ends it: an error at that sequence, in a push
  next = until();
  client.sendOnStream(wire.API.append, opened.correlationId, wire.encodeAppendReq(stream.stream_id, 5, batch));
  await next;
  // and an append after that is refused by an error frame under the same correlation id
  next = until();
  client.sendOnStream(wire.API.append, opened.correlationId, wire.encodeAppendReq(stream.stream_id, 6, batch));
  await next;

  // a topic whose routes cannot be read backs off: asked again at once, it answers the same refusal with
  // no round trip
  const missing = `${topic}_missing`;
  const refreshes = links.stats.refreshes;
  out.missing = [await links.ensureRoutes(missing, 0), await links.ensureRoutes(missing, 0)];
  out.missingRefreshes = links.stats.refreshes - refreshes;
  // a metadata connection that died before the client noticed (its socket still held) fails the read, is
  // replaced, and the read is made again
  links.control.socket.destroy();
  links.router.forget(topic);
  out.afterControlLost = await links.ensureRoutes(topic, 0);

  out.pushes = pushes.map(([kind, push]) => ({ kind, acked_sequence: push.acked_sequence, errors: push.errors }));
  out.refusals = refusals;
  out.stats = links.stats;
  await links.closeAll();
  console.log(JSON.stringify(out));
}

const args = process.argv.slice(2);
if (args[0] === '--addresses') addresses(args[1]);
else if (args[0] === '--live') {
  live(args[1]).catch((err) => {
    console.error(err.stack || err.message);
    process.exit(1);
  });
} else {
  selfTest().catch((err) => {
    console.error(err.stack || err.message);
    process.exit(1);
  });
}
