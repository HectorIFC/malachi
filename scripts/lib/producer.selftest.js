'use strict';

/**
 * The Node producer (./producer.js) against a broker in this process (./testing/fake_broker.js): each case
 * one behaviour of the Elixir producer it mirrors (test/malachi/loadtest/producer_test.exs). ExUnit runs it
 * (test/scripts/producer_js_test.exs), which also runs the producer against the real server.
 *
 *   node scripts/lib/producer.selftest.js              every case
 *   node scripts/lib/producer.selftest.js --live TOPIC  against MALACHI_HOST/PORT: hands over the values
 *                                                       read from stdin (one per line) and prints, as JSON,
 *                                                       the tags acknowledged and the failures
 */

const assert = require('assert');
const wire = require('./wire');
const { Router } = require('./router');
const { Producer, recordBytes, entryBytes } = require('./producer');
const { FakeBroker } = require('./testing/fake_broker');

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));
const entries = (values, key = (v) => v) => values.map((v) => ({ tag: v, record: { key: key(v), value: v } }));
const tags = (events, kind) => events.filter((e) => e.kind === kind).flatMap((e) => e.entries.map((x) => x.tag));
const appended = (broker) => broker.appends.flatMap((a) => a.values);

async function untilTrue(fn, ms = 2000) {
  const deadline = Date.now() + ms;
  while (!fn()) {
    if (Date.now() > deadline) throw new Error(`condition not met in ${ms} ms: ${fn}`);
    await tick(5);
  }
}

function producerOn(broker, opts = {}) {
  return new Producer(new Router(broker.address), { user: 'u', pass: 'p', timeout: 2000 }, opts);
}

async function withBroker(fn, topic = 't', ranges = [[0, 0, 16, 'self']]) {
  const broker = await new FakeBroker().start();
  broker.setRoutes(topic, ranges);
  const producers = [];
  try {
    await fn(broker, (opts) => {
      const p = producerOn(broker, opts);
      producers.push(p);
      return p;
    });
  } finally {
    for (const p of producers) await p.close();
    broker.stop();
  }
}

const cases = [];
const check = (name, fn) => cases.push([name, fn]);

check('every record handed over is acknowledged, under its tag, and lands in order', () =>
  withBroker(async (broker, make) => {
    const p = make({ maxInflight: 4 });
    await p.produce('t', entries(['a', 'b']));
    await p.produce('t', entries(['c']));
    assert.strictEqual(await p.wait(2000), 'idle');
    const events = p.takeEvents();
    assert.deepStrictEqual(tags(events, 'acked'), ['a', 'b', 'c']);
    assert.deepStrictEqual(appended(broker), ['a', 'b', 'c']);
    assert.ok(events.every((e) => Number.isInteger(e.latencyUs) && e.latencyUs >= 0), 'each ack carries its round trip');
  }),
);

check('without lingering each hand-over is its own append; lingering joins them up to linger_bytes', () =>
  withBroker(async (broker, make) => {
    const plain = make({ maxInflight: 8 });
    for (const v of ['a', 'b', 'c']) await plain.produce('t', entries([v]));
    await plain.wait(2000);
    assert.deepStrictEqual(broker.appends.map((a) => a.values), [['a'], ['b'], ['c']]);

    broker.appends = [];
    const unit = entryBytes(entries(['x'])[0]);
    const lingering = make({ maxInflight: 8, lingerMs: 60000, lingerBytes: 3 * unit });
    for (const v of ['x', 'y']) await lingering.produce('t', entries([v]));
    await tick(50);
    assert.deepStrictEqual(broker.appends, [], 'two units wait for the third');
    await lingering.produce('t', entries(['z']));
    await lingering.wait(2000);
    assert.deepStrictEqual(broker.appends.map((a) => a.values), [['x', 'y', 'z']]);
  }),
);

check('a lingering record goes out once it has waited linger_ms, with nothing more handed over', () =>
  withBroker(async (broker, make) => {
    const p = make({ maxInflight: 8, lingerMs: 80, lingerBytes: 1 << 20 });
    const started = Date.now();
    await p.produce('t', entries(['a']));
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.ok(Date.now() - started >= 70, 'it waited its linger');
    assert.deepStrictEqual(appended(broker), ['a']);
  }),
);

check('an append takes whole hand-overs: one never splits, however the bytes fall', () =>
  withBroker(async (broker, make) => {
    const unit = entryBytes(entries(['a'])[0]);
    const p = make({ maxInflight: 8, lingerMs: 100, lingerBytes: 3 * unit });
    await p.produce('t', entries(['a', 'b']));
    await p.produce('t', entries(['c', 'd']));
    assert.strictEqual(await p.wait(2000), 'idle');
    // the first unit alone, since the second would go past linger_bytes; then the second, whole, once it
    // has waited its linger
    assert.deepStrictEqual(broker.appends.map((a) => a.values), [['a', 'b'], ['c', 'd']]);
  }),
);

check('a full buffer behind a full window leaves no room, until the window opens', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 1, lingerBytes: 1 });
    await p.produce('t', entries(['a']));
    await untilTrue(() => broker.appends.length === 1);
    await p.produce('t', entries(['b']));
    await tick(30);
    assert.strictEqual(p.room(), false);
    assert.deepStrictEqual(appended(broker), ['a'], 'one in flight, the window full');
    broker.appendPolicy = () => 'ack';
    broker.ack(broker.appends[0].stream, 1);
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(appended(broker), ['a', 'b']);
  }),
);

check('appends in flight stay under the pipeline and under the window the broker grants', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 3 });
    for (const v of ['a', 'b', 'c', 'd', 'e', 'f']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 3);
    await tick(50);
    assert.strictEqual(broker.appends.length, 3, 'the pipeline');
    // an ack that grants a window wider than the pipeline: the pipeline still holds three in flight
    broker.ack(broker.appends[0].stream, 1);
    await untilTrue(() => broker.appends.length === 4);
    await tick(50);
    assert.strictEqual(broker.appends.length, 4, 'one answered, one more sent');
    // and a window narrower than the pipeline holds one
    broker.window.appends = 1;
    broker.ack(broker.appends[0].stream, 4);
    await untilTrue(() => broker.appends.length === 5);
    await tick(50);
    assert.strictEqual(broker.appends.length, 5, 'the window granted now holds one');
  }),
);

check('an append the byte window cannot take waits for room, rather than going', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    broker.window.bytes = entryBytes(entries(['a'])[0]);
    const p = make({ maxInflight: 8 });
    await p.produce('t', entries(['a']));
    await p.produce('t', entries(['b']));
    await untilTrue(() => broker.appends.length === 1);
    await tick(50);
    assert.deepStrictEqual(appended(broker), ['a']);
    broker.ack(broker.appends[0].stream, 1);
    await untilTrue(() => broker.appends.length === 2);
  }),
);

check('a stream told it moved: what was in flight is answered, and the rest goes to the next stream', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 1 });
    await p.produce('t', entries(['a']));
    await p.produce('t', entries(['b']));
    await untilTrue(() => broker.appends.length === 1);
    const first = broker.appends[0].stream;
    broker.moved(first);
    broker.ack(first, 1);
    broker.appendPolicy = () => 'ack';
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['a', 'b']);
    assert.notStrictEqual(broker.appends[1].stream, first);
    assert.strictEqual(broker.unknownStream, 0, 'nothing more was sent on the stream that moved');
    assert.strictEqual(p.statsNow().moved, 1);
  }),
);

check('records resent after a refused tail keep the order they were handed over in', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    for (const v of ['a', 'b']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 2);
    const first = broker.appends[0].stream;
    // the stream moved; both appends reached it after the move and are refused unknown_stream
    broker.moved(first);
    await untilTrue(() => p.statsNow().moved === 1);
    await p.produce('t', entries(['c']));
    // time enough for a range that did not wait for the drain to open a stream and send c first
    await tick(100);
    broker.refuse(first, 'unknown_stream');
    broker.refuse(first, 'unknown_stream');
    broker.appendPolicy = () => 'ack';
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['a', 'b', 'c']);
    const later = broker.appends.filter((a) => a.stream !== first).flatMap((a) => a.values);
    assert.deepStrictEqual(later, ['a', 'b', 'c']);
  }),
);

check('after stopSending, buffered records are dropped and an append answered unknown_stream is not resent', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 1 });
    await p.produce('t', entries(['a']));
    await p.produce('t', entries(['b']));
    await untilTrue(() => broker.appends.length === 1);
    const first = broker.appends[0].stream;
    await p.stopSending();
    broker.moved(first);
    broker.refuse(first, 'unknown_stream');
    await untilTrue(() => !p.inFlight());
    assert.ok(p.idle());
    assert.deepStrictEqual(p.takeEvents(), []);
    assert.deepStrictEqual(appended(broker), ['a']);
  }),
);

// `not_primary` and `sealed` are the broker's answers to an append it stored nothing of because its range is
// written elsewhere now (a roll placed the successor on another node): routing, sent again on a new stream,
// never a failure.
for (const reason of ['not_primary', 'sealed']) {
  check(`an append refused ${reason} goes out again on the next stream, acknowledged once, with no failure`, () =>
    withBroker(async (broker, make) => {
      broker.appendPolicy = () => 'silent';
      const p = make({ maxInflight: 4 });
      await p.produce('t', entries(['a']));
      await untilTrue(() => broker.appends.length === 1);
      const first = broker.appends[0].stream;
      broker.appendPolicy = () => 'ack';
      broker.ack(first, 1, [{ sequence: 0, reason }]);
      assert.strictEqual(await p.wait(2000), 'idle');
      await p.produce('t', entries(['b']));
      assert.strictEqual(await p.wait(2000), 'idle');
      const events = p.takeEvents();
      assert.deepStrictEqual(tags(events, 'acked'), ['a', 'b']);
      assert.deepStrictEqual(tags(events, 'failed'), []);
      // sent twice, on the first stream and then on the one opened after it, and the stream counted redirected
      assert.deepStrictEqual(broker.appends.map((a) => [a.stream === first, a.sequence, a.values]), [
        [true, 0, ['a']],
        [false, 0, ['a']],
        [false, 1, ['b']],
      ]);
      assert.strictEqual(p.statsNow().redirects, 1);
    }),
  );
}

check('routing refusals on several appends in flight resend them in the order they were sent, after the move', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    for (const v of ['a', 'b', 'c']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 3);
    const first = broker.appends[0].stream;
    // the broker's move first, then each append refused, as a broker sends them
    broker.moved(first);
    broker.appendPolicy = () => 'ack';
    for (const sequence of [0, 1, 2]) broker.ack(first, sequence + 1, [{ sequence, reason: 'not_primary' }]);
    assert.strictEqual(await p.wait(2000), 'idle');
    const events = p.takeEvents();
    assert.deepStrictEqual(tags(events, 'acked'), ['a', 'b', 'c']);
    assert.deepStrictEqual(tags(events, 'failed'), []);
    const later = broker.appends.filter((a) => a.stream !== first).flatMap((a) => a.values);
    assert.deepStrictEqual(later, ['a', 'b', 'c']);
  }),
);

check('an append refused for a reason that is not routing still fails, and is not sent again', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make();
    await p.produce('t', entries(['a']));
    await untilTrue(() => broker.appends.length === 1);
    broker.ack(broker.appends[0].stream, 1, [{ sequence: 0, reason: 'no_quorum' }]);
    await untilTrue(() => !p.inFlight());
    const events = p.takeEvents();
    assert.deepStrictEqual(tags(events, 'failed'), ['a']);
    assert.deepStrictEqual(tags(events, 'acked'), []);
    assert.ok(events.every((e) => e.kind !== 'failed' || e.reason === 'no_quorum'));
    assert.deepStrictEqual(appended(broker), ['a']);
  }),
);

check('a refusal that names no append fails every append in flight on the stream', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    for (const v of ['a', 'b']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 2);
    broker.refuse(broker.appends[0].stream, 'permission_denied');
    await untilTrue(() => !p.inFlight());
    const events = p.takeEvents();
    assert.deepStrictEqual(tags(events, 'failed'), ['a', 'b']);
    assert.ok(events.every((e) => e.reason === 'permission_denied'));
  }),
);

check('an error at or above acked_sequence ends the stream: a gap fails that append, a repeat waits for its original', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    for (const v of ['a', 'b']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 2);
    const first = broker.appends[0].stream;
    broker.ack(first, 0, [{ sequence: 1, reason: 'sequence_gap' }]);
    await untilTrue(() => p.events.length === 1);
    assert.deepStrictEqual(p.events.map((e) => [e.kind, e.reason]), [['failed', 'sequence_gap']]);
    broker.ack(first, 1);
    await untilTrue(() => !p.inFlight());
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['a']);

    for (const v of ['c', 'd']) await p.produce('t', entries([v]));
    await untilTrue(() => broker.appends.length === 4);
    const second = broker.appends[2].stream;
    assert.notStrictEqual(second, first, 'the range opened a new stream');
    broker.ack(second, 0, [{ sequence: 1, reason: 'sequence_repeat' }]);
    await tick(50);
    assert.deepStrictEqual(p.takeEvents(), [], 'a repeat fails nothing');
    broker.ack(second, 2);
    await untilTrue(() => !p.inFlight());
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['c', 'd']);
  }),
);

check('a dropped connection fails what was in flight as connection_lost, and the next records reconnect', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    await p.produce('t', entries(['a']));
    await untilTrue(() => broker.appends.length === 1);
    broker.dropConnections();
    await untilTrue(() => p.events.length === 1);
    assert.deepStrictEqual(p.takeEvents().map((e) => [e.kind, e.reason]), [['failed', 'connection_lost']]);
    assert.strictEqual(p.statsNow().dropped, 1);
    broker.appendPolicy = () => 'ack';
    await tick(250); // the range's backoff
    await p.produce('t', entries(['b']));
    assert.strictEqual(await p.wait(3000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['b']);
  }),
);

check('a drop that ends a topic hold sends what waited behind it, with nothing else to wake it', () =>
  withBroker(async (broker, make) => {
    broker.appendPolicy = () => 'silent';
    const p = make({ maxInflight: 4 });
    await p.produce('t', entries(['a']));
    await untilTrue(() => broker.appends.length === 1);
    // the stream moved with 'a' unanswered: the topic holds while it drains, and 'b' waits behind it
    broker.moved(broker.appends[0].stream);
    await untilTrue(() => p.statsNow().moved === 1);
    broker.appendPolicy = () => 'ack';
    await p.produce('t', entries(['b']));
    await tick(50);
    assert.deepStrictEqual(appended(broker), ['a'], 'held');
    // the draining stream's connection drops: 'a' is lost, the hold ends, and 'b' goes once its range's
    // backoff ends
    broker.dropConnections();
    assert.strictEqual(await p.wait(3000), 'idle');
    const events = p.takeEvents().map((e) => [e.kind, e.entries.map((x) => x.tag), e.reason]);
    assert.deepStrictEqual(events, [
      ['failed', ['a'], 'connection_lost'],
      ['acked', ['b'], undefined],
    ]);
  }),
);

check('a move pushed in the same read as the open answer is taken, after the answer', () =>
  withBroker(async (broker, make) => {
    let opens = 0;
    broker.openPolicy = () => (opens++ === 0 ? 'moved' : 'ok');
    const p = make({});
    await p.produce('t', entries(['a']));
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['a']);
    assert.strictEqual(p.statsNow().moved, 1, 'the move was not dropped');
    assert.strictEqual(broker.unknownStream, 0, 'nothing was sent on the stream that moved');
    assert.strictEqual(broker.opens.length, 2, 'one open more, for the stream after the move');
  }),
);

check('prepare names only its own round refusals, not an event queued before it', () =>
  withBroker(async (broker, make) => {
    const p = make({});
    await p.produce('nope', entries(['a']));
    assert.strictEqual(p.events.length, 1, 'a failure the caller has not taken');
    broker.openPolicy = () => 'silent';
    assert.deepStrictEqual(await p.prepare('t', 200), { error: 'open_timeout' });
  }),
);

check('an open against stale routes reads them again and opens where they say', () =>
  withBroker(async (broker, make) => {
    let refused = 0;
    broker.openPolicy = () => (refused++ === 0 ? { refuse: 'stale_routes' } : 'ok');
    const p = make({});
    await p.produce('t', entries(['a']));
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked'), ['a']);
    assert.strictEqual(p.statsNow().redirects, 1);
    assert.strictEqual(p.statsNow().refreshes, 2);
  }),
);

check('a range redirected time after time backs off from its fifth redirect', () =>
  withBroker(async (broker, make) => {
    broker.openPolicy = () => ({ refuse: 'moved' });
    const p = make({});
    await p.produce('t', entries(['a']));
    await untilTrue(() => broker.opens.length >= 5);
    const lane = [...p.lanes.values()][0];
    await untilTrue(() => lane.redirects >= 5);
    assert.ok(typeof lane.retryAt === 'number', 'a backoff from the fifth');
    const opens = broker.opens.length;
    await tick(20);
    assert.ok(broker.opens.length <= opens + 1, 'not opened again at once');
  }),
);

check('an open refused for another reason fails the range records with that reason', () =>
  withBroker(async (broker, make) => {
    broker.openPolicy = () => ({ refuse: 'permission_denied' });
    const p = make({});
    await p.produce('t', entries(['a']));
    await untilTrue(() => p.events.length === 1);
    assert.deepStrictEqual(p.takeEvents().map((e) => [e.kind, e.reason, e.entries.length]), [['failed', 'permission_denied', 1]]);
  }),
);

check('records for a topic the server does not know fail with its refusal', () =>
  withBroker(async (broker, make) => {
    const p = make({});
    await p.produce('nope', entries(['a']));
    assert.deepStrictEqual(p.takeEvents().map((e) => [e.kind, e.reason]), [['failed', 'no_such_topic']]);
  }),
);

check('a record no active range covers is read again once, then fails as no_range', () =>
  withBroker(
    async (broker, make) => {
      const p = make({});
      const uncovered = [...Array(64).keys()].map((i) => `k${i}`).find((k) => require('./keyspace').positionOf(k, 16) >= 8);
      await p.produce('t', entries([uncovered]));
      assert.deepStrictEqual(p.takeEvents().map((e) => [e.kind, e.reason]), [['failed', 'no_range']]);
      assert.strictEqual(p.statsNow().refreshes, 2, 'read once more before failing');
    },
    't',
    [[0, 0, 8, 'self']],
  ),
);

check('records buffered for a range a split retired go where the routes read now send them', () =>
  withBroker(async (broker, make) => {
    // the range splits before its first open is answered: the open is refused as moved, the routes read
    // again no longer hold it, and its records go to the ranges they now hash into
    broker.openPolicy = (req) => {
      if (req.range !== 0) return 'ok';
      broker.setRoutes('t', [
        [1, 0, 8, 'self'],
        [2, 8, 16, 'self'],
      ]);
      return { refuse: 'moved' };
    };
    const p = make({});
    await p.produce('t', entries(['a', 'b', 'c', 'd'], (v) => `key-${v}`));
    assert.strictEqual(await p.wait(2000), 'idle');
    assert.deepStrictEqual(tags(p.takeEvents(), 'acked').sort(), ['a', 'b', 'c', 'd']);
    assert.deepStrictEqual(broker.opens.filter((o) => o.range === 0).length, 1, 'never opened on the retired range again');
    assert.ok(broker.opens.some((o) => o.range !== 0));
  }),
);

check('across a split, a parent range in backoff hands its records over before its children send', () =>
  withBroker(
    async (broker, make) => {
      // the parent's primary advertises no address: its record waits out a backoff
      broker.extraBrokers = [{ id: 'mute@h', host: '', port: 0, status: 'alive' }];
      const p = make({});
      await p.produce('t', entries(['a'], () => 'same'));
      assert.ok(typeof [...p.lanes.values()][0].retryAt === 'number');
      // the range splits meanwhile; a record for the same key goes to the child that now holds it
      broker.setRoutes('t', [
        [1, 0, 8, 'self'],
        [2, 8, 16, 'self'],
      ]);
      await p.produce('t', entries(['b'], () => 'same'));
      assert.strictEqual(await p.wait(2000), 'idle');
      assert.deepStrictEqual(appended(broker), ['a', 'b'], 'the parent record first, its backoff moot');
    },
    't',
    [[0, 0, 16, 'mute@h']],
  ),
);

check('a primary that advertises no address, in a cluster of several, is a redirect: its records wait', () =>
  withBroker(
    async (broker, make) => {
      broker.extraBrokers = [{ id: 'mute@h', host: '', port: 0, status: 'alive' }];
      const p = make({});
      await p.produce('t', entries(['a']));
      assert.deepStrictEqual(p.takeEvents(), [], 'nothing failed');
      const lane = [...p.lanes.values()][0];
      assert.strictEqual(lane.bufferBytes > 0, true);
      assert.ok(typeof lane.retryAt === 'number' && p.statsNow().redirects === 1);
    },
    't',
    [[0, 0, 16, 'mute@h']],
  ),
);

check('a broker that refuses the connection is retried after a backoff, its routes read again', () =>
  withBroker(
    async (broker, make) => {
      broker.extraBrokers = [{ id: 'gone@h', host: '127.0.0.1', port: 1, status: 'alive' }];
      const p = make({});
      await p.produce('t', entries(['a']));
      const lane = [...p.lanes.values()][0];
      assert.ok(typeof lane.retryAt === 'number');
      assert.strictEqual(p.statsNow().connect_failures, 1);
      assert.strictEqual(p.router.routes('t'), null, 'the routes are read again before the next open');
      assert.deepStrictEqual(p.takeEvents(), []);
    },
    't',
    [[0, 0, 16, 'gone@h']],
  ),
);

check('prepare opens a stream on every active range before any record', () =>
  withBroker(
    async (broker, make) => {
      const p = make({});
      assert.deepStrictEqual(await p.prepare('t', 2000), { ok: true });
      assert.deepStrictEqual(broker.opens.map((o) => o.range).sort(), [1, 2]);
      assert.strictEqual(broker.appends.length, 0);
    },
    't',
    [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ],
  ),
);

check('prepare names the refusal that kept a range closed, a timeout, or the topic refusal', () =>
  withBroker(async (broker, make) => {
    broker.openPolicy = () => ({ refuse: 'permission_denied' });
    assert.deepStrictEqual(await make({}).prepare('t', 300), { error: 'permission_denied' });
    broker.openPolicy = () => 'silent';
    assert.deepStrictEqual(await make({}).prepare('t', 200), { error: 'open_timeout' });
    assert.deepStrictEqual(await make({}).prepare('nope', 200), { error: 'no_such_topic' });
  }),
);

check('a record counts what it takes in a batch, which the broker holds against the window', () => {
  const samples = [
    { tag: 1, record: { key: 'k1', value: 'v' } },
    { tag: 2, record: { key: null, value: Buffer.alloc(300) } },
    { tag: 3, record: { key: 'ключ', value: 'значение', headers: { trace: 'abc', h: '' } } },
  ];
  for (const e of samples) {
    const batch = wire.encodeBatch([{ tombstone: false, record: { ...e.record, timestamp: 1 } }], 'none');
    assert.strictEqual(entryBytes(e), wire.batchHeader(batch).inflated_size, `record ${e.tag}`);
  }
  assert.strictEqual(recordBytes(1, 2), entryBytes({ record: { key: 'kk', value: 'v' } }));
});

check('options are checked', () => {
  const router = new Router({ host: 'h', port: 1 });
  assert.throws(() => new Producer(router, {}, { maxInflight: 0 }), /maxInflight/);
  assert.throws(() => new Producer(router, {}, { lingerMs: -1 }), /lingerMs/);
  assert.throws(() => new Producer(router, {}, { lingerBytes: 1.5 }), /lingerBytes/);
});

async function selfTest() {
  for (const [name, fn] of cases) {
    try {
      await fn();
    } catch (err) {
      err.message = `${name}: ${err.message}`;
      throw err;
    }
  }
  console.log(`passed ${cases.length} producer cases`);
}

// Against the real server: hands over every value of stdin, three at a time, and prints the outcomes.
async function live(topic) {
  const values = require('fs').readFileSync(0, 'utf8').split('\n').filter((v) => v !== '');
  const bootstrap = { host: process.env.MALACHI_HOST || '127.0.0.1', port: parseInt(process.env.MALACHI_PORT, 10) };
  const p = new Producer(new Router(bootstrap), { user: process.env.MALACHI_USER, pass: process.env.MALACHI_PASS }, { maxInflight: 8, label: 'producer-selftest' });
  const prepared = await p.prepare(topic, 10000);
  for (let i = 0; i < values.length; i += 3) {
    await p.produce(topic, entries(values.slice(i, i + 3), (v) => `k${v}`));
    await p.wait(10000, (x) => x.room());
    // a pause now and then, so a split the test makes lands while records are still being handed over
    if (i % 60 === 0) await tick(10);
  }
  const waited = await p.wait(20000);
  const events = p.takeEvents();
  console.log(JSON.stringify({ prepared, waited, acked: tags(events, 'acked'), failed: events.filter((e) => e.kind === 'failed').map((e) => e.reason), stats: p.statsNow() }));
  await p.close();
}

const args = process.argv.slice(2);
(args[0] === '--live' ? live(args[1]) : selfTest()).catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
