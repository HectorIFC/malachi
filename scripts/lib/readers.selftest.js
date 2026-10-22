'use strict';

/**
 * The Node readers (./read_lanes.js, ./fetcher.js, ./consumer.js) against a broker in this process
 * (./testing/fake_broker.js): each case one behaviour of the Elixir readers they mirror
 * (test/malachi/loadtest/consumer_test.exs). ExUnit runs it (test/scripts/readers_js_test.exs), which also
 * runs both readers against the real server.
 *
 *   node scripts/lib/readers.selftest.js               every case
 *   node scripts/lib/readers.selftest.js --live TOPIC   against MALACHI_HOST/PORT: reads TOPIC whole with a
 *                                                       fetcher and with a consumer, printing what each read
 */

const assert = require('assert');
const { Router } = require('./router');
const { Fetcher } = require('./fetcher');
const { Consumer } = require('./consumer');
const readLanes = require('./read_lanes');
const { FakeBroker } = require('./testing/fake_broker');

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));
const conn = { user: 'u', pass: 'p', timeout: 2000 };

async function untilTrue(fn, ms = 2000) {
  const deadline = Date.now() + ms;
  while (!fn()) {
    if (Date.now() > deadline) throw new Error(`condition not met in ${ms} ms: ${fn}`);
    await tick(5);
  }
}

async function withBroker(fn, ranges = [[0, 0, 16, 'self']]) {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', ranges);
  const readers = [];
  try {
    await fn(broker, (make) => {
      const reader = make(new Router(broker.address));
      readers.push(reader);
      return reader;
    });
  } finally {
    for (const r of readers) await r.close();
    broker.stop();
  }
}

const records = (events) => events.filter((e) => e.kind === 'records').reduce((acc, e) => acc + e.count, 0);
const cases = [];
const check = (name, fn) => cases.push([name, fn]);

// ---- read_lanes ----

const lane = (block, start) => ({ block, start });
const routes = (...ranges) => ranges.map(([range, keyStart, keyEnd]) => ({ range, keyStart, keyEnd }));

check('a split child starts where its parent was read', () => {
  const lanes = new Map([[readLanes.laneKey('t', 0), lane([0, 16], { position: [0, 7] })]]);
  const retired = new Map();
  readLanes.reconcile(lanes, retired, 't', routes([1, 0, 8], [2, 8, 16]), () => true, lane);
  assert.deepStrictEqual(lanes.get(readLanes.laneKey('t', 1)).start, { position: [0, 7] });
  assert.deepStrictEqual(lanes.get(readLanes.laneKey('t', 2)).start, { position: [0, 7] });
  assert.ok(retired.has(readLanes.laneKey('t', 0)));
});

check('a merge back into a block read before its split starts where the nearest ancestor holding it was', () => {
  const retired = new Map([
    [readLanes.laneKey('t', 0), lane([0, 16], { position: [0, 3] })],
    [readLanes.laneKey('t', 1), lane([0, 8], { position: [1, 9] })],
    [readLanes.laneKey('t', 2), lane([8, 16], { position: [1, 4] })],
  ]);
  const lanes = new Map();
  readLanes.reconcile(lanes, retired, 't', routes([3, 0, 16]), () => true, lane);
  // range 0 is the only one whose block holds [0, 16]
  assert.deepStrictEqual(lanes.get(readLanes.laneKey('t', 3)).start, { position: [0, 3] });
});

check('a merge of two ranges read as they are starts from the start of its history', () => {
  const lanes = new Map([
    [readLanes.laneKey('t', 1), lane([0, 8], { position: [0, 5] })],
    [readLanes.laneKey('t', 2), lane([8, 16], { position: [0, 6] })],
  ]);
  readLanes.reconcile(lanes, new Map(), 't', routes([3, 0, 16]), () => true, lane);
  assert.strictEqual(lanes.get(readLanes.laneKey('t', 3)).start, 'earliest');
});

check('the nearest ancestor is the one the topic made last, and another topic says nothing', () => {
  const retired = new Map([
    [readLanes.laneKey('t', 0), lane([0, 16], { position: [0, 1] })],
    [readLanes.laneKey('t', 1), lane([0, 8], { position: [1, 2] })],
    [readLanes.laneKey('u', 9), lane([0, 16], { position: [0, 99] })],
  ]);
  const lanes = new Map();
  readLanes.reconcile(lanes, retired, 't', routes([4, 0, 4]), () => true, lane);
  assert.deepStrictEqual(lanes.get(readLanes.laneKey('t', 4)).start, { position: [1, 2] });
});

check('a lane that may not retire yet stays, and a stopped start carries to a split child', () => {
  const lanes = new Map([
    [readLanes.laneKey('t', 0), lane([0, 8], 'stopped')],
    [readLanes.laneKey('t', 5), lane([8, 16], 'earliest')],
  ]);
  const retired = new Map();
  readLanes.reconcile(lanes, retired, 't', routes([1, 0, 4], [2, 4, 8]), (l) => l.start === 'stopped', lane);
  assert.strictEqual(lanes.get(readLanes.laneKey('t', 1)).start, 'stopped');
  assert.ok(lanes.has(readLanes.laneKey('t', 5)), 'not retirable: kept');
});

// ---- fetcher ----

check('a fetcher pages every range in turn, then starts each over (restart) or waits at its end (wait)', () =>
  withBroker(
    async (broker, make) => {
      broker.write('t', 1, ['a', 'b']);
      broker.write('t', 2, ['c']);
      const f = make((router) => new Fetcher(router, conn, { max: 10 }));
      assert.deepStrictEqual(await f.prepare('t'), { ok: true });
      const answers = [];
      for (let i = 0; i < 4; i++) answers.push(await f.fetch('t'));
      assert.deepStrictEqual(answers.map((a) => [a.kind, a.count]), [['ok', 2], ['ok', 1], ['ok', 0], ['ok', 0]]);
      assert.ok(answers.every((a) => Number.isInteger(a.latencyUs)));
      // after an empty page each range goes back to its start
      const starts = broker.fetches.map((x) => [x.range, x.start]);
      assert.deepStrictEqual(starts.slice(2), [[1, { position: [0, 2] }], [2, { position: [0, 1] }]]);
      await f.fetch('t');
      assert.deepStrictEqual(broker.fetches[4].start, 'earliest');

      const waiting = make((router) => new Fetcher(router, conn, { max: 10, atTail: 'wait' }));
      broker.fetches = [];
      for (let i = 0; i < 6; i++) await waiting.fetch('t');
      assert.deepStrictEqual(broker.fetches[4].start, { position: [0, 2] }, 'waits where it reached');
    },
    [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ],
  ),
);

check('fetchPage gives the values with the range, its block, the active ranges and the positions', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['x', 'y']);
    const f = make((router) => new Fetcher(router, conn, { max: 1 }));
    const first = await f.fetchPage('t');
    assert.strictEqual(first.kind, 'page');
    const { values, ...shape } = first.page;
    assert.deepStrictEqual(values.map(String), ['x']);
    assert.deepStrictEqual(shape, { range: 0, active: [0], block: [0, 16], size: 16, from: 'earliest', to: [0, 1] });
    const second = await f.fetchPage('t');
    assert.deepStrictEqual(second.page.from, { position: [0, 1] });
  }),
);

check('a fetch refused is the error, a redirect reads the routes again, a drop is a drop and the next fetch reads', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['x']);
    const f = make((router) => new Fetcher(router, conn));
    broker.fetchPolicy = () => ({ refuse: 'permission_denied' });
    assert.deepStrictEqual(await f.fetch('t'), { kind: 'error', reason: 'permission_denied' });
    broker.fetchPolicy = () => ({ refuse: 'stale_routes' });
    assert.deepStrictEqual(await f.fetch('t'), { kind: 'redirected' });
    assert.strictEqual(f.statsNow().redirects, 1);
    broker.fetchPolicy = () => 'drop';
    assert.deepStrictEqual(await f.fetch('t'), { kind: 'dropped' });
    assert.strictEqual(f.statsNow().dropped, 1);
    broker.fetchPolicy = () => 'ok';
    assert.strictEqual((await f.fetch('t')).count, 1);
  }),
);

check('a primary with no address is a redirect, then held; a broker that takes no connection is connect_failed', () =>
  withBroker(
    async (broker, make) => {
      broker.extraBrokers = [
        { id: 'mute@h', host: '', port: 0, status: 'alive' },
        { id: 'gone@h', host: '127.0.0.1', port: 1, status: 'alive' },
      ];
      const f = make((router) => new Fetcher(router, conn));
      assert.deepStrictEqual(await f.fetch('t'), { kind: 'redirected' });
      const held = await f.fetch('t');
      assert.strictEqual(held.kind, 'held');
      assert.ok(held.ms > 0 && held.ms <= 200);

      broker.setRoutes('t', [[0, 0, 16, 'gone@h']]);
      await tick(250);
      assert.deepStrictEqual(await f.fetch('t'), { kind: 'error', reason: 'connect_failed' });
      assert.strictEqual(f.statsNow().connect_failures, 1);
    },
    [[0, 0, 16, 'mute@h']],
  ),
);

check('the records a page says the fetcher already had are not counted, nor given', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a', 'b', 'c']);
    broker.fetchPolicy = () => ({ skip: 1 });
    const f = make((router) => new Fetcher(router, conn));
    assert.strictEqual((await f.fetch('t')).count, 2);
    const fresh = make((router) => new Fetcher(router, conn));
    assert.deepStrictEqual((await fresh.fetchPage('t')).page.values.map(String), ['b', 'c']);
  }),
);

check('fetches asked at once are answered one at a time, in order', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a', 'b', 'c']);
    const f = make((router) => new Fetcher(router, conn, { max: 1, atTail: 'wait' }));
    const pages = await Promise.all([f.fetchPage('t'), f.fetchPage('t'), f.fetchPage('t')]);
    assert.deepStrictEqual(pages.map((p) => String(p.page.values[0])), ['a', 'b', 'c']);
  }),
);

check('a fetch that waited in the queue goes by the clock it runs at, not the one it was asked at', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a']);
    const f = make((router) => new Fetcher(router, conn));
    await f.prepare('t');
    broker.fetchPolicy = () => ({ delayMs: 300 });
    const slow = f.fetch('t');
    await tick(20);
    // the routes held back for 100 ms while the slow fetch is out; the next fetch is asked inside the hold
    // and runs after it
    f.router.forget('t');
    f.links.unroutable.set('t', { until: require('./links').monoMs() + 100, reason: 'held' });
    const queued = f.fetch('t');
    broker.fetchPolicy = () => 'ok';
    await slow;
    assert.strictEqual((await queued).kind, 'ok');
  }),
);

check('a closed fetcher answers what is still queued as closed, and opens no connection for it', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a']);
    const f = make((router) => new Fetcher(router, conn));
    await f.prepare('t');
    broker.fetchPolicy = () => ({ delayMs: 100 });
    const out = f.fetch('t');
    const queued = f.fetch('t');
    await f.close();
    const sockets = broker.sockets.length;
    await out.catch(() => null);
    assert.deepStrictEqual(await queued, { kind: 'closed' });
    await tick(50);
    assert.strictEqual(broker.sockets.length, sockets, 'no connection opened after close');
  }),
);

check('fetcher options are checked', () => {
  const router = new Router({ host: 'h', port: 1 });
  assert.throws(() => new Fetcher(router, conn, { atTail: 'never' }), /atTail/);
  assert.throws(() => new Fetcher(router, conn, { max: 0 }), /max/);
});

// ---- consumer ----

check('a consumer opens a stream on every range, counts each page, and acks its end with no new window', () =>
  withBroker(
    async (broker, make) => {
      broker.write('t', 1, ['a', 'b']);
      broker.write('t', 2, ['c']);
      const c = make((router) => new Consumer(router, conn, { window: 10, max: 10 }));
      assert.deepStrictEqual(await c.subscribe('t', 2000), { ok: true });
      await untilTrue(() => records(c.events) === 3);
      await untilTrue(() => broker.acks.length === 2);
      assert.deepStrictEqual(broker.acks.map((a) => [a.position, a.window]).sort(), [
        [[0, 1], 0],
        [[0, 2], 0],
      ]);
    },
    [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ],
  ),
);

check('the records a page says the reader already had are not counted', () =>
  withBroker(async (broker, make) => {
    const c = make((router) => new Consumer(router, conn));
    await c.subscribe('t', 2000);
    broker.write('t', 0, ['a', 'b', 'c']);
    broker.pushRecords(broker.consumes[0].id, 0, 10, 2);
    await untilTrue(() => c.events.length === 1);
    assert.deepStrictEqual(c.events.map((e) => [e.kind, e.count]), [['records', 1]]);
  }),
);

check('a split moves the stream: the children open from where the parent was read, the parent not again', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a', 'b']);
    const c = make((router) => new Consumer(router, conn));
    await c.subscribe('t', 2000);
    await untilTrue(() => records(c.events) === 2);
    broker.setRoutes('t', [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ]);
    broker.moved(broker.consumes[0].id, 'split');
    await untilTrue(() => broker.consumes.length === 3);
    assert.deepStrictEqual(
      broker.consumes.slice(1).map((x) => [x.range, x.start]).sort(),
      [
        [1, { position: [0, 2] }],
        [2, { position: [0, 2] }],
      ],
    );
    assert.strictEqual(c.statsNow().moved, 1);
  }),
);

check('a dropped connection reads on from where it stopped', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a', 'b']);
    const c = make((router) => new Consumer(router, conn));
    await c.subscribe('t', 2000);
    await untilTrue(() => records(c.events) === 2);
    broker.dropConnections();
    await untilTrue(() => broker.consumes.length === 2, 3000);
    assert.deepStrictEqual(broker.consumes[1].start, { position: [0, 2] });
    assert.strictEqual(c.statsNow().dropped, 1);
  }),
);

check('a stream the broker refuses fails once: stopped, it is not read again, nor a range a split makes of it', () =>
  withBroker(async (broker, make) => {
    const c = make((router) => new Consumer(router, conn, { onRefusal: 'stop' }));
    await c.subscribe('t', 2000);
    broker.refuse(broker.consumes[0].id, 'permission_denied');
    await untilTrue(() => c.events.length === 1);
    assert.deepStrictEqual(c.takeEvents().map((e) => [e.kind, e.reason]), [['failed', 'permission_denied']]);
    broker.setRoutes('t', [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ]);
    c.router.forget('t');
    await c.start('t');
    await tick(300);
    assert.strictEqual(broker.consumes.length, 1, 'neither the range nor its children opened again');
  }),
);

check('a stream the broker refuses, retried, opens again after a backoff', () =>
  withBroker(async (broker, make) => {
    const c = make((router) => new Consumer(router, conn));
    await c.subscribe('t', 2000);
    broker.refuse(broker.consumes[0].id, 'overloaded');
    await untilTrue(() => broker.consumes.length === 2, 3000);
    assert.deepStrictEqual(c.takeEvents().map((e) => [e.kind, e.reason]), [['failed', 'overloaded']]);
  }),
);

check('an open against stale routes reads them again and opens; one refused otherwise is the refusal', () =>
  withBroker(async (broker, make) => {
    let opens = 0;
    broker.consumePolicy = () => (opens++ === 0 ? { refuse: 'stale_routes' } : 'ok');
    const c = make((router) => new Consumer(router, conn));
    assert.deepStrictEqual(await c.subscribe('t', 2000), { ok: true });
    assert.strictEqual(c.statsNow().redirects, 1);

    broker.consumePolicy = () => ({ refuse: 'permission_denied' });
    const refused = make((router) => new Consumer(router, conn, { onRefusal: 'stop' }));
    assert.deepStrictEqual(await refused.subscribe('t', 2000), { error: 'permission_denied' });
    broker.consumePolicy = () => 'silent';
    const silent = make((router) => new Consumer(router, conn));
    assert.deepStrictEqual(await silent.subscribe('t', 200), { error: 'open_timeout' });
  }),
);

check('a page pushed in the same read as the open answer is counted', () =>
  withBroker(async (broker, make) => {
    broker.write('t', 0, ['a', 'b', 'c']);
    const c = make((router) => new Consumer(router, conn));
    await c.subscribe('t', 2000);
    await untilTrue(() => records(c.events) === 3);
    await untilTrue(() => broker.acks.length === 1);
  }),
);

check('a primary with no address is a redirect for a consumer too: its range waits, nothing fails', () =>
  withBroker(
    async (broker, make) => {
      broker.extraBrokers = [{ id: 'mute@h', host: '', port: 0, status: 'alive' }];
      const c = make((router) => new Consumer(router, conn));
      await c.start('t');
      await tick(50);
      assert.deepStrictEqual(c.events, []);
      assert.strictEqual(c.statsNow().redirects >= 1, true);
      assert.strictEqual(broker.consumes.length, 0);
    },
    [[0, 0, 16, 'mute@h']],
  ),
);

check('a range stopped before subscribe was called is still its refusal, not an empty error', () =>
  withBroker(async (broker, make) => {
    broker.consumePolicy = () => ({ refuse: 'permission_denied' });
    const c = make((router) => new Consumer(router, conn, { onRefusal: 'stop' }));
    await c.start('t');
    await untilTrue(() => c.events.length === 1);
    assert.deepStrictEqual(await c.subscribe('t', 500), { error: 'permission_denied' });
  }),
);

check('a closed consumer opens nothing more, even from a flush already on its way', () =>
  withBroker(
    async (broker, make) => {
      const c = make((router) => new Consumer(router, conn));
      await c.prepare('t');
      const starting = c.start('t');
      await c.close();
      await starting;
      await tick(100);
      assert.ok(broker.consumes.length <= 1, `opened ${broker.consumes.length} after close`);
    },
    [
      [1, 0, 8, 'self'],
      [2, 8, 16, 'self'],
    ],
  ),
);

check('consumer options are checked', () => {
  const router = new Router({ host: 'h', port: 1 });
  assert.throws(() => new Consumer(router, conn, { onRefusal: 'never' }), /onRefusal/);
  assert.throws(() => new Consumer(router, conn, { window: 0 }), /window/);
  assert.throws(() => new Consumer(router, conn, { max: -1 }), /max/);
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
  console.log(`passed ${cases.length} reader cases`);
}

// Against the real server: the topic read whole by a fetcher (waiting at each range's end) and by a consumer.
async function live(topic, expected) {
  const bootstrap = { host: process.env.MALACHI_HOST || '127.0.0.1', port: parseInt(process.env.MALACHI_PORT, 10) };
  const opts = { user: process.env.MALACHI_USER, pass: process.env.MALACHI_PASS, timeout: 5000 };
  const f = new Fetcher(new Router(bootstrap), opts, { max: 7, atTail: 'wait' });
  const fetched = [];
  for (let empty = 0; empty < 6 && fetched.length < expected; ) {
    const answer = await f.fetchPage(topic);
    if (answer.kind !== 'page') throw new Error(`fetch: ${JSON.stringify(answer)}`);
    if (answer.page.values.length === 0) empty += 1;
    fetched.push(...answer.page.values.map(String));
  }
  await f.close();
  const c = new Consumer(new Router(bootstrap), opts, { window: 5, max: 3 });
  const subscribed = await c.subscribe(topic, 10000);
  await c.wait(10000, (x) => records(x.events) >= expected);
  const consumed = records(c.takeEvents());
  await c.close();
  console.log(JSON.stringify({ fetched, subscribed, consumed }));
}

const args = process.argv.slice(2);
(args[0] === '--live' ? live(args[1], Number(args[2])) : selfTest()).catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
