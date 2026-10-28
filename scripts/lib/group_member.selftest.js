'use strict';

/**
 * A consumer group member (./group_member.js) against brokers in this process (./testing/fake_broker.js):
 * how it joins, heartbeats, follows and loses its assignment, finds the cluster again through the brokers
 * the cluster state lists, and commits. ExUnit runs it (test/scripts/cli_js_test.exs).
 *
 *   node scripts/lib/group_member.selftest.js
 */

const assert = require('assert');
const { Router } = require('./router');
const { Links } = require('./links');
const { GroupMember } = require('./group_member');
const { FakeBroker } = require('./testing/fake_broker');

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));
const conn = { user: 'u', pass: 'p', timeout: 500 };

async function untilTrue(fn, ms = 3000) {
  const deadline = Date.now() + ms;
  while (!fn()) {
    if (Date.now() > deadline) throw new Error(`condition not met in ${ms} ms: ${fn}`);
    await tick(5);
  }
}

// A member of group `g` reading topic `t` through `broker`, with every assignment it is handed in `seen`,
// connecting with `connOpts` (`conn` unless given).
async function withMember(fn, setup = () => {}, connOpts = conn) {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', [[0, 0, 16, 'self']]);
  setup(broker);
  const seen = [];
  const router = new Router(broker.address);
  const member = new GroupMember({ router, connOpts, topic: 't', group: 'g', member: 'm', onAssignment: (a) => seen.push(a.ranges) });
  const brokers = [broker];
  try {
    await fn({ broker, member, seen, router, brokers });
  } finally {
    await member.leave();
    for (const b of brokers) b.stop();
  }
}

const calls = (broker, call) => broker.groupCalls.filter((c) => c.call === call);
const cases = [];
const check = (name, fn) => cases.push([name, fn]);

check('a member joins, is handed its ranges, and heartbeats at its generation within the session', () =>
  withMember(async ({ broker, member, seen }) => {
    broker.group = { generation: 7, session_ms: 300, ranges: [0] };
    assert.deepStrictEqual(await member.join(), { generation: 7, session_ms: 300, ranges: [0] });
    assert.deepStrictEqual(seen, [[0]]);
    await untilTrue(() => calls(broker, 'heartbeat').length >= 2);
    assert.ok(calls(broker, 'heartbeat').every((c) => c.generation === 7 && c.member === 'm'));
    assert.deepStrictEqual(seen, [[0]], 'an unchanged assignment is not reported again');
  }),
);

check('a rebalance a heartbeat reports is handed on', () =>
  withMember(async ({ broker, member, seen }) => {
    broker.group = { generation: 1, session_ms: 300, ranges: [0, 1] };
    await member.join();
    broker.group = { generation: 2, session_ms: 300, ranges: [1] };
    await untilTrue(() => seen.length === 2);
    assert.deepStrictEqual(seen, [[0, 1], [1]]);
    await untilTrue(() => calls(broker, 'heartbeat').some((c) => c.generation === 2));
  }),
);

check('a join is asked again while the coordinator moves, and refused otherwise', () =>
  withMember(async ({ broker, member }) => {
    let moving = 2;
    broker.groupPolicy = ({ call }) => (call === 'join' && moving-- > 0 ? { refuse: 'not_owner' } : 'ok');
    await member.join();
    assert.strictEqual(calls(broker, 'join').length, 3);
    broker.groupPolicy = () => ({ refuse: 'permission_denied' });
    const other = new GroupMember({ router: new Router(broker.address), connOpts: conn, topic: 't', group: 'g', member: 'n' });
    await assert.rejects(other.join(), /permission_denied/);
    await other.leave();
  }),
);

check('a join no broker answers is asked again until its wait ends, or until one is there', async () => {
  const gone = await new FakeBroker().start();
  gone.stop();
  const newMember = () => new GroupMember({ router: new Router(gone.address), connOpts: conn, topic: 't', group: 'g', member: 'm' });
  const once = newMember();
  await assert.rejects(once.join(), /metadata_unavailable/);
  await once.leave();
  const patient = newMember();
  const started = Date.now();
  await assert.rejects(patient.join({ waitMs: 500 }), /metadata_unavailable/);
  assert.ok(Date.now() - started >= 500, `gave up after ${Date.now() - started} ms`);
  await patient.leave();

  const waiting = newMember();
  const joined = waiting.join({ waitMs: Infinity });
  await tick(600);
  const back = await new FakeBroker().start(gone.port);
  try {
    assert.deepStrictEqual((await joined).ranges, [0]);
  } finally {
    await waiting.leave();
    back.stop();
  }
});

check('a join whose connection drops is asked again; one not answered is given what is left of the wait', () =>
  withMember(
    async ({ broker, member }) => {
      let dropped = 1;
      broker.groupPolicy = ({ call }) => (call === 'join' && dropped-- > 0 ? 'drop' : 'ok');
      assert.deepStrictEqual((await member.join({ waitMs: 2000 })).ranges, [0]);
      assert.strictEqual(calls(broker, 'join').length, 2);

      broker.groupPolicy = () => 'silent';
      const other = new GroupMember({ router: new Router(broker.address), connOpts: { ...conn, timeout: 30000 }, topic: 't', group: 'g', member: 'n' });
      const giving = Date.now();
      // given the 500 ms of its wait, not the connection's 30 s
      await assert.rejects(other.join({ waitMs: 500 }), /timed out/);
      assert.ok(Date.now() - giving < 1500, `gave up after ${Date.now() - giving} ms, not the connection's timeout`);
      await other.leave();
    },
    () => {},
    { ...conn, timeout: 30000 },
  ),
);

check('a join is given 100 ms at least, however little of its wait is left', () =>
  withMember(
    async ({ broker, member }) => {
      broker.groupPolicy = ({ call }) => (call === 'join' ? { delayMs: 60 } : 'ok');
      assert.deepStrictEqual((await member.join({ waitMs: 30 })).ranges, [0]);
    },
    () => {},
    { ...conn, timeout: 30000 },
  ),
);

check('a member with no request timeout given joins by the client default', async () => {
  const broker = await new FakeBroker().start();
  broker.groupPolicy = ({ call }) => (call === 'join' ? { delayMs: 50 } : 'ok');
  const member = new GroupMember({ router: new Router(broker.address), connOpts: { user: 'u', pass: 'p' }, topic: 't', group: 'g', member: 'm' });
  try {
    let timer = null;
    const late = new Promise((_, reject) => (timer = setTimeout(() => reject(new Error('never joined in 3 s')), 3000)));
    try {
      assert.deepStrictEqual((await Promise.race([member.join({ waitMs: Infinity }), late])).ranges, [0]);
    } finally {
      clearTimeout(timer);
    }
    assert.strictEqual(calls(broker, 'join').length, 1);
  } finally {
    await member.leave();
    broker.stop();
  }
});

check('an answer that comes after its session ended is no membership: the readers stay stopped and it joins again', () =>
  withMember(async ({ broker, member, seen }) => {
    let late = 1;
    // the first join is answered after its 300 ms session: asked again
    broker.groupPolicy = ({ call }) => (call === 'join' && late-- > 0 ? { delayMs: 400 } : 'ok');
    assert.deepStrictEqual((await member.join({ waitMs: 3000 })).ranges, [0]);
    assert.strictEqual(calls(broker, 'join').length, 2);
    assert.deepStrictEqual(seen, [[0]], 'nothing handed on the late answer');

    // a heartbeat answered after its session, with a rebalance: not handed on; the join after it is
    let first = true;
    broker.groupPolicy = ({ call }) => {
      if (call !== 'heartbeat' || !first) return 'ok';
      first = false;
      broker.group = { ...broker.group, generation: 2, ranges: [0, 1] };
      return { delayMs: 400 };
    };
    await untilTrue(() => calls(broker, 'join').length === 3, 3000);
    await untilTrue(() => seen.length === 3);
    await tick(100);
    assert.deepStrictEqual(seen, [[0], [], [0, 1]]);
  }),
);

check('an answer found after its session ended while the member still counts as joined stops the readers', () =>
  withMember(async ({ member, seen }) => {
    await member.join();
    // what a stalled event loop makes of it: the answer handled before the session's timer, already due
    const assignment = { ...member.assignment, ranges: [0, 1] };
    // sent at the monotonic clock's origin: long past its session
    assert.strictEqual(member._joined(assignment, 0), false);
    assert.strictEqual(member.joined, false);
    assert.deepStrictEqual(seen, [[0], []]);
  }),
);

check('a member the coordinator no longer knows stops reading at once and joins again', () =>
  withMember(async ({ broker, member, seen }) => {
    await member.join();
    let once = true;
    broker.groupPolicy = ({ call }) => (call === 'heartbeat' && once ? ((once = false), { refuse: 'unknown_member' }) : 'ok');
    await untilTrue(() => seen.length === 3);
    assert.deepStrictEqual(seen, [[0], [], [0]]);
    assert.strictEqual(calls(broker, 'join').length, 2);
  }),
);

check('a whole session with no answer to a heartbeat stops the reading, and a later answer joins again', () =>
  withMember(
    async ({ broker, member, seen }) => {
      await member.join();
      const answered = Date.now();
      broker.groupPolicy = ({ call }) => (call === 'heartbeat' ? 'silent' : 'ok');
      await untilTrue(() => seen.length === 2);
      // when the 300 ms session ends, not when the heartbeat out then times out (3 s here)
      assert.ok(Date.now() - answered < 1500, `stopped ${Date.now() - answered} ms after the last answer`);
      assert.deepStrictEqual(seen, [[0], []], 'the coordinator has handed the ranges on by now');
      broker.groupPolicy = () => 'ok';
      await untilTrue(() => seen.length === 3, 6000);
      assert.deepStrictEqual(seen.at(-1), [0]);
    },
    () => {},
    { ...conn, timeout: 3000 },
  ),
);

check('the session is counted from when the answered request was sent, not from when its answer came', () =>
  withMember(async ({ broker, member, seen }) => {
    broker.group = { generation: 1, session_ms: 600, ranges: [0] };
    await member.join();
    let taken = null;
    // the first heartbeat is taken at once and answered 300 ms later; none after it is answered
    broker.groupPolicy = ({ call }) => {
      if (call !== 'heartbeat') return 'ok';
      if (taken !== null) return 'silent';
      taken = Date.now();
      return { delayMs: 300 };
    };
    await untilTrue(() => seen.length === 2, 3000);
    // a session counted from the answer would end about 900 ms after the coordinator took the heartbeat
    assert.ok(Date.now() - taken < 780, `stopped ${Date.now() - taken} ms after the coordinator took the heartbeat`);
  }),
);

check('a connection that stops answering is dropped, and the member joins again over a new one', () =>
  withMember(async ({ broker, member, seen }) => {
    await member.join();
    let mute = null;
    broker.groupPolicy = (_req, socket) => {
      if (mute === null) mute = socket;
      return socket === mute ? 'silent' : 'ok';
    };
    await untilTrue(() => seen.length === 3, 5000);
    assert.deepStrictEqual(seen, [[0], [], [0]]);
  }),
);

check('a member leaves over a new connection when the one it had was dropped', () =>
  withMember(async ({ broker, member }) => {
    broker.group = { generation: 1, session_ms: 3000, ranges: [0] };
    await member.join();
    broker.groupPolicy = ({ call }) => (call === 'commit' ? 'drop' : 'ok');
    assert.strictEqual(await member.commit([{ range: 0, position: [0, 1] }]), false);
    broker.groupPolicy = () => 'ok';
    await member.leave();
    assert.strictEqual(calls(broker, 'leave').length, 1);
  }),
);

check('a member leaves while its join again is out, and hears nothing of that join', () =>
  withMember(async ({ broker, member, seen }) => {
    await member.join();
    let once = true;
    // the join again is answered while the leave sent after it is still out
    broker.groupPolicy = ({ call }) => {
      if (call === 'heartbeat' && once) return (once = false), { refuse: 'unknown_member' };
      if (call === 'join') return { delayMs: 50 };
      return call === 'leave' ? { delayMs: 200 } : 'ok';
    };
    await untilTrue(() => calls(broker, 'join').length === 2);
    await member.leave();
    assert.strictEqual(calls(broker, 'leave').length, 1, 'the leave undoes the join still out');
    assert.deepStrictEqual(seen, [[0], []]);
  }),
);

check('a member that leaves while a heartbeat is out sends nothing after the leave, nor reads what it is told', async () => {
  // the heartbeat out is answered, after the leave went out behind it, with an eviction or a rebalance
  for (const answer of [{ delayMs: 100, refuse: 'unknown_member' }, { delayMs: 100 }]) {
    await withMember(async ({ broker, member, seen }) => {
      await member.join();
      let first = true;
      broker.groupPolicy = ({ call }) => {
        if (call === 'leave') return { delayMs: 300 };
        if (call !== 'heartbeat' || !first) return 'ok';
        first = false;
        broker.group = { ...broker.group, generation: 2, ranges: [0, 1] };
        return answer;
      };
      await untilTrue(() => calls(broker, 'heartbeat').length === 1);
      await member.leave();
      const order = broker.groupCalls.map((c) => c.call);
      assert.strictEqual(order.at(-1), 'leave', `${JSON.stringify(answer)}: ${order}`);
      assert.deepStrictEqual(seen, [[0]], `${JSON.stringify(answer)}: told nothing after the leave`);
    });
  }
});

check('a leave the coordinator does not answer is given up on, so a stopping member does not wait out its timeout', () =>
  withMember(
    async ({ broker, member }) => {
      await member.join();
      broker.groupPolicy = ({ call }) => (call === 'leave' ? 'silent' : 'ok');
      const started = Date.now();
      await member.leave();
      assert.ok(Date.now() - started < 2600, `left after ${Date.now() - started} ms`);
      assert.strictEqual(calls(broker, 'leave').length, 1);
    },
    () => {},
    { ...conn, timeout: 30000 },
  ),
);

check('a member whose broker went down heartbeats through another broker the cluster state lists', () =>
  withMember(async ({ broker, member, router, brokers }) => {
    const other = await new FakeBroker().start();
    brokers.push(other);
    broker.extraBrokers = [{ id: other.id, host: '127.0.0.1', port: other.port, status: 'alive' }];
    // the readers' router has read the cluster state
    const links = new Links(router, conn);
    await links.refresh('t');
    await links.closeAll();
    await member.join();
    broker.stop();
    await untilTrue(() => calls(other, 'heartbeat').length >= 1);
  }),
);

check('a member that found no broker for a while heartbeats again once one is back', () =>
  withMember(async ({ broker, member, seen, brokers }) => {
    broker.group = { generation: 1, session_ms: 3000, ranges: [0] };
    await member.join();
    broker.stop();
    // several beats find nothing listening
    await tick(2500);
    const back = await new FakeBroker().start(broker.port);
    brokers.push(back);
    back.group = broker.group;
    await untilTrue(() => back.groupCalls.length >= 1, 3000);
    assert.deepStrictEqual(seen.at(-1), [0]);
  }),
);

check('a commit carries the ranges still held, at the generation, and says whether every one was taken', () =>
  withMember(async ({ broker, member }) => {
    broker.group = { generation: 4, session_ms: 3000, ranges: [0, 1] };
    await member.join();
    assert.strictEqual(await member.commit([]), true);
    assert.strictEqual(await member.commit([{ range: 0, position: [0, 3] }]), true);
    assert.deepStrictEqual(calls(broker, 'commit').at(-1), {
      call: 'commit',
      topic: 't',
      group: 'g',
      member: 'm',
      generation: 4,
      positions: [{ range: 0, position: [0, 3] }],
    });
    // a range handed on is not committed: the member holding it next does that
    assert.strictEqual(await member.commit([{ range: 1, position: [0, 1] }, { range: 5, position: [0, 9] }]), false);
    assert.deepStrictEqual(calls(broker, 'commit').at(-1).positions, [{ range: 1, position: [0, 1] }]);
    const before = calls(broker, 'commit').length;
    assert.strictEqual(await member.commit([{ range: 5, position: [0, 9] }]), false);
    assert.strictEqual(calls(broker, 'commit').length, before, 'nothing of its own to commit: nothing sent');
  }),
);

check('a commit the group refuses for a rebalance, an eviction, a move or a timeout is false; another refusal throws', () =>
  withMember(async ({ broker, member }) => {
    broker.group = { generation: 1, session_ms: 3000, ranges: [0] };
    await member.join();
    const one = [{ range: 0, position: [0, 1] }];
    for (const reason of ['stale_generation', 'not_assigned', 'unknown_member', 'not_owner', 'migrating', 'timeout']) {
      broker.groupPolicy = ({ call }) => (call === 'commit' ? { refuse: reason } : 'ok');
      assert.strictEqual(await member.commit(one), false, reason);
    }
    broker.groupPolicy = ({ call }) => (call === 'commit' ? { refuse: 'permission_denied' } : 'ok');
    await assert.rejects(member.commit(one), /permission_denied/);
  }),
);

check('a commit whose connection drops is false, and the next one connects again', () =>
  withMember(async ({ broker, member }) => {
    broker.group = { generation: 1, session_ms: 3000, ranges: [0] };
    await member.join();
    const one = [{ range: 0, position: [0, 1] }];
    broker.groupPolicy = ({ call }) => (call === 'commit' ? 'drop' : 'ok');
    assert.strictEqual(await member.commit(one), false);
    broker.groupPolicy = () => 'ok';
    assert.strictEqual(await member.commit(one), true);
  }),
);

check('a member that left asks nothing more and commits nothing', () =>
  withMember(async ({ broker, member }) => {
    await member.join();
    await member.leave();
    assert.strictEqual(calls(broker, 'leave').length, 1);
    const asked = broker.groupCalls.length;
    assert.strictEqual(await member.commit([{ range: 0, position: [0, 1] }]), false);
    await tick(400);
    assert.strictEqual(broker.groupCalls.length, asked);
  }),
);

async function selfTest() {
  for (const [name, fn] of cases) {
    try {
      await fn();
    } catch (err) {
      console.error(`FAILED: ${name}`);
      throw err;
    }
  }
  console.log(`passed ${cases.length} group member cases`);
}

selfTest().catch((err) => {
  console.error(err);
  process.exit(1);
});
