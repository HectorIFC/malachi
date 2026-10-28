'use strict';

/**
 * The reference CLIs (scripts/producer.js, consumer.js, subscriber.js) against a broker in this process
 * (./testing/fake_broker.js), for what the server cannot be made to do from a test: go down and come back,
 * refuse an append, or not be there at all. ExUnit runs it (test/scripts/cli_js_test.exs), which runs the
 * CLIs against the real server too.
 *
 *   node scripts/lib/cli.selftest.js
 */

const assert = require('assert');
const path = require('path');
const { spawn } = require('child_process');
const { FakeBroker } = require('./testing/fake_broker');

const tick = (ms = 20) => new Promise((resolve) => setTimeout(resolve, ms));

// Every CLI a case started, killed once the case ends however it ends, so a CLI that hangs does not outlive it.
const started = new Set();

// A CLI started against `port`, its output gathered as it comes: `out()`, `exited` (resolves with the exit
// code), `exitWithin(ms)` (the exit code, or a failure naming what the CLI printed once `ms` passed) and
// `interrupt()` (SIGINT, as Ctrl-C).
function start(script, args, port) {
  const child = spawn(process.execPath, [path.join(__dirname, '..', script), ...args], {
    env: { ...process.env, MALACHI_HOST: '127.0.0.1', MALACHI_PORT: String(port), MALACHI_USER: 'u', MALACHI_PASS: 'p' },
  });
  let out = '';
  child.stdout.on('data', (d) => (out += d));
  child.stderr.on('data', (d) => (out += d));
  const exited = new Promise((resolve) => child.on('exit', (code) => resolve(code)));
  started.add(child);
  exited.then(() => started.delete(child));
  const exitWithin = (ms) => {
    let timer = null;
    const late = new Promise((_, reject) => (timer = setTimeout(() => reject(new Error(`still running after ${ms} ms:\n${out}`)), ms)));
    return Promise.race([exited, late]).finally(() => clearTimeout(timer));
  };
  return { out: () => out, exited, exitWithin, interrupt: () => child.kill('SIGINT'), kill: () => child.kill('SIGKILL') };
}

async function untilOut(cli, pattern, ms = 10000) {
  const deadline = Date.now() + ms;
  while (!pattern.test(cli.out())) {
    if (Date.now() > deadline) throw new Error(`never printed ${pattern}:\n${cli.out()}`);
    await tick(20);
  }
}

async function freePort() {
  const probe = await new FakeBroker().start();
  probe.stop();
  return probe.port;
}

const cases = [];
const check = (name, fn) => cases.push([name, fn]);

check('a follower reads on across its broker going down and coming back, however long that takes', async () => {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', [[0, 0, 16, 'self']]);
  broker.write('t', 0, ['first']);
  const cli = start('consumer.js', ['t', '--follow'], broker.port);
  try {
    await untilOut(cli, /first/);
    broker.stop();
    // fetches find no broker for longer than a run that reads once waits (about ten seconds): waited out,
    // not an exit
    await tick(12000);
    const back = await new FakeBroker().start(broker.port);
    back.setRoutes('t', [[0, 0, 16, 'self']]);
    back.write('t', 0, ['first', 'second']);
    try {
      await untilOut(cli, /second/);
      assert.doesNotMatch(cli.out(), /Error/);
    } finally {
      back.stop();
    }
  } finally {
    cli.kill();
  }
});

check('a group follower and a subscriber started before the cluster wait for it, however long, then read', async () => {
  const port = await freePort();
  const follower = start('consumer.js', ['t', '--group', 'g', '--member', 'f', '--follow'], port);
  const subscriber = start('subscriber.js', ['t', '--group', 's', '--member', 's1'], port);
  try {
    // longer than a run that reads once waits for the cluster
    await tick(12000);
    for (const cli of [follower, subscriber]) assert.doesNotMatch(cli.out(), /Error/);
    const broker = await new FakeBroker().start(port);
    broker.setRoutes('t', [[0, 0, 16, 'self']]);
    broker.write('t', 0, ['late']);
    try {
      for (const cli of [follower, subscriber]) {
        await untilOut(cli, /late/);
        assert.match(cli.out(), /ranges \[0\]/);
      }
    } finally {
      broker.stop();
    }
  } finally {
    follower.kill();
    subscriber.kill();
  }
});

check('a run that reads once gives up on a cluster it cannot reach after about ten seconds, its group join too', async () => {
  for (const args of [['t'], ['t', '--group', 'g']]) {
    const started = Date.now();
    const cli = start('consumer.js', args, await freePort());
    assert.strictEqual(await cli.exitWithin(40000), 1, args.join(' '));
    const waited = Date.now() - started;
    assert.match(cli.out(), /Error: metadata_unavailable/, args.join(' '));
    assert.ok(waited >= 10000 && waited < 20000, `${args.join(' ')}: gave up after ${waited} ms`);
  }
});

// A server that takes connections and never answers anything, as a hung broker does.
async function mute() {
  const server = require('net').createServer((socket) => socket.on('error', () => {}));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { port: server.address().port, stop: () => server.close() };
}

check('a run that reads once gives up on a broker that takes the connection and never answers, in time', async () => {
  const silent = await mute();
  try {
    for (const args of [['t'], ['t', '--group', 'g']]) {
      const started = Date.now();
      const cli = start('consumer.js', args, silent.port);
      assert.strictEqual(await cli.exitWithin(40000), 1, args.join(' '));
      const waited = Date.now() - started;
      assert.match(cli.out(), /Error: metadata_unavailable/, args.join(' '));
      // ten seconds and the connect still on its way then: one address only, which takes the connection at
      // once and then leaves its authentication unanswered for the 15 s it is given
      assert.ok(waited >= 10000 && waited < 27000, `${args.join(' ')}: gave up after ${waited} ms`);
    }
  } finally {
    silent.stop();
  }
});

check('a group join given up on is undone with a leave before the run exits', async () => {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', [[0, 0, 16, 'self']]);
  broker.groupPolicy = ({ call }) => (call === 'join' ? 'silent' : 'ok');
  try {
    const cli = start('consumer.js', ['t', '--group', 'g', '--member', 'm'], broker.port);
    assert.strictEqual(await cli.exitWithin(40000), 1);
    assert.match(cli.out(), /timed out/);
    assert.deepStrictEqual(
      broker.groupCalls.map((c) => c.call),
      ['join', 'leave'],
    );
  } finally {
    broker.stop();
  }
});

check('a producer whose appends are refused says so and exits non-zero', async () => {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', [[0, 0, 16, 'self']]);
  broker.appendPolicy = () => ({ refuse: 'permission_denied' });
  try {
    const cli = start('producer.js', ['t', '3'], broker.port);
    assert.strictEqual(await cli.exitWithin(40000), 1);
    assert.match(cli.out(), /Appended 0 record\(s\)/);
    assert.match(cli.out(), /not every record was appended: \{"permission_denied":3\}/);
  } finally {
    broker.stop();
  }
});

check('a continuous producer appends one record a second until interrupted, then reports what it appended', async () => {
  const broker = await new FakeBroker().start();
  broker.setRoutes('t', [[0, 0, 16, 'self']]);
  try {
    const cli = start('producer.js', ['t', '--continuous'], broker.port);
    await untilOut(cli, /✓ \[2\]/, 5000);
    cli.interrupt();
    assert.strictEqual(await cli.exitWithin(40000), 0);
    const appended = Number(/Stopped\. appended=(\d+) errors=0/.exec(cli.out())[1]);
    assert.ok(appended >= 2, cli.out());
    assert.strictEqual(broker.log.get('t/0').length, appended);
  } finally {
    broker.stop();
  }
});

check('--member without --group is refused by both group CLIs', async () => {
  for (const script of ['consumer.js', 'subscriber.js']) {
    const cli = start(script, ['t', '--member', 'm1'], await freePort());
    assert.strictEqual(await cli.exitWithin(40000), 1, script);
    assert.match(cli.out(), /--member requires --group/, script);
  }
});

async function selfTest() {
  for (const [name, fn] of cases) {
    try {
      await fn();
    } catch (err) {
      console.error(`FAILED: ${name}`);
      throw err;
    } finally {
      for (const child of started) child.kill('SIGKILL');
    }
  }
  console.log(`passed ${cases.length} cli cases`);
}

selfTest().catch((err) => {
  console.error(err);
  process.exit(1);
});
