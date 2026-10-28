'use strict';

/**
 * One member of a consumer group, over the group keys (lib/malachi/wire.ex, keys 32 to 34, and 7 to
 * leave), as NorthGuard keeps groups beside the data path (meetup transcript, 938 to 951): it joins, is
 * handed the ranges it reads and the generation it commits at, heartbeats within its session, and follows
 * every rebalance a heartbeat reports. Reading is the readers' (./fetcher.js, ./consumer.js), which start a
 * range the member is handed from the group's checkpoint (`{ committed: group }`) and read only its ranges.
 *
 * The group keys go over the metadata connection of its own ./links.js: to the bootstrap address or, should
 * that not answer, to each broker the cluster state lists, as the routes are read; any broker routes them to
 * the group's coordinator. A beat that finds no connection, or loses the one it had, connects again on the
 * next one.
 *
 * `onAssignment({ generation, session_ms, ranges })` is called on the join and on every change a heartbeat
 * reports. A member is told it holds no range when the coordinator no longer knows it (`unknown_member`,
 * after which it joins again in the same beat), and when a whole session passed since it sent the last
 * request the coordinator answered, whatever request is still out then (it joins again on a later beat):
 * the coordinator counts the session from no earlier than it took that request, and hands the ranges on
 * once it ends, so the readers stop no later. Once it leaves, a member sends nothing more but the leave and
 * hands its readers nothing more.
 */

const { Links, monoMs } = require('./links');
const { MalachiError, DEFAULT_TIMEOUT } = require('./client');
const { sleep } = require('./cli');

// How long to wait before asking again a coordinator that is moving (`not_owner`, `migrating`).
const RETRY_MS = 200;
const RETRYABLE = new Set(['not_owner', 'migrating']);
// The least a join is given to be answered in, however little of its `waitMs` is left.
const MIN_JOIN_TIMEOUT_MS = 100;
// How long a leave may take before the member gives up on it: the coordinator evicts it at the end of its
// session anyway, and a process stopping should not wait that long.
const LEAVE_TIMEOUT_MS = 2000;
// The refusals of a commit that leave its positions to whoever holds the ranges next: a rebalance
// (`stale_generation`, `not_assigned`), an eviction (`unknown_member`), a coordinator moving (`not_owner`,
// `migrating`), or one that did not answer in time (`timeout`, which the server says may be sent again).
const REFUSED = new Set(['stale_generation', 'not_assigned', 'unknown_member', 'timeout', ...RETRYABLE]);

class GroupMember {
  // A member routing by `router` (./router.js, the readers' own, whose cluster state names the brokers to try)
  // and connecting with `connOpts` ({ user, pass, timeout }).
  constructor({ router, connOpts, topic, group, member, onAssignment }) {
    this.links = new Links(router, connOpts);
    this.topic = topic;
    this.group = group;
    this.member = member;
    this.onAssignment = onAssignment || (() => {});
    this.assignment = null;
    this.joined = false;
    this.joinSent = false; // whether a join went out, which a leave then undoes
    this.timer = null; // the next beat
    this.deadline = null; // the end of the session counted from the last request the coordinator answered
    this.requestTimeout = connOpts.timeout || DEFAULT_TIMEOUT;
    this.left = false;
  }

  // Joins the group: resolves with the assignment, or rejects with a refusal.
  //   - A coordinator that moves (`not_owner`, `migrating`) is asked again after a back-off, as long as it
  //     takes.
  //   - A cluster that does not answer is asked again until `waitMs` has passed (0, the default: asked once;
  //     Infinity: as long as it takes), then the join rejects with what the last try met: no broker took the
  //     connection (`metadata_unavailable`), the join was not answered in time, its connection dropped (the
  //     transport's reason), or it was answered after its session ended (`answered_after_session`).
  //   - Each join is given the connection's request timeout to be answered in, or what is left of `waitMs`
  //     when that is less, never under MIN_JOIN_TIMEOUT_MS.
  //   - The connection being opened when `waitMs` ends is waited for: ./links.js tries the bootstrap address
  //     and then each broker the cluster state lists, in turn, and ./client.js bounds each connect, and then
  //     its authentication, at 15 s.
  async join({ waitMs = 0 } = {}) {
    const until = monoMs() + waitMs;
    for (;;) {
      const client = await this.links.controlConnection();
      let reason = 'metadata_unavailable';
      if (client) {
        const left = until - monoMs();
        const timeout = waitMs === 0 || left >= this.requestTimeout ? this.requestTimeout : Math.max(MIN_JOIN_TIMEOUT_MS, left);
        try {
          const sent = monoMs();
          this.joinSent = true;
          // an answer later than its session asks again: the coordinator may have let it go by now
          if (this._joined(await client.joinGroup(this.topic, this.group, this.member, timeout), sent)) {
            this._schedule();
            return this.assignment;
          }
          reason = 'answered_after_session';
        } catch (err) {
          if (!(err instanceof MalachiError)) throw err;
          if (RETRYABLE.has(err.message)) {
            await sleep(RETRY_MS);
            continue;
          }
          if (!err.transport && !err.timeout) throw err;
          this.links.dropControl();
          reason = err.message;
        }
      }
      if (monoMs() >= until) throw new MalachiError(reason);
      await sleep(RETRY_MS);
    }
  }

  // Checkpoints `positions` ([{ range, position }]) at the member's generation, those of ranges it still
  // holds. Resolves true when the group took every one; false when some were of a range it no longer holds,
  // or the group refused them (REFUSED), or no broker answered: whoever holds those ranges next reads past
  // them again and commits them.
  async commit(positions) {
    if (positions.length === 0) return true;
    if (!this.joined) return false;
    const held = new Set(this.assignment.ranges);
    const mine = positions.filter((p) => held.has(p.range));
    if (mine.length === 0) return false;
    const client = await this.links.controlConnection();
    if (!client) return false;
    try {
      await client.commitOffsets(this.topic, this.group, this.member, this.assignment.generation, mine);
      return mine.length === positions.length;
    } catch (err) {
      if (!(err instanceof MalachiError)) throw err;
      if (err.transport || err.timeout) {
        this.links.dropControl();
        return false;
      }
      if (REFUSED.has(err.message)) return false;
      throw err;
    }
  }

  // Leaves the group at once, so its ranges go to the others without waiting out the session: whenever a
  // join went out, joined or not as the member last heard (a rejoin may be on its way), over a connection
  // opened again if the one held was dropped. Best effort, for LEAVE_TIMEOUT_MS at most.
  async leave() {
    this.left = true;
    this._stopTimers();
    this.joined = false;
    if (this.joinSent) {
      const leaving = (async () => {
        const client = await this.links.controlConnection();
        if (client) await client.leaveGroup(this.topic, this.group, this.member);
      })();
      let timer = null;
      const late = new Promise((resolve) => (timer = setTimeout(resolve, LEAVE_TIMEOUT_MS)));
      try {
        await Promise.race([leaving, late]);
      } catch (_coordinatorGone) {
        // the coordinator evicts the member when its session ends anyway
      } finally {
        clearTimeout(timer);
      }
    }
    await this.links.closeAll();
  }

  _stopTimers() {
    if (this.timer) clearTimeout(this.timer);
    if (this.deadline) clearTimeout(this.deadline);
    this.timer = null;
    this.deadline = null;
  }

  // The coordinator answered the request sent at `sent`: a member again, its session counted from then (the
  // coordinator took the request no earlier), and true. An answer that came after that session ended is no
  // membership to read by (the coordinator may have handed the ranges on already): false, the readers
  // stopped, and the member joins again.
  _joined(assignment, sent) {
    if (sent + assignment.session_ms <= monoMs()) {
      if (this.joined) this._lost();
      return false;
    }
    this.joined = true;
    if (this.deadline) clearTimeout(this.deadline);
    this.deadline = setTimeout(() => this._sessionEnded(), Math.max(0, sent + assignment.session_ms - monoMs()));
    if (this.deadline.unref) this.deadline.unref();
    this._assign(assignment);
    return true;
  }

  _sessionEnded() {
    this.deadline = null;
    if (this.joined && !this.left) this._lost();
  }

  // No longer a member: the readers stop at once, no later than the coordinator hands its ranges on.
  _lost() {
    this.joined = false;
    this._assign({ ...this.assignment, ranges: [] });
  }

  _assign(assignment) {
    const before = this.assignment;
    this.assignment = assignment;
    const changed =
      before === null ||
      before.generation !== assignment.generation ||
      [...before.ranges].sort().join() !== [...assignment.ranges].sort().join();
    if (changed) this.onAssignment(assignment);
  }

  // A beat every third of the session: within it however one is delayed.
  _schedule() {
    if (this.left) return;
    if (this.timer) clearTimeout(this.timer);
    const every = Math.max(100, Math.floor(this.assignment.session_ms / 3));
    this.timer = setTimeout(() => this._beat(), every);
    if (this.timer.unref) this.timer.unref();
  }

  async _beat() {
    this.timer = null;
    if (this.left) return;
    try {
      if (this.joined) await this._heartbeat();
      else await this._rejoin();
    } catch (_unexpected) {
      // a refusal neither path expects: the next beat asks again
    }
    this._schedule();
  }

  // `this.left` is checked once an answer comes: a member that left while its request was out sends nothing
  // more (a join sent after its leave would make it a member again, one nobody heartbeats) and hands its
  // readers nothing more. Before a request goes out there is no need: a beat runs from its timer, so a leave
  // (from a signal, or from a reader that finished) starts no sooner than the beat's await of its connection
  // ends when the connection is held, and when it is still being opened the leave waits on the same connect
  // and resumes after the beat, which waited first; either way the leave goes out behind the beat's request.
  async _heartbeat() {
    const client = await this.links.controlConnection();
    if (!client) return;
    const sent = monoMs();
    try {
      const assignment = await client.groupHeartbeat(this.topic, this.group, this.member, this.assignment.generation);
      if (!this.left) this._joined(assignment, sent);
    } catch (err) {
      if (!(err instanceof MalachiError)) throw err;
      if (err.transport || err.timeout) return this.links.dropControl();
      if (err.message === 'unknown_member' && !this.left) {
        this._lost();
        return this._rejoin();
      }
    }
  }

  async _rejoin() {
    const client = await this.links.controlConnection();
    if (!client) return;
    const sent = monoMs();
    try {
      this.joinSent = true;
      const assignment = await client.joinGroup(this.topic, this.group, this.member);
      // left while the join was out: the leave sent after it undoes it
      if (!this.left) this._joined(assignment, sent);
    } catch (err) {
      if (err instanceof MalachiError && (err.transport || err.timeout)) this.links.dropControl();
    }
  }
}

module.exports = { GroupMember };
