'use strict';

/**
 * The ranges a reading client reads, one lane each, kept in step with a topic's routes, as the Elixir load
 * generator's Malachi.Loadtest.ReadLanes does: the consumer (./consumer.js) and the fetcher (./fetcher.js)
 * share it.
 *
 * A lane is an object with at least `block` (its range's keyspace block, [keyStart, keyEnd]) and `start`
 * (where the next read of the range starts: 'earliest', { position }, or a marker of the client's own, the
 * consumer's 'stopped'). When the routes no longer hold a range, its lane is retired and kept, with its
 * start; a range the routes name with no lane yet gets one starting where its nearest retired ancestor
 * stopped, or else from the start of its history, so a record is read at least once and never lost.
 *
 * The retired ranges of the topic whose block holds a new range's are its ancestors, and the nearest is the
 * one the topic made last, which is the highest range number: a topic numbers its ranges in the order it
 * makes them. A position read in an ancestor names, in the new range, the same record or one before it, so
 * nothing past what was read is skipped and at most some records are read again.
 */

const laneKey = (topic, range) => `${topic}\u0000${range}`;

function splitKey(key) {
  const at = key.lastIndexOf('\u0000');
  return [key.slice(0, at), Number(key.slice(at + 1))];
}

// `lanes` and `retired` (Maps of lane key to lane) brought in step with `ranges`, `topic`'s active ranges
// as the router keeps them ({ range, keyStart, keyEnd }): lanes of ranges they no longer hold retired when
// `retirable(lane)` says so (a consumer keeps one whose stream is still open until it is told it moved),
// and a lane made with `newLane(block, start)` for each range with none. Changes both Maps in place.
function reconcile(lanes, retired, topic, ranges, retirable, newLane) {
  const live = new Set(ranges.map((r) => r.range));
  for (const [key, lane] of [...lanes]) {
    const [laneTopic, range] = splitKey(key);
    if (laneTopic === topic && !live.has(range) && retirable(lane)) {
      lanes.delete(key);
      retired.set(key, lane);
    }
  }
  for (const r of ranges) {
    const key = laneKey(topic, r.range);
    if (!lanes.has(key)) {
      const block = [r.keyStart, r.keyEnd];
      lanes.set(key, newLane(block, startFor(retired, topic, block)));
    }
  }
}

// Where a range of `topic` whose block is `block` starts: where its nearest retired ancestor stopped, or the
// start of its history. Another topic's ranges cover the same keyspace and say nothing about this one's.
function startFor(retired, topic, [keyStart, keyEnd]) {
  let nearest = null;
  for (const [key, lane] of retired) {
    const [laneTopic, range] = splitKey(key);
    const [parentStart, parentEnd] = lane.block;
    if (laneTopic === topic && parentStart <= keyStart && keyEnd <= parentEnd && (nearest === null || range > nearest.range)) {
      nearest = { range, start: lane.start };
    }
  }
  return nearest === null ? 'earliest' : nearest.start;
}

module.exports = { reconcile, laneKey, splitKey };
