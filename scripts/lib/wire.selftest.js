'use strict';

/**
 * The storage policy codecs against the golden frames the Elixir codec is tested with
 * (test/support/fixtures/wire/policy_frames.json). Run: `node scripts/lib/wire.selftest.js [fixture]`.
 * No server needed. ExUnit runs it (test/scripts/policy_js_test.exs), so a byte the two clients disagree
 * on fails the Elixir build too.
 *
 * `--fields` prints the CLI's field table as JSON instead, which that test compares with
 * Malachi.Cluster.Policy.fields/0.
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const wire = require('./wire');

if (process.argv[2] === '--fields') {
  console.log(JSON.stringify(wire.POLICY_FIELDS));
  process.exit(0);
}

const fixture = process.argv[2] || path.join(__dirname, '../../test/support/fixtures/wire/policy_frames.json');
const { cases } = JSON.parse(fs.readFileSync(fixture, 'utf8'));

// Requests are encoded and compared byte for byte; responses are decoded and compared value for value.
const requests = {
  define_policy_req: (v) => wire.encodeDefinePolicyReq(v.name, v.fields),
  delete_policy_req: (v) => wire.encodeDeletePolicyReq(v.name, v.force),
  list_policies_req: () => wire.encodeListPoliciesReq(),
  bind_topic_policy_req: (v) => wire.encodeBindTopicPolicyReq(v.topic, v.name),
  get_topic_policy_req: (v) => wire.encodeGetTopicPolicyReq(v),
};

const responses = {
  list_policies_resp: (buf) => wire.decodeListPoliciesResp(buf),
  topic_policy_resp: (buf) => wire.decodeTopicPolicyResp(buf),
};

let checked = 0;
for (const c of cases) {
  const bytes = Buffer.from(c.hex, 'hex');
  if (requests[c.codec]) {
    assert.strictEqual(requests[c.codec](c.value).toString('hex'), c.hex, `${c.codec}: ${c.name}`);
  } else if (responses[c.codec]) {
    assert.deepStrictEqual(responses[c.codec](bytes), c.value, `${c.codec}: ${c.name}`);
  } else {
    throw new Error(`no Node codec for ${c.codec}`);
  }
  checked++;
}

// A malformed response is refused, never half read.
assert.throws(() => wire.decodeListPoliciesResp(Buffer.from('0000000000', 'hex')), /trailing bytes/);
assert.throws(() => wire.decodeTopicPolicyResp(Buffer.from('0100000001740007', 'hex')), /unknown code 7/);
assert.throws(() => wire.encodeDefinePolicyReq('p', [['retention.max_bytes', -1]]), /non-negative integer/);

// A budget past 2^53 is carried exactly, as a BigInt.
const huge = 2n ** 60n;
const encoded = wire.encodeDefinePolicyReq('p', [['retention.max_bytes', huge]]);
const listed = wire.decodeListPoliciesResp(Buffer.concat([Buffer.from('00000001', 'hex'), encoded]));
assert.deepStrictEqual(listed, [['p', [['retention.max_bytes', huge]]]]);

console.log(`wire.selftest.js passed ${checked} golden frames`);
