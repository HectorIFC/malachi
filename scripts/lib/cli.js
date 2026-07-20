'use strict';

/** Small shared bits for the reference CLIs: ANSI colors, env-backed config, and a tiny arg parser. */

const colors = {
  green: (s) => `\x1b[32m${s}\x1b[0m`,
  red: (s) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s) => `\x1b[33m${s}\x1b[0m`,
  cyan: (s) => `\x1b[36m${s}\x1b[0m`,
  gray: (s) => `\x1b[90m${s}\x1b[0m`,
  bold: (s) => `\x1b[1m${s}\x1b[0m`,
};

// Connection config shared by every CLI. A script overrides user/pass with its role default.
function config(defaults = {}) {
  return {
    host: process.env.MALACHI_HOST || 'localhost',
    port: parseInt(process.env.MALACHI_PORT, 10) || 4040,
    topic: process.env.MALACHI_TOPIC || defaults.topic || 'events',
    username: process.env.MALACHI_USER || defaults.username,
    password: process.env.MALACHI_PASS || defaults.password,
  };
}

// Splits argv into positional args and a flag map. `--group g`/`--max 10` take the next token as a value;
// bare flags like `--follow` become `true`. `-h`/`--help` is surfaced as flags.help.
function parseArgs(argv, valueFlags = []) {
  const positional = [];
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '-h' || a === '--help') {
      flags.help = true;
    } else if (a.startsWith('--')) {
      const name = a.slice(2);
      if (valueFlags.includes(name)) {
        flags[name] = argv[++i];
      } else {
        flags[name] = true;
      }
    } else {
      positional.push(a);
    }
  }
  return { positional, flags };
}

// Prints the error and exits non-zero. The "is the server up?" hint only helps for a connection failure:
// a Node network/system error carries an `err.code` (ECONNREFUSED, ETIMEDOUT, ...); an error the server sent
// back (a MalachiError like permission_denied / invalid_operation) has none, meaning the server is reachable,
// so the hint is suppressed there.
function fail(err, cfg) {
  console.error(colors.red(`\nError: ${err.message}`));
  if (cfg && err.code) console.error(colors.gray(`   Is the server running at ${cfg.host}:${cfg.port}?`));
  process.exit(1);
}

// Resolves after `ms`. Used by the member CLIs to back off before retrying a transient :not_owner.
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Runs `fn`, retrying while it rejects with a **retryable** error (a transient cluster change, a topic
// mid-migration answering :migrating, or a coordinator failover), backing off `ms` between attempts and
// calling `onRetry` each time. Non-retryable errors propagate. The server clears the transient once the
// change settles, so a bounded back-off (not a tight loop) is enough; NorthGuard likewise "moves the
// producers over" to the new location rather than failing them.
async function withRetry(fn, retryable, ms = 200, onRetry = () => {}) {
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      if (!retryable(err)) throw err;
      onRetry();
      await sleep(ms);
    }
  }
}

module.exports = { colors, config, parseArgs, fail, sleep, withRetry };
