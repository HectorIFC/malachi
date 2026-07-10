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

// Prints a connection error hint and exits non-zero — the common failure the CLIs hit.
function fail(err, cfg) {
  console.error(colors.red(`\nError: ${err.message}`));
  if (cfg) console.error(colors.gray(`   Is the server running at ${cfg.host}:${cfg.port}?`));
  process.exit(1);
}

module.exports = { colors, config, parseArgs, fail };
