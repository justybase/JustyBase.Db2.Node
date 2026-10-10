'use strict';
// Minimal .env loader for Db2 live tests and benchmarks (no dependencies).
//
// Lookup order (first file found wins per key):
//   1. `<ts-db2>/.env`
//   2. repo-root `.env` (next to run-db2-*.cjs)
// An explicit `DB2_ENV_FILE` path is honored first when set.
//
// - With DB2_ENV_LOADED=1 no env file is read at all; DB2_TEST_DSN is only derived
//   from DB2_LIVE_TEST_* when those are already set.
//
// Rules:
// - Existing `process.env` values are never overridden.
// - `DB2_TEST_DSN` is derived from `DB2_LIVE_TEST_*` when it is not set,
//   using the same DSN shape as run-db2-*.cjs (URL-encoded, IPv6-safe).
// - Secret values are never logged. Safe to require multiple times.
const fs = require('node:fs');
const path = require('node:path');

const LIVE_KEYS = [
  'DB2_LIVE_TEST_HOST',
  'DB2_LIVE_TEST_PORT',
  'DB2_LIVE_TEST_DATABASE',
  'DB2_LIVE_TEST_USER',
  'DB2_LIVE_TEST_PASSWORD',
];

function parseDotenv(text) {
  const entries = {};
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const body = line.startsWith('export ') ? line.slice('export '.length).trim() : line;
    const equals = body.indexOf('=');
    if (equals < 0) continue;
    const key = body.slice(0, equals).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;
    let value = body.slice(equals + 1).trim();
    if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
      value = value
        .slice(1, -1)
        .replace(/\\n/g, '\n')
        .replace(/\\r/g, '\r')
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, '\\');
    } else if (value.length >= 2 && value.startsWith("'") && value.endsWith("'")) {
      value = value.slice(1, -1);
    } else {
      const comment = value.indexOf(' #');
      if (comment >= 0) value = value.slice(0, comment).trim();
    }
    entries[key] = value;
  }
  return entries;
}

function loadFile(file) {
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch {
    return false;
  }
  for (const [key, value] of Object.entries(parseDotenv(text))) {
    if (process.env[key] === undefined) process.env[key] = value;
  }
  return true;
}

function bracketHost(host) {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

function deriveTestDsn() {
  if (process.env.DB2_TEST_DSN) return process.env.DB2_TEST_DSN;
  if (!LIVE_KEYS.every((key) => process.env[key])) return undefined;
  const dsn =
    `db2://${encodeURIComponent(process.env.DB2_LIVE_TEST_USER)}` +
    `:${encodeURIComponent(process.env.DB2_LIVE_TEST_PASSWORD)}` +
    `@${bracketHost(process.env.DB2_LIVE_TEST_HOST)}` +
    `:${encodeURIComponent(process.env.DB2_LIVE_TEST_PORT)}` +
    `/${encodeURIComponent(process.env.DB2_LIVE_TEST_DATABASE)}`;
  process.env.DB2_TEST_DSN = dsn;
  return dsn;
}

function loadDb2Env() {
  // DB2_ENV_LOADED=1 means the caller already built the environment (for example a
  // hermetic run against a throwaway database): never read env files in that case.
  if (process.env.DB2_ENV_LOADED === '1') return deriveTestDsn();
  const candidates = [];
  if (process.env.DB2_ENV_FILE) candidates.push(path.resolve(process.env.DB2_ENV_FILE));
  candidates.push(path.resolve(__dirname, '..', '.env'));
  candidates.push(path.resolve(__dirname, '..', '..', '.env'));
  for (const file of candidates) loadFile(file);
  const dsn = deriveTestDsn();
  if (dsn) process.env.DB2_ENV_LOADED = '1';
  return dsn;
}

loadDb2Env();

module.exports = { loadDb2Env };
