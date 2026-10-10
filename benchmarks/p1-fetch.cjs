#!/usr/bin/env node
'use strict';
require('../tests/load-env.cjs');
// P1 paired fetch benchmark, TypeScript native driver. Emits JSON lines matching
// cs-db2/benchmarks/P1Fetch so the shared orchestrator can aggregate them.
//
// Usage: DB2_TEST_DSN=... node p1-fetch.cjs --dataset NARROW_10K [--warmups 3] [--samples 7]
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { Db2Connection } = require('../dist/cjs/network/session.js');

function parseArgs(argv) {
  const options = { dataset: undefined, modes: ['native-materialized', 'native-stream'], warmups: 3, samples: 7, manifest: undefined };
  for (let i = 2; i < argv.length; i++) {
    switch (argv[i]) {
      case '--dataset': options.dataset = argv[++i]; break;
      case '--modes': options.modes = argv[++i].split(',').map((s) => s.trim()).filter(Boolean); break;
      case '--warmups': options.warmups = Number(argv[++i]); break;
      case '--samples': options.samples = Number(argv[++i]); break;
      case '--manifest': options.manifest = path.resolve(argv[++i]); break;
      default: throw new Error(`unknown argument ${argv[i]}`);
    }
  }
  if (!options.dataset) throw new Error('--dataset is required');
  return options;
}

function writeU64(buffer, value) {
  buffer.writeBigUInt64BE(BigInt(value) & 0xFFFFFFFFFFFFFFFFn);
}

class CanonicalHash {
  constructor(columns) { this.columns = columns; this.sums = [0n, 0n, 0n, 0n]; this.count = 0n; this.bytes = 0; }
  add(row) {
    const chunks = [];
    for (let i = 0; i < this.columns.length; i++) chunks.push(encodeValue(this.columns[i], i < row.length ? row[i] : null));
    const buffer = Buffer.concat(chunks);
    this.bytes += buffer.length;
    const digest = crypto.createHash('sha256').update(buffer).digest();
    for (let w = 0; w < 4; w++) this.sums[w] = (this.sums[w] + digest.readBigUInt64LE(w * 8)) & 0xFFFFFFFFFFFFFFFFn;
    this.count++;
  }
  finish() {
    const input = Buffer.alloc(40);
    input.writeBigUInt64BE(this.count, 0);
    for (let w = 0; w < 4; w++) input.writeBigUInt64BE(this.sums[w], 8 + w * 8);
    return crypto.createHash('sha256').update(input).digest('hex');
  }
}

function lengthPrefixed(tag, buffer) {
  const header = Buffer.alloc(1 + 4);
  header[0] = tag;
  header.writeUInt32BE(buffer.length, 1);
  return Buffer.concat([header, buffer]);
}

function encodeValue(column, value) {
  if (value === null || value === undefined) return Buffer.from([0x00]);
  switch (column.type) {
    case 'integer': { const b = Buffer.alloc(9); b[0] = 0x02; b.writeBigInt64BE(BigInt(typeof value === 'bigint' ? value : Math.trunc(value)), 1); return b; }
    case 'decimal': return lengthPrefixed(0x03, Buffer.from(typeof value === 'string' ? value : String(value), 'utf8'));
    case 'double': { const b = Buffer.alloc(9); b[0] = 0x04; b.writeDoubleBE(Number(value), 1); return b; }
    case 'varchar': case 'text': return lengthPrefixed(0x01, Buffer.from(String(value), 'utf8'));
    case 'date': case 'timestamp': return lengthPrefixed(0x05, Buffer.from(String(value), 'utf8'));
    case 'boolean': return Buffer.from([0x06, value ? 1 : 0]);
    case 'binary': return lengthPrefixed(0x07, Buffer.from(value));
    default: throw new Error(`unknown canonical type ${column.type}`);
  }
}

function emit(payload) { process.stdout.write(JSON.stringify(payload) + '\n'); }

// The public row stream exposes large progressive LOBs as one-shot per-value
// streams; the benchmark consumes them so the canonical checksum sees the bytes.
async function materializeLobStreams(values) {
  let changed = false;
  const result = values.slice();
  for (let index = 0; index < result.length; index++) {
    const value = result[index];
    if (!value || typeof value !== 'object' || typeof value.stream !== 'function') continue;
    if (value.kind === 'binary-lob') {
      const chunks = [];
      for await (const chunk of value.stream()) chunks.push(chunk instanceof Uint8Array ? chunk : Buffer.from(chunk));
      result[index] = new Uint8Array(Buffer.concat(chunks));
      changed = true;
    } else if (value.kind === 'text-lob') {
      const chunks = [];
      for await (const chunk of value.stream()) chunks.push(chunk);
      const text = chunks.every((chunk) => typeof chunk === 'string') ? chunks.join('') : Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))).toString('utf8');
      result[index] = text;
      changed = true;
    }
  }
  return changed ? result : values;
}

(async () => {
  const options = parseArgs(process.argv);
  if (!options.manifest) throw new Error('--manifest <dataset manifest.json> is required');
  const manifest = JSON.parse(fs.readFileSync(options.manifest, 'utf8'));
  const dataset = manifest.datasets.find((d) => d.name === options.dataset);
  if (!dataset) throw new Error(`dataset ${options.dataset} is not in the manifest`);
  const dsn = process.env.DB2_TEST_DSN;
  if (!dsn) throw new Error('Set DB2_TEST_DSN');
  const expected = manifest.checksum.expected[dataset.name];

  const db = await Db2Connection.connect(dsn);
  // `checksum=true` is the untimed verification pass; timed samples skip hashing so
  // the measurement reflects the driver, not the checksum implementation.
  const modes = {
    'native-materialized': async (checksum) => {
      const started = process.hrtime.bigint();
      const result = await db.query(dataset.select);
      const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
      let bytes = 0;
      let digest = null;
      if (checksum) { const hash = new CanonicalHash(dataset.columns); for (const row of result.rows) hash.add(row); bytes = hash.bytes; digest = hash.finish(); }
      return { rows: result.rows.length, bytes, checksum: digest, elapsedMs, firstRowMs: null };
    },
    'native-stream': async (checksum) => {
      const started = process.hrtime.bigint();
      const hash = checksum ? new CanonicalHash(dataset.columns) : null;
      let rows = 0; let firstRowMs = null;
      for await (const row of db.queryStream(dataset.select)) {
        if (firstRowMs === null) firstRowMs = Number(process.hrtime.bigint() - started) / 1e6;
        const values = row.values;
        let materialized = values;
        for (let index = 0; index < values.length; index++) {
          const value = values[index];
          if (value && typeof value === 'object' && typeof value.stream === 'function') { materialized = await materializeLobStreams(values); break; }
        }
        if (hash) hash.add(materialized);
        else if (materialized.length === 0) throw new Error('unexpected empty row');
        rows++;
      }
      const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
      return { rows, bytes: hash ? hash.bytes : 0, checksum: hash ? hash.finish() : null, elapsedMs, firstRowMs };
    },
  };

  const selected = options.modes.filter((m) => m in modes);
  const verified = new Map();
  try {
    for (const mode of selected) {
      const outcome = await modes[mode](true);
      if (outcome.rows !== dataset.rows) throw new Error(`${mode}: expected ${dataset.rows} rows, got ${outcome.rows}`);
      if (expected && outcome.checksum !== expected) throw new Error(`${mode}: checksum ${outcome.checksum} does not match manifest ${expected}`);
      verified.set(mode, outcome);
    }
    for (const mode of selected) for (let w = 0; w < options.warmups; w++) await modes[mode](false);
    const samples = new Map(selected.map((m) => [m, []]));
    for (let iteration = 0; iteration < options.samples; iteration++) {
      for (let offset = 0; offset < selected.length; offset++) {
        const mode = selected[(iteration + offset) % selected.length];
        if (global.gc) { global.gc(); }
        const outcome = await modes[mode](false);
        if (outcome.rows !== dataset.rows) throw new Error(`${mode}: expected ${dataset.rows} rows, got ${outcome.rows}`);
        samples.get(mode).push(outcome);
        emit({ language: 'typescript', dataset: dataset.name, mode, warmup: false, iteration: iteration + 1, rows: outcome.rows, bytes: verified.get(mode).bytes, checksum: verified.get(mode).checksum, elapsedMs: outcome.elapsedMs, firstRowMs: outcome.firstRowMs });
      }
    }
    const peakRssBytes = process.resourceUsage().maxRSS * 1024;
    for (const mode of selected) {
      const values = samples.get(mode).map((v) => v.elapsedMs).sort((a, b) => a - b);
      const median = values[Math.floor(values.length / 2)];
      const firstRows = samples.get(mode).map((v) => v.firstRowMs).filter((v) => v !== null).sort((a, b) => a - b);
      emit({ language: 'typescript', dataset: dataset.name, mode, summary: true, rows: dataset.rows, bytes: verified.get(mode).bytes, checksum: verified.get(mode).checksum, medianMs: Number(median.toFixed(3)), minMs: Number(values[0].toFixed(3)), maxMs: Number(values.at(-1).toFixed(3)), medianFirstRowMs: firstRows.length ? Number(firstRows[Math.floor(firstRows.length / 2)].toFixed(3)) : null, peakRssBytes });
    }
  } finally {
    await db.close();
  }
})().catch((error) => { console.error(error); process.exit(1); });
