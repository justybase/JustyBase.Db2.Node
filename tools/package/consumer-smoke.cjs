#!/usr/bin/env node
'use strict';

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tarball = process.argv[2] ? path.resolve(process.argv[2]) : undefined;
if (!tarball || !fs.existsSync(tarball)) {
  console.error('usage: node tools/package/consumer-smoke.cjs <package.tgz>');
  process.exit(2);
}

const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'justybase-db2-consumer-'));

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', stdio: 'pipe' });
  if (result.status !== 0 || result.error) {
    console.error(result.stdout || '');
    console.error(result.stderr || result.error?.message || '');
    throw new Error(`${command} ${args.join(' ')} failed with exit ${result.status ?? 'unknown'}`);
  }
}

function install(cwd) {
  run(npm, ['install', '--ignore-scripts', '--no-audit', '--no-fund', tarball], cwd);
}

try {
  const cjs = path.join(root, 'cjs');
  fs.mkdirSync(cjs);
  fs.writeFileSync(path.join(cjs, 'package.json'), JSON.stringify({ private: true, type: 'commonjs' }));
  fs.writeFileSync(path.join(cjs, 'smoke.cjs'), `
const assert = require('node:assert/strict');
const db2 = require('@justybase/db2');
const compat = require('@justybase/db2/compat');
const diagnostics = require('@justybase/db2/diagnostics');
assert.equal(typeof db2.Db2Connection, 'function');
assert.equal(typeof db2.Db2Pool, 'function');
assert.equal('CodePoint' in db2, false, 'protocol internals must not be exported from package root');
assert.equal(typeof compat.Db2CompatDatabase, 'function');
assert.equal(typeof diagnostics, 'object');
`);
  install(cjs);
  run(process.execPath, ['smoke.cjs'], cjs);

  const esm = path.join(root, 'esm');
  fs.mkdirSync(esm);
  fs.writeFileSync(path.join(esm, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
  fs.writeFileSync(path.join(esm, 'smoke.mjs'), `
import assert from 'node:assert/strict';
import * as db2 from '@justybase/db2';
import * as compat from '@justybase/db2/compat';
import * as diagnostics from '@justybase/db2/diagnostics';
assert.equal(typeof db2.Db2Connection, 'function');
assert.equal(typeof db2.Db2Pool, 'function');
assert.equal('CodePoint' in db2, false, 'protocol internals must not be exported from package root');
assert.equal(typeof compat.Db2CompatDatabase, 'function');
assert.equal(typeof diagnostics, 'object');
`);
  install(esm);
  run(process.execPath, ['smoke.mjs'], esm);

  console.log('PASS: exact tarball installs and loads from both CommonJS and ESM consumers');
} catch (error) {
  console.error(`FAIL: package consumer smoke: ${error.message}`);
  process.exitCode = 1;
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
