#!/usr/bin/env node
'use strict';

// Validate either the package that npm would create (no argument) or the exact
// .tgz passed as argv[2]. Release workflows must use the exact-tarball mode.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..', '..');
const tarball = process.argv[2] ? path.resolve(process.argv[2]) : undefined;
const required = [
  'package.json',
  'README.md',
  'LICENSE',
  'THIRD-PARTY-NOTICES.md',
  'dist/cjs/index.js',
  'dist/cjs/index.d.ts',
  'dist/esm/index.js',
  'dist/esm/index.d.ts',
  'dist/esm/package.json',
  'dist/cjs/ibmDbCompat.js',
  'dist/cjs/ibmDbCompat.d.ts',
  'dist/esm/ibmDbCompat.js',
  'dist/cjs/diagnostics.js',
  'dist/esm/diagnostics.js',
];

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

function npmDryRun() {
  const result = spawnSync('npm', ['pack', '--dry-run', '--json'], {
    cwd: root,
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
  });
  if (result.status !== 0 || result.error)
    fail(`npm pack exited ${result.status ?? 'unavailable'}\n${result.stderr || result.error?.message || ''}`);
  try {
    const parsed = JSON.parse(result.stdout)[0];
    return {
      files: parsed.files.map((file) => file.path),
      manifest: JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8')),
      label: 'npm pack --dry-run',
    };
  } catch (error) {
    fail(`npm pack did not produce expected JSON: ${error.message}`);
  }
}

function exactTarball(filename) {
  if (!fs.existsSync(filename)) fail(`tarball does not exist: ${filename}`);
  const listed = spawnSync('tar', ['-tzf', filename], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
  if (listed.status !== 0 || listed.error)
    fail(`could not list tarball ${filename}: ${listed.stderr || listed.error?.message || ''}`);

  const files = listed.stdout.split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => entry.startsWith('package/') ? entry.slice('package/'.length) : entry)
    .filter((entry) => entry && !entry.endsWith('/'));

  const manifestResult = spawnSync('tar', ['-xOzf', filename, 'package/package.json'], {
    encoding: 'utf8',
    maxBuffer: 4 * 1024 * 1024,
  });
  if (manifestResult.status !== 0 || manifestResult.error)
    fail(`could not read package.json from ${filename}: ${manifestResult.stderr || manifestResult.error?.message || ''}`);

  let manifest;
  try { manifest = JSON.parse(manifestResult.stdout); }
  catch (error) { fail(`invalid package.json inside ${filename}: ${error.message}`); }
  return { files, manifest, label: filename };
}

const result = tarball ? exactTarball(tarball) : npmDryRun();
const { files, manifest } = result;
const errors = [];

for (const name of required)
  if (!files.includes(name)) errors.push(`missing required entry: ${name}`);

for (const name of files) {
  const allowed = name.startsWith('dist/') ||
    name === 'package.json' || name === 'README.md' || name === 'LICENSE' || name === 'THIRD-PARTY-NOTICES.md';
  if (!allowed) errors.push(`unexpected entry: ${name}`);
  if (/private key/i.test(name) || /(?:^|\/).*[-.]key\.pem$/i.test(name) || /\.(?:p12|pfx)$/i.test(name))
    errors.push(`credential material must not be packaged: ${name}`);
}

if (manifest.name !== '@justybase/db2') errors.push(`unexpected package name: ${manifest.name}`);
if (!manifest.files || !Array.isArray(manifest.files)) errors.push('package.json must declare the files allowlist');
if (manifest.publishConfig?.access !== 'public') errors.push('publishConfig.access must be "public"');
if (manifest.publishConfig?.registry !== 'https://registry.npmjs.org/') errors.push('publishConfig.registry must be npmjs');
if (manifest.repository?.url !== 'git+https://github.com/justybase/JustyBase.Db2.Node.git')
  errors.push(`repository.url must point to the final public repository (got ${manifest.repository?.url ?? '<missing>'})`);

if (errors.length > 0) {
  console.error(`FAIL: npm tarball contents (${result.label})`);
  for (const error of errors) console.error(`  - ${error}`);
  process.exit(1);
}
console.log(`PASS: npm tarball contents (${files.length} entries, ${result.label})`);
