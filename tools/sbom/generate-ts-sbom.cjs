#!/usr/bin/env node
'use strict';
// Generate a CycloneDX 1.5 SBOM for the TypeScript driver from the committed
// lockfile. Offline; no publication.
// Usage: node tools/sbom/generate-ts-sbom.cjs [output-directory]
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..', '..');
const outDir = process.argv[2] ? path.resolve(process.argv[2]) : path.join(root, 'sbom-out');

const result = spawnSync('npm', ['sbom', '--sbom-format=cyclonedx', '--package-lock-only', '--offline'], {
  cwd: root,
  encoding: 'utf8',
  maxBuffer: 64 * 1024 * 1024,
});
if (result.status !== 0 || result.error) {
  console.error(`FAIL: npm sbom exited ${result.status ?? 'unavailable'}\n${result.stderr || result.error?.message || ''}`);
  process.exit(1);
}

let sbom;
try {
  sbom = JSON.parse(result.stdout);
} catch (error) {
  console.error(`FAIL: npm sbom did not produce JSON: ${error.message}`);
  process.exit(1);
}
if (sbom.bomFormat !== 'CycloneDX') {
  console.error(`FAIL: unexpected bomFormat ${String(sbom.bomFormat)}`);
  process.exit(1);
}

fs.mkdirSync(outDir, { recursive: true });
const out = path.join(outDir, 'ts-db2.cdx.json');
fs.writeFileSync(out, JSON.stringify(sbom, null, 2) + '\n');
console.log(`wrote ${out} with ${(sbom.components ?? []).length} component(s)`);
