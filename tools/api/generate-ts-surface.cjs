#!/usr/bin/env node
'use strict';
// Generate an offline public-API snapshot for the Node.js driver from the
// machine-generated root declarations. The walker follows the actual re-export
// contract: named re-exports include only named symbols, while export * includes
// the complete target module surface.
//
// Usage: node tools/api/generate-ts-surface.cjs [--check]
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { parse } = require('@babel/parser');

const root = path.resolve(__dirname, '..', '..');
const check = process.argv.includes('--check');
const outFile = path.join(root, 'api', 'ts-surface.json');
const entry = path.join(root, 'dist', 'cjs', 'index.d.ts');
const packageManifest = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));

function normalize(text) {
  return text.replace(/\s+/g, ' ').trim();
}

function parseDeclarationFile(file, text = fs.readFileSync(file, 'utf8')) {
  return parse(text, {
    sourceType: 'module',
    plugins: [['typescript', { dts: true }]],
    attachComment: false,
  });
}

function modulePath(fromFile, specifier) {
  const resolved = path.resolve(path.dirname(fromFile), specifier);
  return /\.d\.ts$/.test(resolved) ? resolved : resolved.replace(/\.(?:mjs|cjs|js)$/, '.d.ts');
}

function withoutComments(text, start, end, comments) {
  let result = '';
  let cursor = start;
  for (const comment of comments) {
    if (comment.end <= start || comment.start >= end) continue;
    result += text.slice(cursor, Math.max(cursor, comment.start)) + ' ';
    cursor = Math.min(end, comment.end);
  }
  result += text.slice(cursor, end);
  return normalize(result);
}

function classMemberIsPrivate(member) {
  if (member.kind === 'constructor') return false;
  return member.accessibility === 'private'
    || member.key?.type === 'PrivateName'
    || member.key?.type === 'PrivateIdentifier';
}

function declarationText(text, statement, declaration, comments) {
  if (declaration.type !== 'ClassDeclaration') {
    return withoutComments(text, statement.start, statement.end, comments);
  }

  const body = declaration.body;
  const head = withoutComments(text, statement.start, body.start, comments).trimEnd();
  const members = body.body
    .filter((member) => !classMemberIsPrivate(member))
    .map((member) => withoutComments(text, member.start, member.end, comments));
  const tail = withoutComments(text, body.end, statement.end, comments);
  return normalize(`${head} {${members.length ? ` ${members.join(' ')} ` : ''}}${tail}`);
}

function declaredNames(declaration) {
  if (declaration.type !== 'VariableDeclaration') return [declaration.id?.name].filter(Boolean);
  return declaration.declarations.flatMap((entry) => {
    if (entry.id.type === 'Identifier') return [entry.id.name];
    return [];
  });
}

function declarationKind(declaration) {
  if (declaration.type === 'ClassDeclaration') return 'class';
  if (declaration.type === 'TSInterfaceDeclaration') return 'interface';
  if (declaration.type === 'TSTypeAliasDeclaration') return 'type';
  if (declaration.type === 'TSEnumDeclaration') return 'enum';
  if (declaration.type === 'FunctionDeclaration' || declaration.type === 'TSDeclareFunction') return 'function';
  if (declaration.type === 'VariableDeclaration') return declaration.kind;
  if (declaration.type === 'TSModuleDeclaration') return 'namespace';
  return null;
}

function addItem(items, name, kind, declaration) {
  if (!name || !kind) return;
  const existing = items.get(name);
  if (existing && existing.declarations.includes(declaration)) return;
  if (existing) existing.declarations.push(declaration);
  else items.set(name, { name, kind, declarations: [declaration] });
}

function extractExports(file, items, requestedNames = null) {
  const text = fs.readFileSync(file, 'utf8');
  const ast = parseDeclarationFile(file, text);
  for (const statement of ast.program.body) {
    if (statement.type !== 'ExportNamedDeclaration' || !statement.declaration) continue;
    const declaration = statement.declaration;
    const kind = declarationKind(declaration);
    if (!kind) continue;
    const canonical = declarationText(text, statement, declaration, ast.comments);
    for (const name of declaredNames(declaration)) {
      if (requestedNames === null || requestedNames.has(name)) addItem(items, name, kind, canonical);
    }
  }
}

function addRequest(requests, file, names) {
  const existing = requests.get(file);
  if (existing === null) return;
  if (names === null) {
    requests.set(file, null);
    return;
  }
  const merged = existing ?? new Set();
  for (const name of names) merged.add(name);
  requests.set(file, merged);
}

function rootReexportRequests(file, items) {
  const text = fs.readFileSync(file, 'utf8');
  const ast = parseDeclarationFile(file, text);
  const requests = new Map();

  // Root-local declarations are public too.
  extractExports(file, items, null);

  for (const statement of ast.program.body) {
    const source = (statement.type === 'ExportAllDeclaration' || statement.type === 'ExportNamedDeclaration')
      ? statement.source?.value
      : undefined;
    if (!source?.startsWith('.')) continue;
    const target = modulePath(file, source);
    if (statement.type === 'ExportAllDeclaration') {
      addRequest(requests, target, null);
      continue;
    }

    const names = new Set();
    for (const specifier of statement.specifiers ?? []) {
      if (specifier.type !== 'ExportSpecifier') continue;
      const local = specifier.local;
      if (local.type === 'Identifier') names.add(local.name);
      else if (local.type === 'StringLiteral') names.add(local.value);
    }
    addRequest(requests, target, names);
  }
  return requests;
}

function main() {
  if (!fs.existsSync(entry)) {
    console.error(`FAIL: ${path.relative(root, entry)} is missing; run "npm run build" first`);
    process.exit(2);
  }

  const items = new Map();
  const requests = rootReexportRequests(entry, items);
  for (const [file, requestedNames] of requests) {
    if (!fs.existsSync(file)) {
      console.error(`FAIL: re-export target ${path.relative(root, file)} is missing`);
      process.exit(2);
    }
    extractExports(file, items, requestedNames);
  }

  const sorted = [...items.values()]
    .map((item) => ({ ...item, declarations: item.declarations.sort() }))
    .sort((a, b) => a.name.localeCompare(b.name));

  const surface = {
    generator: 'tools/api/generate-ts-surface.cjs',
    package: packageManifest.name,
    items: sorted,
  };
  const digest = crypto.createHash('sha256').update(JSON.stringify(surface, null, 2)).digest('hex');
  surface.digest = digest;

  if (check) {
    if (!fs.existsSync(outFile)) {
      console.error(`FAIL: ${path.relative(root, outFile)} is missing; run the generator and commit it`);
      process.exit(1);
    }
    const committed = JSON.parse(fs.readFileSync(outFile, 'utf8'));
    if (committed.digest === digest && JSON.stringify(committed) === JSON.stringify(surface)) {
      console.log(`PASS: public API surface unchanged (${sorted.length} exports, ${digest.slice(0, 12)})`);
      return;
    }
    const before = new Map((committed.items ?? []).map((item) => [item.name, item]));
    const after = new Map(sorted.map((item) => [item.name, item]));
    console.error('FAIL: public API surface changed');
    for (const name of before.keys()) if (!after.has(name)) console.error(`  - removed: ${name}`);
    for (const name of after.keys()) if (!before.has(name)) console.error(`  + added: ${name}`);
    for (const [name, item] of after) {
      if (before.has(name) && JSON.stringify(before.get(name)) !== JSON.stringify(item)) {
        console.error(`  ~ changed: ${name}`);
      }
    }
    process.exit(1);
  }

  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(surface, null, 2) + '\n');
  console.log(`wrote ${path.relative(root, outFile)} with ${sorted.length} exports (digest ${digest.slice(0, 12)})`);
}

if (require.main === module) main();

module.exports = { parseDeclarationFile, declarationText, extractExports };
