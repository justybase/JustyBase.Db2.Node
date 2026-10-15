#!/usr/bin/env node
// Fails when a junit report (node --test --test-reporter=junit) contains skipped
// tests, or when it contains no test cases at all. A skipped test in a gating lane
// is not evidence that the behaviour works, so skips must be explicit opt-ins.
//
// Usage:
//   node tools/ci/check-junit-no-skips.cjs <report.xml> [--allow-skip <regex>]...
//
// Each --allow-skip regex is matched against the skip message that the test
// reports (for example the environment variable that opts a test in). Anything
// else is rejected.
'use strict';

const fs = require('node:fs');

const ENTITIES = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };

function decodeXml(value) {
  return value.replace(/&(#x[0-9a-fA-F]+|#\d+|[a-z]+);/g, (match, body) => {
    if (body[0] === '#') {
      const code = body[1] === 'x' || body[1] === 'X' ? parseInt(body.slice(2), 16) : parseInt(body.slice(1), 10);
      return String.fromCodePoint(code);
    }
    return ENTITIES[body] ?? match;
  });
}

function attribute(tag, name) {
  const match = new RegExp(`\\s${name}="([^"]*)"`).exec(tag);
  return match ? decodeXml(match[1]) : undefined;
}

/** Returns every <testcase> element as { name, skipped, skipMessage }. */
function parseTestCases(xml) {
  const cases = [];
  const pattern = /<testcase\b[^>]*?(\/>|>([\s\S]*?)<\/testcase>)/g;
  let match;
  while ((match = pattern.exec(xml)) !== null) {
    const tag = match[0];
    const body = match[2] ?? '';
    const skipped = /<skipped\b/.test(body);
    const skipElement = /<skipped\b[^>]*>/.exec(body);
    cases.push({
      name: attribute(tag, 'name') ?? '(unnamed)',
      skipped,
      skipMessage: skipElement ? attribute(skipElement[0], 'message') ?? '' : '',
    });
  }
  return cases;
}

function main(argv) {
  const allow = [];
  let file;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--allow-skip') {
      const pattern = argv[++i];
      if (pattern === undefined) throw new Error('--allow-skip requires a regular expression');
      allow.push(new RegExp(pattern));
    } else if (file === undefined) {
      file = argv[i];
    } else {
      throw new Error(`unexpected argument: ${argv[i]}`);
    }
  }
  if (file === undefined) throw new Error('usage: check-junit-no-skips.cjs <report.xml> [--allow-skip <regex>]...');

  const xml = fs.readFileSync(file, 'utf8');
  const cases = parseTestCases(xml);
  if (cases.length === 0) {
    console.error(`[no-skips] FAIL: ${file} contains no test cases; nothing was verified`);
    return 1;
  }

  const rejected = cases.filter((testCase) => testCase.skipped
    && !allow.some((pattern) => pattern.test(testCase.skipMessage)));
  const allowed = cases.filter((testCase) => testCase.skipped && !rejected.includes(testCase));

  for (const testCase of allowed) console.log(`[no-skips] allowed skip: ${testCase.name} (${testCase.skipMessage})`);
  if (rejected.length > 0) {
    for (const testCase of rejected) console.error(`[no-skips] FAIL skipped test: ${testCase.name} (${testCase.skipMessage || 'no message'})`);
    console.error(`[no-skips] ${rejected.length} skipped test(s) in ${file} are not allowed`);
    return 1;
  }
  console.log(`[no-skips] PASS: ${cases.length} test case(s) in ${file}, ${allowed.length} allowed skip(s)`);
  return 0;
}

if (require.main === module) {
  try {
    process.exitCode = main(process.argv.slice(2));
  } catch (error) {
    console.error(`[no-skips] ${error.message}`);
    process.exitCode = 2;
  }
}

module.exports = { parseTestCases, decodeXml };
