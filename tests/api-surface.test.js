'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { extractExports } = require('../tools/api/generate-ts-surface.cjs');

test('TypeScript API parser ignores private implementation members and keeps public contract', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ts-api-surface-'));
  try {
    const file = path.join(root, 'index.d.ts');
    fs.writeFileSync(file, `/** Docs do not change the API guard. */
export declare class Db2Connection {
  private readonly implementationState;
  #nativePrivateState;
  private internalOperation(): void;
  private constructor();
  protected extensionPoint(): string;
  execute(sql: string): Promise<void>;
}
export declare const DSS_LIMITS: {
  maxLobMaterializationBytes: number;
  maxMaterializedResultBytes: number;
};
export interface QueryOptions { timeoutMs?: number; }
`);

    const items = new Map();
    extractExports(file, items);
    const connection = items.get('Db2Connection').declarations[0];
    const limits = items.get('DSS_LIMITS').declarations[0];
    const options = items.get('QueryOptions').declarations[0];

    assert.doesNotMatch(connection, /implementationState|nativePrivateState|internalOperation/);
    assert.match(connection, /private constructor\(\)/);
    assert.match(connection, /protected extensionPoint\(\)/);
    assert.match(connection, /execute\(sql: string\)/);
    assert.doesNotMatch(connection, /Docs do not change/);
    assert.match(limits, /maxMaterializedResultBytes: number/);
    assert.match(options, /timeoutMs\?: number/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
