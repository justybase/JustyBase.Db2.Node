'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { DSS_LIMITS } = require('../dist/cjs/network/dss.js');
const { DB2_RESOURCE_DEFAULTS } = require('../dist/cjs/resources.js');

test('public materialization limits and streaming defaults stay aligned', () => {
  assert.equal(DSS_LIMITS.maxLobMaterializationBytes, 16 * 1024 * 1024);
  assert.equal(DSS_LIMITS.maxMaterializedResultBytes, 32 * 1024 * 1024);
  assert.equal(DB2_RESOURCE_DEFAULTS.materializedLobBytes, DSS_LIMITS.maxLobMaterializationBytes);
  assert.equal(DB2_RESOURCE_DEFAULTS.materializedResultBytes, DSS_LIMITS.maxMaterializedResultBytes);
  assert.equal(DB2_RESOURCE_DEFAULTS.maxStreamedLobBytes, 1024 ** 3);
});
