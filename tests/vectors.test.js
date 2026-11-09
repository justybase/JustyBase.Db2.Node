'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const { DsnError, parseDsn } = require('../dist/cjs/config.js');
const {
  Db2ConfigurationError,
  Db2PoolExhaustedError,
  Db2PoolTimeoutError,
  Db2ServerError,
  Db2ServerResponseError,
  Db2TimeoutError,
} = require('../dist/cjs/errors.js');
const { DSS_LIMITS } = require('../dist/cjs/network/dss.js');
const { DEFAULT_POOL_LIMITS, validatePoolLimits } = require('../dist/cjs/network/pool.js');
const { resolveBatchLimits, resolveImportOptions } = require('../dist/cjs/network/session.js');
const { DB2_RESOURCE_DEFAULTS } = require('../dist/cjs/resources.js');

const vectorPath = path.join(__dirname, 'vectors', 'JDC-v1.vectors.json');
const vectorText = fs.readFileSync(vectorPath, 'utf8').replace(/\r\n/g, '\n');
const vectors = JSON.parse(vectorText);

const dsnFields = {
  host: config => config.host,
  port: config => config.port,
  database: config => config.database,
  user: config => config.user,
  password: config => config.password,
  use_ssl: config => config.useSsl,
  timeout_ms: config => config.timeoutMs,
  block_size: config => config.blockSize,
  security_mechanism: config => config.securityMechanism,
  current_schema: config => config.currentSchema,
  ssl_root_ca_path: config => config.sslRootCaPath,
  kerberos_spn: config => config.kerberosSpn,
  kerberos_realm: config => config.kerberosRealm,
  krb5_principal: config => config.kerberosPrincipal,
  krb5_config_file: config => config.krb5ConfigFile,
  krb5_keytab_file: config => config.krb5KeytabFile,
  krb5_ccache_file: config => config.krb5CcacheFile,
  gssapi_token_file: config => config.gssapiTokenFile,
  client_appl_name: config => config.clientApplName,
  client_corr_token: config => config.clientCorrToken,
};

test('the vector copy is byte-canonical, hash-correct and self-contained', () => {
  const digest = crypto.createHash('sha256').update(vectorText, 'utf8').digest('hex');
  assert.equal(fs.readFileSync(`${vectorPath}.sha256`, 'utf8').trim(), digest);
  assert.equal(vectorText.includes('db2-conformance'), false);
  assert.equal(vectors.schema, 'jdc-vectors/v1');
  assert.equal(vectors.contract.version, 'v1');

  const contract = fs.readFileSync(path.join(__dirname, '..', 'docs', 'DRIVER-CONTRACT.md'), 'utf8')
    .replace(/\r\n/g, '\n');
  const body = contract.slice(contract.indexOf('\n') + 1);
  assert.equal(vectors.contract.sha256, crypto.createHash('sha256').update(body, 'utf8').digest('hex'));
});

test('DSN vectors match the parser', () => {
  for (const entry of vectors.dsn.accept) {
    const config = parseDsn(entry.input);
    for (const [field, expected] of Object.entries(entry.expect)) {
      if (field === 'params') {
        for (const [key, value] of Object.entries(expected))
          assert.equal(config.params[key], value, `${entry.id}: params.${key}`);
        continue;
      }
      const read = dsnFields[field];
      assert.ok(read, `${entry.id}: unmapped DSN field ${field}`);
      assert.equal(read(config), expected, `${entry.id}: ${field}`);
    }
  }
  for (const entry of vectors.dsn.reject)
    assert.throws(() => parseDsn(entry.input), DsnError, `${entry.id} must be rejected`);
});

test('error vectors match canonical category, outcome and SQL diagnostics', () => {
  const create = input => {
    switch (input.kind) {
      case 'sql': return new Db2ServerError(input.sql_code, input.sql_state, input.message);
      case 'configuration': return new Db2ConfigurationError(input.message);
      case 'pool-timeout': return new Db2PoolTimeoutError();
      case 'pool-exhausted': return new Db2PoolExhaustedError();
      case 'timeout': return new Db2TimeoutError(input.message);
      case 'server-response': return new Db2ServerResponseError(input.message);
      default: throw new Error(`unmapped error kind ${input.kind}`);
    }
  };
  for (const entry of vectors.errors) {
    const error = create(entry.input);
    assert.equal(error.canonicalCategory, entry.expect.category, `${entry.id}: category`);
    assert.equal(error.outcome, entry.expect.outcome, `${entry.id}: outcome`);
    if ('sql_code' in entry.expect) assert.equal(error.sqlCode, entry.expect.sql_code, `${entry.id}: sqlCode`);
    if ('sql_state' in entry.expect) assert.equal(error.sqlState, entry.expect.sql_state, `${entry.id}: sqlState`);
    for (const fragment of entry.expect.message_contains ?? [])
      assert.equal(error.message.includes(fragment), true, `${entry.id}: message missing ${fragment}`);
  }
});

test('limit vectors match the driver defaults', () => {
  const limits = {
    max_dss_size: DSS_LIMITS.maxDssObjectBytes,
    max_ddm_object_size: DSS_LIMITS.maxDdmObjectBytes,
    max_reply_chain_bytes: DSS_LIMITS.maxReplyChainBytes,
    max_reply_chain_packets: DSS_LIMITS.maxReplyChainPackets,
    max_materialized_lob_size: DSS_LIMITS.maxLobMaterializationBytes,
    max_materialized_result_bytes: DSS_LIMITS.maxMaterializedResultBytes,
    max_streamed_lob_bytes: DB2_RESOURCE_DEFAULTS.maxStreamedLobBytes,
    lob_chunk_bytes: DB2_RESOURCE_DEFAULTS.chunkBytes,
  };
  for (const entry of vectors.limits) {
    assert.ok(entry.field in limits, `unmapped limit field ${entry.field}`);
    assert.equal(limits[entry.field], entry.value, entry.id);
  }
});

test('bound vectors match the option validators', () => {
  const validate = (entry, value) => {
    if (entry.surface === 'batch') {
      resolveBatchLimits({ [entry.field === 'max_rows' ? 'maxRows' : 'maxBytes']: value });
    } else if (entry.surface === 'pool') {
      const field = entry.field === 'max_connections' ? 'maxConnections' : 'maxWaiters';
      validatePoolLimits({ ...DEFAULT_POOL_LIMITS, [field]: value });
    } else {
      const field = { max_batch_rows: 'maxBatchRows', max_batch_bytes: 'maxBatchBytes',
        notify_after_rows: 'notifyAfterRows' }[entry.field];
      assert.ok(field, `unmapped bound field ${entry.field}`);
      resolveImportOptions({ [field]: value });
    }
  };
  for (const entry of vectors.bounds) {
    if (entry.min > 0) assert.throws(() => validate(entry, entry.min - 1), entry.id);
    validate(entry, entry.min);
    if (entry.max !== undefined) {
      assert.throws(() => validate(entry, entry.max + 1), entry.id);
      validate(entry, entry.max);
    }
  }
});

test('default vectors match the driver option types', () => {
  const batch = resolveBatchLimits();
  const importOptions = resolveImportOptions();
  for (const entry of vectors.defaults) {
    let actual;
    if (entry.surface === 'pool') {
      const field = { max_connections: 'maxConnections', max_waiters: 'maxWaiters',
        acquire_timeout_ms: 'acquireTimeoutMs', connect_timeout_ms: 'connectTimeoutMs',
        idle_timeout_ms: 'idleTimeoutMs', max_lifetime_ms: 'maxLifetimeMs',
        reap_interval_ms: 'reapIntervalMs' }[entry.field];
      assert.ok(field, `unmapped pool field ${entry.field}`);
      actual = DEFAULT_POOL_LIMITS[field];
    } else if (entry.surface === 'batch') {
      actual = entry.field === 'max_rows' ? batch.maxRows
        : entry.field === 'max_encoded_bytes' ? batch.maxBytes
          : undefined;
      assert.notEqual(actual, undefined, `unmapped batch field ${entry.field}`);
    } else {
      const field = { max_batch_rows: 'maxBatchRows', max_batch_bytes: 'maxBatchBytes',
        notify_after_rows: 'notifyAfterRows', table_lock: 'tableLock', policy: 'policy' }[entry.field];
      assert.ok(field, `unmapped import field ${entry.field}`);
      actual = importOptions[field];
    }
    assert.equal(actual, entry.value, `${entry.surface}.${entry.field}`);
  }
});
