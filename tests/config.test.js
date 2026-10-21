const test = require('node:test');
const assert = require('node:assert/strict');
const { formatConfig, normalizeCurrentSchema, parseDsn, redactDsn } = require('../dist/cjs/config.js');
const { Db2Error } = require('../dist/cjs/errors.js');

test('parses URL, SSL URL, security, client accounting, and percent escaped values', () => {
  const basic = parseDsn('db2://db2inst1:password@localhost:50000/SAMPLE');
  assert.deepEqual([basic.host, basic.port, basic.database, basic.user, basic.password, basic.useSsl, basic.timeoutMs],
    ['localhost', 50000, 'SAMPLE', 'db2inst1', 'password', false, 30_000]);

  const secure = parseDsn('db2s://myuser:sec%40ret@db2server:50001/PRODDB?timeout=15s&block_size=131072');
  assert.deepEqual([secure.host, secure.port, secure.database, secure.user, secure.password, secure.useSsl, secure.timeoutMs, secure.blockSize],
    ['db2server', 50001, 'PRODDB', 'myuser', 'sec@ret', true, 15_000, 131_072]);

  const serverAuth = parseDsn('db2://localhost:50000/TESTDB?Authentication=SERVER&security_mechanism=3&spn=db2%2Fserver@CORP.LOCAL&krb5_config=%2Fetc%2Fkrb5.conf&krb5_keytab=%2Fetc%2Fdb2.keytab&krb5_ccache=%2Ftmp%2Fkrb5cc_1000&client_applname=orders&client_corr_token=trace');
  assert.deepEqual([serverAuth.securityMechanism, serverAuth.kerberosSpn, serverAuth.krb5ConfigFile, serverAuth.krb5KeytabFile, serverAuth.krb5CcacheFile, serverAuth.clientApplName, serverAuth.clientCorrToken],
    [3, 'db2/server@CORP.LOCAL', '/etc/krb5.conf', '/etc/db2.keytab', '/tmp/krb5cc_1000', 'orders', 'trace']);
});

test('parses key/value aliases, limits and unknown options', () => {
  const config = parseDsn('server=mydb2.local;port=50000;dbname=TESTDB;uid=alice;pwd=secret123;ssl=no;timeout=45s;blocksize=65536;custom_key=x;');
  assert.deepEqual([config.host, config.port, config.database, config.user, config.password, config.useSsl, config.timeoutMs, config.blockSize, config.params.custom_key],
    ['mydb2.local', 50000, 'TESTDB', 'alice', 'secret123', false, 45_000, 65_536, 'x']);
  assert.equal(parseDsn('database=TESTDB;ssl=yes').port, 50001);
  assert.equal(parseDsn('database=TESTDB;port=50000;ssl=yes').port, 50001);
  assert.equal(parseDsn('database=TESTDB;ssl=yes;port=50000').port, 50000);
  assert.equal(parseDsn('db2://host/TESTDB?timeout=2m30s').timeoutMs, 150_000);
});

test('parses IBM-style Server host:port endpoint', () => {
  const config = parseDsn('Server=db.example.test:50001;Database=TEST;UID=user;PWD=secret;Security=SSL;SSLServerCertificate=/tmp/db2-ca.pem');
  assert.deepEqual([config.host, config.port, config.database, config.user, config.password, config.useSsl, config.sslRootCaPath],
    ['db.example.test', 50001, 'TEST', 'user', 'secret', true, '/tmp/db2-ca.pem']);
  assert.equal(parseDsn('db2://host/TEST?Security=SSL&SSLServerCertificate=%2Ftmp%2Fca.pem').sslRootCaPath, '/tmp/ca.pem');
  assert.throws(() => parseDsn('Server=host:50001;Database=TEST;Security=SERVER_ENCRYPT'), /only Security=SSL is supported/);
});

test('parses JustyBase current schema and connect timeout option names', () => {
  const config = parseDsn('Database=TEST;CurrentSchema=Sales;connectTimeout=12');
  assert.equal(config.currentSchema, 'Sales');
  assert.equal(config.timeoutMs, 12_000);
});

test('parses supported authentication modes and keeps unsupported ones fail-closed', () => {
  for (const auth of ['SERVER', 'server']) {
    assert.equal(parseDsn(`Database=TESTDB;Authentication=${auth}`).securityMechanism, 3);
    assert.equal(parseDsn(`db2://host/TESTDB?Authentication=${auth}`).securityMechanism, 3);
  }
  for (const [mode, mechanism] of [['SERVER_ENCRYPT', 9], ['server_encrypt', 9], ['KERBEROS', 11], ['GSSPLUGIN', 11]]) {
    assert.equal(parseDsn(`Database=TESTDB;Authentication=${mode}`).securityMechanism, mechanism);
  }
  for (const [value, mechanism] of [['7', 7], ['usrencpwd', 7], ['ENCRYPTED_PASSWORD_SECURITY', 7], ['secmec7', 7], ['9', 9], ['dh_encrypted_password', 9], ['eusridpwd', 9], ['11', 11], ['kerberos', 11], ['gssapi', 11]]) {
    assert.equal(parseDsn(`Database=TESTDB;security_mechanism=${value}`).securityMechanism, mechanism);
  }
  assert.equal(parseDsn('Database=TESTDB;gssapi_token_file=/tmp/token.bin').gssapiTokenFile, '/tmp/token.bin');
  assert.equal(parseDsn('Database=TESTDB;security_mechanism=11;gssapi_token=/tmp/t.bin').gssapiTokenFile, '/tmp/t.bin');
  for (const option of ['Authentication=CLIENT', 'Authentication=TOKEN', 'Authentication=CERTIFICATE', 'security_mechanism=4', 'security_mechanism=6', 'security_mechanism=encrypted_password', 'security_mechanism=usrenpwd', 'security_mechanism=10', 'security_mechanism=12', 'security_mechanism=garbage']) {
    const [key, value] = option.split('=');
    assert.throws(() => parseDsn(`Database=TESTDB;${option}`), /unsupported DB2 authentication mode/);
    assert.throws(() => parseDsn(`db2://host/TESTDB?${key}=${encodeURIComponent(value)}`), /unsupported DB2 authentication mode/);
  }
});

test('rejects empty and invalid URL DSNs without leaking passwords', () => {
  assert.throws(() => parseDsn('  '), /empty DSN/);
  assert.throws(() => parseDsn('db2://u:secret@host/'), /missing database/);
  assert.equal(redactDsn('db2://u:secret@host/DB?x=1;pwd=other'), 'db2://u:******@host/DB?x=1;pwd=******');
  // A password that itself contains '@' must be masked completely, not only up to the first one.
  assert.equal(redactDsn('db2://u:p@ss@host/DB'), 'db2://u:******@host/DB');
  assert.equal(redactDsn('db2://u:p@ss@host/DB;pwd=other'), 'db2://u:******@host/DB;pwd=******');
  const config = parseDsn('db2://db2admin:super_secret_password@db2.example.com:50000/TESTDB?ssl=true&timeout=10s');
  const formatted = formatConfig(config);
  assert.equal(formatted, 'Config{Host:db2.example.com, Port:50001, Database:TESTDB, User:db2admin, Password:"******", UseSSL:true, Timeout:10s}');
  assert.doesNotMatch(formatted, /super_secret_password/);
});

test('invalid DSNs expose a stable Db2 error category and retain the DsnError API', () => {
  assert.throws(() => parseDsn('db2://user:secret@host/'), (error) => {
    assert.ok(error instanceof Db2Error);
    assert.equal(error.name, 'DsnError');
    assert.equal(error.category, 'invalid-connection-string');
    assert.equal(error.sqlCode, undefined);
    assert.doesNotMatch(error.message, /secret/u);
    return true;
  });
});

test('normalizes URL scheme case and keeps deterministic key/value edge handling', () => {
  // RFC 3986: the scheme is case-insensitive. Upper/mixed case must reach the URL parser.
  const upper = parseDsn('DB2://db2admin:pw@host:50000/TESTDB?timeout=5s');
  assert.deepEqual([upper.host, upper.port, upper.database, upper.user, upper.password, upper.useSsl, upper.timeoutMs],
    ['host', 50000, 'TESTDB', 'db2admin', 'pw', false, 5_000]);
  assert.equal(parseDsn('Db2S://host/TESTDB').useSsl, true);
  // Later duplicate keys win; empty values are tolerated; empty key/value tokens are skipped.
  assert.equal(parseDsn('Database=TESTDB;UID=a;UID=b').user, 'b');
  assert.equal(parseDsn('Database=TESTDB;UID=a;PWD=').password, '');
  assert.equal(parseDsn('server=host;;port=50000;').port, 50000);
});

test('normalizeCurrentSchema trims, defaults empty, and rejects invalid delimited identifiers', () => {
  assert.equal(normalizeCurrentSchema(undefined), '');
  assert.equal(normalizeCurrentSchema('   '), '');
  assert.equal(normalizeCurrentSchema('  Sales '), 'Sales');
  assert.equal(normalizeCurrentSchema('a'.repeat(128)), 'a'.repeat(128));
  assert.throws(() => normalizeCurrentSchema('a'.repeat(129)), /exceeds 128 characters/);
  assert.throws(() => normalizeCurrentSchema('APP\u0000X'), /NUL byte/);
  // The DSN parser trims values; normalization still runs before I/O in connect().
  assert.equal(parseDsn('Database=TESTDB;CurrentSchema= Sales ').currentSchema, 'Sales');
});

test('DSN parity corpus: URL and key/value edge cases accept or reject consistently', () => {
  const accepts = [
    { input: 'db2://user:pass@host:50000/TESTDB', expect: { host: 'host', port: 50000, database: 'TESTDB', user: 'user', password: 'pass', useSsl: false } },
    { input: 'db2s://user:pass@host/TESTDB', expect: { useSsl: true, port: 50000 } },
    { input: 'DB2://user:pass@host:50000/TESTDB', expect: { host: 'host', port: 50000 } },
    { input: 'db2://us%40er:pa%40ss@host/TESTDB', expect: { user: 'us@er', password: 'pa@ss' } },
    { input: 'db2://user:pass@[::1]:50000/TESTDB', expect: { host: '::1', port: 50000 } },
    { input: 'db2://user@host/TESTDB', expect: { user: 'user', password: '' } },
    { input: 'db2://host/TESTDB', expect: { user: '', password: '' } },
    { input: 'db2://user:pass@host/TESTDB?timeout=5s', expect: { timeoutMs: 5000 } },
    { input: 'db2://user:pass@host/TESTDB?timeout=7', expect: { timeoutMs: 7000 } },
    { input: 'db2://user:pass@host/TESTDB?security_mechanism=9', expect: { securityMechanism: 9 } },
    { input: 'db2://user:pass@host/TESTDB?block_size=131072', expect: { blockSize: 131072 } },
    { input: 'http://user:pass@host/TESTDB', expect: { database: '' } },
    { input: 'db2://u:p@host/TESTDB?timeout=abc', expect: { timeoutMs: 30000 } },
    { input: 'db2://u:p@host/TESTDB?port=abc', expect: { port: 50000 } },
    { input: 'db2://u:p@host/TESTDB?port=70000', expect: { port: 50000 } },
    { input: 'db2://u:p@host/TESTDB?block_size=512', expect: { blockSize: 65535 } },
    { input: 'Database=TESTDB;Host=h;Port=50001;UID=u;PWD=p', expect: { host: 'h', port: 50001, user: 'u', password: 'p' } },
    { input: 'DATABASE=TESTDB;HOST=h', expect: { host: 'h', database: 'TESTDB' } },
    { input: 'Database=TESTDB;UID=a;UID=b', expect: { user: 'b' } },
    { input: 'Database=TESTDB;Unknown=1', expect: { unknown: '1' } },
    { input: 'Database=TESTDB;PWD=a=b', expect: { password: 'a=b' } },
    { input: 'Database=TESTDB;PWD=a;b', expect: { password: 'a' } },
    { input: 'Server=host:50001;Database=TESTDB', expect: { host: 'host', port: 50001 } },
    { input: 'Server=[::1]:50001;Database=TESTDB', expect: { host: '::1', port: 50001 } },
    { input: 'Database=TESTDB;Authentication=SERVER_ENCRYPT', expect: { securityMechanism: 9 } },
    { input: 'Database=TESTDB;Timeout=5s', expect: { timeoutMs: 5000 } },
    { input: 'Database=TESTDB', expect: { host: 'localhost', port: 50000, database: 'TESTDB' } },
  ];
  for (const { input, expect } of accepts) {
    const config = parseDsn(input);
    for (const [key, value] of Object.entries(expect)) {
      if (key === 'unknown') assert.equal(config.params.Unknown, value, `${input} -> unknown`);
      else assert.deepEqual(config[key], value, `${input} -> ${key}`);
    }
  }
  const rejects = [
    '', '   ', 'db2://host', 'db2://user:pass@host:0/TESTDB', 'db2://user:pass@host:70000/TESTDB',
    'db2://user:pa%ZZss@host/TESTDB', 'db2://u:p@host/TESTDB?Authentication=BOGUS', 'Database=TESTDB;Authentication=TOKEN',
  ];
  for (const input of rejects) assert.throws(() => parseDsn(input), undefined, `expected reject: ${input}`);
});
