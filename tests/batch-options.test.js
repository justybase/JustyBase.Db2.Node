'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { Db2Connection } = require('../dist/cjs/network/session.js');

const config = {
  host: 'localhost', port: 50000, database: 'TESTDB', user: 'u', password: 'p',
  useSsl: false, sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '',
  timeoutMs: 1000, blockSize: 65535, securityMechanism: 3,
  kerberosSpn: '', kerberosRealm: '', kerberosPrincipal: '',
  krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
  clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '',
  clientCorrToken: '', params: {},
};

test('batch bounds are validated before any I/O', async () => {
  const connection = new Db2Connection(new EventEmitter(), config);
  for (const maxRows of [0, -1, 32768, 1.5]) {
    await assert.rejects(connection.executeBatch('INSERT INTO T VALUES (?)', [[1]], { maxRows }), /maxRows/u);
  }
  for (const maxBytes of [0, -1, 1.5]) {
    await assert.rejects(connection.executeBatch('INSERT INTO T VALUES (?)', [[1]], { maxBytes }), /maxBytes/u);
  }
});
