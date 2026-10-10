'use strict';

const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

function runOpenSsl(args, cwd) {
  try {
    execFileSync('openssl', args, { cwd, stdio: ['ignore', 'ignore', 'pipe'] });
  } catch (error) {
    const stderr = error?.stderr?.toString?.().trim();
    const detail = stderr || error?.message || String(error);
    throw new Error(`OpenSSL test-certificate generation failed: ${detail}`);
  }
}

function writeExtensionFile(directory, name, lines) {
  const file = path.join(directory, name);
  fs.writeFileSync(file, `${lines.join('\n')}\n`, { mode: 0o600 });
  return file;
}

function createCertificateAuthority(directory, prefix, commonName) {
  const key = `${prefix}-key.pem`;
  const cert = `${prefix}.pem`;
  runOpenSsl([
    'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
    '-keyout', key,
    '-out', cert,
    '-subj', `/CN=${commonName}`,
    '-days', '1',
    '-sha256',
  ], directory);
  return { key, cert };
}

function createSignedCertificate(directory, options) {
  const key = `${options.prefix}-key.pem`;
  const csr = `${options.prefix}.csr`;
  const cert = `${options.prefix}.pem`;
  const ext = writeExtensionFile(directory, `${options.prefix}.ext`, options.extensions);

  runOpenSsl([
    'req', '-new', '-newkey', 'rsa:2048', '-nodes',
    '-keyout', key,
    '-out', csr,
    '-subj', `/CN=${options.commonName}`,
    '-sha256',
  ], directory);

  runOpenSsl([
    'x509', '-req',
    '-in', csr,
    '-CA', options.ca.cert,
    '-CAkey', options.ca.key,
    '-CAcreateserial',
    '-out', cert,
    '-days', '1',
    '-sha256',
    '-extfile', path.basename(ext),
  ], directory);

  return { key, cert };
}

function createTlsFixtures() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'justybase-db2-node-tls-'));
  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    fs.rmSync(directory, { recursive: true, force: true });
  };

  try {
    const ca = createCertificateAuthority(directory, 'ca', 'JustyBase Db2 test CA');
    const otherCa = createCertificateAuthority(directory, 'other-ca', 'JustyBase Db2 untrusted test CA');

    createSignedCertificate(directory, {
      prefix: 'server',
      commonName: '127.0.0.1',
      ca,
      extensions: [
        'basicConstraints=critical,CA:FALSE',
        'keyUsage=critical,digitalSignature,keyEncipherment',
        'extendedKeyUsage=serverAuth',
        'subjectAltName=IP:127.0.0.1',
      ],
    });

    createSignedCertificate(directory, {
      prefix: 'client',
      commonName: 'JustyBase Db2 trusted test client',
      ca,
      extensions: [
        'basicConstraints=critical,CA:FALSE',
        'keyUsage=critical,digitalSignature,keyEncipherment',
        'extendedKeyUsage=clientAuth',
      ],
    });

    createSignedCertificate(directory, {
      prefix: 'other-client',
      commonName: 'JustyBase Db2 untrusted test client',
      ca: otherCa,
      extensions: [
        'basicConstraints=critical,CA:FALSE',
        'keyUsage=critical,digitalSignature,keyEncipherment',
        'extendedKeyUsage=clientAuth',
      ],
    });

    const file = (name) => path.join(directory, name);
    process.once('exit', cleanup);
    return { directory, file, cleanup };
  } catch (error) {
    cleanup();
    throw error;
  }
}

module.exports = { createTlsFixtures };
