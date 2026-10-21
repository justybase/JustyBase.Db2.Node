# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/) and this
project uses [Semantic Versioning](https://semver.org/).

Version policy: the current version is `0.1.0-alpha.2`, published to npm as
`@justybase/db2` under the `alpha` dist-tag. Each publication is an explicit
release decision made through the manual publish workflow.

## [Unreleased]

## [0.1.0-alpha.2] - 2026-10-21

### Added

- DRDA `USRENCPWD` (SECMEC 7) authentication: `security_mechanism=7` (`usrencpwd`, `encrypted_password_security`) sends a clear user ID and the DES-CBC-encrypted password in `SECCHK/SECTKN`, with the IV taken from the CP500 user ID. AES (`ENCALG=2`) fails closed. Verified live on Db2 LUW 11.5.4.0 and 12.1.5.0 with `AUTHENTICATION=SERVER_ENCRYPT`; opt-in live test `tests/secmec7-live.test.js` (`DB2_SECMEC7_TEST_DSN`).

### Changed

- When the server rejects the requested security mechanism, `ACCSECRD`'s SECMEC list is decoded in full and the error names every mechanism the server accepts plus `SECCHKCD` (`DB2 rejected security mechanism 3 (SECCHKCD=1); the server accepts 7, 9, 10`). Previously only the first value was read and reported as "selected". A non-zero `SECCHKCD` for the requested mechanism now fails before `SECCHK`.

## [0.1.0-alpha.1] - 2026-10-15

### Added

- Skip gate `tools/ci/check-junit-no-skips.cjs`: every live and offline lane writes a junit report and fails on skipped tests without an allowed reason, or when no test ran.
- Live coverage for session context switches (`setCurrentSchema`, `changeDatabase` to a second database, fail-closed database switch, `switchUser` rejection) in `tests/live/session-context.test.js`.
- Live TLS checks (`tests/live/tls.test.js`) against an ephemeral-CA listener provisioned by `--full`: verified connection, hostname mismatch and unknown CA are rejected.
- `--soak` suite of `scripts/test-db2-integration.sh`: seeds its fixtures (`benchmarks/resource-soak-seed.cjs`) and runs the TypeScript resource soak for 15 minutes by default, failing on FD, socket, spool or 512 MiB RSS problems.
- `npm run typecheck` type-checks the tooling and the tests that are already clean (`tsconfig.checkjs.json`, a ratchet list); `npm run test:coverage` prints the offline coverage report, also produced in CI.
- Release dry-run runs a live Db2 smoke suite against a fresh container before packing; the manual qualification workflow adds a 15-minute soak job.
- `DB2_IT_KEEP_CONTAINER=1` keeps the integration container for diagnosis; `DB2_IT_REPORT_DIR` receives the junit report and the soak log.
- Native TypeScript/Node.js DRDA driver for Db2 LUW: DSN parsing (URL and
  key/value), TCP/TLS and mutual-TLS plumbing, SECMEC 3/9/11 authentication
  paths, pools, commands, structured errors and bounded resource controls.
- Typed parameter binding and result decoding for the verified LUW type set,
  batch execution with confirmed partial progress, row streaming with bounded
  LOB locators (`Db2LobStream`), row import with atomic/checkpoint policies,
  procedure result sets with OUT/INOUT parameters, and cancellation via
  `AbortSignal` and command deadlines.
- JustyBase compatibility adapter (`@justybase/db2/compat`) for selected
  Promise-based `ibm_db` migration scenarios.
- FIFO pool admission, bounded wait queues, acquire/connect deadlines, idle and
  lifetime controls, shutdown, diagnostics, and process-wide import spool
  quotas.
- Incremental DSS reply reading for progressive LOB chunks and a local 1 GiB
  stream qualification worker.
- Dual-module packaging: `require()` resolves to `dist/cjs/` and `import`
  resolves to `dist/esm/`.
- `Db2Connection.changeDatabase()` and `setCurrentSchema()`;
  `Db2Pool.drainIdle()` plus `clearAllDb2Pools()` for test/maintenance cleanup.
- Import options `notifyAfterRows`, `tableLock`, and `commandTimeoutMs`.
- Offline SBOM generation, TypeScript API-surface guard and npm package-content
  validation.
- Public-facing README, advanced driver guide, qualification summary,
  contributing guide and security policy.

### Changed

- The `ibm_db` compatibility facade (`@justybase/db2/compat`) matches node-ibm_db more closely: `getColumnMetadata()` adds `index` and `SQL_DESC_DISPLAY_SIZE`, reports ODBC type codes in `SQL_DESC_CONSIZE_TYPE` and `REAL`/`DOUBLE` names, and follows node-ibm_db for `SQL_DESC_LENGTH`; server errors from the facade use the `SQL0204N` message token and expose lower-case `sqlcode`/`sqlstate`; inline BLOB values are `Buffer`s. `SQL_DESC_PRECISION` reports the declared precision. `open(dsn, { decimal: "number", bigint: "string" })` opts into node-ibm_db's value shapes; the defaults stay exact (DECIMAL string, BIGINT `bigint`). Native API messages are unchanged. The deliberate differences are listed in `MIGRATION-IBM-DB2.md`.
- Tests that cannot run no longer pass silently: the SECMEC 9 profile, the ibm_db parity oracle and the IBM import oracle are reported as skipped with their reason.
- Kerberos/GSSAPI is documented as caller-supplied token only; keytab and credential-cache settings are rejected by a regression test.
- `scripts/test-db2-integration.sh` raises the test database's `LOGSECOND` to 512 (online, recorded before and after in `db-log-config.txt`), following the C# qualification, so a long atomic import cannot exhaust the 22-file default log. The driver is unchanged: SQL-964/57011 is still reported. On failure the script prints only the db2diag lines that name ADM1823E or SQL-964.
- `scripts/test-db2-integration.sh` starts a fresh container (up to three attempts) when Db2 never becomes ready, and abandons an attempt at once when setup reports completion but the database is missing, and checks readiness without a `docker logs | grep -q` pipeline that can misreport under `pipefail`. Test failures are never retried.
- Public package identity prepared as `@justybase/db2` with final repository
  metadata targeting `justybase/JustyBase.Db2.Node`.
- Root package exports now focus on the supported application API; the
  `ibm_db` compatibility facade and diagnostics are explicit subpaths.
- Release dry-run now packs once, validates the exact `.tgz`, and installs that
  artifact in clean CommonJS and ESM consumer projects.
- CI adds Windows/macOS package-entry smoke coverage and an aggregate `CI gate`.
- TLS test certificates and private keys are generated at runtime instead of
  being stored in the repository.
- DECFLOAT decoding now distinguishes signaling NaN (`sNaN`, `0x7e..`) from
  quiet NaN (`NaN`, `0x7c..`) and the encoder accepts an `sNaN` input, matching
  Db2's own `CAST('sNaN' AS DECFLOAT)` text rendering. Previously both folded to
  `NaN`.
- CI adds a blocking `parity-ibmdb` lane that runs a differential SELECT/type
  corpus against a pinned, test-only `node-ibm_db@4.0.1` oracle on a fresh Db2
  12.1.5 container (`./scripts/test-db2-integration.sh --parity`).
- Qualification baseline moved to Db2 LUW 12.1.5 for both TCP and TLS; the
  integration script pins the IBM Db2 Community image by digest.

### Fixed

- URL DSNs with IPv6 hosts no longer keep `[...]` brackets from Node URL
  serialization.
- Oversized reply chains destroy the socket and mark the session closed rather
  than returning an uncertain connection to reuse.
- Materialized LOBs stop at 16 MiB per value and materialized query results at
  32 MiB; progressive row streams retain bounded chunk reads.
