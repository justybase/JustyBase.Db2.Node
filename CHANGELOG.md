# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/) and this
project uses [Semantic Versioning](https://semver.org/).

Version policy: the current development version is `0.1.0-alpha.1`.
The npm package is not published yet. Public package publication remains a
separate, explicit release decision after final qualification in the clean
public repository.

## [Unreleased]

### Changed

- Public package identity prepared as `@justybase/db2` with final repository
  metadata targeting `justybase/JustyBase.Db2.Node`.
- Root package exports now focus on the supported application API; the
  `ibm_db` compatibility facade and diagnostics are explicit subpaths.
- Release dry-run now packs once, validates the exact `.tgz`, and installs that
  artifact in clean CommonJS and ESM consumer projects.
- CI adds Windows/macOS package-entry smoke coverage and an aggregate `CI gate`.
- TLS test certificates and private keys are generated at runtime instead of
  being stored in the repository.

## [0.1.0-alpha.1] - unreleased

### Added

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

- Qualification baseline moved to Db2 LUW 12.1.5 for both TCP and TLS; the
  integration script pins the IBM Db2 Community image by digest.

### Fixed

- URL DSNs with IPv6 hosts no longer keep `[...]` brackets from Node URL
  serialization.
- Oversized reply chains destroy the socket and mark the session closed rather
  than returning an uncertain connection to reuse.
- Materialized LOBs stop at 16 MiB per value and materialized query results at
  32 MiB; progressive row streams retain bounded chunk reads.
