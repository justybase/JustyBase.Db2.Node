# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Node.js 22+ and the locked dependencies (`npm ci --ignore-scripts`). The tests load compiled output from `dist/`, so run `npm run build` before any direct `node --test` call, or you will test stale code. `npm test` builds first.

```bash
npm run build                      # tsc -> dist/cjs and dist/esm
npm test                           # build + all offline tests (tests/*.test.js)
npm run typecheck                  # tsc -p tsconfig.checkjs.json (tooling + ratcheted test files)
node tools/api/generate-ts-surface.cjs --check   # public API snapshot must match api/ts-surface.json
npm run test:esm                   # ESM entry smoke test

node --test tests/config.test.js                                   # one file
node --test --test-name-pattern="DSN" tests/config.test.js         # one test by name
```

CI also runs `node tools/sbom/generate-ts-sbom.cjs` and `node tools/sbom/check-package-contents.cjs` (the npm tarball allowlist). Before submitting a change, CONTRIBUTING.md's list of checks applies.

Live Db2 tests (`tests/live/*.test.js`, run serially with `--test-concurrency=1`):

```bash
npm run test:live                  # needs DB2_TEST_DSN or DB2_LIVE_TEST_{HOST,PORT,DATABASE,USER,PASSWORD}
npm run test:integration:db2       # smoke; starts a throwaway Db2 12.1.5 container via Docker
npm run test:integration:db2 -- --full     # full live suite (slow)
./scripts/test-db2-integration.sh --parity # differential check vs node-ibm_db (test-only oracle)
./scripts/test-db2-integration.sh --soak   # 15-min resource soak (DB2_IT_SOAK_SECONDS)
```

`tests/load-env.cjs` reads `.env` (repo root, or `DB2_ENV_FILE`) and derives `DB2_TEST_DSN` from `DB2_LIVE_TEST_*`. `.env` is gitignored. The Docker-based script never reads `.env` or caller `DB2_*` variables.

## Architecture

**Layering.** A request flows top-down through these layers, and each layer is only reachable from the one above it:

1. **Public surface** (`src/index.ts`, `src/ibmDbCompat.ts`, `src/diagnostics.ts`). The root package exports only connection, pool, DSN/config, errors, resources and import APIs. `./compat` (the `ibm_db`-style facade) and `./diagnostics` are experimental subpaths. Codecs, code points, converters and transport are internal and must stay out of the root exports.
2. **Session core** (`src/network/session.ts`, about 3.7k lines). `Db2Connection`, `Db2Statement`, `Db2Transaction` and `Db2ProcedureReader` live here, along with the orchestration for query, streaming, LOB, parameter (EXTDTA) and cancellation flows, and the row decoder plans (`compileRowDecoderPlan`, `decodeRowsWithPlan`). Most cross-cutting behavior ends up in this file.
3. **DRDA/DDM** (`src/network/ddm.ts`, `src/network/codepoint.ts`). It packs requests (`packExecSqlStatement…`, `packOpenQuery`, `packInterruptRdbRequest`, and so on) and parses replies (SQLCARD, SQLDA/column metadata, result-set sections, progressive LOB chunks).
4. **DSS framing** (`src/network/dss.ts`, `src/network/stream.ts`). Splits DDM objects into DSS segments (`CONTINUATION` bit, 0x7fff max segment). Reads replies incrementally from a `ByteReader`.
5. **Value conversion** (`src/converters/*`, `src/types/sqltype.ts`). SQLDTA parameter encoding, DECFLOAT, EBCDIC cp500.
6. **Security** (`src/security/secmec9.ts`). SECMEC 9 encrypted user/password exchange. Kerberos is limited to a caller-supplied GSSAPI token.

**Config and errors.** `src/config.ts` parses URL-style DSNs and a documented subset of IBM CLI key/value strings, and rejects unsupported security modes rather than downgrading them. `src/errors.ts` defines the `Db2Error` hierarchy. Errors keep SQLCODE/SQLSTATE and an `outcome` (`not-started` | `confirmed` | `unknown`) so callers can tell whether a mutation happened.

**Pooling.** `src/network/pool.ts` (`Db2Pool`) bounds physical sessions and admission waiters. On release it probes and restores the session state (schema, isolation, client identity). If that probe or restore fails, the session is discarded instead of reused.

**Resources and import.** `src/resources.ts` (`Db2ResourceBudget`) holds the process-wide materialization and spool quotas. `src/import.ts` (`importRows`) builds a bounded prepared-INSERT pipeline on top of the session layer.

## Invariants (preserve these)

These come from CONTRIBUTING.md and the code. They are the reason much of the code looks defensive.

- **Fail closed.** If protocol or session state is uncertain, discard the connection. Never reuse it.
- **No automatic replay** of mutating SQL or transaction control after an uncertain transport outcome.
- **Cancellation is two-step.** An interrupt request (`INTRDBRQS`) is only an acknowledgment. The original command must still settle, and its result determines the final outcome. A confirmed cancel surfaces as SQLCODE -952 / SQLSTATE 57014.
- **Bounded memory.** Single materialized LOB is limited to 16 MiB and materialized result to 32 MiB. Prefer incremental parsing over buffering.
- **Stable error classification.** Preserve SQLCODE/SQLSTATE and never put credentials into logs or error messages.
- **Public API is a contract.** Changing exports fails `tools/api/generate-ts-surface.cjs --check`. Regenerate `api/ts-surface.json` only when the change is intentional, and update README/CHANGELOG/docs in the same change. Do not regenerate it just to make CI green.

## Tests

- `tests/*.test.js` are offline protocol and unit tests. They drive the codecs and session with in-memory readers (for example, the `MemoryReader` helper in `tests/protocol.test.js`) and do not need Db2.
- `tests/live/*.test.js` need a real Db2 server. Without `DB2_TEST_DSN` they skip.
- CI runs `tools/ci/check-junit-no-skips.cjs`, which fails the build on any skip without an allowed reason. Do not weaken or skip a failing test to get green.
- `DB2_SECMEC9_TEST_DSN` opts into the SECMEC 9 live test. `DB2_PARITY_IBMDB=1` opts into the `ibm_db` parity test. `DB2_IBM_IMPORT_ORACLE` is an optional external oracle supplied by the db2-conformance harness. This repository never depends on other driver repositories.
- Test certificates and private keys are generated at runtime into a temp directory (OpenSSL). Never commit them. `tests/fixtures/tls/` and `*.pem`/`*.key` are gitignored.

## Build and packaging

- `tsconfig.json` (strict, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`) is the base. `tsconfig.cjs.json` and `tsconfig.esm.json` each emit one module format into `dist/`. The ESM build gets a `package.json` with `type: module`.
- `package.json` `exports` maps `.`, `./compat`, and `./diagnostics` to both formats. `files` restricts the published tarball to `dist/cjs`, `dist/esm`, and the docs.
- `dist/` is gitignored and is regenerated by `npm run build`, so do not edit it by hand.
