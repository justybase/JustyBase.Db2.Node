# Contributing

Thanks for helping improve JustyBase Db2 for Node.js (`@justybase/db2`).

## Development principles

Changes should preserve these project properties unless the change intentionally revises the contract:

- fail closed when protocol/session state is uncertain,
- do not automatically replay mutating SQL or transaction control after uncertain transport outcomes,
- keep pool admission, materialization and spool usage bounded,
- preserve structured SQLCODE/SQLSTATE/error classification,
- avoid exposing credentials in logs/errors,
- prefer incremental parsing/streaming over unbounded buffering,
- keep public API changes intentional and reviewable.

## Supported development baseline

CI verifies Node.js 22 and 24.13.1 on Linux and runs additional package-entry smoke coverage on Windows and macOS. Use the locked dependency graph from `package-lock.json`.

## Before submitting a change

Run:

```bash
npm ci --ignore-scripts
npm run build
node tools/api/generate-ts-surface.cjs --check
node --test tests/*.test.js
node tools/sbom/generate-ts-sbom.cjs
node tools/sbom/check-package-contents.cjs
```

Offline mutual-TLS tests require OpenSSL and generate their credentials in a temporary directory. Never add generated private keys to the repository.

Live Db2 qualification is separate:

```bash
npm run test:integration:db2 -- --full
```

This starts its own pinned ephemeral Db2 Community container and does not use a developer `.env` or an existing database. Every run writes a junit report and rejects skipped tests that lack an allowed reason. Set `DB2_IT_KEEP_CONTAINER=1` to keep the container after a run for diagnosis, and `DB2_IT_REPORT_DIR` to receive the junit report.

## Public API changes

Read [API-COMPATIBILITY.md](API-COMPATIBILITY.md).

Do not regenerate the API snapshot simply to make CI green. First determine whether the consumer-visible contract is intentionally changing. Update README/changelog/docs in the same change when behavior changes.

Do not add low-level DRDA/DDM protocol helpers to the root package. Experimental compatibility and diagnostics APIs belong behind their explicit package subpaths.

## Tests

Prefer deterministic offline protocol/resource tests for boundaries, malformed replies, cancellation ordering and lifecycle behavior. Use live Db2 tests where behavior genuinely depends on server interaction.

Do not weaken, skip or delete a failing test merely to make CI pass.

## Diagnostics

`TS_DB2_TRACE_PROCEDURE_CURSOR=1` writes procedure result-set protocol traces to stderr (code points, payload lengths, column types). The trace logs protocol metadata only: code points, lengths, counts and column types, never credentials or SQL text. At a block boundary it can show the first unparsed byte of row data, so keep it off when working with production data. It is a developer aid, not part of the public API.

## Performance and memory

For hot-path changes:

- avoid unnecessary `Buffer`/`Uint8Array` copies,
- preserve incremental parsing,
- keep importer and LOB staging bounded,
- consider Node heap/external/array-buffer usage for large-data paths,
- do not trade correctness or session synchronization for micro-optimizations.

## Secrets and test data

Never commit:

- real Db2 passwords or credential-bearing DSNs,
- GitHub/cloud/npm tokens,
- private TLS keys,
- production database dumps or confidential rows,
- private qualification environment files.

Use environment variables and ignored local files for live tests. Test certificates/keys must be generated at runtime and disposed after the test.

## Commit scope

Prefer focused commits with a clear reason and tests. Documentation-only and test-only changes should not alter runtime behavior implicitly.
