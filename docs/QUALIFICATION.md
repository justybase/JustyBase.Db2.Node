# Qualification and compatibility

This document summarizes the compatibility and release gates used for the current alpha. Detailed cross-driver qualification evidence is maintained privately by the project maintainer; this repository contains only a public-safe summary.

## Current tested baseline

Maintainer qualification currently targets **Linux x64** with:

- **Db2 LUW 12.1.5** over TCP.
- **Db2 LUW 12.1.5** over TLS.
- Node.js **24.13.1** for qualification, with CI also covering Node.js **22**.

## Publicly reproducible qualification

The repository contains release checks that contributors can reproduce directly from a checkout:

- complete offline TypeScript/Node unit and protocol tests,
- CommonJS and native ESM package-entry checks,
- public API-surface verification,
- npm tarball allowlist and exact-tarball consumer smoke tests,
- SBOM generation,
- Windows and macOS package-entry smoke coverage,
- a full live Db2 LUW qualification run against a fresh, digest-pinned Db2 12.1.5 Community container via `./scripts/test-db2-integration.sh --full`, which also provisions an ephemeral-CA TLS listener and a second database for the session-switch tests,
- a blocking differential SELECT/type parity run against the official `node-ibm_db` provider via `./scripts/test-db2-integration.sh --parity` (CI `parity-ibmdb` lane).

The public live qualification covers the repository's `tests/live/*.test.js` suite for the single fresh Db2 instance created by that script. It is intentionally self-contained: it does not consume developer `.env` files or an existing database.

The `--parity` lane runs `tests/live/smoke.test.js` plus `tests/live/parity-ibmdb.test.js` against the same throwaway container. The oracle (`ibm_db@4.0.1`, installed with `npm install --no-save` inside the script) is test-only: it never enters `package.json`, the lockfile, the SBOM or the published tarball, and the driver itself keeps no IBM CLI/ODBC runtime dependency. Without `DB2_PARITY_IBMDB=1` the parity test is reported as **skipped with its reason**, never as passed; with the flag set a missing oracle fails closed. The lane covers SELECT value-shape parity only (scalars, NULLs, temporal/text/binary, boolean/integer input conversions, small LOBs); DML, procedures, cancellation, large streaming LOBs and soak remain maintainer-only gates below.

## Skip policy

Every run writes a junit report and `tools/ci/check-junit-no-skips.cjs` fails the run when a test is skipped without an allowed reason, or when the report contains no test cases. Allowed skips are explicit and named:

- `DB2_SECMEC9_TEST_DSN is not set` (offline lane; SECMEC 9 is an opt-in profile),
- `DB2_SECMEC7_TEST_DSN is not set` (offline lane; SECMEC 7 is an opt-in `AUTHENTICATION=SERVER_ENCRYPT` profile),
- `DB2_IBM_IMPORT_ORACLE is not set` (full suite only; the IBM .NET import cross-check needs an oracle this repository does not provision).

Tests that need a profile that a lane does not provision (for example TLS outside `--full`) are reported as skipped and fail the gate in that lane, so a lane never looks green while silently verifying less.

## Maintainer release qualification

Before an npm release, the maintainer additionally runs broader qualification outside the public GitHub Actions workflow. Those checks include:

- IBM-provider parity checks for selected values and operations,
- cancellation / SQL interrupt scenarios beyond the public smoke path where environment-specific setup is required,
- **1 GiB** streamed BLOB checksum verification,
- bounded RSS and spool usage checks,
- cross-driver soak comparison with the C# and Rust drivers (the public `--soak` suite covers the TypeScript driver alone),
- cross-driver evidence and exact source revision/runtime recording.

The 15-minute TypeScript soak is public: `./scripts/test-db2-integration.sh --soak` (`DB2_IT_SOAK_SECONDS`, default 900) seeds its fixtures, runs `benchmarks/resource-soak.cjs` and fails on FD, socket, spool or 512 MiB RSS growth. These additional checks are release gates maintained by the project; their detailed harness/evidence is not part of this repository. A green public CI run should therefore be read as evidence that the public checks passed, not as proof that every maintainer-only release gate was executed in GitHub Actions.

Any source change after a successful qualification invalidates that qualification for release purposes until the changed revision is tested again.

## Qualification budgets

The maintained cross-driver qualification currently uses:

- up to **10 concurrent sessions**,
- **512 MiB** maximum process RSS,
- **1 GiB** large streamed LOB workload,
- process-wide **10 GiB** spool budget,
- **15 minutes** measured soak after warm-up.

These are release gates, not universal application defaults.

## Current public resource limits

The alpha intentionally exposes bounded materialization controls. Current defaults include:

- **16 MiB** maximum materialized LOB,
- **32 MiB** maximum materialized result,
- bounded progressive locator reads,
- shared spool quotas for import staging,
- fail-closed disposal when protocol cleanup cannot be proven.

## Scope limitations

Passing the maintained qualification does not imply verification of every Db2 environment.

Notably:

- Db2 for z/OS and IBM i are outside the maintained LUW qualification baseline,
- trusted-context switching depends on server configuration/privileges,
- Kerberos/GSSAPI is environment-dependent,
- exact LOB representations can vary by Db2 version and query shape,
- the project is independent and not IBM-certified.

Validate materially different deployments before relying on the driver.
