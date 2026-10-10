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
- a full live Db2 LUW qualification run against a fresh, digest-pinned Db2 12.1.5 Community container via `./scripts/test-db2-integration.sh --full`.

The public live qualification covers the repository's `tests/live/*.test.js` suite for the single fresh Db2 instance created by that script. It is intentionally self-contained: it does not consume developer `.env` files or an existing database.

## Maintainer release qualification

Before an npm release, the maintainer additionally runs broader qualification outside the public GitHub Actions workflow. Those checks include:

- IBM-provider parity checks for selected values and operations,
- cancellation / SQL interrupt scenarios beyond the public smoke path where environment-specific setup is required,
- **1 GiB** streamed BLOB checksum verification,
- bounded RSS and spool usage checks,
- a measured **15-minute soak** after warm-up,
- cross-driver evidence and exact source revision/runtime recording.

These additional checks are release gates maintained by the project; their detailed harness/evidence is not part of this repository. A green public CI run should therefore be read as evidence that the public checks passed, not as proof that every maintainer-only release gate was executed in GitHub Actions.

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
