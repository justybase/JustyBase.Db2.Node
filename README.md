# JustyBase Db2 for Node.js

<p align="center">
  <img src="assets/justybase-db2-node-banner.png" alt="JustyBase Db2 for Node.js visual identity" width="100%" />
</p>

> Managed Node.js driver for **IBM Db2 LUW**, speaking DRDA/DDM directly over TCP/TLS without IBM CLI runtime dependencies.

[![CI](https://github.com/justybase/JustyBase.Db2.Node/actions/workflows/ci.yml/badge.svg)](https://github.com/justybase/JustyBase.Db2.Node/actions/workflows/ci.yml)
[![Node.js](https://img.shields.io/badge/Node.js-22%2B-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](CHANGELOG.md)

> **Alpha software.** APIs and protocol coverage may change before a stable release. Validate the driver against your own Db2 environment and workload before using it for production-critical data.

## Overview

`@justybase/db2` is a Node.js-native Db2 LUW client written in TypeScript. It uses Node's TCP/TLS stack and implements the DRDA/DDM protocol directly, so applications do not need IBM CLI, ODBC or native Db2 client libraries at runtime.

The current alpha includes direct/prepared SQL, transactions, bounded pooling, stored procedures, cancellation, incremental row streaming, progressive LOB streaming, typed values, bounded import and a Promise-based compatibility facade for selected `ibm_db` migration scenarios.

> **Independent project:** this project is not maintained, endorsed, sponsored or certified by IBM. IBM and Db2 are trademarks of International Business Machines Corporation.

## Why JustyBase Db2 for Node.js?

- **No native addon requirement** — Node TCP/TLS only.
- **Promise-first API** — designed for modern async Node applications.
- **CommonJS and ESM** — both entry points are built and tested.
- **AsyncIterable streaming** — incremental rows and progressive large-value reads.
- **Bounded resources** — bounded pools, materialization limits and process-wide spool quotas.
- **Structured Db2 errors** — SQLCODE/SQLSTATE preserved when available.
- **Cancellation support** — `AbortSignal`, explicit interrupts and command deadlines.
- **Migration path from `ibm_db`** — selected compatibility helpers are available without pretending to be binary-compatible.

## Feature matrix

| Feature | Status | Notes |
| --- | :---: | --- |
| TCP connections | ✅ | Native Node socket transport |
| TLS 1.2 / 1.3 | ✅ | Certificate/hostname validation |
| Password authentication | ✅ | DRDA server authentication |
| SECMEC 9 | ✅ | Encrypted user/password exchange |
| Kerberos / GSSAPI path | 🧪 | Caller-supplied GSSAPI token only (`gssapi_token_file`); keytab and credential-cache settings are rejected because the driver does not acquire tickets; not verified against a KDC in this repository |
| Direct SQL | ✅ | `query` / `execute` |
| Prepared statements | ✅ | Reusable positional parameters |
| Transactions | ✅ | Async transaction handle |
| Connection pool | ✅ | Bounded sessions and waiters |
| Row streaming | ✅ | `queryStream()` / `AsyncIterable` |
| BLOB / CLOB / DBCLOB / XML | ✅ | Materialized and streamed paths |
| Progressive LOB streaming | ✅ | One-shot streamed locator values |
| Stored procedures | ✅ | OUT/INOUT and result cursors |
| Batch DML | ✅ | Bounded execution with progress semantics |
| Cancellation | ✅ | `AbortSignal` + DRDA SQL interrupt |
| Row import | ✅ | Bounded prepared-insert pipeline |
| `ibm_db` migration facade | 🧪 | `@justybase/db2/compat` |
| IBM CLI dependency | ❌ | Not required at runtime |

## Requirements

- Node.js **22 or newer**.
- Current CI verifies Node **22** and **24.13.1** on Linux and runs additional smoke coverage on Windows and macOS.
- IBM Db2 LUW reachable over TCP or TLS.

Maintainer qualification currently targets Db2 LUW 12.1.5 over TCP and Db2 LUW 12.1.5 over TLS. See [Qualification and compatibility](docs/QUALIFICATION.md).

## Installation

The package is **not published to npm yet** and intentionally remains `private: true` during public-release preparation.

For development, install from a local checkout:

```bash
npm install ../JustyBase.Db2.Node
```

After the first public npm release the package will install as:

```bash
npm install @justybase/db2
```

Both module systems are supported: `require("@justybase/db2")` resolves to the CommonJS entry point and `import ... from "@justybase/db2"` resolves to native ESM.

## Quick start

```ts
import { Db2Connection } from "@justybase/db2";

const db = await Db2Connection.connect(
  "db2://user:password@localhost:50000/TESTDB",
);

try {
  const result = await db.query("VALUES 1");
  console.log(result.rows);
} finally {
  await db.close();
}
```

Use environment variables or a secret store for real credentials.

## Public package surface

The root package intentionally exposes the supported connection, pool, values/import, resource-limit and error APIs. Low-level DRDA/DDM codecs, code points, EBCDIC converters and transport implementation helpers are not part of the root consumer contract.

Optional surfaces are explicit subpaths:

```ts
import { open } from "@justybase/db2/compat";
import * as diagnostics from "@justybase/db2/diagnostics";
```

See [API compatibility policy](API-COMPATIBILITY.md).

## TLS

Use `db2s://` and a trusted CA/server certificate path:

```ts
const dsn = `db2s://${encodeURIComponent(user)}:${encodeURIComponent(password)}` +
  `@db2.example.com:50001/TESTDB?ssl_root_ca=${encodeURIComponent("/etc/db2/db2-ca.pem")}`;

const db = await Db2Connection.connect(dsn);
```

IBM CLI-style key/value strings support the documented subset including `Security=SSL` and `SSLServerCertificate=...`. Unsupported security modes are rejected rather than silently downgraded.

## Prepared statements

```ts
const statement = await db.prepare("VALUES CAST(? AS INTEGER)");
const result = await statement.query([42]);
```

Parameters are positional.

## Transactions

```ts
const tx = await db.beginTransaction();
try {
  await tx.execute(
    "UPDATE APP.ACCOUNTS SET BALANCE = BALANCE - ? WHERE ID = ?",
    [25, 7],
  );
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

Uncertain transaction state is not returned to a pool as a reusable clean session.

## Streaming rows

```ts
for await (const row of db.queryStream(
  "SELECT ID, NAME FROM APP.CUSTOMERS ORDER BY ID",
)) {
  console.log(row.values);
}
```

The stream retains bounded protocol state rather than materializing the complete result set.

## Large objects

Materialized values are deliberately bounded. The current alpha limits a single materialized LOB to **16 MiB** and a materialized query result to **32 MiB**.

Progressive row-stream locators are exposed as one-shot stream values:

```ts
for await (const row of db.queryStream("SELECT PAYLOAD FROM APP.DOCUMENTS")) {
  const value = row.values[0];
  if (value && typeof value === "object" && "stream" in value) {
    for await (const chunk of value.stream()) {
      consume(chunk);
    }
  }
}
```

Consume a progressive LOB before advancing past the owning row. Some inline/EXTDTA representations are still materialized and remain subject to the limits above.

## Pooling

`Db2Pool` bounds both physical sessions and admission waiters and provides acquire/connect deadlines, idle/lifetime controls and shutdown behavior.

The default reset behavior validates relevant Db2 session state before reuse so schema/isolation/client identity changes do not leak between callers. See [Advanced driver guide](docs/DRIVER-GUIDE.md).

## Cancellation and deadlines

Most command APIs accept `QueryOptions` with `AbortSignal`/deadline controls. Cancellation uses a separate DRDA interrupt exchange.

An interrupt acknowledgment means Db2 accepted the interrupt request; it does **not** prove that the original command has finished cancelling. Await the original operation to learn the final outcome and whether the session remains reusable.

See [Advanced driver guide](docs/DRIVER-GUIDE.md#cancellation-and-command-deadlines).

## Stored procedures

`Db2Parameter` supports input, output and input/output directions. Dynamic result cursors can be consumed through `Db2ProcedureReader`; output values become final only after the procedure reader has been completely drained/finished.

## Migration from `ibm_db`

A selected Promise-based compatibility facade is available from `@justybase/db2/compat` for applications moving away from Node `ibm_db`.

See [MIGRATION-IBM-DB2.md](MIGRATION-IBM-DB2.md).

This is a source/API migration aid, not binary compatibility with IBM software and not a claim of complete `ibm_db` parity.

## Import

`Db2Connection.importRows()` copies an `AsyncIterable` of rows into an existing table using bounded prepared INSERT execution. It supports atomic and checkpoint-style commit policies and bounded large-value staging.

It is not IBM CLI LOAD.

## Errors and warnings

`Db2Error` and its subclasses preserve categories plus SQLCODE/SQLSTATE where supplied by Db2. Authentication failures expose server security codes without echoing configured passwords.

`addWarningListener()` observes successful SQL warnings without converting them into command failures.

## Testing

```bash
npm ci --ignore-scripts
npm run build
node tools/api/generate-ts-surface.cjs --check
node --test tests/*.test.js
```

Offline TLS tests generate short-lived test certificates and private keys at runtime using OpenSSL; private keys are not stored in the repository.

### Local Db2 integration tests

```bash
npm run test:integration:db2             # smoke suite: connect, DML/types/NULLs, parameters, transactions, errors
npm run test:integration:db2 -- --full   # complete live suite (slow)
```

The command (a thin wrapper around `scripts/test-db2-integration.sh`) starts a temporary IBM Db2 12.1.5 Community container, runs the live integration tests against it, and removes the container, its volumes and temporary files afterwards, also when a test fails or the run is interrupted. It never uses an existing database: `DB2_*` variables from your environment are cleared, `.env` files are not read, and the connection string is built from the container that this invocation started.

Requirements: Linux x86_64, Docker with a running daemon (the Db2 container runs in privileged mode), about 4 GB of free memory, 10 GB of free disk space and Node.js 22 or newer. The first run is slower because Docker has to download the Db2 image (several GB). The script passes `LICENSE=accept` to the IBM image, so running it means accepting the Db2 Community Edition license terms.

CI also validates the public TypeScript surface, generates an SBOM and checks npm package contents. Release dry-run packs once and verifies the exact tarball from clean CommonJS and ESM consumers.

## Documentation

- [Advanced driver guide](docs/DRIVER-GUIDE.md)
- [Qualification and compatibility](docs/QUALIFICATION.md)
- [Migration from `ibm_db`](MIGRATION-IBM-DB2.md)
- [API compatibility policy](API-COMPATIBILITY.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Known limitations

- Alpha software; APIs may still change before a stable release.
- Maintained compatibility testing targets Db2 LUW, not Db2 for z/OS or IBM i.
- Some inline/EXTDTA LOB forms are materialized rather than progressively streamed.
- Procedure-cursor progressive LOBs are handled more conservatively than ordinary query-stream locators.
- Trusted-context switching requires server configuration and privileges. Kerberos/GSSAPI accepts only a caller-supplied token (`gssapi_token_file`); keytab and credential-cache settings are rejected rather than silently ignored.

## License and provenance

The project is licensed under the [MIT License](LICENSE).

The implementation uses the MIT-licensed [`go-db2`](https://github.com/go-db2/go-db2) project as a primary protocol and implementation reference. Required upstream notices for derived or adapted portions are retained in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

The package does not bundle IBM CLI libraries or IBM driver binaries. Application/server licensing remains the responsibility of the user.
