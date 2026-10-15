# Migrating Node.js callers from `ibm_db`

The compatibility facade covers the Promise-based patterns used by JustyBase's Db2 extension: opening a connection, running materialized object-row queries, and fetching a query result incrementally with column metadata. The implementation uses this package's TypeScript DRDA session and does not load `ibm_db`, IBM CLI, or ODBC.

The facade is intentionally published as the explicit experimental subpath `@justybase/db2/compat`; the supported native driver API remains at `@justybase/db2`.

## Open a connection

```ts
import { open } from "@justybase/db2/compat";

const db = await open("Server=db.example.test:50000;Database=TESTDB;UID=db2user;PWD=secret");
try {
  // queries
} finally {
  await db.close();
}
```

`open()` accepts the package's URL DSN, key/value DSN, or a `Config` object. Common IBM keys `Server`, `Database`, `UID`/`User ID`, and `PWD`/`Password` are accepted; `Server=host:port` supplies both endpoint parts. Review security, TLS certificate, and authentication settings when migrating. Connection pools from `ibm_db` are not shared: create a `Db2Pool` from `@justybase/db2` explicitly when a pool is needed.

## Materialized query results

```ts
const rows = await db.query("SELECT ID, NAME FROM APP.CUSTOMER WHERE ID = ?", [42]);
for (const row of rows) console.log(row.ID, row.NAME);
```

`query()` returns object rows keyed by Db2 column labels, which fits the common `ibm_db` `Database.query()` consumption pattern. This materializes the complete result in memory. Use the native `db.connection.query()` if positional value arrays are preferred, or the cursor API below for bounded-memory reads.

## Incremental `queryResult` and fetch

```ts
const cursor = await db.queryResult(
  "SELECT ID, NAME FROM APP.CUSTOMER WHERE ID > ? ORDER BY ID",
  [100],
);
try {
  const metadata = cursor.getColumnMetadata();
  console.log(metadata.map(column => column.SQL_DESC_NAME));
  for (let row = await cursor.fetch(); row !== null; row = await cursor.fetch()) {
    console.log(row.ID, row.NAME);
  }
} finally {
  await cursor.close();
}
```

`fetch()` returns one object row or `null`; `fetchAll()` materializes the remaining rows. Metadata is available before the first fetch, including empty result sets. `close()` ends the row generator; the underlying driver drains an open cursor before releasing the session. Progressive LOB values returned in cursor rows remain the native one-shot `Db2LobStream` values and must be consumed before fetching a later row. Pass `QueryOptions` as the final argument to `query` or `queryResult` to set `AbortSignal` and command deadlines.

Duplicate or empty column labels are disambiguated in object rows and name accessors (`ID`, `ID__1`, ...; empty labels become `COLUMN<n>`), so no value is silently overwritten. `getColumnMetadata()` keeps the original server labels in `SQL_DESC_NAME`. `getName()`/`getValue()` use the suffixed keys consistently.

Failures from the facade reject with the driver's typed errors and additionally expose IBM-spelled `SQLCODE`/`SQLSTATE` aliases and the node-ibm_db lower-case `sqlcode`/`sqlstate` properties, next to the native `sqlCode`/`sqlState`. A server error message uses the node-ibm_db token shape, for example `[JustyBase][DB2] SQL0204N  DB2INST1.MISSING is an undefined name.  SQLSTATE=42704`, so code that matches `SQLnnnnN` or `SQLSTATE=` in the message keeps working. The native API keeps its `DB2 SQLCODE=... SQLSTATE=...` messages unchanged.

Note that `sqlstate` carries the native Db2 SQLSTATE. node-ibm_db reports an ODBC-mapped value for some errors; for a missing table it reports `42S02` where Db2 reports `42704`.

`queryResult()` also exposes `getColumnMetadataSync()` and `getColumnNamesSync()` aliases. They are synchronous because the metadata has already been received before the cursor is returned. It additionally implements JustyBase's reader-shaped `fieldCount`, `read()`, `getName()`, `getTypeName()`, `getValue()`, `nextResult()`, and `close()` methods; row operations remain asynchronous. `nextResult()` returns `false` because multiple result sets are exposed through the native procedure API rather than this compatibility cursor. Procedure cursors expose terminal `outputParameters` after `finish()`.

For consumers shaped around JustyBase's `createCommand()`, use `database.createCommand(sql, params)`. Its `executeReader()` returns the same reader adapter, and DML populates `_recordsAffected`; `execute()` drains row-returning commands and completes non-row commands. `commandTimeout` is expressed in seconds to match JustyBase's `DatabaseCommand` contract and is translated to the driver's millisecond deadline. `cancel()` aborts the command's `AbortSignal`. This adapter does not implement a mutable parameter collection or synchronous `executeSql`.

## Column metadata and value shapes

`getColumnMetadata()` returns `index` (1-based), `SQL_DESC_NAME`, `SQL_DESC_TYPE_NAME`, `SQL_DESC_CONSIZE_TYPE` (ODBC type code), `SQL_DESC_DISPLAY_SIZE`, `SQL_DESC_PRECISION`, `SQL_DESC_SCALE`, and `SQL_DESC_LENGTH`. Except for the precision, the values were checked field by field against node-ibm_db 4.0.1 on Db2 LUW 12.1.5 for SMALLINT, INTEGER, BIGINT, DOUBLE, REAL, DECIMAL, CHAR, VARCHAR, DATE, TIME, TIMESTAMP (scale 0 and 6), BLOB, CLOB and BINARY columns. Binary types report the declared length in `SQL_DESC_LENGTH`; other types report the display size.

`SQL_DESC_PRECISION` reports the declared precision. node-ibm_db reports 0 for every probed column; the facade deliberately does not copy that.

Binary values (BLOB, BINARY, VARBINARY) are Node.js `Buffer`s from both `query()` and the cursor, as in node-ibm_db.

Deliberate differences from node-ibm_db, where the facade keeps the more exact or more accurate value:

- DECIMAL values are strings, which keeps every digit; node-ibm_db returns a JavaScript number, which is an IEEE-754 double. Pass `{ decimal: "number" }` as the second argument of `open()` to get numbers.
- BIGINT values are JavaScript `bigint`; node-ibm_db returns a string. `JSON.stringify` rejects a `bigint`, so pass `{ bigint: "string" }` to `open()` when results are serialized.
- `sqlstate` carries the native Db2 SQLSTATE (`42704` for a missing table). node-ibm_db reports the ODBC-mapped value (`42S02`).
- VARBINARY reports `VARBINARY` in `SQL_DESC_TYPE_NAME`; node-ibm_db reports a `VARCHAR () FOR BIT DATA` name.
- Metadata for an unknown column type reports `SQL_DESC_CONSIZE_TYPE` 0.

```ts
const db = await open(dsn, { decimal: "number", bigint: "string" });
```

## Mapping common `ibm_db` calls

| `ibm_db` pattern | Compatibility facade |
|---|---|
| `await ibm_db.open(dsn)` | `await open(dsn)` |
| `await database.query(sql, params)` | `await database.query(sql, params)` |
| `await database.queryResult(sql, params)` | `await database.queryResult(sql, params)` |
| `result.fetch()` | `await result.fetch()` |
| `result.fetchAllSync()` | `await result.fetchAll()` |
| `result.getColumnMetadataSync()` | `result.getColumnMetadata()` |
| `result.getColumnNamesSync()` | `result.getColumnNames()` |
| `database.close()` | `await database.close()` |
| JustyBase `executeSql(sql)` result shape | `await database.executeSql(sql, params, options)` |
| JustyBase `createCommand(sql)` | `database.createCommand(sql, params)` |
| `querySync`, `openSync`, `fetchAllSync` | No synchronous API; use the Promise API |
| ODBC-specific options/results | Review individually; not emulated |

The migration facade deliberately does not reproduce callback overloads, synchronous methods, ODBC result tuples, or undocumented `ibm_db` behavior. For prepared statements, transactions, batch DML, row streams, procedure outputs, cancellation, and pooling, use the native `Db2Connection`/`Db2Pool` APIs from `@justybase/db2`. For example, `db.connection.prepare(sql)`, `statement.execute(params)`, `db.connection.beginTransaction()`, and `db.connection.executeBatch(sql, sets)`.

## JustyBase integration notes

The existing JustyBase Db2 extension consumes the `ibm_db` Promise API behind its own `Db2Connection`: `open`, `query`, `queryResult`, `fetch`, `fetchAllSync`, column metadata, and result/database close. The facade supplies asynchronous equivalents for the central connection and cursor flows, including the metadata method spellings the extension invokes after its asynchronous `queryResult()`. Its `executeSql()` returns the same `{ columns, rows, recordsAffected }` data shape asynchronously. The extension's current `executeSql` contract is synchronous; migration therefore requires its host contract to accept a Promise or a small adapter at that boundary. Do not block the Node event loop to preserve synchronous method names.

Before replacing a production extension backend, run its existing extension-host smoke checks and its result-shape tests against this driver. Confirm SQL type metadata, duplicate/unnamed column labels, LOB handling, query errors, connection lifecycle, and cancellation behavior with the target Db2 server and queries. Compatibility is intentionally API-shaped and does not claim complete behavioral parity with IBM's CLI provider.

Differential SELECT/type parity against the official provider is covered by `tests/live/parity-ibmdb.test.js` (`./scripts/test-db2-integration.sh --parity`, blocking `parity-ibmdb` CI lane, pinned test-only `ibm_db@4.0.1` oracle). It verifies value shapes for scalars, NULLs, temporal/text/binary values and selected input conversions; it does not certify DML, procedures, LOB streaming edge cases or cancellation parity.

Two known oracle-side limitations are quarantined inside that suite (verified live against Db2 12.1.5, all 11 parity cases green). The first is a confirmed defect in `node-ibm_db` itself, isolated with CLP ground truth and a direct CLI (C, `SQL_C_DOUBLE`) probe: the CLI detects that DECFLOAT `Infinity`/`-Infinity`/`NaN` cannot be converted to `SQL_C_DOUBLE` and returns `CLI0111E`/SQLSTATE `22003` with the output buffer left untouched, but `node-ibm_db` binds DECFLOAT as `SQL_C_DOUBLE` (`src/odbc_result.cpp:1284`) and then reads the buffer without checking the fetch error, returning the previous column's value, an uninitialized denormal, or zero. The server, the CLI, this harness and ts-db2 are all correct; the values are therefore checked against Db2-documented literals instead. The second is `ibm_db` `query()` parameter binding rejecting binary/LOB host variables with SQL0301N even for typed columns (LOB writes go through ts-db2, both drivers read back).
