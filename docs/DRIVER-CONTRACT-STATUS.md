# JDC v1 status — TypeScript / Node.js

Contract: [`docs/DRIVER-CONTRACT.md`](DRIVER-CONTRACT.md) (JDC v1).
Last reviewed: 2026-11-08.

This file is the repo-specific conformance record. It is not part of the
normative text. `compliant` means every `MUST` in the group is met; `partial`
means some are met and the rest are listed; `gap` means the group is not met.
`gap` and `partial` entries are implementation work for the next conformance
pass; this file records them, it does not waive the requirement.

## Summary

| Status | Requirements |
| --- | --- |
| compliant | ERR-02, ERR-05–09, OUT-01, OUT-06–09, DSN-01, DSN-03–06, DSN-09–10, LIM-03–05, LIM-08, POOL-01–07, SES-01–05, SES-07–10, VAL-01–10, CAN-01–09, BAT-01, BAT-03–06, BAT-07–09 |
| partial | ERR-01, ERR-03–04, OUT-02–05, DSN-02, LIM-01, LIM-06 |
| gap | DSN-07–08, LIM-02, LIM-07, SES-06, BAT-02 |

## Details

| Requirements | Status | Notes |
| --- | --- | --- |
| ERR-01 | partial | Most failures are `Db2Error`; timeout and batch wrappers now expose `category`/`outcome` too. Raw socket errors still surface as plain `Error`. |
| ERR-02 | compliant | All canonical categories exist, including `authorization`, `parameter-conversion`, `server-response`, `operation-cancelled`, `timeout` and `other`. Extras (`server`, `invalid-connection-string`, `unsupported-feature`) map through `canonicalCategory`. |
| ERR-03–04 | partial | SQL/auth/authz/transport/protocol/resource/pool/batch mapping is stable; `canonicalCategory` gives cross-driver equivalence, but raw socket errors are still unclassified. |
| ERR-05–08 | compliant | SQLCODE/SQLSTATE, message format (`SQLCODE=… SQLSTATE=…`), redaction and batch progress are present. |
| ERR-09 | compliant | Serialized diagnostics use `canonicalCategory`; `server` → `sql-server`, `invalid-connection-string` → `configuration`, `unsupported-feature` → `unsupported`. |
| OUT-01 | compliant | Public spellings are exactly `not-started`, `confirmed`, `unknown`. |
| OUT-02–05 | partial | A command timeout records whether the first request was written and reports `not-started`/`unknown`; batch wrappers derive the outcome from the failing set. Raw transport errors still do not carry an outcome. |
| OUT-06–07 | compliant | No automatic replay of mutating work; uncertain sessions are discarded, and the pool now discards a lease whose classified outcome is `unknown`. |
| OUT-08 | compliant | Batch errors expose completed sets, in-flight index and rows, and derive `confirmed`/`not-started`/`unknown` from the cause and failing set. |
| OUT-09 | compliant | A confirmed `-952` is `operation-cancelled` with `confirmed`; unconfirmed settlement is `unknown`. |
| DSN-01 | compliant | Case-insensitive key/value and URL parsing; last value wins. |
| DSN-03–06 | compliant | `currentschema`, security mechanism, TLS and authentication shorthand are implemented. |
| DSN-02 | partial | Core aliases are present except `krb5_principal`/`kerberos_principal`/`client_principal` and `kerberos_spn`. |
| DSN-07–08 | gap | No protocol-limit keys (the SHOULD for LOB/streamed limits is not met), and unknown `krb5`/`kerberos`/`gssapi` keys are stored as opaque parameters instead of failing closed. |
| DSN-09–10 | compliant | No password in diagnostics/config dumps; required identity values fail before I/O. |
| LIM-01 | partial | No configurable protocol limits; invalid numeric values for the exposed options are generally ignored rather than rejected. |
| LIM-02 | gap | DSS 256 MiB, DDM 256 MiB, reply chain 512 MiB and 2048 packets exceed the JDC defaults; 16 MiB LOB, 32 MiB result, 1 GiB streamed and 64 KiB chunks match. |
| LIM-03 | compliant | The constants are within the hard ceilings; there are no configurable values to clamp. |
| LIM-04–05 | compliant | Materialized LOB/result limits fail closed. |
| LIM-06 | partial | Reply-chain packet/byte caps are enforced, but several paths throw a generic `Error` instead of a resource-limit category. |
| LIM-07 | gap | `executeBatch` has no row/byte bound; import staging is bounded. |
| LIM-08 | compliant | A process-wide 10 GiB spool budget is reserved before disk writes and released on success, error and cancellation. |
| POOL-01–07 | compliant | Defaults, admission errors, reset/discard, drain/shutdown and diagnostics counters match the contract. |
| SES-01–05 | compliant | Connect/close, prepare/execute, transactions, `changeDatabase` and `setCurrentSchema` match. |
| SES-06 | gap | No public current-schema getter. |
| SES-07 | compliant | Isolation is transaction-scoped (`beginTransaction(isolation)`) and restores the previous register. |
| SES-08–10 | compliant | Per-session transaction state, ordered batch progress and `AbortSignal` support are present. |
| VAL-01–10 | compliant | Exact strings are used for DECIMAL/DECFLOAT and temporal values; NULL, binary, CP500, metadata and progressive LOB rules are met. |
| CAN-01–02 | compliant | Two-step interrupt and original-operation settlement are implemented. |
| CAN-03 | compliant | A confirmed `-952` is `operation-cancelled` with `confirmed`; SQLCODE/SQLSTATE are preserved. |
| CAN-04–07 | compliant | Race handling, unconfirmed discard, idle cancellation and idempotent cancel are covered. |
| CAN-08 | compliant | `Db2TimeoutError` carries `category: timeout` and a phase-derived outcome; a confirmed `-952` becomes `confirmed`, a settlement failure `unknown`. |
| CAN-09 | compliant | Cancellation is safe from another task while the operation lock is held. |
| BAT-01 | compliant | Ordered execution, stop-on-first-failure and completed/affected counts. |
| BAT-02 | gap | `executeBatch` accepts no bounded options; it loops over all sets. |
| BAT-03–05 | compliant | Batch cause/progress, import policies and import defaults (256 / 8 MiB / 10,000 / atomic) match. |
| BAT-06 | compliant | The 1 GiB streamed-LOB budget and 64 KiB chunks are constants (`DB2_RESOURCE_DEFAULTS`); they are not DSN-configurable, which the contract does not require. |
| BAT-07–09 | compliant | Materialized limits fail closed, progressive LOB has a cancel signal, and the large-data path streams. |
