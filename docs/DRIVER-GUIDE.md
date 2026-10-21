# Advanced driver guide

This guide collects operational details that are intentionally kept out of the main README.

## Connections and authentication

The driver accepts URL-style Db2 DSNs and a documented subset of IBM CLI-style key/value strings. Unsupported security/authentication modes fail during validation rather than silently changing behavior.

Use TLS for credentials in transit. Hostname/IP verification remains enabled when TLS is configured.

Authentication is selected with `security_mechanism` (or `secmec`) in a URL query or key/value DSN:

- Not set (the default) requests `3` first. If the server refuses it and lists `9` or `7` (for example `AUTHENTICATION=SERVER_ENCRYPT`), the driver sends a second `ACCSEC` on the same connection with `9`, or `7` when `9` is not listed. Nothing else is followed, and no credentials are sent before the server accepts a mechanism.
- `3` (`USRIDPWD`) sends the user ID and password without DRDA credential encryption.
- `7` (`USRENCPWD`, also `usrencpwd` or `encrypted_password_security`) sends the user ID in clear text and encrypts only the password with DES. It needs a server with `AUTHENTICATION=SERVER_ENCRYPT`.
- `9` (`EUSRIDPWD`) encrypts both user ID and password; `Authentication=SERVER_ENCRYPT` continues to select this mechanism.
- `11` uses the caller-provided GSSAPI token.

An explicitly configured mechanism (including `3` and `Authentication=SERVER`) is never changed. When the server rejects it, `ACCSECRD` lists the mechanisms it accepts (for example `7, 9, 10` on `SERVER_ENCRYPT`, `3, 5` on `SERVER`) and the connection fails with a `Db2UnsupportedFeatureError` naming them, before any `SECCHK` is sent. A failed encrypted authentication is never retried with mechanism 3.

Mechanism 7 requests DES (`ENCALG=1`). A server selecting AES (`ENCALG=2`) or any other algorithm fails closed with a `Db2UnsupportedFeatureError` before credentials are sent. DRDA Diffie–Hellman credential encryption does not authenticate the peer and is vulnerable to man-in-the-middle attacks; use TLS on untrusted networks.

## Pooling

`Db2Pool` bounds physical sessions and queued waiters. It supports acquire/connect deadlines, idle/lifetime controls and explicit shutdown.

The default pool reset probes relevant Db2 session state before reuse and restores observed changes. This prevents caller-specific schema/isolation/client identity state from leaking into later leases.

If probing or restoration fails, the connection is discarded rather than reused.

`drainIdle()` closes idle sessions without stopping the pool; `clearAllDb2Pools()` does the same for every live pool. Active leases are untouched and pools reopen sessions on demand. Both are intended for test teardown, not the hot path.

`changeDatabase()` reconnects a direct session to another database with fail-closed guards (no active transaction, no pooled lease, validated name). `setCurrentSchema()` runs a quoted `SET CURRENT SCHEMA` whose lease-local change the pool reset restores on release.

## Row streaming

`queryStream()` and `queryStreamWithParams()` return async iterables that consume protocol data incrementally.

Breaking out of a stream early triggers cleanup before the connection is returned. A pooled stream holds its lease for the duration of the stream.

## Progressive LOB streaming

Progressive row-stream locators are exposed as one-shot stream values. Binary values yield `Uint8Array` chunks; text values yield strings.

Consume a progressive LOB before advancing the owning row stream. If a value is abandoned, the driver drains remaining locator data before continuing when safe.

Inline and some EXTDTA representations are still materialized and therefore remain subject to materialization limits.

## Streaming inputs

BLOB/CLOB/DBCLOB/XML stream parameter types accept an async iterable plus a declared byte length. The driver sends bounded EXTDTA frames while preserving parameter order.

The declared length is part of the wire contract. Provide a source that contains exactly the intended payload when it may be reused.

## Cancellation and command deadlines

Cancellation uses a separate DRDA interrupt exchange.

The sequence matters:

1. an interrupt request is sent,
2. Db2 can acknowledge the interrupt,
3. the original command must still settle,
4. only the original command result establishes the final outcome.

An interrupt acknowledgment alone does not prove that the command has stopped.

A confirmed cancellation normally surfaces as SQLCODE `-952` / SQLSTATE `57014`. If cleanup cannot be confirmed, the connection is discarded instead of reused.

`AbortSignal`, command deadlines and cancellation-cleanup budgets are supported by the relevant command APIs.

## Transactions

Transaction handles preserve explicit ownership of transactional state. If a transaction-control exchange starts and fails, the connection is closed rather than reused with uncertain server state.

## Procedures

Procedure execution supports input, output and input/output parameters plus dynamic result cursors.

A `Db2ProcedureReader` should be finished or closed so remaining cursors can be drained and final output values collected. Output parameters are not considered final before the procedure lifecycle reaches its terminal state.

## Import pipeline

`importRows()` copies an async row source into an existing table using bounded prepared INSERT execution.

Policies include atomic and commit-every-rows/checkpoint style behavior. Batches are bounded by row and estimated-byte limits. Progressive source LOBs can be staged in private temporary storage under a shared process quota before the source cursor advances.

`notifyAfterRows` (default 10,000) additionally reports progress after each staged interval and doubles as a cooperative abort hook: an observer error stops the import keeping the last confirmed checkpoint. `tableLock` takes an exclusive lock for atomic imports (rejected with checkpoint policies). `commandTimeoutMs` bounds each LOCK TABLE and row INSERT; a timed-out write discards the session fail-closed.

The importer is not IBM CLI LOAD.

## Warnings

`addWarningListener()` exposes successful SQL warnings while preserving command success. Server errors remain exceptions.

## Error model

`Db2Error` categories distinguish configuration, closed connections, missing databases, authentication, authorization, server SQL, transport/protocol failures and unsupported behavior. SQLCODE/SQLSTATE are retained when supplied by Db2.

Credentials are not included in authentication error messages.

## Fail-closed behavior

The driver intentionally discards uncertain connections rather than speculating that protocol state is reusable. Examples include:

- incomplete cursor cleanup,
- reply-chain/resource-limit violations,
- transport failure during an active exchange,
- unconfirmed cancellation cleanup,
- failed transaction control,
- impossible/malformed server reply structures.

## Compatibility facade

The Promise-based migration facade provides selected `ibm_db`-style workflows. It is an application migration aid, not binary compatibility and not a complete implementation of IBM's API surface. See `MIGRATION-IBM-DB2.md`.
