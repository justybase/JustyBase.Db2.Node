# API compatibility policy — JustyBase Db2 for Node.js

The project mechanically tracks its consumer-visible TypeScript API so accidental export changes fail CI.

The snapshot lives in [`api/ts-surface.json`](api/ts-surface.json) and is generated from the emitted root declaration surface using a TypeScript-aware AST parser.

## Public package boundaries

The supported root package is:

```text
@justybase/db2
```

It exposes the connection, pool, configuration/DSN, errors/outcomes, resource controls, values and import APIs intended for normal applications.

Optional surfaces are explicit subpaths:

```text
@justybase/db2/compat
@justybase/db2/diagnostics
```

Low-level DRDA/DDM codecs, code points, EBCDIC/SQLDTA converters, raw transport/session helpers and other implementation modules are intentionally not exported from the package root. Their emitted files are implementation detail and are not a compatibility contract.

## Stability tiers

| Tier | Surface | Alpha policy |
| --- | --- | --- |
| Supported alpha | Root `@justybase/db2`: `Config`/DSN parsing, `Db2Connection`, `Db2Pool`, structured errors/outcomes, resource limits, values and import API | Changes must be deliberate, documented and reflected in the snapshot. Prefer migration guidance where practical. |
| Experimental | `@justybase/db2/compat`, `@justybase/db2/diagnostics`, environment-dependent security/auth helpers reachable through supported APIs | May change between alpha releases; changes must still be explicit and documented. |
| Internal | DRDA/DDM codecs, code points, raw parser/transport helpers and converter implementation modules | Not public package API. No compatibility promise. |

Before 1.0, Semantic Versioning permits breaking changes through minor-version changes. The current `0.1.0-alpha.1` is intentionally not presented as a stable 1.0 contract.

## Snapshot behavior

The current parser records exported interfaces, type aliases, classes, functions and consumer-visible class members from the root declaration entry point. Private fields/methods and documentation comments are excluded so implementation-only refactors do not create false API changes.

The snapshot is a guard for the root application API. Explicit experimental subpaths are tested separately through package consumer smoke tests.

## Commands

```bash
npm run build
node tools/api/generate-ts-surface.cjs          # regenerate api/ts-surface.json
node tools/api/generate-ts-surface.cjs --check  # CI check; fails on drift
```

## Change process

1. Decide whether the export/API change is intentional.
2. Update implementation/tests.
3. Update README, CHANGELOG, migration notes and affected guides.
4. Regenerate the API snapshot in the same reviewed change.
5. Provide migration guidance for supported-alpha consumers where practical.

Do **not** regenerate the snapshot merely to make CI green.

The npm package remains `private: true` in the private release-preparation repository. The fresh public repository will remove that publication guard only as an explicit release step.
