<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-A-001: `custom_scan.rs` treats `shared_bgworker_client()` as `Option` instead of `PgBoltResult`

| Field | Value |
| :--- | :--- |
| Title | `custom_scan.rs` treats `shared_bgworker_client()` as `Option` instead of `PgBoltResult` |
| Severity | critical |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:607-621`, `src/bgworker.rs:652-676` |

## Description
Under `#[cfg(feature = "pgrx")]`, `bolt_exec_custom_scan` (`src/custom_scan.rs:607-611`) evaluates:
```rust
let bg_client = if (*bolt_node).use_bgworker {
    crate::bgworker::shared_bgworker_client()
} else {
    None
};
```
and matches `bg_client` against `Some(client)` / `None` (`src/custom_scan.rs:612-621`). However, `crate::bgworker::shared_bgworker_client()` (`src/bgworker.rs:652`) returns `PgBoltResult<BgWorkerClient>`, not `Option<BgWorkerClient>`. Compiling with `--features pg15|pg16|pg17|pg18` fails type-checking in `src/custom_scan.rs`.

## Reproduction
1. Inspect `src/custom_scan.rs:607-621` and `src/bgworker.rs:652`.
2. Build with PostgreSQL extension features enabled (`cargo check --features pg16`).
3. Compiler reports `mismatched types: expected Result<BgWorkerClient, PgBoltError>, found Option<_>` in `else { None }`.

## Suggested fix
Unpack `crate::bgworker::shared_bgworker_client()` cleanly as `Result<BgWorkerClient, PgBoltError>` in `bolt_exec_custom_scan`:
- When `(*bolt_node).use_bgworker` is `true`, call `shared_bgworker_client()` and route `Ok(client)` through `ScanExecutionBackend::BgWorker(&client)`, propagating `Err(err)` into the existing `should_fallback_on_error` / CPU fallback path.
- Add or run a `#[cfg(feature = "pgrx")]` check (`cargo pgrx test`) covering `CustomScan` execution with `pg_bolt.use_bgworker = on`.

## Resolution (Wave 3 Agent D)
- `src/custom_scan.rs` (`bolt_exec_custom_scan`, lines 664-677): unpacks `crate::bgworker::shared_bgworker_client()` cleanly as `PgBoltResult<BgWorkerClient>` via `.and_then(|client| state.execute(ScanExecutionBackend::BgWorker(&client))).map(|_| ())` when `(*bolt_node).use_bgworker` is `true`, routing `Err(err)` into the `should_fallback_on_error` / PostgreSQL `fallback_plan` path without `Option`/`Result` mismatch.
- Note: coordinator build / `#[pg_test]` verification pending (Cargo/rustc invocations are disabled for worker agents). Also see `W3-D-003` regarding fallback eligibility when `shared_bgworker_client()` / IPC returns `PgBoltError::Ipc`.
