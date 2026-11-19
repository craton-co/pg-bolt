<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-001: Cargo optional dev-dependency error and crate dependency alignment

| Field | Value |
| :--- | :--- |
| Title | Cargo optional dev-dependency error and crate dependency alignment |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `Cargo.toml`, `build.rs`, `src/error.rs`, `src/runtime.rs` |

## Description
The initial specification and draft `PG_BOLT_PLAN_AND_CODE.md` contained several manifest and `craton-bolt` API mismatches that prevent building or testing `pg_bolt`:
1. Placing `pgrx-tests = { version = "0.19", optional = true }` under `[dev-dependencies]` is rejected by Cargo (`dev-dependencies are not allowed to be optional`), while an unconditional `[dev-dependencies] pgrx-tests = "0.19"` forces `pgrx`/`pgrx-pg-sys` (and `pg_config` discovery) to compile during standalone `cargo test --no-default-features --features cuda-stub`.
2. `PG_BOLT_PLAN_AND_CODE.md:132-137` referenced `arrow-* = "54"`, `thiserror = "2.0"`, and `craton-bolt = { default-features = true }`, whereas `../bolt/Cargo.toml` pins `arrow = "53"`, `sqlparser = "0.52"`, `thiserror = "1"`, `parking_lot = "0.12"`, `log = "0.4"`, and exposes `cudarc`, `cuda-stub`, and `storage` features.
3. `PG_BOLT_PLAN_AND_CODE.md:203,257` called nonexistent `Engine::new()` and `RecordBatch::from(handle)` instead of `Engine::builder().sql_semantics(SqlSemantics::Standard).build()` and `handle.into_record_batch()`.

## Reproduction
Running `cargo test --no-default-features --features cuda-stub` against the draft `Cargo.toml` fails manifest parsing if `optional = true` is under `[dev-dependencies]`, or fails on missing PostgreSQL headers/`pg_config` if `pgrx-tests` is non-optional.

## Suggested fix
Fixed in Wave 1 (`Cargo.toml`, `build.rs`, `src/error.rs`, `src/runtime.rs`):
- Moved `pgrx-tests = { version = "0.19", optional = true }` to `[dependencies]`, gated via `pg_test = ["pgrx", "dep:pgrx-tests"]` and weak feature propagation `"pgrx-tests?/pg15"`..`"pgrx-tests?/pg18"`.
- Pinned `craton-bolt = { path = "../bolt", version = "0.12.0", default-features = false }` and aligned all `arrow*`, `sqlparser`, `thiserror`, `parking_lot`, and `log` versions with `../bolt`.
- Updated `BoltSessionRuntime` to use `Engine::builder().sql_semantics(SqlSemantics::Standard)` and `handle.into_record_batch()`.
- Follow-up verification: run `cargo test --no-default-features --features cuda-stub` once all Wave 1 modules are present.
