<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-008: Add CI build/test matrix and full pgrx pg_test integration coverage

| Field | Value |
| :--- | :--- |
| Title | Add CI build/test matrix and full pgrx pg_test integration coverage |
| Severity | medium |
| Status | fixed |
| Kind | test |
| Files | `Cargo.toml`, `src/lib.rs`, `docker/Dockerfile.pg17-cuda`, `.github/workflows/ci.yml` |

## Description
Wave 1 (`error.rs`, `guc.rs`, `runtime.rs`, `lib.rs`, `ipc.rs`, `mvcc.rs`) provides pure-Rust unit tests runnable under `--no-default-features --features cuda-stub`, and `src/lib.rs` defines initial `#[pg_test]` tests under `#[cfg(all(feature = "pgrx", any(test, feature = "pg_test")))]`. However:
1. Neither `cargo test --no-default-features --features cuda-stub` nor `cargo pgrx test pg15..pg18` has been executed yet (deferred to the coordinator / Wave 2 validation pass).
2. `docker/` and CI workflows do not yet exercise the full feature matrix (`cuda-stub`, `cuda`, `pg15`, `pg16`, `pg17`, `pg18`, and `clippy --all-targets -- -D warnings`).
3. End-to-end `#[pg_test]` coverage for `bolt.cache_table`, `bolt.sync_table`, `bolt.tables`, `bolt.query`, `bolt.query_json`, `pg_bolt.max_vram_mb` LRU eviction, and `CustomScan` EXPLAIN plans awaits completion of `arrow_bridge.rs`, `planner.rs`, `custom_scan.rs`, and `bgworker.rs`.

## Reproduction
Inspect `C:\craton\pg_bolt\docker` (empty) and `src/lib.rs` `pg_tests` module. Real PostgreSQL SQL integration tests cannot run until all Wave 1 modules are written and `cargo pgrx test` executes.

## Suggested fix
1. Coordinator runs `cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings`, `cargo test --no-default-features --features cuda-stub`, `cargo check --no-default-features --features "pg17 cuda-stub"`, and `cargo pgrx test pg17` after Wave 1 merges.
2. Expand `pg_tests` in Wave 2 to cover `bolt.cache_table` hit/miss behavior, `bolt.sync_table`, `bolt.tables()`, `bolt.query()` / `bolt.query_json()` (on GPU CI), `CustomScan` plan selection, and `pg_bolt.enabled = off` error paths.
3. Populate `docker/` Dockerfiles and CI scripts for PostgreSQL 15-18 + CUDA Toolkit >= 12.
