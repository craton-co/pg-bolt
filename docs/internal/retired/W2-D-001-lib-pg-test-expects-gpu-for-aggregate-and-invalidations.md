<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-D-001: `lib.rs` `pg_test` expects `"Gpu"` for `GROUP BY` aggregate and lacks OID invalidation wiring

| Field | Value |
| :--- | :--- |
| Title | `lib.rs` `pg_test` expects `"Gpu"` for `GROUP BY` aggregate and lacks OID invalidation wiring |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:27-34,76-125,251-327,436-439`, `src/planner.rs:46-61,1432-1442` |

## Description
Two gaps remain in `src/lib.rs` after Wave 2 planner and runtime updates:
1. `pg_tests::test_validation_and_table_lifecycle` (`src/lib.rs:436-439`) asserts `"Gpu"` for `bolt::explain_tier("SELECT id, SUM(amount) FROM sales GROUP BY id")`, whereas `craton-bolt` 0.12.0 plans `Aggregate` nodes on `ExecutionTier::Hybrid` (the standalone tests in `src/runtime.rs:1880-1883`, `src/bgworker.rs:859-867`, and `src/planner.rs:2115-2156` have been updated, leaving `lib.rs` out of sync).
2. `src/planner.rs` now exposes `bump_catalog_epoch()`, `invalidate_relation_oid(rel_oid)`, and `register_planner_table_with_oid(meta, schema, rel_oid)`, but `src/lib.rs` does not yet register `pg_xact`/executor/utility DML invalidation hooks or pass resolved `Oid` values when caching PostgreSQL relations.

## Reproduction
1. Run `cargo pgrx test` for `pg16` (or inspect `src/lib.rs:436-439`).
2. `test_validation_and_table_lifecycle` receives `"Hybrid"` instead of `"Gpu"`.
3. Modify a cached table via SQL `INSERT`/`UPDATE`/`DELETE` and observe `INVALIDATED_REL_OIDS` in `src/planner.rs` is not notified unless wired from `src/lib.rs`.

## Suggested fix
- Update `src/lib.rs:436-439` to expect `"Hybrid"` for `GROUP BY` and add a pure-projection `bolt::explain_tier` assertion expecting `"Gpu"`.
- Resolve relation OIDs in `bolt::{load_table, cache_table, sync_table}` / executor hooks in `src/lib.rs` and invoke `planner::invalidate_relation_oid(rel_oid)` / `planner::bump_catalog_epoch()` on DML commits/aborts.

## Resolution (Wave 2-3)
- `pg_tests::test_validation_and_table_lifecycle` (`src/lib.rs`) asserts `"Hybrid"` for `GROUP BY`, `"Gpu"` for pure projection, and `bolt::explain_sql`.
- `pg_hooks::install()` (`_PG_init`) wires `ExecutorEnd`, `ProcessUtility`, `RegisterXactCallback`, `RegisterSubXactCallback`, and `CacheRegisterRelcacheCallback` into `planner::invalidate_relation_oid` and `planner::register_planner_table_with_oid`, and `pg_hooks::sync_dirty_state` synchronizes dirty/clean state with both direct and `BoltBgWorker` runtimes (`client.set_table_dirty`).
- `pg_tests::test_dml_dirty_tracking_and_subxact_rollback` verifies subtransaction rollback restoration, committed DML dirty tracking, `api::tables()` worker parity, and `bolt::cache_table` refresh.
