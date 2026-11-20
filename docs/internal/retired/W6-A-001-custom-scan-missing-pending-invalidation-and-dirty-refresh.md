<!-- SPDX-License-Identifier: Apache-2.0 -->

# W6-A-001: `CustomScan` execution omits `reconcile_pending_invalidations` and `refresh_dirty_referenced_tables`

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` execution omits `reconcile_pending_invalidations` and `refresh_dirty_referenced_tables` |
| Severity | high |
| Status | retired |
| Kind | bug |
| Files | `src/custom_scan.rs:807-845`, `src/lib.rs:945-993` |

## Description
`bolt.query` and `bolt.query_json` in `src/lib.rs:1622-1640` explicitly invoke:
1. `pg_hooks::reconcile_pending_invalidations()` to process relcache catalog flushes and DDL drops/modifications.
2. `pg_hooks::refresh_dirty_referenced_tables(sql)` to re-extract any dirtied relations prior to running the query on Bolt.

However, transparent `CustomScan` plan execution (`bolt_exec_custom_scan` in `src/custom_scan.rs:807-845`) directly dispatches `state.execute(ScanExecutionBackend::Direct(rt))` or `BgWorker(&client)` without first reconciling pending invalidations or auto-refreshing dirty tables.

If a cached relation was dirtied by prior DML or altered by concurrent DDL in the session, `CustomScan` execution either:
1. In `Direct` mode: fails with `PgBoltError::Mvcc("cached table is dirty and awaiting MVCC sync")`. If `fallback_on_oom` is false, this triggers `pgrx::error!` and query abortion instead of refreshing or falling back to CPU scan.
2. In `BgWorker` mode: risks executing against stale VRAM data if the dirty flag was only set in backend-local transaction state.

## Reproduction
1. Cache table `sales` via `bolt.load_table('sales')`.
2. Insert a row into `sales`: `INSERT INTO sales VALUES (...)`. Table is marked dirty locally.
3. Run an offloadable analytical query handled by the transparent planner hook: `SELECT region_id, SUM(amount) FROM sales GROUP BY region_id`.
4. Actual result: `bolt_exec_custom_scan` fails with `PgBoltError::Mvcc` because the table was not refreshed before custom scan execution. Expected: auto-refresh dirty tables or cleanly fall back to CPU execution.

## Suggested fix
1. In `bolt_exec_custom_scan` before invoking `state.execute(...)`:
   - Call `crate::pg_hooks::reconcile_pending_invalidations()`.
   - Call `crate::pg_hooks::refresh_dirty_referenced_tables(&state.plan().sql)`.
2. In `should_fallback_on_error`, treat `PgBoltError::Mvcc` as a valid trigger to fall back to the CPU subplan (`(*bolt_node).fallback_plan`) when configured.

## Resolution
- In `bolt_exec_custom_scan`, added `reconcile_pending_invalidations()` and `refresh_dirty_referenced_tables(&state.plan.sql)` prior to executing direct or bgworker scan backends.
- Added `PgBoltError::Mvcc(_)` into `is_gpu_execution_error`, safely allowing host CPU subplan fallback when `fallback_on_oom` is active.
- Added regression test `mvcc_dirty_error_triggers_fallback_when_allowed` in `src/custom_scan.rs`.
- Retired in Wave 6.
