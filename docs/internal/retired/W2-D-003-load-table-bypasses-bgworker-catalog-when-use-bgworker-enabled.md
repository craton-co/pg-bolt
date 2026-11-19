<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-D-003: Control-plane APIs (`load_table`/`query`/`evict_table`) bypass `BoltBgWorker` when `pg_bolt.use_bgworker` is enabled

| Field | Value |
| :--- | :--- |
| Title | Control-plane APIs (`load_table`/`query`/`evict_table`) bypass `BoltBgWorker` when `pg_bolt.use_bgworker` is enabled |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:76-159,251-355`, `src/custom_scan.rs:607-621`, `src/bgworker.rs:652-676` |

## Description
Under `W1-D-004`, `pgrx_custom_scan::bolt_exec_custom_scan` (`src/custom_scan.rs:607-621`) dispatches `CustomScan` queries through `crate::bgworker::shared_bgworker_client()` whenever `pg_bolt.use_bgworker` is enabled (otherwise falling back to thread-local `with_bolt_runtime`).

However, `pg_bolt::api` (`src/lib.rs:76-159`) and the SQL UDFs (`bolt::{load_table, cache_table, sync_table, evict_table, query, query_json, explain_tier, explain_sql, status, reset_vram}`) still execute exclusively against thread-local `with_bolt_runtime`. As a result, a table loaded via `SELECT bolt.load_table('public.sales', 'sales')` is registered only in the client backend's thread-local `BoltSessionRuntime`, not in the persistent `BoltBgWorker` runtime that `ExecCustomScan` queries.

## Reproduction
1. Leave `pg_bolt.use_bgworker = on` (default) and run `SELECT bolt.load_table('public.sales', 'sales');`.
2. Execute a query intercepted by `pg_bolt_planner_hook` (`CustomScan`).
3. `bolt_exec_custom_scan` dispatches to `shared_bgworker_client()`, which does not contain `"sales"` in its `BoltSessionRuntime`.

## Suggested fix
- Route `api::{load_batch, cache_batch_if_needed, sync_delta_batch, evict_table, tables, query, explain_tier, explain_sql, status}` through `bgworker::shared_bgworker_client()` when `current_config().use_bgworker` is true (alongside `W2-C-002` cross-process DSM transport and `_PG_init` BGWorker registration).
- Add a test verifying `bolt.load_table` followed by `CustomScan` execution under `use_bgworker = true` hits the cached table in `BoltBgWorker`.

## Resolution (Wave 2-3)
- `api::{load_batch, cache_batch_if_needed, sync_delta_batch, evict_table, tables, query, explain_tier, explain_sql, status}` (`src/lib.rs`) dispatch through `bgworker::shared_bgworker_client()` whenever `pg_bolt.use_bgworker` is enabled and synchronize `PLANNER_CATALOG` (`register_planner_table` / `unregister_planner_table`).
- `pg_hooks::sync_dirty_state` propagates dirty/clean transitions via `client.set_table_dirty`, and `bolt::explain_sql` exposes `api::explain_sql` under `#[pg_schema] mod bolt`.
- `api::tests::api_sync_delta_validates_bolt_name_and_supports_direct_and_bgworker_modes` and `pg_tests::test_validation_and_table_lifecycle` / `test_dml_dirty_tracking_and_subxact_rollback` cover both `use_bgworker` and direct modes including `SET LOCAL pg_bolt.min_offload_rows = 1` query execution after `bolt::load_table`.
