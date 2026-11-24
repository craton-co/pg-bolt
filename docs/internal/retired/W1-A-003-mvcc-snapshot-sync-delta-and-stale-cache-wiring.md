<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-003: Wire PostgreSQL DML invalidation and incremental MVCC deltas into cached tables

| Field | Value |
| :--- | :--- |
| Title | Wire PostgreSQL DML invalidation and incremental MVCC deltas into cached tables |
| Severity | high |
| Status | fixed |
| Kind | proposal |
| Files | `src/lib.rs`, `src/runtime.rs`, `src/mvcc.rs`, `src/planner.rs`, `src/custom_scan.rs` |

## Description
`src/mvcc.rs` (`MvccSnapshot`, `HeapTupleHeaderMeta`, `VisibilityMap`, `filter_batch_by_visibility`) and `src/runtime.rs` (`BoltSessionRuntime::append_delta_batch`, `mark_dirty`, `clear_dirty`) provide the core MVCC visibility and delta-ingestion building blocks, and `bolt.cache_table` checks `meta.dirty` to skip redundant SPI reloads when clean. However, PostgreSQL DML (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`) and DDL (`ALTER TABLE`, `DROP TABLE`) do not yet trigger `ExecutorEnd_hook` / relcache invalidation callbacks to mark affected `bolt_name` entries dirty or extract MVCC-filtered delta tuples. Consequently, `bolt.cache_table` or transparent `CustomScan` offload can read stale cached batches after table mutations unless `bolt.sync_table` / `bolt.load_table` is manually invoked.

## Reproduction
1. Run `SELECT * FROM bolt.load_table('sales', 'sales');`.
2. Execute `INSERT INTO sales VALUES (...)` or `DELETE FROM sales WHERE ...` in PostgreSQL.
3. Run `SELECT * FROM bolt.cache_table('sales', 'sales');` or `bolt.query('SELECT COUNT(*) FROM sales')`.
4. Expected: Cache is either marked dirty/invalidated and refreshed or MVCC-filtered at the active snapshot before returning results.
5. Actual: `meta.dirty` remains `false` until explicit `bolt.sync_table('sales', 'sales')`.

## Resolution
1. **DML and DDL Invalidation Hooks**: `pg_hooks` intercepts `ExecutorEnd` (`CMD_INSERT`, `CMD_UPDATE`, `CMD_DELETE`, `CMD_MERGE`) and `ProcessUtility` (`TRUNCATE`, `ALTER TABLE`, `DROP`, `COPY FROM`, `RENAME`), immediately marking affected tables dirty in transaction context and notifying `planner::invalidate_relation_oid`.
2. **Stale Cache Auto-Refresh in Query Execution**: `bolt::query` and `bolt::query_json` invoke `pg_hooks::refresh_dirty_referenced_tables(sql)`, safely re-extracting dirty tables from PostgreSQL via SPI prior to query execution.
3. **Dirty Table Flagging**: `BoltSessionRuntime::execute_sql_with_timeout` and `api::query` inspect referenced tables; any query targeting an un-refreshed dirty table is rejected with `PgBoltError::Mvcc("cached table ... is dirty and awaiting MVCC sync")`.
4. **Incremental MVCC Delta Ingestion**: Implemented `bolt.sync_delta(bolt_name, delta_query)` UDF and wired `api::sync_delta_batch` to append incremental `RecordBatch` deltas into Bolt VRAM while clearing the dirty flag and notifying planner metadata.
5. **Tests**: Added `test_sync_delta_and_query_dirty_table_auto_refresh` in `src/lib.rs::pg_tests`, `api_query_flags_dirty_table_and_sync_delta_clears_dirty` in `src/lib.rs::api::tests`, and `execute_sql_rejects_dirty_tables` in `src/runtime.rs`.
