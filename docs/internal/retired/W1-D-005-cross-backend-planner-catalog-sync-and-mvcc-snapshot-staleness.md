# W1-D-005: `PLANNER_CATALOG` is process-local and blind to cross-backend mutations and active MVCC transaction visibility

| Field | Value |
| :--- | :--- |
| Title | `PLANNER_CATALOG` is process-local and blind to cross-backend mutations and active MVCC transaction visibility |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/planner.rs:949-987,1145-1165`, `src/runtime.rs:428-430,506-537`, `src/mvcc.rs` |

## Description
`src/planner.rs` maintains `PLANNER_CATALOG` as an in-process `LazyLock<RwLock<PlannerTableCatalog>>` updated by `BoltSessionRuntime::{register_batch, evict_table, set_dirty}` (`src/runtime.rs:429,508,534`). This leaves two consistency gaps in PostgreSQL:
1. **Cross-backend visibility**: When the `pg_bolt GPU daemon` background worker (`src/bgworker.rs`) or another PostgreSQL backend loads, syncs, marks dirty, or evicts a table, client backends running `pg_bolt_planner_hook` observe an empty or outdated process-local `PLANNER_CATALOG`.
2. **Intra-transaction and snapshot MVCC staleness**: PostgreSQL guarantees repeatable-read / read-committed visibility per `ActiveSnapshot` and sees uncommitted `INSERT`/`UPDATE`/`DELETE` mutations written earlier in the same transaction (`GetCurrentCommandId`). `analyze_sql_eligibility` checks only `CachedTableMeta::dirty`, which is never flipped automatically by PostgreSQL heap DML hooks (`ExecutorEnd` / `ProcessUtility` / `XactCallback`) unless wired to `src/mvcc.rs` and shared invalidation.

## Reproduction
1. Load `sales` into `BoltBgWorker` from backend A; execute `SELECT SUM(price) FROM sales` in backend B -> backend B's `PLANNER_CATALOG` is empty and misses GPU offload.
2. In a single backend, run `bolt.load_table('sales', 'sales')`, then run `BEGIN; DELETE FROM sales; SELECT COUNT(*) FROM sales;` -> if `sales` is not marked dirty on local DML or snapshot mismatch, `planner_hook` risks serving the stale cached row count.

## Resolution
1. **Monotonic Catalog Invalidation Epochs**:
   - `src/runtime.rs` exposes `current_catalog_epoch()` and `bump_catalog_epoch()`.
   - `BoltSessionRuntime` records `table_epochs: HashMap<String, u64>` whenever tables are registered, delta-appended, or cleared.
   - `mark_dirty` advances `bump_catalog_epoch()`.
   - `is_table_epoch_stale(bolt_name, current_epoch)` detects cross-backend and intra-session invalidations.
2. **PostgreSQL MVCC Snapshot Staleness Tracking**:
   - `BoltSessionRuntime` tracks `table_snapshots: HashMap<String, (u32, u32)>` (`xmin`, `xmax`).
   - `on_table_loaded` in `src/lib.rs` captures `pg_sys::GetActiveSnapshot()` (`xmin`, `xmax`) during table registration.
   - `is_snapshot_stale(bolt_name, active_xmin, active_xmax)` evaluates wraparound-aware XID precedence using `mvcc::xid_precedes` to detect whether transactions committed after the cache snapshot make it stale.
   - In PostgreSQL queries, `pg_hooks::refresh_dirty_referenced_tables` re-extracts stale relations via SPI, and `execute_sql_with_timeout` rejects dirty/stale tables with `PgBoltError::Mvcc`.
3. **Tests Added**:
   - `catalog_epoch_and_staleness_tracking` in `src/runtime.rs::tests::stub_tests`.
   - `mvcc_snapshot_staleness_detection` in `src/runtime.rs::tests::stub_tests`.
   - `test_sync_delta_and_query_dirty_table_auto_refresh` in `src/lib.rs::pg_tests`.
