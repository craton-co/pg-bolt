<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-007: Reconcile bolt cache state across PostgreSQL transaction rollback and DDL

| Field | Value |
| :--- | :--- |
| Title | Reconcile bolt cache state across PostgreSQL transaction rollback and DDL |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs`, `src/runtime.rs`, `src/custom_scan.rs`, `src/planner.rs` |

## Description
Calling `bolt.load_table`, `bolt.sync_table`, or `bolt.evict_table` inside an explicit PostgreSQL transaction (`BEGIN; SELECT * FROM bolt.load_table(...); ROLLBACK;`) mutates in-memory `BoltSessionRuntime` state immediately without participating in PostgreSQL's transaction / subtransaction lifecycle (`RegisterXactCallback` / `RegisterSubXactCallback`). Likewise, `ROLLBACK` after `ALTER TABLE` or `TRUNCATE` can leave stale schema or evicted/loaded table entries out of sync with PostgreSQL's committed catalog snapshot.

## Reproduction
1. Execute `BEGIN; SELECT * FROM bolt.evict_table('sales'); ROLLBACK;`.
2. Expected: If transactional semantics are desired (or if load/evict is explicitly documented as non-transactional session cache control), rollback restores or reconciles cache visibility safely against the committed relation OID/schema.
3. Actual: `evict_table` permanently evicts `'sales'` even if the wrapping SQL transaction aborts; conversely, `load_table` on an uncommitted `CREATE TABLE` within an aborted block leaves an orphaned table entry in `bolt.tables()`.

## Resolution
1. **Runtime Transaction & Subtransaction Checkpointing**:
   - Added `SessionTxFrame` and `tx_stack` to `BoltSessionRuntime` in `src/runtime.rs`.
   - Implemented `begin_transaction`, `commit_transaction`, `abort_transaction`, `begin_subtransaction`, `commit_subtransaction`, and `abort_subtransaction`.
   - `register_batch` records newly added tables in the active transaction frame; on rollback (`abort_transaction` or `abort_subtransaction`), uncommitted tables are evicted immediately.
   - `mark_dirty` records dirty modifications alongside their prior clean status; on abort, the dirty flag is restored to clean if it was clean prior to the transaction/savepoint.
2. **PostgreSQL Callback Integration**:
   - In `src/lib.rs::pg_hooks`, `bolt_xact_callback` (`XACT_EVENT_COMMIT`, `XACT_EVENT_ABORT`) and `bolt_subxact_callback` (`SUBXACT_EVENT_START_SUB`, `SUBXACT_EVENT_COMMIT_SUB`, `SUBXACT_EVENT_ABORT_SUB`) delegate transaction/savepoint lifecycle events directly into `with_bolt_runtime` when executing in backend mode.
   - Documented transactional rollback vs session-scoped immediate VRAM eviction in `bolt::{load_table, cache_table, sync_table, sync_delta, evict_table, reset_vram}`.
3. **Tests Added**:
   - `transaction_abort_evicts_new_tables_and_restores_dirty_flags` in `src/runtime.rs::tests::stub_tests`.
   - `subtransaction_savepoint_rollback_and_commit_hierarchy` in `src/runtime.rs::tests::stub_tests`.
   - `api_runtime_transaction_abort_restores_clean_state` in `src/lib.rs::api::tests`.
   - `test_dml_dirty_tracking_and_subxact_rollback` in `src/lib.rs::pg_tests`.
