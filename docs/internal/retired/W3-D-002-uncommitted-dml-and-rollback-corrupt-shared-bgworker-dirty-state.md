<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-D-002: Uncommitted DML eager sync and `ROLLBACK` reset corrupt global `BgWorker` `dirty` state across backends

| Field | Value |
| :--- | :--- |
| Title | Uncommitted DML eager sync and `ROLLBACK` reset corrupt global `BgWorker` `dirty` state across backends |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:143-168,217-231,726-795,839-965,1597-1695` |

## Description
In `pg_hooks` (`src/lib.rs`), `bolt_executor_end_hook` (`lines 963-987`) calls `mark_oid_dirty_in_tx(oid, false)` (`lines 751-781`) immediately after each `INSERT`/`UPDATE`/`DELETE`/`MERGE` statement before transaction commit. `mark_oid_dirty_in_tx` invokes `sync_dirty_state(..., true)` (`lines 723-749`), which sends `client.set_table_dirty(bolt_name, true)` to the singleton shared `BgWorker` when `pg_bolt.use_bgworker = true`.

Two concurrency bugs result from sharing uncommitted backend transaction state with the global `BgWorker`:
1. **Premature invalidation across concurrent backends**: Session A running `BEGIN; INSERT INTO sales ...;` (uncommitted) immediately flips `dirty = true` in the global `BgWorker` before commit.
2. **Silent stale-cache resurrection on `ROLLBACK`**: Suppose Session A runs `BEGIN; INSERT INTO sales ...;` (recording `prev_dirty = false` in Session A's local `TRACKER`), and concurrently Session B commits `INSERT INTO sales ...;` (making the cached table genuinely stale in `BgWorker`). When Session A executes `ROLLBACK;`, `apply_frame_abort` (`lines 853-876`) checks Session A's local `prev_dirty == false` and sends `sync_dirty_state(..., false)` -> `client.set_table_dirty(bolt_name, false)` to the global `BgWorker`, clearing Session B's committed `dirty = true` state so subsequent queries silently read stale GPU data.

## Reproduction
1. Load `public.sales` with `pg_bolt.use_bgworker = on`.
2. Session A executes `BEGIN; INSERT INTO public.sales VALUES (...);` (remains open).
3. Session B executes `INSERT INTO public.sales VALUES (...); COMMIT;` (`sales` is now genuinely modified relative to the GPU cache).
4. Session A executes `ROLLBACK;`.
5. `apply_frame_abort` sends `set_table_dirty("public.sales", false)` to `BgWorker`, causing `bolt.tables()` and queries to treat the stale cache as clean.

## Suggested fix
- Do not broadcast `set_table_dirty(..., true)` to the shared global `BgWorker` until top-level transaction commit (`apply_commit` / `XACT_EVENT_COMMIT`), keeping in-flight uncommitted DML dirty flags strictly backend-local so concurrent snapshots are unaffected and `ROLLBACK` only clears backend-local state.
- Pair committed dirty state in shared memory with a monotonically increasing committed generation counter (`dirty_epoch`) so neither rollback nor cross-backend races can ever clear a newer committed invalidation (`W1-D-005`).

## Resolution
Fixed in `src/lib.rs`:
- Split `sync_dirty_state` into `sync_local_dirty_state` (updates backend-local `PLANNER_CATALOG` / `INVALIDATED_REL_OIDS`, and local `with_bolt_runtime` only when `!current_config().use_bgworker`) and `broadcast_bgworker_dirty` (sends `client.set_table_dirty(bolt_name, true)` to `shared_bgworker_client()` when `current_config().use_bgworker` is true).
- Updated `mark_oid_dirty_in_tx` and `apply_frame_abort` to call only `sync_local_dirty_state`, keeping uncommitted DML invalidations backend-local and ensuring rollbacks never clear committed `BgWorker` dirty flags. Also prevented `apply_frame_abort` from re-registering tables evicted in an outer aborted frame into `PLANNER_CATALOG`.
- Updated `apply_commit` to collect committed dirtied tables across frames (excluding `evict_names` and verifying the table remains dirty in `TRACKER`) and invoke `broadcast_bgworker_dirty(&bolt_name)` on top-level commit.
- Updated `api::tables()` (and `api::cache_batch_if_needed()` via `tables()`) under `#[cfg(feature = "pgrx")]` to overlay `pg_hooks::is_table_dirty(&m.bolt_name)` onto `BgWorker` metadata so uncommitted local DML remains visible in `bolt.tables()` and `bolt.cache_table()`.
- Added `pg_tests::test_uncommitted_dml_and_rollback_preserve_bgworker_dirty_state` covering uncommitted isolation, commit broadcast, and rollback preservation of concurrent `BgWorker` dirty state. Cross-backend shared-memory `PLANNER_CATALOG` / `dirty_epoch` synchronization remains tracked in `W1-D-005`.
