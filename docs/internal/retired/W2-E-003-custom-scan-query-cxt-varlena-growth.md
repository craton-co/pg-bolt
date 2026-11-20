<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-E-003: `CustomScan` allocates `text`/`numeric` tuple varlenas in `es_query_cxt` for the entire query lifetime

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` allocates `text`/`numeric` tuple varlenas in `es_query_cxt` for the entire query lifetime |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:379-442,566-835`, `src/arrow_bridge.rs:4372-4449` |

## Description
In `pgrx_custom_scan::bolt_exec_custom_scan` (`src/custom_scan.rs:456-497`), each row calls `populate_slot_from_batch(batch, state.cursor, slot)` (`src/custom_scan.rs:864-900`) and then `pg_sys::ExecStoreVirtualTuple(slot)`. Inside `arrow_cell_to_datum` (`src/custom_scan.rs:752,856`), `Utf8`/`LargeUtf8`/`Utf8View`/`Dictionary` text values and `Decimal128` (`AnyNumeric`) values call `.into_datum()`, which allocates PostgreSQL varlenas via `palloc` in `CurrentMemoryContext`.

Because `bolt_exec_custom_scan` never switches into an executor per-tuple `MemoryContext` (or resets a dedicated per-tuple context on each `bolt_exec_custom_scan` / `ExecClearTuple` cycle), `CurrentMemoryContext` remains `ExecutorState::es_query_cxt`. Every emitted varlena/numeric cell accumulates in `es_query_cxt` until `ExecutorEnd` rather than being reclaimed row-by-row.

## Reproduction
1. Execute a `CustomScan` query returning millions of `text` or `numeric` rows (for example, `SELECT label, amount FROM large_sales`).
2. Observe PostgreSQL backend memory usage: RSS and `es_query_cxt` grow linearly with total varlena output bytes across the scan even though `ExecStoreVirtualTuple` only holds one tuple at a time.

## Suggested fix
- Create a short-lived per-tuple `MemoryContext` in `PgBoltCustomScanNode` (created under `es_query_cxt` in `bolt_begin_custom_scan`, reset on each `bolt_exec_custom_scan` call right after `ExecClearTuple(slot)`), and switch into it for `populate_slot_from_batch` (`MemoryContextSwitchTo`) before returning to the caller context.
- Note: ensure any downstream PostgreSQL operator materializing the virtual tuple (e.g., `Sort` or `Hash`) calls `ExecCopySlot` / materializes out of `slot` as PostgreSQL's executor contract requires for virtual tuple slots.
- Add a `#[pg_test]` verifying `es_query_cxt` / scan memory stays bounded over multi-thousand-row `text`/`numeric` `CustomScan` queries.

## Resolution (Wave 3 Agent D)
- `src/custom_scan.rs`: `PgBoltCustomScanNode` now owns a child `tuple_cxt: pg_sys::MemoryContext` allocated under `(*estate).es_query_cxt` via `AllocSetContextCreateInternal` in `bolt_begin_custom_scan` (with `ALLOCSET_DEFAULT_{MIN,INIT,MAX}SIZE`).
- `bolt_exec_custom_scan` calls `pg_sys::ExecClearTuple(slot)` followed by `pg_sys::MemoryContextReset((*bolt_node).tuple_cxt)` at the start of every row fetch, switches `CurrentMemoryContext` via an unwind-safe RAII `MemoryContextGuard` around `crate::arrow_bridge::write_record_batch_row_datums` (reclaiming per-row varlena `palloc`s), and restores `CurrentMemoryContext` before `ExecStoreVirtualTuple(slot)`.
- `bolt_rescan_custom_scan` clears `slot` and resets `tuple_cxt`; `bolt_end_custom_scan` clears `slot` and calls `MemoryContextDelete(tuple_cxt)`; `bolt_memory_context_cleanup` clears `tuple_cxt` safely after PostgreSQL deletes children of `es_query_cxt` (paired with `W2-B-004`, coordinator `#[pg_test]` run pending).
