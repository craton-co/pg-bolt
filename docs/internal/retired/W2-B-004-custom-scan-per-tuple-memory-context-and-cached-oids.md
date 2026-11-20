<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-B-004: `CustomScan` per-tuple `MemoryContext` leak and per-row OID `Vec` allocation in `populate_tuple_slot_from_batch`

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` per-tuple `MemoryContext` leak and per-row OID `Vec` allocation in `populate_tuple_slot_from_batch` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:683-688`, `src/arrow_bridge.rs:3569-4029` |

## Description
`src/custom_scan.rs:683-688` (`bolt_exec_custom_scan`) emits each `RecordBatch` row into PostgreSQL's `TupleTableSlot` via:
```rust
crate::arrow_bridge::populate_tuple_slot_from_batch(batch, state.cursor, slot)
pg_sys::ExecStoreVirtualTuple(slot);
```
Two performance and memory-management defects exist on this hot per-tuple path:
1. **Per-tuple PostgreSQL `palloc` leak across large scans**: `arrow_cell_to_pg_datum` (`src/arrow_bridge.rs:3569-3937`) calls `IntoDatum` for pass-by-reference types (`TEXT`, `BYTEA`, `UUID`, `NUMERIC`), which `palloc`s new varlena/fixed allocations inside the caller's active `CurrentMemoryContext`. `bolt_exec_custom_scan` does not call `ExecClearTuple(slot)` nor switch `CurrentMemoryContext` into a resettable per-tuple child `MemoryContext` (e.g. owned by `PgBoltCustomScanNode` and reset via `MemoryContextReset` at the start of each `bolt_exec_custom_scan` call once downstream nodes have consumed the prior tuple), so long scans leak per-row `palloc` allocations until executor shutdown.
2. **Per-row Rust `Vec<Oid>` allocation**: `populate_tuple_slot_from_batch` (`src/arrow_bridge.rs:4027`) allocates `let oids: Vec<pgrx::pg_sys::Oid> = attrs.iter().map(|a| a.atttypid).collect();` on every single tuple instead of precomputing column OIDs once in `bolt_begin_custom_scan` and calling `write_record_batch_row_datums` with `&state.slot_oids`.

## Reproduction
1. Inspect `src/custom_scan.rs:676-688` and `src/arrow_bridge.rs:3988-4029`.
2. Execute a `CustomScan` returning 10M rows with `TEXT`, `BYTEA`, `UUID`, or `NUMERIC` columns: every row allocates a Rust `Vec<Oid>` and `palloc`s datums without per-tuple `MemoryContextReset`.

## Suggested fix
- In `src/custom_scan.rs`: allocate a per-tuple `MemoryContext` (`AllocSetContextCreate` under `es_query_cxt`) during `bolt_begin_custom_scan`, reset it at the start of `bolt_exec_custom_scan` (`ExecClearTuple(slot)` + `MemoryContextReset(tuple_cxt)`), switch `CurrentMemoryContext` around slot population (`write_record_batch_row_datums` / `populate_tuple_slot_from_batch`), and delete it in `bolt_end_custom_scan`.
- Cache `slot_oids: Vec<pg_sys::Oid>` once per `TupleDesc` in `CustomScanState` and invoke `crate::arrow_bridge::write_record_batch_row_datums` directly instead of calling `populate_tuple_slot_from_batch` on every row.

## Resolution (Wave 3 Agents B & D)
- `src/arrow_bridge.rs`: added `#[cfg(feature = "pgrx")] pub unsafe fn extract_tuple_desc_oids` (line 4425) and `pub unsafe fn populate_tuple_slot_with_oids`, refactored `populate_tuple_slot_from_batch` to reuse them, added 0-column `row_idx` bounds validation, and switched `write_record_batch_row_datums` (line 4372) to `.zip()` iteration.
- `src/custom_scan.rs` (`lines 379-442, 566-835`): adds `tuple_cxt: pg_sys::MemoryContext`, `slot_oids: Vec<pg_sys::Oid>` (initialized via `ptr::write(&raw mut (*node).slot_oids, Vec::new())`), and unwind-safe RAII `MemoryContextGuard` on `PgBoltCustomScanNode`; allocates `tuple_cxt` once under `(*estate).es_query_cxt` via `AllocSetContextCreateInternal` with `ALLOCSET_DEFAULT_{MIN,INIT,MAX}SIZE` and caches `slot_oids` once via `extract_tuple_desc_oids(slot)` in `bolt_begin_custom_scan`; clears `slot` + resets `tuple_cxt` at the start of `bolt_exec_custom_scan` and `bolt_rescan_custom_scan`; switches `CurrentMemoryContext` into `tuple_cxt` around `write_record_batch_row_datums(..., &(*bolt_node).slot_oids, values, isnull)` before `ExecStoreVirtualTuple(slot)`; deletes `tuple_cxt` after `ExecClearTuple` in `bolt_end_custom_scan`; and nulls `tuple_cxt` without double-freeing in `bolt_memory_context_cleanup` (since PostgreSQL deletes child contexts before reset callbacks) while dropping `slot_oids` idempotently.
- Coordinator build/`#[pg_test]` verification pending (Cargo/rustc invocations are disabled for worker agents).
