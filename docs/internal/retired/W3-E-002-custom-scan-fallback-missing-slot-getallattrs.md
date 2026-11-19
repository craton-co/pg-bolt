<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-E-002: `CustomScan` CPU fallback `copy_non_resjunk_slot_attrs` skips `slot_getallattrs`

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` CPU fallback `copy_non_resjunk_slot_attrs` skips `slot_getallattrs` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:435-505,651-676` |

## Description
When `bolt_exec_custom_scan` activates CPU fallback (`fallback_active = true`, `src/custom_scan.rs:651-676`), it fetches a tuple into `fb_slot` (`exec_fn(fb_state)`) and copies attribute values to the `CustomScan` output `slot` via `copy_non_resjunk_slot_attrs(fb_slot, slot, (*bolt_node).fallback_plan)` (`src/custom_scan.rs:435-505`).

`copy_non_resjunk_slot_attrs` directly reads `(*src_slot).tts_values` and `(*src_slot).tts_isnull` without first calling `pg_sys::slot_getallattrs(src_slot)`. In PostgreSQL 12+, `SeqScan`, `IndexScan`, and `Sort` emit `BufferHeapTupleTableSlot` / `HeapTupleTableSlot` / `MinimalTupleTableSlot` instances that deform attributes lazily; until `slot_getallattrs` is invoked, `tts_nvalid` can be `0` and `tts_values` / `tts_isnull` contain stale or uninitialized entries. Additionally, pass-by-reference varlena `Datum`s copied directly from `fb_slot` into virtual `slot` must remain valid past the next child fetch/unpin if the parent node retains tuples.

## Reproduction
1. Trigger a `CustomScan` query whose preserved `orig_plan` produces a non-virtual slot (for example, a direct `SeqScan` returning `BufferHeapTupleTableSlot` with `tts_nvalid == 0`).
2. Force `VramExhausted` (or GPU fallback) during `ExecCustomScan`.
3. `copy_non_resjunk_slot_attrs` reads un-deformed `(*fb_slot).tts_values` and emits zeroed or garbage `Datum`s to PostgreSQL.

## Suggested fix
- Call `pg_sys::slot_getallattrs(src_slot)` before reading `(*src_slot).tts_values` / `(*src_slot).tts_isnull` inside `copy_non_resjunk_slot_attrs` (after checking `!TupIsNull(src_slot)`).
- Ensure any non-byval varlena copied out of a buffer-backed `src_slot` is copied/materialized cleanly into the destination per-tuple context (or stored using a fallback `ProjectionInfo` / materialized slot) so buffer unpins do not invalidate emitted `Datum`s.

## Resolution
- Updated `copy_non_resjunk_slot_attrs` (`src/custom_scan.rs:487-585`) to accept `tuple_cxt: pg_sys::MemoryContext` (passed from `(*bolt_node).tuple_cxt` in `bolt_exec_custom_scan`, `src/custom_scan.rs:771-777`), validate slot pointers, ensure full lazy-slot attribute deformation via `slot_getsomeattrs_int(src_slot, (*src_td).natts)` when `tts_nvalid < natts` (equivalent to PostgreSQL's `static inline` `slot_getallattrs`), switch into `tuple_cxt` via `MemoryContextGuard::switch_to`, and route both equal-width and resjunk-filtered column copies through a shared `copy_slot_datum` helper (`src/custom_scan.rs:470-485`) that zeroes NULL datums, forwards pass-by-value datums, and clones pass-by-reference datums via `pg_sys::datumCopy`.
- Handled PostgreSQL 18 `TupleDescData.compact_attrs` vs pre-PG18 `TupleDescData.attrs` in `tuple_desc_attr_layout` (`src/custom_scan.rs:453-464`). Coordinator `pgrx` build/test runs pending.
