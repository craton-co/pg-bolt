<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-005: 8KB heap page reader lacks HOT `LP_REDIRECT`, `attisdropped` alignment, and PG 11+ `attmissingval` defaults

| Field | Value |
| :--- | :--- |
| Title | 8KB heap page reader lacks HOT `LP_REDIRECT`, `attisdropped` alignment, and PG 11+ `attmissingval` defaults |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs:342-350,1139-1380` |

## Description
`decode_heap_page_tuples` assumes a simplified 1-to-1 physical tuple layout matching `schema: &[PgColumnMeta]`, which breaks on three real PostgreSQL heap storage mechanisms:
1. **`ALTER TABLE ... DROP COLUMN` (`attisdropped = true`)**: PostgreSQL leaves dropped column slots physically present inside existing heap tuples (`natts` includes dropped columns, null-bitmap bits and fixed-width values remain on disk until `VACUUM FULL`). Passing only visible `PgColumnMeta` columns causes `natts > schema.len()` errors or shifts subsequent column offsets and null-bitmap indices.
2. **PG 11+ Fast Defaults (`atthasmissing` / `attmissingval`)**: Tuples written before `ALTER TABLE ADD COLUMN ... DEFAULT <val> NOT NULL` have `natts < schema.len()`. `decode_tuple_slice` emits `PgDatumValue::Null` for `col_idx >= natts`, violating `NOT NULL` and losing the table's `attmissingval` constant.
3. **HOT (`LP_REDIRECT = 2`) line pointers**: Currently non-`LP_NORMAL` slots are skipped, which can skip redirected HOT chain roots or misrepresent dead-tuple pruning state depending on vacuum / `HeapTupleSatisfiesMVCC` root traversal.

## Reproduction
1. Create a PostgreSQL table `(a int4, b text, c int8)`, insert rows, run `ALTER TABLE t DROP COLUMN b;`, and feed its 8KB page with visible schema `[a, c]` to `decode_heap_page_tuples`.
2. `decode_tuple_slice` rejects `natts (3) > schema.len() (2)`.
3. On `ALTER TABLE t ADD COLUMN d int4 DEFAULT 42 NOT NULL`, pre-ALTER tuples yield `Null` for `d` instead of `42`.

## Suggested fix
- Add optional physical-attribute metadata on `PgColumnMeta` (e.g., `attlen: i16`, `attalign: u8`, `is_dropped: bool`, `missing_default: Option<PgDatumValue>`), advance cursor/null-bitmap over dropped columns without emitting them into the Arrow batch, and fill `col_idx >= natts` from `missing_default` when present.
- Follow in-page `LP_REDIRECT` item pointers (`lp_off` encodes the target `OffsetNumber`) with cycle detection when scanning non-index-only HOT chains while avoiding double-counting `HEAP_ONLY_TUPLE` (`0x8000` in `t_infomask2`) targets.
- Add unit tests for `attisdropped`, `attmissingval`, and `LP_REDIRECT -> HEAP_ONLY_TUPLE` pages.

## Resolution (Wave 2 Agent B)
- Extended `src/arrow_bridge.rs` with `PgColumnSpec` (`attisdropped`, physical `attlen`/`attalign`, `attmissing`), `decode_heap_page_tuples_ext`, `decode_heap_pages_to_arrow_with_specs`, and `encode_test_heap_page_ext`.
- Skips physical null-bitmap slots and byte cursors for dropped columns without emitting Arrow columns, materializes trailing `attmissing` defaults when `col_idx >= natts`, and follows intra-page `LP_REDIRECT` pointers with cycle detection and `HEAP_ONLY_TUPLE` target deduplication.
- Added unit test `heap_page_hot_redirect_attisdropped_and_attmissing` in `src/arrow_bridge.rs`.
