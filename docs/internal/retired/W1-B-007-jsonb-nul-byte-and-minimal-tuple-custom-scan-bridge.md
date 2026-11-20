<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-007: `jsonb` `\u0000` NUL-byte failure and missing direct `MinimalTuple` / `TupleTableSlot` result path

| Field | Value |
| :--- | :--- |
| Title | `jsonb` `\u0000` NUL-byte failure and missing direct `MinimalTuple` / `TupleTableSlot` result path |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs:1919-1953`, `src/lib.rs:331-352` |

## Description
Two gaps affect query result delivery back to PostgreSQL:
1. **NUL byte rejection in `pgrx::JsonB`**: Arrow `StringArray` values are valid UTF-8 and may contain U+0000 (`'\0'`), whereas PostgreSQL's `jsonb` type explicitly forbids `\u0000` inside JSON strings (`unsupported Unicode escape sequence` / datum conversion failure when constructing `JsonB` datums). `record_batch_to_pgrx_jsonb` wraps `serde_json::Value` directly without sanitizing or rejecting NUL bytes with a clean contextual `PgBoltError::Bridge`.
2. **JSON-only result path allocates per-row maps**: `bolt.query` serializes every column cell into `serde_json::Map` -> `Vec<serde_json::Value>` -> `Vec<pgrx::JsonB>`, which requires `jsonb_to_record` on the SQL side and cannot power a zero-parse `CustomScan` execution node returning native PostgreSQL `MinimalTuple` / `TupleTableSlot` rows.

## Reproduction
1. Serialize a `RecordBatch` containing `"abc\0def"` via `record_batch_to_pgrx_jsonb` and return it from a `#[pg_extern]` returning `JsonB`: PostgreSQL raises a Datum serialization error.
2. Run a 1M-row query through `bolt.query`: `Vec<serde_json::Value>` + `Vec<pgrx::JsonB>` materializes all 1M JSON objects in heap memory simultaneously.

## Resolution (Wave 5)
- **JSONB NUL byte sanitation**: `validate_json_value_no_nul` recursively scans string values and JSON object keys, returning a contextual `PgBoltError::Bridge` naming the offending column and row before any invalid `pgrx::JsonB` datum is constructed.
- **Lazy streaming JSONB iterator**: Added `record_batch_to_json_iter` and `record_batch_to_pgrx_jsonb_iter` yielding rows one at a time, eliminating peak memory spikes on large query outputs.
- **Direct slot row bridge for CustomScan**: Implemented `arrow_batch_to_slot_row` and `arrow_batch_to_slot_row_with_oids` (reusing `write_record_batch_row_datums` and `ExecStoreVirtualTuple`). `CustomScan` nodes (`src/custom_scan.rs`) emit native PostgreSQL virtual tuples directly without JSON intermediate serialization.
- **Unit test coverage**: Tested in `jsonb_nul_byte_guard_and_streaming_iter` in `src/arrow_bridge.rs`.
