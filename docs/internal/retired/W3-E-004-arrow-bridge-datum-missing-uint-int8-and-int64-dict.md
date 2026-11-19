<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-E-004: `arrow_bridge::arrow_cell_to_pg_datum` still omits `Int8`/`UInt8..=UInt64` and `Dictionary<Int64, Utf8>`

| Field | Value |
| :--- | :--- |
| Title | `arrow_bridge::arrow_cell_to_pg_datum` still omits `Int8`/`UInt8..=UInt64` and `Dictionary<Int64, Utf8>` |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs:3570-3937`, `src/custom_scan.rs:685` |

## Description
`CustomScan` slot emission (`src/custom_scan.rs:685`) was consolidated onto `arrow_bridge::populate_tuple_slot_from_batch` -> `arrow_cell_to_pg_datum` (`src/arrow_bridge.rs:3570-3937`). While `arrow_cell_to_pg_datum` already covers `Binary`/`LargeBinary`/`BinaryView`, `FixedSizeBinary(16)`, `Time32`/`Time64`, `StringView`, and checked `format_decimal128_json` formatting (partially resolving `W2-E-001`), two gaps from `W2-E-001` remain in `src/arrow_bridge.rs`:
1. **`Int8` and unsigned integers (`UInt8..=UInt64`) unimplemented**: Returning `DataType::UInt64` (for example from `COUNT(*)` or unsigned Arrow projections) or `Int8`/`UInt8`/`UInt16`/`UInt32` fails with `"unsupported Arrow result type in PostgreSQL Datum bridge"`.
2. **`Dictionary<Int64, Utf8>` and `Dictionary<_, Utf8View>` unsupported**: Line 3751 matches `DataType::Dictionary(_, value_type)` for any key type `_` (`Utf8` / `LargeUtf8`), yet line 3764 unconditionally downcasts to `DictionaryArray<Int32Type>`, failing on `Int64`-keyed Arrow dictionaries or `Utf8View` values.

## Reproduction
1. Pass a `RecordBatch` containing a `UInt64Array` or `DictionaryArray<Int64Type>` column to `arrow_cell_to_pg_datum` / `populate_tuple_slot_from_batch`.
2. Conversion fails with `PgBoltError::Bridge(...)` (causing `pgrx::error!` inside `bolt_exec_custom_scan`).

## Suggested fix
- Add `DataType::Int8`, `UInt8`, `UInt16`, `UInt32`, and `UInt64` arms to `arrow_cell_to_pg_datum` with checked `i16`/`i32`/`i64` conversions against `INT2OID`/`INT4OID`/`INT8OID`.
- Support `Int8`/`Int16`/`Int32`/`Int64` and `UInt8..=UInt64` dictionary key arrays and `StringArray`/`LargeStringArray`/`StringViewArray` dictionary value arrays inside `arrow_cell_to_pg_datum` (and `ArrowColumnAccessor::from_array` fast path at `src/arrow_bridge.rs:3166`).
