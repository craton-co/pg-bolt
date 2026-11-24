<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-E-001: CustomScan slot bridge omits unsigned ints, `Dictionary<Int64, Utf8>`, extended OIDs, and checked decimal scale

| Field | Value |
| :--- | :--- |
| Title | CustomScan slot bridge omits unsigned ints, `Dictionary<Int64, Utf8>`, extended OIDs, and checked decimal scale |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs`, `src/custom_scan.rs`, `src/planner.rs:2149-2195` |

## Description
`pgrx_custom_scan::arrow_cell_to_datum` (`src/custom_scan.rs:622-862`) converts Arrow result cells emitted by `craton-bolt` into PostgreSQL `pg_sys::Datum` values for `TupleTableSlot`, and `targetlist_types_supported` (`src/planner.rs:1024-1061`) gates CustomScan targetlist OIDs. Four defects remain:
1. **Unsigned integers and `Int8` unsupported**: `arrow_cell_to_datum` handles `Int16`/`Int32`/`Int64`, but rejects `DataType::Int8`, `DataType::UInt8`, `DataType::UInt16`, `DataType::UInt32`, and `DataType::UInt64` (including `UInt64` counts/metrics returned by `craton-bolt`) with `unsupported Arrow result type in CustomScan slot`.
2. **`Dictionary<Int64, Utf8>` panic/rejection**: Line 754 matches `DataType::Dictionary(_, value_type)` with a wildcard key type, yet line 766 unconditionally downcasts to `DictionaryArray<Int32Type>`, failing on `Int64`-keyed Arrow dictionaries.
3. **Extended types (`BYTEAOID`, `TIMEOID`, `UUIDOID`) unmapped**: Neither `arrow_cell_to_datum` (`Binary`, `Time64(Microsecond)`, `FixedSizeBinary(16)`) nor `targetlist_types_supported` handles the `W1-B-006` extended types.
4. **Unchecked `scale` math in `format_decimal128`**: `src/custom_scan.rs:588-595` computes `(-scale) as u32` (overflows on `i8::MIN`) and `10_i128.pow(scale as u32)` (panics if `scale > 38`).

## Reproduction
1. Return a `RecordBatch` containing a `UInt64Array` or `DictionaryArray<Int64Type>` from `CustomScan`.
2. `arrow_cell_to_datum` fails inside `bolt_exec_custom_scan` and raises `pgrx::error!`.
3. Call `format_decimal128(1, 39)`: `10_i128.pow(39)` panics on arithmetic overflow.

## Suggested fix
- Add `Int8`, `UInt8..=UInt64` arms to `arrow_cell_to_datum` with checked `i64`/`i32` conversions (`INT2OID`/`INT4OID`/`INT8OID`).
- Extract dictionary string lookup into a helper supporting `Int8`/`Int16`/`Int32`/`Int64` and `UInt8..=UInt64` keys (mirroring `src/ipc.rs:124-161`).
- Add `DataType::Binary`, `DataType::Time64(TimeUnit::Microsecond)`, and `DataType::FixedSizeBinary(16)` handling in `arrow_cell_to_datum` and `BYTEAOID`/`TIMEOID`/`UUIDOID` in `targetlist_types_supported`.
- Guard `format_decimal128` against `scale > 38` and negative overflow using `i128::checked_pow` / `saturating_neg`.
- Add unit tests covering `format_decimal128` boundary scales (`-128`, `0`, `10`, `39`) and `#[pg_test]` coverage for `UInt64` and `Dictionary<Int64, Utf8>` slot output.

## Resolution (Wave 3 Agent B)
- `src/arrow_bridge.rs` (`arrow_cell_to_pg_datum` + `convert_int_for_oid`): supports all 8 Arrow integer widths (`Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`) with lossless `i128` widening and range-checked conversion into `INT2OID`/`INT4OID`/`INT8OID`, returning `PgBoltError::Bridge` on `UInt64 > i64::MAX` or target overflow.
- `src/arrow_bridge.rs` (`resolve_any_dict_idx`, `dict_str_at`, `dict_bytes_at`, `OwnedColumnJsonExtractor::DictString`): supports all 8 integer dictionary key widths (`Int8..Int64`, `UInt8..UInt64`) with `Utf8 | LargeUtf8 | Utf8View` and `Binary | LargeBinary | BinaryView` (`BYTEAOID`) values.
- `src/arrow_bridge.rs` (`arrow_cell_to_pg_datum`) + `src/planner.rs:1778-1796` (`targetlist_types_supported`): supports `BYTEAOID` (`Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary`), `UUIDOID` (`FixedSizeBinary(16)`), and `TIMEOID` (`Time32`, `Time64`, with pre-division `[0, PG_TIME_MAX_USEC * 1_000]` bounds validation on `Time64(Nanosecond)`).
- `src/arrow_bridge.rs` (`format_decimal128_json`, `checked_max_decimal128_magnitude`): handles negative scales via `scale.unsigned_abs()` (including `i8::MIN`) and `scale > 38` via pure string slice padding, plus checked `10_u128.checked_pow` magnitude validation.
- Added pure-Rust unit tests (`decimal128_format_scale_boundaries`, `convert_int_widening_narrowing_and_overflow`, `dictionary_all_key_widths_and_views`) in `src/arrow_bridge.rs`; coordinator build and live `#[pg_test]` execution remain pending.
