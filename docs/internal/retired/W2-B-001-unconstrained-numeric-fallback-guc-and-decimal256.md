<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-B-001: Unconstrained `NUMERIC` (`atttypmod == -1`) dynamic precision/scale and `Decimal256` / `Float64` fallback GUC

| Field | Value |
| :--- | :--- |
| Title | Unconstrained `NUMERIC` (`atttypmod == -1`) dynamic precision/scale and `Decimal256` / `Float64` fallback GUC |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs`, `src/guc.rs` |

## Description
Wave 2 wired `decode_numeric_typmod`, `PgColumnSpec`, `rows_to_record_batch_with_typmods`, `extract_relation_to_arrow` `atttypmod` propagation, and explicit `NaN` / `Infinity` / `-Infinity` diagnostics. However, PostgreSQL `NUMERIC` columns without a typmod (`atttypmod == -1`) or with `precision > 38` still fell back statically to `DataType::Decimal128(38, 10)` (`PG_NUMERIC_PRECISION = 38`, `PG_NUMERIC_SCALE = 10`).

As a result, valid unconstrained `NUMERIC` tables containing >10 fractional digits, magnitudes `>= 10^28`, or heterogeneous precision >38 errored during `parse_decimal_str_to_i128` unless schema-level typmod fit `Decimal128`.

## Reproduction
1. Define a PostgreSQL table `CREATE TABLE t (x NUMERIC);` (`atttypmod == -1`) and insert `'0.123456789012345'::numeric` or `'1e32'::numeric`.
2. Load via `extract_relation_to_arrow` or `decode_heap_pages_to_arrow`.
3. Ingestion failed because `DataType::Decimal128(38, 10)` could not represent scale 15 or integer magnitude `>= 10^28`.

## Resolution (Wave 5)
- **Configurable GUC**: Added `pg_bolt.numeric_unconstrained_fallback` (`error` | `decimal256` | `float64`, default `float64`) in `src/guc.rs` and added `numeric_unconstrained_fallback` to `BoltConfig`.
- **Extended typmod decoding**: Added `decode_raw_numeric_typmod` decoding `(precision, scale)` without the 38-precision cap.
- **Dynamic batch inference**: Added `infer_numeric_precision_scale` and integrated dynamic typmod synthesis in `rows_to_record_batch_with_typmods`. Unconstrained columns with scale > 10 (such as `0.00000000000001`, scale 14) or magnitude >= 10^28 (such as `1e32`, int digits 33) dynamically adapt their `Decimal128(p, s)` schema.
- **Decimal256 & Float64 support**: Implemented `parse_decimal_str_to_i256` and `parse_numeric_str_to_f64`. When precision exceeds 38 or values contain `NaN`/`Infinity`, conversion seamlessly falls back to `Float64` or `Decimal256` according to the GUC.
- **Unit test coverage**: Added unit tests in `src/guc.rs` (`numeric_unconstrained_fallback_parsing_and_override`) and `src/arrow_bridge.rs` (`numeric_unconstrained_fallback_and_dynamic_inference`).
