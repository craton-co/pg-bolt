<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-004: Static `Decimal128(38, 10)` `NUMERICOID` mapping rejects `> 10` fractional digits, `> 10^28` integers, and PG `NaN`/`Infinity`

| Field | Value |
| :--- | :--- |
| Title | Static `Decimal128(38, 10)` `NUMERICOID` mapping rejects `> 10` fractional digits, `> 10^28` integers, and PG `NaN`/`Infinity` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs:50-54,107-130,238-315` |

## Description
`map_pg_oid_to_arrow(NUMERICOID)` always maps PostgreSQL `NUMERIC` to fixed `DataType::Decimal128(38, 10)`. While this safely avoids `f64` precision loss on typical monetary columns, three valid PostgreSQL `NUMERIC` cases fail ingestion with `PgBoltError::Bridge`:
1. **Fractional scale > 10** (e.g., `NUMERIC(20, 12)` or un-typmodded `0.12345678901`): `parse_decimal_str_to_i128(s, 10)` refuses silent truncation and errors.
2. **Integer magnitude >= `10^28`** (e.g., `NUMERIC(38, 0)` with 35 integer digits): scaling by `10^10` pushes the value past `10^38 - 1`.
3. **Unconstrained `NUMERIC` columns (`atttypmod == -1`) or `NaN` / `Infinity` / `-Infinity`**: PostgreSQL allows heterogeneous per-row scales and `NaN` / `±Infinity` inside `NUMERIC`, whereas Arrow `Decimal128` has fixed schema-wide `(precision, scale)` and no special-value representation.

## Reproduction
1. Run `bolt.load_table` on a table with `val NUMERIC(20, 14)` containing `0.00000000000001` or `1e30::numeric` or `'NaN'::numeric`.
2. `parse_decimal_str_to_i128` / `Decimal128(38, 10)` validation returns `Err(PgBoltError::Bridge(...))`.

## Resolution (Wave 5)
- **Typmod decoding**: Implemented `decode_numeric_typmod` (precision <= 38) and `decode_raw_numeric_typmod` (precision up to 76) extracting precision and scale from PostgreSQL `atttypmod`.
- **Dynamic batch scale & precision inference**: Added `infer_numeric_precision_scale` and integrated dynamic typmod synthesis into `rows_to_record_batch_with_typmods`. Unconstrained columns with scale > 10 (e.g. `0.00000000000001`, scale 14) or large magnitudes (e.g. `1e32`, integer 33) dynamically adapt their `Decimal128(p, s)` schema without truncation or error.
- **Configurable unconstrained fallback GUC**: Added `pg_bolt.numeric_unconstrained_fallback` (`error` | `decimal256` | `float64`, default `float64`) in `src/guc.rs`. When values exceed `Decimal128` precision or contain `NaN`/`Infinity`, conversion falls back safely to native `Float64` or `Decimal256` without panicking.
- **NaN / Infinity diagnostics**: Enhanced error diagnostics pointing users to `pg_bolt.numeric_unconstrained_fallback = 'float64'`.
- **Unit test coverage**: Added `numeric_unconstrained_fallback_and_dynamic_inference` in `src/arrow_bridge.rs`.
