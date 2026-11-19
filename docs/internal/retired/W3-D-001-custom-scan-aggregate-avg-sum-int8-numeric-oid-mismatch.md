<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-D-001: `CustomScan` aggregate `AVG`/`SUM(bigint)`/`STDDEV` output `Float64`/`Int64` fails on PostgreSQL `NUMERICOID`

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` aggregate `AVG`/`SUM(bigint)`/`STDDEV` output `Float64`/`Int64` fails on PostgreSQL `NUMERICOID` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs:280-293,4100-4131`, `src/planner.rs:2149-2195`, `../bolt/src/plan/logical_plan.rs:2316-2366` |

## Description
When PostgreSQL plans aggregate queries over integer or numeric columns:
- `AVG(smallint|int4|int8|numeric)`, `STDDEV(...)`, and `VARIANCE(...)` have PostgreSQL targetlist `exprType == pg_sys::NUMERICOID` (`1700`).
- `SUM(int8)` (`BIGINT`) also has PostgreSQL targetlist `exprType == pg_sys::NUMERICOID` (`1700`).

`planner::targetlist_types_supported` (`src/planner.rs:2149-2195`) permits `NUMERICOID`, so `pg_bolt_planner_hook` attaches a `CustomScan` and forwards the SQL to `craton-bolt`. However, in `craton-bolt` (`../bolt/src/plan/logical_plan.rs:2316-2366`):
1. `AggregateExpr::Avg`, `VarPop`, `VarSamp`, `StddevPop`, and `StddevSamp` always return `DataType::Float64`.
2. `sum_output_dtype(DataType::Int64)` returns `DataType::Int64`.

During tuple emission (`bolt_exec_custom_scan` -> `write_record_batch_row_datums` -> `arrow_cell_to_pg_datum`), `convert_int_for_oid` (`src/arrow_bridge.rs:280-293`) rejects `NUMERICOID`, and the `DataType::Float64` arm (`src/arrow_bridge.rs:4119-4131`) only accepts `FLOAT8OID | FLOAT4OID`. As a result, `bolt_exec_custom_scan` aborts with `pgrx::error!("pg_bolt tuple conversion error: ... incompatible with PostgreSQL OID Oid(1700)")`.

## Reproduction
1. Load a table `sales(region_id INT4, qty INT8, price INT4)` above `min_offload_rows` with `pg_bolt.enable_custom_scan = on`.
2. Execute `SELECT region_id, AVG(price) FROM sales GROUP BY region_id;` or `SELECT SUM(qty) FROM sales;`.
3. `craton-bolt` executes the query cleanly (`Float64` / `Int64`), then `bolt_exec_custom_scan` fails in `arrow_cell_to_pg_datum` because PostgreSQL's `TupleDesc` expects `NUMERICOID`.

## Suggested fix
- In `src/arrow_bridge.rs` (`convert_int_for_oid` and `arrow_cell_to_pg_datum`): support converting `Int8..Int64` / `UInt8..UInt64` and `Float32` / `Float64` values when the target PostgreSQL column OID is `NUMERICOID` (e.g., via `AnyNumeric::try_from(...)` or formatted numeric parsing via `str_to_pg_numeric`), rejecting non-finite floats cleanly only if PostgreSQL numeric policy disallows them.
- In `src/planner.rs` / `../bolt`: also check whether `SUM(int8)` can overflow `i64` in Bolt vs PostgreSQL `NUMERIC` widening (`W1-E-006`) and guard or decline offloading `SUM(bigint)` unless overflow/widening semantics match.

## Resolution
- Added `PgIntDatum::Numeric(i128)` and accepted `NUMERICOID` across all signed and unsigned Arrow integer types (`Int8..Int64`, `UInt8..UInt64`) in `convert_int_for_oid` (`src/arrow_bridge.rs`).
- Added shared `str_to_pg_numeric_datum` (backed by `AnyNumeric::from_str`, matching the existing `Decimal128` path with `typmod = -1`), `format_finite_float_for_numeric`, and `float_to_pg_numeric_datum` helpers in `src/arrow_bridge.rs`, wiring `PgIntDatum::Numeric`, `DataType::Float32`, `DataType::Float64`, and `DataType::Decimal128` through them in `arrow_cell_to_pg_datum` while rejecting non-finite `NaN`/`±Infinity` floats with `PgBoltError::Bridge(...)` and normalizing `-0.0` to `"0"`.
- Added unit tests (`convert_int_and_float_for_numeric_oid`) in `arrow_bridge::tests` covering signed/unsigned `NUMERICOID` integer conversion and finite/non-finite float formatting.
- Note: `SUM(bigint)` exceeding `i64::MAX` in `craton-bolt` (`sum_output_dtype(DataType::Int64) -> DataType::Int64`) and `Float64` vs PostgreSQL `NUMERIC` precision semantics remain tracked under `W1-E-006`.
