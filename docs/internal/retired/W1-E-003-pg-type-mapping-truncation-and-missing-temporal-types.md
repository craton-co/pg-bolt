# W1-E-003: PostgreSQL-to-Arrow type bugs (`INT2`, `NUMERIC`), missing temporal OIDs, and JSON conversion gaps

| Field | Value |
| :--- | :--- |
| Title | PostgreSQL-to-Arrow type bugs (`INT2`, `NUMERIC`), missing temporal OIDs, and JSON conversion gaps |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:51,353-444,455-499,660-661` |

## Description
The planned `src/arrow_bridge.rs` and `bolt.query` in `PG_BOLT_PLAN_AND_CODE.md` have multiple type-conversion defects and omissions:
1. `INT2OID` (`:356, 413`) is mapped to `SupportedPgType::Int4` and read via `row.get::<i32>(ordinal)`, which mismatches a 2-byte PostgreSQL `smallint` Datum instead of reading `i16` and widening or emitting `Int16`/`Int32`.
2. `NUMERICOID` (`:359, 431`) is mapped to `Float8`/`f64`, which fails `pgrx` Datum extraction for `AnyNumeric` and silently discards exact base-10 decimal precision (contradicting Phase 1's `Decimal128` requirement at `:51`).
3. `DATEOID`, `TIMESTAMPOID`, and `TIMESTAMPTZOID` are rejected despite `craton-bolt` 0.12.0 supporting Arrow `Date32` and `Timestamp` natively.
4. `record_batch_to_json_rows` (`:481-486`) encodes `NaN`/`Infinity` floats as `Value::String` inside numeric JSON fields (or breaks typed JSON consumers), omits formatting for `Dictionary(Int32, Utf8)`, `Decimal128`, `Date32`, and `Timestamp`, and `bolt.query` (`:660-661`) collects every row into `Vec<JsonB>` in RAM before yielding.

## Reproduction
1. Inspect `PG_BOLT_PLAN_AND_CODE.md:353-444` (`from_pg_oid` and `rows_to_record_batch`).
2. Feed a PostgreSQL table containing `smallint`, `numeric(18, 4)`, `date`, or `timestamptz`, or run a `Float64` query producing `NaN` / `Decimal128` / `Date32` columns into `record_batch_to_json_rows`.

## Suggested fix
- Decode `INT2OID` as `i16` (preserving or explicitly widening to `Int32` safely).
- Map `NUMERICOID` to Arrow `Decimal128(38, 10)` (or scale-preserving `Decimal128`) and `DATEOID`/`TIMESTAMPOID`/`TIMESTAMPTZOID` to `Date32` / `Timestamp(Microsecond, ...)`.
- Implement clean `record_batch_to_json_rows` formatting for `DictionaryArray`, `Decimal128Array`, `Date32Array`, `Timestamp*Array`, and `NaN`/`±Infinity` floats, and stream output batches rather than cloning full `Vec<JsonB>` buffers when used in `SetOfIterator`.
