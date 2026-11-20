# W1-E-006: PostgreSQL vs `craton-bolt` SQL semantics divergence (`NaN`, integer overflow, `NULLS FIRST/LAST`)

| Field | Value |
| :--- | :--- |
| Title | PostgreSQL vs `craton-bolt` SQL semantics divergence (`NaN`, integer overflow, `NULLS FIRST/LAST`) |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:202-209,507-550`, `../bolt/docs/SQL_REFERENCE.md:155-183` |

## Description
`Engine::new()` in `PG_BOLT_PLAN_AND_CODE.md:205` defaults to `craton_bolt::SqlSemantics::Ieee`, which diverges from PostgreSQL semantics in three user-visible ways (`../bolt/docs/SQL_REFERENCE.md:155-183`):
1. **`NaN` ordering & equality**: `SqlSemantics::Ieee` treats `NaN = NaN` and `NaN > x` as false, whereas PostgreSQL treats `NaN` as equal to `NaN` and greater than all non-NaN values (`+Infinity`).
2. **Integer arithmetic overflow**: `Ieee` wraps `Int32`/`Int64` `+`, `-`, `*` on overflow, whereas PostgreSQL raises an overflow error (`integer out of range`).
3. **`NULL` sort placement**: `Ieee` defaults to `ASC NULLS FIRST` and `DESC NULLS LAST`; PostgreSQL defaults to `ASC NULLS LAST` and `DESC NULLS FIRST`. Even `craton_bolt::SqlSemantics::Standard` uses `NULLS LAST` for both `ASC` and `DESC`, still diverging on `DESC` without explicit `NULLS FIRST`.

## Reproduction
1. Compare `../bolt/docs/SQL_REFERENCE.md:155-183` with PostgreSQL `ORDER BY col DESC` and `SELECT 2147483647 + 1`.
2. Offload `SELECT val FROM tbl ORDER BY val DESC` (containing NULLs) to `Engine::new()` and compare row order against PostgreSQL.

## Suggested fix
Configure `SqlSemantics::Standard` when appropriate, rewrite implicit PostgreSQL `ORDER BY ... ASC`/`DESC` clauses to explicit `NULLS LAST`/`NULLS FIRST` during CustomScan SQL generation (or guard `ORDER BY` / floating-point `NaN`-sensitive predicates in eligibility analysis until PostgreSQL-exact sorting is guaranteed).

## Resolution (Wave 5)
- Resolved in `src/planner.rs`:
  1. `NaN` floating point comparisons guard: `OffloadAstVisitor` inspects expressions for `isnan(...)` function calls, casts of `'nan'` strings to float/numeric, binary comparisons (`=`, `!=`, `<`, `>`, etc.) with NaN, `IN` lists, `BETWEEN`, and `IS [NOT] DISTINCT FROM`. Any query with NaN comparisons is flagged as `"NaN floating-point semantics"` in `unsupported_constructs` and kept on PostgreSQL CPU.
  2. `SUM(bigint)` overflow detection guard:
     - `inspect_sum_arg_expr` catches explicit `CAST(... AS BIGINT)` and `::int8` inside `SUM(...)`.
     - `analyze_sql_eligibility` resolves column identifiers inside `SUM(...)` against cached Arrow schemas. If any summed column is `DataType::Int64`, offload is declined with `"SUM(bigint) on column '{col_name}' integer overflow and NUMERIC return type semantics diverge from PostgreSQL"`.
  3. Null sort ordering alignment (`DESC NULLS FIRST`, `ASC NULLS LAST`):
     - `normalize_pg_order_by_exprs` ensures all unspecified `ORDER BY` items are rewritten with explicit `nulls_first` clauses (`DESC NULLS FIRST`, `ASC NULLS LAST`).
     - Applied to query-level `order_by`, window specifications, named windows, and aggregate `within_group` clauses.
- Tests added: `nan_floating_point_comparisons_and_predicates_are_safely_guarded`, `sum_bigint_overflow_semantics_divergence_is_guarded`, and `unprojected_order_by_and_desc_nulls_first_semantics` in `src/planner.rs`.
