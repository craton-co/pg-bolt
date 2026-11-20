# W1-E-008: Upstream `craton-bolt` proposal — PostgreSQL `DESC NULLS FIRST` semantics and query/DDL cancel safety

| Field | Value |
| :--- | :--- |
| Title | Upstream `craton-bolt` proposal — PostgreSQL `DESC NULLS FIRST` semantics and query/DDL cancel safety |
| Severity | low |
| Status | resolved |
| Kind | proposal |
| Files | `../bolt/docs/SQL_REFERENCE.md:155-183`, `../bolt/docs/LIMITATIONS.md` |

## Description
When PostgreSQL offloads queries to `craton-bolt` 0.12.0:
1. Neither `SqlSemantics::Ieee` (`ASC NULLS FIRST`, `DESC NULLS LAST`) nor `SqlSemantics::Standard` (`ASC NULLS LAST`, `DESC NULLS LAST`) matches PostgreSQL's default `DESC NULLS FIRST` ordering (`../bolt/docs/SQL_REFERENCE.md:155-183`).
2. `pg_bolt` must rewrite or guard implicit `ORDER BY` null ordering and integer-overflow semantics on the extension side until `craton-bolt` offers a `SqlSemantics::Postgres` preset or equivalent native option.

## Reproduction
Compare `SqlSemantics::Standard` default sort null placement against PostgreSQL `ORDER BY col DESC` behavior on nullable columns.

## Suggested fix
Add a `SqlSemantics::Postgres` mode (or per-query `NULLS FIRST/LAST` default configuration) in `craton-bolt` alongside total-order NaN and checked integer arithmetic so PostgreSQL offloads require fewer planner rewrites and guards.

## Concrete Upstream Proposal (`craton-bolt`)
1. **`craton_bolt::SqlSemantics::Postgres` mode**:
   - Sort null ordering: Default to `ASC NULLS LAST` and `DESC NULLS FIRST` when `nulls_first` is unspecified.
   - Total-order IEEE-754 semantics: `NaN == NaN` evaluates to true, `NaN > +Infinity` evaluates to true (matching PostgreSQL's `float4_cmp` / `float8_cmp`).
   - Checked arithmetic & aggregation: Detect overflow in `sum(bigint)` / integer addition, either returning an arbitrary-precision decimal/numeric accumulator or raising an explicit overflow error matching PostgreSQL `integer out of range`.
   - Stream cancellation: Native cancellation token check in streaming physical operators.

## Extension-Side Resolution (`src/planner.rs`)
Pending the upstream release of `SqlSemantics::Postgres`, `pg_bolt` implements the necessary semantics guards and rewrites:
1. `normalize_pg_order_by_exprs` rewrites all unspecified `ORDER BY` items in queries, windows, and aggregates to explicit `DESC NULLS FIRST` and `ASC NULLS LAST` clauses.
2. `OffloadAstVisitor` guards `NaN` floating point comparisons and expressions (`isnan`, `= 'nan'`, `> 'nan'`, `IN ('nan')`, `BETWEEN`, `IS [NOT] DISTINCT FROM`), keeping affected queries on PostgreSQL CPU.
3. `analyze_sql_eligibility` guards `SUM(bigint)` overflow divergence by checking column data types in Arrow schemas and rejecting `SUM(bigint)` offloads with an explicit divergence reason.
4. Cancellation checks: `check_pg_cancel_pending()` invoked at CustomScan boundaries and in execution wrappers.
- Tests added: `nan_floating_point_comparisons_and_predicates_are_safely_guarded`, `sum_bigint_overflow_semantics_divergence_is_guarded`, and `unprojected_order_by_and_desc_nulls_first_semantics` in `src/planner.rs`.
