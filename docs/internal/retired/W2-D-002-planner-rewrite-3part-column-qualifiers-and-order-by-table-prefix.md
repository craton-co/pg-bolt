<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-D-002: `planner::rewrite_sql_for_bolt` omits 3-part column qualifier collapse and `ORDER BY` table prefix stripping

| Field | Value |
| :--- | :--- |
| Title | `planner::rewrite_sql_for_bolt` omits 3-part column qualifier collapse and `ORDER BY` table prefix stripping |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/planner.rs:663-1172,2699-2857`, `src/runtime.rs:867-878`, `../bolt/src/plan/sql_frontend.rs:7166-7183` |

## Description
`src/planner.rs::rewrite_sql_for_bolt` rewrites schema-qualified `TableFactor::Table` nodes to `CachedTableMeta.bolt_name` (attaching an `AS <table_name>` alias when needed) and normalizes implicit `ORDER BY` clauses to `ASC NULLS LAST` / `DESC NULLS FIRST`. Two PostgreSQL SQL forms still fail `craton-bolt` 0.12.0 lowering:
1. **3-part column identifiers (`schema.table.col`)**: Unlike `src/runtime.rs:867-878`, `SqlBoltRewriter` does not collapse `public.sales.region_id` to `sales.region_id`.
2. **Table-qualified `ORDER BY` expressions (`ORDER BY sales.region_id`)**: In `craton-bolt` (`../bolt/src/plan/sql_frontend.rs:7166-7183`), `lower_order_by` passes `NameResolver::empty()`, rejecting qualified output column references with `unknown table qualifier 'sales'`.

## Reproduction
1. Pass `SELECT public.sales.region_id, price FROM public.sales ORDER BY sales.price` to `analyze_sql_eligibility`.
2. `craton-bolt::plan::parse_sql` rejects the 3-part column qualifier and qualified `ORDER BY` identifier, causing valid queries to stay on PostgreSQL CPU.

## Suggested fix
- Collapse unambiguous `schema.table.col` `CompoundIdentifier` expressions in `SqlBoltRewriter` to match the rewritten table alias (sharing normalization rules with `src/runtime.rs::SelectExprVisitor`).
- Where safe (single-table queries or matching output aliases), rewrite qualified `ORDER BY alias.col` expressions to unqualified `col`—or support alias resolution in `lower_order_by` upstream in `craton-bolt`.

## Resolution (Wave 3 Agent D)
- Extended `SqlBoltRewriter` in `src/planner.rs` (`lines 663-1172`) with scope-ordered `Query` / `SetExpr` / `Select` / `TableFactor` rewriting (`SelectScope`, `OrderByProjectionLookup`, `ScopedExprVisitor`):
  1. Rewrites `FROM` `TableFactor::Table` items before visiting `JOIN ON`, `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `QUALIFY`, `WINDOW`, and `ORDER BY`, collapsing unambiguous 3-part `schema.table.col` `CompoundIdentifier`s to `<effective_table_alias>.<col>` while delegating nested subqueries cleanly without cross-scope leakage or stopping sibling traversal.
  2. Strips table/schema qualifiers in top-level/subquery `Query.order_by` expressions for single-table queries (matching `table.col` or `schema.table.col` -> `col`), and strips/remaps unambiguous projected column references (`ORDER BY table.col` -> unique output column or alias) in multi-table joins while preserving qualified references in `SELECT`/`ON`/`WHERE` and inside `OVER (ORDER BY ...)` window specs.
- Updated and expanded unit tests in `src/planner.rs` (`lines 2699-2857`) covering 3-part single-table collapse, aggregate `ORDER BY`, multi-table joins with output aliases, ambiguous join `ORDER BY` preservation, and `analyze_sql_eligibility` pass verification (coordinator `cargo test` run pending).
