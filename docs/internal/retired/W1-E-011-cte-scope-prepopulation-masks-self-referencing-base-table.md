# W1-E-011: `SqlInspector` pre-populates all CTE aliases before inspecting CTE bodies, masking self-referencing base tables

| Field | Value |
| :--- | :--- |
| Title | `SqlInspector` pre-populates all CTE aliases before inspecting CTE bodies, masking self-referencing base tables |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/planner.rs:343-355,391-398,730-738`, `../bolt/src/plan/sql_frontend.rs:6707-6730` |

## Description
In `SqlInspector::pre_visit_query` (`src/planner.rs:343-355`), every `cte.alias.name` in `with.cte_tables` is inserted into `scope` and pushed onto `self.cte_scopes` **before** `sqlparser::ast::Visit` descends into each CTE's `cte.query` (`inspect_set_expr(&query.body)`).

In PostgreSQL non-recursive `WITH` queries (and in `craton-bolt`'s `../bolt/src/plan/sql_frontend.rs:6707-6730`), CTEs are scoped sequentially: inside `WITH sales AS (SELECT region_id, price FROM sales WHERE price > 10) SELECT * FROM sales`, the `sales` inside the CTE body refers to the base table `public.sales`, not the CTE `sales` being defined; similarly, `cte_1` cannot reference a later `cte_2`. Because `pre_visit_query` puts all CTE names into `cte_scopes` upfront, `is_cte_in_scope("sales")` (`src/planner.rs:391`) returns `true` inside the CTE body, omits `"sales"` from `referenced_tables`, and causes `analyze_sql_eligibility` (`src/planner.rs:730-738`) to falsely conclude `"query references no base tables"` (or skip dirty/uncached checks for shadowed tables).

## Reproduction
1. Cache `sales` (`200_000` rows, schema `(region_id, price, ...)`).
2. Call `inspect_sql_for_offload("WITH sales AS (SELECT region_id, price FROM sales WHERE region_id < 50) SELECT region_id, SUM(price) FROM sales GROUP BY region_id")`.
3. `inspection.referenced_tables` is empty instead of `["sales"]`, causing `analyze_sql_eligibility` to reject a valid query with `"query references no base tables"`.

## Suggested fix
Remove pre-population of all CTE aliases in `pre_visit_query` for non-recursive `WITH`. Instead, track CTE inspection explicitly: for each CTE in `query.with.cte_tables`, inspect `cte.query` first (seeing only *preceding* CTEs in `scope`), then insert `cte.alias.name` into `scope` before inspecting subsequent CTEs and `query.body`.
