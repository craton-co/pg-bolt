# W1-D-003: `planner_hook` raw `query_string` forwarding cannot handle `$1` bound parameters, view/rule expansion, or `search_path`

| Field | Value |
| :--- | :--- |
| Title | `planner_hook` raw `query_string` forwarding cannot handle `$1` bound parameters, view/rule expansion, or `search_path` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/planner.rs:1003-1180`, `src/custom_scan.rs:425-445` |

## Description
`pg_bolt_planner_hook` (`src/planner.rs:1109-1180`) extracts the SQL text from PostgreSQL's `query_string` using `stmt_location`/`stmt_len` and sends the SQL string to `analyze_sql_eligibility` and `BoltCustomScanPlan`. While `pg_bolt_planner_hook` guards against `bound_params`, `hasModifyingCTE`, `rowMarks`, `hasRowSecurity`, and `CURSOR_OPT_SCROLL`, forwarding raw `query_string` still has major PostgreSQL fidelity gaps:
1. **Parameterized / prepared queries (`$1`, `$2`)**: ORMs, JDBC/psycopg prepared statements, and PL/pgSQL queries supply `ParamListInfo` with `$N` placeholders, which `inspect_sql_for_offload` and `pg_bolt_planner_hook` must skip.
2. **Per-session `search_path` aliasing**: If `public.sales` is cached with `bolt_name = "sales"`, and another session sets `SET search_path = tenant2, public;` and runs `SELECT * FROM sales`, `query_string` contains unqualified `sales` even though PostgreSQL bound `(*parse).rtable` to `tenant2.sales`! Inspecting `query_string` alone cannot verify relation OIDs or schema resolution.
3. **Views, rules, and partitioned tables**: If a query references a PostgreSQL view `v_sales` or a partitioned parent `sales_part`, PostgreSQL's rewriter/planner expands `(*parse).rtable` / `Query` differently from the raw table token in `query_string`. If a table or alias shares the view's unqualified name, `query_string` can alias the wrong relation or miss partition pruning.

## Reproduction
1. Load `public.sales` as `"sales"`.
2. Create `tenant2.sales` with different data, run `SET search_path = tenant2, public; SELECT COUNT(*) FROM sales;`.
3. `query_string` contains `sales`, which matches `public.sales` in `PLANNER_CATALOG` unless `(*parse).rtable` relation OIDs/namespaces are verified against `CachedTableMeta::pg_relation`.

## Suggested fix
- Validate every `RTE_RELATION` entry in `(*parse).rtable` in `pg_bolt_planner_hook` (checking `relkind == RELKIND_RELATION` and matching resolved `schema.relname` / `Oid` against cached `CachedTableMeta`), declining offload whenever `rtable` contains views, foreign tables, partitioned roots, or a schema differing from `CachedTableMeta::pg_relation`.
- Medium-term: construct Bolt logical plans or normalized SQL directly from PostgreSQL's rewritten `Query` / `PlannedStmt` AST with `ParamListInfo` constants inlined and `ReScanCustomScan` checking `chgParam` to re-execute when loop/bind parameters change.

## Resolution (Wave 5)
- Resolved in `src/planner.rs`:
  1. `collect_and_validate_rtable`: Traverses all `RangeTblEntry` nodes in `(*parse).rtable` and `(*planned).rtable`. Rejects offload if any entry is not a plain base relation (`rtekind == RTE_RELATION`, `relkind == 'r'`, `get_rel_relkind(relid) == 'r'`). Safely declines if `rtable` contains views (`'v'`), partitioned roots (`'p'`), foreign tables (`'f'`), materialized views (`'m'`), subqueries, CTEs, functions, or row-level security (`securityQuals`).
  2. Namespace/schema validation in `validate_rtable_relations`: Verifies resolved PostgreSQL namespace OID and relation name against `CachedTableMeta::pg_relation`. Rejects offload if PostgreSQL resolved `search_path` to a different schema than the cached relation.
  3. Relation count consistency: Enforces `matched_bolt_names.len() == verified_pg_rels.len() && verified_pg_rels.len() == inspection.referenced_tables.len()`, preventing hidden view/rule expansions.
  4. Bound parameter guarding: `inspect_sql_for_offload` sets `has_parameters = true` on any `$1..$N` placeholders; `validate_rtable_relations` and `analyze_sql_eligibility` reject queries with unbound parameters; `extract_bound_param_literals` and `substitute_sql_params` perform safe substitution only when parameters are finite and supported; `pg_bolt_planner_hook` guards `paramExecTypes`, `hasSubLinks`, and `hasTargetSRFs`.
- Tests added: `inspect_catches_parameters_tvf_and_lateral`, `substitute_sql_params_and_epoch_invalidation_behave_safely`, and `unprojected_order_by_and_desc_nulls_first_semantics` in `src/planner.rs`.
