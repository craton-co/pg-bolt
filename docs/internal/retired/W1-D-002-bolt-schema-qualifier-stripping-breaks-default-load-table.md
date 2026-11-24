# W1-D-002: `craton-bolt` schema-qualifier stripping breaks default `bolt.load_table('public.sales')` alias and cross-schema resolution

| Field | Value |
| :--- | :--- |
| Title | `craton-bolt` schema-qualifier stripping breaks default `bolt.load_table('public.sales')` alias and cross-schema resolution |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:57-73`, `src/runtime.rs:397-406`, `src/planner.rs:509-546,743-795`, `../bolt/src/plan/sql_frontend.rs:9646-9659` |

## Description
Two incompatible behaviors interact when loading schema-qualified PostgreSQL tables:
1. When a user invokes `SELECT bolt.load_table('public.sales')` with `bolt_alias = NULL`, `api::prepare_extract` (`src/lib.rs:70`) sets `bolt_name = "public.sales"`. `BoltSessionRuntime::register_batch` (`src/runtime.rs:404`) then calls `self.engine.register_table("public.sales", batch)`.
2. Inside `craton-bolt`, `single_ident_from_object_name` (`../bolt/src/plan/sql_frontend.rs:9646-9659`) discards schema qualifiers and passes only the final segment (`"sales"`) to `TableProvider::schema`. As a result, `SELECT SUM(price) FROM public.sales` fails with `unknown table 'sales'` even though `public.sales` was registered!
3. Conversely, if two distinct tables `public.sales` (`pub_sales`) and `analytics.sales` (`ana_sales`) are cached and joined in one SQL query, `craton-bolt` strips both qualifiers to `"sales"` and cannot disambiguate them (`src/planner.rs` explicitly guards against this collision during eligibility analysis).

## Reproduction
1. Load `public.sales` without an explicit alias: `api::load_batch("public.sales", None, batch)` registers `bolt_name = "public.sales"`.
2. Execute `api::query("SELECT COUNT(*) FROM public.sales")` or `api::explain_tier("SELECT COUNT(*) FROM public.sales")`.
3. `craton-bolt` looks up `"sales"` in `Engine`/`MemTableProvider`, missing `"public.sales"`.

## Suggested fix
- Short-term (`pg_bolt`): when `bolt_alias` is omitted in `prepare_extract`, normalize default `bolt_name` cleanly or rewrite AST `TableFactor::Table { name }` references in `planner.rs`/`runtime.rs` to exact `meta.bolt_name` identifiers (quoting/disambiguating schemas like `public_sales` vs `analytics_sales`) before passing SQL to `craton-bolt`.
- Long-term (`craton-bolt` upstream): preserve full normalized `schema.table` names in `MemTableProvider` and `single_ident_from_object_name` instead of silently dropping leading qualifiers.

## Wave 2 Progress (`src/runtime.rs`)
- `BoltSessionRuntime` now maps schema-qualified `bolt_name`s (such as `"public.sales"` and `"analytics.sales"`) to collision-free single-segment `engine_ident`s (`engine_idents` / `reverse_engine_idents`) and rewrites `sqlparser` AST table references, qualified wildcards, and 3-part column identifiers (`public.sales.id`) before passing SQL to `craton-bolt` `Engine` / `MemTableProvider` while keeping `CachedTableMeta.bolt_name` intact.
- `schema_qualified_tables_rewrite_and_resolve_cleanly` covers multi-schema disambiguation, joins, and CTE shadowing in `runtime.rs`.
- Remaining: `planner.rs` 3-part column/ORDER BY normalization (`W2-D-002`) and long-term upstream `craton-bolt` schema qualification.

## Resolution (Wave 5)
- Resolved in `src/planner.rs` and `src/runtime.rs`:
  1. `resolve_cached_table` maps schema-qualified and unqualified table references against `CachedTableMeta` entries with exact and namespace-aware rules.
  2. `rewrite_sql_for_bolt` rewrites schema-qualified base tables (`public.sales`) to exact 1-part quoted `bolt_name` identifiers (`"public.sales" AS sales`) when `alias.is_none()`, preserving the trailing table name as an alias so downstream column references resolve cleanly.
  3. `resolve_three_part_alias` collapses 3-part column references (`public.sales.price`) to `<table_alias>.<col>` (`sales.price`).
  4. Single-table and unambiguous `ORDER BY` qualifiers are normalized to output column identifiers for Bolt's `lower_order_by`.
  5. Cross-schema bare-name collisions in multi-table queries without distinct aliases are detected and safely declined.
- Tests added: `rewrite_sql_for_bolt_normalizes_qualified_tables_and_pg_null_ordering` and `analyze_eligibility_rejects_cross_schema_bare_name_collision` in `src/planner.rs`.
