<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-A-001: Update `sql/pg_bolt--0.1.0.sql` and docs for `bolt.explain_sql(sql text)`

| Field | Value |
| :--- | :--- |
| Title | Update `sql/pg_bolt--0.1.0.sql` and docs for `bolt.explain_sql(sql text)` |
| Severity | low |
| Status | fixed |
| Kind | docs |
| Files | `sql/pg_bolt--0.1.0.sql:85-90`, `docs/SQL_API.md`, `README.md`, `CHANGELOG.md`, `src/lib.rs:1407-1412` |

## Description
In Wave 3 (`src/lib.rs`), `#[pg_extern] pub fn explain_sql(sql: &str) -> Result<String, PgBoltError>` was added inside `#[pg_schema] mod bolt` alongside `PgTests` and `api::explain_sql` BGWorker routing (addressing the Rust side of `W3-E-001`). However, the checked-in static SQL script [`sql/pg_bolt--0.1.0.sql`](../../../sql/pg_bolt--0.1.0.sql) and user-facing documentation ([`docs/SQL_API.md`](../../SQL_API.md), [`README.md`](../../../README.md)) only list the original 10 functions (`load_table` through `reset_vram`) and omit `bolt.explain_sql(sql text)`.

## Reproduction
1. Inspect `sql/pg_bolt--0.1.0.sql` (lines 1-93): no `CREATE FUNCTION bolt.explain_sql` declaration exists.
2. Inspect `docs/SQL_API.md` and `README.md`: `bolt.explain_sql` is not documented alongside `bolt.explain_tier`.

## Suggested fix
- Add `CREATE FUNCTION bolt.explain_sql(sql text) RETURNS text STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'explain_sql_wrapper';` (matching `explain_tier` volatility/parallel attributes) to `sql/pg_bolt--0.1.0.sql` (or an extension upgrade script if targeting pre-installed `0.1.0` DBs).
- Document `bolt.explain_sql(sql text)` in `docs/SQL_API.md` and `README.md`.

## Resolution
- `sql/pg_bolt--0.1.0.sql` (`85-90`) declares `CREATE FUNCTION "bolt"."explain_sql"("sql" text) RETURNS text AS 'MODULE_PATHNAME', 'explain_sql_wrapper' LANGUAGE c VOLATILE STRICT;` matching `src/lib.rs` (`1407-1412`).
- `docs/SQL_API.md`, `README.md`, and `CHANGELOG.md` are updated to document `bolt.explain_sql(sql text) -> text` (including syntax, parameters, return value, `pg_bolt.use_bgworker` on/off behavior, and SQL examples) and remove the stale pending notes.
- Note: live `#[pg_test]` runtime execution (`test_validation_and_table_lifecycle` in `src/lib.rs`) requires a local PostgreSQL server / `pg_config`.
