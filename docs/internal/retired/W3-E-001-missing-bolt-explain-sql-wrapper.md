<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-E-001: Expose `bolt.explain_sql(sql text)` PostgreSQL SQL wrapper matching `api::explain_sql`

| Field | Value |
| :--- | :--- |
| Title | Expose `bolt.explain_sql(sql text)` PostgreSQL SQL wrapper matching `api::explain_sql` |
| Severity | low |
| Status | fixed |
| Kind | proposal |
| Files | `src/lib.rs:265-276,1407-1412,1472-1474`, `sql/pg_bolt--0.1.0.sql:85-90`, `docs/SQL_API.md`, `README.md`, `CHANGELOG.md` |

## Description
`pg_bolt` now provides `BoltSessionRuntime::explain_sql`, `BgWorkerClient::explain_sql`, and `pg_bolt::api::explain_sql` with `pg_bolt.use_bgworker` IPC parity, returning the logical and physical plan text for analytical queries. However, `#[pg_schema] mod bolt` (`src/lib.rs:1176-1382`) and [`sql/pg_bolt--0.1.0.sql`](../../../sql/pg_bolt--0.1.0.sql) currently expose only `bolt.explain_tier(sql text)` (`"Gpu"` / `"Hybrid"` / `"Host"`), leaving SQL sessions without a `bolt.explain_sql(sql text)` UDF.

## Reproduction
1. Install `pg_bolt` (`CREATE EXTENSION pg_bolt;`).
2. Run `SELECT bolt.explain_sql('SELECT id, amount FROM sales');`.
3. PostgreSQL fails with `function bolt.explain_sql(unknown) does not exist`.

## Suggested fix
- Add `#[pg_extern] pub fn explain_sql(sql: &str) -> PgBoltResult<String>` inside `#[pg_schema] mod bolt` in `src/lib.rs` (invoking `pg_hooks::reconcile_pending_invalidations()` and `api::explain_sql(sql)`).
- Add the matching `CREATE FUNCTION bolt.explain_sql(sql text) RETURNS text ... STRICT` declaration to `sql/pg_bolt--0.1.0.sql` (or an extension upgrade script) and a `#[pg_test]` verifying it in both `use_bgworker` modes.

## Resolution
- `src/lib.rs` (`1407-1412`) exposes `#[pg_extern] pub fn explain_sql(sql: &str) -> Result<String, PgBoltError>` inside `#[pg_schema] mod bolt`, calling `pg_hooks::reconcile_pending_invalidations()` and `api::explain_sql(sql)` (which routes across `pg_bolt.use_bgworker` on/off at `src/lib.rs:265-276`), with `#[pg_test]` coverage in `test_validation_and_table_lifecycle` (`1472-1474`) and pure-Rust `use_bgworker` parity coverage in `tests/runtime_and_vram_test.rs` (`api_explain_sql_passes_with_and_without_bgworker`).
- `sql/pg_bolt--0.1.0.sql` (`85-90`) declares `"bolt"."explain_sql"("sql" text) RETURNS text ... VOLATILE STRICT`.
- `docs/SQL_API.md`, `README.md`, and `CHANGELOG.md` are all updated to document `bolt.explain_sql(sql text)`.
- Note: live `#[pg_test]` runtime execution requires a local PostgreSQL server / `pg_config`.
