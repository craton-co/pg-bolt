<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-004: Fix pgrx STRICT NULL-default trap, extension control schema error, type aliases, and GUC bugs

| Field | Value |
| :--- | :--- |
| Title | Fix pgrx STRICT NULL-default trap, extension control schema error, type aliases, and GUC bugs |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `pg_bolt.control`, `sql/pg_bolt--0.1.0.sql`, `src/guc.rs`, `src/lib.rs`, `src/runtime.rs` |

## Description
Auditing `PG_BOLT_PLAN_AND_CODE.md` and PostgreSQL/`pgrx` 0.19 extension rules uncovered five concrete extension-loading and SQL bugs:
1. Setting `schema = bolt` in `pg_bolt.control` causes `CREATE EXTENSION pg_bolt;` to fail with `ERROR: schema "bolt" does not exist` unless the caller manually ran `CREATE SCHEMA bolt` beforehand, because PostgreSQL resolves `control->schema` prior to executing the extension install script.
2. Functions accepting `DEFAULT NULL` parameters (`bolt.load_table`, `bolt.cache_table`, `bolt.sync_table`) must not be declared `STRICT` in SQL, or PostgreSQL short-circuits calls with default `NULL` `bolt_alias` without invoking Rust.
3. `#[pg_extern]` signatures inside `#[pg_schema] mod bolt` cannot use Rust `type` aliases (`PgBoltResult<...>`, `CachedTableRow`) because `pgrx-sql-entity-graph` inspects raw AST tokens via `syn`.
4. `PG_BOLT_PLAN_AND_CODE.md:629` formatted unvalidated `format!("SELECT * FROM {pg_relation}")`, allowing SPI SQL injection (also filed in `W1-E-002`).
5. `PG_BOLT_PLAN_AND_CODE.md:708` ran invalid SQL `SHOW pg_bolt.enabled = 'on'` expecting `bool` instead of `TEXT`, and registering GUCs with `GucContext::Postmaster` breaks dynamic `CREATE EXTENSION` outside `shared_preload_libraries`.

## Reproduction
1. Run `CREATE EXTENSION pg_bolt;` with `schema = bolt` in `.control` on a fresh database.
2. Call `SELECT * FROM bolt.load_table('sales');` (omitting `bolt_alias`) on a `STRICT` SQL declaration.
3. Run `#[pg_test]` with `SHOW pg_bolt.enabled = 'on'`.

## Suggested fix
Fixed in Wave 1:
- Removed `schema = bolt` from `pg_bolt.control` (`relocatable = false`, `superuser = true`) and placed `CREATE SCHEMA IF NOT EXISTS "bolt";` in `sql/pg_bolt--0.1.0.sql`.
- Omitted `STRICT` on `load_table`, `cache_table`, and `sync_table` in `sql/pg_bolt--0.1.0.sql`, while keeping `STRICT` on single/non-null-arg UDFs.
- Used explicit `Result<TableIterator<'static, (name!(...), ...)>, PgBoltError>` signatures on every `#[pg_extern]` in `src/lib.rs`.
- Enforced `quote_qualified_identifier` before SPI SQL construction and used `GucContext::Userset`/`Suset` in `src/guc.rs` plus `SELECT current_setting('pg_bolt.enabled')::bool` and `SHOW pg_bolt.enabled` in `pg_tests`.
