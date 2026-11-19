# W1-E-002: SQL injection via unquoted relation name interpolation in `bolt.load_table`

| Field | Value |
| :--- | :--- |
| Title | SQL injection via unquoted relation name interpolation in `bolt.load_table` |
| Severity | critical |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:624-630` |

## Description
In `PG_BOLT_PLAN_AND_CODE.md:624-630`, the planned SQL function `bolt.load_table(pg_relation text)` carries `#[pg_extern(superuser = true)]` and executes:
```rust
// Quote relation safely via SPI query
let sql = format!("SELECT * FROM {pg_relation}");
```
Contrary to the comment, `{pg_relation}` is interpolated verbatim into an SPI SQL statement without identifier validation or quoting. Calling `SELECT bolt.load_table('sales; DROP TABLE users--')` executes arbitrary SQL inside a superuser SPI session.

## Reproduction
1. Inspect `PG_BOLT_PLAN_AND_CODE.md:624-630`.
2. Pass a malicious argument such as `"sales; DROP TABLE users--"` or `"\"bad\0tbl\""` to `load_table`.

## Suggested fix
Parse and validate relation identifiers before emitting SQL via a strict `quote_qualified_identifier` helper (supporting `table`, `schema.table`, and validly double-quoted identifiers such as `my"schema.my"tbl`, while rejecting NUL bytes, empty segments, trailing dots, unquoted semicolons/spaces, or more than two dot-separated components) and query PostgreSQL `regclass` / `pg_catalog` metadata safely where SPI is used.
