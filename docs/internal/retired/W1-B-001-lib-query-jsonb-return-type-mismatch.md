<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-001: `lib.rs` `bolt.query` / `bolt.query_json` expects `Vec<pgrx::JsonB>` from `record_batch_to_json_rows`

| Field | Value |
| :--- | :--- |
| Title | `lib.rs` `bolt.query` / `bolt.query_json` expects `Vec<pgrx::JsonB>` from `record_batch_to_json_rows` |
| Severity | critical |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:246,331-352`, `src/arrow_bridge.rs:1919-1953` |

## Description
Under `--features pgrx`, `src/lib.rs` imports `record_batch_to_json_rows` (line 246) and calls it inside `bolt::query` (`SetOfIterator<'static, pgrx::JsonB>`, line 339-340) and `bolt::query_json` (`.map(|jb| jb.0)`, lines 347-350), assuming `record_batch_to_json_rows` returns `PgBoltResult<Vec<pgrx::JsonB>>` (matching the prototype in `PG_BOLT_PLAN_AND_CODE.md:455`).

In `src/arrow_bridge.rs`, `record_batch_to_json_rows` is portable and returns `PgBoltResult<Vec<serde_json::Value>>`, while the `pgrx`-gated wrapper is `record_batch_to_pgrx_jsonb(batch: &RecordBatch) -> PgBoltResult<Vec<pgrx::JsonB>>` (`src/arrow_bridge.rs:1947-1953`). Compiling with `--features pgrx` will fail type-checking in `src/lib.rs` until `lib.rs` switches to `record_batch_to_pgrx_jsonb` (or builds `pgrx::JsonB` directly from `record_batch_to_json_rows`).

## Reproduction
1. Inspect `src/lib.rs:246,339-350` and `src/arrow_bridge.rs:1919-1953`.
2. Build with PostgreSQL extension features enabled (`cargo check --features pg16`).
3. Compilation fails because `Vec<serde_json::Value>` does not match `Vec<pgrx::JsonB>` and `serde_json::Value` has no tuple field `.0`.

## Suggested fix
In `src/lib.rs`:
- `bolt::query`: call `crate::arrow_bridge::record_batch_to_pgrx_jsonb(&res.batch)?`.
- `bolt::query_json`: call `crate::arrow_bridge::record_batch_to_json_rows(&res.batch)?` directly and pass the returned `Vec<serde_json::Value>` into `pgrx::JsonB(serde_json::Value::Array(rows))` without unwrapping/re-wrapping `pgrx::JsonB`.
- Cover both functions in `pg_test` integration tests under `--features pg16,pg_test`.

## Resolution (Wave 2)
Fixed in `src/lib.rs`: `bolt::query` now uses `record_batch_to_pgrx_jsonb(&res.batch)?` to yield `SetOfIterator<'static, pgrx::JsonB>`, and `bolt::query_json` passes `record_batch_to_json_rows(&res.batch)?` directly into `pgrx::JsonB(serde_json::Value::Array(rows))`.
