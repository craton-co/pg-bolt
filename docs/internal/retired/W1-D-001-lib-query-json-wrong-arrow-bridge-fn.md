# W1-D-001: `lib.rs` calls `record_batch_to_json_rows` expecting `Vec<pgrx::JsonB>` instead of `record_batch_to_pgrx_jsonb`

| Field | Value |
| :--- | :--- |
| Title | `lib.rs` calls `record_batch_to_json_rows` expecting `Vec<pgrx::JsonB>` instead of `record_batch_to_pgrx_jsonb` |
| Severity | critical |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs:246,359-380`, `src/arrow_bridge.rs:1955-1987` |

## Description
Under `--features pgrx`, `src/arrow_bridge.rs` defines two separate helpers:
- `pub fn record_batch_to_json_rows(batch: &RecordBatch) -> PgBoltResult<Vec<serde_json::Value>>` (`src/arrow_bridge.rs:1955`)
- `#[cfg(feature = "pgrx")] pub fn record_batch_to_pgrx_jsonb(batch: &RecordBatch) -> PgBoltResult<Vec<pgrx::JsonB>>` (`src/arrow_bridge.rs:1982`)

However, `src/lib.rs:246` imports `record_batch_to_json_rows` into `mod bolt` and calls it at `src/lib.rs:367` (expecting `SetOfIterator<'static, pgrx::JsonB>`) and `src/lib.rs:375` (`.into_iter().map(|jb| jb.0)` expecting `pgrx::JsonB` rather than `serde_json::Value`). This triggers a type-mismatch compile error whenever `--features pgrx` is enabled.

## Reproduction
1. Inspect `src/arrow_bridge.rs:1955-1987` and `src/lib.rs:359-380`.
2. Build or check `pg_bolt` with `--features pgrx,pg16`: `record_batch_to_json_rows` yields `Vec<serde_json::Value>`, so `SetOfIterator<pgrx::JsonB>` and `jb.0` fail to type-check.

## Suggested fix
- In `src/lib.rs:246,367`, switch `bolt::query` to `crate::arrow_bridge::record_batch_to_pgrx_jsonb(&res.batch)?`.
- In `src/lib.rs:375`, pass `record_batch_to_json_rows(&res.batch)?` directly into `serde_json::Value::Array(rows)` without `.map(|jb| jb.0)`.

## Resolution (Wave 2)
Fixed in `src/lib.rs`: `bolt::query` now calls `record_batch_to_pgrx_jsonb(&res.batch)?`, and `bolt::query_json` wraps `record_batch_to_json_rows(&res.batch)?` directly in `pgrx::JsonB(serde_json::Value::Array(rows))`.
