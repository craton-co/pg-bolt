<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-C-001: Add `BoltSessionRuntime::execute_sql_with_timeout` and `reconfigure` for `pgrx` BGWorker timeout/SIGHUP parity

| Field | Value |
| :--- | :--- |
| Title | Add `BoltSessionRuntime::execute_sql_with_timeout` and `reconfigure` for `pgrx` BGWorker timeout/SIGHUP parity |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/runtime.rs:731-768`, `src/bgworker.rs:421-443,668-690`, `src/guc.rs:107-125` |

## Description
`BoltSessionRuntime::execute_sql(&mut self, sql: &str)` (`src/runtime.rs:731-768`) reads `current_config().statement_timeout_ms` internally without accepting an explicit per-request `timeout_ms` parameter, and `BoltSessionRuntime` does not expose a live `reconfigure(&mut self, cfg: &BoltConfig)` method.
- In non-`pgrx` builds, `src/bgworker.rs` applies per-request `timeout_ms` overrides on the worker thread via `crate::guc::with_test_config(...)` around `runtime.execute_sql(sql)`.
- Under `#[cfg(feature = "pgrx")]` non-test builds, `guc::current_config()` reads global `GucSetting` statics directly (ignoring `TEST_CONFIG_OVERRIDE`), so per-request `IpcCommand::ExecuteSql { timeout_ms }` overrides in `pg_bolt_bgworker_main` still fall back to global `pg_bolt.statement_timeout_ms`, and `SIGHUP` updates (`ProcessConfigFile(PGC_SIGHUP)`) cannot shrink `BoltSessionRuntime`'s cached `max_vram_mb` or flip `allow_hybrid_fallback` without a `reconfigure(&cfg)` helper.

## Reproduction
1. Inside `pg_bolt_bgworker_main` (`#[cfg(feature = "pgrx")]`), keep global `pg_bolt.statement_timeout_ms = 0` and submit `IpcCommand::ExecuteSql { sql, timeout_ms: 50 }`.
2. `runtime.execute_sql(&sql)` reads `PG_BOLT_STATEMENT_TIMEOUT_MS.get()` (`0`) instead of `timeout_ms = 50`.
3. Update `pg_bolt.max_vram_mb` in `postgresql.conf` and send `SIGHUP`: `pg_bolt_bgworker_main` reloads `worker_cfg = current_config()`, but `runtime`'s cached `effective_max_vram_bytes()` stays fixed at startup GUC value.

## Suggested fix
- Add `pub fn execute_sql_with_timeout(&mut self, sql: &str, timeout_ms: u64) -> PgBoltResult<BoltExecutionResult>` on `BoltSessionRuntime` using `CancellationToken::new().with_timeout(Duration::from_millis(timeout_ms))` directly when `timeout_ms > 0`, and have `bgworker::execute_sql_with_effective_timeout` call it directly across both `pgrx` and standalone builds.
- Add `pub fn reconfigure(&mut self, cfg: &BoltConfig) -> PgBoltResult<()>` on `BoltSessionRuntime` (updating `max_vram_bytes` / evicting LRU tables if needed and updating hybrid fallback policy) and invoke it on `SIGHUP` in `pg_bolt_bgworker_main`.

## Resolution (Wave 3)
- Updated `execute_sql_with_effective_timeout` in `src/bgworker.rs` to invoke `runtime.execute_sql_with_timeout(sql, effective_timeout_ms)` directly without `with_test_config` under any feature gate.
- Updated `pg_bolt_bgworker_main` (`#[cfg(feature = "pgrx")]`) in `src/bgworker.rs` to call `runtime.reconfigure(&worker_cfg)` after reloading `current_config()` on `SIGHUP`.
- Added unit test `execute_sql_timeout_ms_propagates_and_overrides_default` in `src/bgworker.rs`.
