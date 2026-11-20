<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-A-003: `CustomScan` execution ignores client `statement_timeout` and `QueryCancelPending` over BGWorker IPC

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` execution ignores client `statement_timeout` and `QueryCancelPending` over BGWorker IPC |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:121-140`, `src/planner.rs:1862-1869`, `src/runtime.rs` |

## Description
When `pg_bolt_planner_hook` attaches a `CustomScan` (`src/planner.rs:1862-1869`), `BoltCustomScanPlan.timeout_ms` records only `config.statement_timeout_ms` at plan time. During execution (`src/custom_scan.rs:128-140`):
1. Under `ScanExecutionBackend::BgWorker(client)`, `client.execute_sql(&self.plan.sql, self.plan.timeout_ms)` sends `self.plan.timeout_ms` without reading the client backend's live `pgrx::pg_sys::StatementTimeout` (`effective_timeout_with_pg`) or checking `pg_cancel_pending()` first. Because `BoltBgWorker` runs in a separate OS process, client `SET statement_timeout = '50ms'` is invisible inside the worker.
2. Under `ScanExecutionBackend::Direct(runtime)`, if `self.plan.timeout_ms == 0`, it calls `runtime.execute_sql(...)` (re-reading `pg_bolt.statement_timeout_ms`) rather than consistently calling `runtime.execute_sql_with_timeout(..., self.plan.timeout_ms)`.

## Reproduction
1. Enable `pg_bolt.use_bgworker = on` with `pg_bolt.statement_timeout_ms = 0`.
2. In a client session, set `SET statement_timeout = '20ms';` and run an offloaded query hitting `CustomScan`.
3. `bolt_exec_custom_scan` sends `timeout_ms = 0` to `BoltBgWorker`, bypassing client `statement_timeout`.

## Suggested fix
- In `BoltCustomScanState::execute` (`src/custom_scan.rs:128-140`), check `crate::runtime::check_pg_cancel_pending()?` and compute `let timeout_ms = crate::runtime::effective_timeout_with_pg(self.plan.timeout_ms);` before dispatching to either `runtime.execute_sql_with_timeout(&self.plan.sql, timeout_ms)` or `client.execute_sql(&self.plan.sql, timeout_ms)`.
- Pair with in-flight `CMD_CANCEL_REQUEST` IPC propagation tracked in `W1-A-006`.

## Resolution
- Updated `BoltCustomScanState::execute` (`src/custom_scan.rs:121-137`) to call `crate::runtime::check_pg_cancel_pending()?;` at entry, compute `let timeout_ms = crate::runtime::effective_timeout_with_pg(self.plan.timeout_ms);`, and pass `timeout_ms` to both `runtime.execute_sql_with_timeout(&self.plan.sql, timeout_ms)` (`Direct`) and `client.execute_sql(&self.plan.sql, timeout_ms)` (`BgWorker`).
- Existing `custom_scan::tests::direct_execute_honors_zero_and_nonzero_timeout_paths` covers the direct execution path; coordinator build/test runs pending.
