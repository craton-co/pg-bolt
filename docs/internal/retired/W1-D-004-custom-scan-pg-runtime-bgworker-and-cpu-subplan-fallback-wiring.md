# W1-D-004: `CustomScan` execution needs shared `BgWorkerClient` wiring, `Direct` `timeout_ms` support, and fallback child `Plan` execution

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` execution needs shared `BgWorkerClient` wiring, `Direct` `timeout_ms` support, and fallback child `Plan` execution |
| Severity | high |
| Status | fixed |
| Kind | proposal |
| Files | `src/custom_scan.rs:128-140,425-455`, `src/runtime.rs:644-681`, `src/bgworker.rs:134-192,330-344`, `src/planner.rs:1049-1106` |

## Description
Three cross-module gaps limit `BoltCustomScanState` inside live PostgreSQL backends (`#[cfg(feature = "pgrx")]`):
1. **Per-backend CUDA fallback in `bolt_exec_custom_scan`**: `src/custom_scan.rs` currently executes `ScanExecutionBackend::Direct` via `with_bolt_runtime` inside `bolt_exec_custom_scan` because `src/bgworker.rs` does not yet expose a backend-side `BgWorkerClient` handle connected via PostgreSQL DSM/Shmem (`W1-C-004`). That initializes a ~300 MB CUDA context per backend process even when `pg_bolt.use_bgworker = on`.
2. **Per-plan `timeout_ms` ignored on `Direct` backend**: `BoltSessionRuntime::execute_sql(&mut self, sql: &str)` (`src/runtime.rs:644,662`) only reads `current_config().statement_timeout_ms` and ignores `BoltCustomScanPlan::timeout_ms` (just as `bgworker.rs:330` still ignores `IpcCommand::ExecuteSql { timeout_ms }`, tracked in `W1-C-006`).
3. **No fallback PostgreSQL subplan inside `ExecCustomScan`**: In pure Rust (`BoltCustomScanState::execute_with_cpu_fallback`), `VramExhausted` invokes a CPU fallback closure cleanly. Inside `bolt_exec_custom_scan` (`pgrx`), `VramExhausted` calls `pgrx::error!` because `attach_custom_scan_plan` (`src/planner.rs:1049`) did not attach the original `standard_planner` `Plan` node inside `CustomScan.custom_plans`.

## Reproduction
1. Attach a `CustomScan` plan under `pg_bolt.use_bgworker = on`, or set `BoltCustomScanPlan { timeout_ms: 100, .. }` while `pg_bolt.statement_timeout_ms = 0`, or trigger `VramExhausted` inside `ExecCustomScan`.
2. Backend initializes local runtime instead of IPC worker, `Direct` ignores `plan.timeout_ms`, and `VramExhausted` aborts the SQL statement instead of falling back to CPU execution.

## Suggested fix
- Add `BoltSessionRuntime::execute_sql_with_timeout(&mut self, sql: &str, timeout_ms: u64)` backed by `CancellationToken::with_timeout` and call it from both `ScanExecutionBackend::Direct` and `bgworker.rs::dispatch_command`.
- Expose a global backend `BgWorkerClient` accessor once cross-process Shmem/DSM (`W1-C-004`) is wired, and route `bolt_exec_custom_scan` through `ScanExecutionBackend::BgWorker` when `current_config().use_bgworker` is true.
- Push the original `standard_planner` `orig_plan` onto `CustomScan.custom_plans` (initializing it lazily only if GPU execution returns `VramExhausted` before rows are emitted) so `ExecCustomScan` falls back transparently to PostgreSQL CPU tuples mid-query.
