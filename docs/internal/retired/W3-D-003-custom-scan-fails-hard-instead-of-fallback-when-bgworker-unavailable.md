<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-D-003: `CustomScan` aborts query instead of falling back when `use_bgworker = on` and `shared_bgworker_client()` fails with `PgBoltError::Ipc`

| Field | Value |
| :--- | :--- |
| Title | `CustomScan` aborts query instead of falling back when `use_bgworker = on` and `shared_bgworker_client()` fails with `PgBoltError::Ipc` |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/custom_scan.rs:312-330,664-702`, `src/planner.rs:2295-2375`, `src/bgworker.rs:867-891` |

## Description
`pg_bolt.use_bgworker` defaults to `true`, and `pg_bolt_planner_hook` (`src/planner.rs:2295-2375`) attaches a `CustomScan` (preserving PostgreSQL's native `orig_plan` in `custom_plans`) whenever `PLANNER_CATALOG` has a matching cached relation, without checking whether the background worker IPC channel is healthy (`bgworker::shared_bgworker_client().is_ok()`).

During execution in `bolt_exec_custom_scan` (`src/custom_scan.rs:664-702`):
- If `(*bolt_node).use_bgworker` is `true` and `crate::bgworker::shared_bgworker_client()` (`src/bgworker.rs:867-891`) or IPC submission returns `Err(PgBoltError::Ipc(_))` (e.g., worker startup/attach failure, worker thread panic/shutdown, or ring unavailability), `exec_res` becomes `Err(PgBoltError::Ipc(_))`.
- `should_fallback_on_error(&err, state.plan.fallback_on_oom)` (`src/custom_scan.rs:312-330`) only returns `true` for `PgBoltError::VramExhausted` and `PgBoltError::Bolt` GPU-keyword errors.
- Consequently, `bolt_exec_custom_scan` raises `pgrx::error!("pg_bolt CustomScan execution failed: {err}")` and aborts the SQL query even though a valid PostgreSQL `fallback_plan` is attached on the `CustomScan` node.

## Reproduction
1. Load a table into `PLANNER_CATALOG` with `pg_bolt.use_bgworker = on` and `pg_bolt.enable_custom_scan = on`.
2. Induce worker unavailability / `PgBoltError::Ipc` when `shared_bgworker_client()` or IPC dispatch executes for an eligible `SELECT ... FROM sales`.
3. `pg_bolt_planner_hook` attaches `CustomScan`, and `bolt_exec_custom_scan` errors out instead of delegating to `(*bolt_node).fallback_plan`.

## Suggested fix
- Either decline attaching `CustomScan` in `pg_bolt_planner_hook` when `config.use_bgworker` is `true` and the shared background worker is unavailable, or allow `bolt_exec_custom_scan` to fall back to `(*bolt_node).fallback_plan` on `PgBoltError::Ipc` when `fallback_on_oom` (`allow_hybrid_fallback`) is `true` (never on `PgBoltError::Cancelled`).
- Add a unit test covering `CustomScan` fallback/decline behavior when `shared_bgworker_client()` returns `PgBoltError::Ipc`.

## Resolution
- Updated `is_gpu_execution_error` (`src/custom_scan.rs:313-328`) to return `true` for `PgBoltError::Ipc(_)` (alongside `PgBoltError::VramExhausted(_)` and GPU-keyword `PgBoltError::Bolt(_)`) while explicitly returning `false` for `PgBoltError::Cancelled(_)` so cancellations are never masked by fallback.
- Added unit tests in `custom_scan::tests` (`non_gpu_errors_are_not_masked_by_fallback` and `ipc_errors_fallback_when_allowed_and_propagate_when_disabled`, `src/custom_scan.rs:1058-1116`) verifying `PgBoltError::Cancelled` never triggers fallback and `PgBoltError::Ipc` triggers CPU fallback when `fallback_on_oom == true` and propagates when `fallback_on_oom == false`. Coordinator build/test runs pending.
