<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-006: Bridge PostgreSQL SIGINT query cancellation and statement_timeout into IPC CancellationToken

| Field | Value |
| :--- | :--- |
| Title | Bridge PostgreSQL SIGINT query cancellation and statement_timeout into IPC CancellationToken |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/runtime.rs`, `src/ipc.rs`, `src/bgworker.rs`, `src/guc.rs` |

## Description
`BoltSessionRuntime::execute_sql` configures `CancellationToken::new().with_timeout(Duration::from_millis(timeout_ms))` from `pg_bolt.statement_timeout_ms`. However:
1. User-initiated query cancellation in PostgreSQL (`pg_cancel_backend`, `Ctrl+C` in `psql`, or PostgreSQL's own `statement_timeout`) sets `QueryCancelPending` / `ProcDiePending` in the backend rather than firing Rust's `CancellationToken::cancel()`.
2. `ipc::IpcCommand` does not yet carry a cancellation token/epoch or request-cancel control command so a canceled backend can abort an in-flight query executing inside the CUDA background worker.

## Reproduction
1. Launch a long-running query via `bolt.query(...)` with `pg_bolt.statement_timeout_ms = 0` and PostgreSQL `SET statement_timeout = '50ms'` (or issue `SELECT pg_cancel_backend(...)`).
2. Expected: In-flight Bolt GPU query cancels cooperatively via `CancellationToken::cancel()` and returns promptly.
3. Actual: `craton-bolt` finishes the GPU work before the PostgreSQL backend observes `CHECK_FOR_INTERRUPTS()` upon regaining control.

## Suggested fix
1. Add a per-request cancel flag / command (`CMD_CANCEL_REQUEST`) or shared atomic flag per ring slot in `ipc::SharedMemoryArena` wired to `CancellationToken`.
2. Poll `pg_sys::QueryCancelPending` / `pg_sys::ProcDiePending` in the backend wait loop during IPC or via a lightweight watchdog/signal-safe flag, calling `token.cancel()` and propagating `PgBoltError::Bolt` / PG interrupt handling cleanly.
3. Verify cancellation with an integration test firing `pg_cancel_backend` mid-query.

## Wave 2 Progress (`src/error.rs`, `src/runtime.rs`)
- Added `PgBoltError::Cancelled(String)` mapped from `craton_bolt::BoltError::Cancelled` (`src/error.rs`).
- Added `BoltSessionRuntime::execute_sql_with_timeout(&mut self, sql, timeout_ms)` (`src/runtime.rs`), combining explicit `timeout_ms` with PostgreSQL `StatementTimeout`, checking `QueryCancelPending`/`ProcDiePending`, and spawning a `Condvar`-synchronized watcher under `pgrx` to fire `CancellationToken::cancel()` cooperatively.
- Remaining: `bgworker::execute_sql_with_effective_timeout` calling `execute_sql_with_timeout` (`W2-C-001`) and `CMD_CANCEL_REQUEST` IPC slot propagation (`W1-C-006`).

## Wave 3 Progress (`src/runtime.rs`, `src/lib.rs`)
- Hardened `run_engine_sql_with_pg_cancel`'s `pgrx` `Condvar` watcher with an RAII `WatcherDoneGuard` (preventing `std::thread::scope` join hang on panic unwind), early exit on `watcher_token.is_cancelled()`, and shared `pg_cancel_pending()` / `effective_timeout_with_pg()` helpers.
- Wired `api::query` (`src/lib.rs`) through `check_pg_cancel_pending()` and `effective_timeout_with_pg(current_config().statement_timeout_ms)` so client-session `SET statement_timeout` and pending interrupts apply to `api::query` over BGWorker IPC too (remaining: `CMD_CANCEL_REQUEST` for in-flight BGWorker queries and `custom_scan.rs` client timeout propagation tracked in `W3-A-003`).

## Wave 5 Resolution (`src/ipc.rs`, `src/bgworker.rs`)
- **Protocol & Slot Cancellation**: Added `CMD_CANCEL_REQUEST = 15` wire tag and `IpcCommand::CancelRequest { req_id: u64 }` variant. Added per-slot atomic cancellation flags (`cancel_flags: Vec<Arc<AtomicBool>>`) in `SharedMemoryArena` and `cancel_requested: AtomicBool` in `PgShmemSlotDesc`.
- **Cancellation Query & Control API**: Added `recv_request_with_cancel`, `request_cancel`, `is_cancel_requested`, and `cancel_flag_for_req` on `SharedMemoryArena`.
- **Client Polling**: Updated `SharedMemoryArena::wait_for_response` to slice timeout into 25ms increments, checking `inner.cancel_flags[slot_idx]` and `crate::runtime::pg_cancel_pending()`, firing `pg_shmem::signal_slot_cancel(req_id)` and returning `Err(PgBoltError::Cancelled(...))` upon interrupt.
- **Worker Bridge**: Added `BgWorkerClient::request_cancel(req_id)`. In `step_command` and `dispatch_command`, handled `IpcCommand::CancelRequest` and checked `cancel_flag` prior to execution. In `dispatch_command` under `pgrx`, spawned a watcher thread bridging `cancel_flag` into `QueryCancelPending = 1` during query execution (triggering `run_engine_sql_with_pg_cancel`'s internal watcher in `runtime.rs` to call `CancellationToken::cancel()`), cleanly resetting it to 0 on exit.
- **Unit Tests**:
  - `cancel_request_wire_encoding_and_decoding` in `src/ipc.rs`.
  - `shared_memory_arena_cooperative_cancellation_and_poll` in `src/ipc.rs`.
  - `test_bgworker_dispatch_cancel_flag_short_circuit` in `src/bgworker.rs`.
  - `test_bgworker_cancel_request_command_and_cooperative_cancel` in `src/bgworker.rs`.
