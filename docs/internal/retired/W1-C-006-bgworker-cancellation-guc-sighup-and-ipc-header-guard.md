# W1-C-006: BGWorker query cancellation propagation, `SIGHUP` GUC reload, and Arrow IPC header bounds guard

| Field | Value |
| :--- | :--- |
| Title | BGWorker query cancellation propagation, `SIGHUP` GUC reload, and Arrow IPC header bounds guard |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/bgworker.rs`, `src/ipc.rs`, `../bolt/src/exec/cancel.rs:65-130` |

## Description
Three runtime robustness defects identified during Phase 2 review:
1. **Timed-out query GPU starvation (`bgworker.rs`)**: `IpcCommand::ExecuteSql { sql, timeout_ms }` enforces `timeout_ms` on the client's `submit_request` wait, and abandons the slot if the client times out. However, unless `BoltSessionRuntime` executes the query with a `craton_bolt::CancellationToken::with_timeout(Duration::from_millis(timeout_ms))` (via `Engine::sql_with_cancel`), a slow or hung query keeps running on the single-threaded GPU worker and starves all other backends.
2. **`SIGHUP` live runtime GUC reload (`bgworker.rs`)**: `pg_bolt_bgworker_main` invokes `ProcessConfigFile(PGC_SIGHUP)` on `SIGHUP`, which refreshes `GucSetting` globals, but `BoltSessionRuntime` has no reconfiguration API yet to apply updated `pg_bolt.max_vram_mb` or `pg_bolt.allow_hybrid_fallback` to an already-running worker without restart.
3. **Malformed Arrow IPC `meta_len` panic in `arrow-ipc 53.4.1` (`ipc.rs`)**: `arrow_ipc::reader::StreamReader::try_new` / message reading reads a signed `i32` FlatBuffers metadata length and allocates `vec![0; meta_len as usize]` directly without validating `meta_len > 0` or `meta_len as usize <= remaining_bytes`. `ipc_stream_to_record_batch` now guards the initial schema frame, but multi-message IPC streams still depend on validating subsequent batch message headers (or tracking `arrow-ipc` upstream fixes).

## Reproduction
1. Pass `[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]` (continuation marker + `meta_len = -1`) directly to `arrow_ipc::reader::StreamReader::try_new`: `vec![0; (-1_i32) as usize]` panics with capacity overflow.
2. Submit a long-running query with `timeout_ms = 50` to a worker without `CancellationToken`: the client returns an error after 50ms while the worker stays blocked until the query completes.

## Suggested fix
- Pre-validate the initial Arrow IPC stream framing (`meta_len > 0` and bounded by input slice length) inside `ipc_stream_to_record_batch` (fixed directly in `src/ipc.rs`), and add a test covering truncated/negative-length IPC payloads.
- Expose a timeout-aware execution method on `BoltSessionRuntime` backed by `Engine::sql_with_cancel` + `CancellationToken::with_timeout` so `bgworker.rs` cancels overdue queries inside Bolt itself.
- Re-sync runtime config from `BoltConfig::from_gucs()` on `BackgroundWorker::sighup_received()` inside `pg_bolt_bgworker_main`.

## Resolution (Wave 2 + Wave 3)
- `ipc_stream_to_record_batch` (`src/ipc.rs`) pre-validates Arrow IPC frames (`decode_frame`) including positive `meta_len`, bounds checks, duplicate schema rejection, and EOF validation.
- `BoltSessionRuntime::execute_sql_with_timeout` and `BoltSessionRuntime::reconfigure` (`src/runtime.rs`) were implemented and wired into `src/bgworker.rs` (`execute_sql_with_effective_timeout` calls `execute_sql_with_timeout` directly across both `pgrx` and non-`pgrx` builds, and `pg_bolt_bgworker_main` invokes `runtime.reconfigure(&worker_cfg)` on `SIGHUP`).
