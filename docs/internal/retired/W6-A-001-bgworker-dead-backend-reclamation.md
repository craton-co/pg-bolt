# W6-A-001: Cross-Backend Dead Client PID Reclamation in PG Shmem Slots

| Field | Value |
| :--- | :--- |
| Title | Cross-Backend Dead Client PID Reclamation in PG Shmem Slots |
| Severity | low |
| Status | retired |
| Kind | proposal |
| Files | `src/bgworker.rs:1150-1180`, `src/ipc.rs:3270-3330` |

## Description
When a PostgreSQL client backend dies abruptly (e.g., killed by SIGKILL, kernel OOM-killer, or abnormal container termination) while holding an allocated slot in `pg_shmem`, the backend does not have an opportunity to execute its `Drop` handlers or mark `PG_SLOT_ABANDONED`.

While `pg_bolt` now sweeps `PG_SLOT_ABANDONED` and claims pending/completed slots cleanly during normal operation and query cancellation, slots held by dead backend PIDs that died mid-initialization or after completion could persist until the background worker restarts.

## Reproduction
1. Start PostgreSQL with `shared_preload_libraries = 'pg_bolt'`.
2. Connect a client backend and issue a long query or chunked transfer.
3. Abruptly kill the client backend OS process via `kill -9 <client_pid>`.
4. The slot allocated by that backend retains `client_pid = <client_pid>` in PostgreSQL shared memory.

## Suggested fix
In `claim_pending_request` or a periodic background worker sweep in `pg_bolt_bgworker_main`:
1. Inspect slots in `PG_SLOT_INITIALIZING`, `PG_SLOT_PENDING`, or `PG_SLOT_COMPLETED`.
2. If `client_pid > 0`, probe process liveness (e.g. via `libc::kill(client_pid, 0) == -1 && errno == ESRCH` on Unix or `OpenProcess` check on Windows).
3. If the recorded backend PID is deceased, reset the slot to `PG_SLOT_FREE` and log a notice in PostgreSQL logs.

## Resolution
- Implemented `is_process_alive(pid: i32) -> bool` with cross-platform OS probing (`OpenProcess` + `GetExitCodeProcess` on Windows, POSIX `kill(pid, 0)` on Unix).
- Integrated dead client backend reclamation directly into `claim_pending_request` and provided `sweep_dead_client_slots() -> usize`.
- Verified via unit test `process_liveness_probes_and_dead_backend_detection`.
- Retired in Wave 6.
