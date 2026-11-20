<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-C-002: Wire `_PG_init` shmem/bgworker hooks and DSM segment payload transport across PG processes

| Field | Value |
| :--- | :--- |
| Title | Wire `_PG_init` shmem/bgworker hooks and DSM segment payload transport across PG processes |
| Severity | high |
| Status | fixed |
| Kind | proposal |
| Files | `src/lib.rs:27-34`, `src/ipc.rs`, `src/bgworker.rs`, `src/custom_scan.rs:425-510` |

## Description
`src/ipc.rs` now provides `init_shmem_hooks()` and `pg_shmem::{PgShmemRingControl, PgShmemSlotDesc, PgShmemLwLockGuard, register_worker_latch, signal_worker_latch, wait_latch_timeout}` (`ShmemInitStruct`, `RequestAddinShmemSpace`, `RequestNamedLWLockTranche`, `LWLock`, `SetLatch`/`WaitLatch`), and `src/bgworker.rs` exposes `shared_bgworker_client()`.

Two cross-module steps remain before separate PostgreSQL client backends share the single persistent `pg_bolt GPU daemon` worker across OS processes:
1. `src/lib.rs::_PG_init` (`:27-34`) currently calls only `guc::register_gucs()` and `planner::install_planner_hook()`, and does not yet invoke `ipc::init_shmem_hooks()` and `bgworker::register_bgworker()`.
2. `PgShmemRingControl` and `PgShmemSlotDesc` currently hold control state, `worker_latch`, and `dsm_handle` fields, while `pg_bolt_bgworker_main` and `shared_bgworker_client()` still instantiate in-process `SharedMemoryArena` (`parking_lot::Mutex` + `Vec<u8>`) rather than attaching client-created `dsm_segment` (`dsm_create` / `dsm_attach` / `dsm_segment_address`) payloads referenced via `PgShmemSlotDesc::dsm_handle`.

## Reproduction
1. Load `pg_bolt` via `shared_preload_libraries = 'pg_bolt'`.
2. `_PG_init` does not yet call `ipc::init_shmem_hooks()` or `bgworker::register_bgworker()`, and separate backend processes calling `shared_bgworker_client()` spawn a per-process worker thread rather than exchanging `IpcCommand`/`IpcResponse` payloads over `PgShmemRingControl` + `dsm_segment`.

## Suggested fix
- Call `ipc::init_shmem_hooks()` and `bgworker::register_bgworker()` from `src/lib.rs::_PG_init` (see also `W1-A-002`).
- Back `SharedMemoryArena` under `#[cfg(feature = "pgrx")]` (when `is_shmem_initialized()` is true) with `PgShmemLwLockGuard`, `PgShmemSlotDesc` state transitions, `dsm_create`/`dsm_attach` payload segments pinned in `TopMemoryContext`/`dsm_pin_mapping`, and `SetLatch`/`WaitLatch` wakeups, and route `custom_scan.rs` (`W1-D-004`) through `shared_bgworker_client()`.

## Progress Note (Wave 3)
- `src/lib.rs::_PG_init` (`lines 29-42`) now invokes `ipc::init_shmem_hooks()` and `bgworker::register_bgworker()`.
- Remaining (tracked alongside `W1-C-004`): wire `SharedMemoryArena` / `pg_bolt_bgworker_main` / `shared_bgworker_client()` under `#[cfg(feature = "pgrx")]` to transfer payloads across OS processes via `PgShmemRingControl`, `PgShmemSlotDesc`, and `dsm_segment`.

## Wave 5 Resolution (`src/ipc.rs`, `src/bgworker.rs`)
- **DSM Segment RAII Lifetime**: Added `PgDsmSegment` wrapping `dsm_create`, `dsm_attach`, `dsm_pin_mapping`, and automatic `dsm_detach` on Drop.
- **Client DSM Request Submission**: Implemented `pg_shmem::shmem_submit_request(cmd, timeout)` which creates a DSM segment for the wire payload, transitions a ring slot from `PG_SLOT_FREE` to `PG_SLOT_PENDING`, triggers `signal_worker_latch()`, polls with 25ms interrupt checks (`QueryCancelPending`/`ProcDiePending`), and reads the completed response payload directly from DSM.
- **Client Preference & Transparent Fallback**: `BgWorkerClient::submit` checks `is_shmem_initialized() && self.use_shmem_when_initialized`, routing across processes via `shmem_submit_request` in production and falling back to in-process `SharedMemoryArena` for unit tests or uninitialized environments.
- **Worker DSM Processing**: In `pg_bolt_bgworker_main`, added polling loop draining pending slots via `pg_shmem::claim_pending_request()`, attaching to client DSM segments, executing commands through `dispatch_command`, writing responses back to DSM, setting `PG_SLOT_COMPLETED`, and waking the client with `pg_sys::SetLatch(client_latch)`.
- **Unit Tests**:
  - `test_bgworker_client_preference_and_shmem_fallback` in `src/bgworker.rs`.
  - `shmem_slot_desc_mock_state_transitions` in `src/ipc.rs`.
  - `shmem_ring_control_offsets_and_budget_tracking` in `src/ipc.rs`.
