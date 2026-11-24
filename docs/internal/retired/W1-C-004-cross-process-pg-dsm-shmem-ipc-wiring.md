# W1-C-004: Cross-process PostgreSQL DSM / `ShmemInitStruct` / `LWLock` / `Latch` wiring for `SharedMemoryArena`

| Field | Value |
| :--- | :--- |
| Title | Cross-process PostgreSQL DSM / `ShmemInitStruct` / `LWLock` / `Latch` wiring for `SharedMemoryArena` |
| Severity | high |
| Status | fixed |
| Kind | proposal |
| Files | `src/ipc.rs`, `src/bgworker.rs`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:20-40,56-60` |

## Description
`src/ipc.rs` implements `SharedMemoryArena` using `parking_lot::{Mutex, Condvar}` and heap-allocated `Vec<u8>` slot buffers, which validates the ring protocol, slot capacity bounds, backpressure, timeouts, and shutdown semantics inside a single OS process (threads and unit tests).

However, PostgreSQL is a multi-process (`fork()`) architecture: client backends and `pg_bolt GPU daemon` (`BackgroundWorker`) run in separate OS processes with separate virtual address spaces. Heap `Arc<SharedMemoryArena>` instances are not shared across `fork()`ed Postgres backends unless backed by PostgreSQL shared memory (`ShmemInitStruct` / `dsm_create` / `shm_open`) synchronized via `LWLock`s and woken via `SetLatch`/`WaitLatch`.

## Reproduction
1. Register `pg_bolt_bgworker_main` via `shared_preload_libraries = 'pg_bolt'`.
2. Connect from a separate `psql` backend process and submit an `IpcCommand` via a process-local `Arc<SharedMemoryArena>`.
3. The client backend and background worker observe separate heap arenas rather than a shared cross-process ring.

## Suggested fix
- Register shared-memory size (`RequestAddinShmemSpace`) and an `LWLock` tranche (`RequestNamedLWLockTranche("pg_bolt", ...)`) in `shmem_request_hook` / `_PG_init`, and initialize a fixed-layout `#[repr(C)]` ring header + slot buffers in `shmem_startup_hook` via `ShmemInitStruct`.
- Store each waiting backend's `PGPROC` / `WaitLatch` target or `pg_atomic_u32` wake flag in the slot header so `pg_bolt_bgworker_main` wakes the exact client backend with `SetLatch`, and the client wakes the worker's `MyLatch` on enqueue.
- Keep `SharedMemoryArena` as the in-process engine/test transport and share the identical binary `IpcCommand`/`IpcResponse` wire framing across both transports.

## Wave 5 Resolution (`src/ipc.rs`, `src/bgworker.rs`)
- **Shared Memory Hooks & Layout**:
  - `init_shmem_hooks()` installs `pg_bolt_shmem_request_hook` and `pg_bolt_shmem_startup_hook` on postmaster startup.
  - Sized via `RequestAddinShmemSpace` and initialized via `ShmemInitStruct` with `PgShmemRingControl` (`#[repr(C)]`) and contiguous `PgShmemSlotDesc` slice.
  - Named `LWLock` tranche (`"pg_bolt"`) initialized via `RequestNamedLWLockTranche` and managed via RAII `PgShmemLwLockGuard`.
- **Slot State Machine & Latch Signaling**:
  - `PgShmemSlotDesc` implements atomic states: `PG_SLOT_FREE (0)`, `PG_SLOT_PENDING (1)`, `PG_SLOT_IN_FLIGHT (2)`, `PG_SLOT_COMPLETED (3)`, `PG_SLOT_ABANDONED (4)`.
  - Added `cancel_requested: AtomicBool`, `client_pid: AtomicI32`, and `client_latch: AtomicPtr<pg_sys::Latch>`.
  - Backend signals `worker_latch` via `signal_worker_latch()`, and worker signals `client_latch` upon request completion via `SetLatch`.
- **Payload Management with Dynamic Shared Memory (DSM)**:
  - Concrete `PgDsmSegment` RAII struct wraps `pg_sys::dsm_create`, `pg_sys::dsm_attach`, `pg_sys::dsm_segment_address`, `pg_sys::dsm_segment_handle`, and pins mappings via `pg_sys::dsm_pin_mapping` before unpinning/detaching on Drop.
- **Wire Protocol Sharing**:
  - Both in-process `SharedMemoryArena` and cross-process `PgShmemRingControl` share identical binary framing (`encode_wire`, `decode_wire`, `encode_into_slice`, `decode_from_slice`) without duplicate formats.
- **Unit Tests**:
  - `shmem_slot_desc_mock_state_transitions` in `src/ipc.rs`.
  - `shmem_ring_control_offsets_and_budget_tracking` in `src/ipc.rs`.
  - `direct_slice_serialization_roundtrip` in `src/ipc.rs`.
