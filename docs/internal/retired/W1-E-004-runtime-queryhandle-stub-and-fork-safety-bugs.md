# W1-E-004: `runtime.rs` `QueryHandle::into()` compile failure, missing `cuda-stub` fallback, and CUDA fork-safety gap

| Field | Value |
| :--- | :--- |
| Title | `runtime.rs` `QueryHandle::into()` compile failure, missing `cuda-stub` fallback, and CUDA fork-safety gap |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:15-16,202-209,257,266-287`, `../bolt/src/exec/engine.rs:1659-1692,5348-5406` |

## Description
The session runtime design in `PG_BOLT_PLAN_AND_CODE.md` conflicts with `craton-bolt` 0.12.0 and PostgreSQL's multi-process architecture:
1. Line 257 writes `let batch: RecordBatch = handle.into();`. In `craton-bolt` 0.12.0 (`engine.rs:5348-5406`), `QueryHandle` does not implement `From<QueryHandle> for RecordBatch`; callers must invoke `handle.into_record_batch()` (or `handle.record_batch()`).
2. `Engine::new()` (`:205`) invokes `cuda_sys::init()`, which unconditionally fails with `BoltError::Unsupported` under `cuda-stub`, so `BoltSessionRuntime` cannot initialize on CPU-only developer/CI machines without an explicit stub runtime path.
3. `EngineBuilder::build` (`engine.rs:1659-1692`) allows only **one active `Engine` per OS process** (`ACTIVE_ENGINE` atomic guard). Using `thread_local! BOLT_RT` on multi-threaded callers or test runners triggers `"only one active Engine is supported per process"`, and initializing CUDA inside a PostgreSQL postmaster prior to `fork()` corrupts child backend CUDA contexts (as well as reintroducing the ~300 MB/backend context overhead noted at `:15-16`).

## Reproduction
1. Compile `PG_BOLT_PLAN_AND_CODE.md:257` against `craton-bolt` 0.12.0 (`E0277: the trait From<QueryHandle> is not implemented for RecordBatch`).
2. Call `Engine::new()` with `--features cuda-stub`, or construct two `BoltSessionRuntime` instances concurrently in the same process.

## Suggested fix
- Replace `handle.into()` with `handle.into_record_batch()`.
- Support `cuda-stub` cleanly in `BoltSessionRuntime` (tracking tables/VRAM LRU and explaining `ExecutionTier::Host` locally when `is_cuda_stub()` is true, while rejecting GPU kernel execution cleanly).
- Enforce a `postmaster` fork guard before lazy CUDA initialization in backends, route shared execution through the dedicated `BoltBgWorker` + shared-memory IPC in production, and serialize single-process test access via `test_process_lock()`.
