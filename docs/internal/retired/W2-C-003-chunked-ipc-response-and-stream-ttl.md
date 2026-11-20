<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-C-003: Chunked IPC response streaming (`IpcResponse::BatchChunk`) and partial-stream TTL cleanup

| Field | Value |
| :--- | :--- |
| Title | Chunked IPC response streaming (`IpcResponse::BatchChunk`) and partial-stream TTL cleanup |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/ipc.rs:895-1155,1935-2040`, `src/bgworker.rs:195-245,425-505` |

## Description
`W1-C-005` adds request-side chunked streaming (`IpcChunkHeader`, `split_payload_into_chunks`, `ChunkReassembler`, `IpcCommand::LoadBatchChunk`, `IpcCommand::AppendDeltaChunk`, `SharedMemoryArena::submit_chunked_*`, and `BgWorkerClient::load_batch_chunked` / `append_delta_chunked`) so arbitrarily large tables load cleanly over fixed `ipc_slot_bytes` rings.

Two symmetry gaps remain for production multi-tenant workloads:
1. **Oversized query result batches (`IpcResponse::Batch`)**: When `ExecuteSql` returns a result `RecordBatch` whose Arrow IPC stream exceeds `slot_bytes`, `SharedMemoryArena::complete_request` rejects encoding `IpcResponse::Batch`, causing `complete_or_fallback` in `src/bgworker.rs` to return `IpcResponse::Error` instead of streaming chunks back to `BgWorkerClient::execute_sql`.
2. **Stalled partial request streams in `ChunkReassembler`**: If a client backend crashes, times out, or hits an intermediate error after sending chunk `0..k` of `LoadBatchChunk`, those buffered chunks remain in the worker's `ChunkReassembler` map until overwritten or worker shutdown.

## Reproduction
1. Configure `ipc_slot_bytes = 4096` and execute a query whose result `RecordBatch` encodes to 16 KiB over `BgWorkerClient::execute_sql`: `complete_or_fallback` returns `response exceeded IPC limit`.
2. Abort a multi-chunk `LoadBatchChunk` transfer mid-stream: buffered chunk `Vec<u8>` allocations stay retained in `ChunkReassembler::streams`.

## Suggested fix
- Add `IpcCommand::CancelChunkStream { stream_id: u64 }` (sent automatically on client error/timeout via an RAII guard in `SharedMemoryArena::submit_chunked_with`) plus a last-activity timestamp / idle TTL eviction pass in `ChunkReassembler`.
- Add `IpcResponse::BatchChunk` (or sideband `dsm_handle` under `pgrx`) and matching chunk reassembly in `BgWorkerClient::execute_sql` for query results larger than `slot_bytes`.

## Resolution (Wave 3)
- Added `IpcCommand::CancelChunkStream { stream_id: u64 }`, `IpcCommand::FetchResponseChunk { stream_id: u64, chunk_idx: u32 }`, `IpcResponse::BatchChunk`, `ResponseChunkStash`, `ChunkStreamCancelGuard` (RAII cancellation on partial request/response failure or timeout), and `last_updated` + `prune_stale_streams` idle TTL cleanup in `ChunkReassembler` and `ResponseChunkStash` (`src/ipc.rs`).
- Wired `dispatch_command`, `run_worker_loop`, `pg_bolt_bgworker_main`, and `BgWorkerClient::execute_sql` (`src/bgworker.rs`) to stream oversized query result batches across tiny `slot_bytes` rings while preserving single-frame overflow rejection (`total_rejected_oversized`) on `submit_request`.
- Added unit tests in `src/ipc.rs` and `src/bgworker.rs`.
