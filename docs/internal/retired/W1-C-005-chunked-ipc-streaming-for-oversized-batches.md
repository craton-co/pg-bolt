# W1-C-005: Chunked IPC streaming / DSM sideband for >`slot_bytes` Arrow batches

| Field | Value |
| :--- | :--- |
| Title | Chunked IPC streaming / DSM sideband for >`slot_bytes` Arrow batches |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/ipc.rs`, `src/bgworker.rs`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:58-60,757-766` |

## Description
`SharedMemoryArena` bounds each ring slot to `IpcRingConfig::slot_bytes` (e.g., 16 MiB) and cleanly rejects oversized `LoadBatch`, `AppendDelta`, or `Batch` responses with `PgBoltError::Ipc` to avoid unbounded OOM.

However, real analytical workloads routinely exceed 16 MiB in a single relation or query result—for example, the 5,000,000-row `sales` benchmark in `PG_BOLT_PLAN_AND_CODE.md:757-766` (~160 MiB raw Arrow IPC payload). Without multi-chunk streaming or out-of-band DSM handle passing, loading or returning >`slot_bytes` batches over the ring fails.

## Reproduction
1. Start `BoltBgWorker` with `slot_bytes = 16 * 1024 * 1024`.
2. Call `BgWorkerClient::load_batch("sales", "sales", &five_million_row_batch)`.
3. Expected: table loads cleanly by chunking or sideband transfer. Actual: rejected with `PgBoltError::Ipc` because encoded IPC exceeds `slot_bytes`.

## Suggested fix
- Split `LoadBatch` and large `Batch` responses into chunked Arrow IPC frames (`BeginTableStream` / `TableChunk { stream_id, seq, eof }` / `EndTableStream`) bounded by `slot_bytes`, using `BoltSessionRuntime`'s incremental `append_delta` / streaming registration on the worker side.
- For single giant result batches under PostgreSQL, optionally allocate an ephemeral `dsm_segment` (`dsm_segment_handle`) and pass only the `dsm_handle` + byte length through the fixed ring slot.
