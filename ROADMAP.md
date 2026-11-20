<!-- SPDX-License-Identifier: Apache-2.0 -->

# Roadmap

`pg_bolt` tracks implemented capabilities against production deployment milestones. All 66 architectural issues filed across Waves 1–6 have been resolved and retired to [`docs/internal/retired/`](docs/internal/retired/).

## Milestone 1: Core Extension & Pure-Rust / CUDA Foundations (Completed)

- [x] `pgrx` 0.19 extension skeleton with `pg15`–`pg18` and `cuda` / `cuda-stub` feature gates
- [x] `bolt.*` SQL surface and `pg_bolt.*` GUCs
- [x] Arrow bridge (incl. `BYTEA`/`TIME`/`UUID`, inline `PGLZ` decompression, `attisdropped`/`attmissingval`/`LP_REDIRECT`), bounded SPI extraction, and parallel MVCC visibility filter (`pg_xact`/`pg_multixact`/`suboverflowed`)
- [x] Fork-safe `BoltSessionRuntime` with LRU VRAM budgeting and `craton-bolt` `0.12.0` execution
- [x] `SqlInspector` AST eligibility analysis, `ORDER BY` null-ordering + 3-part `schema.table.col` rewrite, and `CustomScan` lifecycle with `orig_plan` CPU fallback & per-tuple `MemoryContext`
- [x] `pg_hooks` transaction/executor DML invalidation hooks (`XactCallback`, `SubXactCallback`, `RelcacheCallback`) + worker `SetTableDirty` / `ExplainSql` IPC parity
- [x] `SharedMemoryArena` ring buffer + `BoltBgWorker` command loop with chunked IPC request & large `ExecuteSql` response streaming, `pg_shmem` control hooks, and `MinimalTuple` extended/view/`Dictionary<Int64, Utf8>` support
- [x] `CustomScan` `NUMERICOID` aggregate result bridge (`W3-D-001`), `Ipc` fallback (`W3-D-003`), `slot_getallattrs` + `datumCopy` varlena isolation (`W3-E-002`), `INDEX_VAR` `targetlist` rewriting (`W3-E-003`), live PG `statement_timeout` & `QueryCancelPending` forwarding (`W3-A-003`), and `bolt.explain_sql(sql text)` (`W3-A-001`, `W3-E-001`)
- [x] CI workflow matrix and CUDA PostgreSQL 17 Dockerfile (`W1-A-008`)

## Milestone 2: Multi-Backend PostgreSQL Production Hardening (Completed)

- [x] **Cross-Process PostgreSQL DSM Payload Ring (`W1-C-004`, `W2-C-002`)**: `_PG_init` unconditionally registers `ipc::init_shmem_hooks()` and `bgworker::register_bgworker()`; `ipc::pg_shmem` provides `PgShmemRingControl` + `PgShmemSlotDesc` headers, named `LWLock` tranche, latch signaling, and `PgDsmSegment` RAII payload transport across separate `fork()`ed processes.
- [x] **Cross-Backend Planner Catalog & Runtime Parity (`W1-D-005`, `W3-A-002`)**: Atomic monotonic `RUNTIME_CATALOG_EPOCH` and `table_epochs` tracking across backends; snapshot staleness detection via 32-bit wrap-safe `xid_precedes`; runtime OOC memory budget tracker with LRU shedding down to dynamic GUC limits.
- [x] **Standalone Offline TOAST Page Reconstruction & LZ4 Decompression (`W2-B-003`)**: Pure-Rust inline `LZ4` (`4B_C` method 1) and `PGLZ` (`4B_C` method 0) decompression; `StandaloneToastReader` decodes raw `pg_toast_<oid>` relation pages, reconstructs contiguous chunks, and detoasts multi-chunk external `1B_E` pointers without requiring an active PostgreSQL backend.
- [x] **Planner Hook, Upper Rel & Semantics Divergence Guards (`W1-D-003`, `W1-D-006`, `W1-E-006`, `W1-E-008`)**: Validate relation OIDs, schema namespaces, and `relkind == RELKIND_RELATION` against `CachedTableMeta`, declining offload on views, partitioned roots, and foreign tables; filter `resjunk` entries for unprojected `ORDER BY` targets and wire `CustomPath` hooks; guard `NaN` floating point comparisons and `SUM(bigint)` overflow divergence; normalize implicit sorts to `DESC NULLS FIRST`.
- [x] **Streaming Multi-Batch SPI, Unconstrained NUMERIC & Cooperative Cancel (`W1-A-006`, `W1-A-007`, `W2-B-001`, `W2-B-002`)**: `extract_relation_to_arrow_stream` yields bounded cursor chunks; dynamic `NUMERIC` typmod parsing supports up to 76 digits with `Decimal256` and configurable `Float64` fallback; transactional savepoint rollback frames (`SessionTxFrame`) recover uncommitted table registrations and restore dirty flags on abort; `CMD_CANCEL_REQUEST` and per-slot atomic cancellation flags forward PostgreSQL interrupts into `CancellationToken`.
- [x] **Hardware Benchmarks**: End-to-end benchmark suite executed on NVIDIA GeForce RTX 2060 verifying 889.07 MB/s heap page decode, 43,599 IPC ops/sec, and 35.74 million rows/sec GPU query execution.

## Milestone 3: Future Directions & Upstream Enhancements

- [ ] Linux Docker CI automated live `cargo pgrx test` execution with PostgreSQL 15, 16, 17, and 18.
- [ ] Upstream `craton-bolt` RFC for native `SqlSemantics::Postgres` (checked integer arithmetic, `DESC NULLS FIRST` preset).
- [ ] Upstream `craton-bolt` RFC for dynamic in-place `Engine::set_memory_budget`.
- [ ] Direct GPU-direct storage (GDS / cuFile) bypass for raw NVMe tablespace scans.
