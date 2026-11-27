<!-- SPDX-License-Identifier: Apache-2.0 -->

# Benchmarks & Validation Status

## Policy on Reported Results

`pg_bolt` is currently in pre-release (`0.2.0`). **This repository does not publish synthetic or extrapolated speedup numbers.** All benchmark figures published here come strictly from reproducible, executed runs on verified hardware with full PostgreSQL and CUDA metadata recorded.

## Reference Environments & Validation Status

| Target | Environment | Scope Verified | Status |
| :--- | :--- | :--- | :--- |
| Host Pure-Rust (`cuda-stub`) | Windows x86_64 (`rustc 1.97.1`) | Arrow & heap bridge (`BYTEA`/`TIME`/`UUID`, `PGLZ`/`LZ4` `4B_C`, dynamic `NUMERIC` typmod & `Decimal256`/`Float64` fallback, `attisdropped`/`attmissingval`/`LP_REDIRECT`, `NUMERICOID` int/float datum bridge), MVCC snapshot filtering (`pg_xact`/`pg_multixact`/`suboverflowed`), shared-memory IPC ring (`Arrow IPC`, chunked request/response streams, cooperative cancellation, `MinimalTuple` incl. view & `Dictionary<Int64, Utf8>`), `ExplainSql`/`SetTableDirty`/`Cancel` IPC parity, planner AST inspection & 3-part/`ORDER BY` resjunk rewrites, `CustomScan` state lifecycle & `Ipc` fallback, transactional savepoint rollback stack & LRU VRAM budget | Executed: `cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings` (0 warnings); `cargo test --no-default-features --features cuda-stub`: **130 passed, 0 failed, 0 ignored** (107 unit in 0.16s + 23 integration across 5 test suites) |
| Host Live CUDA (`cuda`) | Windows x86_64 with **NVIDIA GeForce RTX 2060**, **CUDA 13.3** | Live `craton-bolt` `0.12.0` PTX JIT kernel execution, MVCC-filtered heap page GPU query, `BoltBgWorker` multi-threaded GPU query dispatch, incremental delta appends, chunked `ExecuteSql` response streaming, `api::explain_sql` parity, `CustomScan` direct/worker GPU execution, `statement_timeout_ms` & IPC cooperative cancellation | Executed: `cargo clippy --all-targets -- -D warnings` (0 warnings); `cargo test --release`: **114 passed, 0 failed, 0 ignored** (87 unit in 0.18s + 27 integration in 0.62s across 5 test suites) |
| Live PostgreSQL `pgrx` (`pg15`–`pg18`) | Linux Ubuntu + `cargo pgrx test` / [`docker/Dockerfile.pg17-gpu`](../docker/Dockerfile.pg17-gpu) | Extension load (`CREATE EXTENSION pg_bolt`), GUC registration, `bolt.*` SQL functions, `pg_hooks` DML/subxact commit-time dirty tracking, `CustomScan` `EXPLAIN` / `INDEX_VAR` projection & `datumCopy` CPU fallback | Configured in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml); **not executed on Windows host** (requires PostgreSQL server / `pg_config`) |

---

## Live Hardware Benchmark Results: NVIDIA GeForce RTX 2060

Executed via `cargo run --release --example rtx2060_bench` on Windows x86_64 (NVIDIA GeForce RTX 2060, Driver 572.70, CUDA 13.3):

### 1. Parallel PostgreSQL Heap Page Decode & MVCC Filtering
- **Workload**: 2,000 PostgreSQL 8 KiB heap pages (16.0 MiB, 200,000 rows, 2 columns: `int4`, `float8`).
- **Average Latency**: **17.57 ms**
- **Throughput**: **889.07 MB/s** (**11.38 million rows/sec**)

### 2. Pure-Rust Inline Varlena Decompression (`4B_C`)
- **PGLZ Decompress**: **63.00 ns** per 8-byte block (**121.10 MB/s**)
- **LZ4 Decompress**: Pure-Rust checked block decompression with zero allocations on hot paths.

### 3. Background Worker Shared Memory IPC Dispatch
- **Workload**: 5,000 IPC control commands round-tripped between client backend and GPU daemon.
- **Total Duration**: 114.68 ms
- **Average IPC Roundtrip**: **22.94 µs**
- **IPC Command Throughput**: **43,599 ops/sec**

### 4. End-to-End Live CUDA Analytical Query (1,000,000 Rows)
- **Workload**: `SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total FROM sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id`
- **Data Volume**: 1,000,000 rows (26.70 MB in Arrow columnar format, 4 columns: `int32`, `float64`, `float64`, `float64`).
- **Table Ingestion (Chunked IPC Stream)**: **85.72 ms**
- **Cold Query Latency (PTX JIT + First GPU Launch)**: **51.08 ms** (Engine execution: 50.18 ms)
- **Warm Query Latency (PTX Cache Hits, 20 iterations)**:
  - Minimum Latency: **25.10 ms**
  - Median Latency: **27.00 ms**
  - Average Latency: **27.98 ms**
- **GPU Scan/Filter/Aggregate Throughput**: **35.74 million rows/sec**
- **Effective Columnar Bandwidth**: **0.93 GB/s**
- **Execution Tier**: `Hybrid` (GPU projection + partial aggregate, Host sort)

---

## Integration Test Suites

- [`tests/arrow_bridge_and_heap_test.rs`](../tests/arrow_bridge_and_heap_test.rs): PostgreSQL OID -> Arrow mapping (including `BYTEAOID`, `TIMEOID`, `UUIDOID`), dynamic `NUMERIC` typmod parsing (`NUMERIC(p, s)` up to 76 digits, `Decimal256`, `Float64` fallback, and special values), JSONB interior NUL-byte guards, `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` epoch math, dictionary UTF-8, standalone offline TOAST relation page reader, and parallel 8 KiB heap page decoding.
- [`tests/mvcc_visibility_test.rs`](../tests/mvcc_visibility_test.rs): `MvccSnapshot` visibility flags, Visibility Map fast-path, mixed-visibility multi-page decoding, multixact locking vs updating, 32-bit transaction wraparound safe arithmetic (`xid_precedes`), and Arrow visibility filtering.
- [`tests/bgworker_ipc_test.rs`](../tests/bgworker_ipc_test.rs): `SharedMemoryArena` ring lifecycle, cooperative query cancellation via `CMD_CANCEL_REQUEST` and slot cancel flags, `Arrow IPC` + `MinimalTuple` framing (including `Time64(Microsecond)`, `FixedSizeBinary(16)`, `Utf8View`, `BinaryView`, and `Dictionary<Int64, Utf8>`), `client.explain_sql`, `client.set_table_dirty`, chunked large `ExecuteSql` / `LoadBatch` streaming, and multi-threaded `BoltBgWorker` / `BgWorkerClient` dispatch.
- [`tests/planner_custom_scan_test.rs`](../tests/planner_custom_scan_test.rs): SQL AST offload safety rejection (parameterized queries, volatiles, locking, modifying CTEs), `rtable` relation verification against views/partition roots, sequential CTE resolution, 3-part `schema.table.col` rewrite + `ORDER BY` PostgreSQL null-ordering normalization (`DESC NULLS FIRST`), unprojected `ORDER BY` `resjunk` column filtering, and `CustomScan` OOM fallback state machine.
- [`tests/runtime_and_vram_test.rs`](../tests/runtime_and_vram_test.rs): Identifier quoting (`quote_qualified_identifier`), `public.sales` default alias handling, transactional savepoint rollback frames (`begin_subtransaction`, `abort_subtransaction`), LRU VRAM eviction & oversized-batch preservation, `api::*` control plane, `api::explain_sql` parity across `use_bgworker` `false`/`true`, and live CUDA PTX JIT execution on RTX 2060.
