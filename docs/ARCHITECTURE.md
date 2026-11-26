<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture

`pg_bolt` bridges PostgreSQL's backend C/SQL runtime (`pgrx` 0.19) to [`craton-bolt`](https://github.com/craton-co/bolt) (`0.12.0`), an Apache Arrow-native analytical query engine with runtime NVRTC PTX JIT compilation.

## Module Overview

```text
  PostgreSQL Client / SQL
     │
     ├── Explicit SQL (`bolt.*` in `src/lib.rs`)
     │      └── load_table / cache_table / sync_table / evict_table /
     │          tables / query / query_json / explain_tier / status / reset_vram
     │
     └── Transparent Planner Hook (`src/planner.rs` -> `src/custom_scan.rs`)
            │   inspect_sql_for_offload (sqlparser 0.52)
            │   analyze_sql_eligibility + cost model
            ▼
       CustomScan Plan / Execution (`BoltCustomScanState`)
            │
            ├── Direct Backend (`BoltSessionRuntime` in `src/runtime.rs`)
            └── IPC Ring (`SharedMemoryArena` -> `BoltBgWorker` in `src/ipc.rs`, `src/bgworker.rs`)
                   │
                   ▼
             craton-bolt `Engine` (PTX JIT / Hybrid / Host)
                   ▲
                   │ Arrow RecordBatch snapshots + MVCC delta visibility
             `src/arrow_bridge.rs` & `src/mvcc.rs`
             (SPI extraction & 8 KiB heap page reader)
```

| Module | Responsibility |
| :--- | :--- |
| `src/lib.rs` | `_PG_init`, `pg_hooks` (DML/Xact/SubXact/Relcache dirty tracking), `#[pg_schema] mod bolt` SQL wrappers, pure-Rust `api` control plane with `use_bgworker` parity |
| `src/guc.rs` | `BoltConfig` and 9 `pg_bolt.*` PostgreSQL GUCs |
| `src/error.rs` | `PgBoltError` enum and `BoltError` OOM/error mapping |
| `src/arrow_bridge.rs` | PG OID <-> Arrow type mapping (`BYTEA`/`TIME`/`UUID`), `NUMERIC` `atttypmod`, dictionary encoding, chunked SPI extraction, 8 KiB heap page decoder (inline `PGLZ`, backend TOAST detoasting, `LP_REDIRECT`, `attisdropped`, `attmissingval`), `TupleTableSlot` & JSON conversion |
| `src/mvcc.rs` | PostgreSQL `MvccSnapshot` visibility predicate (`t_xmin`/`t_xmax`/hints, `pg_xact` wraparound, `pg_multixact`, `suboverflowed`) and Visibility Map filtering |
| `src/runtime.rs` | `BoltSessionRuntime`, `StandardTableProvider` (`SqlSemantics::Standard`), fork safety (`owner_pid`), LRU VRAM budgeting, identifier quoting, logical/physical `explain_sql` |
| `src/planner.rs` | AST offload visitor, `ORDER BY` PG null-ordering & 3-part `schema.table.col` rewrite, eligibility checks, cost model, `CustomScan` attachment (`planner_hook`) |
| `src/custom_scan.rs` | `CustomScanMethods` / `CustomExecMethods` callbacks, preserved `orig_plan` CPU fallback, resettable per-tuple `MemoryContext`, and slot emission |
| `src/ipc.rs` | Ring buffer (`SharedMemoryArena`), chunked request & response streaming, `pg_shmem` (`ShmemInitStruct`/`LWLock`/`Latch`) control structures, Arrow IPC streams, `MinimalTuple` framing |
| `src/bgworker.rs` | CUDA background worker command loop (`BoltBgWorker`) and `BgWorkerClient` (`ExecuteSql`, `LoadBatch*`, `ExplainSql`, `SetTableDirty`, etc.) |

## Fork Safety & Lazy CUDA Initialization

PostgreSQL initializes shared extensions inside the postmaster process (`_PG_init`) and subsequently `fork()`s worker and client backend processes. CUDA driver state (`CUcontext`) is not fork-safe across `fork()`:

- `_PG_init` registers GUCs, `ipc::init_shmem_hooks()`, conditional `bgworker::register_bgworker()` (when preloaded via `shared_preload_libraries` with `pg_bolt.use_bgworker = on`), `planner::install_planner_hook()`, and `pg_hooks::install()` without initializing CUDA contexts inside the postmaster.
- `BoltSessionRuntime` captures `owner_pid = std::process::id()` on construction inside the active backend or background worker. Every operation calls `check_fork_safety()`, returning `PgBoltError::ForkHazard { expected_pid, current_pid }` if invoked after an unexpected `fork()`.
- Calling `bolt.reset_vram()` shuts down any in-process shared background worker handle, drops `BoltSessionRuntime`, clears planner/hook trackers, and releases cached tables and CUDA allocations.

## Error Model

All modules return `PgBoltResult<T>` (`Result<T, PgBoltError>`). Upstream `craton_bolt::BoltError::Memory`, `BoltError::GpuCapacity`, and `BoltError::CudaWithCode { code: 2, .. }` map specifically to `PgBoltError::VramExhausted` so `CustomScan` can distinguish GPU OOM conditions from non-OOM failures.

## Current Scope Limitations

Core Wave 2/3 integrations (chunked IPC request + `ExecuteSql` response streaming, `SetTableDirty`/`ExplainSql` IPC parity, `pg_hooks` transaction/executor invalidation, inline `PGLZ` `4B_C` + `attisdropped`/`attmissingval`/`LP_REDIRECT` page support, `CustomScan` `orig_plan` fallback + per-tuple `MemoryContext` isolation, and `BYTEA`/`TIME`/`UUID` + view/`Dictionary<Int64, Utf8>` `MinimalTuple` framing) are implemented; remaining items tracked in [`docs/internal/issues/`](internal/issues/) include:

- **Cross-process DSM payload ring (`W1-C-004`, `W2-C-002`)**: `pg_shmem` control structures and `_PG_init` hooks exist, while `SharedMemoryArena` payload transport still uses process-local buffers until `dsm_create`/`dsm_attach` cross-process attachment is wired.
- **Cross-backend `PLANNER_CATALOG` & worker timeout/SIGHUP parity (`W1-D-005`, `W2-C-001`)**: `PLANNER_CATALOG` remains process-local across separate OS backends, and `pgrx` BGWorker per-request timeout / `SIGHUP` runtime reconfiguration await `execute_sql_with_timeout` / `reconfigure`.
- **Offline LZ4 & standalone `pg_toast` pages (`W2-B-003`)**: Pure-Rust inline `PGLZ` (`4B_C` method 0) works offline and `PgBackendToastDetoaster` resolves `1B_E` pointers under PostgreSQL, while offline inline `LZ4` (`4B_C` method 1) and standalone `pg_toast_<oid>` page readers remain proposed.
- **Remaining `CustomScan`, `arrow_cell_to_pg_datum` & SQL wrapper gaps (`W2-E-001`, `W3-E-001..004`)**: Missing `bolt.explain_sql` SQL wrapper, `UInt64`/`Int8`/`Dictionary<Int64, Utf8>` support in `arrow_bridge::arrow_cell_to_pg_datum`, `slot_getallattrs` in `copy_non_resjunk_slot_attrs`, and `INDEX_VAR` projection rewriting for `CustomScan` targetlists.
