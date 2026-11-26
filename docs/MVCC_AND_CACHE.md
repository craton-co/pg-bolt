<!-- SPDX-License-Identifier: Apache-2.0 -->

# MVCC Visibility & VRAM Cache Management

`pg_bolt` snapshots PostgreSQL tables into Apache Arrow `RecordBatch`es and maintains an explicit per-runtime VRAM cache budget with LRU eviction.

## Snapshot & MVCC Visibility Model

### 1. Logical Cache Operations (`bolt.*`) & Automatic Invalidation (`pg_hooks`)

- `bolt.load_table(pg_relation, bolt_alias)`: Extracts live rows visible to the active SQL snapshot via bounded SPI cursor chunks (transcoding server encodings to UTF-8 via `pg_server_to_any`), encodes low-cardinality strings as UTF-8 / `Dictionary<Int32, Utf8>`, converts `DATE`/`TIME`/`TIMESTAMP`/`TIMESTAMPTZ`/`NUMERIC(p, s)`/`BYTEA`/`UUID` to Apache Arrow formats, and pins the `RecordBatch`.
- `bolt.cache_table(...)`: Skips extraction if an un-dirty cached entry already exists.
- `bolt.sync_table(...)`: Refreshes the cached snapshot when PostgreSQL data changes.
- `bolt.evict_table(...)` / `bolt.reset_vram()`: Drops individual tables or shuts down shared worker state, runtime trackers, and CUDA allocations.
- `pg_hooks` (`src/lib.rs`): Installs `ExecutorEnd_hook`, `ProcessUtility_hook`, `RegisterXactCallback`, `RegisterSubXactCallback`, and `CacheRegisterRelcacheCallback` to automatically track DML/DDL mutations per relation `Oid`, restore clean state across subtransaction/transaction rollbacks, and synchronize dirty flags over IPC (`SetTableDirty`) when `pg_bolt.use_bgworker = on`.

### 2. Page-Level MVCC Visibility (`src/mvcc.rs` & `src/arrow_bridge.rs`)

For raw 8 KiB (`PG_BLCKSZ = 8192`) PostgreSQL heap pages, `pg_bolt` provides a snapshot visibility filter and parallel multi-page decoder (`decode_heap_pages_to_arrow` / `decode_heap_pages_to_arrow_with_specs`):

- `HeapTupleHeaderMeta` captures `t_xmin`, `t_xmax`, `t_infomask`, `t_infomask2`, `t_hoff`, and `natts` using PostgreSQL tuple flags (`HEAP_XMIN_COMMITTED`, `HEAP_XMIN_INVALID`, `HEAP_XMAX_COMMITTED`, `HEAP_XMAX_INVALID`, `HEAP_HASNULL`, `HEAP_HASVARWIDTH`, `HEAP_ONLY_TUPLE`).
- `MvccSnapshot` (`xmin`, `xmax`, `active_xips`, `suboverflowed`, `own_xid`, `pg_xact`, `pg_multixact`, oldest XID horizon) evaluates `is_tuple_visible(header, snapshot)`:
  - Resolves committed/aborted hints, `pg_xact`/CLOG wraparound lookups, `pg_multixact` member locks, and `suboverflowed` / own-XID visibility cleanly.
  - Accepts tuples committed before `snapshot` whose `t_xmax` is invalid, uncommitted, active in `active_xips`, or `>= snapshot.xmax`.
- `VisibilityMap` allows skipping per-tuple MVCC evaluation on pages flagged all-visible.
- `decode_heap_pages_to_arrow_with_specs` follows `LP_REDIRECT` HOT chains (with target-before-redirect deduplication), preserves physical offsets across `attisdropped` attributes, materializes PostgreSQL 11+ `attmissingval` fast defaults when `col_idx >= tuple.natts`, decompresses inline **PGLZ (`4B_C` compression method `0`)** varlenas in pure Rust, and resolves external `1B_E` TOAST pointers via `PgBackendToastDetoaster` (`pg_detoast_datum_packed`) inside PostgreSQL backends.

## VRAM Budget & LRU Eviction (`src/runtime.rs`)

When `pg_bolt.max_vram_mb > 0`:
1. `BoltSessionRuntime::register_batch` estimates the incoming batch size (`batch.get_array_memory_size()`) and checks the remaining budget before modifying existing cache state (`enforce_vram_budget`).
2. If the incoming batch alone exceeds the total budget (`max_vram_mb * 1024 * 1024`), `register_batch` immediately fails with `PgBoltError::VramExhausted` without evicting live cached tables (`W1-E-010`).
3. Otherwise, cached tables are evicted in ascending `last_accessed_seq` order (LRU) until the new table fits, updating `eviction_count`.
4. Both `execute_sql` and table loads advance `last_accessed_seq` for referenced tables.

## Tracked MVCC, Compression & Heap Proposals

Open enhancements tracked in [`docs/internal/issues/`](internal/issues/):
- **Pure-Rust `pglz` vs offline `LZ4` & standalone TOAST page reader (`W2-B-003`)**: Pure-Rust `PGLZ` (`4B_C` method `0`) decompression works offline without a backend, and `PgBackendToastDetoaster` detoasts both compressed and external `1B_E` datums under active PostgreSQL backends. Adding an optional pure-Rust `LZ4` block decompressor (`4B_C` method `1`, PostgreSQL 14+ `default_toast_compression = lz4`) and standalone `pg_toast_<oid>` page reconstruction for zero-backend offline scans remains proposed in `W2-B-003`.
- **Cross-backend `PLANNER_CATALOG` & snapshot synchronization (`W1-D-005`)**: Sharing cached relation schemas, invalidation epochs, and active MVCC snapshots across separate OS backends via PostgreSQL shared memory / DSM.
- **Unconstrained `NUMERIC` & streaming multi-batch SPI (`W2-B-001`, `W2-B-002`)**: Configurable `Decimal256`/`Float64` fallback when unconstrained `NUMERIC` (`atttypmod == -1`) exceeds `(38, 10)` and direct multi-batch SPI streaming into chunked IPC.
