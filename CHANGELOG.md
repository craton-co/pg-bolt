<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to `pg_bolt` are documented in this file.

## [0.2.0] - 2026-11-27

### Added
- **Local Docker CI script**: [`scripts/ci-local.ps1`](scripts/ci-local.ps1), [`scripts/ci-local.sh`](scripts/ci-local.sh), and root [`run-ci.sh`](run-ci.sh) / [`run-ci.ps1`](run-ci.ps1) reproducing all `.github/workflows/ci.yml` jobs locally in isolated Linux containers.
- **Multi-column GROUP BY with pre-aggregation**: Bolt GPU offload support for multi-column grouped aggregations.
- **SQL install & migration scripts**: Updated to `sql/pg_bolt--0.2.0.sql` and `sql/pg_bolt--0.1.0--0.2.0.sql`.

## [0.1.1] - 2026-11-13 (Full Pre-Release Validation)

### Added
- **pgrx 0.19 PostgreSQL extension crate** targeting PostgreSQL `15`, `16`, `17`, and `18` (`pg15`–`pg18` features) with dual `cuda` (default) and `cuda-stub` build modes.
- **`bolt` SQL API** ([`sql/pg_bolt--0.1.1.sql`](sql/pg_bolt--0.1.1.sql)): `load_table`, `cache_table`, `sync_table`, `sync_delta`, `evict_table`, `tables`, `query`, `query_json`, `explain_tier`, `explain_sql`, `status`, and `reset_vram`.
- **10 PostgreSQL GUCs**: `pg_bolt.enabled`, `pg_bolt.min_offload_rows`, `pg_bolt.max_vram_mb`, `pg_bolt.enable_custom_scan`, `pg_bolt.allow_hybrid_fallback`, `pg_bolt.statement_timeout_ms`, `pg_bolt.use_bgworker`, `pg_bolt.ipc_ring_slots`, `pg_bolt.ipc_slot_bytes`, and `pg_bolt.numeric_unconstrained_fallback`.
- **PostgreSQL-to-Arrow & 8 KiB heap decoder**:
  - Supports `BOOL`, `INT2` (widened to `Int32`), `INT4`, `INT8`, `FLOAT4`, `FLOAT8`, `NUMERIC` (dynamic precision/scale up to 76 digits, `Decimal256`, and configurable `Float64` fallback), `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `TEXT`/`VARCHAR`/`BPCHAR` (with trailing space normalization), and extended types (`BYTEA`, `TIME`, `UUID`).
  - JSONB interior NUL-byte (`\u0000`) validation and direct `arrow_batch_to_slot_row` virtual slot populator for CustomScan.
  - Raw heap page decoding (`decode_heap_pages_to_arrow_with_specs`) with inline pure-Rust `PGLZ` (`4B_C` method 0) and pure-Rust `LZ4` (`4B_C` method 1) decompression, `PgBackendToastDetoaster` (`1B_E` external TOAST via `pg_detoast_datum_packed`), standalone offline TOAST relation page decoder (`StandaloneToastReader`), `LP_REDIRECT` HOT chain resolution, `attisdropped` alignment handling, and `attmissingval` defaults.
- **MVCC visibility engine & transactional recovery**:
  - Evaluates `t_xmin`/`t_xmax`/hints, `VisibilityMap` bitmaps, `pg_xact`/CLOG wraparound horizons with 32-bit wrap-safe `xid_precedes` arithmetic, `pg_multixact` member resolution, and `suboverflowed` / own-XID subtransactions.
  - Transactional savepoint rollback hierarchy (`SessionTxFrame`): rolls back staged cached table mutations and restores clean dirty flags on `XACT_EVENT_ABORT` and `SUBXACT_EVENT_ABORT_SUB`.
  - Automatic dirty cache query rejection and SPI delta auto-refresh via `bolt.sync_delta`.
- **Session runtime & VRAM budget manager**: Enforces PostgreSQL postmaster-vs-backend fork safety (`ForkHazard`), LRU cache eviction under `pg_bolt.max_vram_mb`, dynamic reconfigure memory tracking, `SqlSemantics::Standard` compatibility wrapping `craton-bolt` `0.12.0`, and `api::explain_sql` parity across `pg_bolt.use_bgworker` modes.
- **SQL eligibility analyzer & `CustomScan` hook**:
  - Inspects SQL ASTs (`sqlparser` 0.52), validates relation OIDs and schema namespaces against `CachedTableMeta`, rejecting views, partitioned roots, foreign tables, and parameterized `$1..$N` queries.
  - Filters `resjunk` columns for unprojected `ORDER BY` targets and installs `CustomPath` hooks.
  - Normalizes `ORDER BY` to PostgreSQL null-ordering (`ASC NULLS LAST`, `DESC NULLS FIRST`) and 3-part `schema.table.col` column qualifiers.
  - Guards `NaN` floating point comparisons and `SUM(bigint)` overflow divergence.
  - Attaches `CustomScan` nodes with `INDEX_VAR` `scan.plan.targetlist` references, preserved `orig_plan` CPU subplan fallback (`slot_getallattrs` + `datumCopy` varlena isolation and `PgBoltError::Ipc` fallback), and cooperative cancellation via `QueryCancelPending`.
- **Background worker & shared-memory ring IPC**:
  - Supports chunked request payloads (`LoadBatchChunk`, `AppendDeltaChunk`) and chunked large `ExecuteSql` result streaming across bounded `ipc_slot_bytes` rings.
  - Cooperative query cancellation via `CMD_CANCEL_REQUEST` and per-slot atomic cancellation flags.
  - Cross-process PostgreSQL `ShmemInitStruct`/`LWLock`/`Latch` ring control structures (`PgShmemRingControl`) with `PgDsmSegment` RAII payload attachments.
  - `MinimalTuple` binary framing including `Time64(Microsecond)`, `FixedSizeBinary(16)`, `Utf8View`, `BinaryView`, and `Dictionary<Int64, Utf8>`.
- **Hardware Benchmarks**: End-to-end reproducible benchmarks executed on NVIDIA GeForce RTX 2060 (11.38 million rows/sec heap page decode, 43,599 IPC ops/sec, 35.74 million rows/sec CUDA analytical query throughput).
- **Packaging & CI**: `docker/Dockerfile.pg17-gpu`, `.github/workflows/ci.yml`, `deny.toml`, and `NOTICE`/`LICENSE` dual Apache-2.0 / AGPL-3.0-only compliance documentation.
