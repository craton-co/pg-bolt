<!-- SPDX-License-Identifier: Apache-2.0 -->

# pg_bolt

PostgreSQL extension (`pgrx` 0.19) for GPU-accelerated analytical SQL via [`craton-bolt`](https://github.com/craton-co/bolt) (`0.12.0`).

`pg_bolt` caches PostgreSQL relations as Apache Arrow `RecordBatch` snapshots, maintains an explicit VRAM cache with LRU eviction, analyzes incoming SQL queries for GPU or Hybrid execution eligibility, and exposes both explicit `bolt.*` SQL entry points and a PostgreSQL `CustomScan` planner hook.

## Status & Support Matrix

- **Extension version**: `0.2.0` (Rust edition `2021`, MSRV `1.85`)
- **PostgreSQL targets**: PostgreSQL `15`, `16`, `17`, `18` (`pg15`–`pg18` Cargo features via `pgrx = "0.19"`)
- **CUDA requirement**: NVIDIA CUDA Toolkit `>= 12.0` (NVRTC runtime PTX JIT + driver API) for `cuda` builds; `--no-default-features --features cuda-stub` for GPU-less CI/host testing
- **Validation status**:
  - **Host Pure-Rust (`cuda-stub`)**: `cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings` (0 warnings) and `cargo test --no-default-features --features cuda-stub` pass **130 tests** (**107** unit + **23** integration across `arrow_bridge_and_heap_test` (5), `bgworker_ipc_test` (5), `mvcc_visibility_test` (3), `planner_custom_scan_test` (4), and `runtime_and_vram_test` (6); 0 failed, 0 ignored).
  - **Host Live CUDA (`RTX 2060`, CUDA `13.3`)**: `cargo clippy --all-targets -- -D warnings` (0 warnings) and `cargo test --release` pass **114 tests** (**87** unit + **27** integration including live PTX JIT kernel execution, MVCC-filtered heap page GPU queries, `BoltBgWorker` chunked streaming, and `CustomScan` direct/worker GPU execution; 0 failed, 0 ignored).
  - **Live Hardware Benchmark Results (RTX 2060)**: `cargo run --release --example rtx2060_bench` verified **889.07 MB/s** heap page decode throughput, **43,599** IPC ops/sec, and **35.74 million rows/sec** GPU scan/filter/aggregate throughput on 1,000,000 analytical rows.
  - **Issue Tracking**: All 66 issues filed across Waves 1–6 have been fixed/resolved and retired to [`docs/internal/retired/`](docs/internal/retired/). 0 open issues remain.
  - **Live PostgreSQL `pgrx` (`pg15`–`pg18`)**: Configured in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) and [`docker/Dockerfile.pg17-gpu`](docker/Dockerfile.pg17-gpu); `cargo pgrx test` against an active PostgreSQL server was **not run** on this Windows host (no local PostgreSQL / `pg_config` installation).

## Quick Start

### 1. Docker (PostgreSQL 17 + CUDA 12.6)

Build from the parent workspace containing sibling `bolt/` and `pg-bolt/` directories:

```bash
docker build -f pg-bolt/docker/Dockerfile.pg17-gpu -t ghcr.io/craton-co/pg-bolt:pg17-gpu .
docker run --rm -it --gpus all -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/craton-co/pg-bolt:pg17-gpu
```

### 2. SQL Usage
``
```sql
-- Requires superuser (default in pg_bolt.control). For background worker mode,
-- set shared_preload_libraries = 'pg_bolt' in postgresql.conf and restart.
CREATE EXTENSION pg_bolt;

-- Pin a PostgreSQL table into Bolt Arrow/VRAM storage:
SELECT * FROM bolt.load_table('public.sales', 'sales');

-- Inspect the planned Bolt execution tier ('Gpu', 'Hybrid', or 'Host') or full logical/physical plan:
SELECT bolt.explain_tier(
    'SELECT region_id, SUM((price * (1.0 + tax)) - discount) FROM sales WHERE region_id < 50 GROUP BY region_id'
);
SELECT bolt.explain_sql(
    'SELECT region_id, SUM((price * (1.0 + tax)) - discount) FROM sales WHERE region_id < 50 GROUP BY region_id'
);

-- Query directly via Bolt:
SELECT * FROM bolt.query_json(
    'SELECT region_id, COUNT(*) AS rows FROM sales GROUP BY region_id ORDER BY region_id'
);

-- Inspect cached tables and runtime telemetry:
SELECT * FROM bolt.tables();
SELECT bolt.status();
```

## SQL Entry Points (`bolt` schema)

| Function | Signature | Notes |
| :--- | :--- | :--- |
| `bolt.load_table` | `(pg_relation text, bolt_alias text DEFAULT NULL) -> TABLE(...)` | Extracts relation and replaces cached snapshot |
| `bolt.cache_table` | `(pg_relation text, bolt_alias text DEFAULT NULL) -> TABLE(...)` | Loads only when uncached or marked dirty |
| `bolt.sync_table` | `(pg_relation text, bolt_alias text DEFAULT NULL) -> TABLE(...)` | Refreshes/synchronizes cached snapshot |
| `bolt.evict_table` | `(bolt_name text) -> boolean` | Evicts a cached table from VRAM & planner catalog |
| `bolt.tables` | `() -> TABLE(bolt_table text, source_relation text, num_rows bigint, approx_bytes bigint)` | Lists currently cached tables |
| `bolt.query` | `(sql text) -> SETOF jsonb` | Streams result rows as JSON objects |
| `bolt.query_json` | `(sql text) -> jsonb` | Returns full result array as a single `jsonb` |
| `bolt.explain_tier` | `(sql text) -> text` | Plans SQL and returns `"Gpu"`, `"Hybrid"`, or `"Host"` |
| `bolt.explain_sql` | `(sql text) -> text` | Plans SQL and returns formatted logical and physical execution plan text |
| `bolt.status` | `() -> jsonb` | VRAM cache, PTX JIT cache, pool, and PID telemetry |
| `bolt.reset_vram` | `() -> text` | Releases shared background worker handle, runtime, and VRAM allocations |

See [docs/SQL_API.md](docs/SQL_API.md) and [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for full details.

## Documentation

- [Installation & Build Guide](docs/INSTALL.md)
- [Architecture & Fork Safety](docs/ARCHITECTURE.md)
- [SQL API Reference](docs/SQL_API.md)
- [GUC Configuration](docs/CONFIGURATION.md)
- [CustomScan Planner Integration](docs/CUSTOM_SCAN.md)
- [MVCC Visibility & VRAM Cache](docs/MVCC_AND_CACHE.md)
- [Benchmarks & Validation Status](docs/BENCHMARKS.md)
- [Roadmap](ROADMAP.md) · [Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

## Licensing

- **Source code in this repository**: Licensed under the **Apache License, Version 2.0** ([LICENSE](LICENSE)).
- **Compiled binaries & combined distributions**: `pg_bolt` depends on `craton-bolt` (`0.12.0`), which is licensed under **GNU Affero General Public License v3.0 only (`AGPL-3.0-only`)**. When distributing compiled `pg_bolt.so` / `pg_bolt.dll` binaries or images linking `craton-bolt`, the terms of **`AGPL-3.0-only`** govern the combined work unless a separate commercial license has been obtained from **Craton Software Company** (`https://craton.com.ar`, `opensource@craton.com.ar`). See [NOTICE](NOTICE) for full third-party notices.
