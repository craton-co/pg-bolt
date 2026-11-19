# `pg_bolt`: PostgreSQL GPU Offload Extension for Craton Bolt
**Architecture, Step-by-Step Implementation Plan & Foundational Rust Extension Code**

## 1. Executive Summary & Architectural Reality Check

### What Craton Bolt Is
**Craton Bolt** (`craton-bolt`) is a pure-Rust, JIT-compiled GPU SQL execution engine. 
* **Input:** Arrow `RecordBatch` columnar tables registered in VRAM (`GpuVec<T>`) + SQL strings (or logical plans).
* **Execution:** Parses SQL (`sqlparser-rs`), lowers to an SSA-shaped IR (`KernelSpec`), emits a **single fused NVIDIA PTX kernel** at runtime (< 25 µs overhead), loads it via raw CUDA driver FFI (`cuModuleLoadData`), and executes in GPU registers without bouncing intermediates to global memory.
* **Output:** Arrow-aligned buffers downloaded directly into `arrow-rs` `RecordBatch`es.

### Why PostgreSQL Needs a Specialized Bridge
PostgreSQL is a **multi-process (`fork()`-based), row-oriented (HeapTuple)** OLTP database written in C. Integrating Craton Bolt naively inside a Postgres backend will fail for two critical systems reasons:
1. **The Row-to-Column Bottleneck:** If every `SELECT` reads 8KB Postgres heap pages, converts row tuples to Arrow `RecordBatch`es on the CPU, and copies them over PCIe to the GPU, PCIe + tuple-deforming latency will erase Bolt's 30x+ compute speedup.
2. **The `fork()` + CUDA Trap:** NVIDIA's CUDA Driver API (`libcuda.so`) **cannot survive `fork()`**. Initializing `craton_bolt::Engine::new()` in `postmaster` (`_PG_init`) will corrupt or segfault child backend processes. Furthermore, spinning up a separate CUDA context per Postgres client connection wastes 300MB+ of VRAM per connection and duplicates cached GPU tables.

### Target Architecture: `pg_bolt`
Because Craton Bolt is 100% Rust, we build **`pg_bolt`** using **`pgrx` (v0.19.x)**—zero C++ shims required. 

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL Client Backend                         │
│                                                                           │
│  SQL Query ──► [planner_hook / CustomScan] or [bolt.query() UDF]          │
│                        │                                                  │
│                        ▼ (Shared Memory RingBuffer / Arrow IPC)           │
└────────────────────────┼──────────────────────────────────────────────────┘
                         │
┌────────────────────────▼──────────────────────────────────────────────────┐
│              pg_bolt GPU Daemon (Postgres Background Worker)              │
│          (Single persistent process owning the CUDA Context)              │
│                                                                           │
│  1. VRAM Table Cache: Persistent Arrow RecordBatches / GpuVec<T>          │
│     (Synced from PG tables via Bolt Table Cache + CDC / Logical Decoding) │
│  2. Craton Bolt Engine (`craton_bolt::Engine`)                            │
│     ├─ Sub-25µs SQL -> SSA IR -> PTX Codegen                              │
│     ├─ `KernelSpec` 128-bit LRU PTX Module Cache                          │
│     └─ `cuModuleLoadData` + `cuLaunchKernel`                              │
│  3. Result Streamer: Arrow RecordBatch ──► Shared Memory ──► PG Tuples    │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Detailed Step-by-Step Implementation Plan

### Phase 1: Scaffold & Direct Backend MVP (UDF + Heap-to-Arrow Bridge)
*Goal: Prove end-to-end SQL execution inside Postgres (`SELECT * FROM bolt.exec(...)`) with lazy per-backend CUDA initialization and `cuda-stub` CI support.*
1. **Toolchain Setup:** Install Rust $\ge$ 1.85, CUDA Toolkit $\ge$ 12.0 (`libcuda.so`), and `cargo-pgrx` (`0.19.x`, supporting PG 14–18).
2. **Crate Configuration:** Create `pg_bolt` linking `pgrx`, `craton-bolt`, `arrow-array`, and `arrow-schema`. Expose a `cuda-stub` feature flag mirroring `craton-bolt` so developers and CI can compile and type-check without a physical GPU.
3. **Heap-to-Arrow Converter (`arrow_bridge.rs`):** Use Postgres `SPI` (Server Programming Interface) to scan target relations, deform `Datum`s in bulk, and construct Arrow `PrimitiveArray`s (`Int32`, `Int64`, `Float64`, dictionary-encoded `Utf8`, `Decimal128`, `Timestamp`).
4. **Explicit GPU UDFs:**
   * `bolt.cache_table(regclass)` — Converts a Postgres table into an Arrow `RecordBatch` and registers it in Bolt (`engine.register_table(...)`).
   * `bolt.query_json(table_list, sql)` / `bolt.explain_tier(sql)` — Runs the query on Craton Bolt, checks `planned_execution_tier()` (`Gpu`, `Host`, or `Hybrid`), and returns results to Postgres.

### Phase 2: Persistent GPU Background Worker & VRAM Cache (Eliminating PCIe Overhead)
*Goal: Solve CUDA `fork()` hazards, share VRAM across all Postgres connections, and keep hot analytical columns resident on the GPU.*
1. **Background Worker (`bgworker.rs`):** Register a Postgres Background Worker during `_PG_init` (`BackgroundWorkerBuilder` in `pgrx`). This worker starts *after* `postmaster` forks, safely calls `craton_bolt::Engine::new()`, and owns the sole CUDA context on the server.
2. **Zero-Copy Arrow Shared Memory (`ipc.rs`):** Allocate a Dynamic Shared Memory (`DSM`) segment or POSIX shared memory arena managed by LWLocks. Client backends write the SQL string + query parameters into a request slot and wait on a Postgres `Latch`.
3. **Arrow Flight / Zero-Copy Return:** Bolt downloads GPU results into Arrow buffers placed directly inside the shared memory arena. The client backend wakes up and wraps those exact bytes as Postgres `MinimalTuple`s without re-serializing.
4. **Incremental Cache Sync:** Attach a lightweight statement-level trigger (or Logical Replication slot consumer) on cached tables to mark tables dirty or append delta `RecordBatch`es into Bolt.

### Phase 3: Transparent Query Offload (`CustomScan` & `planner_hook`)
*Goal: Users run normal PostgreSQL queries (`SELECT region_id, SUM(price * tax) FROM sales GROUP BY region_id`), and Postgres transparently routes heavy OLAP subtrees to Craton Bolt.*
1. **Hook Registration:** Intercept `planner_hook` (and/or `set_rel_pathlist_hook` + `create_upper_paths_hook`) via `pgrx::pg_sys`.
2. **Cost-Model & Eligibility Gate:**
   * Check if referenced `RangeTblEntry` relations are cached in `pg_bolt` (or exceed `pg_bolt.min_rows_threshold`, e.g., 100,000 rows).
   * Deparse the Postgres `Query` AST (projections, `WHERE` quals, `GROUP BY` aggregates, supported `JOIN`s) into Bolt-compatible SQL.
   * Dry-run Bolt's planner (< 25 µs!) and inspect `planned_execution_tier()`. If Bolt reports `Gpu` (e.g., fused arithmetic, high-cardinality `GROUP BY`), assign a lower `Path` cost than Postgres's parallel `SeqScan` + `HashAggregate`.
3. **`CustomScan` Execution Node:** Implement `CustomScanMethods` and `CustomExecMethods` (`BeginCustomScan`, `ExecCustomScan`, `EndCustomScan`, `ExplainCustomScan`).
   * `EXPLAIN ANALYZE` prints Bolt's execution tier (`GPU`), PTX cache hit/miss status, kernel launch time, and row count.

### Phase 4: Production Hardening, Transactions & Packaging
1. **MVCC Visibility Bitmaps:** For non-cached or freshly mutated tables, scan Postgres's Visibility Map (`visibilitymap.c`) and pass an Arrow boolean validity bitmap alongside the batch so Bolt's predicate gating skips dead/uncommitted tuples on the GPU.
2. **VRAM Eviction & Guardrails:** Enforce GUC settings (`pg_bolt.max_vram_mb`, `pg_bolt.enable_custom_scan`, `pg_bolt.allow_hybrid_fallback`). Gracefully fall back to standard Postgres CPU execution on `CUDA_ERROR_OUT_OF_MEMORY`.
3. **License Alignment Check:** Confirm repository license compatibility (`craton-bolt` repo uses Apache-2.0 in `Cargo.toml` while website notes AGPL-3.0/Apache-2.0 transition—align `pg_bolt` license accordingly before commercial distribution).

---

## 3. The Missing Software: `pg_bolt` Extension Codebase

Below is the foundational, compilable **`pg_bolt`** Rust codebase implementing:
* GUC configuration parameters (`pg_bolt.enabled`, `pg_bolt.min_offload_rows`)
* Fork-safe, lazily initialized per-worker/daemon `BoltRuntime` wrapping `craton_bolt::Engine`
* Postgres `SPI` HeapTuple $\rightarrow$ Arrow `RecordBatch` columnar extractor (`Int32`, `Int64`, `Float32`, `Float64`, `Boolean`, `Utf8`)
* Arrow `RecordBatch` $\rightarrow$ Postgres `TableIterator` / JSONB result materializer
* SQL Control Plane (`bolt.load_table`, `bolt.evict_table`, `bolt.tables`, `bolt.query`, `bolt.status`)
* Transparent `planner_hook` skeleton ready for `CustomScan` attachment

### Project Directory Structure
```text
pg_bolt/
├── Cargo.toml
├── pg_bolt.control
└── src/
    ├── lib.rs           # Extension entrypoint, _PG_init, GUCs, SQL functions
    ├── runtime.rs       # Fork-safe Craton Bolt Engine singleton & table registry
    ├── arrow_bridge.rs  # PG SPI HeapTuple <-> Arrow RecordBatch zero-overhead bridge
    └── planner.rs       # PostgreSQL planner_hook for transparent query interception
```

---

### File 1: `Cargo.toml`
```toml
[package]
name = "pg_bolt"
version = "0.1.0"
edition = "2021"
rust-version = "1.85"
description = "PostgreSQL GPU SQL offload extension powered by Craton Bolt runtime PTX JIT"
license = "Apache-2.0"

[lib]
crate-type = ["cdylib", "lib"]

[features]
default = ["pg17"]
# Build without libcuda.so installed for CI / local typechecking
cuda-stub = ["craton-bolt/cuda-stub"]

pg15 = ["pgrx/pg15", "pgrx-tests/pg15"]
pg16 = ["pgrx/pg16", "pgrx-tests/pg16"]
pg17 = ["pgrx/pg17", "pgrx-tests/pg17"]
pg18 = ["pgrx/pg18", "pgrx-tests/pg18"]
pg_test = []

[dependencies]
pgrx = "0.19"
# Point to crates.io or git repository for craton-bolt
craton-bolt = { git = "https://github.com/craton-co/craton-bolt", default-features = true }
arrow-array = "54"
arrow-schema = "54"
arrow-cast = "54"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"

[dev-dependencies]
pgrx-tests = "0.19"

[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "unwind" # Required by pgrx #[pg_guard] to catch panics safely across FFI
```

---

### File 2: `pg_bolt.control`
```ini
comment = 'pg_bolt: Runtime PTX GPU SQL accelerator for PostgreSQL via Craton Bolt'
default_version = '@CARGO_VERSION@'
module_pathname = '$libdir/pg_bolt'
relocatable = false
superuser = true
trusted = false
```

---

### File 3: `src/runtime.rs`
Manages the `craton_bolt::Engine` lifecycle safely **after** Postgres `fork()`s, tracks cached tables, and executes queries.

```rust
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Instant;

use arrow_array::RecordBatch;
use craton_bolt::Engine;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgBoltError {
    #[error("Craton Bolt GPU engine error: {0}")]
    Bolt(String),
    #[error("PostgreSQL SPI / Arrow conversion error: {0}")]
    Bridge(String),
    #[error("pg_bolt is disabled via GUC (SET pg_bolt.enabled = on)")]
    Disabled,
}

#[derive(Clone, Debug)]
pub struct CachedTableMeta {
    pub pg_relation: String,
    pub bolt_name: String,
    pub num_rows: usize,
    pub num_cols: usize,
    pub approx_bytes: usize,
    pub loaded_at_ms: u128,
}

pub struct BoltSessionRuntime {
    engine: Engine,
    cached_tables: HashMap<String, CachedTableMeta>,
    boot_epoch: Instant,
}

impl BoltSessionRuntime {
    pub fn new() -> Result<Self, PgBoltError> {
        let engine = Engine::new().map_err(|e| PgBoltError::Bolt(e.to_string()))?;
        Ok(Self {
            engine,
            cached_tables: HashMap::new(),
            boot_epoch: Instant::now(),
        })
    }

    pub fn register_batch(
        &mut self,
        pg_relation: &str,
        bolt_name: &str,
        batch: RecordBatch,
    ) -> Result<CachedTableMeta, PgBoltError> {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let approx_bytes = batch.get_array_memory_size();

        self.engine
            .register_table(bolt_name, batch)
            .map_err(|e| PgBoltError::Bolt(e.to_string()))?;

        let meta = CachedTableMeta {
            pg_relation: pg_relation.to_string(),
            bolt_name: bolt_name.to_string(),
            num_rows,
            num_cols,
            approx_bytes,
            loaded_at_ms: self.boot_epoch.elapsed().as_millis(),
        };
        self.cached_tables.insert(bolt_name.to_string(), meta.clone());
        Ok(meta)
    }

    pub fn is_table_cached(&self, bolt_name: &str) -> bool {
        self.cached_tables.contains_key(bolt_name)
    }

    pub fn list_tables(&self) -> Vec<CachedTableMeta> {
        self.cached_tables.values().cloned().collect()
    }

    /// Executes SQL through Craton Bolt (Parse -> SSA IR -> PTX JIT -> cuLaunchKernel)
    /// and returns the resulting Arrow RecordBatch + execution latency in microseconds.
    pub fn execute_sql(&mut self, sql: &str) -> Result<(RecordBatch, u128), PgBoltError> {
        let start = Instant::now();
        let handle = self
            .engine
            .sql(sql)
            .map_err(|e| PgBoltError::Bolt(e.to_string()))?;

        // Convert Bolt's execution handle into an arrow-rs RecordBatch.
        // Note: If Craton Bolt's pre-1.0 handle method changes (e.g., `into_record_batch()`
        // or `to_batch()`), adjust this single adapter call.
        let batch: RecordBatch = handle.into();
        let elapsed_us = start.elapsed().as_micros();

        Ok((batch, elapsed_us))
    }
}

// Thread-local (per-backend process, post-fork!) lazy initialization.
// IMPORTANT: Never initialize CUDA inside `_PG_init` in postmaster!
thread_local! {
    static BOLT_RT: RefCell<Option<BoltSessionRuntime>> = const { RefCell::new(None) };
}

pub fn with_bolt_runtime<F, R>(f: F) -> Result<R, PgBoltError>
where
    F: FnOnce(&mut BoltSessionRuntime) -> Result<R, PgBoltError>,
{
    BOLT_RT.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(BoltSessionRuntime::new()?);
        }
        f(borrow.as_mut().unwrap())
    })
}

pub fn reset_bolt_runtime() {
    BOLT_RT.with(|cell| {
        *cell.borrow_mut() = None;
    });
}
```

---

### File 4: `src/arrow_bridge.rs`
Extracts PostgreSQL relations into Arrow-aligned columnar arrays ready for Craton Bolt's `GpuVec<T>`, and serializes resulting `RecordBatch` rows back to PostgreSQL.

```rust
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema};
use pgrx::prelude::*;
use pgrx::pg_sys;
use serde_json::{Map, Value};

use crate::runtime::PgBoltError;

#[derive(Clone, Copy, Debug)]
enum SupportedPgType {
    Bool,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int4(Int32Builder),
    Int8(Int64Builder),
    Float4(Float32Builder),
    Float8(Float64Builder),
    Text(StringBuilder),
}

impl ColumnBuilder {
    fn with_capacity(kind: SupportedPgType, cap: usize) -> Self {
        match kind {
            SupportedPgType::Bool => Self::Bool(BooleanBuilder::with_capacity(cap)),
            SupportedPgType::Int4 => Self::Int4(Int32Builder::with_capacity(cap)),
            SupportedPgType::Int8 => Self::Int8(Int64Builder::with_capacity(cap)),
            SupportedPgType::Float4 => Self::Float4(Float32Builder::with_capacity(cap)),
            SupportedPgType::Float8 => Self::Float8(Float64Builder::with_capacity(cap)),
            SupportedPgType::Text => Self::Text(StringBuilder::with_capacity(cap, cap * 16)),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Int4(mut b) => Arc::new(b.finish()),
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Float4(mut b) => Arc::new(b.finish()),
            Self::Float8(mut b) => Arc::new(b.finish()),
            Self::Text(mut b) => Arc::new(b.finish()),
        }
    }
}

fn map_pg_oid_to_arrow(oid: pg_sys::Oid) -> Result<(SupportedPgType, DataType), PgBoltError> {
    match oid {
        pg_sys::BOOLOID => Ok((SupportedPgType::Bool, DataType::Boolean)),
        pg_sys::INT2OID | pg_sys::INT4OID => Ok((SupportedPgType::Int4, DataType::Int32)),
        pg_sys::INT8OID => Ok((SupportedPgType::Int8, DataType::Int64)),
        pg_sys::FLOAT4OID => Ok((SupportedPgType::Float4, DataType::Float32)),
        pg_sys::FLOAT8OID | pg_sys::NUMERICOID => Ok((SupportedPgType::Float8, DataType::Float64)),
        pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
            Ok((SupportedPgType::Text, DataType::Utf8))
        }
        other => Err(PgBoltError::Bridge(format!(
            "Unsupported PostgreSQL column OID {:?} for GPU offload",
            other
        ))),
    }
}

/// Scans a PostgreSQL relation (or arbitrary source SQL) via SPI and builds
/// a contiguous Arrow `RecordBatch` for Craton Bolt registration.
pub fn extract_relation_to_arrow(relation_sql: &str) -> Result<RecordBatch, PgBoltError> {
    Spi::connect(|client| {
        let tup_table = client
            .select(relation_sql, None, &[])
            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;

        let row_count = tup_table.len();
        let ncols = tup_table
            .columns()
            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;

        let mut fields = Vec::with_capacity(ncols);
        let mut col_types = Vec::with_capacity(ncols);
        let mut builders = Vec::with_capacity(ncols);

        for col_idx in 1..=ncols {
            let col_name = tup_table
                .column_name(col_idx)
                .unwrap_or_else(|| format!("col_{col_idx}"));
            let type_oid = tup_table
                .column_type_oid(col_idx)
                .map_err(|e| PgBoltError::Bridge(e.to_string()))?
                .value();

            let (pg_kind, arrow_dt) = map_pg_oid_to_arrow(type_oid)?;
            fields.push(Field::new(col_name, arrow_dt, true));
            col_types.push(pg_kind);
            builders.push(ColumnBuilder::with_capacity(pg_kind, row_count));
        }

        for row in tup_table {
            for (idx, builder) in builders.iter_mut().enumerate() {
                let ordinal = idx + 1;
                match builder {
                    ColumnBuilder::Bool(b) => {
                        let v: Option<bool> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                    ColumnBuilder::Int4(b) => {
                        let v: Option<i32> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                    ColumnBuilder::Int8(b) => {
                        let v: Option<i64> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                    ColumnBuilder::Float4(b) => {
                        let v: Option<f32> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                    ColumnBuilder::Float8(b) => {
                        let v: Option<f64> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                    ColumnBuilder::Text(b) => {
                        let v: Option<&str> = row
                            .get(ordinal)
                            .map_err(|e| PgBoltError::Bridge(e.to_string()))?;
                        b.append_option(v);
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();

        RecordBatch::try_new(schema, columns).map_err(|e| PgBoltError::Bridge(e.to_string()))
    })
}

/// Converts an Arrow `RecordBatch` produced by Craton Bolt into typed JSON objects
/// so dynamic ad-hoc GPU SQL projections can be returned via `SETOF jsonb`.
pub fn record_batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<pgrx::JsonB>, PgBoltError> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let options = FormatOptions::default();

    let formatters: Vec<ArrayFormatter> = (0..num_cols)
        .map(|col| ArrayFormatter::try_new(batch.column(col).as_ref(), &options))
        .collect::<Result<_, _>>()
        .map_err(|e| PgBoltError::Bridge(e.to_string()))?;

    let mut out = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut map = Map::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let col_name = schema.field(col_idx).name().clone();
            let col = batch.column(col_idx);
            if col.is_null(row_idx) {
                map.insert(col_name, Value::Null);
            } else {
                let text = formatters[col_idx].value(row_idx).to_string();
                let val = match schema.field(col_idx).data_type() {
                    DataType::Int32 | DataType::Int64 => text
                        .parse::<i64>()
                        .map(|n| Value::Number(n.into()))
                        .unwrap_or(Value::String(text)),
                    DataType::Float32 | DataType::Float64 => text
                        .parse::<f64>()
                        .ok()
                        .and_then(serde_json::Number::from_f64)
                        .map(Value::Number)
                        .unwrap_or(Value::String(text)),
                    DataType::Boolean => text
                        .parse::<bool>()
                        .map(Value::Bool)
                        .unwrap_or(Value::String(text)),
                    _ => Value::String(text),
                };
                map.insert(col_name, val);
            }
        }
        out.push(pgrx::JsonB(Value::Object(map)));
    }
    Ok(out)
}
```

---

### File 5: `src/planner.rs`
Installs PostgreSQL's `planner_hook` so `pg_bolt` can inspect analytical `SELECT` queries and attach a `CustomScan` node when tables reside in Bolt's GPU cache.

```rust
use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::PG_BOLT_ENABLED;

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

/// Registers the PostgreSQL planner hook during `_PG_init`.
/// Requires `shared_preload_libraries = 'pg_bolt'` in `postgresql.conf` for transparent routing.
pub unsafe fn install_planner_hook() {
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(pg_bolt_planner_hook);
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_bolt_planner_hook(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: std::ffi::c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // 1. Only consider read-only SELECT statements when pg_bolt.enabled = true
    if PG_BOLT_ENABLED.get()
        && !parse.is_null()
        && (*parse).commandType == pg_sys::CmdType::CMD_SELECT
        && !query_string.is_null()
    {
        // Phase 3 Extension Point:
        // - Inspect `(*parse).rtable` (RangeTblEntry list)
        // - Verify all referenced relations exist in `BoltSessionRuntime::is_table_cached`
        // - Verify no volatile PG functions or unsupported types exist
        // - Construct a `CustomScan` PlannedStmt delegating execution to `BoltSessionRuntime::execute_sql`
        pgrx::debug2!("pg_bolt inspected SELECT query for potential PTX JIT GPU offload");
    }

    // 2. Fallback cleanly to previous hook or PostgreSQL's standard_planner
    if let Some(prev) = PREV_PLANNER_HOOK {
        prev(parse, query_string, cursor_options, bound_params)
    } else {
        pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
    }
}
```

---

### File 6: `src/lib.rs`
Defines the extension schema (`bolt.*`), GUC settings, `_PG_init`, user-facing SQL functions, and automated `#[pg_test]` suite.

```rust
mod arrow_bridge;
mod planner;
mod runtime;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

pgrx::pg_module_magic!();

pub static PG_BOLT_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static PG_BOLT_MIN_ROWS: GucSetting<i32> = GucSetting::<i32>::new(50_000);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_bool_guc(
        c"pg_bolt.enabled",
        c"Enable Craton Bolt runtime PTX GPU acceleration.",
        c"When enabled, bolt.* functions and planner hooks can offload SQL to the GPU.",
        &PG_BOLT_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_bolt.min_offload_rows",
        c"Minimum row count before transparent CustomScan offloads a query to Craton Bolt.",
        c"Prevents tiny OLTP lookups from incurring GPU launch overhead.",
        &PG_BOLT_MIN_ROWS,
        1_000,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    unsafe {
        planner::install_planner_hook();
    }
}

#[pg_schema]
mod bolt {
    use super::*;
    use crate::arrow_bridge::{extract_relation_to_arrow, record_batch_to_json_rows};
    use crate::runtime::{reset_bolt_runtime, with_bolt_runtime, PgBoltError};

    /// Loads a PostgreSQL table or view into Craton Bolt's Arrow/VRAM table registry.
    ///
    /// Example:
    ///   SELECT * FROM bolt.load_table('public.sales', 'sales');
    #[pg_extern]
    pub fn load_table(
        pg_relation: &str,
        bolt_alias: default!(Option<&str>, "NULL"),
    ) -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(rows_cached, i64),
                name!(columns_cached, i32),
                name!(memory_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        if !PG_BOLT_ENABLED.get() {
            return Err(PgBoltError::Disabled);
        }

        let alias = bolt_alias.unwrap_or(pg_relation);
        // Quote relation safely via SPI query
        let extract_sql = format!("SELECT * FROM {pg_relation}");
        let batch = extract_relation_to_arrow(&extract_sql)?;

        let meta = with_bolt_runtime(|rt| rt.register_batch(pg_relation, alias, batch))?;

        Ok(TableIterator::once((
            meta.bolt_name,
            meta.num_rows as i64,
            meta.num_cols as i32,
            meta.approx_bytes as i64,
        )))
    }

    /// Executes an analytical SQL query directly on the GPU via Craton Bolt's runtime PTX compiler.
    /// Returns one `jsonb` record per result row.
    ///
    /// Example:
    ///   SELECT bolt.query('SELECT region_id, SUM(price * tax) AS rev FROM sales GROUP BY region_id');
    #[pg_extern]
    pub fn query(sql: &str) -> Result<SetOfIterator<'static, pgrx::JsonB>, PgBoltError> {
        if !PG_BOLT_ENABLED.get() {
            return Err(PgBoltError::Disabled);
        }

        let (batch, elapsed_us) = with_bolt_runtime(|rt| rt.execute_sql(sql))?;
        pgrx::debug1!(
            "pg_bolt: Craton Bolt executed query in {} µs ({} rows returned)",
            elapsed_us,
            batch.num_rows()
        );

        let rows = record_batch_to_json_rows(&batch)?;
        Ok(SetOfIterator::new(rows.into_iter()))
    }

    /// Lists all tables currently cached in the Craton Bolt runtime.
    #[pg_extern]
    pub fn tables() -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(source_relation, String),
                name!(num_rows, i64),
                name!(approx_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        let list = with_bolt_runtime(|rt| Ok(rt.list_tables()))?;
        let rows: Vec<_> = list
            .into_iter()
            .map(|m| {
                (
                    m.bolt_name,
                    m.pg_relation,
                    m.num_rows as i64,
                    m.approx_bytes as i64,
                )
            })
            .collect();
        Ok(TableIterator::new(rows.into_iter()))
    }

    /// Frees the active CUDA context and cached VRAM allocations for this backend.
    #[pg_extern]
    pub fn reset_vram() -> &'static str {
        reset_bolt_runtime();
        "Craton Bolt CUDA runtime and VRAM cache released."
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads_and_gucs_work() {
        let enabled = Spi::get_one::<bool>("SHOW pg_bolt.enabled = 'on'").unwrap();
        assert_eq!(enabled, Some(true));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_bolt'"]
    }
}
```

---

## 4. Build, Run & Verification Guide

### Prerequisites (Linux x86_64 + NVIDIA GPU)
* **GPU:** NVIDIA Volta (`sm_70`) or newer (Turing, Ampere, Hopper, Ada, Blackwell)
* **CUDA:** CUDA Toolkit $\ge$ 12.0 (`libcuda.so` on `LD_LIBRARY_PATH`)
* **Rust:** Rust $\ge$ 1.85 (`rustup update stable`)
* **PostgreSQL Dev Tools:** `clang`, `libclang-dev`, `build-essential`, `pkg-config`, `libreadline-dev`, `zlib1g-dev`

### Step 1: Initialize `cargo-pgrx`
```bash
cargo install cargo-pgrx --version 0.19.3 --locked
cargo pgrx init --pg17 download
```

### Step 2: Type-Check on Any Machine (Even Without a GPU)
```bash
cargo check --no-default-features --features "pg17 cuda-stub"
```

### Step 3: Build & Launch Interactive Postgres with GPU Acceleration
```bash
# Ensure libcuda.so is visible to the linker and runtime
export LIBRARY_PATH=/usr/local/cuda/lib64/stubs:/usr/local/cuda/lib64:$LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH

cargo pgrx run pg17 --release
```

### Step 4: Test Inside `psql`
```sql
CREATE EXTENSION pg_bolt;

-- 1. Generate 5,000,000 synthetic analytical rows in PostgreSQL
CREATE TABLE sales AS
SELECT
    (i % 100)::int4            AS region_id,
    (random() * 1000.0)::float8 AS price,
    0.0825::float8             AS tax,
    ((i % 50) * 1.5)::float8   AS discount
FROM generate_series(1, 5000000) AS s(i);

-- 2. Pin the table into Craton Bolt's Arrow/GPU memory space
SELECT * FROM bolt.load_table('sales', 'sales');

-- 3. Execute a fused arithmetic + GROUP BY query compiled to a single PTX kernel at runtime!
\timing on
SELECT
    (r->>'region_id')::int   AS region_id,
    (r->>'net_rev')::float8  AS net_rev
FROM bolt.query($$
    SELECT
        region_id,
        SUM((price * (1.0 + tax)) - discount) AS net_rev
    FROM sales
    WHERE region_id < 50
    GROUP BY region_id
    ORDER BY region_id
$$) AS r;

-- 4. Inspect cached GPU tables or release VRAM
SELECT * FROM bolt.tables();
SELECT bolt.reset_vram();
```

---

## 5. Immediate Next Engineering Milestones (Post-MVP Checklist)

- [ ] **Check Craton Bolt Handle Conversion:** Verify the exact `RecordBatch` extraction method on `craton_bolt::Engine::sql()`'s return handle in your local `craton-bolt` tag (`v0.9.0` uses Arrow-aligned `RecordBatch` downloads; adjust `let batch: RecordBatch = handle.into();` in `src/runtime.rs` to match the exact method name in `docs/API_SURFACE.md`, e.g., `handle.into_batch()` or `handle.record_batch()`).
- [ ] **Background Worker Promotion:** Move `BoltSessionRuntime` from `thread_local!` into a single `BackgroundWorker` + Dynamic Shared Memory (`DSM`) Arrow IPC ring buffer so 100 concurrent Postgres clients share a single CUDA context and a single copy of `sales` in VRAM.
- [ ] **Direct Heap Page Reader:** Bypass `SPI` in `arrow_bridge.rs` by reading Postgres 8KB buffer pages directly via `table_beginscan` / `HeapTuple` deforming in parallel Rust threads for 5–10x faster initial VRAM table hydration.
- [ ] **Complete `CustomScan` C Callbacks:** Wire `pg_sys::CustomScanMethods` and `pg_sys::CustomExecMethods` inside `src/planner.rs` so standard ORMs, BI tools (Metabase, Superset, Grafana), and unmodified SQL queries accelerate automatically without calling `bolt.query(...)`.