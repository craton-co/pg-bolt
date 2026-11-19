<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL API Reference

All SQL objects are installed into the `"bolt"` schema ([`sql/pg_bolt--0.1.0.sql`](../sql/pg_bolt--0.1.0.sql)). Loading the extension requires superuser privileges (`superuser = true` in [`pg_bolt.control`](../pg_bolt.control)).

## Cache & Lifecycle Functions

### `bolt.load_table(pg_relation text, bolt_alias text DEFAULT NULL)`

Extracts all visible rows from `pg_relation` into an Arrow `RecordBatch`, registers it in Bolt under `bolt_alias` (or `pg_relation` when `NULL` / omitted), and replaces any previous snapshot under that name.

**Returns**: `TABLE(bolt_table text, rows_cached bigint, columns_cached integer, memory_bytes bigint)`

```sql
SELECT * FROM bolt.load_table('public.sales', 'sales');
-- Uses default alias 'public.sales':
SELECT * FROM bolt.load_table('public.sales');
```

Notes:
- `pg_relation` is validated and double-quoted via `quote_qualified_identifier` before executing SPI `SELECT * FROM ...`.
- If adding the new table would exceed `pg_bolt.max_vram_mb`, least-recently-used (`last_accessed_seq`) cached tables are evicted first. If the single table exceeds the entire VRAM cap, `PgBoltError::VramExhausted` is raised without corrupting existing cached tables.

### `bolt.cache_table(pg_relation text, bolt_alias text DEFAULT NULL)`

Lazily ensures `pg_relation` is pinned. If an un-dirty cached entry already exists for the resolved alias, returns existing metadata without re-scanning PostgreSQL.

**Returns**: `TABLE(bolt_table text, rows_cached bigint, columns_cached integer, memory_bytes bigint)`

### `bolt.sync_table(pg_relation text, bolt_alias text DEFAULT NULL)`

Refreshes the cached snapshot for `pg_relation` / `bolt_alias` so subsequent Bolt queries see updated PostgreSQL rows.

**Returns**: `TABLE(bolt_table text, rows_cached bigint, columns_cached integer, memory_bytes bigint)`

### `bolt.evict_table(bolt_name text)` (`STRICT`)

Evicts `bolt_name` from the Bolt runtime and planner catalog.

**Returns**: `boolean` (`true` if evicted, `false` if not cached).

### `bolt.tables()` (`STRICT`)

Lists currently cached tables sorted by `bolt_table`.

**Returns**: `TABLE(bolt_table text, source_relation text, num_rows bigint, approx_bytes bigint)`

### `bolt.reset_vram()` (`STRICT`)

Shuts down the active in-process background worker handle (when present), drops `BoltSessionRuntime`, resets planner/invalidation trackers, and releases cached tables and CUDA allocations.

**Returns**: `text`

## Query & Telemetry Functions

### `bolt.query(sql text)` (`STRICT`)

Executes analytical `sql` on the active Bolt runtime (or `BoltBgWorker` when `pg_bolt.use_bgworker = on`) and streams rows as `SETOF jsonb`.

```sql
SELECT r->>'region_id', (r->>'net_total')::float8
FROM bolt.query(
    'SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total FROM sales GROUP BY region_id'
) AS r;
```

### `bolt.query_json(sql text)` (`STRICT`)

Executes `sql` on the active Bolt runtime (or `BoltBgWorker`) and returns the entire result set as a single `jsonb` array of row objects.

**Returns**: `jsonb`

### `bolt.explain_tier(sql text)` (`STRICT`)

Runs logical optimization and physical lowering against currently cached schemas without executing the query, returning `'Gpu'`, `'Hybrid'`, or `'Host'`.

**Returns**: `text`

### `bolt.explain_sql(sql text)` (`STRICT`)

Reconciles pending relation invalidations and plans `sql` against currently cached Bolt table schemas without executing the query, returning the formatted logical and physical execution plan text.

- **Syntax**: `bolt.explain_sql(sql text) -> text` (`VOLATILE STRICT`)
- **Parameters**:
  - `sql` (`text`): Non-empty analytical SQL query string. Passing `NULL` returns `NULL` (`STRICT`); empty or whitespace-only input raises `PgBoltError::Validation`; when `pg_bolt.enabled = off`, raises `PgBoltError::Disabled`.
- **Returns**: `text` — formatted plan text containing `"Logical plan:\n...\nPhysical plan:\n..."` (along with memory/storage scan notes or host-orchestrated subplan details in live CUDA builds).
- **Mode behavior (`pg_bolt.use_bgworker`)**:
  - `on` (default): Routes the `ExplainSql` command through `BgWorkerClient` over the shared-memory IPC ring to the persistent `BoltBgWorker` runtime.
  - `off`: Plans directly in the calling backend's thread-local `BoltSessionRuntime`.

```sql
SELECT bolt.explain_sql(
    'SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total FROM sales GROUP BY region_id'
);
```

### `bolt.status()` (`STRICT`)

Returns runtime diagnostics as `jsonb` matching `BoltRuntimeStatus`:
- `cuda_stub` (`bool`)
- `owner_pid` (`u32`)
- `cached_tables` (`usize`)
- `total_cached_bytes` (`usize`)
- `max_vram_bytes` (`Option<usize>`)
- `ptx_cache_hits`, `ptx_cache_misses`, `ptx_cache_evictions` (`usize`)
- `pool_stats` (`total_pooled_bytes`, `bucket_count`, `oom_recovery_count`, `proactive_eviction_count`)
- `queries_executed`, `eviction_count` (`u64`)

## Supported PostgreSQL Types

| PostgreSQL Type (OID) | Arrow `DataType` | Notes |
| :--- | :--- | :--- |
| `boolean` (`BOOLOID = 16`) | `Boolean` | Native |
| `smallint` (`INT2OID = 21`) | `Int32` | Widened safely on ingest |
| `integer` (`INT4OID = 23`) | `Int32` | Native |
| `bigint` (`INT8OID = 20`) | `Int64` | Native |
| `real` (`FLOAT4OID = 700`) | `Float32` | Native; JSON `NaN`/`Infinity`/`-Infinity` formatted as strings |
| `double precision` (`FLOAT8OID = 701`) | `Float64` | Native; JSON `NaN`/`Infinity`/`-Infinity` formatted as strings |
| `numeric` (`NUMERICOID = 1700`) | `Decimal128(p, s)` | Honors `atttypmod` (`NUMERIC(p, s)`); unconstrained falls back to `(38, 10)`; special values `NaN`/`Inf` rejected (`W2-B-001`) |
| `date` (`DATEOID = 1082`) | `Date32` | Adjusted from PostgreSQL epoch (`2000-01-01`) to Unix epoch (`1970-01-01`) |
| `timestamp` (`TIMESTAMPOID = 1114`) | `Timestamp(Microsecond, None)` | Epoch adjusted; `±infinity` rejected |
| `timestamptz` (`TIMESTAMPTZOID = 1184`) | `Timestamp(Microsecond, UTC / +00:00)` | Epoch adjusted; `±infinity` rejected |
| `text`, `varchar`, `bpchar` (`25`, `1043`, `1042`) | `Utf8` | UTF-8 strings (server-to-UTF-8 transcoded on SPI ingest; `Utf8View` & `Dictionary<Int32|Int64, Utf8>` in `MinimalTuple` IPC) |
| `bytea` (`BYTEAOID = 17`) | `Binary` | Extended bridge & `CustomScan` slot support (`Binary`, `LargeBinary`, `BinaryView` in IPC/`TupleTableSlot`) |
| `time` (`TIMEOID = 1083`) | `Time64(Microsecond)` | Extended bridge, `MinimalTuple` IPC (`Time32`/`Time64`), and `CustomScan` slot support |
| `uuid` (`UUIDOID = 2950`) | `FixedSizeBinary(16)` | Extended bridge, `MinimalTuple` IPC, and `CustomScan` slot support |

Note: `BYTEAOID`, `TIMEOID`, and `UUIDOID` are supported end-to-end in `TupleTableSlot` conversion (`arrow_bridge::arrow_cell_to_pg_datum`); `UInt64`, `Int8`, and `Dictionary<Int64, Utf8>` `TupleTableSlot` datum emission remain tracked in [`W2-E-001`](internal/issues/W2-E-001-custom-scan-missing-extended-and-unsigned-arrow-slot-types.md) / `W3-E-004`.
