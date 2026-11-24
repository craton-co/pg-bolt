<!-- SPDX-License-Identifier: Apache-2.0 -->

# Configuration (GUCs)

`pg_bolt` registers 9 custom configuration variables (`GUC`s) during `_PG_init` (`src/guc.rs`). All names live under the `pg_bolt.*` prefix.

## Parameter Reference

| Name | Type | Default | Range | Context | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `pg_bolt.enabled` | `bool` | `on` (`true`) | `on` / `off` | `Userset` | Master switch for `bolt.*` execution and planner offload. When `off`, `bolt.*` SQL entry points raise `PgBoltError::Disabled` and `planner_hook` skips offload. |
| `pg_bolt.min_offload_rows` | `integer` | `50000` | `0..=2147483647` | `Userset` | Minimum cached table row count required before `CustomScan` considers offloading a query. Avoids GPU kernel launch overhead on small OLTP tables. |
| `pg_bolt.max_vram_mb` | `integer` | `0` | `0..=2147483647` | `Suset` | GPU VRAM cache budget in MiB (`0` = unlimited). Triggers LRU eviction when loading or syncing tables would exceed the cap. |
| `pg_bolt.enable_custom_scan` | `bool` | `on` (`true`) | `on` / `off` | `Userset` | Enables transparent PostgreSQL `CustomScan` offloading in `planner_hook`. When `off`, only explicit `bolt.query()` / `bolt.query_json()` calls use Bolt. |
| `pg_bolt.allow_hybrid_fallback` | `bool` | `on` (`true`) | `on` / `off` | `Userset` | Allows `CustomScan` to offload queries that lower to `ExecutionTier::Hybrid`. When `off`, Hybrid-tier plans stay on PostgreSQL CPU. |
| `pg_bolt.statement_timeout_ms` | `integer` | `0` | `0..=2147483647` | `Userset` | Cooperative query cancellation timeout in milliseconds passed to `craton-bolt::CancellationToken` (`0` = disabled). |
| `pg_bolt.use_bgworker` | `bool` | `on` (`true`) | `on` / `off` | `Suset` | Routes `bolt.*` / `api::*` control-plane commands (`LoadBatch*`, `AppendDelta*`, `EvictTable`, `ListTables`, `ExplainTier`, `ExplainSql`, `SetTableDirty`, `Status`), `ExecuteSql`, and `CustomScan` queries through `BgWorkerClient` / `BoltBgWorker` when enabled. |
| `pg_bolt.ipc_ring_slots` | `integer` | `16` | `2..=4096` | `Suset` | Number of slots in the IPC shared-memory ring buffer between backends and the background worker. |
| `pg_bolt.ipc_slot_bytes` | `integer` | `16777216` (16 MiB) | `4096..=536870912` (512 MiB) | `Suset` | Byte capacity of each IPC ring buffer slot for Arrow IPC / `MinimalTuple` transfer; both oversized write requests (`LoadBatchChunk` / `AppendDeltaChunk`) and large `ExecuteSql` result responses stream automatically in chunks when exceeding a single slot. |

## Context Notes

- **`Userset`**: Any session can override per-session or per-query with `SET pg_bolt.enabled = off;` or `SET pg_bolt.min_offload_rows = 10000;`.
- **`Suset`**: Requires PostgreSQL superuser privileges (either `postgresql.conf`, `ALTER SYSTEM`, or superuser `SET`).
- Registering `pg_bolt` in `shared_preload_libraries` is required for background worker initialization at server startup, while `Suset`/`Userset` GUC contexts still allow dynamic loading (`CREATE EXTENSION pg_bolt`) in single-backend development setups.

## Example `postgresql.conf`

```ini
shared_preload_libraries = 'pg_bolt'

pg_bolt.enabled = on
pg_bolt.enable_custom_scan = on
pg_bolt.min_offload_rows = 50000
pg_bolt.max_vram_mb = 4096
pg_bolt.statement_timeout_ms = 30000
pg_bolt.use_bgworker = on
pg_bolt.ipc_ring_slots = 16
pg_bolt.ipc_slot_bytes = 16777216
```
