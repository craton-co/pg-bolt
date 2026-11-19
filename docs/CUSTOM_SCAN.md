<!-- SPDX-License-Identifier: Apache-2.0 -->

# CustomScan & Planner Integration

`pg_bolt` hooks into PostgreSQL's planner (`planner_hook` in `src/planner.rs`) to transparently replace eligible analytical scans/aggregations with a PostgreSQL `CustomScan` node (`PgBoltCustomScan` in `src/custom_scan.rs`) when doing so beats estimated PostgreSQL CPU execution cost.

## Eligibility Pipeline (`analyze_sql_eligibility`)

Every query inspected by `planner_hook` passes through two stages before a `CustomScan` node is attached:

### 1. AST Safety Inspection (`inspect_sql_for_offload`)

Powered by `sqlparser` (`0.52`, `PostgreSqlDialect`), `SqlInspector` inspects the query AST and rejects non-analytical constructs:
- Non-`SELECT` statements (`INSERT`, `UPDATE`, `DELETE`, DDL) and multi-statement strings
- Locking clauses (`FOR UPDATE`, `FOR SHARE`)
- Data-modifying CTEs (`WITH ... AS (DELETE ...)`) and `SELECT ... INTO`
- Queries referencing volatile / side-effecting functions (`random()`, `nextval()`, `clock_timestamp()`, `txid_current()`, etc.)
- Parameterized queries (`$1`, `$2`, ...)
- Unsupported SQL constructs flagged during AST traversal

Note: Parse/inspection errors are converted by `analyze_sql_eligibility` into `Ok(PlanEligibility::ineligible(...))` so `planner_hook` leaves unsupported PostgreSQL queries on PostgreSQL's native executor cleanly without aborting client queries. Sequential non-recursive CTE scoping (`WITH sales AS (SELECT ... FROM sales) SELECT ... FROM sales`) tracks CTE definitions sequentially so base-table references inside CTE bodies remain visible (`W1-E-011`).

### 2. Catalog, Freshness, SQL Rewrite & Cost Check

- Requires `pg_bolt.enabled = on` and `pg_bolt.enable_custom_scan = on`.
- Verifies every referenced base relation is cached in Bolt and not marked `dirty` (including dirty flags synchronized over `BgWorkerClient::set_table_dirty` when `pg_bolt.use_bgworker = on`).
- Verifies cached table cardinality meets `pg_bolt.min_offload_rows` (default `50000`).
- Runs `rewrite_sql_for_bolt` to rewrite schema-qualified relations to `bolt_name` (preserving aliases), collapse 3-part `schema.table.col` column qualifiers (in `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`), and normalize PostgreSQL `ORDER BY` null ordering (`ASC` -> `ASC NULLS LAST`, `DESC` -> `DESC NULLS FIRST`) for `craton-bolt` compatibility.
- Maps the table schema to `BoltSchema` (`SqlSemantics::Standard`) and plans `craton-bolt` execution tier (`Gpu`, `Hybrid`, or `Host`).
- Rejects `Hybrid` plans when `pg_bolt.allow_hybrid_fallback = off`.
- Strips `resjunk` target entries from `cscan_tlist` when Bolt computes ordering internally.
- Compares analytical cost estimates:
  - **PostgreSQL CPU estimate (`estimate_pg_cpu_cost`)**: `5.0 + rows * (0.020 + agg + join) + sort` (higher per-row and sort cost).
  - **Bolt CustomScan estimate (`estimate_bolt_custom_scan_cost`)**: `250.0` startup + lower per-row GPU compute + optional H2D transfer (`0.0` when already cached in VRAM).
  - Offloads only when `estimated_bolt_cost < estimated_pg_cost`.

## `CustomScan` Lifecycle, Fallback & `EXPLAIN`

`src/custom_scan.rs` implements PostgreSQL `CustomScanMethods` and `CustomExecMethods`:
- `CreateCustomScanState` / `BeginCustomScan`: Deserializes `BoltCustomScanPlan` from `custom_private`, retains PostgreSQL's preserved `orig_plan` in `custom_plans` as the CPU fallback subplan, creates a resettable per-tuple child `MemoryContext` under `EState::es_query_cxt`, and registers a `MemoryContextCallback` for cleanup across error unwinds.
- `ExecCustomScan`: Executes via `BoltSessionRuntime` (or `BgWorkerClient` over `SharedMemoryArena` when `pg_bolt.use_bgworker = on`). On `VramExhausted` (or GPU execution error when `fallback_on_oom = true`), transparently initializes and executes PostgreSQL's preserved `orig_plan` CPU subplan; otherwise resets the per-tuple `MemoryContext` per row and emits `RecordBatch` cells (`arrow_bridge::write_record_batch_row_datums` / `populate_tuple_slot_from_batch`) into `TupleTableSlot` without leaking varlenas into `es_query_cxt`.
- `ReScanCustomScan` / `EndCustomScan`: Resets cursor (or fallback subplan), releases state and per-tuple memory context, and records stats.
- `ExplainCustomScan`: Reports `"Bolt Execution Tier"`, `"PTX Cache"` (`Pending` / `Hit` / `Miss`), `"Fallback"` when CPU fallback ran, and under `EXPLAIN ANALYZE` reports `"Kernel Time"`, `"Total Exec Time"` (`ms`), and `"Rows Emitted"`.

## Current `CustomScan` Scope & Open Work

See [`docs/internal/issues/`](internal/issues/) for tracked limitations under PostgreSQL:
- Static query-string planning vs PostgreSQL rewritten parsetrees/subplans (`W1-D-003`, `W1-D-006`)
- Supporting `Int8`, unsigned integers (`UInt8..=UInt64`), and `Dictionary<Int64, Utf8>` in `arrow_bridge::arrow_cell_to_pg_datum` (`W2-E-001`, `W3-E-004`)
- Deforming lazy fallback slots via `slot_getallattrs` inside `copy_non_resjunk_slot_attrs` (`W3-E-002`)
- Rewriting `Aggref`/`OpExpr` nodes in `CustomScan::scan.plan.targetlist` to `INDEX_VAR` references (`W3-E-003`)
