<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-A-002: Missing `ExplainSql` and worker dirty-flag sync commands in `ipc.rs` / `bgworker.rs`

| Field | Value |
| :--- | :--- |
| Title | Missing `ExplainSql` and worker dirty-flag sync commands in `ipc.rs` / `bgworker.rs` |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/ipc.rs:620-690`, `src/bgworker.rs:159-318,440-556`, `src/lib.rs:219-231` |

## Description
`pg_bolt::api` (`src/lib.rs`) now dispatches control-plane and query operations through `bgworker::shared_bgworker_client()` whenever `pg_bolt.use_bgworker = true` (default). Two gaps remain in `IpcCommand` / `BgWorkerClient`:
1. **`ExplainSql` missing from IPC**: `api::explain_sql` cannot forward SQL plan explanation over `BgWorkerClient` and currently returns `PgBoltError::Ipc("EXPLAIN SQL over bgworker IPC is not yet implemented; set pg_bolt.use_bgworker = off")` when `use_bgworker = true`.
2. **Worker dirty-flag synchronization missing from IPC**: `pg_hooks` (`src/lib.rs`) marks `PLANNER_CATALOG` and local `BoltSessionRuntime` dirty/clean across `ExecutorEnd`, `ProcessUtility`, and `pg_xact`/`subxact` rollback, and `bolt::cache_table` checks `pg_hooks::is_table_dirty`. However, `BoltBgWorker`'s inner `BoltSessionRuntime` has no `MarkDirty` / `ClearDirty` IPC command, so `api::tables()` / `client.tables_json()` still reports `dirty: false` inside the worker until `cache_table` / `evict_table` refreshes it. Also `bgworker::shutdown_shared_bgworker()` (`src/bgworker.rs:679-688`) works via `Drop`, while `BgWorker` `execute_sql_with_effective_timeout` / `reconfigure` parity under `pgrx` is tracked in `W2-C-001`.

## Reproduction
1. Leave `pg_bolt.use_bgworker = on` (default) and call `api::load_batch(...)` followed by `api::explain_sql("SELECT ... ")`.
2. `api::explain_sql` returns `PgBoltError::Ipc`.
3. Under PostgreSQL with `use_bgworker = on`, commit an `INSERT` against a cached table: `PLANNER_CATALOG` and `pg_hooks` mark the table dirty, whereas `client.tables_json()` inside the worker still reports `dirty: false` until `bolt.cache_table` reloads it.

## Suggested fix
- Add `IpcCommand::ExplainSql { sql: String }` -> `IpcResponse::ExplainSql { plan: String }` and `BgWorkerClient::explain_sql(&self, sql: &str)`, and wire `api::explain_sql` in `src/lib.rs` to call it.
- Add `IpcCommand::SetTableDirty { bolt_name: String, dirty: bool }` -> `IpcResponse::Ack` and call it from `pg_hooks::sync_dirty_state` when `use_bgworker` is enabled.
- Cover both commands in `tests/bgworker_ipc_test.rs`.

## Progress (Wave 3)
- Added `IpcCommand::ExplainSql`, `IpcResponse::ExplainSql`, and `IpcCommand::SetTableDirty` + wire encoding/decoding in `src/ipc.rs`.
- Added `BgWorkerClient::explain_sql`, `BgWorkerClient::set_table_dirty`, worker `dispatch_command` handlers (`runtime.explain_sql`, `runtime.mark_dirty` / `runtime.clear_dirty`), and unit test `explain_sql_and_set_table_dirty_roundtrip_over_bgworker` in `src/bgworker.rs`.
- Remaining outside `src/{ipc,bgworker}.rs`: wire `api::explain_sql` and `pg_hooks::sync_dirty_state` in `src/lib.rs`, and cover both in `tests/bgworker_ipc_test.rs`.
