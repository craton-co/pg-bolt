<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-002: Wire background worker, IPC arena dispatch, and CustomScan into `_PG_init`

| Field | Value |
| :--- | :--- |
| Title | Wire background worker, IPC arena dispatch, and CustomScan into `_PG_init` |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/lib.rs`, `src/bgworker.rs`, `src/custom_scan.rs`, `src/ipc.rs`, `src/planner.rs` |

## Description
`_PG_init` in `src/lib.rs` currently calls `guc::register_gucs()` and `unsafe { planner::install_planner_hook(); }`, and `api::query` / `api::load_batch` execute against the per-backend thread-local `BoltSessionRuntime`. When `pg_bolt.use_bgworker = true` and `pg_bolt` is loaded via `shared_preload_libraries = 'pg_bolt'`, `_PG_init` should also register the dedicated CUDA background worker (`bgworker`) and PostgreSQL shared-memory segment/LwLock hooks, and `api::*` / `bolt::*` should route SQL/table commands through `ipc::SharedMemoryArena` instead of spawning one CUDA context (~300 MiB VRAM overhead) per backend connection.

## Reproduction
1. Set `shared_preload_libraries = 'pg_bolt'` and `pg_bolt.use_bgworker = on` in `postgresql.conf`.
2. Connect 10 concurrent PostgreSQL sessions and run `SELECT * FROM bolt.query(...)` or `bolt.load_table(...)`.
3. Expected: A single persistent CUDA background worker owns the `craton_bolt::Engine` and services backends via `SharedMemoryArena`.
4. Actual: Each backend initializes its own `BoltSessionRuntime` in `BOLT_RT` until Wave 2 wires `bgworker` + `ipc::SharedMemoryArena` dispatch into `_PG_init` and `api`.

## Resolution
1. In `src/lib.rs::_PG_init`, `ipc::init_shmem_hooks()` and `bgworker::register_bgworker()` are now unconditionally wired when `pg_sys::process_shared_preload_libraries_in_progress` is `true`.
2. All `bolt.*` SQL functions (`bolt.query`, `bolt.query_json`, `bolt.status`, `bolt.tables`, `bolt.load_table`, `bolt.cache_table`, `bolt.sync_table`, `bolt.sync_delta`, `bolt.evict_table`) and `api::*` functions route via `bgworker::shared_bgworker_client()` whenever `pg_bolt.use_bgworker = on` (default).
3. Added unit tests in `src/lib.rs::api::tests` (`api_sync_delta_validates_bolt_name_and_supports_direct_and_bgworker_modes`) and `src/bgworker.rs` validating client dispatch, dirty flag sync, and query execution over IPC.
4. Added `#[pg_test]` in `src/lib.rs` validating `owner_pid` from `bolt.status()` and verified `pg_test` configuration with `shared_preload_libraries = 'pg_bolt'`.
