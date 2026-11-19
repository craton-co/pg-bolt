<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-A-002: `BoltSessionRuntime::reconfigure` cannot dynamically resize `craton_bolt::Engine` OOC memory budget

| Field | Value |
| :--- | :--- |
| Title | `BoltSessionRuntime::reconfigure` cannot dynamically resize `craton_bolt::Engine` OOC memory budget |
| Severity | low |
| Status | resolved |
| Kind | proposal |
| Files | `src/runtime.rs`, `src/bgworker.rs`, `src/ipc.rs`, `../bolt/src/exec/engine.rs` |

## Description
`BoltSessionRuntime::new_with_config` (`src/runtime.rs`) passes `pg_bolt.max_vram_mb` at startup to `craton_bolt::Engine::builder().memory_budget(budget)`. On `SIGHUP`, `BoltSessionRuntime::reconfigure(&cfg)` updates `self.max_vram_bytes` and evicts LRU cached tables exceeding the new limit, because `craton_bolt::Engine` exposes no post-build `&mut Engine` method to adjust its internal out-of-core (`memory_budget_bytes`) operator spill threshold after `build()`.

## Reproduction
1. Start PostgreSQL with `pg_bolt.max_vram_mb = 4096`.
2. Lower `pg_bolt.max_vram_mb` to `512` and reload config (`SELECT pg_reload_conf();`).
3. `BoltSessionRuntime::reconfigure` evicts cached tables above 512 MiB, while intermediate execution buffers inside `craton_bolt::Engine` continue using the initial 4096 MiB OOC threshold until worker restart.

## Suggested fix
- Expose `pub fn set_memory_budget(&mut self, bytes: Option<usize>)` on `craton_bolt::Engine` (`../bolt/src/exec/engine.rs`).
- Invoke `self.engine.set_memory_budget(new_budget)` inside `BoltSessionRuntime::reconfigure` under `#[cfg(not(feature = "cuda-stub"))]`.

## Wave 5 Resolution & Runtime Tracking
- **Telemetry & Budget State Tracking**:
  - Implemented `WorkerMemoryBudgetTracker` (`src/bgworker.rs`) storing `initial_engine_budget_bytes`, `current_vram_budget_bytes`, and `reconfigure_count`.
  - Added telemetry logging during SIGHUP:
    `"pg_bolt worker memory budget dynamically reconfigured from {:?} to {:?} bytes (evicting LRU tables; engine OOC operator budget retained until restart)"`
  - In `PgShmemRingControl` (`src/ipc.rs`), added `active_memory_budget_bytes: AtomicU64` updated via `pg_shmem::update_active_budget` on SIGHUP and worker startup so all backends observe the active limit.
- **Architectural Fallback Design**:
  - Because `craton_bolt` 0.12.0 `Engine` and `MemoryGovernor` do not allow in-place reconfiguration after build, `pg_bolt`'s LRU table cache eviction immediately sheds cached tables down to the new GUC limit, freeing GPU VRAM for other allocations, while engine OOC spill capacity retains stability until daemon restart.
- **Unit Tests**:
  - `test_worker_memory_budget_tracker_records_transitions` in `src/bgworker.rs` validates budget tracking across reconfigures (including unbounded 0).
  - `shmem_ring_control_offsets_and_budget_tracking` in `src/ipc.rs` tests cross-process memory budget atomics.
