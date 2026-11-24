# W1-E-010: Eager LRU eviction and `plan_provider` mutation occur before `engine` table registration succeeds

| Field | Value |
| :--- | :--- |
| Title | Eager LRU eviction and `plan_provider` mutation occur before `engine` table registration succeeds |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/runtime.rs:312-371,393-415,459-480` |

## Description
In `BoltSessionRuntime::register_batch` (`src/runtime.rs:393-415`) and `BoltSessionRuntime::append_delta_batch` (`src/runtime.rs:459-480`), `self.enforce_vram_budget(...)` (`src/runtime.rs:312-351`) permanently evicts healthy cached tables via `self.evict_table(&victim_name)` and `self.sync_plan_provider_table(...)` / `set_column_nullability(...)` mutates `self.plan_provider` **before** calling `self.engine.replace_table` / `self.engine.register_table` / `self.engine.register_batch`.

If `self.engine` rejects the new batch (for example due to GPU device OOM, unsupported Arrow layout in extended columns, or dictionary cardinality errors in `Engine`), the caller receives an error, yet:
1. Previously cached LRU tables evicted during `enforce_vram_budget` remain permanently lost from `cached_tables` and `engine`.
2. `self.plan_provider` already reflects the failed table's schema/nullability even though `self.cached_tables` and `self.engine` do not contain it (or still hold the old snapshot when `replace_table` failed), leaving `explain_tier` / `explain_sql` inconsistent with `list_tables` and `execute_sql`.

## Reproduction
1. Set `max_vram_mb = 1` and cache table `t1`.
2. Call `register_batch` (or `append_delta_batch`) with a batch whose `approx_bytes` triggers eviction of `t1` in `enforce_vram_budget`, but fails inside `self.engine.register_table` / `register_batch`.
3. `t1` is gone and `plan_provider` has unregistered/stale metadata while the new batch also failed to register.

## Suggested fix
- Snapshot or compute victim candidates inside `enforce_vram_budget` without deleting `plan_provider` / `cached_tables` state until `engine` operations succeed, or move `self.sync_plan_provider_table` strictly **after** `self.engine.*` returns `Ok(())`.
- On `engine.replace_table` / `register_table` / `register_batch` error, avoid discarding evicted tables preemptively or restore `plan_provider` schema from `self.table_schemas` in a clean rollback helper.

## Resolution (Wave 2)
Fixed in `src/runtime.rs`: `register_batch` and `append_delta_batch` now compute LRU victim lists (`plan_vram_evictions`) and candidate engine identifiers (`preview_engine_ident`) read-only before engine execution, and commit evictions, `engine_idents`, `plan_provider`, `cached_tables`, `table_schemas`, and planner registration strictly after `self.engine` succeeds. Covered by `failed_register_and_delta_preserve_cache_and_rollback_evictions`.
