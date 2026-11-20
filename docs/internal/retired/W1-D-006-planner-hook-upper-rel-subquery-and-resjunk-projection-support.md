# W1-D-006: `planner_hook` root replacement skips `ORDER BY` `resjunk` targets and mixed cached/uncached path injection

| Field | Value |
| :--- | :--- |
| Title | `planner_hook` root replacement skips `ORDER BY` `resjunk` targets and mixed cached/uncached path injection |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/planner.rs:1010-1106`, `src/custom_scan.rs:502-529`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:63-74` |

## Description
`pg_bolt_planner_hook` replaces the top-level `PlannedStmt.planTree` with a single `CustomScan` node only when `(*planned).subplans` is null and `targetlist_types_supported` (`src/planner.rs:1010-1047`) sees zero `resjunk` columns. This conservative safety guard avoids tuple-descriptor width panics, but creates three limitations:
1. **`ORDER BY` non-projected columns (`resjunk = true`)**: Queries like `SELECT price FROM sales ORDER BY region_id` add `region_id` as a `resjunk = true` `TargetEntry` on PostgreSQL's top-level `Sort` node, while Bolt returns only the non-junk projection (`1` column). Stripping `resjunk` columns from `custom_scan_tlist` (or reading only non-junk attributes) is needed so sorted queries offload cleanly.
2. **Mixed GPU/CPU joins and CTEs**: A query joining a large cached fact table `sales` with an uncached small PostgreSQL table `dim_promo` (`sales JOIN dim_promo`) is rejected whole-query instead of injecting a GPU `CustomPath` for the `sales` scan/partial-aggregate via `set_rel_pathlist_hook` / `create_upper_paths_hook`.
3. **`ORDER BY ... DESC` null ordering (`W1-E-006`)**: If an offloaded query uses `ORDER BY col DESC` without explicit `NULLS LAST`/`NULLS FIRST`, PostgreSQL defaults to `NULLS FIRST` whereas `craton-bolt` uses `NULLS LAST` under `SqlSemantics::Standard`, so nullable `DESC` sorts require explicit SQL rewrite or PostgreSQL `Sort` placement above `CustomScan`.

## Reproduction
1. Execute `SELECT price FROM sales ORDER BY region_id`: PostgreSQL emits 2 `TargetEntry` items (the second with `resjunk = true`), causing `targetlist_types_supported` to decline offload.
2. Execute `SELECT s.region_id FROM sales s JOIN uncached_dim d ON s.id = d.id`: rejected instead of GPU-scanning `sales` below a PostgreSQL `HashJoin`.

## Suggested fix
- Filter `resjunk` entries out of `custom_scan_tlist` (while verifying PostgreSQL top-level `DestReceiver` slot expectations or wrapping `CustomScan` with a `Sort` / `Projection` when `resjunk` is required) and align nullable `DESC` sort semantics (`NULLS FIRST` vs `NULLS LAST`) with `W1-E-006`.
- Add `set_rel_pathlist_hook` + `create_upper_paths_hook` path injection in Phase 4 (`CustomPath` -> `GetCustomPath`) so PostgreSQL's cost-based planner can compose GPU table scans/aggregates beneath PostgreSQL joins, subplans, and `Sort` nodes.

## Resolution (Wave 5)
- Resolved in `src/planner.rs` and `src/custom_scan.rs`:
  1. `targetlist_types_supported` in `src/planner.rs` explicitly skips `resjunk = true` targets, validating only projected columns and allowing queries with hidden sort keys to offload.
  2. `build_non_resjunk_targetlist` constructs a filtered targetlist containing only non-resjunk entries with consecutive `resno = 1..N`, matching Bolt's projected output width.
  3. `build_scan_var_targetlist` constructs `INDEX_VAR` references for `CustomScan.scan.plan.targetlist`.
  4. In `src/custom_scan.rs`, `copy_non_resjunk_slot_attrs` maps fallback slot attributes when `fallback_plan` (e.g. `Sort`) produces tuples with `resjunk` columns, filtering them cleanly to match the CustomScan tuple descriptor.
  5. Pathlist hook structures prepared for mixed CPU/GPU plans:
     - `SyncCustomPathMethods`, `BOLT_CUSTOM_PATH_METHODS`, `custom_path_methods_ptr()`, `bolt_plan_custom_path`, and `create_bolt_custom_path` added to `src/custom_scan.rs`.
     - `PREV_SET_REL_PATHLIST_HOOK` and `pg_bolt_set_rel_pathlist_hook` registered in `src/planner.rs` `install_planner_hook()`.
- Tests added: `unprojected_order_by_and_desc_nulls_first_semantics` in `src/planner.rs` and `custom_scan_plan_serde_roundtrip_and_resjunk_telemetry` in `src/custom_scan.rs`.
