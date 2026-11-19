# W1-E-009: Unit tests in `runtime.rs`, `planner.rs`, and `bgworker.rs` expect `ExecutionTier::Gpu` for `GROUP BY` aggregates

| Field | Value |
| :--- | :--- |
| Title | Unit tests in `runtime.rs`, `planner.rs`, and `bgworker.rs` expect `ExecutionTier::Gpu` for `GROUP BY` aggregates |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/runtime.rs:870-873`, `src/planner.rs:886,928-931,1322-1330`, `src/bgworker.rs:663-666`, `../bolt/src/plan/physical_plan.rs:1457-1499` |

## Description
Three unit tests inside `pg_bolt` assert `ExecutionTier::Gpu` on `SELECT ... SUM(...) ... GROUP BY ...` queries:
1. `src/runtime.rs:870-873` (`stub_lifecycle_register_delta_explain_evict_and_fork_guard`): `rt.explain_tier("SELECT id, SUM(val) FROM sales GROUP BY id")` asserts `assert_eq!(tier, ExecutionTier::Gpu)`.
2. `src/planner.rs:1322-1330` (`analyze_eligibility_handles_thresholds_tiers_schemas_and_dirty_tables`): `analyze_sql_eligibility("SELECT region_id, SUM(price * tax) AS rev FROM public.sales GROUP BY region_id", ...)` asserts `assert_eq!(ok.tier, Some(ExecutionTier::Gpu))`.
3. `src/bgworker.rs:663-666` (`worker_lifecycle_and_restart_after_shutdown`): `client.explain_tier("SELECT id, SUM(val) FROM sales GROUP BY id")` asserts `assert_eq!(tier, ExecutionTier::Gpu)`.

In `craton-bolt` 0.12.0 (`../bolt/src/plan/physical_plan.rs:1457-1499`), `PhysicalPlan::planned_execution_tier()` returns `ExecutionTier::Gpu` only for pure `PhysicalPlan::Projection { .. }`, and returns `ExecutionTier::Hybrid` for `PhysicalPlan::Aggregate`, `Distinct`, `Sort`, `Join`, `Limit`, and `Filter`/`Project` host wrappers. Consequently, `cargo test --lib` fails all three unit tests. Additionally, `src/planner.rs:928-931` checks `ExecutionTier::Host` on `physical.planned_execution_tier()`, which is dead code for `lower_physical()` plans (as `Host` is only chosen at runtime by `Engine` `TierPolicy::CostBased`, not `planned_execution_tier()`).

## Reproduction
1. Run `cargo test --lib --no-default-features --features cuda-stub`.
2. Observe assertion failures (`left: Hybrid, right: Gpu`) in `runtime::tests`, `planner::tests`, and `bgworker::tests`.

## Suggested fix
- Update those three unit tests to either use a pure projection (`SELECT id, val * 2.0 AS doubled FROM sales WHERE id > 0`) when asserting `ExecutionTier::Gpu`, or assert `ExecutionTier::Hybrid` / `matches!(tier, ExecutionTier::Gpu | ExecutionTier::Hybrid)` for `GROUP BY` aggregates (matching `src/bgworker.rs:684-687`).
- Add a projection-tier test for `ExecutionTier::Gpu` and a `Hybrid`-rejection test using `allow_hybrid_fallback: false` in `src/planner.rs`.

## Resolution (Wave 2)
Fixed across `src/runtime.rs` (`stub_lifecycle_register_delta_explain_evict_and_fork_guard` now checks `ExecutionTier::Gpu` for pure projection, `ExecutionTier::Hybrid` for `GROUP BY`, and lowercase `"Logical plan:"` / `"Physical plan:"` matching `craton-bolt::Engine::explain_sql`), `src/lib.rs` (`pg_tests::test_validation_and_table_lifecycle` checks `"Hybrid"` for `GROUP BY` and `"Gpu"` for pure projection), `src/planner.rs`, and `src/bgworker.rs`.
