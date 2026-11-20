<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-A-005: Upstream craton-bolt ergonomics for MemTableProvider::set_sql_semantics and serde derives

| Field | Value |
| :--- | :--- |
| Title | Upstream craton-bolt ergonomics for MemTableProvider::set_sql_semantics and serde derives |
| Severity | medium |
| Status | resolved |
| Kind | proposal |
| Files | `src/runtime.rs`, `../bolt/src/plan/sql_frontend.rs`, `../bolt/src/cuda/mem_pool.rs`, `../bolt/src/plan/physical_plan.rs` |

## Description
In `craton-bolt` 0.12.0 (`../bolt`):
1. `MemTableProvider::set_sql_semantics` (`../bolt/src/plan/sql_frontend.rs:696`) is `pub(crate)`, preventing downstream crates from constructing a standalone `MemTableProvider` configured with `SqlSemantics::Standard` matching `Engine::builder().sql_semantics(SqlSemantics::Standard)`. `pg_bolt` works around this in `src/runtime.rs` via a `StandardTableProvider(MemTableProvider)` wrapper overriding `TableProvider::sql_semantics`.
2. `PoolStats` (`../bolt/src/cuda/mem_pool.rs:809`) and `ExecutionTier` (`../bolt/src/plan/physical_plan.rs:23`) do not implement `serde::Serialize`/`Deserialize` (even behind an optional `serde` feature), requiring a custom `pool_stats_serde` adapter inside `src/runtime.rs` to serialize `BoltRuntimeStatus` into `bolt.status()` `jsonb`.

## Reproduction
Attempt to call `MemTableProvider::new().set_sql_semantics(SqlSemantics::Standard)` or put `#[derive(Serialize, Deserialize)]` directly on a struct containing `PoolStats` from an external crate (`pg_bolt`). Both fail compilation due to visibility / missing trait bounds.

## Resolution
1. **Clean In-Tree Wrappers**:
   - `StandardTableProvider`: A zero-overhead newtype `StandardTableProvider(MemTableProvider)` in `src/runtime.rs` delegating all schema, null count, and column nullability calls directly to `MemTableProvider` while overriding `TableProvider::sql_semantics` to return `SqlSemantics::Standard`. Thoroughly documented with upstream rationale.
   - `pool_stats_serde`: A custom serde module in `src/runtime.rs` providing transparent serialization and deserialization for `craton_bolt::PoolStats` via `PoolStatsRepr` without leaking or modifying upstream traits.
2. **Unit Tests Added**:
   - `test_standard_table_provider_and_pool_stats_serde` in `src/runtime.rs::tests::stub_tests` verifies that `StandardTableProvider::sql_semantics()` returns `SqlSemantics::Standard`, and confirms lossless JSON serialization/deserialization roundtrip of `BoltRuntimeStatus` with `pool_stats`.
3. **Upstream Action Plan (Post-0.12.0 PR)**:
   - Propose `pub fn with_sql_semantics(mut self, semantics: SqlSemantics) -> Self` and `pub fn set_sql_semantics(&mut self, semantics: SqlSemantics)` on `MemTableProvider` in `craton-bolt`.
   - Propose an optional `serde` feature in `../bolt/Cargo.toml` deriving `Serialize, Deserialize` on `PoolStats` and `ExecutionTier`.
   - Once upstreamed in a future `craton-bolt` release, `StandardTableProvider` and `pool_stats_serde` can be retired cleanly.
