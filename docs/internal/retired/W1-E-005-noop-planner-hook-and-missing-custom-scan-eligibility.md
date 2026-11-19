# W1-E-005: No-op `planner_hook` and missing CustomScan eligibility, CTE handling, fallback, and cancellation

| Field | Value |
| :--- | :--- |
| Title | No-op `planner_hook` and missing CustomScan eligibility, CTE handling, fallback, and cancellation |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:505-550` |

## Description
`src/planner.rs` in `PG_BOLT_PLAN_AND_CODE.md` merely logs `pg_bolt planner hook called` and delegates unchanged to `standard_planner`. It lacks all required `CustomScan` planning and execution infrastructure:
- No `CustomScanMethods` / `CustomExecMethods` state machine (`begin`, `execute`, `next_row_json`, `rescan`, `end`, `explain`).
- No query eligibility filter rejecting DML (`INSERT`/`UPDATE`/`DELETE`), locking clauses (`SELECT ... FOR UPDATE`), or volatile functions (`random()`, `nextval()`, `clock_timestamp()`, `txid_current()`).
- No table/CTE resolution verifying that all referenced base relations are cached in Bolt without false-positively rejecting CTE aliases as missing tables.
- No threshold check (`min_offload_rows`), `craton-bolt` `ExecutionTier::Gpu` classification gate, cost model comparison (`estimated_bolt_cost < estimated_pg_cost`), `VramExhausted` CPU fallback, or timeout-based cancellation (`Engine::sql_with_cancel`).

## Reproduction
Inspect `PG_BOLT_PLAN_AND_CODE.md:505-550`. Setting `bolt.enable_offload = on` produces zero `Custom Scan` plan nodes and offloads neither SQL nor parameters.

## Suggested fix
Implement SQL eligibility analysis, CTE-aware table extraction, `CustomScan` state lifecycle (`BoltCustomScanState`), cost/tier gating, `execute_with_cpu_fallback` on `VramExhausted`, and `CancellationToken` timeout enforcement via `Engine::sql_with_cancel`.
