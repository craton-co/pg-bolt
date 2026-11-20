<!-- SPDX-License-Identifier: Apache-2.0 -->

# W3-E-003: `attach_custom_scan_plan` keeps `Aggref`/`OpExpr` in `scan.plan.targetlist` instead of `INDEX_VAR` references

| Field | Value |
| :--- | :--- |
| Title | `attach_custom_scan_plan` keeps `Aggref`/`OpExpr` in `scan.plan.targetlist` instead of `INDEX_VAR` references |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/planner.rs:1805-1901` |

## Description
In `attach_custom_scan_plan` (`src/planner.rs:1856-1890`), both `(*cscan).scan.plan.targetlist` and `(*cscan).custom_scan_tlist` are assigned the exact same `cscan_tlist` derived from `(*orig_plan).targetlist`:
```rust
(*cscan).scan.plan.targetlist = cscan_tlist;
(*cscan).custom_scan_tlist = cscan_tlist;
```
When `orig_plan` is an `Agg` or `Sort` over computed expressions (e.g. `SELECT region_id, SUM(price) FROM sales GROUP BY region_id`), `cscan_tlist` contains `Aggref` (`SUM(...)`) and `OpExpr` nodes. Because PostgreSQL's `ExecInitCustomScan` builds projection expressions from `scan.plan.targetlist` against `custom_scan_tlist` (`ss_ScanTupleSlot`), leaving `Aggref` nodes inside `scan.plan.targetlist` (rather than rewriting computed entries in `scan.plan.targetlist` to `Var`s referencing `INDEX_VAR` / `resno` of `custom_scan_tlist`, or suppressing redundant projection) causes `ExecInitExpr` / `ExecAssignScanProjectionInfoWithVarno` to fail or mis-evaluate expressions that `CustomScan` already computed on the GPU.

## Reproduction
1. Load `sales` above `min_offload_rows` and execute `SELECT region_id, SUM(price) FROM sales GROUP BY region_id` under `pg_bolt.enable_custom_scan = on`.
2. `pg_bolt_planner_hook` attaches a `CustomScan` whose `scan.plan.targetlist` contains an `Aggref` node.
3. PostgreSQL's `ExecInitCustomScan` -> `ExecInitExpr` rejects `Aggref` inside a scan targetlist.

## Suggested fix
- When setting `(*cscan).custom_scan_tlist = cscan_tlist`, construct `(*cscan).scan.plan.targetlist` with `TargetEntry` items whose `.expr` is `makeVar(INDEX_VAR, tle.resno, exprType(tle.expr), exprTypmod(tle.expr), exprCollation(tle.expr), 0)` (preserving `resname`, `resno`, and `resorigtbl`/`resorigcol` metadata), so `ExecInitCustomScan` maps columns directly from `ss_ScanTupleSlot` without re-evaluating `Aggref` / `OpExpr` nodes in the scan node.
- Add a `#[pg_test]` running a real SQL `SELECT region_id, SUM(price) ... GROUP BY region_id` through PostgreSQL's executor with `CustomScan` enabled.

## Resolution
- Added `unsafe fn build_scan_var_targetlist(custom_scan_tlist: *mut pg_sys::List) -> *mut pg_sys::List` (`src/planner.rs:2228-2267`) under `#[cfg(feature = "pgrx")]` to clone each `TargetEntry` via `pg_sys::flatCopyTargetEntry` and replace `(*copy).expr` with `pg_sys::makeVar(pg_sys::INDEX_VAR, (*tle).resno, pg_sys::exprType(expr_node), pg_sys::exprTypmod(expr_node), pg_sys::exprCollation(expr_node), 0) as *mut pg_sys::Expr`.
- Updated `attach_custom_scan_plan` (`src/planner.rs:2292-2326`) to construct `scan_tlist = build_scan_var_targetlist(cscan_tlist)` (returning early if null), assign `(*cscan).scan.plan.targetlist = scan_tlist`, and retain `(*cscan).custom_scan_tlist = cscan_tlist`.
- Live PostgreSQL `#[pg_test]` in `src/lib.rs` remains outside this wave's file scope; coordinator `pgrx` build/test runs pending.
