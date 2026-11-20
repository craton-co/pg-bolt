# W1-C-002: `HEAP_XMAX_IS_MULTI` requires PostgreSQL `MultiXactIdGetUpdateXid` member resolution

| Field | Value |
| :--- | :--- |
| Title | `HEAP_XMAX_IS_MULTI` requires PostgreSQL `MultiXactIdGetUpdateXid` member resolution |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/mvcc.rs`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:74` |

## Description
When `HEAP_XMAX_IS_MULTI` (`0x1000`) is set in `t_infomask` and neither `HEAP_XMAX_LOCK_ONLY` (`0x0080`) nor `HEAP_XMAX_INVALID` (`0x0800`) is set, `t_xmax` stores a PostgreSQL `MultiXactId` rather than a `TransactionId`. Because `MultiXactId` uses a separate 32-bit sequence space from `TransactionId`, comparing `t_xmax` against `snapshot.xmin`/`xmax`/`active_xids` is invalid.

In `src/mvcc.rs` (pure-Rust without access to `pg_multixact` SLRU), non-lock-only `MultiXactId` deleters currently fall back conservatively based on `HEAP_XMAX_COMMITTED` alone:
- If `HEAP_XMAX_COMMITTED` is set, the tuple is treated as deleted (`false`), even if the deleting member transaction committed after `snapshot.xmax` or was in `snapshot.active_xids`.
- If `HEAP_XMAX_COMMITTED` is unset, the tuple is treated as visible (`true`), even if the deleting member committed before our snapshot and its hint bit simply hadn't been flushed yet.

## Reproduction
1. In PostgreSQL, Transaction A acquires `SELECT ... FOR KEY SHARE` on a row, and Transaction B executes `UPDATE` or `DELETE` and commits before Transaction C takes a snapshot (without a subsequent reader setting `HEAP_XMAX_COMMITTED`), OR commits after Transaction C takes a snapshot.
2. `t_infomask` has `HEAP_XMAX_IS_MULTI` without `HEAP_XMAX_LOCK_ONLY`.
3. Pure-header MVCC inspection cannot distinguish which member XID performed the update without reading `pg_multixact`.

## Suggested fix
Under `#[cfg(feature = "pgrx")]`, invoke PostgreSQL's `pg_sys::MultiXactIdGetUpdateXid(hdr.t_xmax, hdr.t_infomask)` to extract the actual deleter `TransactionId` and pass it through `HeapTupleSatisfiesMVCC` visibility (`TransactionIdDidCommit` + `XidInMVCCSnapshot`), falling back to `MultiXact` unit tests on pure-Rust builds.
