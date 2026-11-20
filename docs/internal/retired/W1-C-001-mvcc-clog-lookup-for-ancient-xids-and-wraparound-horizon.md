# W1-C-001: Missing `pg_xact` clog lookup for hint-bit-less `< xmin` tuples and XID wraparound horizon guard

| Field | Value |
| :--- | :--- |
| Title | Missing `pg_xact` clog lookup for hint-bit-less `< xmin` tuples and XID wraparound horizon guard |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/mvcc.rs`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:74` |

## Description
Two edge cases affect pure-Rust `MvccSnapshot` visibility checks when scanning cold PostgreSQL heap pages:
1. **Hint-bit-less aborted tuples `< xmin`**: On cold pages where `HEAP_XMIN_COMMITTED` and `HEAP_XMIN_INVALID` have not yet been set by a prior reader or `VACUUM`, `t_xmin < snapshot.xmin` only proves that `t_xmin` is no longer in-progress (`!XidInMVCCSnapshot`); it does **not** prove that `t_xmin` committed. If `t_xmin` aborted and its hint bits were never written (e.g., crash recovery or bulk aborted load), treating all `xid < xmin` as committed without consulting PostgreSQL `pg_xact` (`TransactionIdDidCommit`) leaks aborted rows.
2. **XID wraparound horizon invariant**: Normal XID comparison via `(a as i32).wrapping_sub(b as i32) < 0` handles 32-bit wraparound around `u32::MAX -> FIRST_NORMAL_XID (3)`, but only when `age(xmax, xmin) < 2^31`. Any snapshot constructed with synthetic/corrupt XIDs separated by `>= 2^31` flips comparison polarity.

## Reproduction
1. Construct an `MvccSnapshot::new(100, 200, vec![])` (with `committed_xids` empty or only tracking `[xmin, xmax)`).
2. Evaluate `is_tuple_visible` on a `HeapTupleHeaderMeta` with `t_xmin = 50` (aborted before `xmin`) and `t_infomask = 0` (no hint bits set).
3. Actual: `xid_is_visible(50)` returns `true` because `50 < xmin`. Expected in PostgreSQL: `TransactionIdDidCommit(50)` returns `false`, so the tuple is invisible.

## Suggested fix
- Under `#[cfg(feature = "pgrx")]`, consult `pg_sys::TransactionIdDidCommit` (or ensure the heap page reader sets/checks clog status) whenever neither `HEAP_XMIN_COMMITTED` nor `HEAP_XMIN_INVALID` is set, regardless of whether `t_xmin < xmin`.
- Add debug/runtime validation on `MvccSnapshot::new` / `from_pg_snapshot` verifying the circular distance from `xmin` to `xmax` is `< i32::MAX as u32`, and add wraparound boundary tests (`u32::MAX - 5` to `10`).
