# W1-C-003: Missing own-transaction (`cmin`/`cmax`), `suboverflowed`, and buffer-lock atomicity in MVCC visibility

| Field | Value |
| :--- | :--- |
| Title | Missing own-transaction (`cmin`/`cmax`), `suboverflowed`, and buffer-lock atomicity in MVCC visibility |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/mvcc.rs`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:74,794` |

## Description
PostgreSQL's full `HeapTupleSatisfiesMVCC` depends on three runtime properties beyond `(xmin, xmax, active_xids)`:
1. **Current transaction & command ID (`TransactionIdIsCurrentTransactionId` / `t_cid`)**: Inside an open transaction (`BEGIN; INSERT INTO sales ...; SELECT ... FROM sales;`), uncommitted rows inserted by earlier commands in the *same* transaction have `t_xmin == current_xid` and `t_cid < snapshot.curcid` (and are invisible to `committed_xids`), while rows inserted by later commands (`t_cid >= snapshot.curcid`, e.g., inside a trigger/cursor) must remain invisible. `MvccSnapshot` does not yet carry `current_xid` / `curcid` or `HeapTupleHeaderGetCmin`/`Cmax`.
2. **Subtransaction array overflow (`snapshot->suboverflowed`)**: When a snapshot overflows PostgreSQL's `PGPROC_MAX_CACHED_SUBXIDS` (64 subtransactions), `subxip` is incomplete and `SubTransGetTopmostTransaction(xid)` must be consulted before testing `active_xids`.
3. **Concurrent hint-bit writes on heap pages**: Direct heap-page readers (Milestone 5, `PG_BOLT_PLAN_AND_CODE.md:794`) reading `t_infomask` without holding `BufferLock(BUFFER_LOCK_SHARE)` or using atomic loads can observe torn/stale hint bits concurrently written by another backend.

## Reproduction
1. Run `BEGIN; INSERT INTO t VALUES (...); SELECT * FROM t;` through a direct heap-page MVCC scan using `MvccSnapshot::new(xmin, xmax, active_xids)`.
2. Newly inserted rows with `t_xmin == own_xid` and no `HEAP_XMIN_COMMITTED` fail visibility (`false`).
3. Alternatively, open >64 `SAVEPOINT` subtransactions in a concurrent writer so `suboverflowed = true`, then check subtransaction XID membership via `active_xids` alone.

## Suggested fix
- Extend `MvccSnapshot` (or a `PgSnapshotContext` wrapper under `#[cfg(feature = "pgrx")]`) with optional `own_xids` / `curcid` and `suboverflowed` / `SubTransGetTopmostTransaction` fallback.
- Ensure any direct heap-page decoder acquires `LockBuffer(buf, BUFFER_LOCK_SHARE)` (or copies the page under share lock before unlocking) prior to extracting `HeapTupleHeaderMeta`.
