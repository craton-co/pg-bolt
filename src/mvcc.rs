// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL MVCC tuple visibility and Visibility Map (`_vm`) helpers for
//! Phase 4 consistent snapshots and GPU predicate gating.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;

use crate::error::{PgBoltError, PgBoltResult};

/// PostgreSQL 32-bit transaction identifier (`TransactionId`).
pub type TransactionId = u32;

/// Invalid / unset transaction ID (`InvalidTransactionId`).
pub const INVALID_XID: TransactionId = 0;
/// Transaction ID used during database bootstrap (`BootstrapTransactionId`).
pub const BOOTSTRAP_XID: TransactionId = 1;
/// Permanently frozen transaction ID (`FrozenTransactionId`).
pub const FROZEN_XID: TransactionId = 2;
/// First assignable normal transaction ID (`FirstNormalTransactionId`).
pub const FIRST_NORMAL_XID: TransactionId = 3;

/// `t_infomask` flag: tuple contains at least one NULL attribute (`HEAP_HASNULL`).
pub const HEAP_HASNULL: u16 = 0x0001;
/// `t_infomask` flag: tuple contains at least one variable-width attribute (`HEAP_HASVARWIDTH`).
pub const HEAP_HASVARWIDTH: u16 = 0x0002;
/// `t_infomask` flag: tuple contains at least one external TOAST attribute (`HEAP_HASEXTERNAL`).
pub const HEAP_HASEXTERNAL: u16 = 0x0004;
/// `t_infomask` flag: `t_xmax` is a row-level key-share lock (`HEAP_XMAX_KEYSHR_LOCK`).
pub const HEAP_XMAX_KEYSHR_LOCK: u16 = 0x0010;
/// `t_infomask` flag: tuple has combo CID (`HEAP_COMBOCID`).
pub const HEAP_COMBOCID: u16 = 0x0020;
/// `t_infomask` flag: `t_xmax` is an exclusive lock (`HEAP_XMAX_EXCL_LOCK`).
pub const HEAP_XMAX_EXCL_LOCK: u16 = 0x0040;
/// `t_infomask` flag: `t_xmax` is a row-level lock only, not a delete/update (`HEAP_XMAX_LOCK_ONLY`).
pub const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080;
/// `t_infomask` flag: `t_xmax` is a share lock (`HEAP_XMAX_SHR_LOCK = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK`).
pub const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
/// `t_infomask` mask: all lock bits (`HEAP_LOCK_MASK`).
pub const HEAP_LOCK_MASK: u16 = HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
/// `t_infomask` flag: `t_xmin` is known committed (`HEAP_XMIN_COMMITTED`).
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
/// `t_infomask` flag: `t_xmin` is known invalid/aborted (`HEAP_XMIN_INVALID`).
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
/// `t_infomask` mask: tuple is frozen (`HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID`).
pub const HEAP_XMIN_FROZEN: u16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
/// `t_infomask` flag: `t_xmax` is known committed (`HEAP_XMAX_COMMITTED`).
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
/// `t_infomask` flag: `t_xmax` is known invalid/aborted (`HEAP_XMAX_INVALID`).
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
/// `t_infomask` flag: `t_xmax` holds a `MultiXactId` (`HEAP_XMAX_IS_MULTI`).
pub const HEAP_XMAX_IS_MULTI: u16 = 0x1000;
/// `t_infomask` flag: tuple was updated (`HEAP_UPDATED`).
pub const HEAP_UPDATED: u16 = 0x2000;

/// `t_infomask2` flag: mask for number of attributes (`HEAP_NATTS_MASK`).
pub const HEAP_NATTS_MASK: u16 = 0x07FF;
/// `t_infomask2` flag: keys were updated in tuple (`HEAP_KEYS_UPDATED`).
pub const HEAP_KEYS_UPDATED: u16 = 0x2000;
/// `t_infomask2` flag: tuple was HOT updated (`HEAP_HOT_UPDATED`).
pub const HEAP_HOT_UPDATED: u16 = 0x4000;
/// `t_infomask2` flag: tuple is heap-only tuple (`HEAP_ONLY_TUPLE`).
pub const HEAP_ONLY_TUPLE: u16 = 0x8000;

/// Visibility Map bit: every tuple on the heap page is visible to all active snapshots.
pub const VM_ALL_VISIBLE: u8 = 0x01;
/// Visibility Map bit: every tuple on the heap page is frozen (`HEAP_XMIN_FROZEN` / `FROZEN_XID`).
pub const VM_ALL_FROZEN: u8 = 0x02;

/// Returns `true` if `a` strictly precedes `b` under PostgreSQL wraparound-aware
/// normal XID modulo-$2^{32}$ arithmetic.
#[inline]
pub fn xid_precedes(a: TransactionId, b: TransactionId) -> bool {
    if a == b || a == INVALID_XID || b == INVALID_XID {
        return false;
    }
    if a < FIRST_NORMAL_XID || b < FIRST_NORMAL_XID {
        return a < b;
    }
    // Matches PostgreSQL `NormalTransactionIdPrecedes`: `(int32)(a - b) < 0`.
    (a.wrapping_sub(b) as i32) < 0
}

/// Extension callback for resolving PostgreSQL metadata that lives outside a
/// single heap tuple header (such as `pg_multixact` update XIDs).
pub trait MvccVisibilityExt {
    /// Resolves the updating `TransactionId` stored inside `multixact_id`, if any.
    ///
    /// Return values:
    /// - `Some(INVALID_XID)`: MultiXact was resolved and contains only row locks
    ///   (or an aborted updater), so it does not delete the tuple.
    /// - `Some(xid)`: the deleting/updating `TransactionId`.
    /// - `None`: MultiXact could not be resolved externally; fall back to
    ///   PostgreSQL `MultiXactIdGetUpdateXid` (under `pgrx`) or infomask flags.
    fn resolve_multixact_update_xid(
        &self,
        multixact_id: TransactionId,
        infomask: u16,
    ) -> Option<TransactionId>;
}

/// PostgreSQL MVCC snapshot metadata used to filter heap tuples without holding
/// open page pins across GPU kernel execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvccSnapshot {
    /// All XIDs strictly preceding `xmin` were already finished when the snapshot was taken.
    pub xmin: TransactionId,
    /// First unassigned XID; XIDs `>= xmax` started in the future relative to this snapshot.
    pub xmax: TransactionId,
    /// In-progress XIDs within `[xmin, xmax)` at snapshot time.
    pub active_xids: Vec<TransactionId>,
    /// Commit-log (`pg_xact`) oracle of committed XIDs for hint-bit-less tuples.
    pub committed_xids: HashSet<TransactionId>,
    /// Optional clog truncation / freeze horizon (`oldest_xid <= xmin`).
    oldest_xid: Option<TransactionId>,
    /// Current backend transaction XID (`GetCurrentTransactionIdIfAny`), if active.
    own_xid: Option<TransactionId>,
    /// Known subtransaction XIDs belonging to `own_xid` (pure-Rust testing / mirror).
    own_subxids: HashSet<TransactionId>,
    /// Command identifier (`curcid`) for same-transaction visibility checks.
    curcid: u32,
    /// Whether PGProc subtransaction cache overflowed (`SnapshotData.suboverflowed`).
    suboverflowed: bool,
    /// Optional `pg_subtrans` parent map (`child_xid -> parent_xid`) used when
    /// `suboverflowed` is `true` in non-`pgrx` builds.
    subtrans_parents: HashMap<TransactionId, TransactionId>,
}

impl MvccSnapshot {
    /// Constructs a snapshot from PostgreSQL `SnapshotData` bounds and active XID list.
    pub fn new(
        xmin: TransactionId,
        xmax: TransactionId,
        mut active_xids: Vec<TransactionId>,
    ) -> Self {
        active_xids.sort_unstable();
        active_xids.dedup();
        Self {
            xmin,
            xmax,
            active_xids,
            committed_xids: HashSet::new(),
            oldest_xid: None,
            own_xid: None,
            own_subxids: HashSet::new(),
            curcid: 0,
            suboverflowed: false,
            subtrans_parents: HashMap::new(),
        }
    }

    /// Constructs a snapshot and validates its XID bounds against modulo-$2^{32}$
    /// wraparound horizon limits (`xmax - xmin < i32::MAX`).
    pub fn try_new(
        xmin: TransactionId,
        xmax: TransactionId,
        active_xids: Vec<TransactionId>,
    ) -> PgBoltResult<Self> {
        let snap = Self::new(xmin, xmax, active_xids);
        snap.validate_horizon()?;
        Ok(snap)
    }

    /// Validates that `xmin`, `xmax`, `oldest_xid`, and `active_xids` form a coherent
    /// PostgreSQL MVCC snapshot spanning fewer than `i32::MAX` XIDs.
    pub fn validate_horizon(&self) -> PgBoltResult<()> {
        if self.xmin == INVALID_XID || self.xmax == INVALID_XID {
            return Err(PgBoltError::Mvcc(
                "MVCC snapshot bounds must not contain InvalidTransactionId (0)".into(),
            ));
        }
        if xid_precedes(self.xmax, self.xmin) {
            return Err(PgBoltError::Mvcc(format!(
                "MVCC snapshot xmax ({}) precedes xmin ({})",
                self.xmax, self.xmin
            )));
        }
        let eff_xmin = if self.xmin < FIRST_NORMAL_XID {
            FIRST_NORMAL_XID
        } else {
            self.xmin
        };
        if self.xmax >= FIRST_NORMAL_XID {
            let span = self.xmax.wrapping_sub(eff_xmin);
            if span >= i32::MAX as u32 {
                return Err(PgBoltError::Mvcc(format!(
                    "MVCC snapshot XID span ({span}) meets or exceeds wraparound horizon ({})",
                    i32::MAX
                )));
            }
        }
        if let Some(oldest) = self.oldest_xid {
            if oldest == INVALID_XID {
                return Err(PgBoltError::Mvcc(
                    "MVCC snapshot oldest_xid horizon must not be InvalidTransactionId (0)".into(),
                ));
            }
            if xid_precedes(self.xmin, oldest) {
                return Err(PgBoltError::Mvcc(format!(
                    "MVCC snapshot xmin ({}) precedes oldest_xid horizon ({oldest})",
                    self.xmin
                )));
            }
            let eff_oldest = if oldest < FIRST_NORMAL_XID {
                FIRST_NORMAL_XID
            } else {
                oldest
            };
            if self.xmax >= FIRST_NORMAL_XID {
                let oldest_span = self.xmax.wrapping_sub(eff_oldest);
                if oldest_span >= i32::MAX as u32 {
                    return Err(PgBoltError::Mvcc(format!(
                        "MVCC oldest_xid-to-xmax span ({oldest_span}) meets or exceeds wraparound horizon ({})",
                        i32::MAX
                    )));
                }
            }
        }
        for &xid in &self.active_xids {
            if xid == INVALID_XID {
                return Err(PgBoltError::Mvcc(
                    "MVCC active_xids must not contain InvalidTransactionId (0)".into(),
                ));
            }
            if xid_precedes(xid, self.xmin) {
                return Err(PgBoltError::Mvcc(format!(
                    "MVCC active XID ({xid}) precedes xmin ({})",
                    self.xmin
                )));
            }
            if !xid_precedes(xid, self.xmax) {
                return Err(PgBoltError::Mvcc(format!(
                    "MVCC active XID ({xid}) does not precede xmax ({})",
                    self.xmax
                )));
            }
        }
        Ok(())
    }

    /// Captures a PostgreSQL `Snapshot` pointer (plus active backend XID, if any)
    /// into an owned `MvccSnapshot` suitable for out-of-pin GPU filtering.
    ///
    /// # Safety
    ///
    /// `snapshot` must be a valid, non-null PostgreSQL `SnapshotData` pointer for
    /// the duration of this call.
    #[cfg(feature = "pgrx")]
    pub unsafe fn from_pg_snapshot(snapshot: pgrx::pg_sys::Snapshot) -> PgBoltResult<Self> {
        if snapshot.is_null() {
            return Err(PgBoltError::Mvcc("null PostgreSQL Snapshot pointer".into()));
        }
        // SAFETY: caller guarantees `snapshot` points to valid `SnapshotData`.
        let raw = &*snapshot;
        let mut active_xids = Vec::with_capacity(
            raw.xcnt as usize + usize::try_from(raw.subxcnt.max(0)).unwrap_or(0),
        );
        if raw.xcnt > 0 && !raw.xip.is_null() {
            // SAFETY: PostgreSQL guarantees `xip` holds `xcnt` entries when `xcnt > 0`.
            let xip = std::slice::from_raw_parts(raw.xip, raw.xcnt as usize);
            active_xids.extend(xip.iter().copied().map(u32::from));
        }
        if raw.subxcnt > 0 && !raw.subxip.is_null() {
            // SAFETY: PostgreSQL guarantees `subxip` holds `subxcnt` entries when `subxcnt > 0`.
            let subxip = std::slice::from_raw_parts(raw.subxip, raw.subxcnt as usize);
            active_xids.extend(subxip.iter().copied().map(u32::from));
        }

        // SAFETY: safe FFI query on backend state inside PostgreSQL process.
        let current_xid: u32 = pgrx::pg_sys::GetCurrentTransactionIdIfAny().into();
        let mut snap = Self::try_new(raw.xmin.into(), raw.xmax.into(), active_xids)?
            .with_suboverflowed(raw.suboverflowed);
        snap.curcid = raw.curcid;
        if current_xid != INVALID_XID {
            snap.own_xid = Some(current_xid);
        }
        Ok(snap)
    }

    /// Attaches committed XIDs from `pg_xact` / clog for evaluating tuples without hint bits.
    pub fn with_committed(mut self, committed: impl IntoIterator<Item = TransactionId>) -> Self {
        self.committed_xids.extend(committed);
        self
    }

    /// Configures the `oldest_xid` CLOG truncation / freeze horizon.
    ///
    /// When set, hint-bit-less XIDs strictly preceding `oldest_xid` are treated as
    /// frozen/committed, while hint-bit-less XIDs in `[oldest_xid, xmin)` must be
    /// verified in `pg_xact` (under `pgrx`) or `committed_xids` rather than assumed committed.
    pub fn with_oldest_xid(mut self, oldest_xid: TransactionId) -> Self {
        self.oldest_xid = Some(oldest_xid);
        self
    }

    /// Attaches the current transaction's top-level XID and current command ID (`curcid`)
    /// for intra-transaction visibility evaluation.
    pub fn with_own_xid(mut self, own_xid: TransactionId, curcid: u32) -> Self {
        self.own_xid = if own_xid == INVALID_XID {
            None
        } else {
            Some(own_xid)
        };
        self.curcid = curcid;
        self
    }

    /// Attaches known non-aborted subtransaction XIDs belonging to the current transaction.
    pub fn with_own_subxids(mut self, subxids: impl IntoIterator<Item = TransactionId>) -> Self {
        self.own_subxids
            .extend(subxids.into_iter().filter(|&xid| xid != INVALID_XID));
        self
    }

    /// Marks whether the snapshot's subtransaction array overflowed (`suboverflowed`).
    pub fn with_suboverflowed(mut self, suboverflowed: bool) -> Self {
        self.suboverflowed = suboverflowed;
        self
    }

    /// Supplies `pg_subtrans` parent mappings (`child_xid -> parent_xid`) for resolving
    /// overflowed subtransactions without PostgreSQL shared memory.
    pub fn with_subtrans_map(
        mut self,
        parents: impl IntoIterator<Item = (TransactionId, TransactionId)>,
    ) -> Self {
        self.subtrans_parents.extend(parents);
        self
    }

    /// Returns the configured `oldest_xid` horizon, if any.
    pub const fn oldest_xid(&self) -> Option<TransactionId> {
        self.oldest_xid
    }

    /// Returns the current transaction XID attached to this snapshot, if any.
    pub const fn own_xid(&self) -> Option<TransactionId> {
        self.own_xid
    }

    /// Returns the command ID (`curcid`) for same-transaction tuple visibility.
    pub const fn curcid(&self) -> u32 {
        self.curcid
    }

    /// Returns whether `SnapshotData.suboverflowed` is set.
    pub const fn suboverflowed(&self) -> bool {
        self.suboverflowed
    }

    #[inline]
    fn resolve_topmost_xid(&self, xid: TransactionId) -> TransactionId {
        if xid < FIRST_NORMAL_XID {
            return xid;
        }
        #[cfg(feature = "pgrx")]
        {
            if !self.subtrans_parents.contains_key(&xid) {
                // SAFETY: `xid >= FIRST_NORMAL_XID`; caller only invokes within `[xmin, xmax)`.
                let pg_top: u32 =
                    unsafe { pgrx::pg_sys::SubTransGetTopmostTransaction(xid.into()) }.into();
                if pg_top != INVALID_XID {
                    return pg_top;
                }
            }
        }
        let mut cur = xid;
        // Guard against synthetic cycles in test subtrans maps.
        for _ in 0..1024 {
            match self.subtrans_parents.get(&cur).copied() {
                Some(parent) if parent != INVALID_XID && parent != cur => cur = parent,
                _ => break,
            }
        }
        cur
    }

    #[inline]
    fn is_xid_in_snapshot(&self, xid: TransactionId) -> bool {
        if self.active_xids.binary_search(&xid).is_ok() {
            return true;
        }
        // Only subtransactions strictly within `[xmin, xmax)` can belong to an overflowed
        // active parent; calling `pg_subtrans` for `< xmin` risks truncated SLRU access.
        if self.suboverflowed && !xid_precedes(xid, self.xmin) && xid_precedes(xid, self.xmax) {
            let top = self.resolve_topmost_xid(xid);
            if top != xid && self.active_xids.binary_search(&top).is_ok() {
                return true;
            }
        }
        false
    }

    #[inline]
    fn is_own_xid(&self, xid: TransactionId) -> bool {
        if xid == INVALID_XID {
            return false;
        }
        if self.own_xid == Some(xid) || self.own_subxids.contains(&xid) {
            return true;
        }
        // Subtransactions always have XIDs >= their top-level parent's XID.
        if self.suboverflowed
            && self
                .own_xid
                .is_some_and(|own| !xid_precedes(xid, own) && self.resolve_topmost_xid(xid) == own)
        {
            return true;
        }
        #[cfg(feature = "pgrx")]
        {
            // SAFETY: FFI check against backend's active transaction stack.
            if unsafe { pgrx::pg_sys::TransactionIdIsCurrentTransactionId(xid.into()) } {
                return true;
            }
        }
        false
    }

    #[inline]
    fn clog_did_commit(&self, xid: TransactionId) -> bool {
        if self.committed_xids.contains(&xid) {
            return true;
        }
        #[cfg(feature = "pgrx")]
        {
            // SAFETY: `xid >= FIRST_NORMAL_XID` and not older than `oldest_xid` when checked.
            if unsafe { pgrx::pg_sys::TransactionIdDidCommit(xid.into()) } {
                return true;
            }
        }
        false
    }

    /// Returns `true` when `xid` was committed before this snapshot (handling special XIDs,
    /// `< xmin`, `>= xmax`, `active_xids`, `suboverflowed` top-parent lookup, and `pg_xact`).
    pub fn xid_is_visible(&self, xid: TransactionId) -> bool {
        match xid {
            INVALID_XID => false,
            BOOTSTRAP_XID | FROZEN_XID => true,
            _ => {
                if xid_precedes(xid, self.xmin) {
                    if let Some(oldest) = self.oldest_xid {
                        if xid_precedes(xid, oldest) {
                            // Predates `pg_xact` truncation horizon; already frozen before wraparound.
                            return true;
                        }
                        return self.clog_did_commit(xid);
                    }
                    #[cfg(feature = "pgrx")]
                    {
                        return self.clog_did_commit(xid);
                    }
                    #[cfg(not(feature = "pgrx"))]
                    {
                        return true;
                    }
                }
                if !xid_precedes(xid, self.xmax) {
                    return false;
                }
                if self.is_xid_in_snapshot(xid) {
                    return false;
                }
                self.clog_did_commit(xid)
            }
        }
    }

    /// Snapshot-horizon check for an XID whose commit hint bit (`HEAP_XMIN_COMMITTED` or
    /// `HEAP_XMAX_COMMITTED`) is already set in `t_infomask`.
    #[inline]
    fn xid_committed_and_visible(&self, xid: TransactionId) -> bool {
        match xid {
            INVALID_XID => false,
            BOOTSTRAP_XID | FROZEN_XID => true,
            _ => xid_precedes(xid, self.xmax) && !self.is_xid_in_snapshot(xid),
        }
    }
}

/// Decoded PostgreSQL `HeapTupleHeaderData` visibility and layout fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeapTupleHeaderMeta {
    pub t_xmin: TransactionId,
    pub t_xmax: TransactionId,
    pub t_infomask: u16,
    pub t_infomask2: u16,
    pub t_hoff: u8,
    pub natts: u16,
}

#[inline]
fn resolve_multixact_update_xid(
    hdr: &HeapTupleHeaderMeta,
    ext: Option<&dyn MvccVisibilityExt>,
) -> Option<TransactionId> {
    if let Some(ext) = ext {
        if let Some(resolved) = ext.resolve_multixact_update_xid(hdr.t_xmax, hdr.t_infomask) {
            return Some(resolved);
        }
    }
    #[cfg(feature = "pgrx")]
    {
        if (hdr.t_infomask & HEAP_XMAX_LOCK_ONLY) != 0 {
            return None;
        }
        let mut members: *mut pgrx::pg_sys::MultiXactMember = std::ptr::null_mut();
        // SAFETY: `t_xmax` is a `MultiXactId` when `HEAP_XMAX_IS_MULTI` is set.
        let count = unsafe {
            pgrx::pg_sys::GetMultiXactIdMembers(hdr.t_xmax.into(), &mut members, false, false)
        };
        if count <= 0 || members.is_null() {
            return None;
        }
        // SAFETY: `GetMultiXactIdMembers` returns `count` initialized `MultiXactMember` entries.
        let slice = unsafe { std::slice::from_raw_parts(members, count as usize) };
        let resolved = slice
            .iter()
            .find(|m| {
                matches!(
                    m.status,
                    pgrx::pg_sys::MultiXactStatus::MultiXactStatusUpdate
                        | pgrx::pg_sys::MultiXactStatus::MultiXactStatusNoKeyUpdate
                )
            })
            .map(|m| u32::from(m.xid));
        // SAFETY: `members` was allocated in PostgreSQL's current memory context.
        unsafe { pgrx::pg_sys::pfree(members.cast()) };
        resolved
    }
    #[cfg(not(feature = "pgrx"))]
    {
        None
    }
}

#[inline]
fn evaluate_xmax_visibility(
    hdr: &HeapTupleHeaderMeta,
    snapshot: &MvccSnapshot,
    cmax: Option<u32>,
    ext: Option<&dyn MvccVisibilityExt>,
) -> bool {
    if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0
        || hdr.t_xmax == INVALID_XID
        || (hdr.t_infomask & HEAP_XMAX_LOCK_ONLY) != 0
    {
        return true;
    }

    if (hdr.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        return match resolve_multixact_update_xid(hdr, ext) {
            Some(INVALID_XID) => true,
            Some(update_xid) => {
                if let Some(cmd) = cmax {
                    if snapshot.is_own_xid(update_xid) {
                        return cmd >= snapshot.curcid;
                    }
                }
                !snapshot.xid_is_visible(update_xid)
            }
            None => (hdr.t_infomask & HEAP_XMAX_COMMITTED) == 0,
        };
    }

    if (hdr.t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        return !snapshot.xid_committed_and_visible(hdr.t_xmax);
    }

    if let Some(cmd) = cmax {
        if snapshot.is_own_xid(hdr.t_xmax) {
            return cmd >= snapshot.curcid;
        }
    }
    !snapshot.xid_is_visible(hdr.t_xmax)
}

/// Evaluates tuple visibility following PostgreSQL's `HeapTupleSatisfiesMVCC` rules.
pub fn is_tuple_visible(hdr: &HeapTupleHeaderMeta, snapshot: &MvccSnapshot) -> bool {
    is_tuple_visible_with_multi(hdr, snapshot, None)
}

/// Evaluates tuple visibility with an optional external extension resolver (for example
/// `pg_multixact` update XID resolution when `HEAP_XMAX_IS_MULTI` is set).
pub fn is_tuple_visible_with_multi(
    hdr: &HeapTupleHeaderMeta,
    snapshot: &MvccSnapshot,
    ext: Option<&dyn MvccVisibilityExt>,
) -> bool {
    // Must check `HEAP_XMIN_FROZEN` before `HEAP_XMIN_INVALID` because `0x0300` sets both bits.
    if (hdr.t_infomask & HEAP_XMIN_FROZEN) != HEAP_XMIN_FROZEN {
        if (hdr.t_infomask & HEAP_XMIN_INVALID) != 0 {
            return false;
        }
        let xmin_visible = if (hdr.t_infomask & HEAP_XMIN_COMMITTED) != 0 {
            snapshot.xid_committed_and_visible(hdr.t_xmin)
        } else {
            snapshot.xid_is_visible(hdr.t_xmin)
        };
        if !xmin_visible {
            return false;
        }
    }

    evaluate_xmax_visibility(hdr, snapshot, None, ext)
}

/// Evaluates `HeapTupleSatisfiesMVCC` including intra-transaction command-ID (`cid`)
/// visibility for tuples inserted or modified by the current transaction (`own_xid`).
pub fn is_tuple_visible_in_tx(
    hdr: &HeapTupleHeaderMeta,
    cid: u32,
    snapshot: &MvccSnapshot,
) -> bool {
    is_tuple_visible_in_tx_ext(hdr, cid, cid, snapshot, None)
}

/// Evaluates `HeapTupleSatisfiesMVCC` with distinct `cmin`/`cmax` command IDs and an
/// optional MultiXact extension resolver.
pub fn is_tuple_visible_in_tx_ext(
    hdr: &HeapTupleHeaderMeta,
    cmin: u32,
    cmax: u32,
    snapshot: &MvccSnapshot,
    ext: Option<&dyn MvccVisibilityExt>,
) -> bool {
    // Must check `HEAP_XMIN_FROZEN` before `HEAP_XMIN_INVALID` because `0x0300` sets both bits.
    if (hdr.t_infomask & HEAP_XMIN_FROZEN) != HEAP_XMIN_FROZEN {
        if (hdr.t_infomask & HEAP_XMIN_INVALID) != 0 {
            return false;
        }
        if (hdr.t_infomask & HEAP_XMIN_COMMITTED) != 0 {
            if !snapshot.xid_committed_and_visible(hdr.t_xmin) {
                return false;
            }
        } else if snapshot.is_own_xid(hdr.t_xmin) {
            if cmin >= snapshot.curcid {
                return false;
            }
            if (hdr.t_infomask & HEAP_XMAX_INVALID) != 0 {
                return true;
            }
        } else if !snapshot.xid_is_visible(hdr.t_xmin) {
            return false;
        }
    }

    evaluate_xmax_visibility(hdr, snapshot, Some(cmax), ext)
}

/// RAII guard for a PostgreSQL `Buffer` content lock (`BUFFER_LOCK_SHARE`) that unlocks
/// and optionally releases the pin on drop.
#[cfg(feature = "pgrx")]
#[must_use = "dropping BufferLockGuard immediately unlocks the PostgreSQL buffer"]
pub struct BufferLockGuard {
    buf: pgrx::pg_sys::Buffer,
    release_on_drop: bool,
}

#[cfg(feature = "pgrx")]
impl BufferLockGuard {
    /// Acquires `BUFFER_LOCK_SHARE` on an already-pinned buffer without taking pin ownership.
    ///
    /// # Safety
    ///
    /// The caller must hold a valid pin on `buf` for the lifetime of this guard.
    pub unsafe fn lock_shared(buf: pgrx::pg_sys::Buffer) -> PgBoltResult<Self> {
        Self::lock_impl(buf, false)
    }

    /// Acquires `BUFFER_LOCK_SHARE` on `buf` and takes pin ownership so `Drop` calls
    /// both `LockBuffer(..., BUFFER_LOCK_UNLOCK)` and `ReleaseBuffer(buf)`.
    ///
    /// # Safety
    ///
    /// `buf` must be a valid pinned PostgreSQL buffer whose pin ownership transfers to the guard.
    pub unsafe fn lock_shared_owned(buf: pgrx::pg_sys::Buffer) -> PgBoltResult<Self> {
        Self::lock_impl(buf, true)
    }

    /// Reads `blkno` of `rel` and acquires `BUFFER_LOCK_SHARE`, releasing the pin on drop.
    ///
    /// # Safety
    ///
    /// `rel` must be an open, valid PostgreSQL `Relation`.
    pub unsafe fn read_shared(
        rel: pgrx::pg_sys::Relation,
        blkno: pgrx::pg_sys::BlockNumber,
    ) -> PgBoltResult<Self> {
        if rel.is_null() {
            return Err(PgBoltError::Mvcc(
                "cannot read heap block from null Relation pointer".into(),
            ));
        }
        // SAFETY: `rel` is verified non-null and open per caller contract.
        let buf = pgrx::pg_sys::ReadBuffer(rel, blkno);
        Self::lock_shared_owned(buf)
    }

    unsafe fn lock_impl(buf: pgrx::pg_sys::Buffer, release_on_drop: bool) -> PgBoltResult<Self> {
        if buf == 0 {
            return Err(PgBoltError::Mvcc(
                "cannot acquire BUFFER_LOCK_SHARE on InvalidBuffer".into(),
            ));
        }
        // SAFETY: caller guarantees `buf` is valid and pinned.
        pgrx::pg_sys::LockBuffer(buf, pgrx::pg_sys::BUFFER_LOCK_SHARE as std::ffi::c_int);
        Ok(Self {
            buf,
            release_on_drop,
        })
    }

    /// Returns the underlying PostgreSQL `Buffer` handle.
    pub const fn buffer(&self) -> pgrx::pg_sys::Buffer {
        self.buf
    }
}

#[cfg(feature = "pgrx")]
impl Drop for BufferLockGuard {
    fn drop(&mut self) {
        // SAFETY: guard owns the share lock (and pin when `release_on_drop` is true).
        unsafe {
            pgrx::pg_sys::LockBuffer(
                self.buf,
                pgrx::pg_sys::BUFFER_LOCK_UNLOCK as std::ffi::c_int,
            );
            if self.release_on_drop {
                pgrx::pg_sys::ReleaseBuffer(self.buf);
            }
        }
    }
}

/// Packed 2-bit-per-page PostgreSQL Visibility Map (`4` heap blocks per byte).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibilityMap {
    num_pages: usize,
    bytes: Vec<u8>,
}

impl VisibilityMap {
    /// Creates an empty Visibility Map covering `num_pages` heap pages.
    pub fn new(num_pages: usize) -> Self {
        Self {
            num_pages,
            bytes: vec![0; num_pages.div_ceil(4)],
        }
    }

    /// Parses raw PostgreSQL Visibility Map bytes for a relation of `num_pages` blocks.
    pub fn from_bytes(raw: &[u8], num_pages: usize) -> PgBoltResult<Self> {
        let needed = num_pages.div_ceil(4);
        if raw.len() < needed {
            return Err(PgBoltError::Mvcc(format!(
                "visibility map buffer too short: got {} bytes, need at least {} for {} pages",
                raw.len(),
                needed,
                num_pages
            )));
        }
        let mut bytes = raw[..needed].to_vec();
        let rem = num_pages % 4;
        if rem != 0 {
            if let Some(last) = bytes.last_mut() {
                let valid_bits = (1_u8 << (rem * 2)) - 1;
                *last &= valid_bits;
            }
        }
        Ok(Self { num_pages, bytes })
    }

    /// Returns the number of heap pages tracked by this Visibility Map.
    pub const fn num_pages(&self) -> usize {
        self.num_pages
    }

    /// Returns the packed underlying visibility-map byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Sets the `all_visible` and `all_frozen` bits for `block_no` (no-op if out of bounds).
    pub fn set_page_flags(&mut self, block_no: usize, all_visible: bool, all_frozen: bool) {
        if block_no >= self.num_pages {
            return;
        }
        let byte_idx = block_no >> 2;
        let shift = (block_no & 0x03) << 1;
        let mut flags = 0_u8;
        if all_visible {
            flags |= VM_ALL_VISIBLE;
        }
        if all_frozen {
            flags |= VM_ALL_FROZEN;
        }
        self.bytes[byte_idx] = (self.bytes[byte_idx] & !(0x03 << shift)) | (flags << shift);
    }

    /// Returns `(all_visible, all_frozen)` for `block_no` (`(false, false)` if out of bounds).
    pub fn page_flags(&self, block_no: usize) -> (bool, bool) {
        if block_no >= self.num_pages {
            return (false, false);
        }
        let byte_idx = block_no >> 2;
        let shift = (block_no & 0x03) << 1;
        let bits = (self.bytes[byte_idx] >> shift) & 0x03;
        ((bits & VM_ALL_VISIBLE) != 0, (bits & VM_ALL_FROZEN) != 0)
    }

    /// Returns `true` if `block_no` has `VM_ALL_VISIBLE` set.
    pub fn is_all_visible(&self, block_no: usize) -> bool {
        self.page_flags(block_no).0
    }

    /// Returns `true` if `block_no` has `VM_ALL_FROZEN` set.
    pub fn is_all_frozen(&self, block_no: usize) -> bool {
        self.page_flags(block_no).1
    }
}

/// Evaluates `headers` against `snapshot` into a non-null Arrow `BooleanArray` visibility bitmap.
pub fn build_visibility_bitmap(
    headers: &[HeapTupleHeaderMeta],
    snapshot: &MvccSnapshot,
) -> BooleanArray {
    let buffer = BooleanBuffer::collect_bool(headers.len(), |idx| {
        is_tuple_visible(&headers[idx], snapshot)
    });
    BooleanArray::new(buffer, None)
}

/// Evaluates `headers` against `snapshot` and optional MultiXact resolver into an Arrow `BooleanArray`.
pub fn build_visibility_bitmap_with_ext(
    headers: &[HeapTupleHeaderMeta],
    snapshot: &MvccSnapshot,
    ext: Option<&dyn MvccVisibilityExt>,
) -> BooleanArray {
    let buffer = BooleanBuffer::collect_bool(headers.len(), |idx| {
        is_tuple_visible_with_multi(&headers[idx], snapshot, ext)
    });
    BooleanArray::new(buffer, None)
}

/// Evaluates `headers` against `snapshot` with intra-transaction command-ID visibility.
pub fn build_visibility_bitmap_in_tx(
    headers: &[HeapTupleHeaderMeta],
    cid: u32,
    snapshot: &MvccSnapshot,
) -> BooleanArray {
    let buffer = BooleanBuffer::collect_bool(headers.len(), |idx| {
        is_tuple_visible_in_tx(&headers[idx], cid, snapshot)
    });
    BooleanArray::new(buffer, None)
}

/// Filters `batch` to only rows where `validity` is `true` using `arrow_select`.
pub fn filter_batch_by_visibility(
    batch: &RecordBatch,
    validity: &BooleanArray,
) -> PgBoltResult<RecordBatch> {
    if batch.num_rows() != validity.len() {
        return Err(PgBoltError::Mvcc(format!(
            "visibility bitmap length {} does not match RecordBatch row count {}",
            validity.len(),
            batch.num_rows()
        )));
    }
    filter_record_batch(batch, validity)
        .map_err(|e| PgBoltError::Mvcc(format!("failed to filter batch by visibility: {e}")))
}

/// Appends `validity` as boolean column `col_name` to `batch` for GPU predicate gating (`WHERE <col_name> = true`).
pub fn append_visibility_column(
    batch: &RecordBatch,
    validity: &BooleanArray,
    col_name: &str,
) -> PgBoltResult<RecordBatch> {
    if col_name.is_empty() {
        return Err(PgBoltError::Mvcc(
            "visibility column name cannot be empty".to_owned(),
        ));
    }
    if batch.num_rows() != validity.len() {
        return Err(PgBoltError::Mvcc(format!(
            "visibility bitmap length {} does not match RecordBatch row count {}",
            validity.len(),
            batch.num_rows()
        )));
    }
    if batch.schema().index_of(col_name).is_ok() {
        return Err(PgBoltError::Mvcc(format!(
            "RecordBatch already contains column '{col_name}'"
        )));
    }

    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len() + 1);
    fields.extend(schema.fields().iter().cloned());
    fields.push(Arc::new(Field::new(
        col_name,
        DataType::Boolean,
        validity.null_count() > 0,
    )));
    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
    columns.extend(batch.columns().iter().cloned());
    columns.push(Arc::new(validity.clone()));

    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| PgBoltError::Mvcc(format!("failed to append visibility column: {e}")))
}

#[cfg(all(test, not(feature = "pgrx")))]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};

    fn hdr(xmin: TransactionId, xmax: TransactionId, infomask: u16) -> HeapTupleHeaderMeta {
        HeapTupleHeaderMeta {
            t_xmin: xmin,
            t_xmax: xmax,
            t_infomask: infomask,
            t_infomask2: 1,
            t_hoff: 24,
            natts: 1,
        }
    }

    #[test]
    fn snapshot_handles_special_xids_bounds_active_and_clog() {
        let snap = MvccSnapshot::new(100, 200, vec![150, 175]).with_committed([120]);
        assert!(!snap.xid_is_visible(INVALID_XID));
        assert!(snap.xid_is_visible(BOOTSTRAP_XID));
        assert!(snap.xid_is_visible(FROZEN_XID));
        assert!(snap.xid_is_visible(50));
        assert!(snap.xid_is_visible(120));
        assert!(!snap.xid_is_visible(121));
        assert!(!snap.xid_is_visible(150));
        assert!(!snap.xid_is_visible(200));
        assert!(!snap.xid_is_visible(250));
    }

    #[test]
    fn snapshot_handles_xid_wraparound() {
        let near_max = u32::MAX - 5;
        let snap = MvccSnapshot::new(near_max, 15, vec![5]).with_committed([u32::MAX, 3, 10]);
        assert!(snap.xid_is_visible(u32::MAX - 20));
        assert!(snap.xid_is_visible(u32::MAX));
        assert!(snap.xid_is_visible(3));
        assert!(snap.xid_is_visible(10));
        assert!(!snap.xid_is_visible(5));
        assert!(!snap.xid_is_visible(15));
        assert!(!snap.xid_is_visible(25));
    }

    #[test]
    fn tuple_visibility_covers_frozen_aborted_locked_deleted_and_multi() {
        let snap = MvccSnapshot::new(100, 200, vec![150]).with_committed([130, 140]);

        // Frozen tuple stays visible regardless of t_xmin numeric value.
        assert!(is_tuple_visible(
            &hdr(500, INVALID_XID, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID),
            &snap
        ));
        // Aborted xmin is invisible.
        assert!(!is_tuple_visible(
            &hdr(50, INVALID_XID, HEAP_XMIN_INVALID),
            &snap
        ));
        // Hint-bit committed in [xmin, xmax) is visible even without clog entry.
        assert!(is_tuple_visible(
            &hdr(110, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            &snap
        ));
        // Active xmin even with hint bit set is invisible to our snapshot.
        assert!(!is_tuple_visible(
            &hdr(150, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            &snap
        ));
        // Hint-bit-less tuple uses committed_xids oracle.
        assert!(is_tuple_visible(&hdr(130, INVALID_XID, 0), &snap));
        assert!(!is_tuple_visible(&hdr(131, INVALID_XID, 0), &snap));

        // Row lock only (HEAP_XMAX_LOCK_ONLY) does not delete the tuple.
        assert!(is_tuple_visible(
            &hdr(
                50,
                60,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_COMMITTED
            ),
            &snap
        ));
        // Committed delete visible to snapshot hides the tuple.
        assert!(!is_tuple_visible(
            &hdr(50, 120, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
            &snap
        ));
        // Committed delete by an active or future deleter leaves the tuple visible.
        assert!(is_tuple_visible(
            &hdr(50, 150, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
            &snap
        ));
        assert!(is_tuple_visible(
            &hdr(50, 210, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
            &snap
        ));
        // Hint-bit-less delete resolved via clog oracle.
        assert!(!is_tuple_visible(&hdr(50, 140, HEAP_XMIN_COMMITTED), &snap));
        assert!(is_tuple_visible(&hdr(50, 141, HEAP_XMIN_COMMITTED), &snap));

        // MultiXact lock-only vs committed delete.
        assert!(is_tuple_visible(
            &hdr(
                50,
                9999,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY
            ),
            &snap
        ));
        assert!(!is_tuple_visible(
            &hdr(
                50,
                9999,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
            ),
            &snap
        ));
    }

    #[test]
    fn visibility_map_packs_two_bits_per_page() {
        let mut vm = VisibilityMap::new(6);
        assert_eq!(vm.as_bytes().len(), 2);
        vm.set_page_flags(0, true, false);
        vm.set_page_flags(3, true, true);
        vm.set_page_flags(5, false, true);
        vm.set_page_flags(99, true, true); // out-of-bounds ignored

        assert_eq!(vm.page_flags(0), (true, false));
        assert!(vm.is_all_visible(0));
        assert!(!vm.is_all_frozen(0));
        assert_eq!(vm.page_flags(3), (true, true));
        assert_eq!(vm.page_flags(4), (false, false));
        assert_eq!(vm.page_flags(5), (false, true));
        assert_eq!(vm.page_flags(99), (false, false));

        let roundtrip = VisibilityMap::from_bytes(vm.as_bytes(), 6).unwrap();
        assert_eq!(roundtrip, vm);
        assert!(VisibilityMap::from_bytes(&[0xFF], 6).is_err());
    }

    #[test]
    fn visibility_bitmap_filters_and_appends_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let snap = MvccSnapshot::new(100, 200, vec![150]);
        let headers = [
            hdr(50, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            hdr(150, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            hdr(50, 90, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
        ];
        let bitmap = build_visibility_bitmap(&headers, &snap);
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.value(0));
        assert!(!bitmap.value(1));
        assert!(!bitmap.value(2));

        let filtered = filter_batch_by_visibility(&batch, &bitmap).unwrap();
        assert_eq!(filtered.num_rows(), 1);
        let ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);

        let gated = append_visibility_column(&batch, &bitmap, "_pg_visible").unwrap();
        assert_eq!(gated.num_columns(), 3);
        assert_eq!(gated.num_rows(), 3);
        assert!(append_visibility_column(&batch, &bitmap, "id").is_err());
    }

    #[test]
    fn xid_precedes_antisymmetric_and_wraparound() {
        assert!(!xid_precedes(INVALID_XID, BOOTSTRAP_XID));
        assert!(!xid_precedes(BOOTSTRAP_XID, INVALID_XID));
        assert!(xid_precedes(BOOTSTRAP_XID, FROZEN_XID));
        assert!(!xid_precedes(FROZEN_XID, BOOTSTRAP_XID));
        assert!(xid_precedes(FROZEN_XID, FIRST_NORMAL_XID));
        assert!(!xid_precedes(FIRST_NORMAL_XID, FROZEN_XID));
        assert!(xid_precedes(u32::MAX - 2, FIRST_NORMAL_XID));
        assert!(!xid_precedes(FIRST_NORMAL_XID, u32::MAX - 2));
        assert!(!xid_precedes(42, 42));
    }

    #[test]
    fn snapshot_try_new_and_oldest_xid_clog_horizon() {
        assert!(MvccSnapshot::try_new(100, 200, vec![150]).is_ok());
        assert!(MvccSnapshot::try_new(0, 200, vec![]).is_err());
        assert!(MvccSnapshot::try_new(200, 100, vec![]).is_err());
        assert!(MvccSnapshot::try_new(10, 10 + (i32::MAX as u32), vec![]).is_err());
        assert!(MvccSnapshot::try_new(100, 200, vec![200]).is_err());

        // oldest_xid precedes xmin cleanly, and guards [oldest_xid, xmin) hint-bit-less clog checks.
        let snap = MvccSnapshot::try_new(100, 200, vec![])
            .unwrap()
            .with_oldest_xid(80)
            .with_committed([90]);
        assert!(snap.validate_horizon().is_ok());
        // Strictly older than oldest_xid (80): assumed frozen before truncation.
        assert!(snap.xid_is_visible(50));
        // Within [oldest_xid, xmin) without hint bit: requires clog/committed_xids confirmation.
        assert!(snap.xid_is_visible(90));
        assert!(!snap.xid_is_visible(91));

        // xmin preceding oldest_xid fails horizon validation.
        let bad_oldest = MvccSnapshot::new(100, 200, vec![]).with_oldest_xid(120);
        assert!(bad_oldest.validate_horizon().is_err());
    }

    struct MapMultiResolver(std::collections::HashMap<TransactionId, TransactionId>);

    impl MvccVisibilityExt for MapMultiResolver {
        fn resolve_multixact_update_xid(
            &self,
            multixact_id: TransactionId,
            _infomask: u16,
        ) -> Option<TransactionId> {
            self.0.get(&multixact_id).copied()
        }
    }

    #[test]
    fn multixact_resolver_distinguishes_lock_only_aborted_and_committed_updater() {
        let mut map = std::collections::HashMap::new();
        map.insert(7001, INVALID_XID); // lock-only multixact -> tuple remains visible
        map.insert(7002, 141); // uncommitted / aborted updater -> tuple remains visible
        map.insert(7003, 140); // committed updater -> tuple is deleted (not visible)
        let resolver = MapMultiResolver(map);

        let snap = MvccSnapshot::new(100, 200, vec![150]).with_committed([140]);
        assert!(is_tuple_visible_with_multi(
            &hdr(
                50,
                7001,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
            ),
            &snap,
            Some(&resolver),
        ));
        assert!(is_tuple_visible_with_multi(
            &hdr(
                50,
                7002,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
            ),
            &snap,
            Some(&resolver),
        ));
        assert!(!is_tuple_visible_with_multi(
            &hdr(
                50,
                7003,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
            ),
            &snap,
            Some(&resolver),
        ));
        // Unknown multi falls back cleanly.
        assert!(!is_tuple_visible_with_multi(
            &hdr(
                50,
                7999,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
            ),
            &snap,
            Some(&resolver),
        ));
    }

    #[test]
    fn own_xid_curcid_and_suboverflowed_visibility() {
        let snap = MvccSnapshot::try_new(100, 200, vec![150])
            .unwrap()
            .with_committed([120, 180])
            .with_own_xid(205, 5)
            .with_own_subxids([206])
            .with_suboverflowed(true)
            .with_subtrans_map([(180, 150), (207, 206), (206, 205)]);

        // Tuple inserted by own transaction before curcid is visible; at/after curcid is not.
        assert!(is_tuple_visible_in_tx(
            &hdr(205, INVALID_XID, HEAP_XMAX_INVALID),
            4,
            &snap
        ));
        assert!(!is_tuple_visible_in_tx(
            &hdr(205, INVALID_XID, HEAP_XMAX_INVALID),
            5,
            &snap
        ));
        // Own subtransaction resolved via overflowed subtrans chain.
        assert!(is_tuple_visible_in_tx(
            &hdr(207, INVALID_XID, HEAP_XMAX_INVALID),
            2,
            &snap
        ));
        // Tuple inserted at cid=2 and deleted by own tx at cmax=4 (< curcid=5) -> invisible.
        assert!(!is_tuple_visible_in_tx_ext(
            &hdr(205, 205, 0),
            2,
            4,
            &snap,
            None
        ));
        // Tuple inserted at cid=2 and deleted by own tx at cmax=6 (>= curcid=5) -> visible.
        assert!(is_tuple_visible_in_tx_ext(
            &hdr(205, 205, 0),
            2,
            6,
            &snap,
            None
        ));

        // Subtransaction 180 of active parent 150 must be hidden when suboverflowed is true.
        assert!(!is_tuple_visible(
            &hdr(180, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            &snap
        ));
        assert!(!snap.xid_is_visible(180));
    }

    #[test]
    fn suboverflowed_guards_out_of_horizon_subtrans_lookups() {
        // Synthetically map XIDs `< xmin` (80) and `< own_xid` (190) to active/own XIDs.
        // Because subtransactions always follow `xmin` and their top parent XID, the
        // `[xmin, xmax)` and `>= own_xid` horizon guards must ignore these mappings
        // (matching real `pg_subtrans` truncation safety under `pgrx`).
        let snap = MvccSnapshot::try_new(100, 200, vec![150])
            .unwrap()
            // Keep 190 uncommitted so visibility relies solely on the own_xid horizon guard.
            .with_committed([80])
            .with_own_xid(205, 5)
            .with_suboverflowed(true)
            .with_subtrans_map([(80, 150), (190, 205)]);

        assert!(is_tuple_visible(
            &hdr(80, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            &snap
        ));
        assert!(!snap.is_own_xid(190));
        assert!(!is_tuple_visible_in_tx(
            &hdr(190, INVALID_XID, HEAP_XMAX_INVALID),
            2,
            &snap
        ));
    }

    #[test]
    fn validate_horizon_boundary_checks_and_bitmap_helpers() {
        // active_xid preceding xmin is rejected
        assert!(MvccSnapshot::try_new(100, 200, vec![50]).is_err());
        assert!(MvccSnapshot::try_new(100, 200, vec![100]).is_ok());

        // xmin < FIRST_NORMAL_XID with xmax exceeding wraparound horizon is rejected
        assert!(
            MvccSnapshot::try_new(FROZEN_XID, FIRST_NORMAL_XID + (i32::MAX as u32), vec![])
                .is_err()
        );
        assert!(MvccSnapshot::try_new(FROZEN_XID, 100, vec![]).is_ok());

        // Test build_visibility_bitmap_with_ext
        let mut map = std::collections::HashMap::new();
        map.insert(9001, INVALID_XID); // lock-only -> visible
        map.insert(9002, 140); // committed updater -> deleted
        let resolver = MapMultiResolver(map);
        let snap = MvccSnapshot::new(100, 200, vec![150]).with_committed([140]);
        let headers = [
            hdr(
                50,
                9001,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED,
            ),
            hdr(
                50,
                9002,
                HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED,
            ),
        ];
        let bitmap_ext = build_visibility_bitmap_with_ext(&headers, &snap, Some(&resolver));
        assert_eq!(bitmap_ext.len(), 2);
        assert!(bitmap_ext.value(0));
        assert!(!bitmap_ext.value(1));

        // Test build_visibility_bitmap_in_tx
        let snap_tx = MvccSnapshot::try_new(100, 200, vec![])
            .unwrap()
            .with_own_xid(300, 3);
        let tx_headers = [
            hdr(300, INVALID_XID, HEAP_XMAX_INVALID), // cmin=2 < curcid=3 -> visible
            hdr(300, INVALID_XID, HEAP_XMAX_INVALID), // cmin=4 >= curcid=3 -> invisible
        ];
        let bitmap_tx_early = build_visibility_bitmap_in_tx(&tx_headers, 2, &snap_tx);
        assert!(bitmap_tx_early.value(0));
        assert!(bitmap_tx_early.value(1));
        let bitmap_tx_late = build_visibility_bitmap_in_tx(&tx_headers, 4, &snap_tx);
        assert!(!bitmap_tx_late.value(0));
        assert!(!bitmap_tx_late.value(1));
    }
}
