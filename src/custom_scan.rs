// SPDX-License-Identifier: Apache-2.0

//! Phase 3 & Milestone 4 PostgreSQL `CustomScan` plan, state machine, fallback, and EXPLAIN support.

use std::time::Instant;

use arrow_array::RecordBatch;
use craton_bolt::jit::ptx_cache_stats;
use craton_bolt::ExecutionTier;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::arrow_bridge::record_batch_to_json_rows;
use crate::bgworker::BgWorkerClient;
use crate::error::{PgBoltError, PgBoltResult};
use crate::runtime::BoltSessionRuntime;

mod execution_tier_serde {
    use super::*;

    pub fn serialize<S: Serializer>(
        tier: &ExecutionTier,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let label = match tier {
            ExecutionTier::Gpu => "Gpu",
            ExecutionTier::Hybrid => "Hybrid",
            ExecutionTier::Host => "Host",
        };
        serializer.serialize_str(label)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ExecutionTier, D::Error> {
        let label = String::deserialize(deserializer)?;
        match label.as_str() {
            "Gpu" => Ok(ExecutionTier::Gpu),
            "Hybrid" => Ok(ExecutionTier::Hybrid),
            "Host" => Ok(ExecutionTier::Host),
            other => Err(serde::de::Error::custom(format!(
                "unknown ExecutionTier '{other}'"
            ))),
        }
    }
}

/// Serialized plan metadata attached to a PostgreSQL `CustomScan` plan node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoltCustomScanPlan {
    pub sql: String,
    #[serde(with = "execution_tier_serde")]
    pub tier: ExecutionTier,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub fallback_on_oom: bool,
    pub timeout_ms: u64,
}

/// Runtime telemetry collected during `BoltCustomScanState` execution for `EXPLAIN ANALYZE`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BoltCustomScanStats {
    pub ptx_cache_hit: bool,
    pub execution_tier: String,
    pub kernel_time_us: u128,
    pub total_exec_us: u128,
    pub rows_produced: usize,
    pub batches_produced: usize,
    pub fell_back_to_cpu: bool,
    pub fallback_reason: Option<String>,
}

/// Execution transport for a `CustomScan` node: direct in-backend runtime or shared BGWorker IPC.
pub enum ScanExecutionBackend<'a> {
    Direct(&'a mut BoltSessionRuntime),
    BgWorker(&'a BgWorkerClient),
}

/// Stateful cursor and telemetry tracker backing PostgreSQL `CustomExecMethods`.
pub struct BoltCustomScanState {
    plan: BoltCustomScanPlan,
    cached_batch: Option<RecordBatch>,
    json_rows: Option<Vec<serde_json::Value>>,
    cursor: usize,
    stats: BoltCustomScanStats,
    executed: bool,
}

impl BoltCustomScanState {
    /// Initializes a new CustomScan execution state from `plan` (`BeginCustomScan`).
    pub fn begin(plan: BoltCustomScanPlan) -> Self {
        let tier_label = format!("{:?}", plan.tier);
        Self {
            plan,
            cached_batch: None,
            json_rows: None,
            cursor: 0,
            stats: BoltCustomScanStats {
                execution_tier: tier_label,
                ..Default::default()
            },
            executed: false,
        }
    }

    /// Returns the immutable plan descriptor associated with this scan node.
    pub fn plan(&self) -> &BoltCustomScanPlan {
        &self.plan
    }

    /// Returns a snapshot of execution telemetry gathered so far.
    pub fn stats(&self) -> &BoltCustomScanStats {
        &self.stats
    }

    /// Returns the currently materialized Arrow `RecordBatch`, if execution succeeded.
    pub fn cached_batch(&self) -> Option<&RecordBatch> {
        self.cached_batch.as_ref()
    }

    /// Executes the planned SQL against either `Direct` runtime or `BgWorker` IPC.
    ///
    /// Tracks PTX JIT cache hits via `craton_bolt::jit::ptx_cache_stats()` delta, stores the
    /// resulting `RecordBatch`, and updates latency and row-count telemetry.
    pub fn execute(
        &mut self,
        backend: ScanExecutionBackend<'_>,
    ) -> PgBoltResult<Option<&RecordBatch>> {
        crate::runtime::check_pg_cancel_pending()?;
        let timeout_ms = crate::runtime::effective_timeout_with_pg(self.plan.timeout_ms);

        let wall_start = Instant::now();
        let (hits_before, misses_before, _evictions_before) = ptx_cache_stats();

        let (batch, elapsed_us, tier) = match backend {
            ScanExecutionBackend::Direct(runtime) => {
                let res = runtime.execute_sql_with_timeout(&self.plan.sql, timeout_ms)?;
                (res.batch, res.elapsed_us, res.tier)
            }
            ScanExecutionBackend::BgWorker(client) => {
                client.execute_sql(&self.plan.sql, timeout_ms)?
            }
        };

        let (hits_after, misses_after, _evictions_after) = ptx_cache_stats();
        let total_exec_us = wall_start.elapsed().as_micros().max(elapsed_us);

        self.stats.ptx_cache_hit = hits_after > hits_before && misses_after == misses_before;
        self.stats.execution_tier = format!("{tier:?}");
        self.stats.kernel_time_us = elapsed_us;
        self.stats.total_exec_us = total_exec_us;
        self.stats.rows_produced = batch.num_rows();
        self.stats.batches_produced = 1;
        self.stats.fell_back_to_cpu = false;
        self.stats.fallback_reason = None;

        self.cursor = 0;
        self.json_rows = None;
        self.executed = true;
        self.cached_batch = Some(batch);

        Ok(self.cached_batch.as_ref())
    }

    /// Executes on GPU and falls back to `cpu_fallback` on `PgBoltError::VramExhausted` (or any
    /// GPU/VRAM failure when `plan.fallback_on_oom == true`).
    pub fn execute_with_cpu_fallback<F>(
        &mut self,
        backend: ScanExecutionBackend<'_>,
        cpu_fallback: F,
    ) -> PgBoltResult<RecordBatch>
    where
        F: FnOnce() -> PgBoltResult<RecordBatch>,
    {
        let wall_start = Instant::now();
        match self.execute(backend) {
            Ok(Some(batch)) => Ok(batch.clone()),
            Ok(None) => Err(PgBoltError::Bolt(
                "CustomScan backend completed without a RecordBatch".into(),
            )),
            Err(err) => {
                if !should_fallback_on_error(&err, self.plan.fallback_on_oom) {
                    return Err(err);
                }

                let reason = err.to_string();
                let cpu_batch = cpu_fallback()?;
                self.stats.ptx_cache_hit = false;
                self.stats.execution_tier = "HostFallback".to_string();
                self.stats.kernel_time_us = 0;
                self.stats.total_exec_us = wall_start.elapsed().as_micros();
                self.stats.rows_produced = cpu_batch.num_rows();
                self.stats.batches_produced = 1;
                self.stats.fell_back_to_cpu = true;
                self.stats.fallback_reason = Some(reason);

                self.cursor = 0;
                self.json_rows = None;
                self.executed = true;
                self.cached_batch = Some(cpu_batch.clone());
                Ok(cpu_batch)
            }
        }
    }

    #[cfg(any(test, feature = "pgrx"))]
    pub(crate) fn record_cpu_fallback_started(&mut self, reason: String) {
        self.stats.ptx_cache_hit = false;
        self.stats.execution_tier = "HostFallback".to_string();
        self.stats.kernel_time_us = 0;
        self.stats.total_exec_us = 0;
        self.stats.rows_produced = 0;
        self.stats.batches_produced = 0;
        self.stats.fell_back_to_cpu = true;
        self.stats.fallback_reason = Some(reason);
        self.cursor = 0;
        self.json_rows = None;
        self.executed = true;
        self.cached_batch = None;
    }

    #[cfg(any(test, feature = "pgrx"))]
    pub(crate) fn record_cpu_fallback_row(&mut self) {
        self.stats.rows_produced = self.stats.rows_produced.saturating_add(1);
        if self.stats.batches_produced == 0 {
            self.stats.batches_produced = 1;
        }
    }

    /// Returns the next JSON row object from the materialized batch, or `None` at EOF.
    pub fn next_row_json(&mut self) -> PgBoltResult<Option<serde_json::Value>> {
        let Some(batch) = self.cached_batch.as_ref() else {
            return Ok(None);
        };
        if self.json_rows.is_none() {
            self.json_rows = Some(record_batch_to_json_rows(batch)?);
        }
        let rows = self
            .json_rows
            .as_ref()
            .expect("json_rows initialized above");
        if self.cursor >= rows.len() {
            return Ok(None);
        }
        let row = rows[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(row))
    }

    /// Rewinds the row cursor to row 0 so `ReScanCustomScan` can replay cached rows cleanly.
    pub fn rescan(&mut self) {
        self.cursor = 0;
    }

    /// Finalizes the scan (`EndCustomScan`), releasing buffered batches and returning telemetry.
    pub fn end(&mut self) -> BoltCustomScanStats {
        self.cached_batch = None;
        self.json_rows = None;
        self.cursor = 0;
        self.stats.clone()
    }

    /// Formats PostgreSQL-compatible `EXPLAIN` / `EXPLAIN ANALYZE` lines for this scan node.
    pub fn explain(&self, analyze: bool) -> String {
        let cache_label = if !self.executed {
            "Pending"
        } else if self.stats.ptx_cache_hit {
            "Hit"
        } else {
            "Miss"
        };

        let mut lines = vec![
            format!(
                "Custom Scan (pg_bolt {})  (cost=10.00..{:.2} rows={} width=0)",
                self.stats.execution_tier, self.plan.estimated_cost, self.plan.estimated_rows
            ),
            format!("  Bolt Execution Tier: {}", self.stats.execution_tier),
            format!("  PTX Cache: {cache_label}"),
        ];

        if analyze {
            lines.push(format!(
                "  Kernel Time: {:.3} ms",
                (self.stats.kernel_time_us as f64) / 1000.0
            ));
            lines.push(format!(
                "  Total Exec Time: {:.3} ms",
                (self.stats.total_exec_us as f64) / 1000.0
            ));
            lines.push(format!("  Rows Emitted: {}", self.stats.rows_produced));
            lines.push(format!(
                "  Batches Emitted: {}",
                self.stats.batches_produced
            ));
        }

        if self.stats.fell_back_to_cpu {
            let reason = self
                .stats
                .fallback_reason
                .as_deref()
                .unwrap_or("unspecified fallback");
            lines.push(format!("  Fallback: PostgreSQL CPU ({reason})"));
        }

        lines.join("\n")
    }
}

fn is_vram_error(err: &PgBoltError) -> bool {
    matches!(err, PgBoltError::VramExhausted(_))
}

fn should_fallback_on_error(err: &PgBoltError, fallback_on_oom: bool) -> bool {
    is_vram_error(err) || (fallback_on_oom && is_gpu_execution_error(err))
}

fn is_gpu_execution_error(err: &PgBoltError) -> bool {
    match err {
        PgBoltError::VramExhausted(_) | PgBoltError::Ipc(_) | PgBoltError::Mvcc(_) => true,
        PgBoltError::Cancelled(_) => false,
        PgBoltError::Bolt(msg) => {
            let lower = msg.to_ascii_lowercase();
            lower.contains("vram")
                || lower.contains("oom")
                || lower.contains("out of memory")
                || lower.contains("cuda")
                || lower.contains("gpu")
                || lower.contains("ptx")
        }
        _ => false,
    }
}

#[cfg(feature = "pgrx")]
mod pgrx_custom_scan {
    use std::ffi::{c_char, c_int, c_void, CStr, CString};
    use std::mem::size_of;
    use std::ptr;

    use pgrx::pg_sys;
    use pgrx::prelude::pg_guard;

    use super::*;
    use crate::runtime::with_bolt_runtime;

    #[repr(transparent)]
    struct SyncCustomScanMethods(pg_sys::CustomScanMethods);
    unsafe impl Sync for SyncCustomScanMethods {}

    #[repr(transparent)]
    struct SyncCustomExecMethods(pg_sys::CustomExecMethods);
    unsafe impl Sync for SyncCustomExecMethods {}

    const fn make_custom_scan_methods() -> SyncCustomScanMethods {
        // SAFETY: All fields in `CustomScanMethods` are raw pointers or nullable fn pointers.
        let mut m: pg_sys::CustomScanMethods = unsafe { std::mem::zeroed() };
        m.CustomName = c"pg_bolt_gpu".as_ptr();
        m.CreateCustomScanState = Some(bolt_create_custom_scan_state);
        SyncCustomScanMethods(m)
    }

    const fn make_custom_exec_methods() -> SyncCustomExecMethods {
        // SAFETY: All fields in `CustomExecMethods` are raw pointers or nullable fn pointers.
        let mut m: pg_sys::CustomExecMethods = unsafe { std::mem::zeroed() };
        m.CustomName = c"pg_bolt_gpu".as_ptr();
        m.BeginCustomScan = Some(bolt_begin_custom_scan);
        m.ExecCustomScan = Some(bolt_exec_custom_scan);
        m.EndCustomScan = Some(bolt_end_custom_scan);
        m.ReScanCustomScan = Some(bolt_rescan_custom_scan);
        m.ExplainCustomScan = Some(bolt_explain_custom_scan);
        SyncCustomExecMethods(m)
    }

    static BOLT_CUSTOM_SCAN_METHODS: SyncCustomScanMethods = make_custom_scan_methods();
    static BOLT_CUSTOM_EXEC_METHODS: SyncCustomExecMethods = make_custom_exec_methods();

    pub(crate) fn custom_scan_methods_ptr() -> *const pg_sys::CustomScanMethods {
        &raw const BOLT_CUSTOM_SCAN_METHODS.0
    }

    #[repr(transparent)]
    struct SyncCustomPathMethods(pg_sys::CustomPathMethods);
    unsafe impl Sync for SyncCustomPathMethods {}

    const fn make_custom_path_methods() -> SyncCustomPathMethods {
        // SAFETY: All fields in `CustomPathMethods` are raw pointers or nullable fn pointers.
        let mut m: pg_sys::CustomPathMethods = unsafe { std::mem::zeroed() };
        m.CustomName = c"pg_bolt_gpu".as_ptr();
        m.PlanCustomPath = Some(bolt_plan_custom_path);
        SyncCustomPathMethods(m)
    }

    static BOLT_CUSTOM_PATH_METHODS: SyncCustomPathMethods = make_custom_path_methods();

    pub(crate) fn custom_path_methods_ptr() -> *const pg_sys::CustomPathMethods {
        &raw const BOLT_CUSTOM_PATH_METHODS.0
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_plan_custom_path(
        _root: *mut pg_sys::PlannerInfo,
        _rel: *mut pg_sys::RelOptInfo,
        best_path: *mut pg_sys::CustomPath,
        tlist: *mut pg_sys::List,
        clauses: *mut pg_sys::List,
        custom_plans: *mut pg_sys::List,
    ) -> *mut pg_sys::Plan {
        if best_path.is_null() {
            return ptr::null_mut();
        }
        let cscan = pg_sys::palloc0(size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
        (*(cscan as *mut pg_sys::Node)).type_ = pg_sys::NodeTag::T_CustomScan;
        (*cscan).scan.plan.startup_cost = (*best_path).path.startup_cost;
        (*cscan).scan.plan.total_cost = (*best_path).path.total_cost;
        (*cscan).scan.plan.plan_rows = (*best_path).path.rows;
        let cscan_tlist = crate::planner::build_non_resjunk_targetlist(tlist);
        let (final_tlist, scan_tlist) = if cscan_tlist.is_null() {
            (tlist, tlist)
        } else {
            let stl = crate::planner::build_scan_var_targetlist(cscan_tlist);
            (cscan_tlist, if stl.is_null() { cscan_tlist } else { stl })
        };
        (*cscan).scan.plan.targetlist = scan_tlist;
        (*cscan).scan.plan.qual = clauses;
        let parent = (*best_path).path.parent;
        (*cscan).scan.scanrelid = if parent.is_null() { 0 } else { (*parent).relid };
        (*cscan).custom_scan_tlist = final_tlist;
        (*cscan).custom_plans = custom_plans;
        (*cscan).custom_private = (*best_path).custom_private;
        (*cscan).methods = custom_scan_methods_ptr();
        cscan as *mut pg_sys::Plan
    }

    /// Allocates and initializes a `CustomPath` node for a base relation (`W1-D-006`).
    ///
    /// Exposes `craton-bolt` GPU scan paths to PostgreSQL's cost-based path evaluation during
    /// `set_rel_pathlist_hook` for mixed CPU/GPU query plans.
    #[allow(dead_code)]
    pub(crate) unsafe fn create_bolt_custom_path(
        rel: *mut pg_sys::RelOptInfo,
        custom_private: *mut pg_sys::List,
        startup_cost: f64,
        total_cost: f64,
    ) -> *mut pg_sys::CustomPath {
        if rel.is_null() {
            return ptr::null_mut();
        }
        let cpath = pg_sys::palloc0(size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
        (*(cpath as *mut pg_sys::Node)).type_ = pg_sys::NodeTag::T_CustomPath;
        (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
        (*cpath).path.parent = rel;
        (*cpath).path.pathtarget = (*rel).reltarget;
        (*cpath).path.startup_cost = startup_cost;
        (*cpath).path.total_cost = total_cost;
        (*cpath).path.rows = (*rel).rows;
        (*cpath).methods = custom_path_methods_ptr();
        (*cpath).custom_private = custom_private;
        cpath
    }

    struct MemoryContextGuard {
        prev: pg_sys::MemoryContext,
    }

    impl MemoryContextGuard {
        /// Switches `CurrentMemoryContext` to `target` when non-null and restores
        /// the previous context when dropped (including on `pgrx::error!` unwind).
        unsafe fn switch_to(target: pg_sys::MemoryContext) -> Option<Self> {
            if target.is_null() {
                return None;
            }
            let prev = pg_sys::CurrentMemoryContext;
            pg_sys::CurrentMemoryContext = target;
            Some(Self { prev })
        }
    }

    impl Drop for MemoryContextGuard {
        fn drop(&mut self) {
            unsafe {
                pg_sys::CurrentMemoryContext = self.prev;
            }
        }
    }

    #[repr(C)]
    struct PgBoltCustomScanNode {
        css: pg_sys::CustomScanState,
        rust_state: *mut BoltCustomScanState,
        reset_cb: pg_sys::MemoryContextCallback,
        reset_registered: bool,
        estate: *mut pg_sys::EState,
        eflags: c_int,
        fallback_plan: *mut pg_sys::Plan,
        fallback_state: *mut pg_sys::PlanState,
        fallback_active: bool,
        use_bgworker: bool,
        tuple_cxt: pg_sys::MemoryContext,
        slot_oids: Vec<pg_sys::Oid>,
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_memory_context_cleanup(arg: *mut c_void) {
        if arg.is_null() {
            return;
        }
        let bolt_node = arg as *mut PgBoltCustomScanNode;
        (*bolt_node).fallback_state = ptr::null_mut();
        (*bolt_node).fallback_active = false;
        // PG deletes child contexts of `es_query_cxt` before invoking reset callbacks,
        // so only clear the handle here to avoid double-deleting `tuple_cxt`.
        (*bolt_node).tuple_cxt = ptr::null_mut();
        drop(std::mem::take(&mut (*bolt_node).slot_oids));
        if !(*bolt_node).rust_state.is_null() {
            let mut state = Box::from_raw((*bolt_node).rust_state);
            (*bolt_node).rust_state = ptr::null_mut();
            let _ = state.end();
        }
    }

    unsafe fn extract_fallback_plan_from_cscan(
        cscan: *const pg_sys::CustomScan,
    ) -> *mut pg_sys::Plan {
        if cscan.is_null() {
            return ptr::null_mut();
        }
        let plans = (*cscan).custom_plans;
        if plans.is_null() || (*plans).length < 1 || (*plans).elements.is_null() {
            return ptr::null_mut();
        }
        let first_cell = *(*plans).elements;
        first_cell.ptr_value as *mut pg_sys::Plan
    }

    /// Returns `(attbyval, attlen)` metadata for column `idx` in `td`.
    #[inline]
    unsafe fn tuple_desc_attr_layout(td: pg_sys::TupleDesc, idx: usize) -> (bool, i16) {
        #[cfg(not(feature = "pg18"))]
        {
            let attr = &*(*td).attrs.as_ptr().add(idx);
            (attr.attbyval, attr.attlen)
        }
        #[cfg(feature = "pg18")]
        {
            let attr = &*(*td).compact_attrs.as_ptr().add(idx);
            (attr.attbyval, attr.attlen)
        }
    }

    /// Copies one slot attribute into the active memory context (`tuple_cxt`), cloning
    /// pass-by-reference values via `datumCopy` so buffer unpins or child resets cannot
    /// invalidate emitted virtual tuples.
    #[inline]
    unsafe fn copy_slot_datum(
        val: pg_sys::Datum,
        is_null: bool,
        dst_td: pg_sys::TupleDesc,
        dst_idx: usize,
    ) -> pg_sys::Datum {
        if is_null {
            return pg_sys::Datum::from(0usize);
        }
        let (attbyval, attlen) = tuple_desc_attr_layout(dst_td, dst_idx);
        if attbyval {
            val
        } else {
            pg_sys::datumCopy(val, attbyval, c_int::from(attlen))
        }
    }

    unsafe fn copy_non_resjunk_slot_attrs(
        src_slot: *mut pg_sys::TupleTableSlot,
        dst_slot: *mut pg_sys::TupleTableSlot,
        fallback_plan: *mut pg_sys::Plan,
        tuple_cxt: pg_sys::MemoryContext,
    ) -> PgBoltResult<()> {
        if src_slot.is_null() || dst_slot.is_null() {
            return Err(PgBoltError::Bridge(
                "CPU fallback TupleTableSlot pointer is null".into(),
            ));
        }
        let src_td = (*src_slot).tts_tupleDescriptor;
        let dst_td = (*dst_slot).tts_tupleDescriptor;
        if src_td.is_null() || dst_td.is_null() {
            return Err(PgBoltError::Bridge(
                "CPU fallback TupleTableSlot missing TupleDesc".into(),
            ));
        }

        if (*src_td).natts <= 0 || (*dst_td).natts <= 0 {
            return Err(PgBoltError::Bridge(
                "CPU fallback TupleTableSlot has empty TupleDesc".into(),
            ));
        }
        let src_natts = (*src_td).natts as usize;
        let dst_natts = (*dst_td).natts as usize;
        // Direct equivalent of PostgreSQL `slot_getallattrs(src_slot)` (`tuptable.h`):
        // force lazy buffer/heap/minimal slots to deform all columns before reading arrays.
        if c_int::from((*src_slot).tts_nvalid) < (*src_td).natts {
            pg_sys::slot_getsomeattrs_int(src_slot, (*src_td).natts);
        }
        if (*src_slot).tts_values.is_null()
            || (*src_slot).tts_isnull.is_null()
            || (*dst_slot).tts_values.is_null()
            || (*dst_slot).tts_isnull.is_null()
        {
            return Err(PgBoltError::Bridge(
                "CPU fallback TupleTableSlot missing attribute arrays".into(),
            ));
        }

        let src_values = std::slice::from_raw_parts((*src_slot).tts_values, src_natts);
        let src_isnull = std::slice::from_raw_parts((*src_slot).tts_isnull, src_natts);
        let dst_values = std::slice::from_raw_parts_mut((*dst_slot).tts_values, dst_natts);
        let dst_isnull = std::slice::from_raw_parts_mut((*dst_slot).tts_isnull, dst_natts);

        let _cxt_guard = MemoryContextGuard::switch_to(tuple_cxt);

        if src_natts == dst_natts {
            for dst_idx in 0..dst_natts {
                let is_null = src_isnull[dst_idx];
                dst_isnull[dst_idx] = is_null;
                dst_values[dst_idx] =
                    copy_slot_datum(src_values[dst_idx], is_null, dst_td, dst_idx);
            }
            return Ok(());
        }

        if fallback_plan.is_null() {
            return Err(PgBoltError::Bridge(format!(
                "fallback TupleDesc width {src_natts} != CustomScan width {dst_natts} without fallback Plan"
            )));
        }
        let tlist = (*fallback_plan).targetlist;
        if tlist.is_null() || (*tlist).elements.is_null() {
            return Err(PgBoltError::Bridge(
                "fallback Plan missing targetlist for resjunk projection".into(),
            ));
        }
        let tlist_len = usize::try_from((*tlist).length).unwrap_or(0);
        if tlist_len < src_natts {
            return Err(PgBoltError::Bridge(
                "fallback targetlist shorter than fallback TupleDesc".into(),
            ));
        }
        let cells = std::slice::from_raw_parts((*tlist).elements, src_natts);
        let mut dst_idx = 0usize;
        for (src_idx, cell) in cells.iter().enumerate() {
            let tle = cell.ptr_value as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).resjunk {
                continue;
            }
            let src_attr_idx = usize::try_from((*tle).resno)
                .map(|r| r.saturating_sub(1))
                .unwrap_or(src_idx);
            let attr_idx = if src_attr_idx < src_natts {
                src_attr_idx
            } else {
                src_idx
            };
            if dst_idx >= dst_natts {
                return Err(PgBoltError::Bridge(format!(
                    "CPU fallback non-resjunk columns exceed CustomScan width ({dst_natts})"
                )));
            }
            let is_null = src_isnull[attr_idx];
            dst_isnull[dst_idx] = is_null;
            dst_values[dst_idx] = copy_slot_datum(src_values[attr_idx], is_null, dst_td, dst_idx);
            dst_idx += 1;
        }
        if dst_idx != dst_natts {
            return Err(PgBoltError::Bridge(format!(
                "CPU fallback produced {dst_idx} non-resjunk columns, expected {dst_natts}"
            )));
        }
        Ok(())
    }

    unsafe fn decode_plan_from_cscan(
        cscan: *const pg_sys::CustomScan,
    ) -> PgBoltResult<BoltCustomScanPlan> {
        if cscan.is_null() {
            return Err(PgBoltError::Planner("null CustomScan pointer".into()));
        }
        let priv_list = (*cscan).custom_private;
        if priv_list.is_null() || (*priv_list).length < 1 || (*priv_list).elements.is_null() {
            return Err(PgBoltError::Planner(
                "CustomScan custom_private missing serialized BoltCustomScanPlan".into(),
            ));
        }
        let first_cell = *(*priv_list).elements;
        let str_node = first_cell.ptr_value as *mut pg_sys::String;
        if str_node.is_null() || (*str_node).sval.is_null() {
            return Err(PgBoltError::Planner(
                "CustomScan plan string node is null".into(),
            ));
        }
        let json = CStr::from_ptr((*str_node).sval)
            .to_str()
            .map_err(|e| PgBoltError::Planner(format!("invalid UTF-8 in CustomScan plan: {e}")))?;
        serde_json::from_str::<BoltCustomScanPlan>(json).map_err(|e| {
            PgBoltError::Planner(format!("failed to deserialize BoltCustomScanPlan: {e}"))
        })
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_create_custom_scan_state(
        _cscan: *mut pg_sys::CustomScan,
    ) -> *mut pg_sys::Node {
        let node = pg_sys::palloc0(size_of::<PgBoltCustomScanNode>()) as *mut PgBoltCustomScanNode;
        (*(node as *mut pg_sys::Node)).type_ = pg_sys::NodeTag::T_CustomScanState;
        (*node).css.methods = &raw const BOLT_CUSTOM_EXEC_METHODS.0;
        (*node).rust_state = ptr::null_mut();
        (*node).reset_registered = false;
        (*node).estate = ptr::null_mut();
        (*node).eflags = 0;
        (*node).fallback_plan = ptr::null_mut();
        (*node).fallback_state = ptr::null_mut();
        (*node).fallback_active = false;
        (*node).use_bgworker = false;
        (*node).tuple_cxt = ptr::null_mut();
        // SAFETY: `palloc0` leaves zeroed bytes; initialize `Vec` without reading/dropping zeroed state.
        ptr::write(&raw mut (*node).slot_oids, Vec::new());
        node as *mut pg_sys::Node
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_begin_custom_scan(
        node: *mut pg_sys::CustomScanState,
        estate: *mut pg_sys::EState,
        eflags: c_int,
    ) {
        if node.is_null() {
            return;
        }
        let bolt_node = node as *mut PgBoltCustomScanNode;
        let cscan = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
        let plan = match decode_plan_from_cscan(cscan) {
            Ok(plan) => plan,
            Err(err) => pgrx::error!("{err}"),
        };

        (*bolt_node).estate = estate;
        (*bolt_node).eflags = eflags;
        (*bolt_node).fallback_plan = extract_fallback_plan_from_cscan(cscan);
        (*bolt_node).fallback_state = ptr::null_mut();
        (*bolt_node).fallback_active = false;
        (*bolt_node).use_bgworker = crate::guc::current_config().use_bgworker;

        if (*bolt_node).tuple_cxt.is_null()
            && !estate.is_null()
            && !(*estate).es_query_cxt.is_null()
        {
            (*bolt_node).tuple_cxt = pg_sys::AllocSetContextCreateInternal(
                (*estate).es_query_cxt,
                c"pg_bolt_tuple_cxt".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as pg_sys::Size,
            );
        }

        let slot = (*node).ss.ss_ScanTupleSlot;
        if !slot.is_null() && !(*slot).tts_tupleDescriptor.is_null() {
            match crate::arrow_bridge::extract_tuple_desc_oids(slot) {
                Ok(oids) => (*bolt_node).slot_oids = oids,
                Err(err) => pgrx::error!("pg_bolt tuple descriptor error: {err}"),
            }
        }

        let boxed = Box::new(BoltCustomScanState::begin(plan));
        if !(*bolt_node).rust_state.is_null() {
            let mut prev = Box::from_raw((*bolt_node).rust_state);
            let _ = prev.end();
            (*bolt_node).rust_state = ptr::null_mut();
        }
        (*bolt_node).rust_state = Box::into_raw(boxed);

        if !(*bolt_node).reset_registered && !estate.is_null() && !(*estate).es_query_cxt.is_null()
        {
            (*bolt_node).reset_cb.func = Some(bolt_memory_context_cleanup);
            (*bolt_node).reset_cb.arg = bolt_node as *mut c_void;
            pg_sys::MemoryContextRegisterResetCallback(
                (*estate).es_query_cxt,
                &raw mut (*bolt_node).reset_cb,
            );
            (*bolt_node).reset_registered = true;
        }
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_exec_custom_scan(
        node: *mut pg_sys::CustomScanState,
    ) -> *mut pg_sys::TupleTableSlot {
        if node.is_null() {
            return ptr::null_mut();
        }
        let bolt_node = node as *mut PgBoltCustomScanNode;
        let slot = (*node).ss.ss_ScanTupleSlot;
        if slot.is_null() {
            return ptr::null_mut();
        }
        pg_sys::ExecClearTuple(slot);
        if !(*bolt_node).tuple_cxt.is_null() {
            pg_sys::MemoryContextReset((*bolt_node).tuple_cxt);
        }

        if (*bolt_node).rust_state.is_null() {
            return slot;
        }
        let state = &mut *(*bolt_node).rust_state;

        if !(*bolt_node).fallback_active && !state.executed {
            crate::pg_hooks::reconcile_pending_invalidations();
            let _ = crate::pg_hooks::refresh_dirty_referenced_tables(&state.plan.sql);

            let exec_res = if (*bolt_node).use_bgworker {
                crate::bgworker::shared_bgworker_client()
                    .and_then(|client| state.execute(ScanExecutionBackend::BgWorker(&client)))
                    .map(|_| ())
            } else {
                with_bolt_runtime(|rt| state.execute(ScanExecutionBackend::Direct(rt)).map(|_| ()))
            };
            if let Err(err) = exec_res {
                if should_fallback_on_error(&err, state.plan.fallback_on_oom)
                    && !(*bolt_node).fallback_plan.is_null()
                    && !(*bolt_node).estate.is_null()
                {
                    let fb_state = {
                        let _fb_cxt_guard =
                            MemoryContextGuard::switch_to((*(*bolt_node).estate).es_query_cxt);
                        pg_sys::ExecInitNode(
                            (*bolt_node).fallback_plan,
                            (*bolt_node).estate,
                            (*bolt_node).eflags,
                        )
                    };
                    if fb_state.is_null() {
                        pgrx::error!(
                            "pg_bolt CustomScan execution failed ({err}) and fallback subplan failed to initialize"
                        );
                    }
                    (*bolt_node).fallback_state = fb_state;
                    (*bolt_node).fallback_active = true;
                    state.record_cpu_fallback_started(err.to_string());
                } else {
                    pgrx::error!("pg_bolt CustomScan execution failed: {err}");
                }
            }
        }

        if (*bolt_node).fallback_active {
            let fb_state = (*bolt_node).fallback_state;
            if fb_state.is_null() {
                return slot;
            }
            if !(*fb_state).chgParam.is_null() {
                pg_sys::ExecReScan(fb_state);
            }
            let Some(exec_fn) = (*fb_state).ExecProcNode else {
                return slot;
            };
            let fb_slot = exec_fn(fb_state);
            if fb_slot.is_null() || ((*fb_slot).tts_flags & pg_sys::TTS_FLAG_EMPTY as u16) != 0 {
                return slot;
            }
            if let Err(err) = copy_non_resjunk_slot_attrs(
                fb_slot,
                slot,
                (*bolt_node).fallback_plan,
                (*bolt_node).tuple_cxt,
            ) {
                pgrx::error!("pg_bolt CPU fallback tuple conversion error: {err}");
            }
            state.record_cpu_fallback_row();
            pg_sys::ExecStoreVirtualTuple(slot);
            return slot;
        }

        let Some(batch) = state.cached_batch.as_ref() else {
            return slot;
        };
        if state.cursor >= batch.num_rows() {
            return slot;
        }

        let td = (*slot).tts_tupleDescriptor;
        if td.is_null() {
            pgrx::error!("pg_bolt tuple slot has null TupleDesc");
        }
        let natts_raw = (*td).natts;
        if natts_raw < 0 {
            pgrx::error!("pg_bolt tuple slot has negative natts: {natts_raw}");
        }
        let natts = natts_raw as usize;
        if batch.num_columns() != natts {
            pgrx::error!(
                "pg_bolt RecordBatch column count ({}) does not match TupleDesc width ({natts})",
                batch.num_columns()
            );
        }
        if (*bolt_node).slot_oids.len() != natts {
            match crate::arrow_bridge::extract_tuple_desc_oids(slot) {
                Ok(oids) => (*bolt_node).slot_oids = oids,
                Err(err) => pgrx::error!("pg_bolt tuple descriptor error: {err}"),
            }
        }

        let row_res = {
            let _cxt_guard = MemoryContextGuard::switch_to((*bolt_node).tuple_cxt);
            if natts == 0 {
                crate::arrow_bridge::write_record_batch_row_datums(
                    batch,
                    state.cursor,
                    &(*bolt_node).slot_oids,
                    &mut [],
                    &mut [],
                )
            } else if (*slot).tts_values.is_null() || (*slot).tts_isnull.is_null() {
                Err(crate::error::PgBoltError::Bridge(
                    "tuple slot values/isnull arrays are null".into(),
                ))
            } else {
                // SAFETY: PostgreSQL allocates `tts_values` and `tts_isnull` for `natts` attributes.
                let values = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
                let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);
                crate::arrow_bridge::write_record_batch_row_datums(
                    batch,
                    state.cursor,
                    &(*bolt_node).slot_oids,
                    values,
                    isnull,
                )
            }
        };
        if let Err(err) = row_res {
            pgrx::error!("pg_bolt tuple conversion error: {err}");
        }
        state.cursor += 1;
        pg_sys::ExecStoreVirtualTuple(slot);
        slot
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_end_custom_scan(node: *mut pg_sys::CustomScanState) {
        if node.is_null() {
            return;
        }
        let bolt_node = node as *mut PgBoltCustomScanNode;
        let slot = (*node).ss.ss_ScanTupleSlot;
        if !slot.is_null() {
            pg_sys::ExecClearTuple(slot);
        }
        if !(*bolt_node).fallback_state.is_null() {
            let fb_state = (*bolt_node).fallback_state;
            (*bolt_node).fallback_state = ptr::null_mut();
            (*bolt_node).fallback_active = false;
            pg_sys::ExecEndNode(fb_state);
        }
        if !(*bolt_node).rust_state.is_null() {
            let mut state = Box::from_raw((*bolt_node).rust_state);
            (*bolt_node).rust_state = ptr::null_mut();
            let _stats = state.end();
        }
        if !(*bolt_node).tuple_cxt.is_null() {
            let cxt = (*bolt_node).tuple_cxt;
            (*bolt_node).tuple_cxt = ptr::null_mut();
            pg_sys::MemoryContextDelete(cxt);
        }
        drop(std::mem::take(&mut (*bolt_node).slot_oids));
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_rescan_custom_scan(node: *mut pg_sys::CustomScanState) {
        if node.is_null() {
            return;
        }
        let bolt_node = node as *mut PgBoltCustomScanNode;
        let slot = (*node).ss.ss_ScanTupleSlot;
        if !slot.is_null() {
            pg_sys::ExecClearTuple(slot);
        }
        if !(*bolt_node).tuple_cxt.is_null() {
            pg_sys::MemoryContextReset((*bolt_node).tuple_cxt);
        }
        if (*bolt_node).fallback_active && !(*bolt_node).fallback_state.is_null() {
            pg_sys::ExecReScan((*bolt_node).fallback_state);
        }
        if !(*bolt_node).rust_state.is_null() {
            (*(*bolt_node).rust_state).rescan();
        }
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn bolt_explain_custom_scan(
        node: *mut pg_sys::CustomScanState,
        _ancestors: *mut pg_sys::List,
        es: *mut pg_sys::ExplainState,
    ) {
        if node.is_null() || es.is_null() {
            return;
        }
        let bolt_node = node as *mut PgBoltCustomScanNode;
        if (*bolt_node).rust_state.is_null() {
            return;
        }
        let state = &*(*bolt_node).rust_state;

        let cache_label = if !state.executed {
            "Pending"
        } else if state.stats.ptx_cache_hit {
            "Hit"
        } else {
            "Miss"
        };

        explain_text_prop(
            c"Bolt Execution Tier".as_ptr(),
            &state.stats.execution_tier,
            es,
        );
        explain_text_prop(c"PTX Cache".as_ptr(), cache_label, es);

        if (*es).analyze {
            let kernel_ms = (state.stats.kernel_time_us as f64) / 1000.0;
            let total_ms = (state.stats.total_exec_us as f64) / 1000.0;
            pg_sys::ExplainPropertyFloat(c"Kernel Time".as_ptr(), c"ms".as_ptr(), kernel_ms, 3, es);
            pg_sys::ExplainPropertyFloat(
                c"Total Exec Time".as_ptr(),
                c"ms".as_ptr(),
                total_ms,
                3,
                es,
            );
            pg_sys::ExplainPropertyInteger(
                c"Rows Emitted".as_ptr(),
                ptr::null(),
                state.stats.rows_produced as i64,
                es,
            );
        }

        if state.stats.fell_back_to_cpu {
            let reason = state
                .stats
                .fallback_reason
                .as_deref()
                .unwrap_or("CPU fallback");
            explain_text_prop(c"Fallback".as_ptr(), reason, es);
        }
    }

    unsafe fn explain_text_prop(label: *const c_char, value: &str, es: *mut pg_sys::ExplainState) {
        if let Ok(cval) = CString::new(value.replace('\0', " ")) {
            pg_sys::ExplainPropertyText(label, cval.as_ptr(), es);
        }
    }
}

#[cfg(feature = "pgrx")]
#[allow(unused_imports)]
pub(crate) use pgrx_custom_scan::{
    create_bolt_custom_path, custom_path_methods_ptr, custom_scan_methods_ptr,
};

#[cfg(all(test, not(feature = "pgrx")))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    #[cfg(feature = "cuda-stub")]
    use crate::guc::BoltConfig;
    #[cfg(feature = "cuda-stub")]
    use crate::runtime::{reset_bolt_runtime, test_process_lock};

    fn test_plan(sql: &str, fallback_on_oom: bool) -> BoltCustomScanPlan {
        BoltCustomScanPlan {
            sql: sql.to_string(),
            tier: ExecutionTier::Gpu,
            estimated_cost: 420.5,
            estimated_rows: 100_000,
            fallback_on_oom,
            timeout_ms: 5_000,
        }
    }

    fn make_batch(rows: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(rows.to_vec()))]).unwrap()
    }

    #[cfg(feature = "cuda-stub")]
    #[test]
    fn custom_scan_lifecycle_explain_and_vram_fallback() {
        let _lock = test_process_lock();
        reset_bolt_runtime();
        let mut rt = BoltSessionRuntime::new_with_config(&BoltConfig::default()).unwrap();

        let mut state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", true));
        let pre_explain = state.explain(false);
        assert!(pre_explain.contains("Custom Scan (pg_bolt Gpu)"));
        assert!(pre_explain.contains("PTX Cache: Pending"));

        // In cuda-stub mode, GPU execution returns a CUDA/GPU disabled error and falls back cleanly.
        let cpu_batch = state
            .execute_with_cpu_fallback(ScanExecutionBackend::Direct(&mut rt), || {
                Ok(make_batch(&[10, 20, 30]))
            })
            .unwrap();
        assert_eq!(cpu_batch.num_rows(), 3);
        assert!(state.stats().fell_back_to_cpu);
        assert!(state.stats().fallback_reason.is_some());
        assert!(state.explain(true).contains("Fallback: PostgreSQL CPU"));

        // When fallback_on_oom == false, non-VramExhausted GPU error propagates without fallback.
        let mut strict_state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", false));
        let mut cpu_called = false;
        let strict_res =
            strict_state.execute_with_cpu_fallback(ScanExecutionBackend::Direct(&mut rt), || {
                cpu_called = true;
                Ok(make_batch(&[1]))
            });
        assert!(strict_res.is_err());
        assert!(!cpu_called);

        reset_bolt_runtime();
    }

    #[test]
    fn custom_scan_cursor_rescan_and_explain_analyze() {
        let mut state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", true));
        state.cached_batch = Some(make_batch(&[7, 8]));
        state.executed = true;
        state.stats.ptx_cache_hit = true;
        state.stats.kernel_time_us = 1_250;
        state.stats.total_exec_us = 1_500;
        state.stats.rows_produced = 2;
        state.stats.batches_produced = 1;

        let analyze = state.explain(true);
        assert!(analyze.contains("Bolt Execution Tier: Gpu"));
        assert!(analyze.contains("PTX Cache: Hit"));
        assert!(analyze.contains("Kernel Time: 1.250 ms"));
        assert!(analyze.contains("Rows Emitted: 2"));

        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 7);
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 8);
        assert!(state.next_row_json().unwrap().is_none());

        state.rescan();
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 7);

        let end_stats = state.end();
        assert_eq!(end_stats.rows_produced, 2);
        assert!(state.cached_batch().is_none());
    }

    #[test]
    fn non_gpu_errors_are_not_masked_by_fallback() {
        let err = PgBoltError::Validation("bad sql".into());
        assert!(!should_fallback_on_error(&err, true));
        assert!(!should_fallback_on_error(&err, false));

        let oom = PgBoltError::VramExhausted("device out of memory".into());
        assert!(should_fallback_on_error(&oom, true));
        assert!(should_fallback_on_error(&oom, false));

        let gpu_err = PgBoltError::Bolt("CUDA kernel launch failed".into());
        assert!(should_fallback_on_error(&gpu_err, true));
        assert!(!should_fallback_on_error(&gpu_err, false));

        let cancelled = PgBoltError::Cancelled("statement timeout".into());
        assert!(!is_gpu_execution_error(&cancelled));
        assert!(!should_fallback_on_error(&cancelled, true));
        assert!(!should_fallback_on_error(&cancelled, false));

        let mvcc_err = PgBoltError::Mvcc("cached table is dirty and awaiting MVCC sync".into());
        assert!(is_gpu_execution_error(&mvcc_err));
        assert!(should_fallback_on_error(&mvcc_err, true));
        assert!(!should_fallback_on_error(&mvcc_err, false));
    }

    #[test]
    fn ipc_errors_fallback_when_allowed_and_propagate_when_disabled() {
        let ipc_err = PgBoltError::Ipc("shared background worker is not running".into());
        assert!(is_gpu_execution_error(&ipc_err));
        assert!(should_fallback_on_error(&ipc_err, true));
        assert!(!should_fallback_on_error(&ipc_err, false));

        let arena = Arc::new(
            crate::ipc::SharedMemoryArena::new(crate::ipc::IpcRingConfig {
                num_slots: 2,
                slot_bytes: 4096,
            })
            .unwrap(),
        );
        let client = BgWorkerClient::new(arena, Some(std::time::Duration::from_millis(5)));

        let mut fallback_plan = test_plan("SELECT id FROM sales", true);
        fallback_plan.timeout_ms = 5;
        let mut fallback_state = BoltCustomScanState::begin(fallback_plan);
        let batch = fallback_state
            .execute_with_cpu_fallback(ScanExecutionBackend::BgWorker(&client), || {
                Ok(make_batch(&[42]))
            })
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(fallback_state.stats().fell_back_to_cpu);

        let mut strict_plan = test_plan("SELECT id FROM sales", false);
        strict_plan.timeout_ms = 5;
        let mut strict_state = BoltCustomScanState::begin(strict_plan);
        let mut cpu_called = false;
        let strict_err = strict_state
            .execute_with_cpu_fallback(ScanExecutionBackend::BgWorker(&client), || {
                cpu_called = true;
                Ok(make_batch(&[1]))
            })
            .unwrap_err();
        assert!(matches!(strict_err, PgBoltError::Ipc(_)));
        assert!(!cpu_called);
    }

    #[test]
    fn cpu_fallback_row_telemetry_updates_explain_output() {
        let mut state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", true));
        state.record_cpu_fallback_started("VRAM exhausted during kernel run".into());
        state.record_cpu_fallback_row();
        state.record_cpu_fallback_row();

        assert!(state.stats().fell_back_to_cpu);
        assert_eq!(state.stats().execution_tier, "HostFallback");
        assert_eq!(state.stats().rows_produced, 2);
        assert_eq!(state.stats().batches_produced, 1);

        let explain = state.explain(true);
        assert!(explain.contains("Rows Emitted: 2"));
        assert!(explain.contains("Fallback: PostgreSQL CPU (VRAM exhausted during kernel run)"));
    }

    #[cfg(feature = "cuda-stub")]
    #[test]
    fn direct_execute_honors_zero_and_nonzero_timeout_paths() {
        let _lock = test_process_lock();
        reset_bolt_runtime();
        let mut rt = BoltSessionRuntime::new_with_config(&BoltConfig::default()).unwrap();

        let mut no_timeout = test_plan("SELECT id FROM sales", true);
        no_timeout.timeout_ms = 0;
        let mut state_zero = BoltCustomScanState::begin(no_timeout);
        let err_zero = state_zero
            .execute(ScanExecutionBackend::Direct(&mut rt))
            .unwrap_err();
        assert!(is_gpu_execution_error(&err_zero));

        let mut with_timeout = test_plan("SELECT id FROM sales", true);
        with_timeout.timeout_ms = 250;
        let mut state_timeout = BoltCustomScanState::begin(with_timeout);
        let err_timeout = state_timeout
            .execute(ScanExecutionBackend::Direct(&mut rt))
            .unwrap_err();
        assert!(is_gpu_execution_error(&err_timeout));

        reset_bolt_runtime();
    }

    #[test]
    fn custom_scan_plan_serde_roundtrip_and_resjunk_telemetry() {
        let plan = BoltCustomScanPlan {
            sql: "SELECT price FROM sales ORDER BY region_id".to_string(),
            tier: ExecutionTier::Hybrid,
            estimated_cost: 123.45,
            estimated_rows: 50_000,
            fallback_on_oom: true,
            timeout_ms: 10_000,
        };

        let json = serde_json::to_string(&plan).unwrap();
        let decoded: BoltCustomScanPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.sql, plan.sql);
        assert_eq!(decoded.tier, ExecutionTier::Hybrid);
        assert_eq!(decoded.estimated_cost, 123.45);
        assert_eq!(decoded.estimated_rows, 50_000);
        assert!(decoded.fallback_on_oom);
        assert_eq!(decoded.timeout_ms, 10_000);

        // Verify resjunk sort simulation: state tracks rows and telemetry
        let mut state = BoltCustomScanState::begin(decoded);
        state.record_cpu_fallback_started("Sort node resjunk projection fallback".to_string());
        for _ in 0..5 {
            state.record_cpu_fallback_row();
        }
        assert!(state.stats().fell_back_to_cpu);
        assert_eq!(state.stats().rows_produced, 5);
        assert_eq!(state.stats().batches_produced, 1);
        assert_eq!(state.stats().execution_tier, "HostFallback");
        assert!(state
            .explain(true)
            .contains("Fallback: PostgreSQL CPU (Sort node resjunk projection fallback)"));
    }

    #[test]
    fn custom_scan_rescan_replays_json_rows_and_cached_batch() {
        let mut state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", false));
        state.cached_batch = Some(make_batch(&[100, 200, 300]));
        state.executed = true;

        // First pass
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 100);
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 200);
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 300);
        assert!(state.next_row_json().unwrap().is_none());

        // Rescan should rewind cleanly
        state.rescan();
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 100);
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 200);
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 300);
        assert!(state.next_row_json().unwrap().is_none());

        // Multiple rescans without consuming to EOF
        state.rescan();
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 100);
        state.rescan();
        assert_eq!(state.next_row_json().unwrap().unwrap()["id"], 100);
    }

    #[test]
    fn custom_scan_end_cleans_up_all_cached_state_and_resets_cursor() {
        let mut state = BoltCustomScanState::begin(test_plan("SELECT id FROM sales", true));
        state.cached_batch = Some(make_batch(&[42]));
        state.executed = true;
        let _ = state.next_row_json().unwrap();

        let final_stats = state.end();
        assert!(state.cached_batch().is_none());
        assert!(state.next_row_json().unwrap().is_none());
        assert_eq!(final_stats.execution_tier, "Gpu");
    }
}
