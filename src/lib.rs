// SPDX-License-Identifier: Apache-2.0

//! `pg_bolt`: PostgreSQL extension integrating `craton-bolt` runtime PTX GPU SQL acceleration.

pub mod arrow_bridge;
pub mod bgworker;
pub mod custom_scan;
pub mod error;
pub mod guc;
pub mod ipc;
pub mod mvcc;
pub mod planner;
pub mod runtime;

pub use error::{PgBoltError, PgBoltResult};
#[cfg(feature = "pgrx")]
pub use guc::PG_BOLT_ENABLED;
pub use guc::{current_config, set_runtime_config, with_test_config, BoltConfig};
pub use runtime::{
    bump_catalog_epoch, current_catalog_epoch, quote_qualified_identifier, reset_bolt_runtime,
    test_process_lock, with_bolt_runtime, BoltExecutionResult, BoltRuntimeStatus,
    BoltSessionRuntime, CachedTableMeta, SessionTxFrame,
};

#[cfg(feature = "pgrx")]
pgrx::pg_module_magic!();

#[cfg(feature = "pgrx")]
#[pgrx::prelude::pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    ipc::init_shmem_hooks();
    // SAFETY: Reading `process_shared_preload_libraries_in_progress` and installing
    // backend hooks runs once in `_PG_init` prior to backend fork (W1-A-002).
    unsafe {
        if pgrx::pg_sys::process_shared_preload_libraries_in_progress {
            bgworker::register_bgworker();
        }
        planner::install_planner_hook();
        pg_hooks::install();
    }
}

#[inline]
#[allow(dead_code)]
fn is_sql_ident_boundary(ch: char) -> bool {
    !ch.is_alphanumeric() && ch != '_' && ch != '$'
}

#[allow(dead_code)]
fn match_ident_segment(sql: &str, seg: &str) -> bool {
    if seg.is_empty() {
        return false;
    }
    let seg_lower = seg.to_ascii_lowercase();
    let sql_lower = sql.to_ascii_lowercase();
    let mut search_from = 0;
    while let Some(pos) = sql_lower[search_from..].find(&seg_lower) {
        let abs_pos = search_from + pos;
        let end_pos = abs_pos + seg.len();
        search_from = end_pos;

        let prev_ok = if abs_pos == 0 {
            true
        } else {
            sql[..abs_pos]
                .chars()
                .next_back()
                .is_none_or(is_sql_ident_boundary)
        };
        let next_ok = if end_pos >= sql.len() {
            true
        } else {
            sql[end_pos..]
                .chars()
                .next()
                .is_none_or(is_sql_ident_boundary)
        };

        if prev_ok && next_ok {
            return true;
        }
    }
    false
}

/// Checks whether `sql` references `bolt_name` or its relation tail on discrete identifier boundaries.
#[allow(dead_code)]
pub(crate) fn sql_references_table(sql: &str, bolt_name: &str) -> bool {
    if match_ident_segment(sql, bolt_name) {
        return true;
    }
    if let Some((_, tail)) = bolt_name.rsplit_once('.') {
        if match_ident_segment(sql, tail) {
            return true;
        }
    }
    false
}

/// Pure-Rust control-plane entry points backing both SQL UDFs and standalone tests.
pub mod api {
    use arrow_array::RecordBatch;
    use craton_bolt::ExecutionTier;

    use crate::bgworker::{shared_bgworker_client, shutdown_shared_bgworker, BgWorkerClient};
    use crate::error::{PgBoltError, PgBoltResult};
    use crate::guc::current_config;
    use crate::runtime::{
        parse_qualified_segments, quote_qualified_identifier, reset_bolt_runtime,
        with_bolt_runtime, BoltExecutionResult, BoltRuntimeStatus, CachedTableMeta,
    };

    #[inline]
    fn use_bgworker_backend() -> bool {
        current_config().use_bgworker
    }

    fn client_tables(client: &BgWorkerClient) -> PgBoltResult<Vec<CachedTableMeta>> {
        let json = client.tables_json()?;
        serde_json::from_str::<Vec<CachedTableMeta>>(&json).map_err(|e| {
            PgBoltError::Ipc(format!(
                "failed to parse bgworker tables metadata JSON: {e}"
            ))
        })
    }

    fn find_client_table(
        client: &BgWorkerClient,
        bolt_name: &str,
    ) -> PgBoltResult<CachedTableMeta> {
        client_tables(client)?
            .into_iter()
            .find(|m| m.bolt_name == bolt_name)
            .ok_or_else(|| {
                PgBoltError::Ipc(format!(
                    "bgworker did not report metadata for table `{bolt_name}` after mutation"
                ))
            })
    }

    /// Checks whether `pg_bolt.enabled` is turned on.
    pub fn ensure_enabled() -> PgBoltResult<()> {
        if !current_config().enabled {
            return Err(PgBoltError::Disabled);
        }
        Ok(())
    }

    /// Resolves the target Bolt alias and constructs a SQL-injection-safe extraction query.
    pub fn prepare_extract(
        pg_relation: &str,
        bolt_alias: Option<&str>,
    ) -> PgBoltResult<(String, String)> {
        ensure_enabled()?;
        let quoted = quote_qualified_identifier(pg_relation)?;
        let alias = match bolt_alias {
            Some(a) if !a.trim().is_empty() => a.trim().to_string(),
            Some(_) => {
                return Err(PgBoltError::Validation(
                    "bolt_alias must not be empty when provided".into(),
                ))
            }
            None => pg_relation.trim().to_string(),
        };
        Ok((alias, format!("SELECT * FROM {quoted}")))
    }

    /// Registers or atomically refreshes a table snapshot in the active Bolt runtime (BGWorker or session).
    pub fn load_batch(
        pg_relation: &str,
        bolt_alias: Option<&str>,
        batch: RecordBatch,
    ) -> PgBoltResult<CachedTableMeta> {
        ensure_enabled()?;
        let _ = quote_qualified_identifier(pg_relation)?;
        let alias = match bolt_alias {
            Some(a) if !a.trim().is_empty() => a.trim(),
            Some(_) => {
                return Err(PgBoltError::Validation(
                    "bolt_alias must not be empty when provided".into(),
                ))
            }
            None => pg_relation.trim(),
        };
        let _ = parse_qualified_segments(alias)?;
        #[cfg(feature = "pgrx")]
        let schema = batch.schema();

        let meta = if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            let _ack = client.load_batch_chunked(pg_relation.trim(), alias, &batch)?;
            find_client_table(&client, alias)?
        } else {
            with_bolt_runtime(|rt| rt.register_batch(pg_relation.trim(), alias, batch))?
        };

        #[cfg(feature = "pgrx")]
        crate::planner::register_planner_table(meta.clone(), schema);

        Ok(meta)
    }

    /// Pins a table only if it is absent or marked dirty, invoking `extract` lazily on cache miss.
    pub fn cache_batch_if_needed(
        pg_relation: &str,
        bolt_alias: Option<&str>,
        extract: impl FnOnce(&str) -> PgBoltResult<RecordBatch>,
    ) -> PgBoltResult<CachedTableMeta> {
        let (alias, extract_sql) = prepare_extract(pg_relation, bolt_alias)?;
        let existing = if use_bgworker_backend() {
            tables()?
                .into_iter()
                .find(|m| m.bolt_name == alias && !m.dirty)
        } else {
            with_bolt_runtime(|rt| {
                Ok(rt
                    .get_table_meta(&alias)
                    .filter(|meta| !meta.dirty)
                    .cloned())
            })?
        };
        if let Some(hit) = existing {
            return Ok(hit);
        }
        let batch = extract(&extract_sql)?;
        load_batch(pg_relation, Some(&alias), batch)
    }

    /// Appends an incremental MVCC delta batch to an already-cached table.
    pub fn sync_delta_batch(bolt_name: &str, delta: RecordBatch) -> PgBoltResult<CachedTableMeta> {
        ensure_enabled()?;
        let _ = parse_qualified_segments(bolt_name)?;
        let trimmed = bolt_name.trim();
        #[cfg(feature = "pgrx")]
        let schema = delta.schema();

        let meta = if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            let _ack = client.append_delta_chunked(trimmed, &delta)?;
            find_client_table(&client, trimmed)?
        } else {
            with_bolt_runtime(|rt| rt.append_delta_batch(trimmed, delta))?
        };

        #[cfg(feature = "pgrx")]
        {
            crate::planner::register_planner_table(meta.clone(), schema);
            crate::pg_hooks::on_table_synced(&meta);
        }

        Ok(meta)
    }

    /// Evicts a cached table from Bolt VRAM and planner metadata.
    pub fn evict_table(bolt_name: &str) -> PgBoltResult<bool> {
        let trimmed = bolt_name.trim();
        if trimmed.is_empty() {
            return Err(PgBoltError::Validation(
                "bolt_name must not be empty".into(),
            ));
        }
        let evicted = if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            client.evict_table(trimmed)?
        } else {
            with_bolt_runtime(|rt| rt.evict_table(trimmed))?
        };
        #[cfg(feature = "pgrx")]
        crate::planner::unregister_planner_table(trimmed);
        Ok(evicted)
    }

    /// Lists all currently cached Bolt tables ordered by name.
    pub fn tables() -> PgBoltResult<Vec<CachedTableMeta>> {
        if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            let mut list = client_tables(&client)?;
            #[cfg(feature = "pgrx")]
            for m in &mut list {
                m.dirty = m.dirty || crate::pg_hooks::is_table_dirty(&m.bolt_name);
            }
            list.sort_by(|a, b| a.bolt_name.cmp(&b.bolt_name));
            Ok(list)
        } else {
            with_bolt_runtime(|rt| Ok(rt.list_tables()))
        }
    }

    /// Executes an analytical SQL query on Bolt and returns the resulting `RecordBatch` + metadata.
    pub fn query(sql: &str) -> PgBoltResult<BoltExecutionResult> {
        ensure_enabled()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }
        crate::runtime::check_pg_cancel_pending()?;
        let timeout_ms =
            crate::runtime::effective_timeout_with_pg(current_config().statement_timeout_ms);

        #[cfg(feature = "pgrx")]
        {
            let current_tables = tables()?;
            for t in current_tables {
                if t.dirty && super::sql_references_table(sql, &t.bolt_name) {
                    return Err(PgBoltError::Mvcc(format!(
                        "cached table `{}` is dirty and awaiting MVCC sync",
                        t.bolt_name
                    )));
                }
            }
        }

        if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            let (batch, elapsed_us, tier) = client.execute_sql(sql, timeout_ms)?;
            Ok(BoltExecutionResult {
                batch,
                elapsed_us,
                tier,
            })
        } else {
            with_bolt_runtime(|rt| rt.execute_sql_with_timeout(sql, timeout_ms))
        }
    }

    /// Plans `sql` against currently cached schemas and reports its `ExecutionTier`.
    pub fn explain_tier(sql: &str) -> PgBoltResult<ExecutionTier> {
        ensure_enabled()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }
        if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            client.explain_tier(sql)
        } else {
            with_bolt_runtime(|rt| rt.explain_tier(sql))
        }
    }

    /// Returns a logical and physical execution plan string for `sql`.
    pub fn explain_sql(sql: &str) -> PgBoltResult<String> {
        ensure_enabled()?;
        if sql.trim().is_empty() {
            return Err(PgBoltError::Validation(
                "SQL query must not be empty".into(),
            ));
        }
        if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            client.explain_sql(sql)
        } else {
            with_bolt_runtime(|rt| rt.explain_sql(sql))
        }
    }

    /// Returns a snapshot of runtime status, VRAM usage, PTX cache stats, and pool stats.
    pub fn status() -> PgBoltResult<BoltRuntimeStatus> {
        if use_bgworker_backend() {
            let client = shared_bgworker_client()?;
            let json = client.status_json()?;
            serde_json::from_str::<BoltRuntimeStatus>(&json)
                .map_err(|e| PgBoltError::Ipc(format!("failed to parse bgworker status JSON: {e}")))
        } else {
            with_bolt_runtime(|rt| Ok(rt.status()))
        }
    }

    /// Drops the active Bolt runtime and releases cached tables/CUDA resources.
    pub fn reset_vram() -> &'static str {
        shutdown_shared_bgworker();
        reset_bolt_runtime();
        #[cfg(feature = "pgrx")]
        {
            crate::planner::clear_planner_tables();
            crate::pg_hooks::reset_tracker();
        }
        "Craton Bolt CUDA runtime and VRAM cache released."
    }

    #[cfg(all(test, feature = "cuda-stub"))]
    mod tests {
        use std::sync::Arc;

        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        use super::*;
        use crate::guc::{with_test_config, BoltConfig};
        use crate::runtime::test_process_lock;

        fn make_batch(n: usize) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            let vals: Vec<i32> = (0..n as i32).collect();
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals))]).unwrap()
        }

        #[test]
        fn api_disabled_guc_blocks_execution_and_loading_but_allows_admin() {
            let _lock = test_process_lock();
            reset_vram();
            with_test_config(
                BoltConfig {
                    enabled: false,
                    ..BoltConfig::default()
                },
                || {
                    assert!(matches!(
                        load_batch("public.t", Some("t"), make_batch(4)).unwrap_err(),
                        PgBoltError::Disabled
                    ));
                    assert!(matches!(
                        query("SELECT 1").unwrap_err(),
                        PgBoltError::Disabled
                    ));
                    assert!(matches!(
                        explain_tier("SELECT 1").unwrap_err(),
                        PgBoltError::Disabled
                    ));
                    assert!(matches!(
                        explain_sql("SELECT 1").unwrap_err(),
                        PgBoltError::Disabled
                    ));
                    assert!(tables().is_ok());
                    assert!(status().unwrap().cuda_stub);
                    assert!(!evict_table("missing").unwrap());
                },
            );
            reset_vram();
        }

        #[test]
        fn api_cache_batch_if_needed_avoids_redundant_extraction() {
            let _lock = test_process_lock();
            reset_vram();
            let mut extract_calls = 0usize;
            let meta1 = cache_batch_if_needed("public.sales", Some("sales"), |sql| {
                assert_eq!(sql, "SELECT * FROM \"public\".\"sales\"");
                extract_calls += 1;
                Ok(make_batch(10))
            })
            .unwrap();
            assert_eq!(meta1.num_rows, 10);
            assert_eq!(extract_calls, 1);

            let meta2 = cache_batch_if_needed("public.sales", Some("sales"), |_| {
                extract_calls += 1;
                Ok(make_batch(99))
            })
            .unwrap();
            assert_eq!(meta2.num_rows, 10);
            assert_eq!(extract_calls, 1);

            let delta = sync_delta_batch("sales", make_batch(5)).unwrap();
            assert_eq!(delta.num_rows, 15);
            assert_eq!(tables().unwrap().len(), 1);
            assert!(evict_table("sales").unwrap());
            assert!(tables().unwrap().is_empty());
            reset_vram();
        }

        #[test]
        fn api_sync_delta_validates_bolt_name_and_supports_direct_and_bgworker_modes() {
            let _lock = test_process_lock();
            reset_vram();

            for bad in ["", "   ", "bad\0name", "public..sales"] {
                let err = sync_delta_batch(bad, make_batch(2)).unwrap_err();
                assert!(
                    matches!(err, PgBoltError::Validation(_)),
                    "expected Validation for {bad:?}, got {err:?}"
                );
            }

            for bad_sql in ["", "   "] {
                assert!(matches!(
                    explain_sql(bad_sql).unwrap_err(),
                    PgBoltError::Validation(_)
                ));
            }

            // Default mode uses bgworker IPC: explain_sql, explain_tier, dirty flags, delta sync,
            // and query all route cleanly through the worker runtime.
            let meta = load_batch("public.bg_sales", Some("bg_sales"), make_batch(6)).unwrap();
            assert_eq!(meta.num_rows, 6);
            assert!(!meta.dirty);
            let bg_plan = explain_sql("SELECT x FROM bg_sales").unwrap();
            assert!(bg_plan.contains("Logical plan:"));
            assert!(bg_plan.contains("Physical plan:"));
            assert_eq!(
                explain_tier("SELECT x * 2 AS doubled FROM bg_sales").unwrap(),
                ExecutionTier::Gpu
            );

            let client = shared_bgworker_client().unwrap();
            client.set_table_dirty("bg_sales", true).unwrap();
            assert!(tables().unwrap()[0].dirty);
            client.set_table_dirty("bg_sales", false).unwrap();
            assert!(!tables().unwrap()[0].dirty);

            let delta_meta = sync_delta_batch("bg_sales", make_batch(4)).unwrap();
            assert_eq!(delta_meta.num_rows, 10);
            assert_eq!(tables().unwrap()[0].num_rows, 10);

            assert!(matches!(
                query("SELECT COUNT(*) FROM bg_sales").unwrap_err(),
                PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")
            ));
            assert!(status().unwrap().cuda_stub);
            assert!(evict_table("bg_sales").unwrap());
            reset_vram();

            // Direct in-thread mode supports full explain_sql + explain_tier + query parity.
            let direct_cfg = BoltConfig {
                use_bgworker: false,
                ..BoltConfig::default()
            };
            with_test_config(direct_cfg, || {
                let direct_meta =
                    load_batch("public.direct_sales", Some("direct_sales"), make_batch(8)).unwrap();
                assert_eq!(direct_meta.num_rows, 8);
                let plan = explain_sql("SELECT x FROM direct_sales").unwrap();
                assert!(plan.contains("Logical plan:"));
                assert!(plan.contains("Physical plan:"));
                assert_eq!(
                    explain_tier("SELECT x * 2 AS doubled FROM direct_sales").unwrap(),
                    ExecutionTier::Gpu
                );
                assert!(matches!(
                    query("SELECT COUNT(*) FROM direct_sales").unwrap_err(),
                    PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")
                ));
                reset_vram();
            });
        }

        #[test]
        fn api_query_flags_dirty_table_and_sync_delta_clears_dirty() {
            let _lock = test_process_lock();
            reset_vram();

            let direct_cfg = BoltConfig {
                use_bgworker: false,
                ..BoltConfig::default()
            };
            with_test_config(direct_cfg, || {
                let meta =
                    load_batch("public.sync_sales", Some("sync_sales"), make_batch(5)).unwrap();
                assert_eq!(meta.num_rows, 5);
                assert!(!meta.dirty);

                // Mark dirty directly on runtime
                with_bolt_runtime(|rt| {
                    assert!(rt.mark_dirty("sync_sales"));
                    Ok(())
                })
                .unwrap();

                // Query against dirty table must be flagged with Mvcc error
                let err = query("SELECT COUNT(*) FROM sync_sales").unwrap_err();
                assert!(
                    matches!(err, PgBoltError::Mvcc(ref msg) if msg.contains("dirty")),
                    "expected Mvcc dirty error, got {err:?}"
                );

                // Incremental MVCC delta ingestion appends rows and clears dirty status
                let delta_meta = sync_delta_batch("sync_sales", make_batch(3)).unwrap();
                assert_eq!(delta_meta.num_rows, 8);
                assert!(!delta_meta.dirty);

                // Query no longer rejected by dirty flag (proceeds to engine execution)
                let query_after = query("SELECT COUNT(*) FROM sync_sales").unwrap_err();
                assert!(
                    matches!(query_after, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")),
                    "expected cuda-stub error after dirty cleared, got {query_after:?}"
                );

                reset_vram();
            });
        }

        #[test]
        fn api_runtime_transaction_abort_restores_clean_state() {
            let _lock = test_process_lock();
            reset_vram();

            let direct_cfg = BoltConfig {
                use_bgworker: false,
                ..BoltConfig::default()
            };
            with_test_config(direct_cfg, || {
                let meta = load_batch("public.tx_item", Some("tx_item"), make_batch(4)).unwrap();
                assert!(!meta.dirty);

                with_bolt_runtime(|rt| {
                    rt.begin_transaction(42);
                    rt.mark_dirty("tx_item");
                    assert!(rt.get_table_meta("tx_item").unwrap().dirty);

                    // Abort transaction
                    rt.abort_transaction(42).unwrap();
                    assert!(!rt.get_table_meta("tx_item").unwrap().dirty);
                    Ok(())
                })
                .unwrap();

                assert!(!tables().unwrap()[0].dirty);
                reset_vram();
            });
        }

        #[test]
        fn sql_references_table_boundary_matching() {
            // Discrete token matches
            assert!(super::super::sql_references_table(
                "SELECT * FROM sales",
                "sales"
            ));
            assert!(super::super::sql_references_table(
                "SELECT * FROM public.sales",
                "public.sales"
            ));
            assert!(super::super::sql_references_table(
                "SELECT * FROM sales",
                "public.sales"
            ));
            assert!(super::super::sql_references_table(
                "SELECT * FROM \"sales\"",
                "sales"
            ));
            assert!(super::super::sql_references_table(
                "SELECT * FROM (sales)",
                "sales"
            ));
            assert!(super::super::sql_references_table(
                "SELECT id, sales.x FROM sales",
                "sales"
            ));

            // Substring non-matches (ident chars before or after)
            assert!(!super::super::sql_references_table(
                "SELECT count(*) FROM orders",
                "s"
            ));
            assert!(!super::super::sql_references_table(
                "SELECT count(*) FROM sales_2026",
                "sales"
            ));
            assert!(!super::super::sql_references_table(
                "SELECT count(*) FROM mysales",
                "sales"
            ));
            assert!(!super::super::sql_references_table("SELECT 1", "t"));
        }
    }
}

/// PostgreSQL executor, utility DDL, transaction/subtransaction, and relcache hooks
/// keeping Bolt cache metadata, `PLANNER_CATALOG`, and MVCC state consistent with `pg_xact`.
#[cfg(feature = "pgrx")]
mod pg_hooks {
    use std::collections::{HashMap, HashSet};
    use std::ffi::{c_void, CString};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::LazyLock;

    use arrow_schema::SchemaRef;
    use parking_lot::Mutex;
    use pgrx::pg_sys;

    use crate::api;
    use crate::guc::current_config;
    use crate::planner;
    use crate::runtime::{parse_qualified_segments, with_bolt_runtime, CachedTableMeta};

    #[derive(Clone)]
    struct TrackedRelation {
        meta: CachedTableMeta,
        schema: SchemaRef,
        pg_oid: Option<pg_sys::Oid>,
        pg_oid_u32: u32,
        dirty: bool,
    }

    #[derive(Default)]
    struct TxFrame {
        subid: pg_sys::SubTransactionId,
        added_or_replaced: Vec<String>,
        dirtied: Vec<(String, bool)>,
        pending_evict_on_commit: HashSet<String>,
    }

    #[derive(Default)]
    struct TrackerState {
        tables: HashMap<String, TrackedRelation>,
        oid_to_bolts: HashMap<u32, HashSet<String>>,
        subxact_stack: Vec<TxFrame>,
        pending_relcache_flush_all: bool,
        pending_relcache_oids: HashSet<u32>,
    }

    static TRACKER: LazyLock<Mutex<TrackerState>> =
        LazyLock::new(|| Mutex::new(TrackerState::default()));
    static TRACKER_ACTIVE: AtomicBool = AtomicBool::new(false);
    static HOOKS_INSTALLED: AtomicBool = AtomicBool::new(false);

    static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
    static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

    /// Installs PostgreSQL executor, utility, transaction, subtransaction, and relcache hooks.
    pub(crate) unsafe fn install() {
        if HOOKS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
        pg_sys::ExecutorEnd_hook = Some(bolt_executor_end_hook);

        PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(bolt_process_utility_hook);

        pg_sys::RegisterXactCallback(Some(bolt_xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(bolt_subxact_callback), std::ptr::null_mut());
        pg_sys::CacheRegisterRelcacheCallback(
            Some(bolt_relcache_callback),
            pg_sys::Datum::from(0usize),
        );
    }

    fn refresh_active_flag(state: &TrackerState) {
        TRACKER_ACTIVE.store(!state.tables.is_empty(), Ordering::Release);
    }

    fn ensure_root_frame(stack: &mut Vec<TxFrame>) -> &mut TxFrame {
        if stack.is_empty() {
            stack.push(TxFrame {
                subid: 0,
                ..TxFrame::default()
            });
        }
        stack.last_mut().expect("subxact stack has root frame")
    }

    fn record_dirtied_in_frame(frame: &mut TxFrame, bolt_name: &str, prev_dirty: bool) {
        if !frame.dirtied.iter().any(|(name, _)| name == bolt_name) {
            frame.dirtied.push((bolt_name.to_string(), prev_dirty));
        }
    }

    /// Resolves a PostgreSQL relation specification to its `pg_class.oid`, if visible in catalogs.
    pub(crate) fn resolve_relation_oid(pg_relation: &str) -> Option<pg_sys::Oid> {
        // SAFETY: Querying transaction state and syscache C functions within PG backend.
        unsafe {
            if !pg_sys::IsTransactionState() {
                return None;
            }
            let segs = parse_qualified_segments(pg_relation).ok()?;
            let norm: Vec<String> = segs
                .into_iter()
                .map(|(val, quoted)| {
                    if quoted {
                        val
                    } else {
                        val.to_ascii_lowercase()
                    }
                })
                .collect();
            let oid = match norm.as_slice() {
                [rel] => {
                    let c_rel = CString::new(rel.as_str()).ok()?;
                    pg_sys::RelnameGetRelid(c_rel.as_ptr())
                }
                [schema, rel] | [_, schema, rel] => {
                    let c_schema = CString::new(schema.as_str()).ok()?;
                    let c_rel = CString::new(rel.as_str()).ok()?;
                    let ns_oid = pg_sys::get_namespace_oid(c_schema.as_ptr(), true);
                    if ns_oid == pg_sys::InvalidOid {
                        return None;
                    }
                    pg_sys::get_relname_relid(c_rel.as_ptr(), ns_oid)
                }
                _ => return None,
            };
            (oid != pg_sys::InvalidOid).then_some(oid)
        }
    }

    unsafe fn resolve_range_var_oid(rv: *const pg_sys::RangeVar) -> Option<pg_sys::Oid> {
        if rv.is_null() || !pg_sys::IsTransactionState() || (*rv).relname.is_null() {
            return None;
        }
        let oid = if (*rv).schemaname.is_null() {
            pg_sys::RelnameGetRelid((*rv).relname)
        } else {
            let ns_oid = pg_sys::get_namespace_oid((*rv).schemaname, true);
            if ns_oid == pg_sys::InvalidOid {
                return None;
            }
            pg_sys::get_relname_relid((*rv).relname, ns_oid)
        };
        (oid != pg_sys::InvalidOid).then_some(oid)
    }

    pub(crate) fn on_table_loaded(
        meta: CachedTableMeta,
        schema: SchemaRef,
        pg_oid: Option<pg_sys::Oid>,
    ) {
        let oid_u32 = pg_oid.map_or(0, |o| o.to_u32());
        planner::register_planner_table_with_oid(meta.clone(), schema.clone(), oid_u32);

        let mut state = TRACKER.lock();
        let bolt_name = meta.bolt_name.clone();
        if let Some(prev) = state.tables.get(&bolt_name) {
            if prev.pg_oid_u32 != 0 && prev.pg_oid_u32 != oid_u32 {
                if let Some(set) = state.oid_to_bolts.get_mut(&prev.pg_oid_u32) {
                    set.remove(&bolt_name);
                    if set.is_empty() {
                        state.oid_to_bolts.remove(&prev.pg_oid_u32);
                    }
                }
            }
        }
        state.tables.insert(
            bolt_name.clone(),
            TrackedRelation {
                meta,
                schema,
                pg_oid,
                pg_oid_u32: oid_u32,
                dirty: false,
            },
        );
        if oid_u32 != 0 {
            state
                .oid_to_bolts
                .entry(oid_u32)
                .or_default()
                .insert(bolt_name.clone());
        }
        // SAFETY: `IsTransactionState` only reads backend xact state.
        if unsafe { pg_sys::IsTransactionState() } {
            let frame = ensure_root_frame(&mut state.subxact_stack);
            if !frame.added_or_replaced.contains(&bolt_name) {
                frame.added_or_replaced.push(bolt_name.clone());
            }
            frame.pending_evict_on_commit.remove(&bolt_name);

            let snap = unsafe { pg_sys::GetActiveSnapshot() };
            if !snap.is_null() {
                let xmin = unsafe { (*snap).xmin };
                let xmax = unsafe { (*snap).xmax };
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| {
                        rt.set_table_snapshot(&bolt_name, xmin, xmax);
                        Ok(())
                    });
                }
            }
        }
        refresh_active_flag(&state);
    }

    pub(crate) fn bind_existing_table(meta: &CachedTableMeta, pg_oid: Option<pg_sys::Oid>) {
        let Some(oid) = pg_oid else {
            return;
        };
        let oid_u32 = oid.to_u32();
        if oid_u32 == 0 {
            return;
        }
        let mut state = TRACKER.lock();
        if let Some(entry) = state.tables.get_mut(&meta.bolt_name) {
            entry.pg_oid = Some(oid);
            entry.pg_oid_u32 = oid_u32;
            let schema = entry.schema.clone();
            state
                .oid_to_bolts
                .entry(oid_u32)
                .or_default()
                .insert(meta.bolt_name.clone());
            drop(state);
            planner::register_planner_table_with_oid(meta.clone(), schema, oid_u32);
        }
    }

    pub(crate) fn on_table_evicted(bolt_name: &str) {
        let mut state = TRACKER.lock();
        if let Some(removed) = state.tables.remove(bolt_name) {
            if removed.pg_oid_u32 != 0 {
                if let Some(set) = state.oid_to_bolts.get_mut(&removed.pg_oid_u32) {
                    set.remove(bolt_name);
                    if set.is_empty() {
                        state.oid_to_bolts.remove(&removed.pg_oid_u32);
                    }
                }
            }
        }
        for frame in &mut state.subxact_stack {
            frame.pending_evict_on_commit.remove(bolt_name);
        }
        refresh_active_flag(&state);
    }

    pub(crate) fn reset_tracker() {
        let mut state = TRACKER.lock();
        state.tables.clear();
        state.oid_to_bolts.clear();
        state.subxact_stack.clear();
        state.pending_relcache_flush_all = false;
        state.pending_relcache_oids.clear();
        refresh_active_flag(&state);
    }

    pub(crate) fn is_table_dirty(bolt_name: &str) -> bool {
        let state = TRACKER.lock();
        state.tables.get(bolt_name).is_some_and(|t| t.dirty)
    }

    pub(crate) fn on_table_synced(meta: &CachedTableMeta) {
        let mut state = TRACKER.lock();
        if let Some(entry) = state.tables.get_mut(&meta.bolt_name) {
            entry.dirty = meta.dirty;
            entry.meta = meta.clone();
        }
    }

    fn sync_local_dirty_state(
        bolt_name: &str,
        mut meta: CachedTableMeta,
        schema: SchemaRef,
        oid_u32: u32,
        dirty: bool,
    ) {
        meta.dirty = dirty;
        if dirty && oid_u32 != 0 {
            planner::invalidate_relation_oid(oid_u32);
        }
        planner::register_planner_table_with_oid(meta, schema, oid_u32);
        if !current_config().use_bgworker {
            let _ = with_bolt_runtime(|rt| {
                if dirty {
                    rt.mark_dirty(bolt_name);
                } else {
                    rt.clear_dirty(bolt_name);
                }
                Ok(())
            });
        }
    }

    fn broadcast_bgworker_dirty(bolt_name: &str) {
        if current_config().use_bgworker {
            if let Ok(client) = crate::bgworker::shared_bgworker_client() {
                let _ = client.set_table_dirty(bolt_name, true);
            }
        }
    }

    fn mark_oid_dirty_in_tx(oid: pg_sys::Oid, evict_on_commit: bool) {
        let oid_u32 = oid.to_u32();
        if oid_u32 == 0 {
            return;
        }
        let mut sync_updates = Vec::new();
        {
            let mut state = TRACKER.lock();
            let Some(names) = state.oid_to_bolts.get(&oid_u32).cloned() else {
                return;
            };
            for bolt_name in names {
                let Some(entry) = state.tables.get_mut(&bolt_name) else {
                    continue;
                };
                let prev_dirty = entry.dirty;
                entry.dirty = true;
                entry.meta.dirty = true;
                let meta_snapshot = entry.meta.clone();
                let schema_snapshot = entry.schema.clone();
                let frame = ensure_root_frame(&mut state.subxact_stack);
                record_dirtied_in_frame(frame, &bolt_name, prev_dirty);
                if evict_on_commit {
                    frame.pending_evict_on_commit.insert(bolt_name.clone());
                }
                sync_updates.push((bolt_name, meta_snapshot, schema_snapshot));
            }
        }
        for (bolt_name, meta, schema) in sync_updates {
            sync_local_dirty_state(&bolt_name, meta, schema, oid_u32, true);
        }
    }

    pub(crate) fn reconcile_pending_invalidations() {
        if !TRACKER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        // SAFETY: `IsTransactionState` reads backend transaction flag safely.
        if !unsafe { pg_sys::IsTransactionState() } {
            return;
        }
        let candidates: Vec<(String, pg_sys::Oid, u32)> = {
            let mut state = TRACKER.lock();
            if !state.pending_relcache_flush_all && state.pending_relcache_oids.is_empty() {
                return;
            }
            let flush_all = state.pending_relcache_flush_all;
            let oid_filter: HashSet<u32> = std::mem::take(&mut state.pending_relcache_oids);
            state.pending_relcache_flush_all = false;

            state
                .tables
                .iter()
                .filter_map(|(bolt_name, entry)| {
                    let oid = entry.pg_oid?;
                    if flush_all || oid_filter.contains(&entry.pg_oid_u32) {
                        Some((bolt_name.clone(), oid, entry.pg_oid_u32))
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut to_evict = Vec::new();
        let mut to_mark_dirty = Vec::new();
        for (bolt_name, pg_oid, _oid_u32) in candidates {
            // SAFETY: Inside active transaction state; `get_rel_name` returns palloc'd name or NULL if dropped.
            let name_ptr = unsafe { pg_sys::get_rel_name(pg_oid) };
            if name_ptr.is_null() {
                to_evict.push(bolt_name);
            } else {
                unsafe { pg_sys::pfree(name_ptr.cast()) };
                to_mark_dirty.push(pg_oid);
            }
        }
        for bolt_name in to_evict {
            let _ = api::evict_table(&bolt_name);
            on_table_evicted(&bolt_name);
        }
        for pg_oid in to_mark_dirty {
            mark_oid_dirty_in_tx(pg_oid, false);
        }
    }

    /// Re-extracts and refreshes a table from PostgreSQL if currently marked dirty.
    pub(crate) fn refresh_table_if_dirty(bolt_name: &str) -> PgBoltResult<bool> {
        let entry = {
            let state = TRACKER.lock();
            state.tables.get(bolt_name).cloned()
        };
        let Some(entry) = entry else {
            return Ok(false);
        };
        if !entry.dirty && !is_table_dirty(bolt_name) {
            return Ok(false);
        }
        let (alias, extract_sql) =
            api::prepare_extract(&entry.meta.pg_relation, Some(&entry.meta.bolt_name))?;
        let batch = crate::arrow_bridge::extract_relation_to_arrow(&extract_sql)?;
        let schema = batch.schema();
        let meta = api::load_batch(&entry.meta.pg_relation, Some(&alias), batch)?;
        on_table_loaded(meta, schema, entry.pg_oid);
        Ok(true)
    }

    /// Automatically refreshes all dirty tables referenced in `sql` prior to query execution.
    pub(crate) fn refresh_dirty_referenced_tables(sql: &str) -> PgBoltResult<()> {
        if !TRACKER_ACTIVE.load(Ordering::Acquire) {
            return Ok(());
        }
        let dirty_names: Vec<String> = {
            let state = TRACKER.lock();
            state
                .tables
                .iter()
                .filter(|(_, t)| t.dirty)
                .map(|(name, _)| name.clone())
                .collect()
        };
        if dirty_names.is_empty() {
            return Ok(());
        }
        for name in dirty_names {
            if super::sql_references_table(sql, &name) {
                let _ = refresh_table_if_dirty(&name)?;
            }
        }
        Ok(())
    }

    fn apply_frame_abort(frames: Vec<TxFrame>) {
        if frames.is_empty() {
            return;
        }
        let mut to_evict = Vec::new();
        let mut to_restore_clean = Vec::new();
        {
            let mut state = TRACKER.lock();
            for frame in frames.into_iter().rev() {
                for bolt_name in frame.added_or_replaced {
                    if let Some(removed) = state.tables.remove(&bolt_name) {
                        if removed.pg_oid_u32 != 0 {
                            if let Some(set) = state.oid_to_bolts.get_mut(&removed.pg_oid_u32) {
                                set.remove(&bolt_name);
                                if set.is_empty() {
                                    state.oid_to_bolts.remove(&removed.pg_oid_u32);
                                }
                            }
                        }
                    }
                    to_evict.push(bolt_name);
                }
                for (bolt_name, prev_dirty) in frame.dirtied {
                    if !prev_dirty {
                        if let Some(entry) = state.tables.get_mut(&bolt_name) {
                            entry.dirty = false;
                            entry.meta.dirty = false;
                            to_restore_clean.push((
                                bolt_name,
                                entry.meta.clone(),
                                entry.schema.clone(),
                                entry.pg_oid_u32,
                            ));
                        }
                    }
                }
            }
            refresh_active_flag(&state);
        }

        if !to_evict.is_empty() {
            let evicted_set: HashSet<&str> = to_evict.iter().map(String::as_str).collect();
            to_restore_clean
                .retain(|(bolt_name, _, _, _)| !evicted_set.contains(bolt_name.as_str()));
        }

        for bolt_name in to_evict {
            let _ = api::evict_table(&bolt_name);
        }
        for (bolt_name, meta, schema, oid_u32) in to_restore_clean {
            sync_local_dirty_state(&bolt_name, meta, schema, oid_u32, false);
        }
    }

    fn apply_commit(frames: Vec<TxFrame>) {
        if frames.is_empty() {
            return;
        }
        let mut evict_names = HashSet::new();
        let mut dirty_candidates = HashSet::new();
        for frame in frames {
            evict_names.extend(frame.pending_evict_on_commit);
            for (bolt_name, _) in frame.dirtied {
                dirty_candidates.insert(bolt_name);
            }
        }
        dirty_candidates.retain(|name| !evict_names.contains(name));
        if evict_names.is_empty() && dirty_candidates.is_empty() {
            return;
        }
        let mut dirty_names = Vec::new();
        {
            let mut state = TRACKER.lock();
            for bolt_name in &evict_names {
                if let Some(removed) = state.tables.remove(bolt_name) {
                    if removed.pg_oid_u32 != 0 {
                        if let Some(set) = state.oid_to_bolts.get_mut(&removed.pg_oid_u32) {
                            set.remove(bolt_name);
                            if set.is_empty() {
                                state.oid_to_bolts.remove(&removed.pg_oid_u32);
                            }
                        }
                    }
                }
            }
            for bolt_name in dirty_candidates {
                if state.tables.get(&bolt_name).is_some_and(|t| t.dirty) {
                    dirty_names.push(bolt_name);
                }
            }
            if !evict_names.is_empty() {
                refresh_active_flag(&state);
            }
        }
        for bolt_name in evict_names {
            let _ = api::evict_table(&bolt_name);
        }
        for bolt_name in dirty_names {
            broadcast_bgworker_dirty(&bolt_name);
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    pub(crate) fn test_simulate_top_level_commit() {
        let frames = {
            let mut state = TRACKER.lock();
            std::mem::take(&mut state.subxact_stack)
        };
        apply_commit(frames);
    }

    unsafe fn collect_modified_rel_oids(
        planned: *const pg_sys::PlannedStmt,
        out: &mut Vec<pg_sys::Oid>,
    ) {
        if planned.is_null() {
            return;
        }
        let rtable = (*planned).rtable;
        if rtable.is_null() || (*rtable).length <= 0 || (*rtable).elements.is_null() {
            return;
        }
        let rt_cells = std::slice::from_raw_parts((*rtable).elements, (*rtable).length as usize);

        let plan_tree = (*planned).planTree;
        if !plan_tree.is_null()
            && (*(plan_tree as *const pg_sys::Node)).type_ == pg_sys::NodeTag::T_ModifyTable
        {
            let mt = plan_tree as *const pg_sys::ModifyTable;
            let rels = (*mt).resultRelations;
            if !rels.is_null() && (*rels).length > 0 && !(*rels).elements.is_null() {
                let idx_cells =
                    std::slice::from_raw_parts((*rels).elements, (*rels).length as usize);
                for cell in idx_cells {
                    let rti = cell.int_value;
                    if rti >= 1 {
                        if let Some(rt_cell) = rt_cells.get((rti - 1) as usize) {
                            let rte = rt_cell.ptr_value as *const pg_sys::RangeTblEntry;
                            if !rte.is_null()
                                && (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
                                && (*rte).relid != pg_sys::InvalidOid
                            {
                                out.push((*rte).relid);
                            }
                        }
                    }
                }
            }
        }

        if out.is_empty() {
            for rt_cell in rt_cells {
                let rte = rt_cell.ptr_value as *const pg_sys::RangeTblEntry;
                if !rte.is_null()
                    && (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
                    && (*rte).relid != pg_sys::InvalidOid
                {
                    out.push((*rte).relid);
                }
            }
        }
    }

    #[pgrx::prelude::pg_guard]
    unsafe extern "C-unwind" fn bolt_executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
        if !query_desc.is_null() && TRACKER_ACTIVE.load(Ordering::Acquire) {
            let op = (*query_desc).operation;
            if matches!(
                op,
                pg_sys::CmdType::CMD_INSERT
                    | pg_sys::CmdType::CMD_UPDATE
                    | pg_sys::CmdType::CMD_DELETE
                    | pg_sys::CmdType::CMD_MERGE
            ) {
                let mut oids = Vec::new();
                collect_modified_rel_oids((*query_desc).plannedstmt, &mut oids);
                for oid in oids {
                    mark_oid_dirty_in_tx(oid, false);
                }
            }
        }
        let prev = PREV_EXECUTOR_END_HOOK;
        if let Some(prev_fn) = prev {
            prev_fn(query_desc);
        } else {
            pg_sys::standard_ExecutorEnd(query_desc);
        }
    }

    #[pgrx::prelude::pg_guard]
    unsafe extern "C-unwind" fn bolt_process_utility_hook(
        pstmt: *mut pg_sys::PlannedStmt,
        query_string: *const std::ffi::c_char,
        read_only_tree: bool,
        context: pg_sys::ProcessUtilityContext,
        params: pg_sys::ParamListInfo,
        query_env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        let active = TRACKER_ACTIVE.load(Ordering::Acquire)
            && !pstmt.is_null()
            && !(*pstmt).utilityStmt.is_null();

        let mut pre_target_oids: Vec<(pg_sys::Oid, bool)> = Vec::new();
        let mut tracked_oids_for_drop: Vec<pg_sys::Oid> = Vec::new();
        let mut is_drop = false;

        if active {
            let utility_stmt = (*pstmt).utilityStmt;
            match (*utility_stmt).type_ {
                pg_sys::NodeTag::T_TruncateStmt => {
                    let stmt = utility_stmt as *const pg_sys::TruncateStmt;
                    let rels = (*stmt).relations;
                    if !rels.is_null() && (*rels).length > 0 && !(*rels).elements.is_null() {
                        let cells =
                            std::slice::from_raw_parts((*rels).elements, (*rels).length as usize);
                        for cell in cells {
                            if let Some(oid) =
                                resolve_range_var_oid(cell.ptr_value as *const pg_sys::RangeVar)
                            {
                                pre_target_oids.push((oid, true));
                            }
                        }
                    }
                }
                pg_sys::NodeTag::T_AlterTableStmt => {
                    let stmt = utility_stmt as *const pg_sys::AlterTableStmt;
                    if let Some(oid) = resolve_range_var_oid((*stmt).relation) {
                        pre_target_oids.push((oid, true));
                    }
                }
                pg_sys::NodeTag::T_RenameStmt => {
                    let stmt = utility_stmt as *const pg_sys::RenameStmt;
                    if let Some(oid) = resolve_range_var_oid((*stmt).relation) {
                        pre_target_oids.push((oid, true));
                    }
                }
                pg_sys::NodeTag::T_CopyStmt => {
                    let stmt = utility_stmt as *const pg_sys::CopyStmt;
                    if (*stmt).is_from {
                        if let Some(oid) = resolve_range_var_oid((*stmt).relation) {
                            pre_target_oids.push((oid, false));
                        }
                    }
                }
                pg_sys::NodeTag::T_DropStmt => {
                    is_drop = true;
                    let state = TRACKER.lock();
                    tracked_oids_for_drop =
                        state.tables.values().filter_map(|t| t.pg_oid).collect();
                }
                _ => {}
            }
        }

        let prev = PREV_PROCESS_UTILITY_HOOK;
        if let Some(prev_fn) = prev {
            prev_fn(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }

        if active {
            for (oid, evict_on_commit) in pre_target_oids {
                mark_oid_dirty_in_tx(oid, evict_on_commit);
            }
            if is_drop {
                for oid in tracked_oids_for_drop {
                    let rel_name_ptr = pg_sys::get_rel_name(oid);
                    if rel_name_ptr.is_null() {
                        mark_oid_dirty_in_tx(oid, true);
                    } else {
                        pg_sys::pfree(rel_name_ptr.cast());
                    }
                }
            }
        }
    }

    unsafe extern "C-unwind" fn bolt_xact_callback(event: pg_sys::XactEvent, _arg: *mut c_void) {
        let _ = catch_unwind(AssertUnwindSafe(|| match event {
            pg_sys::XactEvent::XACT_EVENT_COMMIT => {
                let frames = {
                    let mut state = TRACKER.lock();
                    std::mem::take(&mut state.subxact_stack)
                };
                apply_commit(frames);
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| rt.commit_transaction(0));
                }
            }
            pg_sys::XactEvent::XACT_EVENT_ABORT => {
                let frames = {
                    let mut state = TRACKER.lock();
                    std::mem::take(&mut state.subxact_stack)
                };
                apply_frame_abort(frames);
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| rt.abort_transaction(0));
                }
            }
            _ => {}
        }));
    }

    unsafe extern "C-unwind" fn bolt_subxact_callback(
        event: pg_sys::SubXactEvent,
        my_subid: pg_sys::SubTransactionId,
        parent_subid: pg_sys::SubTransactionId,
        _arg: *mut c_void,
    ) {
        let _ = catch_unwind(AssertUnwindSafe(|| match event {
            pg_sys::SubXactEvent::SUBXACT_EVENT_START_SUB => {
                let mut state = TRACKER.lock();
                ensure_root_frame(&mut state.subxact_stack);
                state.subxact_stack.push(TxFrame {
                    subid: my_subid,
                    ..TxFrame::default()
                });
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| {
                        rt.begin_subtransaction(my_subid as u64);
                        Ok(())
                    });
                }
            }
            pg_sys::SubXactEvent::SUBXACT_EVENT_COMMIT_SUB => {
                let mut state = TRACKER.lock();
                let Some(pos) = state
                    .subxact_stack
                    .iter()
                    .rposition(|f| f.subid == my_subid)
                else {
                    return;
                };
                let child = state.subxact_stack.remove(pos);
                let parent_idx = state
                    .subxact_stack
                    .iter()
                    .rposition(|f| f.subid == parent_subid)
                    .unwrap_or_else(|| {
                        if state.subxact_stack.is_empty() {
                            state.subxact_stack.push(TxFrame {
                                subid: 0,
                                ..TxFrame::default()
                            });
                        }
                        state.subxact_stack.len() - 1
                    });
                let parent = &mut state.subxact_stack[parent_idx];
                for added in child.added_or_replaced {
                    if !parent.added_or_replaced.contains(&added) {
                        parent.added_or_replaced.push(added);
                    }
                }
                for (name, prev_dirty) in child.dirtied {
                    record_dirtied_in_frame(parent, &name, prev_dirty);
                }
                parent
                    .pending_evict_on_commit
                    .extend(child.pending_evict_on_commit);
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| {
                        rt.commit_subtransaction(my_subid as u64);
                        Ok(())
                    });
                }
            }
            pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB => {
                let aborted_frames = {
                    let mut state = TRACKER.lock();
                    let Some(pos) = state
                        .subxact_stack
                        .iter()
                        .rposition(|f| f.subid == my_subid)
                    else {
                        return;
                    };
                    state.subxact_stack.split_off(pos)
                };
                apply_frame_abort(aborted_frames);
                if !current_config().use_bgworker {
                    let _ = with_bolt_runtime(|rt| rt.abort_subtransaction(my_subid as u64));
                }
            }
            _ => {}
        }));
    }

    unsafe extern "C-unwind" fn bolt_relcache_callback(_arg: pg_sys::Datum, relid: pg_sys::Oid) {
        if !TRACKER_ACTIVE.load(Ordering::Acquire) {
            return;
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let mut state = TRACKER.lock();
            if relid == pg_sys::InvalidOid {
                state.pending_relcache_flush_all = true;
            } else {
                let oid_u32 = relid.to_u32();
                if state.oid_to_bolts.contains_key(&oid_u32) {
                    state.pending_relcache_oids.insert(oid_u32);
                }
            }
        }));
    }
}

#[cfg(feature = "pgrx")]
#[pgrx::prelude::pg_schema]
mod bolt {
    use pgrx::prelude::*;

    use crate::api;
    use crate::arrow_bridge::{
        extract_relation_to_arrow, record_batch_to_json_rows, record_batch_to_pgrx_jsonb,
    };
    use crate::error::PgBoltError;
    use crate::pg_hooks;

    /// Loads (or replaces) a PostgreSQL table or view snapshot into Bolt's Arrow/VRAM cache.
    ///
    /// Newly loaded entries inside an active transaction or `SAVEPOINT` are evicted automatically
    /// if that transaction or subtransaction rolls back.
    #[pg_extern]
    pub fn load_table(
        pg_relation: &str,
        bolt_alias: default!(Option<&str>, "NULL"),
    ) -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(rows_cached, i64),
                name!(columns_cached, i32),
                name!(memory_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        pg_hooks::reconcile_pending_invalidations();
        let (alias, extract_sql) = api::prepare_extract(pg_relation, bolt_alias)?;
        let rel_oid = pg_hooks::resolve_relation_oid(pg_relation);
        let batch = extract_relation_to_arrow(&extract_sql)?;
        let schema = batch.schema();
        let meta = api::load_batch(pg_relation, Some(&alias), batch)?;
        pg_hooks::on_table_loaded(meta.clone(), schema, rel_oid);
        Ok(TableIterator::once((
            meta.bolt_name,
            meta.num_rows as i64,
            meta.num_cols as i32,
            meta.approx_bytes as i64,
        )))
    }

    /// Ensures a PostgreSQL relation is pinned in Bolt, extracting via SPI only if uncached or dirty.
    #[pg_extern]
    pub fn cache_table(
        pg_relation: &str,
        bolt_alias: default!(Option<&str>, "NULL"),
    ) -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(rows_cached, i64),
                name!(columns_cached, i32),
                name!(memory_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        pg_hooks::reconcile_pending_invalidations();
        let (alias, extract_sql) = api::prepare_extract(pg_relation, bolt_alias)?;
        let rel_oid = pg_hooks::resolve_relation_oid(pg_relation);
        if pg_hooks::is_table_dirty(&alias) {
            let batch = extract_relation_to_arrow(&extract_sql)?;
            let schema = batch.schema();
            let meta = api::load_batch(pg_relation, Some(&alias), batch)?;
            pg_hooks::on_table_loaded(meta.clone(), schema, rel_oid);
            return Ok(TableIterator::once((
                meta.bolt_name,
                meta.num_rows as i64,
                meta.num_cols as i32,
                meta.approx_bytes as i64,
            )));
        }

        let mut loaded_schema = None;
        let meta = api::cache_batch_if_needed(pg_relation, Some(&alias), |sql| {
            let batch = extract_relation_to_arrow(sql)?;
            loaded_schema = Some(batch.schema());
            Ok(batch)
        })?;
        if let Some(schema) = loaded_schema {
            pg_hooks::on_table_loaded(meta.clone(), schema, rel_oid);
        } else {
            pg_hooks::bind_existing_table(&meta, rel_oid);
        }
        Ok(TableIterator::once((
            meta.bolt_name,
            meta.num_rows as i64,
            meta.num_cols as i32,
            meta.approx_bytes as i64,
        )))
    }

    /// Evicts a cached table from Bolt's VRAM and host metadata registry immediately.
    ///
    /// Note: eviction frees session/worker VRAM immediately rather than deferring until commit.
    #[pg_extern]
    pub fn evict_table(bolt_name: &str) -> Result<bool, PgBoltError> {
        let evicted = api::evict_table(bolt_name)?;
        pg_hooks::on_table_evicted(bolt_name.trim());
        Ok(evicted)
    }

    /// Re-extracts a PostgreSQL relation snapshot and atomically refreshes its Bolt cache entry.
    #[pg_extern]
    pub fn sync_table(
        pg_relation: &str,
        bolt_alias: default!(Option<&str>, "NULL"),
    ) -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(rows_cached, i64),
                name!(columns_cached, i32),
                name!(memory_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        load_table(pg_relation, bolt_alias)
    }

    /// Ingests an incremental delta batch into an existing cached Bolt table using an extraction query.
    #[pg_extern]
    pub fn sync_delta(
        bolt_name: &str,
        delta_query: &str,
    ) -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(rows_cached, i64),
                name!(columns_cached, i32),
                name!(memory_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        pg_hooks::reconcile_pending_invalidations();
        let delta = extract_relation_to_arrow(delta_query)?;
        let meta = api::sync_delta_batch(bolt_name, delta)?;
        Ok(TableIterator::once((
            meta.bolt_name,
            meta.num_rows as i64,
            meta.num_cols as i32,
            meta.approx_bytes as i64,
        )))
    }

    /// Lists all tables currently cached in the active Bolt runtime.
    #[pg_extern]
    pub fn tables() -> Result<
        TableIterator<
            'static,
            (
                name!(bolt_table, String),
                name!(source_relation, String),
                name!(num_rows, i64),
                name!(approx_bytes, i64),
            ),
        >,
        PgBoltError,
    > {
        pg_hooks::reconcile_pending_invalidations();
        let rows: Vec<_> = api::tables()?
            .into_iter()
            .map(|m| {
                (
                    m.bolt_name,
                    m.pg_relation,
                    m.num_rows as i64,
                    m.approx_bytes as i64,
                )
            })
            .collect();
        Ok(TableIterator::new(rows.into_iter()))
    }

    /// Executes an analytical SQL query on Bolt and returns one `jsonb` object per result row.
    #[pg_extern]
    pub fn query(sql: &str) -> Result<SetOfIterator<'static, pgrx::JsonB>, PgBoltError> {
        pg_hooks::reconcile_pending_invalidations();
        pg_hooks::refresh_dirty_referenced_tables(sql)?;
        let res = api::query(sql)?;
        pgrx::debug1!(
            "pg_bolt: executed query ({:?}) in {} us ({} rows returned)",
            res.tier,
            res.elapsed_us,
            res.batch.num_rows()
        );
        let rows = record_batch_to_pgrx_jsonb(&res.batch)?;
        Ok(SetOfIterator::new(rows.into_iter()))
    }

    /// Executes an analytical SQL query on Bolt and returns the entire result set as a `jsonb` array.
    #[pg_extern]
    pub fn query_json(sql: &str) -> Result<pgrx::JsonB, PgBoltError> {
        pg_hooks::reconcile_pending_invalidations();
        pg_hooks::refresh_dirty_referenced_tables(sql)?;
        let res = api::query(sql)?;
        let rows = record_batch_to_json_rows(&res.batch)?;
        Ok(pgrx::JsonB(serde_json::Value::Array(rows)))
    }

    /// Plans `sql` against currently cached Bolt schemas and returns `"Gpu"`, `"Hybrid"`, or `"Host"`.
    #[pg_extern]
    pub fn explain_tier(sql: &str) -> Result<String, PgBoltError> {
        pg_hooks::reconcile_pending_invalidations();
        let tier = api::explain_tier(sql)?;
        Ok(format!("{tier:?}"))
    }

    /// Plans `sql` against currently cached Bolt schemas and returns logical and physical plan text.
    #[pg_extern]
    pub fn explain_sql(sql: &str) -> Result<String, PgBoltError> {
        pg_hooks::reconcile_pending_invalidations();
        api::explain_sql(sql)
    }

    /// Returns runtime telemetry, VRAM budget usage, PTX cache stats, and memory pool metrics as `jsonb`.
    #[pg_extern]
    pub fn status() -> Result<pgrx::JsonB, PgBoltError> {
        let st = api::status()?;
        let val = serde_json::to_value(st)
            .map_err(|e| PgBoltError::Bolt(format!("failed to serialize runtime status: {e}")))?;
        Ok(pgrx::JsonB(val))
    }

    /// Immediately releases the active Bolt runtime, cached tables, and CUDA allocations for this backend.
    ///
    /// Note: VRAM release is immediate and is not rolled back if a surrounding transaction aborts.
    #[pg_extern]
    pub fn reset_vram() -> &'static str {
        api::reset_vram()
    }
}

#[cfg(all(feature = "pgrx", any(test, feature = "pg_test")))]
#[pgrx::prelude::pg_schema]
mod pg_tests {
    use pgrx::prelude::*;

    use super::{bolt, pg_hooks};

    #[pg_test]
    fn test_extension_loads_and_gucs_work() {
        let enabled = Spi::get_one::<bool>("SELECT current_setting('pg_bolt.enabled')::bool")
            .expect("SPI current_setting query should succeed");
        assert_eq!(enabled, Some(true));

        let shown = Spi::get_one::<String>("SHOW pg_bolt.enabled")
            .expect("SHOW pg_bolt.enabled should succeed");
        assert_eq!(shown.as_deref(), Some("on"));
    }

    #[pg_test]
    fn test_validation_and_table_lifecycle() {
        let _ = bolt::reset_vram();
        assert!(bolt::load_table("public..sales", None).is_err());
        assert!(bolt::load_table("sales\0bad", None).is_err());
        assert!(!bolt::evict_table("nonexistent").unwrap());

        Spi::run(
            "CREATE TEMP TABLE bolt_test_sales (id int4 NOT NULL, amount float8 NOT NULL);
             INSERT INTO bolt_test_sales VALUES (1, 10.5), (2, 20.5);",
        )
        .expect("creating test table via SPI should succeed");

        assert!(bolt::load_table("bolt_test_sales", Some("sales")).is_ok());
        assert_eq!(
            bolt::explain_tier("SELECT id, SUM(amount) FROM sales GROUP BY id").unwrap(),
            "Hybrid"
        );
        assert_eq!(
            bolt::explain_tier("SELECT id, amount * 2.0 AS doubled FROM sales").unwrap(),
            "Gpu"
        );
        let plan = bolt::explain_sql("SELECT id, amount * 2.0 AS doubled FROM sales").unwrap();
        assert!(plan.contains("Logical plan:"));
        assert!(plan.contains("Physical plan:"));

        Spi::run("SET LOCAL pg_bolt.min_offload_rows = 1;")
            .expect("setting min_offload_rows should succeed");
        let sum = Spi::get_one::<f64>("SELECT SUM(amount) FROM bolt_test_sales")
            .expect("planner offload + fallback aggregate query should succeed");
        assert_eq!(sum, Some(31.0));

        assert!(bolt::evict_table("sales").unwrap());
        assert!(!bolt::evict_table("sales").unwrap());

        let status_json = bolt::status().unwrap();
        assert!(status_json.0.get("owner_pid").is_some());
        assert!(bolt::reset_vram().contains("released"));
    }

    #[pg_test]
    fn test_dml_dirty_tracking_and_subxact_rollback() {
        let _ = bolt::reset_vram();
        Spi::run(
            "CREATE TEMP TABLE bolt_hook_sales (id int4 NOT NULL, amount float8 NOT NULL);
             INSERT INTO bolt_hook_sales VALUES (1, 10.0);",
        )
        .expect("creating hook test table should succeed");

        assert!(bolt::load_table("bolt_hook_sales", Some("hook_sales")).is_ok());
        assert!(!pg_hooks::is_table_dirty("hook_sales"));
        let meta = super::api::tables()
            .unwrap()
            .into_iter()
            .find(|t| t.bolt_name == "hook_sales")
            .unwrap();
        assert!(!meta.dirty);

        // Subtransaction DML followed by exception rollback must restore clean state.
        Spi::run(
            "DO $$
             BEGIN
                 INSERT INTO bolt_hook_sales VALUES (2, 20.0);
                 RAISE EXCEPTION 'trigger_subxact_abort';
             EXCEPTION
                 WHEN raise_exception THEN NULL;
             END $$;",
        )
        .expect("subtransaction abort test block should complete cleanly");
        assert!(
            !pg_hooks::is_table_dirty("hook_sales"),
            "rolled-back INSERT must restore clean cache state"
        );
        let meta_after_rollback = super::api::tables()
            .unwrap()
            .into_iter()
            .find(|t| t.bolt_name == "hook_sales")
            .unwrap();
        assert!(!meta_after_rollback.dirty);

        // Committed DML marks cached table dirty until cache_table/sync_table refreshes it.
        Spi::run("INSERT INTO bolt_hook_sales VALUES (3, 30.0);")
            .expect("DML insert should succeed");
        assert!(
            pg_hooks::is_table_dirty("hook_sales"),
            "committed INSERT must mark cached table dirty"
        );
        let meta_dirty = super::api::tables()
            .unwrap()
            .into_iter()
            .find(|t| t.bolt_name == "hook_sales")
            .unwrap();
        assert!(meta_dirty.dirty);

        let refreshed: Vec<_> = bolt::cache_table("bolt_hook_sales", Some("hook_sales"))
            .unwrap()
            .collect();
        assert_eq!(refreshed[0].1, 2);
        assert!(!pg_hooks::is_table_dirty("hook_sales"));
        let meta_clean = super::api::tables()
            .unwrap()
            .into_iter()
            .find(|t| t.bolt_name == "hook_sales")
            .unwrap();
        assert_eq!(meta_clean.num_rows, 2);
        assert!(!meta_clean.dirty);
        assert!(bolt::reset_vram().contains("released"));
    }

    #[pg_test]
    fn test_uncommitted_dml_and_rollback_preserve_bgworker_dirty_state() {
        let _ = bolt::reset_vram();
        if !crate::guc::current_config().use_bgworker {
            return;
        }
        Spi::run(
            "CREATE TEMP TABLE bolt_w3d002_sales (id int4 NOT NULL, amount float8 NOT NULL);
             INSERT INTO bolt_w3d002_sales VALUES (1, 10.0);",
        )
        .expect("creating W3-D-002 test table should succeed");

        assert!(bolt::load_table("bolt_w3d002_sales", Some("w3d002_sales")).is_ok());

        let bg_dirty = || -> bool {
            let client = crate::bgworker::shared_bgworker_client().unwrap();
            let list: Vec<crate::CachedTableMeta> =
                serde_json::from_str(&client.tables_json().unwrap()).unwrap();
            list.into_iter()
                .find(|t| t.bolt_name == "w3d002_sales")
                .unwrap()
                .dirty
        };

        // Uncommitted DML marks backend-local state and api::tables() dirty without touching
        // shared BgWorker state before top-level commit.
        Spi::run("INSERT INTO bolt_w3d002_sales VALUES (2, 20.0);")
            .expect("uncommitted INSERT should succeed");
        assert!(pg_hooks::is_table_dirty("w3d002_sales"));
        assert!(
            super::api::tables()
                .unwrap()
                .into_iter()
                .find(|t| t.bolt_name == "w3d002_sales")
                .unwrap()
                .dirty
        );
        assert!(
            !bg_dirty(),
            "uncommitted DML must not dirty shared BgWorker before commit"
        );

        // Top-level commit broadcasts committed dirty state to BgWorker.
        pg_hooks::test_simulate_top_level_commit();
        assert!(
            bg_dirty(),
            "top-level commit must broadcast dirty state to shared BgWorker"
        );

        // Reload clean, simulate concurrent Session B commit dirtying BgWorker, then roll back
        // Session A subtransaction DML: rollback must preserve Session B's BgWorker dirty flag.
        assert!(bolt::load_table("bolt_w3d002_sales", Some("w3d002_sales")).is_ok());
        assert!(!pg_hooks::is_table_dirty("w3d002_sales"));
        assert!(!bg_dirty());

        crate::bgworker::shared_bgworker_client()
            .unwrap()
            .set_table_dirty("w3d002_sales", true)
            .unwrap();

        Spi::run(
            "DO $$
             BEGIN
                 INSERT INTO bolt_w3d002_sales VALUES (3, 30.0);
                 RAISE EXCEPTION 'trigger_rollback';
             EXCEPTION
                 WHEN raise_exception THEN NULL;
             END $$;",
        )
        .expect("subtransaction rollback block should complete cleanly");

        assert!(
            !pg_hooks::is_table_dirty("w3d002_sales"),
            "Session A rollback must restore backend-local clean state"
        );
        assert!(
            bg_dirty(),
            "Session A rollback must not clear concurrent BgWorker dirty state"
        );
        assert!(
            super::api::tables()
                .unwrap()
                .into_iter()
                .find(|t| t.bolt_name == "w3d002_sales")
                .unwrap()
                .dirty,
            "api::tables() must reflect committed BgWorker dirty state after local rollback"
        );

        let refreshed: Vec<_> = bolt::cache_table("bolt_w3d002_sales", Some("w3d002_sales"))
            .unwrap()
            .collect();
        assert_eq!(refreshed[0].1, 2);
        assert!(!bg_dirty());
        assert!(bolt::reset_vram().contains("released"));
    }

    #[pg_test]
    fn test_sync_delta_and_query_dirty_table_auto_refresh() {
        let _ = bolt::reset_vram();
        Spi::run(
            "CREATE TEMP TABLE bolt_delta_sales (id int4 NOT NULL, amount float8 NOT NULL);
             INSERT INTO bolt_delta_sales VALUES (1, 10.0), (2, 20.0);",
        )
        .expect("creating delta test table should succeed");

        assert!(bolt::load_table("bolt_delta_sales", Some("delta_sales")).is_ok());

        // Incremental sync_delta appends rows
        let delta_res: Vec<_> =
            bolt::sync_delta("delta_sales", "SELECT 3 AS id, 30.0::float8 AS amount")
                .unwrap()
                .collect();
        assert_eq!(delta_res[0].1, 3);

        // Committed DML marks table dirty
        Spi::run("INSERT INTO bolt_delta_sales VALUES (4, 40.0);")
            .expect("DML insert should succeed");
        assert!(pg_hooks::is_table_dirty("delta_sales"));

        // bolt::query automatically refreshes dirty table before execution
        let _ = bolt::query_json("SELECT COUNT(*) AS total FROM delta_sales");
        assert!(!pg_hooks::is_table_dirty("delta_sales"));
        assert!(bolt::reset_vram().contains("released"));
    }
}

#[cfg(all(feature = "pgrx", test))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_bolt'"]
    }
}
