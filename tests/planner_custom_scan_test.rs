// SPDX-License-Identifier: Apache-2.0
#![cfg(not(feature = "pgrx"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use craton_bolt::ExecutionTier;
use pg_bolt::bgworker::BgWorkerClient;
use pg_bolt::custom_scan::{BoltCustomScanPlan, BoltCustomScanState, ScanExecutionBackend};
use pg_bolt::error::PgBoltError;
use pg_bolt::guc::BoltConfig;
use pg_bolt::ipc::{
    record_batch_to_ipc_stream, IpcCommand, IpcResponse, IpcRingConfig, SharedMemoryArena,
};
use pg_bolt::planner::{analyze_sql_eligibility, inspect_sql_for_offload, rewrite_sql_for_bolt};
use pg_bolt::runtime::CachedTableMeta;

#[cfg(not(feature = "cuda-stub"))]
use pg_bolt::bgworker::BoltBgWorker;
#[cfg(not(feature = "cuda-stub"))]
use pg_bolt::runtime::{test_process_lock, BoltSessionRuntime};

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region_id", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
        Field::new("tax", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
    ]))
}

fn cached_meta(name: &str, pg_relation: &str, num_rows: usize, dirty: bool) -> CachedTableMeta {
    CachedTableMeta {
        pg_relation: pg_relation.to_string(),
        bolt_name: name.to_string(),
        num_rows,
        num_cols: 4,
        approx_bytes: num_rows * 28,
        loaded_at_ms: 1,
        last_accessed_seq: 1,
        dirty,
    }
}

fn default_sales_cache(
    rows: usize,
    dirty: bool,
) -> (HashMap<String, CachedTableMeta>, HashMap<String, SchemaRef>) {
    let mut metas = HashMap::new();
    let mut schemas = HashMap::new();
    metas.insert(
        "sales".to_string(),
        cached_meta("sales", "public.sales", rows, dirty),
    );
    schemas.insert("sales".to_string(), sales_schema());
    (metas, schemas)
}

fn sample_result_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region_id", DataType::Int32, false),
        Field::new("net_total", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 25])),
            Arc::new(Float64Array::from(vec![335.0, 300.0])),
        ],
    )
    .expect("valid result RecordBatch")
}

#[test]
fn planner_rejects_dml_locking_volatiles_uncached_small_dirty_and_disallowed_hybrid() {
    let (metas, schemas) = default_sales_cache(100_000, false);
    let cfg = BoltConfig::default();

    // Mutating SQL and row-locking clauses fail SQL inspection and are gracefully marked
    // ineligible by `analyze_sql_eligibility` so `planner_hook` leaves execution on PostgreSQL.
    for rejected_sql in [
        "INSERT INTO sales VALUES (1, 10.0, 0.1, 0.0)",
        "UPDATE sales SET price = price * 1.1",
        "DELETE FROM sales WHERE region_id = 10",
        "SELECT region_id FROM sales FOR UPDATE",
        "WITH d AS (DELETE FROM sales RETURNING region_id) SELECT * FROM d",
        "SELECT * INTO sales_copy FROM sales",
    ] {
        assert!(
            inspect_sql_for_offload(rejected_sql).is_err(),
            "inspect_sql_for_offload must reject {rejected_sql}"
        );
        let elig = analyze_sql_eligibility(rejected_sql, &metas, &schemas, &cfg)
            .expect("analyze_sql_eligibility must return Ok(ineligible) for unsupported SQL");
        assert!(
            !elig.eligible,
            "analyze_sql_eligibility must mark rejected SQL ineligible: {rejected_sql}"
        );
    }

    // Volatile functions disqualify offload cleanly (`eligible == false`).
    for volatile_sql in [
        "SELECT region_id, random() FROM sales",
        "SELECT region_id, nextval('sales_id_seq') FROM sales",
        "SELECT region_id FROM sales WHERE clock_timestamp() IS NOT NULL",
        "SELECT region_id, txid_current() FROM sales",
    ] {
        let elig = analyze_sql_eligibility(volatile_sql, &metas, &schemas, &cfg).unwrap();
        assert!(
            !elig.eligible,
            "volatile query must be ineligible: {volatile_sql}"
        );
        assert!(
            elig.reason.contains("volatile"),
            "unexpected reason for {volatile_sql}: {}",
            elig.reason
        );
    }

    // Uncached tables and wrong schema qualifiers are rejected.
    let uncached = analyze_sql_eligibility(
        "SELECT region_id, SUM(price) FROM uncached_orders GROUP BY region_id",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(!uncached.eligible);
    assert!(uncached.reason.contains("not cached"));

    let wrong_schema = analyze_sql_eligibility(
        "SELECT region_id, SUM(price) FROM analytics.sales GROUP BY region_id",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(!wrong_schema.eligible);
    assert!(wrong_schema.reason.contains("not cached"));

    // Parameterized queries ($1) cannot be offloaded as static SQL.
    let param_elig = analyze_sql_eligibility(
        "SELECT region_id FROM sales WHERE region_id < $1",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(!param_elig.eligible);

    // Below min_offload_rows threshold.
    let (small_metas, small_schemas) = default_sales_cache(10_000, false);
    let below_min = analyze_sql_eligibility(
        "SELECT region_id, SUM(price) FROM sales GROUP BY region_id",
        &small_metas,
        &small_schemas,
        &cfg,
    )
    .unwrap();
    assert!(!below_min.eligible);
    assert!(below_min.reason.contains("below min_offload_rows"));

    // Cost model still protects tiny tables even when min_offload_rows == 0.
    let (tiny_metas, tiny_schemas) = default_sales_cache(500, false);
    let zero_threshold_cfg = BoltConfig {
        min_offload_rows: 0,
        ..BoltConfig::default()
    };
    let too_costly = analyze_sql_eligibility(
        "SELECT region_id, price * tax AS tax_amt FROM sales WHERE region_id < 50",
        &tiny_metas,
        &tiny_schemas,
        &zero_threshold_cfg,
    )
    .unwrap();
    assert!(!too_costly.eligible);
    assert!(too_costly.estimated_bolt_cost >= too_costly.estimated_pg_cost);

    // Dirty cached tables awaiting MVCC sync must not be served stale.
    let (dirty_metas, dirty_schemas) = default_sales_cache(100_000, true);
    let dirty = analyze_sql_eligibility(
        "SELECT region_id, SUM(price) FROM sales GROUP BY region_id",
        &dirty_metas,
        &dirty_schemas,
        &cfg,
    )
    .unwrap();
    assert!(!dirty.eligible);
    assert!(dirty.reason.contains("dirty"));

    // When allow_hybrid_fallback == false, Hybrid-tier plans are rejected in favor of PG CPU.
    let strict_gpu_cfg = BoltConfig {
        allow_hybrid_fallback: false,
        ..BoltConfig::default()
    };
    let hybrid_rejected = analyze_sql_eligibility(
        "SELECT region_id, SUM(price) FROM sales GROUP BY region_id ORDER BY region_id",
        &metas,
        &schemas,
        &strict_gpu_cfg,
    )
    .unwrap();
    assert!(!hybrid_rejected.eligible);
    assert_eq!(hybrid_rejected.tier, Some(ExecutionTier::Hybrid));
    assert!(hybrid_rejected.reason.contains("allow_hybrid_fallback"));

    // Multi-column GROUP BY with pre-aggregation (e.g. q2_region_category_rev) is
    // supported on Bolt GPU and eligible for CustomScan offload.
    let q2_multi_col = analyze_sql_eligibility(
        "SELECT region_id, tax, SUM(price * (1.0 - discount / 100.0)) AS gross_rev, \
         AVG(price) AS avg_price, COUNT(*) AS order_lines \
         FROM public.sales WHERE region_id >= 10 AND region_id < 50 \
         GROUP BY region_id, tax ORDER BY region_id, tax",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(
        q2_multi_col.eligible,
        "multi-column GROUP BY with pre-aggregation should be eligible: {}",
        q2_multi_col.reason
    );
    assert!(matches!(
        q2_multi_col.tier,
        Some(ExecutionTier::Gpu | ExecutionTier::Hybrid)
    ));
    assert!(q2_multi_col.estimated_bolt_cost < q2_multi_col.estimated_pg_cost);
}

#[test]
fn planner_accepts_gpu_projection_hybrid_agg_and_ctes_without_false_missing_table() {
    let (metas, schemas) = default_sales_cache(200_000, false);
    let cfg = BoltConfig::default();

    let proj = analyze_sql_eligibility(
        "SELECT region_id, (price * (1.0 + tax)) - discount AS net FROM public.sales WHERE region_id < 50",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(
        proj.eligible,
        "projection should be eligible: {}",
        proj.reason
    );
    assert_eq!(proj.tier, Some(ExecutionTier::Gpu));
    assert!(proj.estimated_bolt_cost < proj.estimated_pg_cost);

    let agg = analyze_sql_eligibility(
        "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
         FROM public.sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id",
        &metas,
        &schemas,
        &cfg,
    )
    .unwrap();
    assert!(
        agg.eligible,
        "analytical agg should be eligible: {}",
        agg.reason
    );
    assert!(matches!(
        agg.tier,
        Some(ExecutionTier::Gpu | ExecutionTier::Hybrid)
    ));
    assert!(agg.estimated_bolt_cost < agg.estimated_pg_cost);

    // Non-recursive CTE referencing cached `sales` must not report `filtered` as missing table.
    let cte_sql = "WITH filtered AS (SELECT region_id, price, tax, discount FROM sales WHERE region_id < 50) \
                   SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
                   FROM filtered GROUP BY region_id ORDER BY region_id";
    let cte_elig = analyze_sql_eligibility(cte_sql, &metas, &schemas, &cfg).unwrap();
    assert!(
        cte_elig.eligible,
        "CTE query over cached table must be eligible, got: {}",
        cte_elig.reason
    );
    assert_eq!(cte_elig.referenced_tables, vec!["sales".to_string()]);
    assert!(cte_elig.estimated_bolt_cost < cte_elig.estimated_pg_cost);

    // Sequential self-referencing CTE (`W1-E-011`): `sales` inside the CTE body refers to the
    // cached base table `sales`, not the CTE alias being defined.
    let self_ref_cte_sql =
        "WITH sales AS (SELECT region_id, price, tax, discount FROM sales WHERE region_id < 50) \
         SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
         FROM sales GROUP BY region_id ORDER BY region_id";
    let self_ref_insp = inspect_sql_for_offload(self_ref_cte_sql)
        .expect("inspect_sql_for_offload must handle sequential self-referencing CTE");
    assert_eq!(self_ref_insp.referenced_tables, vec!["sales".to_string()]);
    assert!(self_ref_insp.cte_names.contains(&"sales".to_string()));

    let self_ref_elig = analyze_sql_eligibility(self_ref_cte_sql, &metas, &schemas, &cfg)
        .expect("analyze_sql_eligibility on self-referencing CTE");
    assert!(
        self_ref_elig.eligible,
        "sequential self-referencing CTE over cached sales must be eligible, got: {}",
        self_ref_elig.reason
    );
    assert_eq!(self_ref_elig.referenced_tables, vec!["sales".to_string()]);
}

#[test]
fn custom_scan_state_lifecycle_explain_rescan_and_vram_oom_cpu_fallback() {
    // 1. Successful BgWorker IPC batch execution, row iteration, rescan, explain, and end.
    let arena = Arc::new(
        SharedMemoryArena::new(IpcRingConfig {
            num_slots: 4,
            slot_bytes: 128 * 1024,
        })
        .unwrap(),
    );
    let ipc_payload = record_batch_to_ipc_stream(&sample_result_batch()).unwrap();
    let server_arena = Arc::clone(&arena);
    let server = thread::spawn(move || {
        while let Some((req_id, cmd)) = server_arena.recv_request(Duration::from_millis(50)) {
            match cmd {
                IpcCommand::ExecuteSql { .. } => {
                    server_arena
                        .complete_request(
                            req_id,
                            IpcResponse::Batch {
                                ipc_payload: ipc_payload.clone(),
                                elapsed_us: 420,
                                tier: "Gpu".to_string(),
                            },
                        )
                        .expect("complete ExecuteSql IPC response");
                    break;
                }
                _ => {
                    server_arena
                        .complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: "ignored".into(),
                            },
                        )
                        .expect("complete ignored IPC response");
                }
            }
        }
    });

    let client = BgWorkerClient::new(Arc::clone(&arena), Some(Duration::from_secs(2)));
    let plan = BoltCustomScanPlan {
        sql: "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total FROM sales GROUP BY region_id"
            .to_string(),
        tier: ExecutionTier::Gpu,
        estimated_cost: 350.25,
        estimated_rows: 2,
        fallback_on_oom: true,
        timeout_ms: 2_000,
    };

    let mut state = BoltCustomScanState::begin(plan);
    let pre_explain = state.explain(false);
    assert!(pre_explain.contains("Custom Scan (pg_bolt Gpu)"));
    assert!(pre_explain.contains("PTX Cache: Pending"));

    let batch = state
        .execute(ScanExecutionBackend::BgWorker(&client))
        .unwrap()
        .expect("CustomScan produced batch");
    assert_eq!(batch.num_rows(), 2);

    let post_explain = state.explain(true);
    assert!(post_explain.contains("Bolt Execution Tier: Gpu"));
    assert!(post_explain.contains("Kernel Time: 0.420 ms"));
    assert!(post_explain.contains("Rows Emitted: 2"));
    assert!(post_explain.contains("Batches Emitted: 1"));

    let row0 = state.next_row_json().unwrap().unwrap();
    assert_eq!(row0["region_id"], 10);
    assert_eq!(row0["net_total"], 335.0);
    let row1 = state.next_row_json().unwrap().unwrap();
    assert_eq!(row1["region_id"], 25);
    assert!(state.next_row_json().unwrap().is_none());

    state.rescan();
    assert_eq!(state.next_row_json().unwrap().unwrap()["region_id"], 10);

    let end_stats = state.end();
    assert_eq!(end_stats.rows_produced, 2);
    assert!(!end_stats.fell_back_to_cpu);
    assert!(state.cached_batch().is_none());

    server.join().unwrap();
    arena.shutdown();

    // 2. Real VramExhausted error over IPC triggers `execute_with_cpu_fallback`.
    let oom_arena = Arc::new(
        SharedMemoryArena::new(IpcRingConfig {
            num_slots: 4,
            slot_bytes: 64 * 1024,
        })
        .unwrap(),
    );
    let oom_server_arena = Arc::clone(&oom_arena);
    let oom_server = thread::spawn(move || {
        let mut handled = 0;
        while handled < 2 {
            if let Some((req_id, _cmd)) = oom_server_arena.recv_request(Duration::from_millis(50)) {
                let resp = if handled == 0 {
                    IpcResponse::Error {
                        message: "GPU device out of memory".to_string(),
                        vram_exhausted: true,
                    }
                } else {
                    IpcResponse::Error {
                        message: "invalid column reference".to_string(),
                        vram_exhausted: false,
                    }
                };
                oom_server_arena
                    .complete_request(req_id, resp)
                    .expect("complete OOM/error IPC response");
                handled += 1;
            }
        }
    });

    let oom_client = BgWorkerClient::new(Arc::clone(&oom_arena), Some(Duration::from_secs(2)));
    let oom_plan = BoltCustomScanPlan {
        sql: "SELECT region_id, net_total FROM sales".to_string(),
        tier: ExecutionTier::Hybrid,
        estimated_cost: 900.0,
        estimated_rows: 2,
        fallback_on_oom: true,
        timeout_ms: 2_000,
    };
    let mut oom_state = BoltCustomScanState::begin(oom_plan);
    let cpu_batch = oom_state
        .execute_with_cpu_fallback(ScanExecutionBackend::BgWorker(&oom_client), || {
            Ok(sample_result_batch())
        })
        .expect("VramExhausted must fall back to PostgreSQL CPU closure");
    assert_eq!(cpu_batch.num_rows(), 2);
    assert!(oom_state.stats().fell_back_to_cpu);
    assert_eq!(oom_state.stats().execution_tier, "HostFallback");
    assert!(oom_state
        .stats()
        .fallback_reason
        .as_deref()
        .unwrap_or("")
        .contains("out of memory"));
    assert!(oom_state.explain(true).contains("Fallback: PostgreSQL CPU"));
    assert_eq!(oom_state.next_row_json().unwrap().unwrap()["region_id"], 10);

    // Non-VRAM logical error with fallback_on_oom=false must NOT silently mask the error.
    let strict_plan = BoltCustomScanPlan {
        sql: "SELECT nonexistent FROM sales".to_string(),
        tier: ExecutionTier::Gpu,
        estimated_cost: 100.0,
        estimated_rows: 1,
        fallback_on_oom: false,
        timeout_ms: 2_000,
    };
    let mut strict_state = BoltCustomScanState::begin(strict_plan);
    let mut fallback_invoked = false;
    let err = strict_state
        .execute_with_cpu_fallback(ScanExecutionBackend::BgWorker(&oom_client), || {
            fallback_invoked = true;
            Ok(sample_result_batch())
        })
        .unwrap_err();
    assert!(!fallback_invoked);
    assert!(matches!(err, PgBoltError::Bolt(_)));

    oom_server.join().unwrap();
    oom_arena.shutdown();
}

#[cfg(not(feature = "cuda-stub"))]
#[test]
fn custom_scan_executes_real_gpu_queries_via_direct_and_bgworker_backends() {
    let _guard = test_process_lock();
    let sales_batch = RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int32Array::from(vec![10, 10, 25, 80])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 500.0])),
            Arc::new(Float64Array::from(vec![0.10, 0.20, 0.05, 0.25])),
            Arc::new(Float64Array::from(vec![5.0, 10.0, 15.0, 0.0])),
        ],
    )
    .unwrap();
    let sql = "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
               FROM sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id"
        .to_string();

    // Direct BoltSessionRuntime execution path.
    {
        let mut rt = BoltSessionRuntime::new().expect("Direct CUDA runtime");
        rt.register_batch("public.sales", "sales", sales_batch.clone())
            .unwrap();

        let mut direct_state = BoltCustomScanState::begin(BoltCustomScanPlan {
            sql: sql.clone(),
            tier: ExecutionTier::Hybrid,
            estimated_cost: 250.0,
            estimated_rows: 2,
            fallback_on_oom: true,
            timeout_ms: 5_000,
        });
        let res = direct_state
            .execute_with_cpu_fallback(ScanExecutionBackend::Direct(&mut rt), || {
                panic!("CPU fallback should not run when GPU execution succeeds")
            })
            .unwrap();
        assert_eq!(res.num_rows(), 2);
        assert!(!direct_state.stats().fell_back_to_cpu);

        let r0 = direct_state.next_row_json().unwrap().unwrap();
        let r1 = direct_state.next_row_json().unwrap().unwrap();
        assert_eq!(r0["region_id"], 10);
        assert!((r0["net_total"].as_f64().unwrap() - 335.0).abs() < 1e-6);
        assert_eq!(r1["region_id"], 25);
        assert!((r1["net_total"].as_f64().unwrap() - 300.0).abs() < 1e-6);
        assert!(direct_state.next_row_json().unwrap().is_none());

        direct_state.rescan();
        assert_eq!(
            direct_state.next_row_json().unwrap().unwrap()["region_id"],
            10
        );
        let _ = direct_state.end();
    }

    // BgWorker IPC execution path.
    {
        let worker = BoltBgWorker::start(BoltConfig::default()).expect("BgWorker CUDA startup");
        let client = worker.client();
        client
            .load_batch("public.sales", "sales", &sales_batch)
            .unwrap();

        let mut ipc_state = BoltCustomScanState::begin(BoltCustomScanPlan {
            sql,
            tier: ExecutionTier::Hybrid,
            estimated_cost: 250.0,
            estimated_rows: 2,
            fallback_on_oom: true,
            timeout_ms: 5_000,
        });
        let res = ipc_state
            .execute(ScanExecutionBackend::BgWorker(&client))
            .unwrap()
            .unwrap();
        assert_eq!(res.num_rows(), 2);
        assert_eq!(ipc_state.next_row_json().unwrap().unwrap()["region_id"], 10);
        assert!(ipc_state.explain(true).contains("Rows Emitted: 2"));
        let _ = ipc_state.end();

        worker.shutdown();
    }
}

#[test]
fn planner_rewrites_and_accepts_three_part_column_qualifiers_and_order_by() {
    let (metas, schemas) = default_sales_cache(200_000, false);
    let cfg = BoltConfig::default();

    let agg_sql = "SELECT public.sales.region_id, SUM(public.sales.price) \
                   FROM public.sales \
                   GROUP BY public.sales.region_id \
                   ORDER BY public.sales.region_id DESC";
    let rewritten_agg = rewrite_sql_for_bolt(agg_sql, &metas).unwrap();
    assert!(
        !rewritten_agg.contains("public.sales"),
        "expected 3-part qualifiers to be stripped, got: {rewritten_agg}"
    );
    assert!(
        rewritten_agg.contains("DESC NULLS FIRST"),
        "expected PostgreSQL DESC NULLS FIRST normalization, got: {rewritten_agg}"
    );

    let agg_elig = analyze_sql_eligibility(agg_sql, &metas, &schemas, &cfg).unwrap();
    assert!(
        agg_elig.eligible,
        "3-part qualified aggregate query should be eligible, got: {}",
        agg_elig.reason
    );
    assert!(matches!(
        agg_elig.tier,
        Some(ExecutionTier::Gpu | ExecutionTier::Hybrid)
    ));
    assert_eq!(agg_elig.referenced_tables, vec!["public.sales".to_string()]);

    let proj_sql = "SELECT public.sales.region_id, public.sales.price \
                    FROM public.sales \
                    WHERE public.sales.region_id < 50 \
                    ORDER BY public.sales.price DESC";
    let rewritten_proj = rewrite_sql_for_bolt(proj_sql, &metas).unwrap();
    assert!(
        !rewritten_proj.contains("public.sales"),
        "expected 3-part qualifiers to be stripped from projection/ORDER BY, got: {rewritten_proj}"
    );
    assert!(
        rewritten_proj.contains("DESC NULLS FIRST"),
        "expected PostgreSQL DESC NULLS FIRST normalization, got: {rewritten_proj}"
    );

    let proj_elig = analyze_sql_eligibility(proj_sql, &metas, &schemas, &cfg).unwrap();
    assert!(
        proj_elig.eligible,
        "3-part qualified projection query should be eligible, got: {}",
        proj_elig.reason
    );
    assert_eq!(proj_elig.tier, Some(ExecutionTier::Hybrid));
    assert_eq!(
        proj_elig.referenced_tables,
        vec!["public.sales".to_string()]
    );
}
