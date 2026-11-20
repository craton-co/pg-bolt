// SPDX-License-Identifier: Apache-2.0
#![cfg(not(feature = "pgrx"))]

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use craton_bolt::ExecutionTier;
use pg_bolt::api;
use pg_bolt::error::PgBoltError;
use pg_bolt::guc::{with_test_config, BoltConfig};
use pg_bolt::runtime::{
    quote_qualified_identifier, reset_bolt_runtime, test_process_lock, BoltSessionRuntime,
};

struct RuntimeResetGuard;

impl Drop for RuntimeResetGuard {
    fn drop(&mut self) {
        pg_bolt::bgworker::shutdown_shared_bgworker();
        reset_bolt_runtime();
    }
}

fn sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("region_id", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
        Field::new("tax", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
    ]))
}

fn build_sales_batch(rows: &[(i32, f64, f64, f64)]) -> RecordBatch {
    let region_ids: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let prices: Vec<f64> = rows.iter().map(|r| r.1).collect();
    let taxes: Vec<f64> = rows.iter().map(|r| r.2).collect();
    let discounts: Vec<f64> = rows.iter().map(|r| r.3).collect();

    RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int32Array::from(region_ids)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(taxes)),
            Arc::new(Float64Array::from(discounts)),
        ],
    )
    .expect("valid sales RecordBatch")
}

fn build_sized_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Float64, false),
    ]));
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let vals: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.25).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .expect("valid sized RecordBatch")
}

#[test]
fn quote_qualified_identifier_escapes_valid_and_rejects_malformed() {
    assert_eq!(quote_qualified_identifier("sales").unwrap(), "\"sales\"");
    assert_eq!(
        quote_qualified_identifier("public.sales").unwrap(),
        "\"public\".\"sales\""
    );
    assert_eq!(
        quote_qualified_identifier("my\"schema.my\"tbl").unwrap(),
        "\"my\"\"schema\".\"my\"\"tbl\""
    );
    // Quoted identifiers containing dots and mixed unquoted/quoted segments (W1-E-012).
    assert_eq!(
        quote_qualified_identifier(r#""my.schema"."my.tbl""#).unwrap(),
        r#""my.schema"."my.tbl""#
    );
    assert_eq!(
        quote_qualified_identifier(r#"public."sales.2026""#).unwrap(),
        r#""public"."sales.2026""#
    );
    // Double-quoting prevents injection payloads from breaking out of identifier context.
    assert_eq!(
        quote_qualified_identifier("sales; DROP TABLE users--").unwrap(),
        "\"sales; DROP TABLE users--\""
    );

    let (alias, extract_sql) = api::prepare_extract("public.sales", None).unwrap();
    assert_eq!(alias, "public.sales");
    assert_eq!(extract_sql, "SELECT * FROM \"public\".\"sales\"");

    for bad in [
        "",
        "   ",
        ".sales",
        "sales.",
        "public..sales",
        "public .sales",
        "public. sales",
        "sales\0x",
        "sales\nDROP TABLE users",
        "a.b.c.d",
    ] {
        let err = quote_qualified_identifier(bad).unwrap_err();
        assert!(
            matches!(err, PgBoltError::Validation(_)),
            "expected Validation error for {bad:?}, got {err:?}"
        );
    }
}

#[test]
fn runtime_lifecycle_delta_dirty_evict_and_explain_tier() {
    let _guard = test_process_lock();
    let mut rt = BoltSessionRuntime::new().expect("BoltSessionRuntime startup");

    assert_eq!(rt.owner_pid(), std::process::id());
    rt.check_fork_safety().unwrap();
    assert_eq!(rt.status().cuda_stub, cfg!(feature = "cuda-stub"));
    assert_eq!(rt.has_engine(), cfg!(not(feature = "cuda-stub")));

    let initial = build_sales_batch(&[
        (10, 100.0, 0.10, 5.0),
        (20, 200.0, 0.05, 10.0),
        (60, 500.0, 0.20, 25.0),
    ]);
    let meta = rt
        .register_batch("public.sales", "sales", initial)
        .expect("register sales");
    assert_eq!(meta.pg_relation, "public.sales");
    assert_eq!(meta.bolt_name, "sales");
    assert_eq!(meta.num_rows, 3);
    assert_eq!(meta.num_cols, 4);
    assert!(!meta.dirty);
    assert!(rt.is_table_cached("sales"));
    assert_eq!(rt.table_names(), vec!["sales".to_string()]);
    assert!(rt.table_schemas().contains_key("sales"));

    let delta = build_sales_batch(&[(10, 50.0, 0.10, 2.0), (20, 40.0, 0.25, 0.0)]);
    let after_delta = rt
        .append_delta_batch("sales", delta)
        .expect("append delta rows");
    assert_eq!(after_delta.num_rows, 5);
    assert!(!after_delta.dirty);

    // Schema mismatch on append_delta_batch must fail cleanly.
    let wrong_schema_delta = build_sized_batch(4);
    let schema_err = rt
        .append_delta_batch("sales", wrong_schema_delta)
        .unwrap_err();
    assert!(matches!(schema_err, PgBoltError::Bridge(_)));

    // Missing table delta must return Mvcc error.
    let missing_err = rt
        .append_delta_batch("missing_tbl", build_sales_batch(&[(1, 1.0, 0.0, 0.0)]))
        .unwrap_err();
    assert!(matches!(missing_err, PgBoltError::Mvcc(_)));

    assert!(rt.mark_dirty("sales"));
    assert!(!rt.mark_dirty("nonexistent"));
    assert!(rt.get_table_meta("sales").unwrap().dirty);
    assert!(rt.clear_dirty("sales"));
    assert!(!rt.get_table_meta("sales").unwrap().dirty);

    let tables = rt.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].bolt_name, "sales");
    assert_eq!(tables[0].num_rows, 5);

    let proj_tier = rt
        .explain_tier("SELECT region_id, (price * (1.0 + tax)) - discount AS net FROM sales WHERE region_id < 50")
        .expect("explain projection tier");
    assert_eq!(proj_tier, ExecutionTier::Gpu);

    let agg_tier = rt
        .explain_tier(
            "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
             FROM sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id",
        )
        .expect("explain aggregate tier");
    assert!(matches!(
        agg_tier,
        ExecutionTier::Gpu | ExecutionTier::Hybrid
    ));

    let plan_text = rt
        .explain_sql("SELECT region_id, SUM(price) FROM sales GROUP BY region_id")
        .expect("explain sql");
    assert!(plan_text.contains("Logical plan"));
    assert!(plan_text.contains("Physical plan"));

    let st = rt.status();
    assert_eq!(st.cached_tables, 1);
    assert_eq!(st.owner_pid, std::process::id());
    assert!(st.total_cached_bytes > 0);

    #[cfg(feature = "cuda-stub")]
    {
        let stub_err = rt.execute_sql("SELECT COUNT(*) FROM sales").unwrap_err();
        assert!(matches!(stub_err, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")));
    }

    assert!(rt.evict_table("sales").unwrap());
    assert!(!rt.evict_table("sales").unwrap());
    assert!(!rt.is_table_cached("sales"));
    assert_eq!(rt.list_tables().len(), 0);
    assert_eq!(rt.status().eviction_count, 1);
}

#[test]
fn lru_vram_budget_evicts_oldest_and_rejects_oversized_batch() {
    let _guard = test_process_lock();
    let cfg = BoltConfig {
        max_vram_mb: 1,
        ..BoltConfig::default()
    };

    with_test_config(cfg.clone(), || {
        let mut rt = BoltSessionRuntime::new_with_config(&cfg).expect("runtime with 1 MiB cap");

        // Each row is 4 + 8 = 12 bytes -> 30k rows ~ 360 KiB, 35k rows ~ 420 KiB.
        // t1 + t2 (~780 KiB <= 1 MiB) and t1 + t3 (~720 KiB <= 1 MiB) fit after t2 eviction.
        let t1 = build_sized_batch(30_000);
        let t2 = build_sized_batch(35_000);
        let t3 = build_sized_batch(30_000);

        rt.register_batch("public.t1", "t1", t1).unwrap();
        rt.register_batch("public.t2", "t2", t2).unwrap();

        let seq_before = rt.get_table_meta("t1").unwrap().last_accessed_seq;
        // `execute_sql` advances LRU access sequence for referenced tables before stub check.
        let _ = rt.execute_sql("SELECT COUNT(*) FROM t1");
        let seq_after = rt.get_table_meta("t1").unwrap().last_accessed_seq;
        assert!(seq_after > seq_before, "t1 LRU sequence should advance");

        // Registering t3 (~360 KiB) exceeds 1 MiB total; t2 (LRU) must be evicted while t1 survives.
        rt.register_batch("public.t3", "t3", t3).unwrap();
        assert!(rt.is_table_cached("t1"), "recently used t1 must survive");
        assert!(!rt.is_table_cached("t2"), "oldest t2 must be evicted");
        assert!(
            rt.is_table_cached("t3"),
            "newly registered t3 must be cached"
        );
        assert!(rt.status().eviction_count >= 1);

        // Single batch exceeding the entire 1 MiB cap must return VramExhausted and preserve cache.
        let oversized = build_sized_batch(150_000); // ~1.8 MiB > 1 MiB
        let err = rt
            .register_batch("public.huge", "huge", oversized)
            .unwrap_err();
        assert!(
            matches!(err, PgBoltError::VramExhausted(_)),
            "expected VramExhausted, got {err:?}"
        );
        assert!(rt.is_table_cached("t1"));
        assert!(rt.is_table_cached("t3"));
        assert!(!rt.is_table_cached("huge"));
    });
}

#[test]
fn api_control_plane_respects_enabled_guc_and_resets_cleanly() {
    let _guard = test_process_lock();
    let _reset = RuntimeResetGuard;
    reset_bolt_runtime();

    let disabled = BoltConfig {
        enabled: false,
        ..BoltConfig::default()
    };
    with_test_config(disabled, || {
        let err = api::ensure_enabled().unwrap_err();
        assert!(matches!(err, PgBoltError::Disabled));
    });

    let batch = build_sales_batch(&[(7, 25.0, 0.1, 1.0)]);
    let meta = api::load_batch("public.api_sales", Some("api_sales"), batch.clone()).unwrap();
    assert_eq!(meta.num_rows, 1);

    let mut extract_called = false;
    let cached = api::cache_batch_if_needed("public.api_sales", Some("api_sales"), |_sql| {
        extract_called = true;
        Ok(batch.clone())
    })
    .unwrap();
    assert!(!extract_called, "cache hit must skip extract closure");
    assert_eq!(cached.num_rows, 1);

    let updated =
        api::sync_delta_batch("api_sales", build_sales_batch(&[(8, 30.0, 0.2, 0.0)])).unwrap();
    assert_eq!(updated.num_rows, 2);
    assert_eq!(api::tables().unwrap().len(), 1);
    assert!(api::evict_table("api_sales").unwrap());
    assert_eq!(api::tables().unwrap().len(), 0);
    let _ = api::reset_vram();
}

#[cfg(not(feature = "cuda-stub"))]
#[test]
fn gpu_ptx_jit_executes_sales_query_delta_and_timeout() {
    let _guard = test_process_lock();
    let mut rt = BoltSessionRuntime::new().expect("CUDA BoltSessionRuntime");

    // region 10: (100*(1.10) - 5) + (200*(1.20) - 10) = 105.0 + 230.0 = 335.0
    // region 25: (300*(1.05) - 15) = 300.0
    // region 75: filtered out by `region_id < 50`
    let initial = build_sales_batch(&[
        (10, 100.0, 0.10, 5.0),
        (25, 300.0, 0.05, 15.0),
        (10, 200.0, 0.20, 10.0),
        (75, 999.0, 0.50, 0.0),
    ]);
    rt.register_batch("public.sales", "sales", initial).unwrap();

    let sql = "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
               FROM sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id";
    let res = rt.execute_sql(sql).expect("GPU execution of sales query");
    assert_eq!(res.batch.num_rows(), 2);

    let regions = res
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("region_id Int32Array");
    let totals = res
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("net_total Float64Array");

    assert_eq!(regions.value(0), 10);
    assert!((totals.value(0) - 335.0).abs() < 1e-6);
    assert_eq!(regions.value(1), 25);
    assert!((totals.value(1) - 300.0).abs() < 1e-6);

    // Append incremental delta rows and confirm new rows reflect in subsequent GPU query.
    // delta region 10: 50*(1.10) - 2 = 53.0 -> new region 10 total = 388.0
    // delta region 40: 80*(1.25) - 0 = 100.0 -> new region 40 total = 100.0
    let delta = build_sales_batch(&[(10, 50.0, 0.10, 2.0), (40, 80.0, 0.25, 0.0)]);
    let delta_meta = rt.append_delta_batch("sales", delta).unwrap();
    assert_eq!(delta_meta.num_rows, 6);

    // Execute with cooperative cancellation timeout enabled via statement_timeout_ms.
    let timeout_cfg = BoltConfig {
        statement_timeout_ms: 5_000,
        ..BoltConfig::default()
    };
    let res2 = with_test_config(timeout_cfg, || rt.execute_sql(sql))
        .expect("GPU execution after delta and with timeout token");
    assert_eq!(res2.batch.num_rows(), 3);

    let regions2 = res2
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let totals2 = res2
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(regions2.value(0), 10);
    assert!((totals2.value(0) - 388.0).abs() < 1e-6);
    assert_eq!(regions2.value(1), 25);
    assert!((totals2.value(1) - 300.0).abs() < 1e-6);
    assert_eq!(regions2.value(2), 40);
    assert!((totals2.value(2) - 100.0).abs() < 1e-6);
}

#[test]
fn public_schema_default_alias_plans_and_executes_count_query() {
    let _guard = test_process_lock();
    let _reset = RuntimeResetGuard;
    reset_bolt_runtime();

    let (alias, extract_sql) = api::prepare_extract("public.sales", None).unwrap();
    assert_eq!(alias, "public.sales");
    assert_eq!(extract_sql, "SELECT * FROM \"public\".\"sales\"");

    let mut rt = BoltSessionRuntime::new().expect("BoltSessionRuntime startup");
    let batch = build_sales_batch(&[
        (10, 100.0, 0.10, 5.0),
        (20, 200.0, 0.05, 10.0),
        (30, 300.0, 0.20, 15.0),
    ]);
    let meta = rt
        .register_batch("public.sales", &alias, batch)
        .expect("register public.sales under default alias");
    assert_eq!(meta.pg_relation, "public.sales");
    assert_eq!(meta.bolt_name, "public.sales");

    let sql = "SELECT COUNT(*) FROM public.sales";
    let tier = rt
        .explain_tier(sql)
        .expect("explain_tier on default public.sales alias");
    assert!(matches!(
        tier,
        ExecutionTier::Gpu | ExecutionTier::Hybrid | ExecutionTier::Host
    ));

    #[cfg(not(feature = "cuda-stub"))]
    {
        let res = rt
            .execute_sql(sql)
            .expect("execute SELECT COUNT(*) FROM public.sales");
        assert_eq!(res.batch.num_rows(), 1);
        let col = res.batch.column(0);
        let count_val = if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
            arr.value(0) as u64
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
            arr.value(0)
        } else {
            panic!("unexpected COUNT(*) column type: {:?}", col.data_type());
        };
        assert_eq!(count_val, 3);
    }
}

#[test]
fn api_explain_sql_passes_with_and_without_bgworker() {
    let _guard = test_process_lock();
    let _reset = RuntimeResetGuard;
    pg_bolt::bgworker::shutdown_shared_bgworker();
    reset_bolt_runtime();

    let batch = build_sales_batch(&[(10, 100.0, 0.10, 5.0), (20, 200.0, 0.05, 10.0)]);
    let sql = "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
               FROM public.api_explain_sales GROUP BY region_id ORDER BY region_id";

    for use_bgworker in [false, true] {
        let cfg = BoltConfig {
            use_bgworker,
            ..BoltConfig::default()
        };
        with_test_config(cfg, || {
            pg_bolt::bgworker::shutdown_shared_bgworker();
            reset_bolt_runtime();

            let meta = api::load_batch(
                "public.api_explain_sales",
                Some("api_explain_sales"),
                batch.clone(),
            )
            .unwrap();
            assert_eq!(meta.num_rows, 2);

            let plan = api::explain_sql(sql).unwrap();
            assert!(
                plan.contains("Logical plan:"),
                "use_bgworker={use_bgworker} missing Logical plan header: {plan}"
            );
            assert!(
                plan.contains("Physical plan:"),
                "use_bgworker={use_bgworker} missing Physical plan header: {plan}"
            );

            assert!(api::evict_table("api_explain_sales").unwrap());
            pg_bolt::bgworker::shutdown_shared_bgworker();
            reset_bolt_runtime();
        });
    }
}
