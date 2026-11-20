// SPDX-License-Identifier: Apache-2.0
#![cfg(not(feature = "pgrx"))]

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    FixedSizeBinaryArray, Float64Array, Int32Array, RecordBatch, StringArray, StringViewArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use craton_bolt::ExecutionTier;
use pg_bolt::arrow_bridge::encode_dictionary_utf8;
use pg_bolt::bgworker::BoltBgWorker;
use pg_bolt::error::PgBoltError;
use pg_bolt::guc::BoltConfig;
use pg_bolt::ipc::{
    ipc_stream_to_record_batch, minimal_tuples_to_record_batch, record_batch_to_ipc_stream,
    record_batch_to_minimal_tuples, IpcCommand, IpcResponse, IpcRingConfig, SharedMemoryArena,
};
use pg_bolt::runtime::{test_process_lock, BoltRuntimeStatus, CachedTableMeta};

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

fn rich_ipc_batch() -> RecordBatch {
    let raw_dict = StringArray::from(vec![
        Some("us-east"),
        None,
        Some("eu-west"),
        Some("us-east"),
    ]);
    let dict = encode_dictionary_utf8(&raw_dict).expect("encode Dictionary<Int32, Utf8>");

    let schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, true),
        Field::new("id", DataType::Int32, true),
        Field::new("score", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(38, 10), true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "ts_plain",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "ts_utc",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "region_dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let amounts = Decimal128Array::from(vec![
        Some(12_345_000_000_000_i128),
        None,
        Some(-999_000_000_000_i128),
        Some(42_i128),
    ])
    .with_data_type(DataType::Decimal128(38, 10));

    let ts_utc = TimestampMicrosecondArray::from(vec![
        Some(0),
        Some(1_704_067_200_123_456),
        None,
        Some(-1_000_000),
    ])
    .with_timezone("UTC");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(-42), Some(7)])),
            Arc::new(Float64Array::from(vec![
                Some(3.5),
                Some(0.0),
                None,
                Some(-1.25),
            ])),
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some("gamma-utf8-🚀"),
                Some(""),
            ])),
            Arc::new(amounts),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(19_724),
                None,
                Some(-5),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(100),
                None,
                Some(1_700_000_000_000_000),
                Some(-50),
            ])),
            Arc::new(ts_utc),
            Arc::new(dict),
        ],
    )
    .expect("valid rich batch")
}

#[test]
fn ipc_stream_and_minimal_tuples_round_trip_rich_types_and_nulls() {
    let batch = rich_ipc_batch();

    let ipc_bytes = record_batch_to_ipc_stream(&batch).expect("encode IPC stream");
    let decoded_ipc = ipc_stream_to_record_batch(&ipc_bytes).expect("decode IPC stream");
    assert_eq!(decoded_ipc, batch);

    let tuples = record_batch_to_minimal_tuples(&batch).expect("encode MinimalTuples");
    assert_eq!(tuples.len(), batch.num_rows());
    let decoded_tuples =
        minimal_tuples_to_record_batch(&tuples, &batch.schema()).expect("decode MinimalTuples");
    assert_eq!(decoded_tuples, batch);

    // Empty 0-row batch round-trip through both codecs.
    let empty = RecordBatch::new_empty(batch.schema());
    let empty_ipc = record_batch_to_ipc_stream(&empty).unwrap();
    assert_eq!(
        ipc_stream_to_record_batch(&empty_ipc).unwrap().num_rows(),
        0
    );
    let empty_tuples = record_batch_to_minimal_tuples(&empty).unwrap();
    assert!(empty_tuples.is_empty());
    assert_eq!(
        minimal_tuples_to_record_batch(&empty_tuples, &empty.schema())
            .unwrap()
            .num_rows(),
        0
    );

    // Truncated or corrupt IPC stream must error cleanly instead of panicking.
    assert!(matches!(
        ipc_stream_to_record_batch(&[1, 2, 3]),
        Err(PgBoltError::Ipc(_))
    ));
    let mut bad_prefix = vec![0xFF, 0xFF, 0xFF, 0xFF];
    bad_prefix.extend_from_slice(&(-1_i32).to_le_bytes());
    assert!(matches!(
        ipc_stream_to_record_batch(&bad_prefix),
        Err(PgBoltError::Ipc(_))
    ));
}

#[test]
fn shared_memory_arena_rejects_oversized_payload_times_out_and_shuts_down() {
    let arena = Arc::new(
        SharedMemoryArena::new(IpcRingConfig {
            num_slots: 4,
            slot_bytes: 512,
        })
        .expect("arena init"),
    );

    // Payload exceeding slot_bytes (512) must fail with PgBoltError::Ipc.
    let oversized_sql = format!("SELECT '{}'", "x".repeat(1024));
    let oversized_err = arena
        .submit_request(
            IpcCommand::ExecuteSql {
                sql: oversized_sql,
                timeout_ms: 0,
            },
            Some(Duration::from_millis(200)),
        )
        .unwrap_err();
    assert!(matches!(oversized_err, PgBoltError::Ipc(_)));
    assert_eq!(arena.stats().total_rejected_oversized, 1);

    // Timeout when no worker drains pending request.
    let timeout_err = arena
        .submit_request(IpcCommand::GetStatus, Some(Duration::from_millis(25)))
        .unwrap_err();
    assert!(matches!(timeout_err, PgBoltError::Ipc(_)));
    assert_eq!(arena.stats().total_timed_out, 1);
    assert_eq!(arena.stats().free_slots, 4);

    // Round-trip request/response across producer and consumer threads.
    let worker_arena = Arc::clone(&arena);
    let responder = thread::spawn(move || {
        while let Some((req_id, cmd)) = worker_arena.recv_request(Duration::from_millis(100)) {
            match cmd {
                IpcCommand::GetStatus => {
                    worker_arena
                        .complete_request(
                            req_id,
                            IpcResponse::StatusJson {
                                json: "{\"ok\":true}".to_string(),
                            },
                        )
                        .expect("complete status IPC request");
                    break;
                }
                _ => {
                    worker_arena
                        .complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: "unexpected".to_string(),
                            },
                        )
                        .expect("complete unexpected IPC request");
                }
            }
        }
    });

    let resp = arena
        .submit_request(IpcCommand::GetStatus, Some(Duration::from_secs(2)))
        .expect("arena request completion");
    assert_eq!(
        resp,
        IpcResponse::StatusJson {
            json: "{\"ok\":true}".to_string()
        }
    );
    responder.join().unwrap();

    arena.shutdown();
    let stats = arena.stats();
    assert!(stats.is_shutdown);
    assert_eq!(stats.free_slots, 4);
}

#[test]
fn bgworker_concurrent_clients_run_ipc_commands_and_clean_shutdown() {
    let _guard = test_process_lock();
    let cfg = BoltConfig {
        enabled: true,
        ipc_ring_slots: 8,
        ipc_slot_bytes: 1024 * 1024,
        ..BoltConfig::default()
    };

    let worker = BoltBgWorker::start(cfg).expect("BoltBgWorker startup");
    let root_client = worker.client();

    let shared_sales = build_sales_batch(&[
        (10, 100.0, 0.10, 5.0),
        (20, 200.0, 0.05, 10.0),
        (70, 500.0, 0.20, 0.0),
    ]);
    let shared_ack = root_client
        .load_batch("public.sales", "sales", &shared_sales)
        .expect("load shared sales");
    assert!(shared_ack.contains("3 rows"));

    thread::scope(|s| {
        for tid in 0..4_usize {
            let client = worker.client();
            s.spawn(move || {
                let tbl = format!("thread_sales_{tid}");
                let rel = format!("public.{tbl}");
                let base_price = 100.0 + (tid as f64) * 50.0;

                // Region 10: (base_price * 1.10) - 5.0; Delta adds (50.0 * 1.10) - 2.0 = 53.0
                // Region 60: filtered out when querying `region_id < 50`
                let initial =
                    build_sales_batch(&[(10, base_price, 0.10, 5.0), (60, 400.0, 0.25, 0.0)]);
                let ack = client.load_batch(&rel, &tbl, &initial).unwrap();
                assert!(ack.contains("2 rows"));

                let delta = build_sales_batch(&[(10, 50.0, 0.10, 2.0), (20, 80.0, 0.25, 0.0)]);
                let delta_ack = client.append_delta(&tbl, &delta).unwrap();
                assert!(delta_ack.contains("4 rows"));

                let proj_tier = client
                    .explain_tier(&format!(
                        "SELECT region_id, price * tax AS tax_amt FROM {tbl} WHERE region_id < 50"
                    ))
                    .unwrap();
                assert_eq!(proj_tier, ExecutionTier::Gpu);

                let agg_tier = client
                    .explain_tier(&format!(
                        "SELECT region_id, SUM(price) FROM {tbl} GROUP BY region_id"
                    ))
                    .unwrap();
                assert!(matches!(
                    agg_tier,
                    ExecutionTier::Gpu | ExecutionTier::Hybrid
                ));

                let status: BoltRuntimeStatus =
                    serde_json::from_str(&client.status_json().unwrap()).unwrap();
                assert_eq!(status.owner_pid, std::process::id());
                assert!(status.cached_tables >= 1);

                let tables: Vec<CachedTableMeta> =
                    serde_json::from_str(&client.tables_json().unwrap()).unwrap();
                let mine = tables
                    .iter()
                    .find(|m| m.bolt_name == tbl)
                    .expect("thread table visible in ListTables");
                assert_eq!(mine.num_rows, 4);

                #[cfg(feature = "cuda-stub")]
                {
                    let err = client
                        .execute_sql(&format!("SELECT COUNT(*) FROM {tbl}"), 1_000)
                        .unwrap_err();
                    assert!(
                        matches!(err, PgBoltError::Bolt(ref msg) if msg.contains("cuda-stub")),
                        "expected cuda-stub error, got {err:?}"
                    );
                    assert!(client.status_json().is_ok());
                }

                #[cfg(not(feature = "cuda-stub"))]
                {
                    let sql = format!(
                        "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
                         FROM {tbl} WHERE region_id < 50 GROUP BY region_id ORDER BY region_id"
                    );
                    let (res_batch, _elapsed, exec_tier) = client
                        .execute_sql(&sql, 5_000)
                        .expect("GPU worker execute_sql");
                    assert!(matches!(
                        exec_tier,
                        ExecutionTier::Gpu | ExecutionTier::Hybrid
                    ));
                    assert_eq!(res_batch.num_rows(), 2);

                    let regions = res_batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let totals = res_batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    let expected_reg10 = (base_price * 1.10 - 5.0) + (50.0 * 1.10 - 2.0);
                    let expected_reg20 = 80.0 * 1.25;
                    assert_eq!(regions.value(0), 10);
                    assert!((totals.value(0) - expected_reg10).abs() < 1e-6);
                    assert_eq!(regions.value(1), 20);
                    assert!((totals.value(1) - expected_reg20).abs() < 1e-6);

                    // Concurrent read against the shared `sales` table.
                    let (shared_res, _, _) = client
                        .execute_sql(
                            "SELECT COUNT(*) AS cnt FROM sales WHERE region_id < 50",
                            5_000,
                        )
                        .unwrap();
                    assert_eq!(shared_res.num_rows(), 1);
                }

                assert!(client.evict_table(&tbl).unwrap());
            });
        }
    });

    let remaining: Vec<CachedTableMeta> =
        serde_json::from_str(&root_client.tables_json().unwrap()).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].bolt_name, "sales");
    assert!(root_client.evict_table("sales").unwrap());

    let arena_stats = worker.arena().stats();
    assert!(arena_stats.total_completed > 0);
    assert_eq!(arena_stats.total_rejected_oversized, 0);

    worker.shutdown();
}

#[test]
fn ipc_minimal_tuples_round_trip_extended_view_and_int64_dict_types() {
    let mut dict_builder = StringDictionaryBuilder::<Int64Type>::new();
    dict_builder.append_value("north-america");
    dict_builder.append_null();
    dict_builder.append_value("europe-west");
    dict_builder.append_value("north-america");

    let u1 = [0x10u8; 16];
    let u2 = [0x22u8; 16];
    let u3 = [0xFEu8; 16];

    let schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new("uid", DataType::FixedSizeBinary(16), true),
        Field::new("sv", DataType::Utf8View, true),
        Field::new("bv", DataType::BinaryView, true),
        Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let uuid_col = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some(u1), None, Some(u2), Some(u3)].into_iter(),
        16,
    )
    .unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(123_456i64),
                Some(45_000_000_000),
                None,
                Some(86_399_999_999),
            ])),
            Arc::new(uuid_col),
            Arc::new(StringViewArray::from(vec![
                Some("inline"),
                None,
                Some("out-of-line-string-view-payload"),
                Some("ok"),
            ])),
            Arc::new(BinaryViewArray::from(vec![
                Some(b"\x01\x02\x03".as_slice()),
                Some(b"out-of-line-binary-view-payload-0123".as_slice()),
                None,
                Some(b"".as_slice()),
            ])),
            Arc::new(dict_builder.finish()),
        ],
    )
    .unwrap();

    let tuples = record_batch_to_minimal_tuples(&batch).unwrap();
    assert_eq!(tuples.len(), 4);

    let decoded = minimal_tuples_to_record_batch(&tuples, &schema).unwrap();
    assert_eq!(decoded.num_rows(), 4);
    assert_eq!(decoded.column(0).as_ref(), batch.column(0).as_ref());
    assert_eq!(decoded.column(1).as_ref(), batch.column(1).as_ref());
    assert_eq!(decoded.column(2).as_ref(), batch.column(2).as_ref());
    assert_eq!(decoded.column(3).as_ref(), batch.column(3).as_ref());

    let decoded_dict = decoded
        .column(4)
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .expect("expected DictionaryArray<Int64Type>");
    assert_eq!(
        decoded_dict.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8))
    );
    let dict_values = decoded_dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected Utf8 dictionary values");
    let dict_keys = decoded_dict.keys();
    let decoded_strings: Vec<Option<&str>> = (0..decoded_dict.len())
        .map(|row| {
            if decoded_dict.is_null(row) {
                None
            } else {
                Some(dict_values.value(dict_keys.value(row) as usize))
            }
        })
        .collect();
    assert_eq!(
        decoded_strings,
        vec![
            Some("north-america"),
            None,
            Some("europe-west"),
            Some("north-america"),
        ]
    );
}

#[test]
fn bgworker_client_supports_explain_sql_and_dirty_flag_sync() {
    let _process_lock = test_process_lock();
    let worker = BoltBgWorker::start(BoltConfig {
        ipc_ring_slots: 8,
        ipc_slot_bytes: 256 * 1024,
        ..BoltConfig::default()
    })
    .unwrap();
    let client = worker.client();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )
    .unwrap();

    client.load_batch("public.sales", "sales", &batch).unwrap();

    let plan = client
        .explain_sql("SELECT id, amount FROM sales WHERE amount > 10.0 ORDER BY id ASC NULLS LAST")
        .unwrap();
    assert!(plan.contains("Logical plan:"));
    assert!(plan.contains("Physical plan:"));

    let initial_tables: Vec<CachedTableMeta> =
        serde_json::from_str(&client.tables_json().unwrap()).unwrap();
    assert_eq!(initial_tables.len(), 1);
    assert!(!initial_tables[0].dirty);

    client.set_table_dirty("sales", true).unwrap();
    let dirty_tables: Vec<CachedTableMeta> =
        serde_json::from_str(&client.tables_json().unwrap()).unwrap();
    assert_eq!(dirty_tables.len(), 1);
    assert!(dirty_tables[0].dirty);

    client.set_table_dirty("sales", false).unwrap();
    let clean_tables: Vec<CachedTableMeta> =
        serde_json::from_str(&client.tables_json().unwrap()).unwrap();
    assert_eq!(clean_tables.len(), 1);
    assert!(!clean_tables[0].dirty);

    worker.shutdown();
}

#[cfg(not(feature = "cuda-stub"))]
#[test]
fn bgworker_streams_large_execute_sql_response_over_small_ipc_slots() {
    let _process_lock = test_process_lock();
    let slot_bytes = 1024;
    let worker = BoltBgWorker::start(BoltConfig {
        ipc_ring_slots: 8,
        ipc_slot_bytes: slot_bytes,
        ..BoltConfig::default()
    })
    .unwrap();
    let client = worker.client();

    let n = 300usize;
    let ids: Vec<i32> = (0..n as i32).collect();
    let amounts: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("row-{i:04}-payload")),
            )),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap();
    assert!(
        record_batch_to_ipc_stream(&batch).unwrap().len() > slot_bytes * 4,
        "test payload must exceed single IPC slot size"
    );

    client
        .load_batch_chunked("public.stream_sales", "stream_sales", &batch)
        .unwrap();

    let (exec_batch, _elapsed_us, _tier) = client
        .execute_sql(
            "SELECT id, label, amount FROM stream_sales ORDER BY id ASC NULLS LAST",
            0,
        )
        .unwrap();
    assert_eq!(exec_batch.num_rows(), n);
    assert!(
        record_batch_to_ipc_stream(&exec_batch).unwrap().len() > slot_bytes * 4,
        "ExecuteSql result batch must have required multi-chunk response streaming"
    );
    let out_ids = exec_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(out_ids.value(0), 0);
    assert_eq!(out_ids.value(n - 1), (n - 1) as i32);

    worker.shutdown();
}
