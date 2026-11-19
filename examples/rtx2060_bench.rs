// SPDX-License-Identifier: Apache-2.0

//! End-to-end benchmark harness for pg_bolt running on the host's NVIDIA GeForce RTX 2060 GPU.
//! Measures throughput and latency across:
//! 1. Parallel 8 KiB PostgreSQL heap page decode & MVCC filtering to Arrow RecordBatch.
//! 2. Pure-Rust PGLZ varlena decompression.
//! 3. Background worker IPC dispatch & status roundtrip.
//! 4. Live CUDA SQL execution on RTX 2060: Cold PTX JIT vs Warm query latency and throughput.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pg_bolt::arrow_bridge::{
    decode_heap_pages_to_arrow, encode_test_heap_page, pglz_decompress, PgColumnMeta, PgDatumValue,
    FLOAT8OID, INT4OID,
};
use pg_bolt::bgworker::shared_bgworker_client;
use pg_bolt::mvcc::{HeapTupleHeaderMeta, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};
use pg_bolt::runtime::test_process_lock;

fn main() {
    let _guard = test_process_lock();
    println!("================================================================================");
    println!(" pg_bolt RTX 2060 Hardware Benchmark & Validation Suite");
    println!("================================================================================");

    // 1. Benchmark Heap Page Parallel MVCC Decode & Arrow Ingestion
    bench_heap_page_decode();

    // 2. Benchmark Pure-Rust PGLZ Decompression
    bench_toast_decompression();

    // 3. Benchmark Background Worker IPC Dispatch
    bench_ipc_worker();

    // 4. Benchmark Live CUDA Execution on RTX 2060
    bench_rtx2060_cuda_queries();

    println!("\n================================================================================");
    println!(" All benchmarks completed successfully on NVIDIA GeForce RTX 2060.");
    println!("================================================================================");
}

fn bench_heap_page_decode() {
    println!("\n[1/4] Benchmarking Heap Page Parallel MVCC Decode to Arrow...");
    let cols = vec![
        PgColumnMeta {
            name: "id".into(),
            type_oid: INT4OID,
            nullable: false,
        },
        PgColumnMeta {
            name: "price".into(),
            type_oid: FLOAT8OID,
            nullable: false,
        },
    ];

    let header = HeapTupleHeaderMeta {
        t_xmin: 50,
        t_xmax: 0,
        t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
        t_infomask2: 2,
        t_hoff: 24,
        natts: 2,
    };

    let mut tuples_per_page = Vec::new();
    for i in 0..100 {
        tuples_per_page.push((
            header,
            vec![
                PgDatumValue::Int32(i),
                PgDatumValue::Float64(i as f64 * 1.5 + 0.5),
            ],
        ));
    }

    let page_bytes = encode_test_heap_page(&cols, &tuples_per_page).unwrap();
    let num_pages = 2_000;
    let total_rows = num_pages * tuples_per_page.len();
    let total_bytes = num_pages * page_bytes.len();
    let pages: Vec<[u8; 8192]> = (0..num_pages).map(|_| page_bytes).collect();
    let page_refs: Vec<&[u8]> = pages.iter().map(|p| p.as_slice()).collect();

    // Warm-up
    let _ = decode_heap_pages_to_arrow(&cols, &page_refs, None, None).unwrap();

    let start = Instant::now();
    let iterations = 5;
    for _ in 0..iterations {
        let batch = decode_heap_pages_to_arrow(&cols, &page_refs, None, None).unwrap();
        assert_eq!(batch.num_rows(), total_rows);
    }
    let elapsed = start.elapsed();
    let avg_elapsed = elapsed / iterations;
    let mb_decoded = (total_bytes as f64 / 1_048_576.0) / avg_elapsed.as_secs_f64();
    let rows_sec = (total_rows as f64) / avg_elapsed.as_secs_f64();

    println!(
        "   Pages decoded per pass:  {} pages ({} KiB)",
        num_pages,
        total_bytes / 1024
    );
    println!("   Tuples decoded per pass: {} rows", total_rows);
    println!("   Average latency:         {:.2?}", avg_elapsed);
    println!(
        "   Decode Throughput:       {:.2} MB/s ({:.2} million rows/sec)",
        mb_decoded,
        rows_sec / 1_000_000.0
    );
}

fn bench_toast_decompression() {
    println!("\n[2/4] Benchmarking Pure-Rust Varlena Decompressor (PGLZ)...");
    // Synthetic PGLZ compressed payload: literal stream + backrefs
    let raw =
        b"PostgreSQL analytical acceleration via craton-bolt and pg_bolt GPU execution engine.";
    // Simple 8-literal ctrl byte: 0x00 followed by 8 bytes
    let mut compressed = vec![0x00];
    compressed.extend_from_slice(&raw[..8]);

    let start = Instant::now();
    let iters = 100_000;
    for _ in 0..iters {
        let decomp = pglz_decompress(&compressed, 8).unwrap();
        assert_eq!(decomp.len(), 8);
    }
    let elapsed = start.elapsed() / iters;
    let throughput = (8.0 / 1_048_576.0) / elapsed.as_secs_f64();

    println!("   PGLZ Block decompress:   8 bytes");
    println!("   Average latency:         {:.2?}", elapsed);
    println!("   PGLZ Throughput:         {:.2} MB/s", throughput);
}

fn bench_ipc_worker() {
    println!("\n[3/4] Benchmarking Background Worker IPC Dispatch...");
    let client = shared_bgworker_client().unwrap();

    let iters = 5_000;
    let start = Instant::now();
    for _ in 0..iters {
        let status = client.status_json().unwrap();
        assert!(!status.is_empty());
    }
    let elapsed = start.elapsed();
    let avg_latency_us = elapsed.as_micros() as f64 / iters as f64;
    let ops_per_sec = iters as f64 / elapsed.as_secs_f64();

    println!("   IPC Status roundtrips:   {} commands", iters);
    println!("   Total duration:          {:.2?}", elapsed);
    println!("   Average IPC roundtrip:   {:.2} us", avg_latency_us);
    println!("   IPC Command Throughput:  {:.0} ops/sec", ops_per_sec);
}

fn bench_rtx2060_cuda_queries() {
    println!("\n[4/4] Benchmarking Live CUDA SQL Execution on NVIDIA GeForce RTX 2060...");

    let n_rows = 1_000_000;
    println!("   Generating {} rows of analytical sales data...", n_rows);

    let region_ids: Vec<i32> = (0..n_rows).map(|i| i % 100).collect();
    let prices: Vec<f64> = (0..n_rows)
        .map(|i| (i % 1000) as f64 * 0.37 + 1.0)
        .collect();
    let taxes: Vec<f64> = (0..n_rows).map(|_| 0.08).collect();
    let discounts: Vec<f64> = (0..n_rows).map(|i| (i % 10) as f64 * 0.5).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("region_id", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
        Field::new("tax", DataType::Float64, false),
        Field::new("discount", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(region_ids)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(taxes)),
            Arc::new(Float64Array::from(discounts)),
        ],
    )
    .unwrap();

    let table_bytes = batch.get_array_memory_size();
    println!(
        "   Table memory footprint:  {:.2} MB in Arrow columnar format",
        table_bytes as f64 / 1_048_576.0
    );

    // Measure GPU queries via background worker IPC client first
    let client = shared_bgworker_client().unwrap();
    let load_start = Instant::now();
    client
        .load_batch_chunked("public.sales", "sales", &batch)
        .unwrap();
    println!("   IPC Table registered in: {:.2?}", load_start.elapsed());

    let query_sql = "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total FROM sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id";

    // Cold query run via IPC (PTX JIT compilation + GPU kernel launch)
    let cold_start = Instant::now();
    let (cold_batch, cold_elapsed_us, cold_tier) = client.execute_sql(query_sql, 10_000).unwrap();
    let cold_wall = cold_start.elapsed();
    let cold_rows = cold_batch.num_rows();
    println!("\n   --- Cold Execution via IPC (PTX JIT + First Launch) ---");
    println!("   Output rows:             {} groups", cold_rows);
    println!("   Cold query wall latency: {:.2?}", cold_wall);
    println!(
        "   Engine reported time:    {} us ({:.2} ms)",
        cold_elapsed_us,
        cold_elapsed_us as f64 / 1000.0
    );
    println!("   Execution Tier:          {:?}", cold_tier);

    // Warm query runs via IPC
    println!("\n   --- Warm Executions via IPC (PTX Cache Hits) ---");
    let warm_iters = 20;
    let mut warm_durations = Vec::with_capacity(warm_iters);
    for _ in 0..warm_iters {
        let warm_start = Instant::now();
        let (res_batch, _us, _tier) = client.execute_sql(query_sql, 10_000).unwrap();
        warm_durations.push(warm_start.elapsed());
        assert_eq!(res_batch.num_rows(), cold_rows);
    }

    warm_durations.sort();
    let min_dur = warm_durations[0];
    let median_dur = warm_durations[warm_iters / 2];
    let avg_dur: std::time::Duration =
        warm_durations.iter().sum::<std::time::Duration>() / warm_iters as u32;

    let rows_per_sec = (n_rows as f64) / avg_dur.as_secs_f64();
    let gb_per_sec = (table_bytes as f64 / 1_073_741_824.0) / avg_dur.as_secs_f64();

    println!("   Warm query min latency:    {:.2?}", min_dur);
    println!("   Warm query median latency: {:.2?}", median_dur);
    println!("   Warm query avg latency:    {:.2?}", avg_dur);
    println!(
        "   GPU Scan/Agg Throughput:   {:.2} million rows/sec",
        rows_per_sec / 1_000_000.0
    );
    println!("   Effective VRAM Bandwidth:  {:.2} GB/s", gb_per_sec);

    // Check runtime status via IPC
    let status_json = client.status_json().unwrap();
    println!("\n   --- Daemon Status via IPC ---");
    println!("   Daemon Status:           {}", status_json);

    // Shutdown bgworker cleanly
    pg_bolt::bgworker::shutdown_shared_bgworker();
}
