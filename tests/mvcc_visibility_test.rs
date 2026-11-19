// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pg_bolt::arrow_bridge::{
    decode_heap_pages_to_arrow, encode_test_heap_page, PgColumnMeta, PgDatumValue, FLOAT8OID,
    INT4OID, PG_BLCKSZ,
};
use pg_bolt::error::PgBoltError;
use pg_bolt::mvcc::{
    append_visibility_column, build_visibility_bitmap, filter_batch_by_visibility,
    is_tuple_visible, HeapTupleHeaderMeta, MvccSnapshot, TransactionId, VisibilityMap,
    BOOTSTRAP_XID, FROZEN_XID, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_LOCK_ONLY, HEAP_XMIN_COMMITTED, HEAP_XMIN_FROZEN, HEAP_XMIN_INVALID, INVALID_XID,
    VM_ALL_FROZEN, VM_ALL_VISIBLE,
};

#[cfg(not(feature = "cuda-stub"))]
use arrow_array::Float64Array;
#[cfg(not(feature = "cuda-stub"))]
use pg_bolt::runtime::{test_process_lock, BoltSessionRuntime};

fn tuple_hdr(xmin: TransactionId, xmax: TransactionId, infomask: u16) -> HeapTupleHeaderMeta {
    HeapTupleHeaderMeta {
        t_xmin: xmin,
        t_xmax: xmax,
        t_infomask: infomask,
        t_infomask2: 4,
        t_hoff: 24,
        natts: 4,
    }
}

#[test]
fn is_tuple_visible_enforces_postgresql_mvcc_and_wraparound_rules() {
    let snap = MvccSnapshot::new(100, 200, vec![150, 175]).with_committed([120, 140]);

    // Frozen tuple (both HEAP_XMIN_FROZEN and FROZEN_XID) is always visible.
    assert!(is_tuple_visible(
        &tuple_hdr(999, INVALID_XID, HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID),
        &snap
    ));
    assert!(is_tuple_visible(
        &tuple_hdr(
            FROZEN_XID,
            INVALID_XID,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID
        ),
        &snap
    ));
    assert!(is_tuple_visible(
        &tuple_hdr(BOOTSTRAP_XID, INVALID_XID, HEAP_XMAX_INVALID),
        &snap
    ));

    // Aborted xmin is never visible.
    assert!(!is_tuple_visible(
        &tuple_hdr(50, INVALID_XID, HEAP_XMIN_INVALID | HEAP_XMAX_INVALID),
        &snap
    ));

    // Uncommitted active xmin is invisible even if hint bit were set.
    assert!(!is_tuple_visible(
        &tuple_hdr(150, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
        &snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(175, INVALID_XID, HEAP_XMAX_INVALID),
        &snap
    ));

    // Hint-bit-less committed xmin uses committed_xids oracle; uncommitted does not.
    assert!(is_tuple_visible(
        &tuple_hdr(120, INVALID_XID, HEAP_XMAX_INVALID),
        &snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(121, INVALID_XID, HEAP_XMAX_INVALID),
        &snap
    ));

    // Future xmin (>= xmax) is invisible.
    assert!(!is_tuple_visible(
        &tuple_hdr(200, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
        &snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(250, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
        &snap
    ));

    // Committed delete visible to snapshot (< xmax and not active) hides row.
    assert!(!is_tuple_visible(
        &tuple_hdr(80, 140, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
        &snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(80, 90, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
        &snap
    ));

    // Future xmax (>= xmax) does NOT delete row for this snapshot.
    assert!(is_tuple_visible(
        &tuple_hdr(80, 200, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
        &snap
    ));
    assert!(is_tuple_visible(
        &tuple_hdr(80, 250, HEAP_XMIN_COMMITTED),
        &snap
    ));

    // Active/uncommitted deleter in active_xids or uncommitted hintless xmax keeps row visible.
    assert!(is_tuple_visible(
        &tuple_hdr(80, 150, HEAP_XMIN_COMMITTED),
        &snap
    ));
    assert!(is_tuple_visible(
        &tuple_hdr(80, 131, HEAP_XMIN_COMMITTED),
        &snap
    ));

    // Row lock only (`SELECT ... FOR UPDATE` -> HEAP_XMAX_LOCK_ONLY) must NOT hide row.
    assert!(is_tuple_visible(
        &tuple_hdr(
            80,
            140,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_COMMITTED
        ),
        &snap
    ));

    // MultiXact lock-only / uncommitted stays visible; committed non-lock MultiXact hidden.
    assert!(is_tuple_visible(
        &tuple_hdr(
            80,
            140,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_COMMITTED
        ),
        &snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(
            80,
            140,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI | HEAP_XMAX_COMMITTED
        ),
        &snap
    ));

    // Wraparound-aware modulo-2^32 comparison across u32::MAX -> small normal XIDs.
    let wrap_snap = MvccSnapshot::new(u32::MAX - 4, 20, vec![7]).with_committed([u32::MAX, 4, 15]);
    assert!(is_tuple_visible(
        &tuple_hdr(u32::MAX, INVALID_XID, HEAP_XMAX_INVALID),
        &wrap_snap
    ));
    assert!(is_tuple_visible(
        &tuple_hdr(4, INVALID_XID, HEAP_XMAX_INVALID),
        &wrap_snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(7, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
        &wrap_snap
    ));
    assert!(!is_tuple_visible(
        &tuple_hdr(u32::MAX - 20, 15, HEAP_XMIN_COMMITTED),
        &wrap_snap
    ));
}

#[test]
fn visibility_map_and_arrow_bitmap_filtering_work_end_to_end() {
    let mut vm = VisibilityMap::new(6);
    vm.set_page_flags(0, false, false);
    vm.set_page_flags(1, true, false);
    vm.set_page_flags(2, true, true);
    vm.set_page_flags(5, true, false);

    assert_eq!(vm.page_flags(0), (false, false));
    assert!(vm.is_all_visible(1));
    assert!(!vm.is_all_frozen(1));
    assert!(vm.is_all_visible(2));
    assert!(vm.is_all_frozen(2));
    assert!(vm.is_all_visible(5));
    assert_eq!(
        vm.as_bytes()[0] & 0x3F,
        (VM_ALL_VISIBLE << 2) | ((VM_ALL_VISIBLE | VM_ALL_FROZEN) << 4)
    );

    let roundtrip = VisibilityMap::from_bytes(vm.as_bytes(), 6).unwrap();
    assert_eq!(roundtrip, vm);

    // Truncated VM buffer errors cleanly.
    assert!(matches!(
        VisibilityMap::from_bytes(&[0u8; 1], 5),
        Err(PgBoltError::Mvcc(_))
    ));

    let snap = MvccSnapshot::new(100, 200, vec![150]).with_committed([120]);
    let headers = vec![
        tuple_hdr(50, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID), // live
        tuple_hdr(50, INVALID_XID, HEAP_XMIN_INVALID | HEAP_XMAX_INVALID),   // aborted
        tuple_hdr(150, INVALID_XID, HEAP_XMAX_INVALID),                      // active xmin
        tuple_hdr(
            60,
            120,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_COMMITTED,
        ), // FOR UPDATE lock -> live
        tuple_hdr(60, 120, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),       // committed delete
    ];
    let bitmap: BooleanArray = build_visibility_bitmap(&headers, &snap);
    assert_eq!(bitmap.len(), 5);
    assert!(bitmap.value(0));
    assert!(!bitmap.value(1));
    assert!(!bitmap.value(2));
    assert!(bitmap.value(3));
    assert!(!bitmap.value(4));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "live",
                "aborted",
                "uncommitted",
                "locked",
                "deleted",
            ])),
        ],
    )
    .unwrap();

    let filtered = filter_batch_by_visibility(&batch, &bitmap).unwrap();
    assert_eq!(filtered.num_rows(), 2);
    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 4]);

    let gated = append_visibility_column(&batch, &bitmap, "_mvcc_visible").unwrap();
    assert_eq!(gated.num_columns(), 3);
    assert_eq!(gated.num_rows(), 5);
    let vis_col = gated
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(vis_col.value(0));
    assert!(!vis_col.value(1));
    assert!(vis_col.value(3));
    assert!(!vis_col.value(4));

    // Validation errors on mismatched bitmap length and duplicate/empty visibility column.
    let short_bitmap = BooleanArray::from(vec![true, false]);
    assert!(matches!(
        filter_batch_by_visibility(&batch, &short_bitmap),
        Err(PgBoltError::Mvcc(_))
    ));
    assert!(matches!(
        append_visibility_column(&batch, &bitmap, ""),
        Err(PgBoltError::Mvcc(_))
    ));
    assert!(matches!(
        append_visibility_column(&batch, &bitmap, "id"),
        Err(PgBoltError::Mvcc(_))
    ));
}

fn build_mixed_mvcc_pages() -> (
    Vec<PgColumnMeta>,
    Vec<[u8; PG_BLCKSZ]>,
    MvccSnapshot,
    VisibilityMap,
) {
    let schema = vec![
        PgColumnMeta {
            name: "region_id".into(),
            type_oid: INT4OID,
            nullable: false,
        },
        PgColumnMeta {
            name: "price".into(),
            type_oid: FLOAT8OID,
            nullable: false,
        },
        PgColumnMeta {
            name: "tax".into(),
            type_oid: FLOAT8OID,
            nullable: false,
        },
        PgColumnMeta {
            name: "discount".into(),
            type_oid: FLOAT8OID,
            nullable: false,
        },
    ];

    let snap = MvccSnapshot::new(100, 200, vec![150]).with_committed([120]);
    let mut vm = VisibilityMap::new(5);
    // Pages 1 and 2 are all-visible (page 2 is also all-frozen); pages 0, 3, 4 require per-tuple checks.
    vm.set_page_flags(1, true, false);
    vm.set_page_flags(2, true, true);

    let row = |region_id: i32, price: f64, tax: f64, discount: f64| {
        vec![
            PgDatumValue::Int32(region_id),
            PgDatumValue::Float64(price),
            PgDatumValue::Float64(tax),
            PgDatumValue::Float64(discount),
        ]
    };

    // Page 0 (mixed live, aborted, uncommitted active, locked-only, committed-deleted):
    // Live contributions:
    //   region 10: 100*(1.10) - 5 = 105.0
    //   region 10 (FOR UPDATE locked): 50*(1.10) - 2 = 53.0
    let page0 = encode_test_heap_page(
        &schema,
        &[
            (
                tuple_hdr(80, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
                row(10, 100.0, 0.10, 5.0),
            ),
            (
                tuple_hdr(90, INVALID_XID, HEAP_XMIN_INVALID | HEAP_XMAX_INVALID), // aborted -> excluded
                row(10, 999.0, 0.50, 0.0),
            ),
            (
                tuple_hdr(150, INVALID_XID, HEAP_XMAX_INVALID), // active uncommitted -> excluded
                row(20, 777.0, 0.25, 0.0),
            ),
            (
                tuple_hdr(
                    85,
                    120,
                    HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_COMMITTED,
                ), // locked only -> live
                row(10, 50.0, 0.10, 2.0),
            ),
            (
                tuple_hdr(85, 120, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED), // committed delete -> excluded
                row(20, 888.0, 0.10, 0.0),
            ),
        ],
    )
    .unwrap();

    // Page 1 (all-visible fast-path): region 20 -> 200*(1.05) - 10 = 200.0
    let page1 = encode_test_heap_page(
        &schema,
        &[(
            tuple_hdr(60, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID),
            row(20, 200.0, 0.05, 10.0),
        )],
    )
    .unwrap();

    // Page 2 (all-visible + all-frozen fast-path): region 20 -> 100*(1.20) - 0 = 120.0
    let page2 = encode_test_heap_page(
        &schema,
        &[(
            tuple_hdr(
                FROZEN_XID,
                INVALID_XID,
                HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID,
            ),
            row(20, 100.0, 0.20, 0.0),
        )],
    )
    .unwrap();

    // Page 3 (active deleter xmax=150 stays visible + future xmax=220 stays visible):
    //   region 30 (deleted by in-progress xid 150 -> live): 80*(1.25) - 0 = 100.0
    //   region 75 (future deleter xmax 220 -> live row, filtered out later by SQL `region_id < 50`): 500*(1.0)
    let page3 = encode_test_heap_page(
        &schema,
        &[
            (
                tuple_hdr(90, 150, HEAP_XMIN_COMMITTED),
                row(30, 80.0, 0.25, 0.0),
            ),
            (
                tuple_hdr(90, 220, HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED),
                row(75, 500.0, 0.0, 0.0),
            ),
        ],
    )
    .unwrap();

    // Page 4 (future insert xmin=210 excluded; hintless committed xmin=120 included):
    //   region 30 -> 40*(1.10) - 4 = 40.0
    let page4 = encode_test_heap_page(
        &schema,
        &[
            (
                tuple_hdr(210, INVALID_XID, HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID), // future insert -> excluded
                row(30, 999.0, 0.90, 0.0),
            ),
            (
                tuple_hdr(120, INVALID_XID, HEAP_XMAX_INVALID), // hintless committed -> live
                row(30, 40.0, 0.10, 4.0),
            ),
        ],
    )
    .unwrap();

    (schema, vec![page0, page1, page2, page3, page4], snap, vm)
}

#[test]
fn decode_heap_pages_filters_mixed_live_dead_uncommitted_tuples_and_uses_vm() {
    let (schema, pages, snap, vm) = build_mixed_mvcc_pages();
    let refs: Vec<&[u8]> = pages.iter().map(|p| p.as_slice()).collect();

    // Without snapshot/VM all 11 physical tuples are decoded.
    let unfiltered = decode_heap_pages_to_arrow(&schema, &refs, None, None).unwrap();
    assert_eq!(unfiltered.num_rows(), 11);

    // With snapshot + VM across 5 pages (parallel scope-thread path), only the 7 visible tuples survive.
    let filtered = decode_heap_pages_to_arrow(&schema, &refs, Some(&snap), Some(&vm)).unwrap();
    assert_eq!(filtered.num_rows(), 7);

    let regions = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(regions.values(), &[10, 10, 20, 20, 30, 75, 30]);

    // Verify all-visible VM flag skips tuple-level filtering on page 1 when tested explicitly:
    // put an uncommitted header on page 1, mark page 1 all-visible in VM, vs unset VM.
    let single_page = encode_test_heap_page(
        &schema,
        &[(
            tuple_hdr(150, INVALID_XID, HEAP_XMAX_INVALID),
            vec![
                PgDatumValue::Int32(99),
                PgDatumValue::Float64(1.0),
                PgDatumValue::Float64(0.0),
                PgDatumValue::Float64(0.0),
            ],
        )],
    )
    .unwrap();
    let no_vm = decode_heap_pages_to_arrow(&schema, &[&single_page], Some(&snap), None).unwrap();
    assert_eq!(no_vm.num_rows(), 0);

    let mut all_vis_vm = VisibilityMap::new(1);
    all_vis_vm.set_page_flags(0, true, false);
    let with_vm =
        decode_heap_pages_to_arrow(&schema, &[&single_page], Some(&snap), Some(&all_vis_vm))
            .unwrap();
    assert_eq!(with_vm.num_rows(), 1);
}

#[cfg(not(feature = "cuda-stub"))]
#[test]
fn gpu_query_over_mvcc_filtered_heap_pages_excludes_dead_and_uncommitted_rows() {
    let _guard = test_process_lock();
    let (schema, pages, snap, vm) = build_mixed_mvcc_pages();
    let refs: Vec<&[u8]> = pages.iter().map(|p| p.as_slice()).collect();

    let visible_batch = decode_heap_pages_to_arrow(&schema, &refs, Some(&snap), Some(&vm)).unwrap();
    assert_eq!(visible_batch.num_rows(), 7);

    let mut rt = BoltSessionRuntime::new().expect("CUDA BoltSessionRuntime");
    rt.register_batch("public.mvcc_sales", "mvcc_sales", visible_batch)
        .unwrap();

    let sql = "SELECT region_id, SUM((price * (1.0 + tax)) - discount) AS net_total \
               FROM mvcc_sales WHERE region_id < 50 GROUP BY region_id ORDER BY region_id";
    let res = rt.execute_sql(sql).expect("GPU query over MVCC batch");
    assert_eq!(res.batch.num_rows(), 3);

    let regions = res
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let totals = res
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // Expected live sums:
    //   region 10: 105.0 + 53.0 = 158.0
    //   region 20: 200.0 + 120.0 = 320.0
    //   region 30: 100.0 + 40.0 = 140.0
    assert_eq!(regions.value(0), 10);
    assert!((totals.value(0) - 158.0).abs() < 1e-6);
    assert_eq!(regions.value(1), 20);
    assert!((totals.value(1) - 320.0).abs() < 1e-6);
    assert_eq!(regions.value(2), 30);
    assert!((totals.value(2) - 140.0).abs() < 1e-6);
}
