// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use pg_bolt::arrow_bridge::{
    decode_heap_page_tuples, decode_heap_pages_to_arrow, encode_dictionary_utf8,
    encode_test_heap_page, map_pg_oid_to_arrow, record_batch_to_json_rows, rows_to_record_batch,
    rows_to_record_batch_with_typmods, PgColumnMeta, PgColumnSpec, PgDatumValue, SupportedPgType,
    BOOLOID, BPCHAROID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    NUMERICOID, PG_BLCKSZ, PG_NUMERIC_PRECISION, PG_NUMERIC_SCALE, PG_PAGE_HEADER_SIZE, TEXTOID,
    TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, UUIDOID, VARCHAROID,
};
use pg_bolt::error::PgBoltError;
use pg_bolt::ipc::{ipc_stream_to_record_batch, record_batch_to_ipc_stream};
use pg_bolt::mvcc::{HeapTupleHeaderMeta, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED};

fn live_header(natts: u16) -> HeapTupleHeaderMeta {
    HeapTupleHeaderMeta {
        t_xmin: 100,
        t_xmax: 0,
        t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
        t_infomask2: natts,
        t_hoff: 24,
        natts,
    }
}

#[test]
fn all_supported_pg_oids_map_and_unsupported_oids_fail() {
    assert_eq!(
        map_pg_oid_to_arrow(BOOLOID).unwrap(),
        (SupportedPgType::Bool, DataType::Boolean)
    );
    assert_eq!(
        map_pg_oid_to_arrow(INT2OID).unwrap(),
        (SupportedPgType::Int2, DataType::Int32)
    );
    assert_eq!(
        map_pg_oid_to_arrow(INT4OID).unwrap(),
        (SupportedPgType::Int4, DataType::Int32)
    );
    assert_eq!(
        map_pg_oid_to_arrow(INT8OID).unwrap(),
        (SupportedPgType::Int8, DataType::Int64)
    );
    assert_eq!(
        map_pg_oid_to_arrow(FLOAT4OID).unwrap(),
        (SupportedPgType::Float4, DataType::Float32)
    );
    assert_eq!(
        map_pg_oid_to_arrow(FLOAT8OID).unwrap(),
        (SupportedPgType::Float8, DataType::Float64)
    );
    assert_eq!(
        map_pg_oid_to_arrow(NUMERICOID).unwrap(),
        (
            SupportedPgType::Numeric,
            DataType::Decimal128(PG_NUMERIC_PRECISION, PG_NUMERIC_SCALE)
        )
    );
    assert_eq!(
        map_pg_oid_to_arrow(DATEOID).unwrap(),
        (SupportedPgType::Date, DataType::Date32)
    );
    assert_eq!(
        map_pg_oid_to_arrow(TIMESTAMPOID).unwrap(),
        (
            SupportedPgType::Timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, None)
        )
    );
    let (tstz_pg, tstz_dt) = map_pg_oid_to_arrow(TIMESTAMPTZOID).unwrap();
    assert_eq!(tstz_pg, SupportedPgType::Timestamptz);
    match tstz_dt {
        DataType::Timestamp(TimeUnit::Microsecond, Some(ref tz)) => {
            assert!(
                tz.as_ref() == "+00:00" || tz.as_ref() == "UTC",
                "expected UTC or +00:00 timezone for TIMESTAMPTZOID, got {tz}"
            );
        }
        other => panic!("unexpected TIMESTAMPTZOID Arrow type: {other:?}"),
    }

    for text_oid in [TEXTOID, VARCHAROID, BPCHAROID] {
        assert_eq!(
            map_pg_oid_to_arrow(text_oid).unwrap(),
            (SupportedPgType::Text, DataType::Utf8)
        );
    }

    // Extended PG types (`W1-B-006`): BYTEAOID, TIMEOID, UUIDOID.
    let (_, bytea_dt) = map_pg_oid_to_arrow(BYTEAOID).unwrap();
    assert_eq!(bytea_dt, DataType::Binary);
    let (_, time_dt) = map_pg_oid_to_arrow(TIMEOID).unwrap();
    assert_eq!(time_dt, DataType::Time64(TimeUnit::Microsecond));
    let (_, uuid_dt) = map_pg_oid_to_arrow(UUIDOID).unwrap();
    assert_eq!(uuid_dt, DataType::FixedSizeBinary(16));

    for unsupported_oid in [999_999, 1_000_001] {
        let err = map_pg_oid_to_arrow(unsupported_oid).unwrap_err();
        assert!(
            matches!(err, PgBoltError::Bridge(_)),
            "expected Bridge error for OID {unsupported_oid}, got {err:?}"
        );
    }
}

#[test]
fn rows_to_record_batch_handles_nulls_int2_widening_numeric_temporal_and_empty() {
    let cols = vec![
        PgColumnMeta {
            name: "small".into(),
            type_oid: INT2OID,
            nullable: true,
        },
        PgColumnMeta {
            name: "amount".into(),
            type_oid: NUMERICOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "order_date".into(),
            type_oid: DATEOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "created_at".into(),
            type_oid: TIMESTAMPTZOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "label".into(),
            type_oid: VARCHAROID,
            nullable: true,
        },
    ];

    // Empty 0-row table must preserve mapped Arrow schema.
    let empty = rows_to_record_batch(&cols, &[]).unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.num_columns(), 5);
    assert_eq!(empty.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(
        empty.schema().field(1).data_type(),
        &DataType::Decimal128(38, 10)
    );
    assert_eq!(empty.schema().field(2).data_type(), &DataType::Date32);

    let rows = vec![
        vec![
            PgDatumValue::Int16(i16::MIN),
            PgDatumValue::Numeric("123456789012345.1234567890".into()),
            PgDatumValue::DateDays(19_724), // 2024-01-02
            PgDatumValue::TimestampMicros(1_704_153_600_123_456),
            PgDatumValue::Text("first".into()),
        ],
        vec![
            PgDatumValue::Null,
            PgDatumValue::Numeric("-42.5".into()),
            PgDatumValue::Null,
            PgDatumValue::Null,
            PgDatumValue::Null,
        ],
        vec![
            PgDatumValue::Int16(i16::MAX),
            PgDatumValue::Null,
            PgDatumValue::DateDays(-1),
            PgDatumValue::TimestampMicros(0),
            PgDatumValue::Text("last".into()),
        ],
    ];

    let batch = rows_to_record_batch(&cols, &rows).unwrap();
    assert_eq!(batch.num_rows(), 3);

    let small = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(small.value(0), i32::from(i16::MIN));
    assert!(small.is_null(1));
    assert_eq!(small.value(2), i32::from(i16::MAX));

    let amount = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 1_234_567_890_123_451_234_567_890_i128);
    assert_eq!(amount.value(1), -425_000_000_000_i128);
    assert!(amount.is_null(2));

    let dates = batch
        .column(2)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.value(0), 19_724);
    assert!(dates.is_null(1));
    assert_eq!(dates.value(2), -1);

    // Silent precision loss beyond specified scale 10 must error cleanly.
    let too_precise = vec![vec![
        PgDatumValue::Int16(1),
        PgDatumValue::Numeric("0.00000000001".into()),
        PgDatumValue::Null,
        PgDatumValue::Null,
        PgDatumValue::Null,
    ]];
    let specs_scale10: Vec<PgColumnSpec> = cols
        .iter()
        .map(|c| {
            let mut s = PgColumnSpec::from_meta(c.clone());
            if s.meta.type_oid == NUMERICOID {
                s.typmod = Some(((38 << 16) | 10) + 4);
            }
            s
        })
        .collect();
    assert!(matches!(
        rows_to_record_batch_with_typmods(&specs_scale10, &too_precise),
        Err(PgBoltError::Bridge(_))
    ));
}

#[test]
fn record_batch_to_json_rows_formats_special_floats_dict_decimal_date_and_timestamp() {
    let regions = StringArray::from(vec![Some("us-east"), None, Some("eu-west")]);
    let dict = encode_dictionary_utf8(&regions).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("price", DataType::Decimal128(38, 10), true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));

    let prices = Decimal128Array::from(vec![
        Some(1_234_500_000_000_i128),
        Some(-5_000_000_000_i128),
        None,
    ])
    .with_data_type(DataType::Decimal128(38, 10));

    let ts = TimestampMicrosecondArray::from(vec![Some(0), Some(1_704_067_200_500_000), None])
        .with_timezone("UTC");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
            ])),
            Arc::new(dict),
            Arc::new(prices),
            Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])),
            Arc::new(ts),
        ],
    )
    .unwrap();

    let rows = record_batch_to_json_rows(&batch).unwrap();
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0]["score"], "NaN");
    assert_eq!(rows[0]["region"], "us-east");
    assert_eq!(rows[0]["price"], "123.4500000000");
    assert_eq!(rows[0]["day"], "1970-01-01");
    assert!(rows[0]["ts"].as_str().unwrap().starts_with("1970-01-01"));

    assert_eq!(rows[1]["score"], "Infinity");
    assert!(rows[1]["region"].is_null());
    assert_eq!(rows[1]["price"], "-0.5000000000");
    assert_eq!(rows[1]["day"], "2024-01-01");
    assert!(rows[1]["ts"].as_str().unwrap().starts_with("2024-01-01"));

    assert_eq!(rows[2]["score"], "-Infinity");
    assert_eq!(rows[2]["region"], "eu-west");
    assert!(rows[2]["price"].is_null());
    assert!(rows[2]["day"].is_null());
    assert!(rows[2]["ts"].is_null());
}

#[test]
fn heap_page_parallel_decode_handles_alignment_varlena_headers_nulls_and_errors() {
    let schema = vec![
        PgColumnMeta {
            name: "k".into(),
            type_oid: INT2OID,
            nullable: true,
        },
        PgColumnMeta {
            name: "big".into(),
            type_oid: INT8OID,
            nullable: true,
        },
        PgColumnMeta {
            name: "note".into(),
            type_oid: TEXTOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "active".into(),
            type_oid: BOOLOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "total".into(),
            type_oid: NUMERICOID,
            nullable: true,
        },
    ];

    let long_note = "bolt-4b-varlena-".repeat(16); // > 125 bytes -> triggers 4-byte 4B_U varlena header
    assert!(long_note.len() > 125);

    // Construct >= 6 pages so parallel scope-thread heap decoder is exercised end to end.
    let mut raw_pages: Vec<[u8; PG_BLCKSZ]> = Vec::new();
    for page_idx in 0..6_i16 {
        let row_short = (
            live_header(schema.len() as u16),
            vec![
                PgDatumValue::Int16(page_idx * 10 + 1),
                PgDatumValue::Int64(1_000_000_000_000 + i64::from(page_idx)),
                PgDatumValue::Text(format!("p{page_idx}-short")), // 1-byte short varlena
                PgDatumValue::Bool(page_idx % 2 == 0),
                PgDatumValue::Numeric(format!("{}.25", 100 + page_idx)),
            ],
        );
        let row_long_and_nulls = (
            live_header(schema.len() as u16),
            vec![
                PgDatumValue::Int16(-page_idx - 1),
                PgDatumValue::Null,
                PgDatumValue::Text(format!("{long_note}{page_idx}")), // 4-byte 4B_U varlena after null
                PgDatumValue::Bool(true),
                PgDatumValue::Null,
            ],
        );
        let page = encode_test_heap_page(&schema, &[row_short, row_long_and_nulls]).unwrap();
        let decoded_single = decode_heap_page_tuples(&schema, &page).unwrap();
        assert_eq!(decoded_single.len(), 2);
        raw_pages.push(page);
    }

    let page_refs: Vec<&[u8]> = raw_pages.iter().map(|p| p.as_slice()).collect();
    let batch = decode_heap_pages_to_arrow(&schema, &page_refs, None, None).unwrap();
    assert_eq!(batch.num_rows(), 12);

    let ks = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let bigs = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let notes = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actives = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let totals = batch
        .column(4)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    for page_idx in 0..6 {
        let even_row = page_idx * 2;
        assert_eq!(ks.value(even_row), (page_idx as i32) * 10 + 1);
        assert_eq!(bigs.value(even_row), 1_000_000_000_000 + page_idx as i64);
        assert_eq!(notes.value(even_row), format!("p{page_idx}-short"));
        assert_eq!(actives.value(even_row), page_idx % 2 == 0);
        assert_eq!(
            totals.value(even_row),
            ((100 + page_idx as i128) * 100 + 25) * 100_000_000_i128
        );

        let odd_row = even_row + 1;
        assert_eq!(ks.value(odd_row), -(page_idx as i32) - 1);
        assert!(bigs.is_null(odd_row));
        assert_eq!(notes.value(odd_row), format!("{long_note}{page_idx}"));
        assert!(actives.value(odd_row));
        assert!(totals.is_null(odd_row));
    }

    // Truncated / non-8KB page must fail cleanly.
    let truncated = vec![0_u8; 4096];
    assert!(matches!(
        decode_heap_pages_to_arrow(&schema, &[truncated.as_slice()], None, None),
        Err(PgBoltError::Bridge(_))
    ));

    // Corrupt PageHeaderData (`pd_lower` < 24 or `pd_lower` > `pd_upper`) must fail cleanly.
    let mut bad_header = [0_u8; PG_BLCKSZ];
    bad_header[12..14].copy_from_slice(&((PG_PAGE_HEADER_SIZE - 4) as u16).to_le_bytes());
    bad_header[14..16].copy_from_slice(&(PG_BLCKSZ as u16).to_le_bytes());
    bad_header[16..18].copy_from_slice(&(PG_BLCKSZ as u16).to_le_bytes());
    assert!(matches!(
        decode_heap_pages_to_arrow(&schema, &[bad_header.as_slice()], None, None),
        Err(PgBoltError::Bridge(_))
    ));

    // Corrupt tuple line pointer overflowing past `pd_special` must fail cleanly.
    let mut corrupt_lp = raw_pages[0];
    let lp_start = PG_PAGE_HEADER_SIZE;
    let bogus_item = (8000_u32) | (1_u32 << 15) | (500_u32 << 17);
    corrupt_lp[lp_start..lp_start + 4].copy_from_slice(&bogus_item.to_le_bytes());
    assert!(matches!(
        decode_heap_pages_to_arrow(&schema, &[corrupt_lp.as_slice()], None, None),
        Err(PgBoltError::Bridge(_))
    ));
}

#[test]
fn extended_pg_types_bytea_time_uuid_round_trip_and_json() {
    let cols = vec![
        PgColumnMeta {
            name: "payload".into(),
            type_oid: BYTEAOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "event_time".into(),
            type_oid: TIMEOID,
            nullable: true,
        },
        PgColumnMeta {
            name: "row_id".into(),
            type_oid: UUIDOID,
            nullable: true,
        },
    ];

    // Verify `rows_to_record_batch` accepts extended OIDs and preserves exact Arrow target types.
    let null_rows = vec![vec![
        PgDatumValue::Null,
        PgDatumValue::Null,
        PgDatumValue::Null,
    ]];
    let null_batch = rows_to_record_batch(&cols, &null_rows).unwrap();
    assert_eq!(null_batch.num_rows(), 1);
    assert_eq!(null_batch.schema().field(0).data_type(), &DataType::Binary);
    assert_eq!(
        null_batch.schema().field(1).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        null_batch.schema().field(2).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    let null_json = record_batch_to_json_rows(&null_batch).unwrap();
    assert_eq!(null_json.len(), 1);
    assert!(null_json[0]["payload"].is_null());
    assert!(null_json[0]["event_time"].is_null());
    assert!(null_json[0]["row_id"].is_null());

    // Round-trip populated extended columns through Arrow IPC stream and JSON serialization.
    let uuid_bytes: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Binary, true),
        Field::new("event_time", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new("row_id", DataType::FixedSizeBinary(16), true),
    ]));
    let payload = BinaryArray::from_iter(vec![Some(b"\xde\xad\xbe\xef".as_slice()), None]);
    let event_time = Time64MicrosecondArray::from(vec![Some(45_296_123_456_i64), None]); // 12:34:56.123456
    let row_id = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![Some(uuid_bytes), None].into_iter(),
        16,
    )
    .unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(payload), Arc::new(event_time), Arc::new(row_id)],
    )
    .unwrap();

    let stream = record_batch_to_ipc_stream(&batch).unwrap();
    let round_tripped = ipc_stream_to_record_batch(&stream).unwrap();
    assert_eq!(round_tripped, batch);

    let rt_payload = round_tripped
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(rt_payload.value(0), b"\xde\xad\xbe\xef");
    assert!(rt_payload.is_null(1));

    let rt_time = round_tripped
        .column(1)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(rt_time.value(0), 45_296_123_456);
    assert!(rt_time.is_null(1));

    let rt_uuid = round_tripped
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(rt_uuid.value(0), &uuid_bytes);
    assert!(rt_uuid.is_null(1));

    let json_rows = record_batch_to_json_rows(&round_tripped).unwrap();
    assert_eq!(json_rows.len(), 2);

    let payload_str = json_rows[0]["payload"]
        .as_str()
        .expect("bytea formats as JSON string");
    assert!(
        payload_str.to_ascii_lowercase().contains("deadbeef"),
        "unexpected bytea JSON representation: {payload_str}"
    );
    let time_str = json_rows[0]["event_time"]
        .as_str()
        .expect("time formats as JSON string");
    assert!(
        time_str.starts_with("12:34:56"),
        "unexpected time JSON representation: {time_str}"
    );
    let uuid_str = json_rows[0]["row_id"]
        .as_str()
        .expect("uuid formats as JSON string");
    assert_eq!(
        uuid_str.replace('-', "").to_ascii_lowercase(),
        "00112233445566778899aabbccddeeff"
    );

    assert!(json_rows[1]["payload"].is_null());
    assert!(json_rows[1]["event_time"].is_null());
    assert!(json_rows[1]["row_id"].is_null());
}
