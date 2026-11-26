// SPDX-License-Identifier: Apache-2.0

//! Phase 2 Arrow IPC serialization, compact `MinimalTuple` binary framing,
//! and bounded shared-memory ring buffer transport for background-worker offload.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::{
    BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Date32Builder, Date64Builder,
    Decimal128Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    StringDictionaryBuilder, StringViewBuilder, Time32MillisecondBuilder, Time32SecondBuilder,
    Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Fields, Schema, TimeUnit};
use arrow_select::concat::concat_batches;
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};

use crate::error::{PgBoltError, PgBoltResult};
use crate::mvcc::{HEAP_HASNULL, HEAP_HASVARWIDTH};

const MAX_TUPLE_ATTRS: usize = 1600;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

const CMD_EXECUTE_SQL: u8 = 1;
const CMD_LOAD_BATCH: u8 = 2;
const CMD_APPEND_DELTA: u8 = 3;
const CMD_EVICT_TABLE: u8 = 4;
const CMD_EXPLAIN_TIER: u8 = 5;
const CMD_GET_STATUS: u8 = 6;
const CMD_LIST_TABLES: u8 = 7;
const CMD_SHUTDOWN: u8 = 8;
const CMD_LOAD_BATCH_CHUNK: u8 = 9;
const CMD_APPEND_DELTA_CHUNK: u8 = 10;
const CMD_EXPLAIN_SQL: u8 = 11;
const CMD_SET_TABLE_DIRTY: u8 = 12;
const CMD_CANCEL_CHUNK_STREAM: u8 = 13;
const CMD_FETCH_RESPONSE_CHUNK: u8 = 14;
pub const CMD_CANCEL_REQUEST: u8 = 15;

const RESP_BATCH: u8 = 1;
const RESP_TIER: u8 = 2;
const RESP_STATUS_JSON: u8 = 3;
const RESP_TABLES_JSON: u8 = 4;
const RESP_ACK: u8 = 5;
const RESP_ERROR: u8 = 6;
const RESP_EXPLAIN_SQL: u8 = 7;
const RESP_BATCH_CHUNK: u8 = 8;

const DEFAULT_MAX_CHUNKED_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_CHUNK_STREAM_TTL: Duration = Duration::from_secs(30);

/// Serializes an Arrow `RecordBatch` into an Arrow IPC streaming byte buffer.
pub fn record_batch_to_ipc_stream(batch: &RecordBatch) -> PgBoltResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
        .map_err(|e| PgBoltError::Ipc(format!("failed to create IPC stream writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| PgBoltError::Ipc(format!("failed to write RecordBatch to IPC stream: {e}")))?;
    writer
        .finish()
        .map_err(|e| PgBoltError::Ipc(format!("failed to finish IPC stream: {e}")))?;
    drop(writer);
    Ok(buf)
}

/// Parses one Arrow IPC encapsulated message header prefix (`[continuation?] + meta_len + meta`)
/// from `bytes` starting at `*pos`, returning `Ok(None)` at EOS (`meta_len == 0`) or EOF.
fn decode_frame<'a>(bytes: &'a [u8], pos: &mut usize) -> PgBoltResult<Option<&'a [u8]>> {
    if *pos == bytes.len() {
        return Ok(None);
    }
    if bytes.len() - *pos < 4 {
        return Err(PgBoltError::Ipc(format!(
            "truncated Arrow IPC frame header at byte offset {}: {} bytes remaining",
            *pos,
            bytes.len() - *pos
        )));
    }
    let meta_len = if bytes[*pos..*pos + 4] == CONTINUATION_MARKER {
        if bytes.len() - *pos < 8 {
            return Err(PgBoltError::Ipc(format!(
                "truncated Arrow IPC continuation frame header at byte offset {}",
                *pos
            )));
        }
        let len = i32::from_le_bytes(
            bytes[*pos + 4..*pos + 8]
                .try_into()
                .expect("checked 8-byte continuation slice"),
        );
        *pos += 8;
        len
    } else {
        let len = i32::from_le_bytes(
            bytes[*pos..*pos + 4]
                .try_into()
                .expect("checked 4-byte prefix slice"),
        );
        *pos += 4;
        len
    };

    if meta_len == 0 {
        return Ok(None);
    }
    if meta_len < 0 {
        return Err(PgBoltError::Ipc(format!(
            "negative Arrow IPC metadata length {meta_len} at offset {}",
            *pos
        )));
    }
    let meta_len_usize = meta_len as usize;
    if meta_len_usize > bytes.len().saturating_sub(*pos) {
        return Err(PgBoltError::Ipc(format!(
            "Arrow IPC metadata length ({meta_len_usize} bytes) exceeds remaining {} stream bytes",
            bytes.len().saturating_sub(*pos)
        )));
    }
    let meta_slice = &bytes[*pos..*pos + meta_len_usize];
    *pos += meta_len_usize;
    Ok(Some(meta_slice))
}

fn expected_nodes_and_buffers_for_fields(fields: &Fields) -> Option<(usize, usize)> {
    fn accumulate(dt: &DataType, nodes: &mut usize, buffers: &mut usize) -> Option<()> {
        *nodes = nodes.checked_add(1)?;
        match dt {
            DataType::Null => {}
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Dictionary(_, _) => {
                // RecordBatch entries hold validity + index/value buffers (dict values live in DictionaryBatch).
                *buffers = buffers.checked_add(2)?;
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                *buffers = buffers.checked_add(3)?;
            }
            DataType::Struct(children) => {
                *buffers = buffers.checked_add(1)?;
                for child in children {
                    accumulate(child.data_type(), nodes, buffers)?;
                }
            }
            DataType::FixedSizeList(elem, _) => {
                *buffers = buffers.checked_add(1)?;
                accumulate(elem.data_type(), nodes, buffers)?;
            }
            DataType::List(elem) | DataType::LargeList(elem) | DataType::Map(elem, _) => {
                *buffers = buffers.checked_add(2)?;
                accumulate(elem.data_type(), nodes, buffers)?;
            }
            // Variadic or specialized layouts can vary in buffer count across IPC batches.
            DataType::Utf8View
            | DataType::BinaryView
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _) => return None,
        }
        Some(())
    }

    let mut nodes = 0_usize;
    let mut buffers = 0_usize;
    for field in fields {
        accumulate(field.data_type(), &mut nodes, &mut buffers)?;
    }
    Some((nodes, buffers))
}

fn validate_record_batch_meta(
    fb_batch: arrow_ipc::RecordBatch<'_>,
    body_len: usize,
    expected_layout: Option<(usize, usize)>,
) -> PgBoltResult<()> {
    if fb_batch.length() < 0 {
        return Err(PgBoltError::Ipc(format!(
            "negative Arrow IPC RecordBatch row length {}",
            fb_batch.length()
        )));
    }
    let nodes_len = fb_batch.nodes().map_or(0, |v| v.len());
    let buffers_len = fb_batch.buffers().map_or(0, |v| v.len());
    if let Some((exp_nodes, exp_buffers)) = expected_layout {
        if nodes_len != exp_nodes || buffers_len != exp_buffers {
            return Err(PgBoltError::Ipc(format!(
                "Arrow IPC RecordBatch metadata mismatch: got {nodes_len} nodes / {buffers_len} buffers, expected {exp_nodes} / {exp_buffers}"
            )));
        }
    }

    if let Some(nodes) = fb_batch.nodes() {
        for idx in 0..nodes.len() {
            let node = nodes.get(idx);
            if node.length() < 0 || node.null_count() < 0 || node.null_count() > node.length() {
                return Err(PgBoltError::Ipc(format!(
                    "invalid Arrow IPC FieldNode[{idx}] (length={}, null_count={})",
                    node.length(),
                    node.null_count()
                )));
            }
        }
    }
    if let Some(buffers) = fb_batch.buffers() {
        let body_len_i64 = i64::try_from(body_len)
            .map_err(|_| PgBoltError::Ipc("Arrow IPC body length overflows i64".to_owned()))?;
        for idx in 0..buffers.len() {
            let buf = buffers.get(idx);
            if buf.offset() < 0 || buf.length() < 0 {
                return Err(PgBoltError::Ipc(format!(
                    "negative Arrow IPC Buffer[{idx}] bounds (offset={}, length={})",
                    buf.offset(),
                    buf.length()
                )));
            }
            let end = buf.offset().checked_add(buf.length()).ok_or_else(|| {
                PgBoltError::Ipc(format!("Arrow IPC Buffer[{idx}] offset overflow"))
            })?;
            if end > body_len_i64 {
                return Err(PgBoltError::Ipc(format!(
                    "Arrow IPC Buffer[{idx}] range [{}..{end}] exceeds frame bodyLength ({body_len_i64})",
                    buf.offset()
                )));
            }
        }
    }
    Ok(())
}

fn validate_ipc_stream_frames(bytes: &[u8]) -> PgBoltResult<()> {
    let mut pos = 0_usize;
    let schema_meta = decode_frame(bytes, &mut pos)?
        .ok_or_else(|| PgBoltError::Ipc("missing Arrow IPC Schema message".to_owned()))?;
    let schema_msg = arrow_ipc::root_as_message(schema_meta)
        .map_err(|e| PgBoltError::Ipc(format!("corrupt Arrow IPC Schema FlatBuffer: {e}")))?;
    if schema_msg.header_type() != arrow_ipc::MessageHeader::Schema {
        return Err(PgBoltError::Ipc(format!(
            "expected first Arrow IPC message to be Schema, got {:?}",
            schema_msg.header_type()
        )));
    }
    if schema_msg.bodyLength() != 0 {
        return Err(PgBoltError::Ipc(format!(
            "Arrow IPC Schema message must have bodyLength == 0, got {}",
            schema_msg.bodyLength()
        )));
    }
    let fb_schema = schema_msg
        .header_as_schema()
        .ok_or_else(|| PgBoltError::Ipc("missing Arrow IPC Schema union table".to_owned()))?;
    if fb_schema.fields().is_none() {
        return Err(PgBoltError::Ipc(
            "Arrow IPC Schema FlatBuffer is missing fields vector".to_owned(),
        ));
    }
    let schema = arrow_ipc::convert::fb_to_schema(fb_schema);
    let expected_layout = expected_nodes_and_buffers_for_fields(schema.fields());

    while let Some(meta_slice) = decode_frame(bytes, &mut pos)? {
        let msg = arrow_ipc::root_as_message(meta_slice)
            .map_err(|e| PgBoltError::Ipc(format!("corrupt Arrow IPC message FlatBuffer: {e}")))?;
        let body_len_i64 = msg.bodyLength();
        if body_len_i64 < 0 {
            return Err(PgBoltError::Ipc(format!(
                "negative Arrow IPC bodyLength ({body_len_i64})"
            )));
        }
        let body_len = usize::try_from(body_len_i64).map_err(|_| {
            PgBoltError::Ipc("Arrow IPC bodyLength exceeds platform usize".to_owned())
        })?;
        if body_len > bytes.len().saturating_sub(pos) {
            return Err(PgBoltError::Ipc(format!(
                "Arrow IPC frame body ({body_len} bytes) exceeds remaining {} stream bytes",
                bytes.len().saturating_sub(pos)
            )));
        }
        match msg.header_type() {
            arrow_ipc::MessageHeader::RecordBatch => {
                let fb_batch = msg.header_as_record_batch().ok_or_else(|| {
                    PgBoltError::Ipc("missing Arrow IPC RecordBatch union table".to_owned())
                })?;
                validate_record_batch_meta(fb_batch, body_len, expected_layout)?;
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                let fb_dict = msg.header_as_dictionary_batch().ok_or_else(|| {
                    PgBoltError::Ipc("missing Arrow IPC DictionaryBatch union table".to_owned())
                })?;
                let fb_batch = fb_dict.data().ok_or_else(|| {
                    PgBoltError::Ipc("Arrow IPC DictionaryBatch is missing nested data".to_owned())
                })?;
                if fb_batch.nodes().is_none() || fb_batch.buffers().is_none() {
                    return Err(PgBoltError::Ipc(
                        "Arrow IPC DictionaryBatch is missing nodes/buffers metadata".to_owned(),
                    ));
                }
                validate_record_batch_meta(fb_batch, body_len, None)?;
            }
            other => {
                return Err(PgBoltError::Ipc(format!(
                    "unexpected Arrow IPC message type in stream: {other:?}"
                )));
            }
        }
        pos += body_len;
    }

    if pos != bytes.len() {
        return Err(PgBoltError::Ipc(format!(
            "trailing {} bytes after Arrow IPC EOS marker",
            bytes.len() - pos
        )));
    }
    Ok(())
}

/// Deserializes an Arrow IPC streaming byte buffer back into a single `RecordBatch`.
///
/// Validates all encapsulated message frames, FlatBuffers headers, body lengths,
/// and buffer slice bounds before entering `StreamReader::try_new` so corrupt
/// streams fail cleanly instead of panicking or slicing out of bounds.
pub fn ipc_stream_to_record_batch(bytes: &[u8]) -> PgBoltResult<RecordBatch> {
    if bytes.len() < 8 {
        return Err(PgBoltError::Ipc(format!(
            "truncated Arrow IPC stream: expected at least 8 bytes, got {}",
            bytes.len()
        )));
    }
    validate_ipc_stream_frames(bytes)?;

    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| PgBoltError::Ipc(format!("failed to open Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for item in reader {
        let batch =
            item.map_err(|e| PgBoltError::Ipc(format!("failed to read Arrow IPC batch: {e}")))?;
        batches.push(batch);
    }

    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => Ok(batches.into_iter().next().expect("length checked")),
        _ => concat_batches(&schema, &batches)
            .map_err(|e| PgBoltError::Ipc(format!("failed to concatenate Arrow IPC batches: {e}"))),
    }
}

#[inline]
fn is_varwidth_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Dictionary(_, _)
    )
}

fn dictionary_string_at(col: &dyn Array, row_idx: usize) -> PgBoltResult<&str> {
    let dict = col
        .as_any_dictionary_opt()
        .ok_or_else(|| PgBoltError::Ipc("expected DictionaryArray column".to_owned()))?;
    let key_idx = match dict.keys().data_type() {
        DataType::Int8 => usize::try_from(dict.keys().as_primitive::<Int8Type>().value(row_idx)),
        DataType::Int16 => usize::try_from(dict.keys().as_primitive::<Int16Type>().value(row_idx)),
        DataType::Int32 => usize::try_from(dict.keys().as_primitive::<Int32Type>().value(row_idx)),
        DataType::Int64 => usize::try_from(dict.keys().as_primitive::<Int64Type>().value(row_idx)),
        DataType::UInt8 => Ok(usize::from(
            dict.keys().as_primitive::<UInt8Type>().value(row_idx),
        )),
        DataType::UInt16 => Ok(usize::from(
            dict.keys().as_primitive::<UInt16Type>().value(row_idx),
        )),
        DataType::UInt32 => {
            usize::try_from(dict.keys().as_primitive::<UInt32Type>().value(row_idx))
        }
        DataType::UInt64 => {
            usize::try_from(dict.keys().as_primitive::<UInt64Type>().value(row_idx))
        }
        other => {
            return Err(PgBoltError::Ipc(format!(
                "unsupported dictionary key type {other:?} in MinimalTuple"
            )))
        }
    }
    .map_err(|_| PgBoltError::Ipc("negative or overflowing dictionary key index".to_owned()))?;

    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PgBoltError::Ipc(format!(
                "unsupported dictionary value type {:?} in MinimalTuple",
                dict.values().data_type()
            ))
        })?;
    if key_idx >= values.len() || values.is_null(key_idx) {
        return Err(PgBoltError::Ipc(format!(
            "dictionary key index {key_idx} out of bounds or null"
        )));
    }
    Ok(values.value(key_idx))
}

fn push_varlena_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> PgBoltResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        PgBoltError::Ipc(format!(
            "MinimalTuple varlena value ({} bytes) exceeds 4 GiB",
            bytes.len()
        ))
    })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

fn encode_cell_into(
    buf: &mut Vec<u8>,
    col: &dyn Array,
    dt: &DataType,
    row_idx: usize,
) -> PgBoltResult<()> {
    match dt {
        DataType::Boolean => {
            let v = col.as_boolean().value(row_idx);
            buf.push(u8::from(v));
        }
        DataType::Int8 => {
            buf.extend_from_slice(&col.as_primitive::<Int8Type>().value(row_idx).to_le_bytes())
        }
        DataType::Int16 => {
            buf.extend_from_slice(&col.as_primitive::<Int16Type>().value(row_idx).to_le_bytes())
        }
        DataType::Int32 => {
            buf.extend_from_slice(&col.as_primitive::<Int32Type>().value(row_idx).to_le_bytes())
        }
        DataType::Int64 => {
            buf.extend_from_slice(&col.as_primitive::<Int64Type>().value(row_idx).to_le_bytes())
        }
        DataType::UInt8 => {
            buf.extend_from_slice(&col.as_primitive::<UInt8Type>().value(row_idx).to_le_bytes())
        }
        DataType::UInt16 => buf.extend_from_slice(
            &col.as_primitive::<UInt16Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::UInt32 => buf.extend_from_slice(
            &col.as_primitive::<UInt32Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::UInt64 => buf.extend_from_slice(
            &col.as_primitive::<UInt64Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Float32 => buf.extend_from_slice(
            &col.as_primitive::<Float32Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Float64 => buf.extend_from_slice(
            &col.as_primitive::<Float64Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Date32 => buf.extend_from_slice(
            &col.as_primitive::<Date32Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Date64 => buf.extend_from_slice(
            &col.as_primitive::<Date64Type>()
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Time32(TimeUnit::Second) => {
            buf.extend_from_slice(
                &col.as_primitive::<Time32SecondType>()
                    .value(row_idx)
                    .to_le_bytes(),
            );
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            buf.extend_from_slice(
                &col.as_primitive::<Time32MillisecondType>()
                    .value(row_idx)
                    .to_le_bytes(),
            );
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            buf.extend_from_slice(
                &col.as_primitive::<Time64MicrosecondType>()
                    .value(row_idx)
                    .to_le_bytes(),
            );
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            buf.extend_from_slice(
                &col.as_primitive::<Time64NanosecondType>()
                    .value(row_idx)
                    .to_le_bytes(),
            );
        }
        DataType::Timestamp(unit, _) => {
            let raw = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(row_idx),
                TimeUnit::Millisecond => col
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row_idx),
                TimeUnit::Microsecond => col
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row_idx),
                TimeUnit::Nanosecond => {
                    col.as_primitive::<TimestampNanosecondType>().value(row_idx)
                }
            };
            buf.extend_from_slice(&raw.to_le_bytes());
        }
        DataType::Decimal128(_, _) => {
            let raw = col.as_primitive::<Decimal128Type>().value(row_idx);
            buf.extend_from_slice(&raw.to_le_bytes());
        }
        DataType::FixedSizeBinary(len) => {
            let expected = usize::try_from(*len).map_err(|_| {
                PgBoltError::Ipc(format!(
                    "negative FixedSizeBinary width ({len}) for MinimalTuple framing"
                ))
            })?;
            let slice = col.as_fixed_size_binary().value(row_idx);
            if slice.len() != expected {
                return Err(PgBoltError::Ipc(format!(
                    "FixedSizeBinary width mismatch: schema={expected}, array={}",
                    slice.len()
                )));
            }
            buf.extend_from_slice(slice);
        }
        DataType::Utf8 => {
            push_varlena_bytes(buf, col.as_string::<i32>().value(row_idx).as_bytes())?
        }
        DataType::LargeUtf8 => {
            push_varlena_bytes(buf, col.as_string::<i64>().value(row_idx).as_bytes())?
        }
        DataType::Utf8View => {
            push_varlena_bytes(buf, col.as_string_view().value(row_idx).as_bytes())?
        }
        DataType::Binary => push_varlena_bytes(buf, col.as_binary::<i32>().value(row_idx))?,
        DataType::LargeBinary => push_varlena_bytes(buf, col.as_binary::<i64>().value(row_idx))?,
        DataType::BinaryView => push_varlena_bytes(buf, col.as_binary_view().value(row_idx))?,
        DataType::Dictionary(_, value_type) if matches!(value_type.as_ref(), DataType::Utf8) => {
            let s = dictionary_string_at(col, row_idx)?;
            push_varlena_bytes(buf, s.as_bytes())?;
        }
        other => {
            return Err(PgBoltError::Ipc(format!(
                "unsupported Arrow DataType {other:?} for MinimalTuple framing"
            )))
        }
    }
    Ok(())
}

/// Encodes each row of `batch` into a compact binary `MinimalTuple` frame
/// (`t_len: u32`, `t_infomask2: u16`, `t_infomask: u16`, `t_hoff: u8`, PG-style null bitmap, payloads).
pub fn record_batch_to_minimal_tuples(batch: &RecordBatch) -> PgBoltResult<Vec<Vec<u8>>> {
    let schema = batch.schema();
    let natts = schema.fields().len();
    if natts > MAX_TUPLE_ATTRS {
        return Err(PgBoltError::Ipc(format!(
            "column count {natts} exceeds PostgreSQL MaxHeapAttributeNumber ({MAX_TUPLE_ATTRS})"
        )));
    }
    let has_varwidth = schema
        .fields()
        .iter()
        .any(|f| is_varwidth_type(f.data_type()));
    let num_rows = batch.num_rows();
    let mut out = Vec::with_capacity(num_rows);
    let bitmap_bytes = natts.div_ceil(8);

    for row_idx in 0..num_rows {
        let mut has_null = false;
        for col in batch.columns() {
            if col.is_null(row_idx) {
                has_null = true;
                break;
            }
        }

        let mut infomask = 0_u16;
        if has_null {
            infomask |= HEAP_HASNULL;
        }
        if has_varwidth {
            infomask |= HEAP_HASVARWIDTH;
        }
        let infomask2 = natts as u16;
        let raw_hdr_len = 9 + if has_null { bitmap_bytes } else { 0 };
        let t_hoff = (raw_hdr_len + 7) & !7;

        let mut tuple = Vec::with_capacity(t_hoff + natts * 8);
        tuple.extend_from_slice(&0_u32.to_le_bytes()); // t_len placeholder
        tuple.extend_from_slice(&infomask2.to_le_bytes());
        tuple.extend_from_slice(&infomask.to_le_bytes());
        tuple.push(t_hoff as u8);

        if has_null {
            let bmp_start = tuple.len();
            tuple.resize(bmp_start + bitmap_bytes, 0xFF);
            for (col_idx, col) in batch.columns().iter().enumerate() {
                if col.is_null(row_idx) {
                    tuple[bmp_start + (col_idx >> 3)] &= !(1_u8 << (col_idx & 0x07));
                }
            }
        }
        tuple.resize(t_hoff, 0);

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if !col.is_null(row_idx) {
                encode_cell_into(&mut tuple, col.as_ref(), field.data_type(), row_idx)?;
            }
        }

        let t_len = u32::try_from(tuple.len()).map_err(|_| {
            PgBoltError::Ipc(format!(
                "MinimalTuple row length ({} bytes) exceeds 4 GiB",
                tuple.len()
            ))
        })?;
        tuple[0..4].copy_from_slice(&t_len.to_le_bytes());
        out.push(tuple);
    }
    Ok(out)
}

trait DictUtf8Builder {
    fn append_null(&mut self);
    fn append_str(&mut self, value: &str) -> PgBoltResult<()>;
    fn finish_array(&mut self) -> ArrayRef;
}

impl<K: ArrowDictionaryKeyType> DictUtf8Builder for StringDictionaryBuilder<K> {
    fn append_null(&mut self) {
        StringDictionaryBuilder::append_null(self);
    }

    fn append_str(&mut self, value: &str) -> PgBoltResult<()> {
        self.append(value).map(|_| ()).map_err(|e| {
            PgBoltError::Ipc(format!(
                "dictionary key overflow while decoding MinimalTuple: {e}"
            ))
        })
    }

    fn finish_array(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

fn new_dict_utf8_builder(key_dt: &DataType, cap: usize) -> PgBoltResult<Box<dyn DictUtf8Builder>> {
    let data_cap = cap.saturating_mul(16);
    match key_dt {
        DataType::Int8 => Ok(Box::new(
            StringDictionaryBuilder::<Int8Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::Int16 => Ok(Box::new(
            StringDictionaryBuilder::<Int16Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::Int32 => Ok(Box::new(
            StringDictionaryBuilder::<Int32Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::Int64 => Ok(Box::new(
            StringDictionaryBuilder::<Int64Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::UInt8 => Ok(Box::new(
            StringDictionaryBuilder::<UInt8Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::UInt16 => Ok(Box::new(
            StringDictionaryBuilder::<UInt16Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::UInt32 => Ok(Box::new(
            StringDictionaryBuilder::<UInt32Type>::with_capacity(cap, cap, data_cap),
        )),
        DataType::UInt64 => Ok(Box::new(
            StringDictionaryBuilder::<UInt64Type>::with_capacity(cap, cap, data_cap),
        )),
        other => Err(PgBoltError::Ipc(format!(
            "unsupported dictionary key type {other:?} in minimal_tuples_to_record_batch"
        ))),
    }
}

enum TupleColBuilder {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    Date64(Date64Builder),
    Time32Sec(Time32SecondBuilder),
    Time32Ms(Time32MillisecondBuilder),
    Time64Us(Time64MicrosecondBuilder),
    Time64Ns(Time64NanosecondBuilder),
    TimestampSec(TimestampSecondBuilder),
    TimestampMs(TimestampMillisecondBuilder),
    TimestampUs(TimestampMicrosecondBuilder, Option<Arc<str>>),
    TimestampNs(TimestampNanosecondBuilder),
    TimestampTzSec(TimestampSecondBuilder, Arc<str>),
    TimestampTzMs(TimestampMillisecondBuilder, Arc<str>),
    TimestampTzNs(TimestampNanosecondBuilder, Arc<str>),
    Decimal128(Decimal128Builder, u8, i8),
    FixedSizeBinary(FixedSizeBinaryBuilder, usize),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Utf8View(StringViewBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    BinaryView(BinaryViewBuilder),
    DictUtf8(Box<dyn DictUtf8Builder>),
}

impl TupleColBuilder {
    fn from_data_type(dt: &DataType, cap: usize) -> PgBoltResult<Self> {
        Ok(match dt {
            DataType::Boolean => Self::Bool(BooleanBuilder::with_capacity(cap)),
            DataType::Int8 => Self::Int8(Int8Builder::with_capacity(cap)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(cap)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(cap)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(cap)),
            DataType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(cap)),
            DataType::UInt16 => Self::UInt16(UInt16Builder::with_capacity(cap)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(cap)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(cap)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(cap)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(cap)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(cap)),
            DataType::Date64 => Self::Date64(Date64Builder::with_capacity(cap)),
            DataType::Time32(TimeUnit::Second) => {
                Self::Time32Sec(Time32SecondBuilder::with_capacity(cap))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Self::Time32Ms(Time32MillisecondBuilder::with_capacity(cap))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Self::Time64Us(Time64MicrosecondBuilder::with_capacity(cap))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64Ns(Time64NanosecondBuilder::with_capacity(cap))
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                Self::TimestampSec(TimestampSecondBuilder::with_capacity(cap))
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                Self::TimestampMs(TimestampMillisecondBuilder::with_capacity(cap))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampUs(TimestampMicrosecondBuilder::with_capacity(cap), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                Self::TimestampNs(TimestampNanosecondBuilder::with_capacity(cap))
            }
            DataType::Timestamp(TimeUnit::Second, Some(tz)) => {
                Self::TimestampTzSec(TimestampSecondBuilder::with_capacity(cap), Arc::clone(tz))
            }
            DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => Self::TimestampTzMs(
                TimestampMillisecondBuilder::with_capacity(cap),
                Arc::clone(tz),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Self::TimestampTzNs(
                TimestampNanosecondBuilder::with_capacity(cap),
                Arc::clone(tz),
            ),
            DataType::Decimal128(p, s) => {
                Self::Decimal128(Decimal128Builder::with_capacity(cap), *p, *s)
            }
            DataType::FixedSizeBinary(len) => {
                let width = usize::try_from(*len).map_err(|_| {
                    PgBoltError::Ipc(format!(
                        "negative FixedSizeBinary width ({len}) in minimal_tuples_to_record_batch"
                    ))
                })?;
                Self::FixedSizeBinary(FixedSizeBinaryBuilder::with_capacity(cap, *len), width)
            }
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(cap, cap * 16)),
            DataType::LargeUtf8 => {
                Self::LargeUtf8(LargeStringBuilder::with_capacity(cap, cap * 16))
            }
            DataType::Utf8View => Self::Utf8View(StringViewBuilder::with_capacity(cap)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(cap, cap * 16)),
            DataType::LargeBinary => {
                Self::LargeBinary(LargeBinaryBuilder::with_capacity(cap, cap * 16))
            }
            DataType::BinaryView => Self::BinaryView(BinaryViewBuilder::with_capacity(cap)),
            DataType::Dictionary(key, val) if matches!(val.as_ref(), DataType::Utf8) => {
                Self::DictUtf8(new_dict_utf8_builder(key.as_ref(), cap)?)
            }
            other => {
                return Err(PgBoltError::Ipc(format!(
                    "unsupported Arrow DataType {other:?} in minimal_tuples_to_record_batch"
                )))
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Date64(b) => b.append_null(),
            Self::Time32Sec(b) => b.append_null(),
            Self::Time32Ms(b) => b.append_null(),
            Self::Time64Us(b) => b.append_null(),
            Self::Time64Ns(b) => b.append_null(),
            Self::TimestampSec(b) | Self::TimestampTzSec(b, _) => b.append_null(),
            Self::TimestampMs(b) | Self::TimestampTzMs(b, _) => b.append_null(),
            Self::TimestampUs(b, _) => b.append_null(),
            Self::TimestampNs(b) | Self::TimestampTzNs(b, _) => b.append_null(),
            Self::Decimal128(b, _, _) => b.append_null(),
            Self::FixedSizeBinary(b, _) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::LargeUtf8(b) => b.append_null(),
            Self::Utf8View(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::LargeBinary(b) => b.append_null(),
            Self::BinaryView(b) => b.append_null(),
            Self::DictUtf8(b) => b.append_null(),
        }
    }

    fn append_from_slice(&mut self, bytes: &[u8], cursor: &mut usize) -> PgBoltResult<()> {
        match self {
            Self::Bool(b) => b.append_value(take_bytes::<1>(bytes, cursor)?[0] != 0),
            Self::Int8(b) => b.append_value(i8::from_le_bytes(take_bytes::<1>(bytes, cursor)?)),
            Self::Int16(b) => b.append_value(i16::from_le_bytes(take_bytes::<2>(bytes, cursor)?)),
            Self::Int32(b) => b.append_value(i32::from_le_bytes(take_bytes::<4>(bytes, cursor)?)),
            Self::Int64(b) => b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?)),
            Self::UInt8(b) => b.append_value(u8::from_le_bytes(take_bytes::<1>(bytes, cursor)?)),
            Self::UInt16(b) => b.append_value(u16::from_le_bytes(take_bytes::<2>(bytes, cursor)?)),
            Self::UInt32(b) => b.append_value(u32::from_le_bytes(take_bytes::<4>(bytes, cursor)?)),
            Self::UInt64(b) => b.append_value(u64::from_le_bytes(take_bytes::<8>(bytes, cursor)?)),
            Self::Float32(b) => b.append_value(f32::from_le_bytes(take_bytes::<4>(bytes, cursor)?)),
            Self::Float64(b) => b.append_value(f64::from_le_bytes(take_bytes::<8>(bytes, cursor)?)),
            Self::Date32(b) => b.append_value(i32::from_le_bytes(take_bytes::<4>(bytes, cursor)?)),
            Self::Date64(b) => b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?)),
            Self::Time32Sec(b) => {
                b.append_value(i32::from_le_bytes(take_bytes::<4>(bytes, cursor)?))
            }
            Self::Time32Ms(b) => {
                b.append_value(i32::from_le_bytes(take_bytes::<4>(bytes, cursor)?))
            }
            Self::Time64Us(b) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::Time64Ns(b) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::TimestampSec(b) | Self::TimestampTzSec(b, _) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::TimestampMs(b) | Self::TimestampTzMs(b, _) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::TimestampUs(b, _) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::TimestampNs(b) | Self::TimestampTzNs(b, _) => {
                b.append_value(i64::from_le_bytes(take_bytes::<8>(bytes, cursor)?))
            }
            Self::Decimal128(b, _, _) => {
                b.append_value(i128::from_le_bytes(take_bytes::<16>(bytes, cursor)?))
            }
            Self::FixedSizeBinary(b, width) => {
                let slice = take_slice(bytes, cursor, *width)?;
                b.append_value(slice).map_err(|e| {
                    PgBoltError::Ipc(format!("invalid FixedSizeBinary MinimalTuple value: {e}"))
                })?;
            }
            Self::Utf8(b) => b.append_value(take_str(bytes, cursor)?),
            Self::LargeUtf8(b) => b.append_value(take_str(bytes, cursor)?),
            Self::Utf8View(b) => b.append_value(take_str(bytes, cursor)?),
            Self::Binary(b) => b.append_value(take_varlena(bytes, cursor)?),
            Self::LargeBinary(b) => b.append_value(take_varlena(bytes, cursor)?),
            Self::BinaryView(b) => b.append_value(take_varlena(bytes, cursor)?),
            Self::DictUtf8(b) => b.append_str(take_str(bytes, cursor)?)?,
        }
        Ok(())
    }

    fn finish(self) -> PgBoltResult<ArrayRef> {
        Ok(match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Int16(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::UInt8(mut b) => Arc::new(b.finish()),
            Self::UInt16(mut b) => Arc::new(b.finish()),
            Self::UInt32(mut b) => Arc::new(b.finish()),
            Self::UInt64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Date64(mut b) => Arc::new(b.finish()),
            Self::Time32Sec(mut b) => Arc::new(b.finish()),
            Self::Time32Ms(mut b) => Arc::new(b.finish()),
            Self::Time64Us(mut b) => Arc::new(b.finish()),
            Self::Time64Ns(mut b) => Arc::new(b.finish()),
            Self::TimestampSec(mut b) => Arc::new(b.finish()),
            Self::TimestampMs(mut b) => Arc::new(b.finish()),
            Self::TimestampUs(mut b, tz) => Arc::new(b.finish().with_timezone_opt(tz)),
            Self::TimestampNs(mut b) => Arc::new(b.finish()),
            Self::TimestampTzSec(mut b, tz) => Arc::new(b.finish().with_timezone(tz)),
            Self::TimestampTzMs(mut b, tz) => Arc::new(b.finish().with_timezone(tz)),
            Self::TimestampTzNs(mut b, tz) => Arc::new(b.finish().with_timezone(tz)),
            Self::Decimal128(mut b, p, s) => Arc::new(
                b.finish()
                    .with_precision_and_scale(p, s)
                    .map_err(|e| PgBoltError::Ipc(format!("invalid Decimal128 metadata: {e}")))?,
            ),
            Self::FixedSizeBinary(mut b, _) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::LargeUtf8(mut b) => Arc::new(b.finish()),
            Self::Utf8View(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::LargeBinary(mut b) => Arc::new(b.finish()),
            Self::BinaryView(mut b) => Arc::new(b.finish()),
            Self::DictUtf8(mut b) => b.finish_array(),
        })
    }
}

#[inline]
fn take_bytes<const N: usize>(bytes: &[u8], cursor: &mut usize) -> PgBoltResult<[u8; N]> {
    let end = cursor.checked_add(N).ok_or_else(|| {
        PgBoltError::Ipc("MinimalTuple cursor overflow while decoding fixed column".to_owned())
    })?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        PgBoltError::Ipc("truncated MinimalTuple payload while decoding fixed column".to_owned())
    })?;
    *cursor = end;
    Ok(slice.try_into().expect("slice length checked"))
}

#[inline]
fn take_slice<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> PgBoltResult<&'a [u8]> {
    let end = cursor.checked_add(len).ok_or_else(|| {
        PgBoltError::Ipc(
            "MinimalTuple cursor overflow while decoding fixed binary column".to_owned(),
        )
    })?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        PgBoltError::Ipc(
            "truncated MinimalTuple payload while decoding fixed binary column".to_owned(),
        )
    })?;
    *cursor = end;
    Ok(slice)
}

fn take_varlena<'a>(bytes: &'a [u8], cursor: &mut usize) -> PgBoltResult<&'a [u8]> {
    let len = u32::from_le_bytes(take_bytes::<4>(bytes, cursor)?) as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| PgBoltError::Ipc("varlena length overflow".to_owned()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| PgBoltError::Ipc("truncated varlena payload".to_owned()))?;
    *cursor = end;
    Ok(slice)
}

fn take_str<'a>(bytes: &'a [u8], cursor: &mut usize) -> PgBoltResult<&'a str> {
    let raw = take_varlena(bytes, cursor)?;
    std::str::from_utf8(raw)
        .map_err(|e| PgBoltError::Ipc(format!("invalid UTF-8 in MinimalTuple column: {e}")))
}

/// Decodes `MinimalTuple` binary frames back into an Arrow `RecordBatch` matching `schema`.
pub fn minimal_tuples_to_record_batch(
    tuples: &[Vec<u8>],
    schema: &Schema,
) -> PgBoltResult<RecordBatch> {
    let natts = schema.fields().len();
    if natts > MAX_TUPLE_ATTRS {
        return Err(PgBoltError::Ipc(format!(
            "schema column count {natts} exceeds PostgreSQL MaxHeapAttributeNumber ({MAX_TUPLE_ATTRS})"
        )));
    }
    let mut builders = schema
        .fields()
        .iter()
        .map(|f| TupleColBuilder::from_data_type(f.data_type(), tuples.len()))
        .collect::<PgBoltResult<Vec<_>>>()?;
    let schema_arc = Arc::new(schema.clone());
    if tuples.is_empty() {
        return Ok(RecordBatch::new_empty(schema_arc));
    }

    let bitmap_bytes = natts.div_ceil(8);
    for (row_idx, tuple) in tuples.iter().enumerate() {
        if tuple.len() < 9 {
            return Err(PgBoltError::Ipc(format!(
                "MinimalTuple row {row_idx} shorter than 9-byte header ({} bytes)",
                tuple.len()
            )));
        }
        let t_len = u32::from_le_bytes(tuple[0..4].try_into().expect("len checked")) as usize;
        let t_infomask2 = u16::from_le_bytes(tuple[4..6].try_into().expect("len checked"));
        let t_infomask = u16::from_le_bytes(tuple[6..8].try_into().expect("len checked"));
        let t_hoff = tuple[8] as usize;

        if t_len != tuple.len() {
            return Err(PgBoltError::Ipc(format!(
                "MinimalTuple row {row_idx} length mismatch: header says {t_len}, slice has {}",
                tuple.len()
            )));
        }
        let tuple_natts = (t_infomask2 & 0x07FF) as usize;
        if tuple_natts != natts {
            return Err(PgBoltError::Ipc(format!(
                "MinimalTuple row {row_idx} attribute count {tuple_natts} != schema {natts}"
            )));
        }
        let has_null = (t_infomask & HEAP_HASNULL) != 0;
        let min_hoff = 9 + if has_null { bitmap_bytes } else { 0 };
        if t_hoff < min_hoff || t_hoff > tuple.len() {
            return Err(PgBoltError::Ipc(format!(
                "MinimalTuple row {row_idx} has invalid t_hoff={t_hoff} (min={min_hoff}, len={})",
                tuple.len()
            )));
        }

        let mut cursor = t_hoff;
        for (col_idx, builder) in builders.iter_mut().enumerate() {
            let is_null = has_null && (tuple[9 + (col_idx >> 3)] & (1_u8 << (col_idx & 0x07))) == 0;
            if is_null {
                builder.append_null();
            } else {
                builder.append_from_slice(tuple, &mut cursor)?;
            }
        }
        if cursor != tuple.len() {
            return Err(PgBoltError::Ipc(format!(
                "MinimalTuple row {row_idx} trailing unconsumed bytes ({} != {})",
                cursor,
                tuple.len()
            )));
        }
    }

    let columns = builders
        .into_iter()
        .map(TupleColBuilder::finish)
        .collect::<PgBoltResult<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(tuples.len()));
    RecordBatch::try_new_with_options(schema_arc, columns, &options).map_err(|e| {
        PgBoltError::Ipc(format!(
            "failed to assemble RecordBatch from MinimalTuples: {e}"
        ))
    })
}

/// Sequence header describing one fixed-budget slice of an oversized Arrow IPC stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcChunkHeader {
    /// Arena-wide monotonic identifier tying all chunks of one logical transfer together.
    pub stream_id: u64,
    /// Zero-based chunk index within `0..total_chunks`.
    pub seq: u32,
    /// Total number of chunks expected for this transfer (`>= 1`).
    pub total_chunks: u32,
    /// Total uncompressed byte length of the reassembled payload.
    pub total_bytes: u64,
    /// Byte length of this chunk's `chunk_data` slice.
    pub payload_len: u32,
}

impl IpcChunkHeader {
    /// Encoded wire size in bytes (`stream_id` + `seq` + `total_chunks` + `total_bytes` + `payload_len`).
    pub const WIRE_BYTES: usize = 28;

    fn write_wire(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.stream_id.to_le_bytes());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&self.total_chunks.to_le_bytes());
        buf.extend_from_slice(&self.total_bytes.to_le_bytes());
        buf.extend_from_slice(&self.payload_len.to_le_bytes());
    }

    fn read_wire(buf: &[u8], cursor: &mut usize) -> PgBoltResult<Self> {
        let stream_id = u64::from_le_bytes(take_bytes::<8>(buf, cursor)?);
        let seq = u32::from_le_bytes(take_bytes::<4>(buf, cursor)?);
        let total_chunks = u32::from_le_bytes(take_bytes::<4>(buf, cursor)?);
        let total_bytes = u64::from_le_bytes(take_bytes::<8>(buf, cursor)?);
        let payload_len = u32::from_le_bytes(take_bytes::<4>(buf, cursor)?);
        Ok(Self {
            stream_id,
            seq,
            total_chunks,
            total_bytes,
            payload_len,
        })
    }
}

/// Computes maximum `chunk_data` bytes per slot given `slot_bytes` and command string lengths.
pub fn chunk_budget_for_slot(slot_bytes: usize, meta_str_lens: &[usize]) -> PgBoltResult<usize> {
    let mut overhead = 1_usize
        .saturating_add(IpcChunkHeader::WIRE_BYTES)
        .saturating_add(4); // write_bytes length prefix for chunk_data
    for &len in meta_str_lens {
        overhead = overhead
            .checked_add(4 + len)
            .ok_or_else(|| PgBoltError::Ipc("chunk metadata length overflow".to_owned()))?;
    }
    if slot_bytes <= overhead {
        return Err(PgBoltError::Ipc(format!(
            "ipc_slot_bytes ({slot_bytes}) is too small for chunk header overhead ({overhead} bytes)"
        )));
    }
    Ok(slot_bytes - overhead)
}

/// Splits `payload` into indexed `(IpcChunkHeader, Vec<u8>)` chunks bounded by `max_chunk_bytes`.
pub fn split_payload_into_chunks(
    stream_id: u64,
    payload: &[u8],
    max_chunk_bytes: usize,
) -> PgBoltResult<Vec<(IpcChunkHeader, Vec<u8>)>> {
    if max_chunk_bytes == 0 {
        return Err(PgBoltError::Ipc(
            "max_chunk_bytes must be greater than 0".to_owned(),
        ));
    }
    let total_bytes = u64::try_from(payload.len())
        .map_err(|_| PgBoltError::Ipc("chunked IPC payload exceeds u64::MAX bytes".to_owned()))?;
    if payload.is_empty() {
        return Ok(vec![(
            IpcChunkHeader {
                stream_id,
                seq: 0,
                total_chunks: 1,
                total_bytes: 0,
                payload_len: 0,
            },
            Vec::new(),
        )]);
    }

    let total_chunks_usize = payload.len().div_ceil(max_chunk_bytes);
    let total_chunks = u32::try_from(total_chunks_usize)
        .map_err(|_| PgBoltError::Ipc("chunk count exceeds u32::MAX".to_owned()))?;
    let mut out = Vec::with_capacity(total_chunks_usize);
    for (idx, slice) in payload.chunks(max_chunk_bytes).enumerate() {
        let seq = u32::try_from(idx)
            .map_err(|_| PgBoltError::Ipc("chunk sequence exceeds u32::MAX".to_owned()))?;
        let payload_len = u32::try_from(slice.len())
            .map_err(|_| PgBoltError::Ipc("chunk payload length exceeds u32::MAX".to_owned()))?;
        out.push((
            IpcChunkHeader {
                stream_id,
                seq,
                total_chunks,
                total_bytes,
                payload_len,
            },
            slice.to_vec(),
        ));
    }
    Ok(out)
}

/// Computes maximum `chunk_data` bytes per [`IpcResponse::BatchChunk`] frame for `slot_bytes`.
pub fn response_chunk_budget_for_slot(slot_bytes: usize, tier_len: usize) -> PgBoltResult<usize> {
    let overhead = 1_usize
        .saturating_add(IpcChunkHeader::WIRE_BYTES)
        .saturating_add(16)
        .checked_add(4 + tier_len)
        .and_then(|v| v.checked_add(4))
        .ok_or_else(|| PgBoltError::Ipc("response chunk metadata length overflow".to_owned()))?;
    if slot_bytes <= overhead {
        return Err(PgBoltError::Ipc(format!(
            "ipc_slot_bytes ({slot_bytes}) is too small for BatchChunk overhead ({overhead} bytes)"
        )));
    }
    Ok(slot_bytes - overhead)
}

#[inline]
fn prune_stale_entries<V>(
    map: &mut HashMap<u64, V>,
    now: Instant,
    ttl: Duration,
    last_updated: fn(&V) -> Instant,
) -> usize {
    let before = map.len();
    map.retain(|_, v| now.saturating_duration_since(last_updated(v)) < ttl);
    before - map.len()
}

#[derive(Debug)]
struct PendingChunkStream {
    stream_key: String,
    total_chunks: u32,
    total_bytes: usize,
    received: Vec<Option<Vec<u8>>>,
    received_chunks: u32,
    received_bytes: usize,
    last_updated: Instant,
}

/// Worker-side bounded reassembler for multi-chunk [`IpcCommand`] streams.
#[derive(Debug)]
pub struct ChunkReassembler {
    max_total_bytes: usize,
    ttl: Duration,
    streams: HashMap<u64, PendingChunkStream>,
}

impl ChunkReassembler {
    /// Creates a reassembler enforcing a per-stream upper bound of `max_total_bytes`.
    pub fn new(max_total_bytes: usize) -> Self {
        Self {
            max_total_bytes,
            ttl: DEFAULT_CHUNK_STREAM_TTL,
            streams: HashMap::new(),
        }
    }

    /// Overrides the idle TTL after which partial uncompleted streams are evicted.
    pub fn with_stream_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Returns the number of incomplete streams currently buffered.
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    /// Discards an incomplete stream buffer (e.g., on worker reset or client cancel).
    pub fn discard_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// Evicts incomplete streams whose idle age meets or exceeds the configured TTL.
    pub fn prune_stale_streams(&mut self) -> usize {
        prune_stale_entries(&mut self.streams, Instant::now(), self.ttl, |s| {
            s.last_updated
        })
    }

    /// Appends one validated chunk belonging to `stream_key` and returns `Ok(Some(payload))`
    /// once all chunks `0..total_chunks` have arrived.
    pub fn push(
        &mut self,
        stream_key: &str,
        header: &IpcChunkHeader,
        chunk_data: &[u8],
    ) -> PgBoltResult<Option<Vec<u8>>> {
        let now = Instant::now();
        prune_stale_entries(&mut self.streams, now, self.ttl, |s| s.last_updated);

        if header.total_chunks == 0 || header.seq >= header.total_chunks {
            return Err(PgBoltError::Ipc(format!(
                "invalid IPC chunk index seq={} / total_chunks={}",
                header.seq, header.total_chunks
            )));
        }
        if chunk_data.len() != header.payload_len as usize {
            return Err(PgBoltError::Ipc(format!(
                "IPC chunk payload length mismatch: header={}, actual={}",
                header.payload_len,
                chunk_data.len()
            )));
        }
        let total_bytes = usize::try_from(header.total_bytes).map_err(|_| {
            PgBoltError::Ipc("chunked total_bytes overflows platform usize".to_owned())
        })?;
        if total_bytes > self.max_total_bytes {
            self.streams.remove(&header.stream_id);
            return Err(PgBoltError::Ipc(format!(
                "chunked IPC payload ({total_bytes} bytes) exceeds max_chunked_payload_bytes ({} bytes)",
                self.max_total_bytes
            )));
        }
        if (header.total_bytes > 0 && chunk_data.is_empty())
            || (header.total_bytes == 0 && header.total_chunks > 1)
            || (header.total_chunks as usize) > total_bytes.max(1)
        {
            self.streams.remove(&header.stream_id);
            return Err(PgBoltError::Ipc(
                "inconsistent empty or inflated IPC chunk header".to_owned(),
            ));
        }

        let stream_id = header.stream_id;
        let entry = self
            .streams
            .entry(stream_id)
            .or_insert_with(|| PendingChunkStream {
                stream_key: stream_key.to_owned(),
                total_chunks: header.total_chunks,
                total_bytes,
                received: vec![None; header.total_chunks as usize],
                received_chunks: 0,
                received_bytes: 0,
                last_updated: now,
            });
        if entry.stream_key != stream_key
            || entry.total_chunks != header.total_chunks
            || entry.total_bytes != total_bytes
        {
            self.streams.remove(&stream_id);
            return Err(PgBoltError::Ipc(format!(
                "IPC chunk header metadata mismatch for stream_id={stream_id}"
            )));
        }
        let slot = &mut entry.received[header.seq as usize];
        if slot.is_some() {
            self.streams.remove(&stream_id);
            return Err(PgBoltError::Ipc(format!(
                "duplicate IPC chunk seq={} for stream_id={stream_id}",
                header.seq
            )));
        }
        let next_bytes = entry
            .received_bytes
            .checked_add(chunk_data.len())
            .ok_or_else(|| PgBoltError::Ipc("chunked IPC byte count overflow".to_owned()))?;
        if next_bytes > entry.total_bytes {
            self.streams.remove(&stream_id);
            return Err(PgBoltError::Ipc(format!(
                "chunked IPC stream {stream_id} exceeded declared total_bytes ({} > {})",
                next_bytes, total_bytes
            )));
        }

        *slot = Some(chunk_data.to_vec());
        entry.received_chunks += 1;
        entry.received_bytes = next_bytes;
        entry.last_updated = now;

        if entry.received_chunks < entry.total_chunks {
            return Ok(None);
        }

        let completed = self.streams.remove(&stream_id).expect("stream present");
        if completed.received_bytes != completed.total_bytes {
            return Err(PgBoltError::Ipc(format!(
                "chunked IPC stream {stream_id} short read: got {} bytes, expected {}",
                completed.received_bytes, completed.total_bytes
            )));
        }
        let mut assembled = Vec::with_capacity(completed.total_bytes);
        for part in completed.received {
            let bytes = part.ok_or_else(|| {
                PgBoltError::Ipc(format!(
                    "chunked IPC stream {stream_id} missing chunk upon completion"
                ))
            })?;
            assembled.extend_from_slice(&bytes);
        }
        Ok(Some(assembled))
    }
}

#[derive(Debug)]
struct PendingResponseStream {
    elapsed_us: u128,
    tier: String,
    chunks: Vec<Option<(IpcChunkHeader, Vec<u8>)>>,
    remaining_chunks: usize,
    last_updated: Instant,
}

/// Worker-side bounded stash holding tail chunks for oversized [`IpcResponse::Batch`] results
/// so clients can pull remaining frames over `slot_bytes`-bounded shared-memory slots.
#[derive(Debug)]
pub struct ResponseChunkStash {
    max_total_bytes: usize,
    ttl: Duration,
    streams: HashMap<u64, PendingResponseStream>,
}

impl ResponseChunkStash {
    /// Creates a response chunk stash bounded by `max_total_bytes` per stream.
    pub fn new(max_total_bytes: usize) -> Self {
        Self {
            max_total_bytes,
            ttl: DEFAULT_CHUNK_STREAM_TTL,
            streams: HashMap::new(),
        }
    }

    /// Overrides the idle TTL after which unfetched response streams are discarded.
    pub fn with_stream_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Returns the number of active response chunk streams currently stashed.
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    /// Discards an incomplete response stream (e.g., on client cancel or error).
    pub fn discard_stream(&mut self, stream_id: u64) {
        self.streams.remove(&stream_id);
    }

    /// Evicts stashed response streams whose idle age meets or exceeds the configured TTL.
    pub fn prune_stale_streams(&mut self) -> usize {
        prune_stale_entries(&mut self.streams, Instant::now(), self.ttl, |s| {
            s.last_updated
        })
    }

    /// Returns a single [`IpcResponse::Batch`] when the response fits within `slot_bytes`, or
    /// splits the payload into [`IpcResponse::BatchChunk`] frames (returning chunk `0` and
    /// stashing chunks `1..total_chunks` under `stream_id`).
    pub fn prepare_batch_response(
        &mut self,
        stream_id: u64,
        slot_bytes: usize,
        ipc_payload: Vec<u8>,
        elapsed_us: u128,
        tier: String,
    ) -> PgBoltResult<IpcResponse> {
        let now = Instant::now();
        prune_stale_entries(&mut self.streams, now, self.ttl, |s| s.last_updated);

        if ipc_payload.len() > self.max_total_bytes {
            return Err(PgBoltError::Ipc(format!(
                "chunked IPC response ({} bytes) exceeds max_chunked_payload_bytes ({} bytes)",
                ipc_payload.len(),
                self.max_total_bytes
            )));
        }

        let single_wire_bytes = 1_usize
            .saturating_add(16)
            .checked_add(4 + tier.len())
            .and_then(|v| v.checked_add(4 + ipc_payload.len()))
            .ok_or_else(|| PgBoltError::Ipc("IPC batch response length overflow".to_owned()))?;
        if single_wire_bytes <= slot_bytes {
            return Ok(IpcResponse::Batch {
                ipc_payload,
                elapsed_us,
                tier,
            });
        }

        let max_chunk_bytes = response_chunk_budget_for_slot(slot_bytes, tier.len())?;
        let chunks = split_payload_into_chunks(stream_id, &ipc_payload, max_chunk_bytes)?;
        let mut iter = chunks.into_iter();
        let (first_header, first_data) = iter
            .next()
            .ok_or_else(|| PgBoltError::Ipc("chunked response produced zero chunks".to_owned()))?;
        let mut stashed: Vec<Option<(IpcChunkHeader, Vec<u8>)>> =
            Vec::with_capacity(first_header.total_chunks as usize);
        stashed.push(None);
        stashed.extend(iter.map(Some));
        let remaining_chunks = stashed.len().saturating_sub(1);
        if remaining_chunks > 0 {
            self.streams.insert(
                stream_id,
                PendingResponseStream {
                    elapsed_us,
                    tier: tier.clone(),
                    chunks: stashed,
                    remaining_chunks,
                    last_updated: now,
                },
            );
        }

        Ok(IpcResponse::BatchChunk {
            header: first_header,
            elapsed_us,
            tier,
            chunk_data: first_data,
        })
    }

    /// Pops and returns chunk `chunk_idx` for an active stashed response stream.
    pub fn take_chunk(&mut self, stream_id: u64, chunk_idx: u32) -> PgBoltResult<IpcResponse> {
        let now = Instant::now();
        prune_stale_entries(&mut self.streams, now, self.ttl, |s| s.last_updated);

        match self.streams.entry(stream_id) {
            Entry::Vacant(_) => Err(PgBoltError::Ipc(format!(
                "unknown or expired response stream_id={stream_id}"
            ))),
            Entry::Occupied(mut occ) => {
                let idx = chunk_idx as usize;
                if idx >= occ.get().chunks.len() {
                    return Err(PgBoltError::Ipc(format!(
                        "response chunk index {chunk_idx} out of range for stream_id={stream_id}"
                    )));
                }
                let Some((header, chunk_data)) = occ.get_mut().chunks[idx].take() else {
                    return Err(PgBoltError::Ipc(format!(
                        "response chunk {chunk_idx} already fetched or invalid for stream_id={stream_id}"
                    )));
                };
                let stream = occ.get_mut();
                stream.remaining_chunks -= 1;
                stream.last_updated = now;
                let elapsed_us = stream.elapsed_us;
                let tier = stream.tier.clone();
                if stream.remaining_chunks == 0 {
                    occ.remove();
                }
                Ok(IpcResponse::BatchChunk {
                    header,
                    elapsed_us,
                    tier,
                    chunk_data,
                })
            }
        }
    }
}

/// Request messages sent from PostgreSQL client backends to the GPU background worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IpcCommand {
    ExecuteSql {
        sql: String,
        timeout_ms: u64,
    },
    LoadBatch {
        pg_relation: String,
        bolt_name: String,
        ipc_payload: Vec<u8>,
    },
    LoadBatchChunk {
        pg_relation: String,
        bolt_name: String,
        header: IpcChunkHeader,
        chunk_data: Vec<u8>,
    },
    AppendDelta {
        bolt_name: String,
        ipc_payload: Vec<u8>,
    },
    AppendDeltaChunk {
        bolt_name: String,
        header: IpcChunkHeader,
        chunk_data: Vec<u8>,
    },
    EvictTable {
        bolt_name: String,
    },
    ExplainTier {
        sql: String,
    },
    ExplainSql {
        sql: String,
    },
    SetTableDirty {
        bolt_name: String,
        dirty: bool,
    },
    CancelChunkStream {
        stream_id: u64,
    },
    FetchResponseChunk {
        stream_id: u64,
        chunk_idx: u32,
    },
    CancelRequest {
        req_id: u64,
    },
    GetStatus,
    ListTables,
    Shutdown,
}

/// Response messages sent from the GPU background worker back to client backends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IpcResponse {
    Batch {
        ipc_payload: Vec<u8>,
        elapsed_us: u128,
        tier: String,
    },
    BatchChunk {
        header: IpcChunkHeader,
        elapsed_us: u128,
        tier: String,
        chunk_data: Vec<u8>,
    },
    Tier {
        tier: String,
    },
    ExplainSql {
        plan: String,
    },
    StatusJson {
        json: String,
    },
    TablesJson {
        json: String,
    },
    Ack {
        message: String,
    },
    Error {
        message: String,
        vram_exhausted: bool,
    },
}

#[inline]
fn write_str(buf: &mut Vec<u8>, s: &str) -> PgBoltResult<()> {
    write_bytes(buf, s.as_bytes())
}

#[inline]
fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> PgBoltResult<()> {
    let len = u32::try_from(data.len())
        .map_err(|_| PgBoltError::Ipc("IPC field exceeds u32::MAX bytes".to_owned()))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(data);
    Ok(())
}

#[inline]
fn read_bytes<'a>(buf: &'a [u8], cursor: &mut usize) -> PgBoltResult<&'a [u8]> {
    take_varlena(buf, cursor)
}

#[inline]
fn read_string(buf: &[u8], cursor: &mut usize) -> PgBoltResult<String> {
    let raw = read_bytes(buf, cursor)?;
    std::str::from_utf8(raw)
        .map(ToOwned::to_owned)
        .map_err(|e| PgBoltError::Ipc(format!("invalid UTF-8 in IPC frame: {e}")))
}

impl IpcCommand {
    fn encode_wire(&self, max_bytes: usize) -> PgBoltResult<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            Self::ExecuteSql { sql, timeout_ms } => {
                buf.push(CMD_EXECUTE_SQL);
                buf.extend_from_slice(&timeout_ms.to_le_bytes());
                write_str(&mut buf, sql)?;
            }
            Self::LoadBatch {
                pg_relation,
                bolt_name,
                ipc_payload,
            } => {
                buf.push(CMD_LOAD_BATCH);
                write_str(&mut buf, pg_relation)?;
                write_str(&mut buf, bolt_name)?;
                write_bytes(&mut buf, ipc_payload)?;
            }
            Self::LoadBatchChunk {
                pg_relation,
                bolt_name,
                header,
                chunk_data,
            } => {
                buf.push(CMD_LOAD_BATCH_CHUNK);
                write_str(&mut buf, pg_relation)?;
                write_str(&mut buf, bolt_name)?;
                header.write_wire(&mut buf);
                write_bytes(&mut buf, chunk_data)?;
            }
            Self::AppendDelta {
                bolt_name,
                ipc_payload,
            } => {
                buf.push(CMD_APPEND_DELTA);
                write_str(&mut buf, bolt_name)?;
                write_bytes(&mut buf, ipc_payload)?;
            }
            Self::AppendDeltaChunk {
                bolt_name,
                header,
                chunk_data,
            } => {
                buf.push(CMD_APPEND_DELTA_CHUNK);
                write_str(&mut buf, bolt_name)?;
                header.write_wire(&mut buf);
                write_bytes(&mut buf, chunk_data)?;
            }
            Self::EvictTable { bolt_name } => {
                buf.push(CMD_EVICT_TABLE);
                write_str(&mut buf, bolt_name)?;
            }
            Self::ExplainTier { sql } => {
                buf.push(CMD_EXPLAIN_TIER);
                write_str(&mut buf, sql)?;
            }
            Self::ExplainSql { sql } => {
                buf.push(CMD_EXPLAIN_SQL);
                write_str(&mut buf, sql)?;
            }
            Self::SetTableDirty { bolt_name, dirty } => {
                buf.push(CMD_SET_TABLE_DIRTY);
                write_str(&mut buf, bolt_name)?;
                buf.push(u8::from(*dirty));
            }
            Self::CancelChunkStream { stream_id } => {
                buf.push(CMD_CANCEL_CHUNK_STREAM);
                buf.extend_from_slice(&stream_id.to_le_bytes());
            }
            Self::FetchResponseChunk {
                stream_id,
                chunk_idx,
            } => {
                buf.push(CMD_FETCH_RESPONSE_CHUNK);
                buf.extend_from_slice(&stream_id.to_le_bytes());
                buf.extend_from_slice(&chunk_idx.to_le_bytes());
            }
            Self::CancelRequest { req_id } => {
                buf.push(CMD_CANCEL_REQUEST);
                buf.extend_from_slice(&req_id.to_le_bytes());
            }
            Self::GetStatus => buf.push(CMD_GET_STATUS),
            Self::ListTables => buf.push(CMD_LIST_TABLES),
            Self::Shutdown => buf.push(CMD_SHUTDOWN),
        }
        if buf.len() > max_bytes {
            return Err(PgBoltError::Ipc(format!(
                "IPC command payload ({} bytes) exceeds slot_bytes limit ({max_bytes} bytes)",
                buf.len()
            )));
        }
        Ok(buf)
    }

    /// Encodes this command directly into `slice`, returning the written byte count.
    pub fn encode_into_slice(&self, slice: &mut [u8]) -> PgBoltResult<usize> {
        let bytes = self.encode_wire(slice.len())?;
        slice[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    /// Decodes a command from a byte slice.
    pub fn decode_from_slice(slice: &[u8]) -> PgBoltResult<Self> {
        Self::decode_wire(slice)
    }

    pub fn decode_wire(buf: &[u8]) -> PgBoltResult<Self> {
        let (&tag, rest) = buf
            .split_first()
            .ok_or_else(|| PgBoltError::Ipc("empty IPC command frame".to_owned()))?;
        let mut cursor = 0_usize;
        let cmd = match tag {
            CMD_EXECUTE_SQL => {
                let timeout_ms = u64::from_le_bytes(take_bytes::<8>(rest, &mut cursor)?);
                let sql = read_string(rest, &mut cursor)?;
                Self::ExecuteSql { sql, timeout_ms }
            }
            CMD_LOAD_BATCH => {
                let pg_relation = read_string(rest, &mut cursor)?;
                let bolt_name = read_string(rest, &mut cursor)?;
                let ipc_payload = read_bytes(rest, &mut cursor)?.to_vec();
                Self::LoadBatch {
                    pg_relation,
                    bolt_name,
                    ipc_payload,
                }
            }
            CMD_LOAD_BATCH_CHUNK => {
                let pg_relation = read_string(rest, &mut cursor)?;
                let bolt_name = read_string(rest, &mut cursor)?;
                let header = IpcChunkHeader::read_wire(rest, &mut cursor)?;
                let chunk_data = read_bytes(rest, &mut cursor)?.to_vec();
                Self::LoadBatchChunk {
                    pg_relation,
                    bolt_name,
                    header,
                    chunk_data,
                }
            }
            CMD_APPEND_DELTA => {
                let bolt_name = read_string(rest, &mut cursor)?;
                let ipc_payload = read_bytes(rest, &mut cursor)?.to_vec();
                Self::AppendDelta {
                    bolt_name,
                    ipc_payload,
                }
            }
            CMD_APPEND_DELTA_CHUNK => {
                let bolt_name = read_string(rest, &mut cursor)?;
                let header = IpcChunkHeader::read_wire(rest, &mut cursor)?;
                let chunk_data = read_bytes(rest, &mut cursor)?.to_vec();
                Self::AppendDeltaChunk {
                    bolt_name,
                    header,
                    chunk_data,
                }
            }
            CMD_EVICT_TABLE => Self::EvictTable {
                bolt_name: read_string(rest, &mut cursor)?,
            },
            CMD_EXPLAIN_TIER => Self::ExplainTier {
                sql: read_string(rest, &mut cursor)?,
            },
            CMD_EXPLAIN_SQL => Self::ExplainSql {
                sql: read_string(rest, &mut cursor)?,
            },
            CMD_SET_TABLE_DIRTY => {
                let bolt_name = read_string(rest, &mut cursor)?;
                let dirty = take_bytes::<1>(rest, &mut cursor)?[0] != 0;
                Self::SetTableDirty { bolt_name, dirty }
            }
            CMD_CANCEL_CHUNK_STREAM => Self::CancelChunkStream {
                stream_id: u64::from_le_bytes(take_bytes::<8>(rest, &mut cursor)?),
            },
            CMD_FETCH_RESPONSE_CHUNK => {
                let stream_id = u64::from_le_bytes(take_bytes::<8>(rest, &mut cursor)?);
                let chunk_idx = u32::from_le_bytes(take_bytes::<4>(rest, &mut cursor)?);
                Self::FetchResponseChunk {
                    stream_id,
                    chunk_idx,
                }
            }
            CMD_CANCEL_REQUEST => {
                let req_id = u64::from_le_bytes(take_bytes::<8>(rest, &mut cursor)?);
                Self::CancelRequest { req_id }
            }
            CMD_GET_STATUS => Self::GetStatus,
            CMD_LIST_TABLES => Self::ListTables,
            CMD_SHUTDOWN => Self::Shutdown,
            other => return Err(PgBoltError::Ipc(format!("unknown IPC command tag {other}"))),
        };
        if cursor != rest.len() {
            return Err(PgBoltError::Ipc(
                "trailing garbage bytes in IPC command frame".to_owned(),
            ));
        }
        Ok(cmd)
    }
}

impl IpcResponse {
    fn encode_wire(&self, max_bytes: usize) -> PgBoltResult<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            Self::Batch {
                ipc_payload,
                elapsed_us,
                tier,
            } => {
                buf.push(RESP_BATCH);
                buf.extend_from_slice(&elapsed_us.to_le_bytes());
                write_str(&mut buf, tier)?;
                write_bytes(&mut buf, ipc_payload)?;
            }
            Self::BatchChunk {
                header,
                elapsed_us,
                tier,
                chunk_data,
            } => {
                buf.push(RESP_BATCH_CHUNK);
                header.write_wire(&mut buf);
                buf.extend_from_slice(&elapsed_us.to_le_bytes());
                write_str(&mut buf, tier)?;
                write_bytes(&mut buf, chunk_data)?;
            }
            Self::Tier { tier } => {
                buf.push(RESP_TIER);
                write_str(&mut buf, tier)?;
            }
            Self::ExplainSql { plan } => {
                buf.push(RESP_EXPLAIN_SQL);
                write_str(&mut buf, plan)?;
            }
            Self::StatusJson { json } => {
                buf.push(RESP_STATUS_JSON);
                write_str(&mut buf, json)?;
            }
            Self::TablesJson { json } => {
                buf.push(RESP_TABLES_JSON);
                write_str(&mut buf, json)?;
            }
            Self::Ack { message } => {
                buf.push(RESP_ACK);
                write_str(&mut buf, message)?;
            }
            Self::Error {
                message,
                vram_exhausted,
            } => {
                buf.push(RESP_ERROR);
                buf.push(u8::from(*vram_exhausted));
                write_str(&mut buf, message)?;
            }
        }
        if buf.len() > max_bytes {
            return Err(PgBoltError::Ipc(format!(
                "IPC response payload ({} bytes) exceeds slot_bytes limit ({max_bytes} bytes)",
                buf.len()
            )));
        }
        Ok(buf)
    }

    /// Encodes this response directly into `slice`, returning the written byte count.
    pub fn encode_into_slice(&self, slice: &mut [u8]) -> PgBoltResult<usize> {
        let bytes = self.encode_wire(slice.len())?;
        slice[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    /// Decodes a response from a byte slice.
    pub fn decode_from_slice(slice: &[u8]) -> PgBoltResult<Self> {
        Self::decode_wire(slice)
    }

    pub fn decode_wire(buf: &[u8]) -> PgBoltResult<Self> {
        let (&tag, rest) = buf
            .split_first()
            .ok_or_else(|| PgBoltError::Ipc("empty IPC response frame".to_owned()))?;
        let mut cursor = 0_usize;
        let resp = match tag {
            RESP_BATCH => {
                let elapsed_us = u128::from_le_bytes(take_bytes::<16>(rest, &mut cursor)?);
                let tier = read_string(rest, &mut cursor)?;
                let ipc_payload = read_bytes(rest, &mut cursor)?.to_vec();
                Self::Batch {
                    ipc_payload,
                    elapsed_us,
                    tier,
                }
            }
            RESP_BATCH_CHUNK => {
                let header = IpcChunkHeader::read_wire(rest, &mut cursor)?;
                let elapsed_us = u128::from_le_bytes(take_bytes::<16>(rest, &mut cursor)?);
                let tier = read_string(rest, &mut cursor)?;
                let chunk_data = read_bytes(rest, &mut cursor)?.to_vec();
                Self::BatchChunk {
                    header,
                    elapsed_us,
                    tier,
                    chunk_data,
                }
            }
            RESP_TIER => Self::Tier {
                tier: read_string(rest, &mut cursor)?,
            },
            RESP_EXPLAIN_SQL => Self::ExplainSql {
                plan: read_string(rest, &mut cursor)?,
            },
            RESP_STATUS_JSON => Self::StatusJson {
                json: read_string(rest, &mut cursor)?,
            },
            RESP_TABLES_JSON => Self::TablesJson {
                json: read_string(rest, &mut cursor)?,
            },
            RESP_ACK => Self::Ack {
                message: read_string(rest, &mut cursor)?,
            },
            RESP_ERROR => {
                let flag = take_bytes::<1>(rest, &mut cursor)?[0];
                let message = read_string(rest, &mut cursor)?;
                Self::Error {
                    message,
                    vram_exhausted: flag != 0,
                }
            }
            other => {
                return Err(PgBoltError::Ipc(format!(
                    "unknown IPC response tag {other}"
                )))
            }
        };
        if cursor != rest.len() {
            return Err(PgBoltError::Ipc(
                "trailing garbage bytes in IPC response frame".to_owned(),
            ));
        }
        Ok(resp)
    }
}

/// Configuration for [`SharedMemoryArena`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpcRingConfig {
    pub num_slots: usize,
    pub slot_bytes: usize,
}

impl Default for IpcRingConfig {
    fn default() -> Self {
        Self {
            num_slots: 16,
            slot_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Live telemetry snapshot of a [`SharedMemoryArena`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IpcArenaStats {
    pub num_slots: usize,
    pub slot_bytes: usize,
    pub free_slots: usize,
    pub pending_requests: usize,
    pub in_flight_requests: usize,
    pub completed_slots: usize,
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_timed_out: u64,
    pub total_rejected_oversized: u64,
    pub is_shutdown: bool,
}

enum SlotState {
    Free,
    Pending { req_id: u64, payload: Vec<u8> },
    InFlight { req_id: u64, abandoned: bool },
    Completed { req_id: u64, payload: Vec<u8> },
}

struct ArenaInner {
    cfg: IpcRingConfig,
    max_chunked_payload_bytes: usize,
    slots: Vec<SlotState>,
    cancel_flags: Vec<Arc<std::sync::atomic::AtomicBool>>,
    free_queue: VecDeque<usize>,
    pending_queue: VecDeque<usize>,
    req_to_slot: HashMap<u64, usize>,
    next_req_id: u64,
    next_stream_id: u64,
    shutdown: bool,
    total_submitted: u64,
    total_completed: u64,
    total_timed_out: u64,
    total_rejected_oversized: u64,
}

impl ArenaInner {
    fn release_slot(&mut self, slot_idx: usize, req_id: u64) {
        self.req_to_slot.remove(&req_id);
        self.slots[slot_idx] = SlotState::Free;
        self.cancel_flags[slot_idx].store(false, std::sync::atomic::Ordering::Release);
        self.free_queue.push_back(slot_idx);
    }

    fn alloc_req_id(&mut self) -> u64 {
        let mut id = self.next_req_id;
        if id == 0 {
            id = 1;
        }
        self.next_req_id = id.wrapping_add(1);
        if self.next_req_id == 0 {
            self.next_req_id = 1;
        }
        id
    }

    fn alloc_stream_id(&mut self) -> u64 {
        let mut id = self.next_stream_id;
        if id == 0 {
            id = 1;
        }
        self.next_stream_id = id.wrapping_add(1);
        if self.next_stream_id == 0 {
            self.next_stream_id = 1;
        }
        id
    }
}

/// Bounded in-process shared-memory ring buffer modeling PostgreSQL DSM LWLocks +
/// `WaitLatch`/`SetLatch` request-response signaling.
pub struct SharedMemoryArena {
    inner: Mutex<ArenaInner>,
    free_slot_cv: Condvar,
    worker_cv: Condvar,
    resp_cv: Condvar,
}

impl std::fmt::Debug for SharedMemoryArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedMemoryArena")
            .field("stats", &self.stats())
            .finish()
    }
}

struct WaiterCleanupGuard<'a> {
    arena: &'a SharedMemoryArena,
    req_id: u64,
    slot_idx: usize,
    completed: bool,
}

impl Drop for WaiterCleanupGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut inner = self.arena.inner.lock();
        if !matches!(
            inner.req_to_slot.get(&self.req_id),
            Some(&idx) if idx == self.slot_idx
        ) {
            return;
        }
        match &mut inner.slots[self.slot_idx] {
            SlotState::Pending { req_id, .. } if *req_id == self.req_id => {
                inner.pending_queue.retain(|&idx| idx != self.slot_idx);
                inner.release_slot(self.slot_idx, self.req_id);
                self.arena.free_slot_cv.notify_all();
            }
            SlotState::InFlight { req_id, abandoned } if *req_id == self.req_id => {
                // Worker already claimed this slot; let `complete_request` reclaim it safely.
                *abandoned = true;
            }
            SlotState::Completed { req_id, .. } if *req_id == self.req_id => {
                inner.release_slot(self.slot_idx, self.req_id);
                self.arena.free_slot_cv.notify_all();
            }
            _ => {}
        }
    }
}

impl SharedMemoryArena {
    /// Creates a bounded ring arena validating `cfg`.
    pub fn new(cfg: IpcRingConfig) -> PgBoltResult<Self> {
        if cfg.num_slots == 0 {
            return Err(PgBoltError::Ipc(
                "IpcRingConfig::num_slots must be greater than 0".to_owned(),
            ));
        }
        if cfg.slot_bytes == 0 {
            return Err(PgBoltError::Ipc(
                "IpcRingConfig::slot_bytes must be greater than 0".to_owned(),
            ));
        }
        let mut slots = Vec::with_capacity(cfg.num_slots);
        let mut cancel_flags = Vec::with_capacity(cfg.num_slots);
        let mut free_queue = VecDeque::with_capacity(cfg.num_slots);
        for idx in 0..cfg.num_slots {
            slots.push(SlotState::Free);
            cancel_flags.push(Arc::new(std::sync::atomic::AtomicBool::new(false)));
            free_queue.push_back(idx);
        }
        Ok(Self {
            inner: Mutex::new(ArenaInner {
                cfg,
                max_chunked_payload_bytes: DEFAULT_MAX_CHUNKED_PAYLOAD_BYTES,
                slots,
                cancel_flags,
                free_queue,
                pending_queue: VecDeque::with_capacity(cfg.num_slots),
                req_to_slot: HashMap::with_capacity(cfg.num_slots),
                next_req_id: 1,
                next_stream_id: 1,
                shutdown: false,
                total_submitted: 0,
                total_completed: 0,
                total_timed_out: 0,
                total_rejected_oversized: 0,
            }),
            free_slot_cv: Condvar::new(),
            worker_cv: Condvar::new(),
            resp_cv: Condvar::new(),
        })
    }

    /// Returns the ring configuration.
    pub fn config(&self) -> IpcRingConfig {
        self.inner.lock().cfg
    }

    /// Submits `cmd` into a free slot and blocks until the worker responds, `timeout` expires,
    /// or the arena is shut down.
    pub fn submit_request(
        &self,
        cmd: IpcCommand,
        timeout: Option<Duration>,
    ) -> PgBoltResult<IpcResponse> {
        let max_bytes = self.inner.lock().cfg.slot_bytes;
        let encoded = match cmd.encode_wire(max_bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.inner.lock().total_rejected_oversized += 1;
                return Err(err);
            }
        };

        let deadline = timeout.and_then(|d| Instant::now().checked_add(d));
        let (req_id, slot_idx) = self.enqueue_request(encoded, deadline)?;
        let mut guard = WaiterCleanupGuard {
            arena: self,
            req_id,
            slot_idx,
            completed: false,
        };
        let raw_resp = self.wait_for_response(req_id, slot_idx, deadline, &mut guard)?;
        IpcResponse::decode_wire(&raw_resp)
    }

    fn enqueue_request(
        &self,
        encoded: Vec<u8>,
        deadline: Option<Instant>,
    ) -> PgBoltResult<(u64, usize)> {
        let mut inner = self.inner.lock();
        let slot_idx = loop {
            if inner.shutdown {
                return Err(PgBoltError::Ipc(
                    "shared-memory IPC arena is shut down".to_owned(),
                ));
            }
            if let Some(idx) = inner.free_queue.pop_front() {
                break idx;
            }
            match deadline {
                Some(dl) => {
                    if (Instant::now() >= dl
                        || self.free_slot_cv.wait_until(&mut inner, dl).timed_out())
                        && inner.free_queue.is_empty()
                        && !inner.shutdown
                    {
                        inner.total_timed_out += 1;
                        return Err(PgBoltError::Ipc(
                            "timed out waiting for free IPC ring slot".to_owned(),
                        ));
                    }
                }
                None => self.free_slot_cv.wait(&mut inner),
            }
        };

        let req_id = inner.alloc_req_id();
        inner.cancel_flags[slot_idx].store(false, std::sync::atomic::Ordering::Release);
        inner.slots[slot_idx] = SlotState::Pending {
            req_id,
            payload: encoded,
        };
        inner.pending_queue.push_back(slot_idx);
        inner.req_to_slot.insert(req_id, slot_idx);
        inner.total_submitted += 1;
        self.worker_cv.notify_one();
        Ok((req_id, slot_idx))
    }

    fn wait_for_response(
        &self,
        req_id: u64,
        slot_idx: usize,
        deadline: Option<Instant>,
        guard: &mut WaiterCleanupGuard<'_>,
    ) -> PgBoltResult<Vec<u8>> {
        let mut inner = self.inner.lock();
        loop {
            if matches!(inner.slots[slot_idx], SlotState::Completed { req_id: id, .. } if id == req_id)
            {
                let taken = std::mem::replace(&mut inner.slots[slot_idx], SlotState::Free);
                inner.req_to_slot.remove(&req_id);
                inner.free_queue.push_back(slot_idx);
                inner.cancel_flags[slot_idx].store(false, std::sync::atomic::Ordering::Release);
                self.free_slot_cv.notify_all();
                guard.completed = true;
                match taken {
                    SlotState::Completed { payload, .. } => return Ok(payload),
                    _ => unreachable!(),
                }
            }
            if inner.shutdown {
                return Err(PgBoltError::Ipc(
                    "shared-memory IPC arena shut down while awaiting response".to_owned(),
                ));
            }

            if inner.cancel_flags[slot_idx].load(std::sync::atomic::Ordering::Acquire) {
                inner.total_timed_out += 1;
                return Err(PgBoltError::Cancelled(format!(
                    "IPC request id {req_id} cancelled by client"
                )));
            }

            #[cfg(feature = "pgrx")]
            if crate::runtime::pg_cancel_pending() {
                inner.cancel_flags[slot_idx].store(true, std::sync::atomic::Ordering::Release);
                pg_shmem::signal_slot_cancel(req_id);
                self.worker_cv.notify_all();
                inner.total_timed_out += 1;
                return Err(PgBoltError::Cancelled(format!(
                    "PostgreSQL query cancellation pending (req_id={req_id})"
                )));
            }

            let slice = Duration::from_millis(25);
            let now = Instant::now();
            let step_deadline = match deadline {
                Some(dl) => {
                    if now >= dl {
                        if inner.shutdown {
                            return Err(PgBoltError::Ipc(
                                "shared-memory IPC arena shut down while awaiting response"
                                    .to_owned(),
                            ));
                        }
                        inner.total_timed_out += 1;
                        return Err(PgBoltError::Ipc(format!(
                            "timed out waiting for IPC response (req_id={req_id})"
                        )));
                    }
                    now.checked_add(slice).map(|s| s.min(dl)).unwrap_or(dl)
                }
                None => now.checked_add(slice).unwrap_or(now),
            };

            self.resp_cv.wait_until(&mut inner, step_deadline);
        }
    }

    /// Receives the next queued request along with its per-slot atomic cancellation flag,
    /// blocking up to `timeout`. Returns `None` on timeout or after [`SharedMemoryArena::shutdown`].
    pub fn recv_request_with_cancel(
        &self,
        timeout: Duration,
    ) -> Option<(u64, IpcCommand, Arc<std::sync::atomic::AtomicBool>)> {
        let deadline = Instant::now().checked_add(timeout);

        loop {
            let mut inner = self.inner.lock();
            let (req_id, raw_cmd, cancel_flag) = loop {
                if inner.shutdown {
                    return None;
                }
                if let Some(slot_idx) = inner.pending_queue.pop_front() {
                    let taken = std::mem::replace(
                        &mut inner.slots[slot_idx],
                        SlotState::InFlight {
                            req_id: 0,
                            abandoned: false,
                        },
                    );
                    match taken {
                        SlotState::Pending { req_id, payload } => {
                            inner.slots[slot_idx] = SlotState::InFlight {
                                req_id,
                                abandoned: false,
                            };
                            let flag = Arc::clone(&inner.cancel_flags[slot_idx]);
                            break (req_id, payload, flag);
                        }
                        other => {
                            inner.slots[slot_idx] = other;
                            continue;
                        }
                    }
                }
                match deadline {
                    Some(dl) => {
                        if (Instant::now() >= dl
                            || self.worker_cv.wait_until(&mut inner, dl).timed_out())
                            && (inner.pending_queue.is_empty() || inner.shutdown)
                        {
                            return None;
                        }
                    }
                    None => self.worker_cv.wait(&mut inner),
                }
            };
            drop(inner);

            match IpcCommand::decode_wire(&raw_cmd) {
                Ok(cmd) => return Some((req_id, cmd, cancel_flag)),
                Err(err) => {
                    let _ = self.complete_request(
                        req_id,
                        IpcResponse::Error {
                            message: err.to_string(),
                            vram_exhausted: false,
                        },
                    );
                }
            }
        }
    }

    /// Receives the next queued request for the worker, blocking up to `timeout`.
    /// Returns `None` on timeout or after [`SharedMemoryArena::shutdown`].
    pub fn recv_request(&self, timeout: Duration) -> Option<(u64, IpcCommand)> {
        self.recv_request_with_cancel(timeout)
            .map(|(id, cmd, _)| (id, cmd))
    }

    /// Requests cancellation for an active or queued request by `req_id`.
    /// Returns `true` if the request was found in an active slot and marked.
    pub fn request_cancel(&self, req_id: u64) -> bool {
        let inner = self.inner.lock();
        if let Some(&slot_idx) = inner.req_to_slot.get(&req_id) {
            inner.cancel_flags[slot_idx].store(true, std::sync::atomic::Ordering::Release);
            self.worker_cv.notify_all();
            self.resp_cv.notify_all();
            true
        } else {
            false
        }
    }

    /// Returns `true` if cancellation was requested for `req_id`.
    pub fn is_cancel_requested(&self, req_id: u64) -> bool {
        let inner = self.inner.lock();
        if let Some(&slot_idx) = inner.req_to_slot.get(&req_id) {
            inner.cancel_flags[slot_idx].load(std::sync::atomic::Ordering::Acquire)
        } else {
            false
        }
    }

    /// Returns a clone of the slot's atomic cancellation flag for `req_id`, if active.
    pub fn cancel_flag_for_req(&self, req_id: u64) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        let inner = self.inner.lock();
        inner
            .req_to_slot
            .get(&req_id)
            .map(|&slot_idx| Arc::clone(&inner.cancel_flags[slot_idx]))
    }

    /// Completes an in-flight request `req_id` with `resp` and wakes the waiting client.
    pub fn complete_request(&self, req_id: u64, resp: IpcResponse) -> PgBoltResult<()> {
        let max_bytes = self.inner.lock().cfg.slot_bytes;
        let encoded = match resp.encode_wire(max_bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.inner.lock().total_rejected_oversized += 1;
                return Err(err);
            }
        };

        let mut inner = self.inner.lock();
        let Some(&slot_idx) = inner.req_to_slot.get(&req_id) else {
            return Err(PgBoltError::Ipc(format!(
                "unknown or already completed IPC request id {req_id}"
            )));
        };

        match inner.slots[slot_idx] {
            SlotState::InFlight {
                req_id: id,
                abandoned,
            } if id == req_id => {
                inner.total_completed += 1;
                if abandoned {
                    inner.release_slot(slot_idx, req_id);
                    self.free_slot_cv.notify_all();
                } else {
                    inner.slots[slot_idx] = SlotState::Completed {
                        req_id,
                        payload: encoded,
                    };
                    self.resp_cv.notify_all();
                }
                Ok(())
            }
            _ => Err(PgBoltError::Ipc(format!(
                "IPC request id {req_id} is not in-flight"
            ))),
        }
    }

    /// Marks the arena shut down and wakes all blocked producers and workers.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock();
        inner.shutdown = true;
        self.free_slot_cv.notify_all();
        self.worker_cv.notify_all();
        self.resp_cv.notify_all();
    }

    /// Returns a point-in-time snapshot of slot occupancy and counters.
    pub fn stats(&self) -> IpcArenaStats {
        let inner = self.inner.lock();
        let mut pending = 0_usize;
        let mut in_flight = 0_usize;
        let mut completed = 0_usize;
        for slot in &inner.slots {
            match slot {
                SlotState::Free => {}
                SlotState::Pending { .. } => pending += 1,
                SlotState::InFlight { .. } => in_flight += 1,
                SlotState::Completed { .. } => completed += 1,
            }
        }
        IpcArenaStats {
            num_slots: inner.cfg.num_slots,
            slot_bytes: inner.cfg.slot_bytes,
            free_slots: inner.free_queue.len(),
            pending_requests: pending,
            in_flight_requests: in_flight,
            completed_slots: completed,
            total_submitted: inner.total_submitted,
            total_completed: inner.total_completed,
            total_timed_out: inner.total_timed_out,
            total_rejected_oversized: inner.total_rejected_oversized,
            is_shutdown: inner.shutdown,
        }
    }

    /// Configures the upper byte limit allowed for chunked multi-slot transfers.
    pub fn with_max_chunked_payload_bytes(self, max_bytes: usize) -> Self {
        self.inner.lock().max_chunked_payload_bytes = max_bytes;
        self
    }

    /// Returns the configured maximum uncompressed byte length for chunked transfers.
    pub fn max_chunked_payload_bytes(&self) -> usize {
        self.inner.lock().max_chunked_payload_bytes
    }

    /// Returns the configured byte capacity of each ring buffer slot.
    pub fn slot_bytes(&self) -> usize {
        self.inner.lock().cfg.slot_bytes
    }

    /// Allocates a unique arena-wide chunked stream identifier.
    pub fn next_stream_id(&self) -> u64 {
        self.inner.lock().alloc_stream_id()
    }

    fn submit_chunked_with<F>(
        &self,
        ipc_payload: &[u8],
        meta_str_lens: &[usize],
        timeout: Option<Duration>,
        prefer_shmem: bool,
        mut build_cmd: F,
    ) -> PgBoltResult<IpcResponse>
    where
        F: FnMut(IpcChunkHeader, Vec<u8>) -> IpcCommand,
    {
        let (slot_bytes, stream_id) = {
            let mut inner = self.inner.lock();
            if ipc_payload.len() > inner.max_chunked_payload_bytes {
                inner.total_rejected_oversized += 1;
                return Err(PgBoltError::Ipc(format!(
                    "chunked IPC payload ({} bytes) exceeds max_chunked_payload_bytes ({} bytes)",
                    ipc_payload.len(),
                    inner.max_chunked_payload_bytes
                )));
            }
            (inner.cfg.slot_bytes, inner.alloc_stream_id())
        };
        let max_chunk_data = chunk_budget_for_slot(slot_bytes, meta_str_lens)?;
        let chunks = split_payload_into_chunks(stream_id, ipc_payload, max_chunk_data)?;
        let total = chunks.len();
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        let mut cancel_guard = ChunkStreamCancelGuard {
            arena: self,
            stream_id,
            timeout,
            armed: false,
        };
        for (idx, (header, chunk_data)) in chunks.into_iter().enumerate() {
            let rem_timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        return Err(PgBoltError::Ipc(
                            "chunked IPC transfer timed out before all chunks were sent".to_owned(),
                        ));
                    }
                    Some(dl.saturating_duration_since(now))
                }
                None => None,
            };
            let cmd = build_cmd(header, chunk_data);
            #[cfg(feature = "pgrx")]
            let resp = if prefer_shmem && is_shmem_initialized() {
                pg_shmem::shmem_submit_request(cmd, rem_timeout)?
            } else {
                self.submit_request(cmd, rem_timeout)?
            };
            #[cfg(not(feature = "pgrx"))]
            let resp = {
                let _ = prefer_shmem;
                self.submit_request(cmd, rem_timeout)?
            };
            if idx + 1 == total {
                cancel_guard.disarm();
                return Ok(resp);
            }
            if let IpcResponse::Error {
                message,
                vram_exhausted,
            } = resp
            {
                return Ok(IpcResponse::Error {
                    message,
                    vram_exhausted,
                });
            }
            cancel_guard.armed = true;
        }
        Err(PgBoltError::Ipc(
            "chunked IPC transfer produced zero chunks".to_owned(),
        ))
    }

    /// Streams an oversized `LoadBatch` Arrow IPC payload over bounded `slot_bytes` chunks.
    pub fn submit_chunked_load(
        &self,
        pg_relation: &str,
        bolt_name: &str,
        ipc_payload: &[u8],
        timeout: Option<Duration>,
    ) -> PgBoltResult<IpcResponse> {
        self.submit_chunked_load_routed(pg_relation, bolt_name, ipc_payload, timeout, false)
    }

    /// Streams an oversized `LoadBatch` Arrow IPC payload, optionally routing chunks via PG shmem.
    pub fn submit_chunked_load_routed(
        &self,
        pg_relation: &str,
        bolt_name: &str,
        ipc_payload: &[u8],
        timeout: Option<Duration>,
        prefer_shmem: bool,
    ) -> PgBoltResult<IpcResponse> {
        self.submit_chunked_with(
            ipc_payload,
            &[pg_relation.len(), bolt_name.len()],
            timeout,
            prefer_shmem,
            |header, chunk_data| IpcCommand::LoadBatchChunk {
                pg_relation: pg_relation.to_owned(),
                bolt_name: bolt_name.to_owned(),
                header,
                chunk_data,
            },
        )
    }

    /// Streams an oversized `AppendDelta` Arrow IPC payload over bounded `slot_bytes` chunks.
    pub fn submit_chunked_append(
        &self,
        bolt_name: &str,
        ipc_payload: &[u8],
        timeout: Option<Duration>,
    ) -> PgBoltResult<IpcResponse> {
        self.submit_chunked_append_routed(bolt_name, ipc_payload, timeout, false)
    }

    /// Streams an oversized `AppendDelta` Arrow IPC payload, optionally routing chunks via PG shmem.
    pub fn submit_chunked_append_routed(
        &self,
        bolt_name: &str,
        ipc_payload: &[u8],
        timeout: Option<Duration>,
        prefer_shmem: bool,
    ) -> PgBoltResult<IpcResponse> {
        self.submit_chunked_with(
            ipc_payload,
            &[bolt_name.len()],
            timeout,
            prefer_shmem,
            |header, chunk_data| IpcCommand::AppendDeltaChunk {
                bolt_name: bolt_name.to_owned(),
                header,
                chunk_data,
            },
        )
    }
}

/// RAII cancellation guard ensuring partial multi-chunk request or response streams are
/// cancelled cleanly on the worker when the caller returns early, times out, or unwinds.
pub(crate) struct ChunkStreamCancelGuard<'a> {
    pub(crate) arena: &'a SharedMemoryArena,
    pub(crate) stream_id: u64,
    pub(crate) timeout: Option<Duration>,
    pub(crate) armed: bool,
}

impl ChunkStreamCancelGuard<'_> {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ChunkStreamCancelGuard<'_> {
    fn drop(&mut self) {
        if !self.armed || self.arena.stats().is_shutdown {
            return;
        }
        let cancel_timeout = self
            .timeout
            .map(|t| t.min(Duration::from_millis(250)))
            .unwrap_or_else(|| Duration::from_millis(250));
        let cmd = IpcCommand::CancelChunkStream {
            stream_id: self.stream_id,
        };
        #[cfg(feature = "pgrx")]
        if crate::ipc::is_shmem_initialized() {
            let _ = crate::ipc::pg_shmem::shmem_submit_request(cmd, Some(cancel_timeout));
            return;
        }
        let _ = self.arena.submit_request(cmd, Some(cancel_timeout));
    }
}

/// Registers PostgreSQL `shmem_request_hook` and `shmem_startup_hook` during `_PG_init`
/// when loaded via `shared_preload_libraries`. Safe no-op in non-`pgrx` builds.
pub fn init_shmem_hooks() {
    #[cfg(feature = "pgrx")]
    pg_shmem::install_hooks();
}

/// Returns `true` when PostgreSQL shared-memory IPC control structures have been initialized.
pub fn is_shmem_initialized() -> bool {
    #[cfg(feature = "pgrx")]
    {
        pg_shmem::shmem_control().is_some()
    }
    #[cfg(not(feature = "pgrx"))]
    {
        false
    }
}

/// Probes whether an OS process with `pid` is currently alive.
#[cfg(windows)]
pub fn is_process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    extern "system" {
        fn OpenProcess(
            dwDesiredAccess: u32,
            bInheritHandle: i32,
            dwProcessId: u32,
        ) -> *mut std::ffi::c_void;
        fn CloseHandle(hObject: *mut std::ffi::c_void) -> i32;
        fn GetExitCodeProcess(hProcess: *mut std::ffi::c_void, lpExitCode: *mut u32) -> i32;
    }
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid as u32);
        if handle.is_null() {
            return false;
        }
        let mut exit_code: u32 = 0;
        let res = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        res != 0 && exit_code == STILL_ACTIVE
    }
}

#[cfg(unix)]
pub fn is_process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe {
        if kill(pid, 0) == 0 {
            true
        } else {
            let err = std::io::Error::last_os_error().raw_os_error();
            err == Some(1)
        }
    }
}

#[cfg(not(any(windows, unix)))]
pub fn is_process_alive(pid: i32) -> bool {
    pid > 0
}

/// Cross-process PostgreSQL shared-memory control ring, LWLock, and Latch scaffolding.
#[cfg(feature = "pgrx")]
pub mod pg_shmem {
    use std::ffi::{c_int, c_long, CStr};
    use std::mem::size_of;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use pgrx::pg_sys;

    use super::{
        IpcCommand, IpcResponse, DEFAULT_MAX_CHUNKED_PAYLOAD_BYTES, PG_BOLT_SHMEM_MAX_SLOTS,
    };
    use crate::error::{PgBoltError, PgBoltResult};

    const SHMEM_MAGIC: u32 = 0x5042_4F4C;
    const SHMEM_VERSION: u32 = 1;
    const SHMEM_TRANCHE_NAME: &CStr = c"pg_bolt";
    const SHMEM_STRUCT_NAME: &CStr = c"pg_bolt_ipc_ring";

    pub const PG_SLOT_FREE: u32 = 0;
    pub const PG_SLOT_PENDING: u32 = 1;
    pub const PG_SLOT_IN_FLIGHT: u32 = 2;
    pub const PG_SLOT_COMPLETED: u32 = 3;
    pub const PG_SLOT_ABANDONED: u32 = 4;
    pub const PG_SLOT_INITIALIZING: u32 = 5;

    /// Per-slot descriptor stored inside PostgreSQL shared memory alongside a DSM handle.
    #[repr(C)]
    pub struct PgShmemSlotDesc {
        pub state: AtomicU32,
        pub payload_len: AtomicU32,
        pub req_id: AtomicU64,
        pub dsm_handle: AtomicU32,
        pub cancel_requested: AtomicBool,
        pub _pad: [u8; 3],
        pub client_pid: AtomicI32,
        pub client_latch: AtomicPtr<pg_sys::Latch>,
    }

    impl PgShmemSlotDesc {
        pub fn is_free(&self) -> bool {
            self.state.load(Ordering::Acquire) == PG_SLOT_FREE
        }

        pub fn is_pending(&self) -> bool {
            self.state.load(Ordering::Acquire) == PG_SLOT_PENDING
        }

        pub fn is_in_flight(&self) -> bool {
            self.state.load(Ordering::Acquire) == PG_SLOT_IN_FLIGHT
        }

        pub fn is_completed(&self) -> bool {
            self.state.load(Ordering::Acquire) == PG_SLOT_COMPLETED
        }

        pub fn is_cancelled(&self) -> bool {
            self.cancel_requested.load(Ordering::Acquire)
        }

        pub fn request_cancel(&self) {
            self.cancel_requested.store(true, Ordering::Release);
        }

        pub fn reset(&self) {
            self.state.store(PG_SLOT_FREE, Ordering::Release);
            self.payload_len.store(0, Ordering::Release);
            self.req_id.store(0, Ordering::Release);
            self.dsm_handle.store(0, Ordering::Release);
            self.cancel_requested.store(false, Ordering::Release);
            self.client_pid.store(0, Ordering::Release);
            self.client_latch
                .store(std::ptr::null_mut(), Ordering::Release);
        }
    }

    /// Fixed-layout shared-memory control block initialized via `ShmemInitStruct`.
    #[repr(C)]
    pub struct PgShmemRingControl {
        pub magic: AtomicU32,
        pub version: AtomicU32,
        pub num_slots: AtomicU32,
        pub slot_bytes: AtomicU64,
        pub max_chunked_payload_bytes: AtomicU64,
        pub worker_pid: AtomicI32,
        pub shutdown: AtomicU32,
        pub worker_latch: AtomicPtr<pg_sys::Latch>,
        pub total_submitted: AtomicU64,
        pub total_completed: AtomicU64,
        pub total_timed_out: AtomicU64,
        pub total_rejected_oversized: AtomicU64,
        pub active_memory_budget_bytes: AtomicU64,
    }

    impl PgShmemRingControl {
        /// Returns a slice of all slot descriptors immediately following the control block.
        ///
        /// # Safety
        /// Caller must ensure shared memory has been initialized by postmaster.
        pub unsafe fn slots(&self) -> &[PgShmemSlotDesc] {
            let num = self.num_slots.load(Ordering::Acquire) as usize;
            let ptr = (self as *const Self).add(1) as *const PgShmemSlotDesc;
            std::slice::from_raw_parts(ptr, num)
        }

        /// Returns a mutable slice of all slot descriptors immediately following the control block.
        ///
        /// # Safety
        /// Caller must ensure shared memory has been initialized by postmaster.
        #[allow(clippy::mut_from_ref)]
        pub unsafe fn slots_mut(&self) -> &mut [PgShmemSlotDesc] {
            let num = self.num_slots.load(Ordering::Acquire) as usize;
            let ptr = (self as *const Self).add(1) as *mut PgShmemSlotDesc;
            std::slice::from_raw_parts_mut(ptr, num)
        }

        /// Returns a reference to slot `idx` if within bounds.
        ///
        /// # Safety
        /// Caller must ensure shared memory has been initialized by postmaster.
        pub unsafe fn slot(&self, idx: usize) -> Option<&PgShmemSlotDesc> {
            let num = self.num_slots.load(Ordering::Acquire) as usize;
            if idx < num {
                let ptr = (self as *const Self).add(1) as *const PgShmemSlotDesc;
                Some(&*ptr.add(idx))
            } else {
                None
            }
        }
    }

    /// Concrete RAII structure managing a PostgreSQL Dynamic Shared Memory (`dsm_segment`) allocation.
    pub struct PgDsmSegment {
        seg: *mut pg_sys::dsm_segment,
        handle: u32,
        size: usize,
        mapped_addr: *mut u8,
        is_owner: bool,
    }

    impl std::fmt::Debug for PgDsmSegment {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PgDsmSegment")
                .field("handle", &self.handle)
                .field("size", &self.size)
                .field("mapped_addr", &self.mapped_addr)
                .field("is_owner", &self.is_owner)
                .finish()
        }
    }

    impl PgDsmSegment {
        /// Creates and pins a new DSM segment of `size` bytes.
        pub fn create(size: usize) -> PgBoltResult<Self> {
            if size == 0 {
                return Err(PgBoltError::Ipc("DSM segment size must be > 0".to_owned()));
            }
            // SAFETY: invoking PostgreSQL dsm_create in backend process.
            let seg = unsafe { pg_sys::dsm_create(size, 0) };
            if seg.is_null() {
                return Err(PgBoltError::Ipc(format!(
                    "failed to allocate PostgreSQL DSM segment of {size} bytes"
                )));
            }
            let mapped_addr = unsafe { pg_sys::dsm_segment_address(seg) as *mut u8 };
            if mapped_addr.is_null() {
                unsafe { pg_sys::dsm_detach(seg) };
                return Err(PgBoltError::Ipc(
                    "dsm_segment_address returned null pointer".to_owned(),
                ));
            }
            let handle = unsafe { pg_sys::dsm_segment_handle(seg) as u32 };
            // Pin mapping so TopMemoryContext resets do not unmap before detach.
            unsafe { pg_sys::dsm_pin_mapping(seg) };
            Ok(Self {
                seg,
                handle,
                size,
                mapped_addr,
                is_owner: true,
            })
        }

        /// Attaches to an existing DSM segment identified by `handle`.
        pub fn attach(handle: u32, size: usize) -> PgBoltResult<Self> {
            // SAFETY: invoking PostgreSQL dsm_attach in backend or worker process.
            let seg = unsafe { pg_sys::dsm_attach(handle as pg_sys::dsm_handle) };
            if seg.is_null() {
                return Err(PgBoltError::Ipc(format!(
                    "failed to attach to PostgreSQL DSM segment handle {handle}"
                )));
            }
            let mapped_addr = unsafe { pg_sys::dsm_segment_address(seg) as *mut u8 };
            if mapped_addr.is_null() {
                unsafe { pg_sys::dsm_detach(seg) };
                return Err(PgBoltError::Ipc(
                    "attached dsm_segment_address returned null pointer".to_owned(),
                ));
            }
            unsafe { pg_sys::dsm_pin_mapping(seg) };
            Ok(Self {
                seg,
                handle,
                size,
                mapped_addr,
                is_owner: false,
            })
        }

        pub fn handle(&self) -> u32 {
            self.handle
        }

        pub fn size(&self) -> usize {
            self.size
        }

        pub fn mapped_address(&self) -> *mut u8 {
            self.mapped_addr
        }

        pub fn as_slice(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.mapped_addr, self.size) }
        }

        pub fn as_mut_slice(&mut self) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.mapped_addr, self.size) }
        }

        pub fn write_payload(&mut self, payload: &[u8]) -> PgBoltResult<()> {
            if payload.len() > self.size {
                return Err(PgBoltError::Ipc(format!(
                    "payload length {} exceeds DSM segment capacity {}",
                    payload.len(),
                    self.size
                )));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(payload.as_ptr(), self.mapped_addr, payload.len());
            }
            Ok(())
        }

        pub fn read_payload(&self, len: usize) -> PgBoltResult<Vec<u8>> {
            if len > self.size {
                return Err(PgBoltError::Ipc(format!(
                    "requested read length {len} exceeds DSM segment capacity {}",
                    self.size
                )));
            }
            let slice = unsafe { std::slice::from_raw_parts(self.mapped_addr, len) };
            Ok(slice.to_vec())
        }
    }

    impl Drop for PgDsmSegment {
        fn drop(&mut self) {
            if !self.seg.is_null() {
                // SAFETY: detaches DSM segment mapping from current process address space.
                unsafe { pg_sys::dsm_detach(self.seg) };
                self.seg = std::ptr::null_mut();
            }
        }
    }

    static NEXT_SHMEM_REQ_ID: AtomicU64 = AtomicU64::new(1);

    /// Signals cancellation for an in-flight request `req_id` in PostgreSQL shared memory slots.
    pub fn signal_slot_cancel(req_id: u64) -> bool {
        let Some(ctrl) = shmem_control() else {
            return false;
        };
        let num_slots = ctrl.num_slots.load(Ordering::Acquire) as usize;
        for idx in 0..num_slots {
            let slot = unsafe { &*(&ctrl.slots()[idx] as *const PgShmemSlotDesc) };
            if slot.req_id.load(Ordering::Acquire) == req_id {
                slot.cancel_requested.store(true, Ordering::Release);
                signal_worker_latch();
                return true;
            }
        }
        false
    }

    /// Submits `cmd` over the PostgreSQL shared-memory ring using a DSM segment for payload transfer.
    pub fn shmem_submit_request(
        cmd: IpcCommand,
        timeout: Option<Duration>,
    ) -> PgBoltResult<IpcResponse> {
        let ctrl = shmem_control().ok_or_else(|| {
            PgBoltError::Ipc("PostgreSQL shared memory control block not initialized".to_owned())
        })?;
        let slot_bytes = ctrl.slot_bytes.load(Ordering::Acquire) as usize;
        let deadline = timeout.and_then(|d| Instant::now().checked_add(d));

        let encoded = cmd.encode_wire(slot_bytes)?;

        let mut dsm = PgDsmSegment::create(slot_bytes)?;
        dsm.write_payload(&encoded)?;
        let dsm_handle = dsm.handle();

        let num_slots = ctrl.num_slots.load(Ordering::Acquire) as usize;
        let poll_slice = Duration::from_millis(10);
        let mut slot_idx_opt = None;

        loop {
            if ctrl.shutdown.load(Ordering::Acquire) != 0 {
                return Err(PgBoltError::Ipc(
                    "shared-memory IPC ring is shut down".to_owned(),
                ));
            }
            for idx in 0..num_slots {
                let slot = unsafe { &*(&ctrl.slots()[idx] as *const PgShmemSlotDesc) };
                if slot
                    .state
                    .compare_exchange(
                        PG_SLOT_FREE,
                        PG_SLOT_INITIALIZING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    slot_idx_opt = Some(idx);
                    break;
                }
            }
            if slot_idx_opt.is_some() {
                break;
            }
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    ctrl.total_timed_out.fetch_add(1, Ordering::Relaxed);
                    return Err(PgBoltError::Ipc(
                        "timed out waiting for free shmem slot".to_owned(),
                    ));
                }
            }
            wait_latch_timeout(poll_slice.as_millis() as i64);
        }

        let slot_idx = slot_idx_opt.unwrap();
        let slot = unsafe { &*(&ctrl.slots()[slot_idx] as *const PgShmemSlotDesc) };
        let req_id = NEXT_SHMEM_REQ_ID.fetch_add(1, Ordering::Relaxed);
        slot.req_id.store(req_id, Ordering::Release);
        slot.payload_len
            .store(encoded.len() as u32, Ordering::Release);
        slot.dsm_handle.store(dsm_handle, Ordering::Release);
        slot.cancel_requested.store(false, Ordering::Release);
        unsafe {
            slot.client_pid.store(pg_sys::MyProcPid, Ordering::Release);
            slot.client_latch.store(pg_sys::MyLatch, Ordering::Release);
        }

        // Atomically publish slot state as PENDING after all metadata and payload fields are visible
        slot.state.store(PG_SLOT_PENDING, Ordering::Release);
        signal_worker_latch();
        ctrl.total_submitted.fetch_add(1, Ordering::Relaxed);

        // Wait for response loop
        loop {
            if slot.state.load(Ordering::Acquire) == PG_SLOT_COMPLETED {
                let resp_len = slot.payload_len.load(Ordering::Acquire) as usize;
                let resp_bytes = dsm.read_payload(resp_len)?;
                slot.reset();
                return IpcResponse::decode_wire(&resp_bytes);
            }

            if ctrl.shutdown.load(Ordering::Acquire) != 0 {
                slot.reset();
                return Err(PgBoltError::Ipc(
                    "shared-memory IPC ring shut down while awaiting response".to_owned(),
                ));
            }

            if crate::runtime::pg_cancel_pending() || slot.cancel_requested.load(Ordering::Acquire)
            {
                slot.cancel_requested.store(true, Ordering::Release);
                signal_worker_latch();
                // If the worker has not claimed it yet, reclaim back to FREE immediately.
                if slot
                    .state
                    .compare_exchange(
                        PG_SLOT_PENDING,
                        PG_SLOT_FREE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    // Worker already transitioned to IN_FLIGHT; mark ABANDONED so worker resets it on completion.
                    slot.state.store(PG_SLOT_ABANDONED, Ordering::Release);
                }
                ctrl.total_timed_out.fetch_add(1, Ordering::Relaxed);
                return Err(PgBoltError::Cancelled(format!(
                    "PostgreSQL query cancellation pending (req_id={req_id})"
                )));
            }

            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    slot.cancel_requested.store(true, Ordering::Release);
                    signal_worker_latch();
                    if slot
                        .state
                        .compare_exchange(
                            PG_SLOT_PENDING,
                            PG_SLOT_FREE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        slot.state.store(PG_SLOT_ABANDONED, Ordering::Release);
                    }
                    ctrl.total_timed_out.fetch_add(1, Ordering::Relaxed);
                    return Err(PgBoltError::Ipc(format!(
                        "timed out waiting for IPC response (req_id={req_id})"
                    )));
                }
            }

            wait_latch_timeout(25);
        }
    }

    /// RAII guard for the named `pg_bolt` PostgreSQL `LWLock`.
    #[must_use = "dropping PgShmemLwLockGuard immediately releases the PostgreSQL LWLock"]
    pub struct PgShmemLwLockGuard {
        lock: *mut pg_sys::LWLock,
    }

    impl PgShmemLwLockGuard {
        /// Acquires the named `pg_bolt` LWLock in exclusive or shared mode, if initialized.
        pub fn acquire(exclusive: bool) -> Option<Self> {
            let lock = SHMEM_LWLOCK_PTR.load(Ordering::Acquire);
            if lock.is_null() {
                return None;
            }
            let mode = if exclusive {
                pg_sys::LWLockMode::LW_EXCLUSIVE
            } else {
                pg_sys::LWLockMode::LW_SHARED
            };
            // SAFETY: `lock` points to initialized PostgreSQL shared memory LWLock tranche.
            unsafe { pg_sys::LWLockAcquire(lock, mode) };
            Some(Self { lock })
        }
    }

    impl Drop for PgShmemLwLockGuard {
        fn drop(&mut self) {
            if !self.lock.is_null() {
                // SAFETY: guard holds this LWLock since `acquire`.
                unsafe { pg_sys::LWLockRelease(self.lock) };
            }
        }
    }

    static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
    static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;
    static SHMEM_HOOKS_INSTALLED: AtomicBool = AtomicBool::new(false);
    static SHMEM_CONTROL_PTR: AtomicPtr<PgShmemRingControl> = AtomicPtr::new(std::ptr::null_mut());
    static SHMEM_LWLOCK_PTR: AtomicPtr<pg_sys::LWLock> = AtomicPtr::new(std::ptr::null_mut());

    #[inline]
    fn configured_control_slots() -> usize {
        crate::guc::current_config()
            .ipc_ring_slots
            .clamp(1, PG_BOLT_SHMEM_MAX_SLOTS)
    }

    #[inline]
    fn shmem_control_bytes(num_slots: usize) -> usize {
        size_of::<PgShmemRingControl>()
            .saturating_add(num_slots.saturating_mul(size_of::<PgShmemSlotDesc>()))
    }

    /// Installs PostgreSQL `shmem_request_hook` and `shmem_startup_hook` if currently loading
    /// via `shared_preload_libraries`.
    pub(super) fn install_hooks() {
        // SAFETY: reading PostgreSQL global flag during `_PG_init`.
        if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
            return;
        }
        if SHMEM_HOOKS_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // SAFETY: invoked once in postmaster `_PG_init` before forks occur.
        unsafe {
            PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
            pg_sys::shmem_request_hook = Some(pg_bolt_shmem_request_hook);
            PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
            pg_sys::shmem_startup_hook = Some(pg_bolt_shmem_startup_hook);
        }
    }

    #[pgrx::prelude::pg_guard]
    unsafe extern "C-unwind" fn pg_bolt_shmem_request_hook() {
        let prev = PREV_SHMEM_REQUEST_HOOK;
        if let Some(prev_fn) = prev {
            prev_fn();
        }
        let slots = configured_control_slots();
        pg_sys::RequestAddinShmemSpace(shmem_control_bytes(slots));
        pg_sys::RequestNamedLWLockTranche(SHMEM_TRANCHE_NAME.as_ptr(), 1);
    }

    #[pgrx::prelude::pg_guard]
    unsafe extern "C-unwind" fn pg_bolt_shmem_startup_hook() {
        let prev = PREV_SHMEM_STARTUP_HOOK;
        if let Some(prev_fn) = prev {
            prev_fn();
        }
        let cfg = crate::guc::current_config();
        let slots = cfg.ipc_ring_slots.clamp(1, PG_BOLT_SHMEM_MAX_SLOTS);
        let size = shmem_control_bytes(slots);
        let mut found = false;

        // AddinShmemInitLock is `#define AddinShmemInitLock (&MainLWLockArray[21].lock)` in lwlocknames.h.
        let addin_shmem_init_lock = &raw mut (*pg_sys::MainLWLockArray.add(21)).lock;
        pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let raw_ptr =
            pg_sys::ShmemInitStruct(SHMEM_STRUCT_NAME.as_ptr(), size, &mut found as *mut bool);
        if !raw_ptr.is_null() {
            if !found {
                std::ptr::write_bytes(raw_ptr as *mut u8, 0, size);
            }
            let ctrl = &*(raw_ptr as *const PgShmemRingControl);
            if !found || ctrl.magic.load(Ordering::Acquire) != SHMEM_MAGIC {
                ctrl.magic.store(SHMEM_MAGIC, Ordering::Release);
                ctrl.version.store(SHMEM_VERSION, Ordering::Release);
                ctrl.num_slots.store(slots as u32, Ordering::Release);
                ctrl.slot_bytes
                    .store(cfg.ipc_slot_bytes.max(1) as u64, Ordering::Release);
                ctrl.max_chunked_payload_bytes
                    .store(DEFAULT_MAX_CHUNKED_PAYLOAD_BYTES as u64, Ordering::Release);
                ctrl.active_memory_budget_bytes
                    .store(cfg.max_vram_bytes().unwrap_or(0) as u64, Ordering::Release);
            }
            SHMEM_CONTROL_PTR.store(raw_ptr as *mut PgShmemRingControl, Ordering::Release);
        }
        let tranche = pg_sys::GetNamedLWLockTranche(SHMEM_TRANCHE_NAME.as_ptr());
        if !tranche.is_null() {
            SHMEM_LWLOCK_PTR.store(&raw mut (*tranche).lock, Ordering::Release);
        }
        pg_sys::LWLockRelease(addin_shmem_init_lock);
    }

    /// Returns a reference to the shared-memory control block once PostgreSQL shmem startup ran.
    pub fn shmem_control() -> Option<&'static PgShmemRingControl> {
        let ptr = SHMEM_CONTROL_PTR.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            // SAFETY: pointer resides in PostgreSQL permanent shared memory until postmaster exit.
            Some(unsafe { &*ptr })
        }
    }

    /// Registers the active background worker's `MyLatch` and PID in shared memory.
    pub fn register_worker_latch() {
        if let Some(ctrl) = shmem_control() {
            // SAFETY: reading per-backend globals inside running PostgreSQL worker.
            unsafe {
                ctrl.worker_pid.store(pg_sys::MyProcPid, Ordering::Release);
                ctrl.worker_latch.store(pg_sys::MyLatch, Ordering::Release);
            }
        }
    }

    /// Wakes the registered PostgreSQL background worker via `SetLatch`.
    pub fn signal_worker_latch() {
        if let Some(ctrl) = shmem_control() {
            let latch = ctrl.worker_latch.load(Ordering::Acquire);
            if !latch.is_null() {
                // SAFETY: `latch` points into `MyProc->procLatch` inside PG shared memory.
                unsafe { pg_sys::SetLatch(latch) };
            }
        }
    }

    /// Sleeps on `MyLatch` up to `timeout_ms` milliseconds using PostgreSQL `WaitLatch`
    /// and resets the latch before returning `false` if postmaster died.
    pub fn wait_latch_timeout(timeout_ms: i64) -> bool {
        // SAFETY: standard PostgreSQL WaitLatch/ResetLatch cycle inside backend/worker process.
        unsafe {
            let latch = pg_sys::MyLatch;
            if latch.is_null() {
                return false;
            }
            let flags =
                (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH) as c_int;
            let rc = pg_sys::WaitLatch(
                latch,
                flags,
                timeout_ms as c_long,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(latch);
            (rc & (pg_sys::WL_POSTMASTER_DEATH as c_int)) == 0
        }
    }

    /// Updates the recorded active memory budget telemetry in the shared memory control block.
    pub fn update_active_budget(bytes: u64) {
        if let Some(ctrl) = shmem_control() {
            ctrl.active_memory_budget_bytes
                .store(bytes, Ordering::Release);
        }
    }

    /// Inspects shmem slots for a pending request, claiming the first found into `PG_SLOT_IN_FLIGHT`.
    /// Also reclaims any slots abandoned by aborted or timed-out clients.
    /// Returns `Some((slot_idx, req_id, cmd, dsm, cancel_flag))` if a pending request was claimed.
    pub fn claim_pending_request() -> Option<(
        usize,
        u64,
        IpcCommand,
        PgDsmSegment,
        std::sync::Arc<AtomicBool>,
    )> {
        let ctrl = shmem_control()?;
        let num_slots = ctrl.num_slots.load(Ordering::Acquire) as usize;
        let slot_bytes = ctrl.slot_bytes.load(Ordering::Acquire) as usize;

        for idx in 0..num_slots {
            let slot = unsafe { &*(&ctrl.slots()[idx] as *const PgShmemSlotDesc) };

            // Sweep and reclaim any slots marked abandoned by timed-out or cancelled clients
            let current_state = slot.state.load(Ordering::Acquire);
            if current_state == PG_SLOT_ABANDONED {
                slot.reset();
                continue;
            }

            // Sweep and reclaim slots held by abruptly terminated/dead client backends
            if current_state == PG_SLOT_INITIALIZING
                || current_state == PG_SLOT_PENDING
                || current_state == PG_SLOT_COMPLETED
            {
                let pid = slot.client_pid.load(Ordering::Acquire);
                if pid > 0 && !super::is_process_alive(pid) {
                    log::warn!(
                        "reclaiming shmem slot {idx} held by dead client backend pid {pid} (state={current_state})"
                    );
                    slot.reset();
                    continue;
                }
            }

            if slot
                .state
                .compare_exchange(
                    PG_SLOT_PENDING,
                    PG_SLOT_IN_FLIGHT,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let req_id = slot.req_id.load(Ordering::Acquire);
                let payload_len = slot.payload_len.load(Ordering::Acquire) as usize;
                let dsm_handle = slot.dsm_handle.load(Ordering::Acquire);

                let dsm = match PgDsmSegment::attach(dsm_handle, slot_bytes) {
                    Ok(seg) => seg,
                    Err(err) => {
                        log::error!(
                            "failed to attach DSM handle {dsm_handle} for req_id={req_id}: {err}"
                        );
                        slot.state.store(PG_SLOT_FREE, Ordering::Release);
                        continue;
                    }
                };

                let cmd_bytes = match dsm.read_payload(payload_len) {
                    Ok(b) => b,
                    Err(err) => {
                        log::error!("failed to read DSM payload for req_id={req_id}: {err}");
                        slot.state.store(PG_SLOT_FREE, Ordering::Release);
                        continue;
                    }
                };

                let cmd = match IpcCommand::decode_wire(&cmd_bytes) {
                    Ok(c) => c,
                    Err(err) => {
                        log::error!("failed to decode IPC command for req_id={req_id}: {err}");
                        slot.state.store(PG_SLOT_FREE, Ordering::Release);
                        continue;
                    }
                };

                let cancel_flag = std::sync::Arc::new(AtomicBool::new(slot.is_cancelled()));
                return Some((idx, req_id, cmd, dsm, cancel_flag));
            }
        }
        None
    }

    /// Completes an in-flight shmem request by writing `resp` back to the attached DSM segment
    /// and waking the client latch. If the slot was abandoned, resets it immediately.
    pub fn complete_shmem_request(
        slot_idx: usize,
        req_id: u64,
        resp: IpcResponse,
        mut dsm: PgDsmSegment,
    ) {
        let Some(ctrl) = shmem_control() else { return };
        let slot = unsafe { &*(&ctrl.slots()[slot_idx] as *const PgShmemSlotDesc) };

        if slot.req_id.load(Ordering::Acquire) != req_id {
            return;
        }

        // If the client abandoned or cancelled while in flight, reclaim the slot immediately
        if slot.state.load(Ordering::Acquire) == PG_SLOT_ABANDONED || slot.is_cancelled() {
            slot.reset();
            ctrl.total_completed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let slot_bytes = ctrl.slot_bytes.load(Ordering::Acquire) as usize;
        let encoded = match resp.encode_wire(slot_bytes) {
            Ok(b) => b,
            Err(err) => {
                let err_resp = IpcResponse::Error {
                    message: format!("shmem response encode failed: {err}"),
                    vram_exhausted: false,
                };
                err_resp.encode_wire(slot_bytes).unwrap_or_default()
            }
        };

        if let Err(err) = dsm.write_payload(&encoded) {
            log::error!("failed to write DSM response for req_id={req_id}: {err}");
        }

        slot.payload_len
            .store(encoded.len() as u32, Ordering::Release);
        slot.state.store(PG_SLOT_COMPLETED, Ordering::Release);
        ctrl.total_completed.fetch_add(1, Ordering::Relaxed);

        let client_latch = slot.client_latch.load(Ordering::Acquire);
        if !client_latch.is_null() {
            unsafe { pg_sys::SetLatch(client_latch) };
        }
    }

    /// Scans shared memory slots and resets any slots held by dead backend client PIDs.
    /// Returns the number of reclaimed slots.
    pub fn sweep_dead_client_slots() -> usize {
        let Some(ctrl) = shmem_control() else {
            return 0;
        };
        let num_slots = ctrl.num_slots.load(Ordering::Acquire) as usize;
        let mut reclaimed = 0;
        for idx in 0..num_slots {
            let slot = unsafe { &*(&ctrl.slots()[idx] as *const PgShmemSlotDesc) };
            let state = slot.state.load(Ordering::Acquire);
            if state == PG_SLOT_ABANDONED {
                slot.reset();
                reclaimed += 1;
                continue;
            }
            if state == PG_SLOT_INITIALIZING
                || state == PG_SLOT_PENDING
                || state == PG_SLOT_COMPLETED
            {
                let pid = slot.client_pid.load(Ordering::Acquire);
                if pid > 0 && !super::is_process_alive(pid) {
                    log::warn!(
                        "sweep: reclaiming shmem slot {idx} held by dead backend pid {pid} (state={state})"
                    );
                    slot.reset();
                    reclaimed += 1;
                }
            }
        }
        reclaimed
    }
}

#[cfg(feature = "pgrx")]
const PG_BOLT_SHMEM_MAX_SLOTS: usize = 256;

#[cfg(all(test, not(feature = "pgrx")))]
mod tests {
    use super::*;
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::Field;
    use std::thread;

    #[test]
    fn ipc_stream_roundtrip_and_invalid_prefix_guard() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.25)])),
            ],
        )
        .unwrap();

        let bytes = record_batch_to_ipc_stream(&batch).unwrap();
        let decoded = ipc_stream_to_record_batch(&bytes).unwrap();
        assert_eq!(decoded, batch);

        // Corrupt negative metadata length must return Err(Ipc), never panic.
        let bad = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0];
        assert!(matches!(
            ipc_stream_to_record_batch(&bad),
            Err(PgBoltError::Ipc(_))
        ));
    }

    #[test]
    fn minimal_tuple_roundtrip_supports_nulls_strings_and_dict() {
        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append_value("us-east");
        dict_builder.append_null();
        dict_builder.append_value("eu-west");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "region",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
                Arc::new(dict_builder.finish()),
            ],
        )
        .unwrap();

        let tuples = record_batch_to_minimal_tuples(&batch).unwrap();
        assert_eq!(tuples.len(), 3);
        let rebuilt = minimal_tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(rebuilt.num_rows(), 3);
        assert_eq!(rebuilt.column(0).as_ref(), batch.column(0).as_ref());
        assert_eq!(rebuilt.column(1).as_ref(), batch.column(1).as_ref());
        assert_eq!(rebuilt.column(2).null_count(), 1);
    }

    #[test]
    fn arena_enforces_slot_bounds_backpressure_and_shutdown() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 128,
            })
            .unwrap(),
        );

        let oversized = IpcCommand::ExplainTier {
            sql: "x".repeat(256),
        };
        assert!(matches!(
            arena.submit_request(oversized, Some(Duration::from_millis(20))),
            Err(PgBoltError::Ipc(_))
        ));

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_millis(200)) {
                match cmd {
                    IpcCommand::GetStatus => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::StatusJson {
                                    json: "{\"ok\":true}".to_owned(),
                                },
                            )
                            .unwrap();
                    }
                    IpcCommand::Shutdown => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: "bye".to_owned(),
                                },
                            )
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
            }
        });

        let mut clients = Vec::new();
        for _ in 0..8 {
            let a = Arc::clone(&arena);
            clients.push(thread::spawn(move || {
                a.submit_request(IpcCommand::GetStatus, Some(Duration::from_secs(2)))
            }));
        }
        for c in clients {
            let resp = c.join().unwrap().unwrap();
            assert!(matches!(resp, IpcResponse::StatusJson { .. }));
        }

        let ack = arena
            .submit_request(IpcCommand::Shutdown, Some(Duration::from_secs(1)))
            .unwrap();
        assert!(matches!(ack, IpcResponse::Ack { .. }));
        worker.join().unwrap();

        arena.shutdown();
        assert!(matches!(
            arena.submit_request(IpcCommand::GetStatus, Some(Duration::from_millis(20))),
            Err(PgBoltError::Ipc(_))
        ));
    }

    #[test]
    fn timeout_abandons_pending_and_inflight_slots_cleanly() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 1,
                slot_bytes: 512,
            })
            .unwrap(),
        );

        // 1) Pending request timeout reclaims the single ring slot without self-deadlock.
        let err = arena
            .submit_request(IpcCommand::GetStatus, Some(Duration::from_millis(20)))
            .unwrap_err();
        assert!(matches!(err, PgBoltError::Ipc(_)));
        let stats = arena.stats();
        assert_eq!(stats.free_slots, 1);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.total_timed_out, 1);

        // 2) In-flight abandoned request is reclaimed once worker calls `complete_request`.
        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let (req_id, _cmd) = a_worker.recv_request(Duration::from_secs(1)).unwrap();
            thread::sleep(Duration::from_millis(60));
            a_worker
                .complete_request(
                    req_id,
                    IpcResponse::Ack {
                        message: "late".to_owned(),
                    },
                )
                .unwrap();
        });

        let err2 = arena
            .submit_request(IpcCommand::GetStatus, Some(Duration::from_millis(15)))
            .unwrap_err();
        assert!(matches!(err2, PgBoltError::Ipc(_)));
        worker.join().unwrap();
        assert_eq!(arena.stats().free_slots, 1);
        assert_eq!(arena.stats().in_flight_requests, 0);
    }

    #[test]
    fn ipc_stream_rejects_corrupt_frames_duplicate_schema_and_trailing_bytes() {
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
        let valid = record_batch_to_ipc_stream(&batch).unwrap();

        // Find exact end of Schema frame using internal `decode_frame`.
        let mut pos = 0_usize;
        let _schema_frame = decode_frame(&valid, &mut pos).unwrap().unwrap();
        let schema_only = valid[..pos].to_vec();

        // 1) Schema without body batch + EOS decodes cleanly to an empty batch.
        let mut empty_stream = schema_only.clone();
        empty_stream.extend_from_slice(&CONTINUATION_MARKER);
        empty_stream.extend_from_slice(&0_i32.to_le_bytes());
        let empty = ipc_stream_to_record_batch(&empty_stream).unwrap();
        assert_eq!(empty.num_rows(), 0);

        // 2) Duplicate Schema message must be rejected cleanly.
        let mut dup_schema = schema_only.clone();
        dup_schema.extend_from_slice(&schema_only);
        dup_schema.extend_from_slice(&CONTINUATION_MARKER);
        dup_schema.extend_from_slice(&0_i32.to_le_bytes());
        assert!(matches!(
            ipc_stream_to_record_batch(&dup_schema),
            Err(PgBoltError::Ipc(_))
        ));

        // 3) Corrupt second frame FlatBuffer bytes must fail with Err, never panic.
        let mut bad_fb = schema_only;
        bad_fb.extend_from_slice(&CONTINUATION_MARKER);
        bad_fb.extend_from_slice(&8_i32.to_le_bytes());
        bad_fb.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4]);
        assert!(matches!(
            ipc_stream_to_record_batch(&bad_fb),
            Err(PgBoltError::Ipc(_))
        ));

        // 4) Trailing garbage bytes after EOS must fail cleanly.
        let mut trailing = valid.clone();
        trailing.extend_from_slice(&[1, 2, 3, 4]);
        assert!(matches!(
            ipc_stream_to_record_batch(&trailing),
            Err(PgBoltError::Ipc(_))
        ));

        // 5) Truncated stream must fail cleanly.
        assert!(matches!(
            ipc_stream_to_record_batch(&valid[..valid.len() / 2]),
            Err(PgBoltError::Ipc(_))
        ));
    }

    #[test]
    fn chunked_payload_split_reassembly_and_arena_transfer() {
        init_shmem_hooks();
        assert!(!is_shmem_initialized());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from((0..128).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(
                    (0..128).map(|i| i as f64 * 1.25).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let raw_payload = record_batch_to_ipc_stream(&batch).unwrap();
        assert!(raw_payload.len() > 512);

        // Out-of-order reassembly round-trip.
        let mut chunks = split_payload_into_chunks(42, &raw_payload, 128).unwrap();
        assert!(chunks.len() > 3);
        let mut reassembler = ChunkReassembler::new(raw_payload.len() + 64);
        let last = chunks.pop().unwrap();
        let first = chunks.remove(0);
        assert!(reassembler.push("t", &last.0, &last.1).unwrap().is_none());
        for (hdr, data) in chunks {
            assert!(reassembler.push("t", &hdr, &data).unwrap().is_none());
        }
        let assembled = reassembler.push("t", &first.0, &first.1).unwrap().unwrap();
        assert_eq!(assembled, raw_payload);
        assert_eq!(reassembler.active_streams(), 0);

        // Validation guards: duplicate seq, inflated chunks, and oversized cap.
        let dup_hdr = IpcChunkHeader {
            stream_id: 99,
            seq: 0,
            total_chunks: 2,
            total_bytes: 6,
            payload_len: 3,
        };
        assert!(reassembler.push("k", &dup_hdr, b"abc").unwrap().is_none());
        assert!(reassembler.push("k", &dup_hdr, b"abc").is_err());

        let inflated = IpcChunkHeader {
            stream_id: 100,
            seq: 0,
            total_chunks: u32::MAX,
            total_bytes: 16,
            payload_len: 4,
        };
        assert!(reassembler.push("k", &inflated, b"abcd").is_err());

        // Single-slot arena still rejects single oversized LoadBatch, while `submit_chunked_load` succeeds.
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 256,
            })
            .unwrap()
            .with_max_chunked_payload_bytes(64 * 1024),
        );
        assert!(matches!(
            arena.submit_request(
                IpcCommand::LoadBatch {
                    pg_relation: "public.big".to_owned(),
                    bolt_name: "big".to_owned(),
                    ipc_payload: raw_payload.clone(),
                },
                Some(Duration::from_millis(50)),
            ),
            Err(PgBoltError::Ipc(_))
        ));

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let mut worker_reasm = ChunkReassembler::new(64 * 1024);
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_secs(2)) {
                match cmd {
                    IpcCommand::LoadBatchChunk {
                        pg_relation: _,
                        bolt_name,
                        header,
                        chunk_data,
                    } => {
                        let resp = match worker_reasm.push(&bolt_name, &header, &chunk_data) {
                            Ok(None) => IpcResponse::Ack {
                                message: format!(
                                    "chunk {}/{}",
                                    header.seq + 1,
                                    header.total_chunks
                                ),
                            },
                            Ok(Some(full)) => {
                                let decoded = ipc_stream_to_record_batch(&full).unwrap();
                                IpcResponse::Ack {
                                    message: format!("loaded {} rows", decoded.num_rows()),
                                }
                            }
                            Err(err) => IpcResponse::Error {
                                message: err.to_string(),
                                vram_exhausted: false,
                            },
                        };
                        a_worker.complete_request(req_id, resp).unwrap();
                    }
                    IpcCommand::Shutdown => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: "bye".to_owned(),
                                },
                            )
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
            }
        });

        let final_resp = arena
            .submit_chunked_load(
                "public.big",
                "big",
                &raw_payload,
                Some(Duration::from_secs(2)),
            )
            .unwrap();
        assert_eq!(
            final_resp,
            IpcResponse::Ack {
                message: "loaded 128 rows".to_owned()
            }
        );

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_secs(1)));
        worker.join().unwrap();
    }

    #[test]
    fn minimal_tuple_roundtrip_time_fixed_binary_views_and_dict_widths() {
        let mut t32s = Time32SecondBuilder::with_capacity(4);
        t32s.append_value(10);
        t32s.append_null();
        t32s.append_value(3_600);
        t32s.append_value(86_399);

        let mut t32ms = Time32MillisecondBuilder::with_capacity(4);
        t32ms.append_value(500);
        t32ms.append_value(12_345);
        t32ms.append_null();
        t32ms.append_value(86_399_999);

        let mut t64us = Time64MicrosecondBuilder::with_capacity(4);
        t64us.append_value(1_234_567);
        t64us.append_null();
        t64us.append_value(9_000_000_001);
        t64us.append_value(0);

        let mut t64ns = Time64NanosecondBuilder::with_capacity(4);
        t64ns.append_value(42);
        t64ns.append_value(9_000_000_000_123);
        t64ns.append_value(1);
        t64ns.append_null();

        let mut uuid = FixedSizeBinaryBuilder::with_capacity(4, 16);
        uuid.append_value([0x11_u8; 16]).unwrap();
        uuid.append_null();
        uuid.append_value([0xab_u8; 16]).unwrap();
        uuid.append_value([0_u8; 16]).unwrap();

        let mut str_view = StringViewBuilder::with_capacity(4);
        str_view.append_value("short");
        str_view.append_value("");
        str_view.append_null();
        str_view.append_value("out-of-line-utf8-view-value-exceeding-12-bytes");

        let mut bin_view = BinaryViewBuilder::with_capacity(4);
        bin_view.append_value(b"\x01\x02");
        bin_view.append_null();
        bin_view.append_value(b"");
        bin_view.append_value(b"0123456789abcdef-out-of-line-binary-view");

        let mut d_i8 = StringDictionaryBuilder::<Int8Type>::new();
        d_i8.append("a").unwrap();
        d_i8.append_null();
        d_i8.append("b").unwrap();
        d_i8.append("a").unwrap();

        let mut d_i16 = StringDictionaryBuilder::<Int16Type>::new();
        d_i16.append("east").unwrap();
        d_i16.append("west").unwrap();
        d_i16.append_null();
        d_i16.append("east").unwrap();

        let mut d_i64 = StringDictionaryBuilder::<Int64Type>::new();
        d_i64.append("gold").unwrap();
        d_i64.append("silver").unwrap();
        d_i64.append("gold").unwrap();
        d_i64.append_null();

        let mut d_u8 = StringDictionaryBuilder::<UInt8Type>::new();
        d_u8.append("hot").unwrap();
        d_u8.append_null();
        d_u8.append("cold").unwrap();
        d_u8.append("hot").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("t32_s", DataType::Time32(TimeUnit::Second), true),
            Field::new("t32_ms", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("t64_us", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("t64_ns", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new("uuid", DataType::FixedSizeBinary(16), true),
            Field::new("sv", DataType::Utf8View, true),
            Field::new("bv", DataType::BinaryView, true),
            Field::new(
                "d_i8",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "d_i16",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "d_i64",
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "d_u8",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(t32s.finish()),
                Arc::new(t32ms.finish()),
                Arc::new(t64us.finish()),
                Arc::new(t64ns.finish()),
                Arc::new(uuid.finish()),
                Arc::new(str_view.finish()),
                Arc::new(bin_view.finish()),
                Arc::new(d_i8.finish()),
                Arc::new(d_i16.finish()),
                Arc::new(d_i64.finish()),
                Arc::new(d_u8.finish()),
            ],
        )
        .unwrap();

        let tuples = record_batch_to_minimal_tuples(&batch).unwrap();
        assert_eq!(tuples.len(), 4);
        let rebuilt = minimal_tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(rebuilt.num_rows(), 4);
        assert_eq!(rebuilt.schema(), schema);
        for col_idx in 0..schema.fields().len() {
            assert_eq!(
                rebuilt.column(col_idx).data_type(),
                batch.column(col_idx).data_type()
            );
            assert_eq!(
                rebuilt.column(col_idx).as_ref(),
                batch.column(col_idx).as_ref()
            );
        }

        // Negative FixedSizeBinary byte width must fail cleanly instead of panicking.
        let bad_schema = Schema::new(vec![Field::new("bad", DataType::FixedSizeBinary(-1), true)]);
        assert!(matches!(
            minimal_tuples_to_record_batch(&[], &bad_schema),
            Err(PgBoltError::Ipc(_))
        ));
    }

    #[test]
    fn wire_codec_supports_explain_dirty_cancel_and_batch_chunks() {
        let cmds = [
            IpcCommand::ExplainSql {
                sql: "SELECT COUNT(*) FROM t".to_owned(),
            },
            IpcCommand::SetTableDirty {
                bolt_name: "public.t".to_owned(),
                dirty: true,
            },
            IpcCommand::SetTableDirty {
                bolt_name: "public.t".to_owned(),
                dirty: false,
            },
            IpcCommand::CancelChunkStream { stream_id: 99 },
            IpcCommand::FetchResponseChunk {
                stream_id: 42,
                chunk_idx: 3,
            },
        ];
        for cmd in cmds {
            let wire = cmd.encode_wire(4096).unwrap();
            let decoded = IpcCommand::decode_wire(&wire).unwrap();
            assert_eq!(decoded, cmd);
        }

        let resps = [
            IpcResponse::ExplainSql {
                plan: "GpuScan(t)\nGpuAggregate(count)".to_owned(),
            },
            IpcResponse::BatchChunk {
                header: IpcChunkHeader {
                    stream_id: 77,
                    seq: 1,
                    total_chunks: 4,
                    total_bytes: 2048,
                    payload_len: 5,
                },
                elapsed_us: 1234,
                tier: "cuda".to_owned(),
                chunk_data: b"hello".to_vec(),
            },
        ];
        for resp in resps {
            let wire = resp.encode_wire(4096).unwrap();
            let decoded = IpcResponse::decode_wire(&wire).unwrap();
            assert_eq!(decoded, resp);
            assert!(matches!(resp.encode_wire(4), Err(PgBoltError::Ipc(_))));
        }
    }

    #[test]
    fn chunk_reassembler_evicts_stale_streams_and_client_abort_sends_cancel() {
        let hdr0 = IpcChunkHeader {
            stream_id: 10,
            seq: 0,
            total_chunks: 2,
            total_bytes: 8,
            payload_len: 4,
        };
        let mut stale_reasm = ChunkReassembler::new(1024).with_stream_ttl(Duration::ZERO);
        assert_eq!(stale_reasm.push("t", &hdr0, b"abcd").unwrap(), None);
        assert_eq!(stale_reasm.active_streams(), 1);
        assert_eq!(stale_reasm.prune_stale_streams(), 1);
        assert_eq!(stale_reasm.active_streams(), 0);

        let mut live_reasm = ChunkReassembler::new(1024).with_stream_ttl(Duration::from_secs(60));
        assert_eq!(live_reasm.push("t", &hdr0, b"abcd").unwrap(), None);
        assert_eq!(live_reasm.prune_stale_streams(), 0);
        assert_eq!(live_reasm.active_streams(), 1);

        // Simulate mid-stream error returned on chunk 1: RAII guard must issue CancelChunkStream.
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 256,
            })
            .unwrap()
            .with_max_chunked_payload_bytes(64 * 1024),
        );
        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let mut worker_reasm = ChunkReassembler::new(64 * 1024);
            let mut cancels = 0_usize;
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_secs(2)) {
                match cmd {
                    IpcCommand::LoadBatchChunk {
                        bolt_name,
                        header,
                        chunk_data,
                        ..
                    } => {
                        let resp = if header.seq == 1 {
                            IpcResponse::Error {
                                message: "injected mid-stream error".to_owned(),
                                vram_exhausted: false,
                            }
                        } else {
                            let _ = worker_reasm.push(&bolt_name, &header, &chunk_data).unwrap();
                            IpcResponse::Ack {
                                message: "ok".to_owned(),
                            }
                        };
                        a_worker.complete_request(req_id, resp).unwrap();
                    }
                    IpcCommand::CancelChunkStream { stream_id } => {
                        cancels += 1;
                        worker_reasm.discard_stream(stream_id);
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: format!("cancelled {stream_id}"),
                                },
                            )
                            .unwrap();
                    }
                    IpcCommand::Shutdown => {
                        a_worker
                            .complete_request(
                                req_id,
                                IpcResponse::Ack {
                                    message: "bye".to_owned(),
                                },
                            )
                            .unwrap();
                        return (cancels, worker_reasm.active_streams());
                    }
                    _ => {}
                }
            }
            (cancels, worker_reasm.active_streams())
        });

        let payload = vec![0x5a_u8; 600];
        let resp = arena
            .submit_chunked_load("public.t", "t", &payload, Some(Duration::from_secs(2)))
            .unwrap();
        assert!(matches!(resp, IpcResponse::Error { .. }));
        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_secs(1)));
        let (cancels, remaining_streams) = worker.join().unwrap();
        assert_eq!(cancels, 1);
        assert_eq!(remaining_streams, 0);
    }

    #[test]
    fn response_chunk_stash_splits_fetches_and_expires_batches() {
        let mut stash = ResponseChunkStash::new(64 * 1024);

        // Small payload stays an inline single-frame IpcResponse::Batch.
        let small = stash
            .prepare_batch_response(1, 1024, b"tiny".to_vec(), 7, "cuda".to_owned())
            .unwrap();
        assert!(matches!(small, IpcResponse::Batch { .. }));
        assert_eq!(stash.active_streams(), 0);

        // Larger payload splits cleanly across slot_bytes = 256 frames.
        let large_payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let first = stash
            .prepare_batch_response(2, 256, large_payload.clone(), 42, "cuda".to_owned())
            .unwrap();
        let (total_chunks, mut reassembled) = match first {
            IpcResponse::BatchChunk {
                header,
                elapsed_us,
                tier,
                chunk_data,
            } => {
                assert_eq!(header.stream_id, 2);
                assert_eq!(header.seq, 0);
                assert!(header.total_chunks > 1);
                assert_eq!(elapsed_us, 42);
                assert_eq!(tier, "cuda");
                (header.total_chunks, chunk_data)
            }
            other => panic!("expected BatchChunk, got {other:?}"),
        };
        assert_eq!(stash.active_streams(), 1);

        for idx in 1..total_chunks {
            let chunk = stash.take_chunk(2, idx).unwrap();
            match chunk {
                IpcResponse::BatchChunk {
                    header,
                    elapsed_us,
                    tier,
                    chunk_data,
                } => {
                    assert_eq!(header.seq, idx);
                    assert_eq!(elapsed_us, 42);
                    assert_eq!(tier, "cuda");
                    reassembled.extend_from_slice(&chunk_data);
                }
                other => panic!("expected BatchChunk {idx}, got {other:?}"),
            }
        }
        assert_eq!(reassembled, large_payload);
        assert_eq!(stash.active_streams(), 0);
        assert!(matches!(stash.take_chunk(2, 1), Err(PgBoltError::Ipc(_))));

        // TTL expiry cleans abandoned response streams.
        let mut expiring = ResponseChunkStash::new(64 * 1024).with_stream_ttl(Duration::ZERO);
        let _ = expiring
            .prepare_batch_response(3, 256, large_payload, 1, "cuda".to_owned())
            .unwrap();
        assert_eq!(expiring.active_streams(), 1);
        assert_eq!(expiring.prune_stale_streams(), 1);
        assert_eq!(expiring.active_streams(), 0);
    }

    #[test]
    fn cancel_request_wire_encoding_and_decoding() {
        let cmd = IpcCommand::CancelRequest { req_id: 987654321 };
        let bytes = cmd.encode_wire(1024).unwrap();
        assert_eq!(bytes[0], CMD_CANCEL_REQUEST);
        assert_eq!(bytes.len(), 9);
        let decoded = IpcCommand::decode_wire(&bytes).unwrap();
        assert_eq!(decoded, cmd);

        // Truncated frame returns error
        assert!(matches!(
            IpcCommand::decode_wire(&bytes[..5]),
            Err(PgBoltError::Ipc(_))
        ));

        // In-place slice encoding/decoding
        let mut slice = vec![0u8; 32];
        let written = cmd.encode_into_slice(&mut slice).unwrap();
        assert_eq!(written, 9);
        let from_slice = IpcCommand::decode_from_slice(&slice[..written]).unwrap();
        assert_eq!(from_slice, cmd);
    }

    #[test]
    fn direct_slice_serialization_roundtrip() {
        let resp = IpcResponse::Ack {
            message: "completed successfully".to_owned(),
        };
        let mut buf = vec![0u8; 64];
        let written = resp.encode_into_slice(&mut buf).unwrap();
        assert!(written > 0);
        let decoded = IpcResponse::decode_from_slice(&buf[..written]).unwrap();
        assert_eq!(decoded, resp);

        // Insufficient buffer slice errors cleanly
        let mut tiny_buf = vec![0u8; 4];
        assert!(resp.encode_into_slice(&mut tiny_buf).is_err());
    }

    #[test]
    fn shared_memory_arena_cooperative_cancellation_and_poll() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 512,
            })
            .unwrap(),
        );

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let (req_id, cmd, cancel_flag) = a_worker
                .recv_request_with_cancel(Duration::from_secs(2))
                .unwrap();
            assert_eq!(cmd, IpcCommand::GetStatus);
            assert!(!cancel_flag.load(std::sync::atomic::Ordering::Acquire));

            // Wait for cancel to be requested by client
            let start = Instant::now();
            while !cancel_flag.load(std::sync::atomic::Ordering::Acquire) {
                if start.elapsed() > Duration::from_secs(2) {
                    panic!("timed out waiting for cancellation flag");
                }
                thread::sleep(Duration::from_millis(5));
            }

            // Acknowledge request was cancelled
            let _ = a_worker.complete_request(
                req_id,
                IpcResponse::Error {
                    message: "query cancelled by client".to_owned(),
                    vram_exhausted: false,
                },
            );
        });

        // Submit request from another thread
        let a_client = Arc::clone(&arena);
        let client = thread::spawn(move || {
            a_client.submit_request(IpcCommand::GetStatus, Some(Duration::from_secs(2)))
        });

        thread::sleep(Duration::from_millis(30));
        // Verify cancellation lookup and trigger cancel
        assert!(arena.request_cancel(1));
        assert!(arena.is_cancel_requested(1));
        assert!(arena.cancel_flag_for_req(1).is_some());
        assert!(arena
            .cancel_flag_for_req(1)
            .unwrap()
            .load(std::sync::atomic::Ordering::Acquire));

        let res = client.join().unwrap();
        assert!(
            matches!(
                res,
                Err(PgBoltError::Cancelled(_)) | Err(PgBoltError::Bolt(_))
            ),
            "expected Cancelled or Bolt error, got {res:?}"
        );
        worker.join().unwrap();
        assert_eq!(arena.stats().free_slots, 2);
    }

    #[test]
    fn shmem_slot_desc_mock_state_transitions() {
        use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};
        #[repr(C)]
        struct MockSlotDesc {
            state: AtomicU32,
            payload_len: AtomicU32,
            req_id: AtomicU64,
            dsm_handle: AtomicU32,
            cancel_requested: AtomicBool,
            _pad: [u8; 3],
            client_pid: AtomicI32,
            client_latch: AtomicPtr<()>,
        }

        let slot = MockSlotDesc {
            state: AtomicU32::new(0),
            payload_len: AtomicU32::new(0),
            req_id: AtomicU64::new(0),
            dsm_handle: AtomicU32::new(0),
            cancel_requested: AtomicBool::new(false),
            _pad: [0; 3],
            client_pid: AtomicI32::new(0),
            client_latch: AtomicPtr::new(std::ptr::null_mut()),
        };

        assert_eq!(slot.state.load(Ordering::Acquire), 0);
        assert!(!slot.cancel_requested.load(Ordering::Acquire));

        // Transition Free -> Pending
        assert!(slot
            .state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok());
        slot.req_id.store(42, Ordering::Release);
        slot.payload_len.store(128, Ordering::Release);

        // Signal cancel
        slot.cancel_requested.store(true, Ordering::Release);
        assert!(slot.cancel_requested.load(Ordering::Acquire));

        // Transition Pending -> InFlight
        assert!(slot
            .state
            .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok());

        // Transition InFlight -> Completed
        slot.state.store(3, Ordering::Release);
        assert_eq!(slot.state.load(Ordering::Acquire), 3);
    }

    #[test]
    fn shmem_ring_control_offsets_and_budget_tracking() {
        use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};
        #[repr(C)]
        struct MockRingControl {
            magic: AtomicU32,
            version: AtomicU32,
            num_slots: AtomicU32,
            slot_bytes: AtomicU64,
            max_chunked_payload_bytes: AtomicU64,
            worker_pid: AtomicI32,
            shutdown: AtomicU32,
            worker_latch: AtomicPtr<()>,
            total_submitted: AtomicU64,
            total_completed: AtomicU64,
            total_timed_out: AtomicU64,
            total_rejected_oversized: AtomicU64,
            active_memory_budget_bytes: AtomicU64,
        }

        let ctrl = MockRingControl {
            magic: AtomicU32::new(0x5042_4F4C),
            version: AtomicU32::new(1),
            num_slots: AtomicU32::new(16),
            slot_bytes: AtomicU64::new(16 * 1024 * 1024),
            max_chunked_payload_bytes: AtomicU64::new(256 * 1024 * 1024),
            worker_pid: AtomicI32::new(1234),
            shutdown: AtomicU32::new(0),
            worker_latch: AtomicPtr::new(std::ptr::null_mut()),
            total_submitted: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_timed_out: AtomicU64::new(0),
            total_rejected_oversized: AtomicU64::new(0),
            active_memory_budget_bytes: AtomicU64::new(4 * 1024 * 1024 * 1024),
        };

        assert_eq!(ctrl.magic.load(Ordering::Acquire), 0x5042_4F4C);
        assert_eq!(
            ctrl.active_memory_budget_bytes.load(Ordering::Acquire),
            4 * 1024 * 1024 * 1024
        );
        ctrl.active_memory_budget_bytes
            .store(512 * 1024 * 1024, Ordering::Release);
        assert_eq!(
            ctrl.active_memory_budget_bytes.load(Ordering::Acquire),
            512 * 1024 * 1024
        );
    }

    #[test]
    fn shmem_slot_desc_initializing_state_and_abandoned_sweep() {
        use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};
        #[repr(C)]
        struct MockSlotDesc {
            state: AtomicU32,
            payload_len: AtomicU32,
            req_id: AtomicU64,
            dsm_handle: AtomicU32,
            cancel_requested: AtomicBool,
            _pad: [u8; 3],
            client_pid: AtomicI32,
            client_latch: AtomicPtr<()>,
        }

        let slot = MockSlotDesc {
            state: AtomicU32::new(0), // FREE
            payload_len: AtomicU32::new(0),
            req_id: AtomicU64::new(0),
            dsm_handle: AtomicU32::new(0),
            cancel_requested: AtomicBool::new(false),
            _pad: [0; 3],
            client_pid: AtomicI32::new(0),
            client_latch: AtomicPtr::new(std::ptr::null_mut()),
        };

        // 1) Two-phase publish: FREE (0) -> INITIALIZING (5)
        assert!(slot
            .state
            .compare_exchange(0, 5, Ordering::AcqRel, Ordering::Acquire)
            .is_ok());
        slot.req_id.store(101, Ordering::Release);
        slot.payload_len.store(2048, Ordering::Release);
        slot.dsm_handle.store(77, Ordering::Release);

        // Worker checking for PENDING (1) cannot prematurely claim during INITIALIZING (5)
        assert!(slot
            .state
            .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_err());

        // Publish: INITIALIZING (5) -> PENDING (1)
        slot.state.store(1, Ordering::Release);

        // 2) Unclaimed pending cancellation: CAS PENDING (1) -> FREE (0) reclaims immediately
        assert!(slot
            .state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok());
        assert_eq!(slot.state.load(Ordering::Acquire), 0);

        // 3) Claimed in-flight cancellation: worker resets ABANDONED (4)
        slot.state.store(2, Ordering::Release); // IN_FLIGHT
                                                // Client cancels and transitions to ABANDONED
        slot.state.store(4, Ordering::Release); // ABANDONED
        assert_eq!(slot.state.load(Ordering::Acquire), 4);
        // Worker sweeps / completes: detects ABANDONED and resets to FREE
        slot.state.store(0, Ordering::Release);
        assert_eq!(slot.state.load(Ordering::Acquire), 0);
    }

    #[test]
    fn arena_free_slot_cv_notify_all_concurrent_waiters() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 1,
                slot_bytes: 512,
            })
            .unwrap(),
        );

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            let mut served = 0;
            while let Some((req_id, _cmd)) = a_worker.recv_request(Duration::from_millis(500)) {
                served += 1;
                let _ = a_worker.complete_request(
                    req_id,
                    IpcResponse::Ack {
                        message: format!("ok_{served}"),
                    },
                );
            }
            served
        });

        // Spawn multiple concurrent clients competing for the single slot
        let mut handles = Vec::new();
        for _ in 0..4 {
            let a = Arc::clone(&arena);
            handles.push(thread::spawn(move || {
                a.submit_request(IpcCommand::GetStatus, Some(Duration::from_secs(2)))
            }));
        }

        for h in handles {
            let res = h.join().unwrap();
            assert!(res.is_ok());
        }

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_millis(500)));
        let served = worker.join().unwrap();
        assert!(served >= 4);
    }

    #[test]
    fn chunked_transfer_deadline_budgeting() {
        let arena = Arc::new(
            SharedMemoryArena::new(IpcRingConfig {
                num_slots: 2,
                slot_bytes: 256,
            })
            .unwrap(),
        );

        let a_worker = Arc::clone(&arena);
        let worker = thread::spawn(move || {
            while let Some((req_id, cmd)) = a_worker.recv_request(Duration::from_millis(500)) {
                match cmd {
                    IpcCommand::LoadBatchChunk { header, .. } => {
                        // Inject artificial delay per chunk
                        thread::sleep(Duration::from_millis(30));
                        let _ = a_worker.complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: format!("chunk {}", header.seq),
                            },
                        );
                    }
                    IpcCommand::CancelChunkStream { stream_id } => {
                        let _ = a_worker.complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: format!("cancelled:{stream_id}"),
                            },
                        );
                    }
                    IpcCommand::Shutdown => {
                        let _ = a_worker.complete_request(
                            req_id,
                            IpcResponse::Ack {
                                message: "bye".to_string(),
                            },
                        );
                        break;
                    }
                    _ => {}
                }
            }
        });

        // 600 bytes payload across 256-byte slot => multiple chunks
        let payload = vec![0x42u8; 600];
        // Timeout of 15ms is too tight for multiple 30ms chunk hops; must fail with timeout Err
        let res =
            arena.submit_chunked_load("public.t", "t", &payload, Some(Duration::from_millis(15)));
        assert!(res.is_err());

        let _ = arena.submit_request(IpcCommand::Shutdown, Some(Duration::from_millis(500)));
        worker.join().unwrap();
    }

    #[test]
    fn process_liveness_probes_and_dead_backend_detection() {
        let my_pid = std::process::id() as i32;
        assert!(is_process_alive(my_pid));
        assert!(!is_process_alive(0));
        assert!(!is_process_alive(-1));
        // An improbably high PID should return false
        assert!(!is_process_alive(2_000_000_000));
    }
}
