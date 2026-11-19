// SPDX-License-Identifier: Apache-2.0

//! Portable PostgreSQL <-> Apache Arrow conversion bridge, `pgrx` SPI extractor,
//! direct 8KB PostgreSQL heap page binary reader (Post-MVP Milestone 3), and
//! typed JSON/JSONB result serialization.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    LargeStringBuilder, StringBuilder, StringDictionaryBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Decimal128Array, Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, RecordBatchOptions, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::i256;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::error::{PgBoltError, PgBoltResult};
use crate::guc::{current_config, NumericUnconstrainedFallback};
use crate::mvcc::{
    is_tuple_visible, HeapTupleHeaderMeta, MvccSnapshot, VisibilityMap, HEAP_HASNULL,
    HEAP_HASVARWIDTH,
};

// ---------------------------------------------------------------------------
// PostgreSQL built-in type OIDs (`catalog/pg_type_d.h`)
// ---------------------------------------------------------------------------

pub const BOOLOID: u32 = 16;
pub const BYTEAOID: u32 = 17;
pub const INT8OID: u32 = 20;
pub const INT2OID: u32 = 21;
pub const INT4OID: u32 = 23;
pub const TEXTOID: u32 = 25;
pub const FLOAT4OID: u32 = 700;
pub const FLOAT8OID: u32 = 701;
pub const BPCHAROID: u32 = 1042;
pub const VARCHAROID: u32 = 1043;
pub const DATEOID: u32 = 1082;
pub const TIMEOID: u32 = 1083;
pub const TIMESTAMPOID: u32 = 1114;
pub const TIMESTAMPTZOID: u32 = 1184;
pub const NUMERICOID: u32 = 1700;
pub const UUIDOID: u32 = 2950;

/// Maximum PostgreSQL `TIME` microseconds per day (`24:00:00.000000`).
pub const PG_TIME_MAX_USEC: i64 = 86_400_000_000;

/// Target Arrow `Decimal128` precision for unconstrained PostgreSQL `NUMERIC`.
pub const PG_NUMERIC_PRECISION: u8 = 38;
/// Target Arrow `Decimal128` fractional scale for unconstrained PostgreSQL `NUMERIC`.
pub const PG_NUMERIC_SCALE: i8 = 10;

// ---------------------------------------------------------------------------
// Direct PostgreSQL 8KB Heap Page Binary Reader Constants (Milestone 3)
// ---------------------------------------------------------------------------

/// PostgreSQL default heap block size (`BLCKSZ = 8192`).
pub const PG_BLCKSZ: usize = 8192;
/// Fixed size of PostgreSQL `PageHeaderData` (`sizeof(PageHeaderData)`).
pub const PG_PAGE_HEADER_SIZE: usize = 24;
/// `ItemIdData.lp_flags` value indicating an active normal heap tuple pointer.
pub const LP_NORMAL: u16 = 1;
/// `ItemIdData.lp_flags` value indicating an intra-page HOT redirect line pointer.
pub const LP_REDIRECT: u16 = 2;
/// Days between Unix epoch (`1970-01-01`) and PostgreSQL epoch (`2000-01-01`).
pub const PG_EPOCH_TO_UNIX_DAYS: i32 = 10_957;
/// Microseconds between Unix epoch (`1970-01-01`) and PostgreSQL epoch (`2000-01-01`).
pub const PG_EPOCH_TO_UNIX_USEC: i64 = 946_684_800_000_000;

const MIN_HEAP_TUPLE_HEADER_SIZE: usize = 23;
const PG_PAGE_LAYOUT_VERSION: u16 = 4;
/// `HeapTupleHeaderData.t_infomask2` flag marking a HOT heap-only tuple target.
pub const HEAP_ONLY_TUPLE: u16 = 0x8000;
const NUMERIC_SHORT_FLAG: u16 = 0x8000;
const NUMERIC_NEG_FLAG: u16 = 0x4000;
const NUMERIC_SPECIAL_MASK: u16 = 0xC000;
const NUMERIC_NAN_HEADER: u16 = 0xC000;
const NUMERIC_PINF_HEADER: u16 = 0xD000;
const NUMERIC_NINF_HEADER: u16 = 0xF000;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_NBASE: u16 = 10_000;
const MAX_TUPLE_ATTRS: usize = 0x07FF;
const MAX_VARLENA_RAW_BYTES: usize = 0x3FFF_FFFF;

// ---------------------------------------------------------------------------
// OID mapping & numeric / dictionary helpers
// ---------------------------------------------------------------------------

/// PostgreSQL column types natively supported by the `pg_bolt` Arrow bridge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SupportedPgType {
    Bool,
    Binary,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Text,
    Uuid,
}

#[inline]
fn checked_max_decimal128_magnitude(precision: u8) -> PgBoltResult<u128> {
    if precision == 0 || precision > PG_NUMERIC_PRECISION {
        return Err(PgBoltError::Bridge(format!(
            "Decimal128 precision {precision} outside valid range 1..={PG_NUMERIC_PRECISION}"
        )));
    }
    10_u128.checked_pow(u32::from(precision)).ok_or_else(|| {
        PgBoltError::Bridge(format!(
            "Decimal128 magnitude 10^{precision} overflows u128"
        ))
    })
}

/// Decodes a PostgreSQL `NUMERIC(precision, scale)` `atttypmod` into Arrow `(precision, scale)`.
///
/// Returns `None` for unconstrained (`typmod < 4`, e.g. `-1`), negative scales, invalid
/// typmods (`scale > precision`), or precisions exceeding Arrow `Decimal128` (`> 38`).
pub fn decode_numeric_typmod(typmod: i32) -> Option<(u8, i8)> {
    if typmod < 4 {
        return None;
    }
    let adjusted = (typmod - 4) as u32;
    let precision = ((adjusted >> 16) & 0xFFFF) as i32;
    let scale = ((adjusted & 0xFFFF) as u16) as i16 as i32;
    if (1..=i32::from(PG_NUMERIC_PRECISION)).contains(&precision)
        && (0..=precision).contains(&scale)
    {
        Some((precision as u8, scale as i8))
    } else {
        None
    }
}

/// Decodes raw PostgreSQL `NUMERIC(precision, scale)` `atttypmod` into `(precision, scale)`
/// without enforcing Arrow `Decimal128`'s 38-precision limit.
///
/// Returns `None` for unconstrained (`typmod < 4`, e.g. `-1`), negative scales, or invalid
/// typmods (`scale > precision`).
pub fn decode_raw_numeric_typmod(typmod: i32) -> Option<(u32, i32)> {
    if typmod < 4 {
        return None;
    }
    let adjusted = (typmod - 4) as u32;
    let precision = (adjusted >> 16) & 0xFFFF;
    let scale = ((adjusted & 0xFFFF) as u16) as i16 as i32;
    if precision >= 1 && scale >= 0 && scale <= precision as i32 {
        Some((precision, scale))
    } else {
        None
    }
}

/// Maps a PostgreSQL type OID to `(SupportedPgType, arrow_schema::DataType)`.
///
/// Notes:
/// - `INT2OID` (`smallint`) widens safely to `DataType::Int32` on ingest so
///   `craton-bolt` GPU `Int32` kernels run natively without `i16`/`i32` ABI mismatches.
/// - `NUMERICOID` without a `typmod` maps to `DataType::Decimal128(38, 10)` so base-10
///   precision is preserved without silent `f64` rounding (use `map_pg_column_to_arrow` /
///   `PgColumnSpec` for explicit `NUMERIC(p, s)` typmods).
/// - `TIMESTAMPTZOID` maps to `DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00"))`
///   so `arrow-cast` works identically with or without the optional `chrono-tz` crate.
/// - `BPCHAROID` (`char(n)`) preserves stored blank-padded spaces verbatim in Arrow `Utf8`,
///   matching PostgreSQL SPI, heap page, and `TupleTableSlot` storage semantics (use
///   [`trim_bpchar_trailing_spaces`] when unpadded text comparison is explicitly desired).
/// - `BYTEAOID` maps to `DataType::Binary`, `TIMEOID` maps to `DataType::Time64(Microsecond)`,
///   and `UUIDOID` maps to `DataType::FixedSizeBinary(16)`.
pub fn map_pg_oid_to_arrow(oid: u32) -> PgBoltResult<(SupportedPgType, DataType)> {
    match oid {
        BOOLOID => Ok((SupportedPgType::Bool, DataType::Boolean)),
        BYTEAOID => Ok((SupportedPgType::Binary, DataType::Binary)),
        INT2OID => Ok((SupportedPgType::Int2, DataType::Int32)),
        INT4OID => Ok((SupportedPgType::Int4, DataType::Int32)),
        INT8OID => Ok((SupportedPgType::Int8, DataType::Int64)),
        FLOAT4OID => Ok((SupportedPgType::Float4, DataType::Float32)),
        FLOAT8OID => Ok((SupportedPgType::Float8, DataType::Float64)),
        NUMERICOID => Ok((
            SupportedPgType::Numeric,
            DataType::Decimal128(PG_NUMERIC_PRECISION, PG_NUMERIC_SCALE),
        )),
        DATEOID => Ok((SupportedPgType::Date, DataType::Date32)),
        TIMEOID => Ok((
            SupportedPgType::Time,
            DataType::Time64(TimeUnit::Microsecond),
        )),
        TIMESTAMPOID => Ok((
            SupportedPgType::Timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )),
        TIMESTAMPTZOID => Ok((
            SupportedPgType::Timestamptz,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
        )),
        TEXTOID | VARCHAROID | BPCHAROID => Ok((SupportedPgType::Text, DataType::Utf8)),
        UUIDOID => Ok((SupportedPgType::Uuid, DataType::FixedSizeBinary(16))),
        _ => Err(PgBoltError::Bridge(format!(
            "unsupported PostgreSQL type OID {oid}"
        ))),
    }
}

/// Trims trailing ASCII space (`' '`) padding produced by PostgreSQL `CHAR(n)` (`BPCHAROID`).
#[inline]
pub fn trim_bpchar_trailing_spaces(s: &str) -> &str {
    s.trim_end_matches(' ')
}

/// Normalizes PostgreSQL `BPCHAR` string by trimming trailing spaces for comparison semantics.
#[inline]
pub fn normalize_bpchar_trailing_spaces(s: &str) -> &str {
    trim_bpchar_trailing_spaces(s)
}

/// Normalizes specified `BPCHAR` columns in `batch` by trimming trailing space padding,
/// ensuring Arrow string equality and join comparisons align with PostgreSQL blank-padded semantics.
pub fn normalize_bpchar_batch(
    batch: &RecordBatch,
    bpchar_col_indices: &[usize],
) -> PgBoltResult<RecordBatch> {
    if bpchar_col_indices.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let mut new_columns = batch.columns().to_vec();
    for &col_idx in bpchar_col_indices {
        if col_idx >= new_columns.len() {
            return Err(PgBoltError::Bridge(format!(
                "BPCHAR column index {col_idx} out of range (batch has {} columns)",
                new_columns.len()
            )));
        }
        let col = &new_columns[col_idx];
        if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
            let mut builder =
                StringBuilder::with_capacity(str_arr.len(), str_arr.value_data().len());
            for i in 0..str_arr.len() {
                if str_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let trimmed = trim_bpchar_trailing_spaces(str_arr.value(i));
                    builder.append_value(trimmed);
                }
            }
            new_columns[col_idx] = Arc::new(builder.finish());
        } else if let Some(lstr_arr) = col.as_any().downcast_ref::<LargeStringArray>() {
            let mut builder =
                LargeStringBuilder::with_capacity(lstr_arr.len(), lstr_arr.value_data().len());
            for i in 0..lstr_arr.len() {
                if lstr_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let trimmed = trim_bpchar_trailing_spaces(lstr_arr.value(i));
                    builder.append_value(trimmed);
                }
            }
            new_columns[col_idx] = Arc::new(builder.finish());
        }
    }
    RecordBatch::try_new(batch.schema(), new_columns).map_err(|e| {
        PgBoltError::Bridge(format!(
            "failed to create normalized BPCHAR RecordBatch: {e}"
        ))
    })
}

#[cfg(any(feature = "pgrx", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgIntDatum {
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Numeric(i128),
}

/// Converts any signed/unsigned Arrow integer cell into a range-checked `INT2`/`INT4`/`INT8`/`NUMERIC` value.
#[cfg(any(feature = "pgrx", test))]
fn convert_int_for_oid(
    col: &dyn Array,
    row_idx: usize,
    target_oid: u32,
) -> PgBoltResult<PgIntDatum> {
    let val_i128 = match col.data_type() {
        DataType::Int8 => i128::from(
            col.as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int8Array".into()))?
                .value(row_idx),
        ),
        DataType::Int16 => i128::from(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int16Array".into()))?
                .value(row_idx),
        ),
        DataType::Int32 => i128::from(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int32Array".into()))?
                .value(row_idx),
        ),
        DataType::Int64 => i128::from(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int64Array".into()))?
                .value(row_idx),
        ),
        DataType::UInt8 => i128::from(
            col.as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt8Array".into()))?
                .value(row_idx),
        ),
        DataType::UInt16 => i128::from(
            col.as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt16Array".into()))?
                .value(row_idx),
        ),
        DataType::UInt32 => i128::from(
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt32Array".into()))?
                .value(row_idx),
        ),
        DataType::UInt64 => i128::from(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt64Array".into()))?
                .value(row_idx),
        ),
        other => {
            return Err(PgBoltError::Bridge(format!(
                "unsupported Arrow integer type {other:?}"
            )));
        }
    };

    let src = col.data_type();
    match target_oid {
        INT2OID => i16::try_from(val_i128)
            .map(PgIntDatum::Int2)
            .map_err(|_| PgBoltError::Bridge(format!("{src} value {val_i128} overflows INT2"))),
        INT4OID => i32::try_from(val_i128)
            .map(PgIntDatum::Int4)
            .map_err(|_| PgBoltError::Bridge(format!("{src} value {val_i128} overflows INT4"))),
        INT8OID => i64::try_from(val_i128)
            .map(PgIntDatum::Int8)
            .map_err(|_| PgBoltError::Bridge(format!("{src} value {val_i128} overflows INT8"))),
        NUMERICOID => Ok(PgIntDatum::Numeric(val_i128)),
        _ => Err(PgBoltError::Bridge(format!(
            "{src} column incompatible with PostgreSQL OID {target_oid}"
        ))),
    }
}

#[cfg(any(feature = "pgrx", test))]
#[derive(Clone, Copy, Debug)]
enum PgFloatInput {
    F32(f32),
    F64(f64),
}

/// Formats a finite `f32`/`f64` for PostgreSQL `NUMERIC` parsing, normalizing `-0.0` to `"0"`
/// and rejecting `NaN` / `±Infinity`.
#[cfg(any(feature = "pgrx", test))]
fn format_finite_float_for_numeric(val: PgFloatInput) -> PgBoltResult<String> {
    match val {
        PgFloatInput::F32(v) if !v.is_finite() => Err(PgBoltError::Bridge(format!(
            "non-finite Float32 value {v} cannot be represented as PostgreSQL NUMERIC"
        ))),
        PgFloatInput::F64(v) if !v.is_finite() => Err(PgBoltError::Bridge(format!(
            "non-finite Float64 value {v} cannot be represented as PostgreSQL NUMERIC"
        ))),
        PgFloatInput::F32(v) => Ok(if v == 0.0 {
            "0".to_owned()
        } else {
            v.to_string()
        }),
        PgFloatInput::F64(v) => Ok(if v == 0.0 {
            "0".to_owned()
        } else {
            v.to_string()
        }),
    }
}

/// Parses a decimal string into `(is_negative, trimmed_digits, base10_exponent)` where
/// the exact mathematical value equals `(-1 if is_negative) * int(trimmed_digits) * 10^base10_exponent`.
fn parse_decimal_parts(s: &str) -> PgBoltResult<(bool, String, i32)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(PgBoltError::Bridge("empty NUMERIC string".into()));
    }
    if trimmed.eq_ignore_ascii_case("nan") {
        return Err(PgBoltError::Bridge(
            "PostgreSQL NUMERIC NaN cannot be represented as Arrow Decimal128".into(),
        ));
    }
    if trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
        || trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
    {
        return Err(PgBoltError::Bridge(
            "PostgreSQL NUMERIC Infinity cannot be represented as Arrow Decimal128".into(),
        ));
    }
    if trimmed.eq_ignore_ascii_case("-inf") || trimmed.eq_ignore_ascii_case("-infinity") {
        return Err(PgBoltError::Bridge(
            "PostgreSQL NUMERIC -Infinity cannot be represented as Arrow Decimal128".into(),
        ));
    }

    let (neg, unsigned) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if unsigned.is_empty() {
        return Err(PgBoltError::Bridge(format!(
            "invalid NUMERIC string '{trimmed}'"
        )));
    }

    let (mantissa, exp) = match unsigned.find(['e', 'E']) {
        Some(idx) => {
            let exp_str = &unsigned[idx + 1..];
            if exp_str.is_empty() {
                return Err(PgBoltError::Bridge(format!(
                    "missing exponent in NUMERIC string '{trimmed}'"
                )));
            }
            let parsed_exp: i32 = exp_str.parse().map_err(|_| {
                PgBoltError::Bridge(format!("invalid exponent in NUMERIC string '{trimmed}'"))
            })?;
            (&unsigned[..idx], parsed_exp)
        }
        None => (unsigned, 0),
    };

    let (int_part, frac_part) = match mantissa.find('.') {
        Some(dot_idx) => (&mantissa[..dot_idx], Some(&mantissa[dot_idx + 1..])),
        None => (mantissa, None),
    };

    if frac_part.is_some_and(|f| f.contains('.')) {
        return Err(PgBoltError::Bridge(format!(
            "multiple decimal points in NUMERIC string '{trimmed}'"
        )));
    }
    let frac_str = frac_part.unwrap_or("");
    if int_part.is_empty() && frac_str.is_empty() {
        return Err(PgBoltError::Bridge(format!(
            "no digits in NUMERIC string '{trimmed}'"
        )));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_str.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(PgBoltError::Bridge(format!(
            "invalid character in NUMERIC string '{trimmed}'"
        )));
    }

    let frac_len = i32::try_from(frac_str.len()).map_err(|_| {
        PgBoltError::Bridge(format!("NUMERIC fractional part too long in '{trimmed}'"))
    })?;
    let base10_exp = exp
        .checked_sub(frac_len)
        .ok_or_else(|| PgBoltError::Bridge(format!("NUMERIC exponent underflow in '{trimmed}'")))?;

    let mut digits = String::with_capacity(int_part.len() + frac_str.len());
    digits.push_str(int_part);
    digits.push_str(frac_str);
    let non_zero_start = digits
        .bytes()
        .position(|b| b != b'0')
        .unwrap_or(digits.len());
    let significant = digits[non_zero_start..].to_owned();

    Ok((neg, significant, base10_exp))
}

/// Parses a base-10 decimal string into a fixed-scale `i128` (`value * 10^scale`).
///
/// Rejects syntax errors, `NaN`/`Infinity`, `i128` overflow, and any non-zero digits
/// that fall beyond `scale` fractional places rather than truncating silently.
pub fn parse_decimal_str_to_i128(s: &str, scale: u8) -> PgBoltResult<i128> {
    let (neg, significant, base10_exp) = parse_decimal_parts(s)?;
    if significant.is_empty() {
        return Ok(0);
    }

    let net_exp = (scale as i32)
        .checked_add(base10_exp)
        .ok_or_else(|| PgBoltError::Bridge(format!("NUMERIC scale exponent overflow for '{s}'")))?;

    let kept_digits = if net_exp < 0 {
        let drop_count = net_exp.unsigned_abs() as usize;
        if drop_count >= significant.len() {
            // Since leading zeros were stripped, `significant` has at least one non-zero digit.
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' loses precision beyond scale {scale}"
            )));
        }
        let split_at = significant.len() - drop_count;
        if significant[split_at..].bytes().any(|b| b != b'0') {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' loses precision beyond scale {scale}"
            )));
        }
        &significant[..split_at]
    } else {
        significant.as_str()
    };

    let mut mag: u128 = 0;
    for b in kept_digits.bytes() {
        mag = mag
            .checked_mul(10)
            .and_then(|v| v.checked_add(u128::from(b - b'0')))
            .ok_or_else(|| {
                PgBoltError::Bridge(format!(
                    "NUMERIC value '{s}' overflows i128 at scale {scale}"
                ))
            })?;
    }

    if net_exp > 0 {
        // Any non-zero magnitude multiplied by 10^39 exceeds `u128::MAX` (~3.4e38).
        if net_exp > 38 {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' overflows i128 at scale {scale}"
            )));
        }
        for _ in 0..net_exp {
            mag = mag.checked_mul(10).ok_or_else(|| {
                PgBoltError::Bridge(format!(
                    "NUMERIC value '{s}' overflows i128 at scale {scale}"
                ))
            })?;
        }
    }

    if neg {
        let min_abs = (i128::MAX as u128) + 1;
        if mag > min_abs {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' underflows i128 at scale {scale}"
            )));
        }
        if mag == min_abs {
            Ok(i128::MIN)
        } else {
            Ok(-(mag as i128))
        }
    } else {
        i128::try_from(mag).map_err(|_| {
            PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' overflows i128 at scale {scale}"
            ))
        })
    }
}

/// Parses a base-10 decimal string into a fixed-scale `i256` (`value * 10^scale`).
///
/// Rejects syntax errors, `NaN`/`Infinity`, `i256` overflow, and any non-zero digits
/// that fall beyond `scale` fractional places rather than truncating silently.
pub fn parse_decimal_str_to_i256(s: &str, scale: u8) -> PgBoltResult<i256> {
    let (neg, significant, base10_exp) = parse_decimal_parts(s)?;
    if significant.is_empty() {
        return Ok(i256::ZERO);
    }

    let net_exp = (scale as i32)
        .checked_add(base10_exp)
        .ok_or_else(|| PgBoltError::Bridge(format!("NUMERIC scale exponent overflow for '{s}'")))?;

    let kept_digits = if net_exp < 0 {
        let drop_count = net_exp.unsigned_abs() as usize;
        if drop_count >= significant.len() {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' loses precision beyond scale {scale}"
            )));
        }
        let split_at = significant.len() - drop_count;
        if significant[split_at..].bytes().any(|b| b != b'0') {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' loses precision beyond scale {scale}"
            )));
        }
        &significant[..split_at]
    } else {
        significant.as_str()
    };

    let mut full_digits = String::with_capacity(kept_digits.len() + net_exp.max(0) as usize);
    full_digits.push_str(kept_digits);
    if net_exp > 0 {
        if net_exp > 76 {
            return Err(PgBoltError::Bridge(format!(
                "NUMERIC value '{s}' overflows i256 at scale {scale}"
            )));
        }
        for _ in 0..net_exp {
            full_digits.push('0');
        }
    }
    let sign_prefix = if neg { "-" } else { "" };
    let signed_str = format!("{sign_prefix}{full_digits}");
    i256::from_string(&signed_str).ok_or_else(|| {
        PgBoltError::Bridge(format!(
            "NUMERIC value '{s}' overflows Decimal256 at scale {scale}"
        ))
    })
}

/// Parses a base-10 or scientific numeric string into `f64`, safely translating PostgreSQL
/// `NaN`, `Infinity`, and `-Infinity` special values without error.
pub fn parse_numeric_str_to_f64(s: &str, col_name: &str) -> PgBoltResult<f64> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
        || trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
    {
        return Ok(f64::INFINITY);
    }
    if trimmed.eq_ignore_ascii_case("-inf") || trimmed.eq_ignore_ascii_case("-infinity") {
        return Ok(f64::NEG_INFINITY);
    }
    trimmed.parse::<f64>().map_err(|e| {
        PgBoltError::Bridge(format!(
            "failed to parse NUMERIC value '{s}' as Float64 in column '{col_name}': {e}"
        ))
    })
}

/// Inspects a slice of decimal string representations to infer the minimum required
/// `(precision, scale)` fitting within Arrow `Decimal128` (`precision <= 38`).
///
/// Returns `None` if any value is `NaN` / `Infinity` or requires precision > 38.
pub fn infer_numeric_precision_scale(values: &[&str]) -> PgBoltResult<Option<(u8, i8)>> {
    let mut max_int_digits: usize = 0;
    let mut max_frac_digits: usize = 0;

    for s in values {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("nan")
            || trimmed.eq_ignore_ascii_case("inf")
            || trimmed.eq_ignore_ascii_case("+inf")
            || trimmed.eq_ignore_ascii_case("infinity")
            || trimmed.eq_ignore_ascii_case("+infinity")
            || trimmed.eq_ignore_ascii_case("-inf")
            || trimmed.eq_ignore_ascii_case("-infinity")
        {
            return Ok(None);
        }
        let (_, significant, base10_exp) = parse_decimal_parts(trimmed)?;
        if significant.is_empty() {
            continue;
        }
        let (int_digits, frac_digits) = if base10_exp >= 0 {
            (significant.len() + base10_exp as usize, 0usize)
        } else {
            let neg_exp = base10_exp.unsigned_abs() as usize;
            if neg_exp < significant.len() {
                (significant.len() - neg_exp, neg_exp)
            } else {
                (0usize, neg_exp)
            }
        };
        max_int_digits = max_int_digits.max(int_digits);
        max_frac_digits = max_frac_digits.max(frac_digits);
    }

    let required_precision = max_int_digits.saturating_add(max_frac_digits).max(1);
    if required_precision <= 38 && max_frac_digits <= required_precision {
        Ok(Some((required_precision as u8, max_frac_digits as i8)))
    } else {
        Ok(None)
    }
}

/// Dictionary-compresses an Arrow `StringArray` into `DictionaryArray<Int32Type>` (`Dictionary<Int32, Utf8>`).
pub fn encode_dictionary_utf8(array: &StringArray) -> PgBoltResult<DictionaryArray<Int32Type>> {
    let value_cap = array.len().clamp(1, 256);
    let data_cap = array.len().saturating_mul(16).clamp(64, 65_536);
    let mut builder =
        StringDictionaryBuilder::<Int32Type>::with_capacity(array.len(), value_cap, data_cap);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            builder.append_null();
        } else {
            builder
                .append(array.value(idx))
                .map_err(|e| PgBoltError::Bridge(format!("dictionary encoding failed: {e}")))?;
        }
    }
    Ok(builder.finish())
}

// ---------------------------------------------------------------------------
// Portable row / datum representation & Arrow builders
// ---------------------------------------------------------------------------

/// Metadata describing one PostgreSQL column to map into an Arrow `Field`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgColumnMeta {
    pub name: String,
    pub type_oid: u32,
    pub nullable: bool,
}

/// Extended PostgreSQL column specification supporting `atttypmod` (`NUMERIC(p, s)`),
/// dropped columns (`attisdropped`), and PostgreSQL 11+ fast defaults (`attmissing`).
#[derive(Clone, Debug, PartialEq)]
pub struct PgColumnSpec {
    pub meta: PgColumnMeta,
    pub typmod: Option<i32>,
    pub attisdropped: bool,
    pub attmissing: Option<PgDatumValue>,
    /// Physical `pg_attribute.attlen` (`-1` varlena, `-2` cstring, `>0` fixed length), used when
    /// `attisdropped` cleared `meta.type_oid` (`InvalidOid = 0`).
    pub attlen: Option<i16>,
    /// Physical `pg_attribute.attalign` (`b'c'` = 1, `b's'` = 2, `b'i'` = 4, `b'd'` = 8).
    pub attalign: Option<u8>,
}

impl PgColumnSpec {
    /// Creates a visible, non-dropped `PgColumnSpec` without typmod or missing-value defaults.
    pub fn from_meta(meta: PgColumnMeta) -> Self {
        Self {
            meta,
            typmod: None,
            attisdropped: false,
            attmissing: None,
            attlen: None,
            attalign: None,
        }
    }

    /// Attaches a PostgreSQL `atttypmod` to this column specification.
    pub fn with_typmod(mut self, typmod: i32) -> Self {
        self.typmod = Some(typmod);
        self
    }

    /// Marks this column as `attisdropped = true` while preserving physical storage width/alignment.
    pub fn dropped(mut self, attlen: i16, attalign: u8) -> Self {
        self.attisdropped = true;
        self.attlen = Some(attlen);
        self.attalign = Some(attalign);
        self
    }

    /// Attaches a PostgreSQL 11+ `attmissingval` default used when `col_idx >= tuple.natts`.
    pub fn with_missing(mut self, default: PgDatumValue) -> Self {
        self.attmissing = Some(default);
        self
    }
}

impl From<PgColumnMeta> for PgColumnSpec {
    fn from(meta: PgColumnMeta) -> Self {
        Self::from_meta(meta)
    }
}

/// Maps a `PgColumnSpec` (honoring `typmod` for `NUMERICOID`) to `(SupportedPgType, DataType)`.
pub fn map_pg_column_to_arrow(spec: &PgColumnSpec) -> PgBoltResult<(SupportedPgType, DataType)> {
    map_pg_column_to_arrow_with_fallback(spec, current_config().numeric_unconstrained_fallback)
}

/// Maps a `PgColumnSpec` to `(SupportedPgType, DataType)` with an explicit unconstrained NUMERIC fallback strategy.
pub fn map_pg_column_to_arrow_with_fallback(
    spec: &PgColumnSpec,
    fallback: NumericUnconstrainedFallback,
) -> PgBoltResult<(SupportedPgType, DataType)> {
    let (pg_ty, base_dt) = map_pg_oid_to_arrow(spec.meta.type_oid)?;
    if pg_ty == SupportedPgType::Numeric {
        if let Some(typmod) = spec.typmod {
            if let Some((precision, scale)) = decode_numeric_typmod(typmod) {
                return Ok((pg_ty, DataType::Decimal128(precision, scale)));
            }
            if let Some((raw_p, raw_s)) = decode_raw_numeric_typmod(typmod) {
                // precision > 38
                return match fallback {
                    NumericUnconstrainedFallback::Decimal256 => {
                        if raw_p <= 76 && raw_s >= 0 && raw_s <= raw_p as i32 {
                            Ok((pg_ty, DataType::Decimal256(raw_p as u8, raw_s as i8)))
                        } else {
                            Ok((SupportedPgType::Float8, DataType::Float64))
                        }
                    }
                    NumericUnconstrainedFallback::Float64 => {
                        Ok((SupportedPgType::Float8, DataType::Float64))
                    }
                    NumericUnconstrainedFallback::Error => Err(PgBoltError::Bridge(format!(
                        "NUMERIC column '{}' precision {raw_p} exceeds Decimal128 max precision 38 (pg_bolt.numeric_unconstrained_fallback = 'error')",
                        spec.meta.name
                    ))),
                };
            }
        }
        // typmod is None or unconstrained (typmod < 4)
        return match fallback {
            NumericUnconstrainedFallback::Decimal256 => {
                Ok((pg_ty, DataType::Decimal256(76, PG_NUMERIC_SCALE)))
            }
            NumericUnconstrainedFallback::Float64 => Ok((
                pg_ty,
                DataType::Decimal128(PG_NUMERIC_PRECISION, PG_NUMERIC_SCALE),
            )),
            NumericUnconstrainedFallback::Error => {
                if spec.typmod == Some(-1) {
                    Err(PgBoltError::Bridge(format!(
                        "unconstrained NUMERIC column '{}' (atttypmod == -1) rejected (pg_bolt.numeric_unconstrained_fallback = 'error')",
                        spec.meta.name
                    )))
                } else {
                    Ok((pg_ty, base_dt))
                }
            }
        };
    }
    Ok((pg_ty, base_dt))
}

/// Portable datum representation shared between row builders, SPI, and 8KB page decoding.
///
/// Temporal variants store Unix-epoch values (`DateDays`: days since 1970-01-01;
/// `TimeMicros`: microseconds since 00:00:00; `TimestampMicros`: microseconds since
/// 1970-01-01T00:00:00Z).
#[derive(Clone, Debug, PartialEq)]
pub enum PgDatumValue {
    Null,
    Bool(bool),
    Bytes(Vec<u8>),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Numeric(String),
    DateDays(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    Text(String),
    Uuid([u8; 16]),
}

#[derive(Clone, Debug)]
struct ResolvedColumnPlan {
    pg_type: SupportedPgType,
    data_type: DataType,
}

enum PgColumnBuilder {
    Bool(BooleanBuilder),
    Binary(BinaryBuilder),
    Int2(Int32Builder),
    Int4(Int32Builder),
    Int8(Int64Builder),
    Float4(Float32Builder),
    Float8(Float64Builder),
    Numeric {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    Numeric256 {
        builder: Decimal256Builder,
        precision: u8,
        scale: i8,
    },
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Timestamptz(TimestampMicrosecondBuilder),
    Text(StringBuilder),
    Uuid(FixedSizeBinaryBuilder),
}

impl PgColumnBuilder {
    fn with_plan(plan: &ResolvedColumnPlan, capacity: usize) -> PgBoltResult<Self> {
        match &plan.data_type {
            DataType::Boolean => Ok(Self::Bool(BooleanBuilder::with_capacity(capacity))),
            DataType::Binary => Ok(Self::Binary(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            ))),
            DataType::Int32 => match plan.pg_type {
                SupportedPgType::Int2 => Ok(Self::Int2(Int32Builder::with_capacity(capacity))),
                _ => Ok(Self::Int4(Int32Builder::with_capacity(capacity))),
            },
            DataType::Int64 => Ok(Self::Int8(Int64Builder::with_capacity(capacity))),
            DataType::Float32 => Ok(Self::Float4(Float32Builder::with_capacity(capacity))),
            DataType::Float64 => Ok(Self::Float8(Float64Builder::with_capacity(capacity))),
            DataType::Decimal128(precision, scale) => {
                let p = *precision;
                let s = *scale;
                if !(1..=PG_NUMERIC_PRECISION).contains(&p) || s < 0 || s > p as i8 {
                    return Err(PgBoltError::Bridge(format!(
                        "invalid NUMERIC Decimal128({p}, {s}) specification"
                    )));
                }
                let builder = Decimal128Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(p, s));
                Ok(Self::Numeric {
                    builder,
                    precision: p,
                    scale: s,
                })
            }
            DataType::Decimal256(precision, scale) => {
                let p = *precision;
                let s = *scale;
                if !(1..=76).contains(&p) || s < 0 || s > p as i8 {
                    return Err(PgBoltError::Bridge(format!(
                        "invalid NUMERIC Decimal256({p}, {s}) specification"
                    )));
                }
                let builder = Decimal256Builder::with_capacity(capacity)
                    .with_data_type(DataType::Decimal256(p, s));
                Ok(Self::Numeric256 {
                    builder,
                    precision: p,
                    scale: s,
                })
            }
            DataType::Date32 => Ok(Self::Date(Date32Builder::with_capacity(capacity))),
            DataType::Time64(TimeUnit::Microsecond) => Ok(Self::Time(
                Time64MicrosecondBuilder::with_capacity(capacity),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Self::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, None)),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Ok(Self::Timestamptz(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone()))),
            )),
            DataType::Utf8 => Ok(Self::Text(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(16),
            ))),
            DataType::FixedSizeBinary(16) => Ok(Self::Uuid(FixedSizeBinaryBuilder::with_capacity(
                capacity, 16,
            ))),
            other => Err(PgBoltError::Bridge(format!(
                "unsupported column data type in plan: {other}"
            ))),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Int2(b) | Self::Int4(b) => b.append_null(),
            Self::Int8(b) => b.append_null(),
            Self::Float4(b) => b.append_null(),
            Self::Float8(b) => b.append_null(),
            Self::Numeric { builder, .. } => builder.append_null(),
            Self::Numeric256 { builder, .. } => builder.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Time(b) => b.append_null(),
            Self::Timestamp(b) | Self::Timestamptz(b) => b.append_null(),
            Self::Text(b) => b.append_null(),
            Self::Uuid(b) => b.append_null(),
        }
    }

    fn append_datum(
        &mut self,
        col_name: &str,
        nullable: bool,
        value: &PgDatumValue,
    ) -> PgBoltResult<()> {
        if matches!(value, PgDatumValue::Null) {
            if !nullable {
                return Err(PgBoltError::Bridge(format!(
                    "null value encountered in non-nullable column '{col_name}'"
                )));
            }
            self.append_null();
            return Ok(());
        }

        match (self, value) {
            (Self::Bool(b), PgDatumValue::Bool(v)) => b.append_value(*v),
            (Self::Binary(b), PgDatumValue::Bytes(bytes)) => b.append_value(bytes),
            (Self::Int2(b), PgDatumValue::Int16(v)) => b.append_value(i32::from(*v)),
            (Self::Int4(b), PgDatumValue::Int32(v)) => b.append_value(*v),
            (Self::Int8(b), PgDatumValue::Int64(v)) => b.append_value(*v),
            (Self::Float4(b), PgDatumValue::Float32(v)) => b.append_value(*v),
            (Self::Float8(b), PgDatumValue::Float64(v)) => b.append_value(*v),
            (Self::Float8(b), PgDatumValue::Numeric(s)) => {
                b.append_value(parse_numeric_str_to_f64(s, col_name)?);
            }
            (
                Self::Numeric {
                    builder,
                    precision,
                    scale,
                },
                PgDatumValue::Numeric(s),
            ) => {
                let scaled = parse_decimal_str_to_i128(s, *scale as u8)?;
                if scaled.unsigned_abs() >= checked_max_decimal128_magnitude(*precision)? {
                    return Err(PgBoltError::Bridge(format!(
                        "NUMERIC value '{s}' exceeds Decimal128({precision}, {scale}) precision in column '{col_name}'"
                    )));
                }
                builder.append_value(scaled);
            }
            (
                Self::Numeric256 {
                    builder,
                    precision,
                    scale,
                },
                PgDatumValue::Numeric(s),
            ) => {
                let scaled = parse_decimal_str_to_i256(s, *scale as u8)?;
                let (_, significant, base10_exp) = parse_decimal_parts(s)?;
                if !significant.is_empty() {
                    let net_exp = (*scale as i32).checked_add(base10_exp).unwrap_or(0);
                    let digit_count = if net_exp < 0 {
                        let drop = net_exp.unsigned_abs() as usize;
                        significant.len().saturating_sub(drop)
                    } else {
                        significant.len().saturating_add(net_exp as usize)
                    };
                    if digit_count > *precision as usize {
                        return Err(PgBoltError::Bridge(format!(
                            "NUMERIC value '{s}' exceeds Decimal256({precision}, {scale}) precision in column '{col_name}'"
                        )));
                    }
                }
                builder.append_value(scaled);
            }
            (Self::Date(b), PgDatumValue::DateDays(v)) => b.append_value(*v),
            (Self::Time(b), PgDatumValue::TimeMicros(us)) => {
                if !(0..=PG_TIME_MAX_USEC).contains(us) {
                    return Err(PgBoltError::Bridge(format!(
                        "TIME value {us} us out of range [0, {PG_TIME_MAX_USEC}] in column '{col_name}'"
                    )));
                }
                b.append_value(*us);
            }
            (Self::Timestamp(b) | Self::Timestamptz(b), PgDatumValue::TimestampMicros(v)) => {
                b.append_value(*v)
            }
            (Self::Text(b), PgDatumValue::Text(s)) => b.append_value(s),
            (Self::Uuid(b), PgDatumValue::Uuid(bytes)) => b.append_value(bytes).map_err(|e| {
                PgBoltError::Bridge(format!("failed to append UUID in column '{col_name}': {e}"))
            })?,
            (_, other) => {
                return Err(PgBoltError::Bridge(format!(
                    "datum variant {other:?} does not match target Arrow type for column '{col_name}'"
                )));
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::Int2(mut b) | Self::Int4(mut b) => Arc::new(b.finish()),
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Float4(mut b) => Arc::new(b.finish()),
            Self::Float8(mut b) => Arc::new(b.finish()),
            Self::Numeric { mut builder, .. } => Arc::new(builder.finish()),
            Self::Numeric256 { mut builder, .. } => Arc::new(builder.finish()),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
            Self::Timestamp(mut b) | Self::Timestamptz(mut b) => Arc::new(b.finish()),
            Self::Text(mut b) => Arc::new(b.finish()),
            Self::Uuid(mut b) => Arc::new(b.finish()),
        }
    }
}

fn build_arrow_schema_and_plans_from_specs(
    specs: &[PgColumnSpec],
) -> PgBoltResult<(SchemaRef, Vec<(ResolvedColumnPlan, &PgColumnSpec)>)> {
    let visible_count = specs.iter().filter(|s| !s.attisdropped).count();
    let mut fields = Vec::with_capacity(visible_count);
    let mut plans = Vec::with_capacity(visible_count);
    for spec in specs {
        if spec.attisdropped {
            continue;
        }
        let (pg_type, data_type) = map_pg_column_to_arrow(spec)?;
        fields.push(Field::new(
            &spec.meta.name,
            data_type.clone(),
            spec.meta.nullable,
        ));
        plans.push((ResolvedColumnPlan { pg_type, data_type }, spec));
    }
    Ok((Arc::new(Schema::new(fields)), plans))
}

fn finish_column_builders(
    schema: SchemaRef,
    builders: Vec<PgColumnBuilder>,
    row_count: usize,
) -> PgBoltResult<RecordBatch> {
    if row_count == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }
    if builders.is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|e| PgBoltError::Bridge(format!("failed to build 0-column RecordBatch: {e}")));
    }
    let arrays: Vec<ArrayRef> = builders.into_iter().map(PgColumnBuilder::finish).collect();
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| PgBoltError::Bridge(format!("failed to build RecordBatch: {e}")))
}

/// Converts row-oriented `PgDatumValue` rows into a typed Arrow `RecordBatch`.
///
/// Supports zero-row inputs (returning an empty `RecordBatch` with the mapped `Schema`)
/// as well as zero-column non-empty batches.
pub fn rows_to_record_batch(
    columns: &[PgColumnMeta],
    rows: &[Vec<PgDatumValue>],
) -> PgBoltResult<RecordBatch> {
    let specs: Vec<PgColumnSpec> = columns
        .iter()
        .cloned()
        .map(PgColumnSpec::from_meta)
        .collect();
    rows_to_record_batch_with_typmods(&specs, rows)
}

/// Converts row-oriented `PgDatumValue` rows into an Arrow `RecordBatch` using extended
/// `PgColumnSpec` definitions (including `NUMERIC(p, s)` typmods and skipping `attisdropped`
/// columns from the resulting Arrow schema).
pub fn rows_to_record_batch_with_typmods(
    columns: &[PgColumnSpec],
    rows: &[Vec<PgDatumValue>],
) -> PgBoltResult<RecordBatch> {
    let mut adjusted_specs = columns.to_vec();
    if !rows.is_empty() {
        for (col_idx, spec) in adjusted_specs.iter_mut().enumerate() {
            if spec.attisdropped {
                continue;
            }
            if spec.meta.type_oid == NUMERICOID
                && (spec.typmod.is_none() || spec.typmod == Some(-1))
            {
                let mut string_vals = Vec::new();
                let mut needs_dynamic = false;
                for row in rows {
                    if col_idx < row.len() {
                        if let PgDatumValue::Numeric(ref s) = row[col_idx] {
                            string_vals.push(s.as_str());
                            if parse_decimal_str_to_i128(s, 10).is_err() {
                                needs_dynamic = true;
                            }
                        }
                    }
                }
                if needs_dynamic {
                    match infer_numeric_precision_scale(&string_vals)? {
                        Some((p, s)) => {
                            let synth_typmod = ((i32::from(p) << 16) | (i32::from(s) & 0xFFFF)) + 4;
                            spec.typmod = Some(synth_typmod);
                        }
                        None => match current_config().numeric_unconstrained_fallback {
                            NumericUnconstrainedFallback::Float64 => {
                                spec.meta.type_oid = FLOAT8OID;
                            }
                            NumericUnconstrainedFallback::Decimal256 => {
                                let synth_typmod = ((76_i32 << 16) | (15_i32 & 0xFFFF)) + 4;
                                spec.typmod = Some(synth_typmod);
                            }
                            NumericUnconstrainedFallback::Error => {
                                return Err(PgBoltError::Bridge(format!(
                                        "unconstrained NUMERIC column '{}' cannot be mapped to Decimal128 (pg_bolt.numeric_unconstrained_fallback = 'error')",
                                        spec.meta.name
                                    )));
                            }
                        },
                    }
                }
            }
        }
    }

    let (schema, plans) = build_arrow_schema_and_plans_from_specs(&adjusted_specs)?;
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut builders = plans
        .iter()
        .map(|(plan, _)| PgColumnBuilder::with_plan(plan, rows.len()))
        .collect::<PgBoltResult<Vec<_>>>()?;

    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != plans.len() {
            return Err(PgBoltError::Bridge(format!(
                "row {row_idx} has {} column values, expected {}",
                row.len(),
                plans.len()
            )));
        }
        for (((_plan, spec), builder), value) in
            plans.iter().zip(builders.iter_mut()).zip(row.iter())
        {
            builder.append_datum(&spec.meta.name, spec.meta.nullable, value)?;
        }
    }

    finish_column_builders(schema, builders, rows.len())
}

// ---------------------------------------------------------------------------
// PostgreSQL epoch / temporal conversions
// ---------------------------------------------------------------------------

#[inline]
fn pg_date_to_unix_days(pg_days: i32) -> PgBoltResult<i32> {
    if pg_days == i32::MIN || pg_days == i32::MAX {
        return Err(PgBoltError::Bridge(
            "PostgreSQL DATE infinity/-infinity cannot be represented as Arrow Date32".into(),
        ));
    }
    pg_days.checked_add(PG_EPOCH_TO_UNIX_DAYS).ok_or_else(|| {
        PgBoltError::Bridge(format!("PostgreSQL DATE value {pg_days} overflows Date32"))
    })
}

#[inline]
fn pg_usec_to_unix_usec(pg_usec: i64) -> PgBoltResult<i64> {
    if pg_usec == i64::MIN || pg_usec == i64::MAX {
        return Err(PgBoltError::Bridge(
            "PostgreSQL TIMESTAMP infinity/-infinity cannot be represented as Arrow Timestamp"
                .into(),
        ));
    }
    pg_usec.checked_add(PG_EPOCH_TO_UNIX_USEC).ok_or_else(|| {
        PgBoltError::Bridge(format!(
            "PostgreSQL TIMESTAMP value {pg_usec} overflows Arrow Timestamp(Microsecond)"
        ))
    })
}

#[inline]
fn unix_days_to_pg_date(unix_days: i32) -> PgBoltResult<i32> {
    let pg_days = unix_days
        .checked_sub(PG_EPOCH_TO_UNIX_DAYS)
        .ok_or_else(|| {
            PgBoltError::Bridge(format!(
                "Unix date {unix_days} underflows PostgreSQL DateADT"
            ))
        })?;
    if pg_days == i32::MIN || pg_days == i32::MAX {
        return Err(PgBoltError::Bridge(
            "date value collides with PostgreSQL DATE infinity sentinel".into(),
        ));
    }
    Ok(pg_days)
}

#[inline]
fn unix_usec_to_pg_usec(unix_usec: i64) -> PgBoltResult<i64> {
    let pg_usec = unix_usec
        .checked_sub(PG_EPOCH_TO_UNIX_USEC)
        .ok_or_else(|| {
            PgBoltError::Bridge(format!(
                "Unix timestamp {unix_usec} underflows PostgreSQL Timestamp"
            ))
        })?;
    if pg_usec == i64::MIN || pg_usec == i64::MAX {
        return Err(PgBoltError::Bridge(
            "timestamp value collides with PostgreSQL TIMESTAMP infinity sentinel".into(),
        ));
    }
    Ok(pg_usec)
}

// ---------------------------------------------------------------------------
// pgrx SPI extraction bridge
// ---------------------------------------------------------------------------

#[cfg(feature = "pgrx")]
struct PgrxPfreeGuard(*mut std::ffi::c_void);

#[cfg(feature = "pgrx")]
impl PgrxPfreeGuard {
    fn if_distinct<T>(ptr: *mut T, original: *const T) -> Self {
        if !ptr.is_null() && !std::ptr::eq(ptr.cast_const(), original) {
            Self(ptr.cast())
        } else {
            Self(std::ptr::null_mut())
        }
    }
}

#[cfg(feature = "pgrx")]
impl Drop for PgrxPfreeGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: pointer was freshly allocated by PostgreSQL (`palloc` / detoast / encoding conversion).
            unsafe { pgrx::pg_sys::pfree(self.0) };
            self.0 = std::ptr::null_mut();
        }
    }
}

#[cfg(feature = "pgrx")]
struct SpiPortalGuard(pgrx::pg_sys::Portal);

#[cfg(feature = "pgrx")]
impl Drop for SpiPortalGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is an open SPI portal owned by this scope.
            unsafe { pgrx::pg_sys::SPI_cursor_close(self.0) };
            self.0 = std::ptr::null_mut();
        }
    }
}

#[cfg(feature = "pgrx")]
struct SpiTupTableGuard(*mut pgrx::pg_sys::SPITupleTable);

#[cfg(feature = "pgrx")]
impl Drop for SpiTupTableGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is owned by the just-finished `SPI_cursor_fetch`.
            unsafe { pgrx::pg_sys::SPI_freetuptable(self.0) };
            self.0 = std::ptr::null_mut();
        }
    }
}

#[cfg(feature = "pgrx")]
unsafe fn read_packed_varlena_ptr(packed: *const u8) -> PgBoltResult<Vec<u8>> {
    if packed.is_null() {
        return Err(PgBoltError::Bridge(
            "detoasted PostgreSQL varlena pointer is null".into(),
        ));
    }
    let first = *packed;
    let (payload_ptr, payload_len) = if (first & 0x01) != 0 {
        let total = usize::from(first >> 1);
        if total < 1 {
            return Err(PgBoltError::Bridge(
                "unexpected external TOAST header (1B_E) after pg_detoast_datum_packed".into(),
            ));
        }
        (packed.add(1), total - 1)
    } else {
        let hdr_word = std::ptr::read_unaligned(packed.cast::<u32>());
        if (hdr_word & 0x03) != 0 {
            return Err(PgBoltError::Bridge(format!(
                "unexpected compressed/non-4B_U varlena header flag 0x{:02x} after detoast",
                hdr_word & 0x03
            )));
        }
        let total = (hdr_word >> 2) as usize;
        if total < 4 {
            return Err(PgBoltError::Bridge(format!(
                "invalid detoasted 4-byte varlena length {total}"
            )));
        }
        (packed.add(4), total - 4)
    };
    Ok(std::slice::from_raw_parts(payload_ptr, payload_len).to_vec())
}

#[cfg(feature = "pgrx")]
unsafe fn decode_pg_varlena_datum(
    datum: pgrx::pg_sys::Datum,
    col_name: &str,
    type_label: &str,
) -> PgBoltResult<Vec<u8>> {
    use pgrx::pg_sys;

    let orig_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
    if orig_ptr.is_null() {
        return Err(PgBoltError::Bridge(format!(
            "null varlena pointer for non-null {type_label} column '{col_name}'"
        )));
    }
    let packed = pg_sys::pg_detoast_datum_packed(orig_ptr);
    let _detoast_guard = PgrxPfreeGuard::if_distinct(packed, orig_ptr.cast_const());
    read_packed_varlena_ptr(packed.cast::<u8>())
}

#[cfg(feature = "pgrx")]
unsafe fn decode_pg_text_datum(
    datum: pgrx::pg_sys::Datum,
    col_name: &str,
    db_encoding: std::ffi::c_int,
) -> PgBoltResult<String> {
    use pgrx::pg_sys;
    use std::ffi::{c_char, c_int, CStr};

    let bytes = decode_pg_varlena_datum(datum, col_name, "TEXT")?;
    let utf8_enc = pg_sys::pg_enc::PG_UTF8 as c_int;
    if db_encoding == utf8_enc {
        return match std::str::from_utf8(&bytes) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
        };
    }

    let len_i32 = match c_int::try_from(bytes.len()) {
        Ok(l) => l,
        Err(_) => return Ok(String::from_utf8_lossy(&bytes).into_owned()),
    };
    let converted = pg_sys::pg_server_to_any(bytes.as_ptr().cast::<c_char>(), len_i32, utf8_enc);
    if converted.is_null() {
        return Ok(String::from_utf8_lossy(&bytes).into_owned());
    }
    if std::ptr::eq(converted.cast::<u8>(), bytes.as_ptr()) {
        // `SQL_ASCII` or pure ASCII returns the input pointer unchanged; convert with lossy fallback on non-UTF-8.
        match std::str::from_utf8(&bytes) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
        }
    } else {
        let _conv_guard = PgrxPfreeGuard(converted.cast());
        let c_str = CStr::from_ptr(converted);
        match c_str.to_str() {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => Ok(String::from_utf8_lossy(c_str.to_bytes()).into_owned()),
        }
    }
}

/// Streams `relation_sql` via a bounded PostgreSQL `SPI` portal cursor (`chunk_rows` per fetch),
/// invoking `on_batch` for each decoded Arrow `RecordBatch` and freeing each chunk's `SPITupleTable`
/// before delivering the batch. Returns the total number of extracted rows.
///
/// When the query produces 0 rows, `on_batch` is invoked once with `RecordBatch::new_empty(schema)`
/// so callers still observe the resolved Arrow schema, and `Ok(0)` is returned.
///
/// Honors `atttypmod` for `NUMERIC(p, s)` columns, preserves PostgreSQL `BPCHAR` trailing spaces,
/// frees detoasted TOASTed `BYTEA`/`TEXT` copies immediately, and transcodes non-UTF-8 server
/// encodings (e.g. `LATIN1`, `WIN1252`) to UTF-8 via `pg_server_to_any(..., PG_UTF8)`.
#[cfg(feature = "pgrx")]
pub fn extract_relation_to_arrow_batches(
    relation_sql: &str,
    chunk_rows: usize,
    mut on_batch: impl FnMut(RecordBatch) -> PgBoltResult<()>,
) -> PgBoltResult<usize> {
    use std::ffi::{c_long, CStr, CString};

    use pgrx::datum::{AnyNumeric, Date, FromDatum, Time, Timestamp, TimestampWithTimeZone, Uuid};
    use pgrx::{pg_sys, Spi};

    if chunk_rows == 0 {
        return Err(PgBoltError::Bridge("SPI chunk_rows must be >= 1".into()));
    }
    let fetch_count = c_long::try_from(chunk_rows).unwrap_or(c_long::MAX);

    let c_sql = CString::new(relation_sql).map_err(|_| {
        PgBoltError::Bridge("SPI relation_sql contains forbidden interior NUL byte".into())
    })?;

    Spi::connect(|_client| -> PgBoltResult<usize> {
        // SAFETY: executing inside an active `Spi::connect` session with valid `c_sql`.
        let portal = unsafe {
            pg_sys::SPI_cursor_open_with_args(
                std::ptr::null(),
                c_sql.as_ptr(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null(),
                true,
                0,
            )
        };
        if portal.is_null() {
            return Err(PgBoltError::Bridge(
                "SPI_cursor_open_with_args failed to open portal".into(),
            ));
        }

        let portal_guard = SpiPortalGuard(portal);

        // SAFETY: portal opened for a SELECT query carries a valid `TupleDesc` (`tupDesc`).
        let tupdesc = unsafe { (*portal_guard.0).tupDesc };
        if tupdesc.is_null() {
            return Err(PgBoltError::Bridge(
                "SPI cursor query did not produce a tuple descriptor".into(),
            ));
        }
        let natts = unsafe { (*tupdesc).natts };
        if natts < 0 {
            return Err(PgBoltError::Bridge(format!(
                "invalid SPI TupleDesc column count {natts}"
            )));
        }
        let ncols = natts as usize;
        let attrs = unsafe { (*tupdesc).attrs.as_slice(ncols) };

        let mut col_specs = Vec::with_capacity(ncols);
        let mut attnums = Vec::with_capacity(ncols);
        for (idx, attr) in attrs.iter().enumerate() {
            if attr.attisdropped {
                continue;
            }
            let fnumber = i32::try_from(idx + 1)
                .map_err(|_| PgBoltError::Bridge("SPI column index overflows i32".into()))?;
            let ordinal = attnums.len() + 1;
            let name = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
                .to_str()
                .ok()
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("col_{ordinal}"));
            let meta = PgColumnMeta {
                name,
                type_oid: attr.atttypid.to_u32(),
                nullable: true,
            };
            col_specs.push(PgColumnSpec::from_meta(meta).with_typmod(attr.atttypmod));
            attnums.push(fnumber);
        }

        let (schema, plans) = build_arrow_schema_and_plans_from_specs(&col_specs)?;
        let db_encoding = unsafe { pg_sys::GetDatabaseEncoding() };
        let mut total_rows = 0usize;

        loop {
            let maybe_batch = {
                // SAFETY: active portal inside `Spi::connect`; fetch up to `fetch_count` rows.
                unsafe { pg_sys::SPI_cursor_fetch(portal_guard.0, true, fetch_count) };
                let tuptable = unsafe { pg_sys::SPI_tuptable };
                let processed = unsafe { pg_sys::SPI_processed };
                let _tt_guard = SpiTupTableGuard(tuptable);

                if processed == 0 || tuptable.is_null() {
                    None
                } else {
                    let batch_rows = usize::try_from(processed).map_err(|_| {
                        PgBoltError::Bridge("SPI_processed chunk row count overflows usize".into())
                    })?;
                    let vals_ptr = unsafe { (*tuptable).vals };
                    if vals_ptr.is_null() {
                        return Err(PgBoltError::Bridge(
                            "SPI tuple table contained null vals pointer".into(),
                        ));
                    }
                    let chunk_tupdesc = unsafe { (*tuptable).tupdesc };
                    let row_tupdesc = if chunk_tupdesc.is_null() {
                        tupdesc
                    } else {
                        chunk_tupdesc
                    };
                    let vals = unsafe { std::slice::from_raw_parts(vals_ptr, batch_rows) };
                    let builder_cap = batch_rows.min(8192);
                    let mut builders = plans
                        .iter()
                        .map(|&(plan, _)| PgColumnBuilder::with_plan(plan, builder_cap))
                        .collect::<PgBoltResult<Vec<_>>>()?;

                    for &heap_tup in vals {
                        if heap_tup.is_null() {
                            return Err(PgBoltError::Bridge(
                                "SPI tuple table contained null HeapTuple".into(),
                            ));
                        }

                        for ((&(plan, spec), &fnumber), builder) in
                            plans.iter().zip(attnums.iter()).zip(builders.iter_mut())
                        {
                            let mut isnull = false;
                            // SAFETY: `heap_tup` and `row_tupdesc` belong to the active chunk tuptable.
                            let datum = unsafe {
                                pg_sys::SPI_getbinval(heap_tup, row_tupdesc, fnumber, &mut isnull)
                            };

                            if isnull {
                                builder.append_datum(&spec.meta.name, true, &PgDatumValue::Null)?;
                                continue;
                            }

                            let col_name = &spec.meta.name;
                            let value = match plan.pg_type {
                                SupportedPgType::Bool => unsafe { bool::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Bool),
                                SupportedPgType::Binary => PgDatumValue::Bytes(unsafe {
                                    decode_pg_varlena_datum(datum, col_name, "BYTEA")
                                }?),
                                SupportedPgType::Int2 => unsafe { i16::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Int16),
                                SupportedPgType::Int4 => unsafe { i32::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Int32),
                                SupportedPgType::Int8 => unsafe { i64::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Int64),
                                SupportedPgType::Float4 => unsafe { f32::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Float32),
                                SupportedPgType::Float8 => unsafe { f64::from_datum(datum, false) }
                                    .map_or(PgDatumValue::Null, PgDatumValue::Float64),
                                SupportedPgType::Numeric => {
                                    unsafe { AnyNumeric::from_datum(datum, false) }
                                        .map_or(PgDatumValue::Null, |n| {
                                            PgDatumValue::Numeric(n.to_string())
                                        })
                                }
                                SupportedPgType::Date => {
                                    match unsafe { Date::from_datum(datum, false) } {
                                        None => PgDatumValue::Null,
                                        Some(d) => PgDatumValue::DateDays(pg_date_to_unix_days(
                                            d.into_inner(),
                                        )?),
                                    }
                                }
                                SupportedPgType::Time => {
                                    match unsafe { Time::from_datum(datum, false) } {
                                        None => PgDatumValue::Null,
                                        Some(t) => PgDatumValue::TimeMicros(t.into_inner()),
                                    }
                                }
                                SupportedPgType::Timestamp => {
                                    match unsafe { Timestamp::from_datum(datum, false) } {
                                        None => PgDatumValue::Null,
                                        Some(ts) => PgDatumValue::TimestampMicros(
                                            pg_usec_to_unix_usec(ts.into_inner())?,
                                        ),
                                    }
                                }
                                SupportedPgType::Timestamptz => {
                                    match unsafe { TimestampWithTimeZone::from_datum(datum, false) }
                                    {
                                        None => PgDatumValue::Null,
                                        Some(tstz) => PgDatumValue::TimestampMicros(
                                            pg_usec_to_unix_usec(tstz.into_inner())?,
                                        ),
                                    }
                                }
                                SupportedPgType::Text => PgDatumValue::Text(unsafe {
                                    decode_pg_text_datum(datum, col_name, db_encoding)
                                }?),
                                SupportedPgType::Uuid => {
                                    match unsafe { Uuid::from_datum(datum, false) } {
                                        None => PgDatumValue::Null,
                                        Some(u) => PgDatumValue::Uuid(*u.as_bytes()),
                                    }
                                }
                            };
                            builder.append_datum(col_name, true, &value)?;
                        }
                    }

                    Some(finish_column_builders(
                        schema.clone(),
                        builders,
                        batch_rows,
                    )?)
                }
            };

            let Some(batch) = maybe_batch else {
                break;
            };
            total_rows = total_rows
                .checked_add(batch.num_rows())
                .ok_or_else(|| PgBoltError::Bridge("SPI total row count overflows usize".into()))?;
            on_batch(batch)?;
        }

        if total_rows == 0 {
            on_batch(RecordBatch::new_empty(schema))?;
        }
        Ok(total_rows)
    })
}

/// Executes `relation_sql` via a chunked PostgreSQL `SPI` portal cursor (`8192` rows/batch)
/// and converts the result into an Arrow `RecordBatch`.
///
/// Honors `atttypmod` for `NUMERIC(p, s)` columns and safely transcodes non-UTF-8 server
/// encodings (e.g. `LATIN1`, `WIN1252`) to UTF-8 via `pg_server_to_any(..., PG_UTF8)`.
#[cfg(feature = "pgrx")]
pub fn extract_relation_to_arrow(relation_sql: &str) -> PgBoltResult<RecordBatch> {
    const SPI_FETCH_CHUNK_ROWS: usize = 8192;

    let mut batches = Vec::new();
    extract_relation_to_arrow_batches(relation_sql, SPI_FETCH_CHUNK_ROWS, |batch| {
        batches.push(batch);
        Ok(())
    })?;

    match batches.len() {
        0 => Err(PgBoltError::Bridge(
            "SPI relation extractor produced no schema batch".into(),
        )),
        1 => batches
            .pop()
            .ok_or_else(|| PgBoltError::Bridge("missing SPI RecordBatch".into())),
        _ => {
            let schema = batches[0].schema();
            arrow_select::concat::concat_batches(&schema, &batches).map_err(|e| {
                PgBoltError::Bridge(format!(
                    "failed to concatenate streamed SPI Arrow batches: {e}"
                ))
            })
        }
    }
}

/// Streams `relation_sql` via bounded PostgreSQL `SPI` cursor chunks of size `chunk_rows`.
///
/// Invokes `on_batch` for each decoded Arrow `RecordBatch` chunk, freeing each chunk's `SPITupleTable`
/// immediately to prevent unbounded memory growth on multi-GB relations.
#[cfg(feature = "pgrx")]
pub fn extract_relation_to_arrow_stream(
    relation_sql: &str,
    chunk_rows: usize,
    on_batch: impl FnMut(RecordBatch) -> PgBoltResult<()>,
) -> PgBoltResult<usize> {
    extract_relation_to_arrow_batches(relation_sql, chunk_rows, on_batch)
}

// ---------------------------------------------------------------------------
// Post-MVP Milestone 3: Direct PostgreSQL 8KB Heap Page Binary Reader
// ---------------------------------------------------------------------------

/// Decoded heap tuple from a raw PostgreSQL 8KB page.
#[derive(Clone, Debug)]
pub struct DecodedPageTuple {
    pub header: HeapTupleHeaderMeta,
    pub values: Vec<PgDatumValue>,
}

#[inline]
const fn align_up(offset: usize, align: usize) -> usize {
    (offset + align - 1) & !(align - 1)
}

#[inline]
fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn read_i16_le(buf: &[u8], off: usize) -> i16 {
    i16::from_le_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn read_i32_le(buf: &[u8], off: usize) -> i32 {
    i32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

#[inline]
fn read_f32_le(buf: &[u8], off: usize) -> f32 {
    f32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn read_f64_le(buf: &[u8], off: usize) -> f64 {
    f64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

/// Optional detoaster hook for resolving PostgreSQL external TOAST pointers (`1B_E`) or
/// non-PGLZ inline compressed varlenas (`4B_C` LZ4) when decoding raw heap pages.
pub trait ToastDetoaster {
    /// Given the raw on-disk varlena bytes starting at its `1B_E` or `4B_C` header,
    /// returns the uncompressed payload bytes (`VARDATA` without varlena header).
    fn detoast(&self, raw_varlena: &[u8]) -> PgBoltResult<Vec<u8>>;
}

/// PostgreSQL backend `ToastDetoaster` delegating to `pg_detoast_datum_packed`.
#[cfg(feature = "pgrx")]
pub struct PgBackendToastDetoaster {
    _private: (),
}

#[cfg(feature = "pgrx")]
impl PgBackendToastDetoaster {
    /// Constructs a `ToastDetoaster` backed by the live PostgreSQL backend.
    ///
    /// # Safety
    /// Caller must invoke `detoast` only inside an active PostgreSQL backend transaction
    /// with a valid `CurrentMemoryContext`, and any decoded `1B_E` TOAST pointers must
    /// reference valid TOAST relations in the connected database.
    pub unsafe fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(feature = "pgrx")]
impl ToastDetoaster for PgBackendToastDetoaster {
    fn detoast(&self, raw_varlena: &[u8]) -> PgBoltResult<Vec<u8>> {
        use pgrx::pg_sys;
        if raw_varlena.is_empty() {
            return Err(PgBoltError::Bridge(
                "empty varlena slice passed to PgBackendToastDetoaster".into(),
            ));
        }
        // SAFETY: caller upheld live PostgreSQL backend context invariant via `Self::new()`.
        unsafe {
            if pg_sys::CurrentMemoryContext.is_null() {
                return Err(PgBoltError::Bridge(
                    "PgBackendToastDetoaster requires an active PostgreSQL MemoryContext".into(),
                ));
            }
            let buf = pg_sys::palloc(raw_varlena.len()).cast::<u8>();
            if buf.is_null() {
                return Err(PgBoltError::Bridge(
                    "PostgreSQL palloc returned null in PgBackendToastDetoaster".into(),
                ));
            }
            let _buf_guard = PgrxPfreeGuard(buf.cast());
            std::ptr::copy_nonoverlapping(raw_varlena.as_ptr(), buf, raw_varlena.len());
            let packed = pg_sys::pg_detoast_datum_packed(buf.cast::<pg_sys::varlena>());
            let _packed_guard = PgrxPfreeGuard::if_distinct(packed.cast::<u8>(), buf.cast_const());
            read_packed_varlena_ptr(packed.cast::<u8>())
        }
    }
}

fn init_decompress_buf(
    compressed_len: usize,
    expected_raw_len: usize,
    codec: &str,
) -> PgBoltResult<Option<Vec<u8>>> {
    if expected_raw_len > MAX_VARLENA_RAW_BYTES {
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena ({codec}): raw length {expected_raw_len} exceeds PostgreSQL 1 GiB limit"
        )));
    }
    if expected_raw_len == 0 {
        if compressed_len == 0 {
            return Ok(None);
        }
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena ({codec}): non-empty compressed stream for 0 raw bytes"
        )));
    }
    // Cap initial reserve to avoid pre-allocating gigabytes on corrupt headers.
    let init_cap = compressed_len
        .saturating_mul(8)
        .clamp(64, 64 * 1024)
        .min(expected_raw_len);
    Ok(Some(Vec::with_capacity(init_cap)))
}

#[inline]
fn copy_match_checked(
    out: &mut Vec<u8>,
    match_off: usize,
    match_len: usize,
    expected_raw_len: usize,
    codec: &str,
) -> PgBoltResult<()> {
    if match_off == 0 || match_off > out.len() {
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena ({codec}): invalid backreference offset {match_off} at output offset {}",
            out.len()
        )));
    }
    if out
        .len()
        .checked_add(match_len)
        .is_none_or(|n| n > expected_raw_len)
    {
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena ({codec}): decompressed backreference exceeds expected raw size"
        )));
    }
    let copy_start = out.len() - match_off;
    // Copy byte-by-byte to support overlapping run-length matches (`match_len > match_off`).
    for i in 0..match_len {
        let b = out[copy_start + i];
        out.push(b);
    }
    Ok(())
}

/// Decompresses a PostgreSQL `pglz` payload (`4B_C` method `0`, `src/common/pg_lzcompress.c`)
/// into `expected_raw_len` bytes with strict bounds checking and zero panics on corrupt inputs.
pub fn pglz_decompress(compressed: &[u8], expected_raw_len: usize) -> PgBoltResult<Vec<u8>> {
    let Some(mut out) = init_decompress_buf(compressed.len(), expected_raw_len, "4B_C PGLZ")?
    else {
        return Ok(Vec::new());
    };

    let mut sp = 0usize;
    while sp < compressed.len() {
        let mut ctrl = compressed[sp];
        sp += 1;
        for _ in 0..8 {
            if sp >= compressed.len() {
                break;
            }
            if (ctrl & 1) != 0 {
                if sp + 2 > compressed.len() {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C PGLZ): truncated backreference tag".into(),
                    ));
                }
                let b0 = compressed[sp];
                let b1 = compressed[sp + 1];
                sp += 2;
                let mut match_len = usize::from(b0 & 0x0F) + 3;
                let match_off = (usize::from(b0 & 0xF0) << 4) | usize::from(b1);
                if match_len == 18 {
                    if sp >= compressed.len() {
                        return Err(PgBoltError::Bridge(
                            "corrupt compressed inline varlena (4B_C PGLZ): truncated extended match length byte".into(),
                        ));
                    }
                    match_len += usize::from(compressed[sp]);
                    sp += 1;
                }
                copy_match_checked(
                    &mut out,
                    match_off,
                    match_len,
                    expected_raw_len,
                    "4B_C PGLZ",
                )?;
            } else {
                if out.len() >= expected_raw_len {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C PGLZ): literal byte exceeds expected raw size".into(),
                    ));
                }
                out.push(compressed[sp]);
                sp += 1;
            }
            ctrl >>= 1;
        }
    }

    if out.len() != expected_raw_len {
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena (4B_C PGLZ): decompressed {} bytes, expected {expected_raw_len}",
            out.len()
        )));
    }
    Ok(out)
}

/// Decompresses a PostgreSQL raw-block LZ4 payload (`4B_C` method `1`, `LZ4_decompress_safe`)
/// into `expected_raw_len` bytes with strict bounds checking and zero panics on corrupt inputs.
pub fn lz4_decompress(compressed: &[u8], expected_raw_len: usize) -> PgBoltResult<Vec<u8>> {
    let Some(mut out) = init_decompress_buf(compressed.len(), expected_raw_len, "4B_C LZ4")? else {
        return Ok(Vec::new());
    };

    let mut sp = 0usize;
    while sp < compressed.len() {
        let token = compressed[sp];
        sp += 1;

        let mut literal_len = usize::from(token >> 4);
        if literal_len == 15 {
            loop {
                if sp >= compressed.len() {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): truncated extended literal length".into(),
                    ));
                }
                let ext = compressed[sp];
                sp += 1;
                literal_len = literal_len.checked_add(usize::from(ext)).ok_or_else(|| {
                    PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): literal length overflow"
                            .into(),
                    )
                })?;
                if literal_len > compressed.len() - sp {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): literal run exceeds compressed stream".into(),
                    ));
                }
                if ext != 255 {
                    break;
                }
            }
        }
        if literal_len > compressed.len() - sp {
            return Err(PgBoltError::Bridge(
                "corrupt compressed inline varlena (4B_C LZ4): truncated literal run".into(),
            ));
        }
        if out
            .len()
            .checked_add(literal_len)
            .is_none_or(|n| n > expected_raw_len)
        {
            return Err(PgBoltError::Bridge(
                "corrupt compressed inline varlena (4B_C LZ4): literal run exceeds expected raw size".into(),
            ));
        }
        out.extend_from_slice(&compressed[sp..sp + literal_len]);
        sp += literal_len;

        // Final sequence in an LZ4 raw block ends immediately after literals (no offset/match).
        if sp == compressed.len() {
            break;
        }
        if sp + 2 > compressed.len() {
            return Err(PgBoltError::Bridge(
                "corrupt compressed inline varlena (4B_C LZ4): truncated match offset".into(),
            ));
        }
        let match_off = usize::from(u16::from_le_bytes([compressed[sp], compressed[sp + 1]]));
        sp += 2;
        if match_off == 0 || match_off > out.len() {
            return Err(PgBoltError::Bridge(format!(
                "corrupt compressed inline varlena (4B_C LZ4): invalid backreference offset {match_off} at output offset {}",
                out.len()
            )));
        }

        let mut match_len = usize::from(token & 0x0F) + 4;
        if (token & 0x0F) == 15 {
            loop {
                if sp >= compressed.len() {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): truncated extended match length".into(),
                    ));
                }
                let ext = compressed[sp];
                sp += 1;
                match_len = match_len.checked_add(usize::from(ext)).ok_or_else(|| {
                    PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): match length overflow"
                            .into(),
                    )
                })?;
                if out
                    .len()
                    .checked_add(match_len)
                    .is_none_or(|n| n > expected_raw_len)
                {
                    return Err(PgBoltError::Bridge(
                        "corrupt compressed inline varlena (4B_C LZ4): extended match exceeds expected raw size".into(),
                    ));
                }
                if ext != 255 {
                    break;
                }
            }
        }
        copy_match_checked(&mut out, match_off, match_len, expected_raw_len, "4B_C LZ4")?;
    }

    if out.len() != expected_raw_len {
        return Err(PgBoltError::Bridge(format!(
            "corrupt compressed inline varlena (4B_C LZ4): decompressed {} bytes, expected {expected_raw_len}",
            out.len()
        )));
    }
    Ok(out)
}

#[inline]
fn external_varlena_total_len(tuple: &[u8], cursor: usize) -> PgBoltResult<(u8, usize)> {
    let va_tag = *tuple.get(cursor + 1).ok_or_else(|| {
        PgBoltError::Bridge("truncated external TOAST pointer (1B_E) in heap tuple".into())
    })?;
    let total_len = match va_tag {
        18 => 18usize,    // VARTAG_ONDISK (`varatt_external` = 16B + 2B header)
        1..=3 => 10usize, // VARTAG_INDIRECT / VARTAG_EXPANDED_* (pointer + 2B header)
        other => {
            return Err(PgBoltError::Bridge(format!(
                "unknown external TOAST pointer tag {other} (1B_E) at tuple offset {cursor}"
            )));
        }
    };
    Ok((va_tag, total_len))
}

/// Skips one varlena datum (including alignment padding, `1B`, `1B_E`, `4B_U`, or `4B_C`)
/// without decompressing or resolving external TOAST pointers; used for `attisdropped` columns.
fn advance_varlena_cursor(tuple: &[u8], cursor: &mut usize) -> PgBoltResult<()> {
    if *cursor >= tuple.len() {
        return Err(PgBoltError::Bridge(
            "unexpected end of heap tuple before dropped varlena header".into(),
        ));
    }
    if tuple[*cursor] == 0x00 {
        *cursor = align_up(*cursor, 4);
        if *cursor >= tuple.len() {
            return Err(PgBoltError::Bridge(
                "unexpected end of heap tuple after dropped varlena alignment padding".into(),
            ));
        }
    }
    let first = tuple[*cursor];
    let total_len = if (first & 0x01) != 0 {
        let len = (first >> 1) as usize;
        if len == 0 {
            external_varlena_total_len(tuple, *cursor)?.1
        } else {
            len
        }
    } else {
        if *cursor % 4 != 0 || *cursor + 4 > tuple.len() {
            return Err(PgBoltError::Bridge(format!(
                "invalid or unaligned 4-byte varlena header at tuple offset {}",
                *cursor
            )));
        }
        let word = read_u32_le(tuple, *cursor);
        let len = (word >> 2) as usize;
        let min_len = if (word & 0x03) == 0x02 { 8 } else { 4 };
        if len < min_len {
            return Err(PgBoltError::Bridge(format!(
                "invalid 4-byte varlena length {len} at tuple offset {}",
                *cursor
            )));
        }
        len
    };
    let end = cursor
        .checked_add(total_len)
        .ok_or_else(|| PgBoltError::Bridge("varlena offset overflow in heap tuple".into()))?;
    if end > tuple.len() {
        return Err(PgBoltError::Bridge(format!(
            "varlena length {total_len} at offset {} exceeds tuple length {}",
            *cursor,
            tuple.len()
        )));
    }
    *cursor = end;
    Ok(())
}

/// Decodes a PostgreSQL varlena datum (`1B`, `4B_U`, inline `4B_C` PGLZ, or optional external
/// `1B_E` / `4B_C` LZ4 via `detoaster`), honoring `att_align_pointer` short-header packing.
fn read_varlena_payload_cow<'a>(
    tuple: &'a [u8],
    cursor: &mut usize,
    detoaster: Option<&dyn ToastDetoaster>,
) -> PgBoltResult<Cow<'a, [u8]>> {
    if *cursor >= tuple.len() {
        return Err(PgBoltError::Bridge(
            "unexpected end of heap tuple before varlena header".into(),
        ));
    }
    if tuple[*cursor] == 0x00 {
        *cursor = align_up(*cursor, 4);
        if *cursor >= tuple.len() {
            return Err(PgBoltError::Bridge(
                "unexpected end of heap tuple after varlena alignment padding".into(),
            ));
        }
    }

    let first = tuple[*cursor];
    if (first & 0x01) != 0 {
        let total_len = (first >> 1) as usize;
        if total_len == 0 {
            let (va_tag, ptr_len) = external_varlena_total_len(tuple, *cursor)?;
            let end = cursor.checked_add(ptr_len).ok_or_else(|| {
                PgBoltError::Bridge("external TOAST pointer offset overflow in heap tuple".into())
            })?;
            if end > tuple.len() {
                return Err(PgBoltError::Bridge(format!(
                    "external TOAST pointer length {ptr_len} at offset {} exceeds tuple length {}",
                    *cursor,
                    tuple.len()
                )));
            }
            let raw_varlena = &tuple[*cursor..end];
            *cursor = end;
            if let Some(resolver) = detoaster {
                return Ok(Cow::Owned(resolver.detoast(raw_varlena)?));
            }
            return Err(PgBoltError::Bridge(format!(
                "cannot decode external TOAST pointer (1B_E, tag {va_tag}) from raw heap page without detoasting"
            )));
        }
        let end = cursor.checked_add(total_len).ok_or_else(|| {
            PgBoltError::Bridge("1-byte varlena offset overflow in heap tuple".into())
        })?;
        if end > tuple.len() {
            return Err(PgBoltError::Bridge(format!(
                "1-byte varlena length {total_len} at offset {} exceeds tuple length {}",
                *cursor,
                tuple.len()
            )));
        }
        let payload = &tuple[*cursor + 1..end];
        *cursor = end;
        Ok(Cow::Borrowed(payload))
    } else {
        if *cursor % 4 != 0 {
            return Err(PgBoltError::Bridge(format!(
                "unaligned 4-byte varlena header at tuple offset {} (first byte 0x{first:02x})",
                *cursor
            )));
        }
        if *cursor + 4 > tuple.len() {
            return Err(PgBoltError::Bridge(
                "truncated 4-byte varlena header in heap tuple".into(),
            ));
        }
        let word = read_u32_le(tuple, *cursor);
        let total_len = (word >> 2) as usize;
        if (word & 0x03) == 0x02 {
            if total_len < 8 {
                return Err(PgBoltError::Bridge(format!(
                    "corrupt compressed inline varlena (4B_C): header length {total_len} < 8 at offset {}",
                    *cursor
                )));
            }
            let end = cursor.checked_add(total_len).ok_or_else(|| {
                PgBoltError::Bridge(
                    "compressed 4-byte varlena offset overflow in heap tuple".into(),
                )
            })?;
            if end > tuple.len() {
                return Err(PgBoltError::Bridge(format!(
                    "compressed 4-byte varlena length {total_len} at offset {} exceeds tuple length {}",
                    *cursor,
                    tuple.len()
                )));
            }
            let va_tcinfo = read_u32_le(tuple, *cursor + 4);
            let method = (va_tcinfo >> 30) & 0x03;
            let expected_raw_len = (va_tcinfo & 0x3FFF_FFFF) as usize;
            let raw_varlena = &tuple[*cursor..end];
            let compressed_payload = &tuple[*cursor + 8..end];
            *cursor = end;
            return match method {
                0 => Ok(Cow::Owned(pglz_decompress(
                    compressed_payload,
                    expected_raw_len,
                )?)),
                1 => {
                    if let Some(resolver) = detoaster {
                        Ok(Cow::Owned(resolver.detoast(raw_varlena)?))
                    } else {
                        Ok(Cow::Owned(lz4_decompress(
                            compressed_payload,
                            expected_raw_len,
                        )?))
                    }
                }
                other => Err(PgBoltError::Bridge(format!(
                    "corrupt compressed inline varlena (4B_C): invalid compression method {other}"
                ))),
            };
        }

        if total_len < 4 {
            return Err(PgBoltError::Bridge(format!(
                "invalid 4-byte varlena length {total_len} at tuple offset {}",
                *cursor
            )));
        }
        let end = cursor.checked_add(total_len).ok_or_else(|| {
            PgBoltError::Bridge("4-byte varlena offset overflow in heap tuple".into())
        })?;
        if end > tuple.len() {
            return Err(PgBoltError::Bridge(format!(
                "4-byte varlena length {total_len} at offset {} exceeds tuple length {}",
                *cursor,
                tuple.len()
            )));
        }
        let payload = &tuple[*cursor + 4..end];
        *cursor = end;
        Ok(Cow::Borrowed(payload))
    }
}

/// Encodes `payload` into `out` using a PostgreSQL 1-byte (`1B`) or 4-byte (`4B_U`) varlena header.
fn write_varlena_payload(out: &mut Vec<u8>, payload: &[u8]) -> PgBoltResult<()> {
    let short_total = payload.len().saturating_add(1);
    if short_total <= 126 {
        let va_header = ((short_total as u8) << 1) | 0x01;
        out.push(va_header);
        out.extend_from_slice(payload);
        return Ok(());
    }
    let aligned = align_up(out.len(), 4);
    out.resize(aligned, 0);
    let total_len = payload
        .len()
        .checked_add(4)
        .ok_or_else(|| PgBoltError::Bridge("varlena payload length overflow".into()))?;
    let total_u32 = u32::try_from(total_len)
        .ok()
        .filter(|&len| len <= (u32::MAX >> 2))
        .ok_or_else(|| PgBoltError::Bridge("varlena payload exceeds 4-byte header range".into()))?;
    out.extend_from_slice(&(total_u32 << 2).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Decodes a PostgreSQL binary `NumericShort` / `NumericLong` payload into a canonical base-10 string.
fn decode_pg_numeric_bytes(payload: &[u8]) -> PgBoltResult<String> {
    if payload.len() < 2 {
        return Err(PgBoltError::Bridge(
            "truncated PostgreSQL NUMERIC header in heap tuple".into(),
        ));
    }
    let header = read_u16_le(payload, 0);
    let top_bits = header & NUMERIC_SPECIAL_MASK;
    if top_bits == NUMERIC_SPECIAL_MASK {
        return match header {
            NUMERIC_NAN_HEADER => Err(PgBoltError::Bridge(
                "PostgreSQL NUMERIC NaN cannot be represented in Arrow Decimal128".into(),
            )),
            NUMERIC_PINF_HEADER => Err(PgBoltError::Bridge(
                "PostgreSQL NUMERIC Infinity (+Infinity) cannot be represented in Arrow Decimal128"
                    .into(),
            )),
            NUMERIC_NINF_HEADER => Err(PgBoltError::Bridge(
                "PostgreSQL NUMERIC -Infinity cannot be represented in Arrow Decimal128".into(),
            )),
            other => Err(PgBoltError::Bridge(format!(
                "unknown PostgreSQL NUMERIC special header 0x{other:04x}"
            ))),
        };
    }

    let (sign_neg, weight, dscale, digit_bytes) = if top_bits == NUMERIC_SHORT_FLAG {
        let neg = (header & NUMERIC_SHORT_SIGN_MASK) != 0;
        let dscale = ((header >> 7) & 0x003F) as usize;
        // 7-bit two's complement weight in range `-64..=63`.
        let weight_raw = (header & 0x007F) << 9;
        let weight = ((weight_raw as i16) >> 9) as isize;
        (neg, weight, dscale, &payload[2..])
    } else {
        if payload.len() < 4 {
            return Err(PgBoltError::Bridge(
                "truncated PostgreSQL NumericLong header in heap tuple".into(),
            ));
        }
        let neg = top_bits == NUMERIC_NEG_FLAG;
        let dscale = (header & 0x3FFF) as usize;
        let weight = read_i16_le(payload, 2) as isize;
        (neg, weight, dscale, &payload[4..])
    };

    if digit_bytes.len() % 2 != 0 {
        return Err(PgBoltError::Bridge(
            "PostgreSQL NUMERIC digit payload length is not a multiple of 2".into(),
        ));
    }

    let ndigits = digit_bytes.len() / 2;
    let mut digits = Vec::with_capacity(ndigits);
    for chunk in digit_bytes.chunks_exact(2) {
        let d = u16::from_le_bytes([chunk[0], chunk[1]]);
        if d >= NUMERIC_NBASE {
            return Err(PgBoltError::Bridge(format!(
                "invalid base-10000 digit {d} in PostgreSQL NUMERIC payload"
            )));
        }
        digits.push(d);
    }

    if digits.is_empty() {
        let mut s = String::from("0");
        if dscale > 0 {
            s.push('.');
            s.extend(std::iter::repeat_n('0', dscale));
        }
        return Ok(s);
    }

    let mut out = String::new();
    if sign_neg {
        out.push('-');
    }

    let mut wrote_int = false;
    if weight >= 0 {
        let int_groups = weight as usize + 1;
        for idx in 0..int_groups {
            let d = digits.get(idx).copied().unwrap_or(0);
            if !wrote_int {
                if d == 0 {
                    continue;
                }
                out.push_str(&d.to_string());
                wrote_int = true;
            } else {
                out.push_str(&format!("{d:04}"));
            }
        }
    }
    if !wrote_int {
        out.push('0');
    }

    let frac_start_idx = weight + 1;
    let extra_frac_groups = (digits.len() as isize - frac_start_idx).max(0) as usize;
    let max_groups = dscale.div_ceil(4).max(extra_frac_groups);
    if max_groups > 0 {
        let mut frac_buf = String::with_capacity(max_groups * 4);
        for g in 0..max_groups {
            let idx = frac_start_idx + g as isize;
            let d = if idx >= 0 && (idx as usize) < digits.len() {
                digits[idx as usize]
            } else {
                0
            };
            frac_buf.push_str(&format!("{d:04}"));
        }
        let last_nz = frac_buf
            .bytes()
            .rposition(|b| b != b'0')
            .map_or(0, |p| p + 1);
        let keep_len = dscale.max(last_nz);
        if keep_len > 0 {
            out.push('.');
            out.push_str(&frac_buf[..keep_len]);
        }
    }

    Ok(out)
}

/// Encodes a canonical decimal string into PostgreSQL `NumericShort` / `NumericLong` payload bytes.
fn encode_pg_numeric_bytes(s: &str) -> PgBoltResult<Vec<u8>> {
    let (neg, significant, base10_exp) = parse_decimal_parts(s)?;
    let dscale = if base10_exp < 0 {
        base10_exp.unsigned_abs() as usize
    } else {
        0
    };

    if significant.is_empty() {
        return if dscale <= 63 {
            let header = NUMERIC_SHORT_FLAG | (((dscale as u16) & 0x003F) << 7);
            Ok(header.to_le_bytes().to_vec())
        } else {
            let mut out = Vec::with_capacity(4);
            out.extend_from_slice(&((dscale as u16) & 0x3FFF).to_le_bytes());
            out.extend_from_slice(&0_i16.to_le_bytes());
            Ok(out)
        };
    }

    let int_len = (significant.len() as i32)
        .checked_add(base10_exp)
        .ok_or_else(|| PgBoltError::Bridge(format!("NUMERIC digit count overflow in '{s}'")))?;

    if int_len > 131_072 || dscale > 16_383 {
        return Err(PgBoltError::Bridge(format!(
            "NUMERIC value '{s}' exceeds PostgreSQL NumericData header bounds"
        )));
    }

    let (int_digits, frac_digits) = if int_len <= 0 {
        let neg_int_len = int_len.unsigned_abs() as usize;
        let mut frac = String::with_capacity(neg_int_len + significant.len());
        frac.extend(std::iter::repeat_n('0', neg_int_len));
        frac.push_str(&significant);
        (String::new(), frac)
    } else if (int_len as usize) >= significant.len() {
        let mut int = String::with_capacity(int_len as usize);
        int.push_str(&significant);
        int.extend(std::iter::repeat_n(
            '0',
            int_len as usize - significant.len(),
        ));
        (int, String::new())
    } else {
        let split = int_len as usize;
        (
            significant[..split].to_owned(),
            significant[split..].to_owned(),
        )
    };

    let mut int_chunks = Vec::new();
    if !int_digits.is_empty() {
        let pad = (4 - (int_digits.len() % 4)) % 4;
        let mut padded = String::with_capacity(pad + int_digits.len());
        padded.extend(std::iter::repeat_n('0', pad));
        padded.push_str(&int_digits);
        for chunk in padded.as_bytes().chunks_exact(4) {
            let d = std::str::from_utf8(chunk)
                .map_err(|_| {
                    PgBoltError::Bridge("non-UTF8 chunk in integer base-10000 encode".into())
                })?
                .parse::<u16>()
                .map_err(|_| {
                    PgBoltError::Bridge("failed to encode integer base-10000 digit".into())
                })?;
            int_chunks.push(d);
        }
    }

    let mut frac_chunks = Vec::new();
    if !frac_digits.is_empty() {
        let pad = (4 - (frac_digits.len() % 4)) % 4;
        let mut padded = String::with_capacity(frac_digits.len() + pad);
        padded.push_str(&frac_digits);
        padded.extend(std::iter::repeat_n('0', pad));
        for chunk in padded.as_bytes().chunks_exact(4) {
            let d = std::str::from_utf8(chunk)
                .map_err(|_| {
                    PgBoltError::Bridge("non-UTF8 chunk in fractional base-10000 encode".into())
                })?
                .parse::<u16>()
                .map_err(|_| {
                    PgBoltError::Bridge("failed to encode fractional base-10000 digit".into())
                })?;
            frac_chunks.push(d);
        }
    }

    let mut combined = int_chunks;
    let int_group_count = combined.len() as i32;
    combined.extend(frac_chunks);

    let first_nonzero = combined
        .iter()
        .position(|&d| d != 0)
        .unwrap_or(combined.len());
    let last_nonzero = combined
        .iter()
        .rposition(|&d| d != 0)
        .map_or(first_nonzero, |p| p + 1);
    let trimmed_digits = &combined[first_nonzero..last_nonzero];

    if trimmed_digits.is_empty() {
        return Ok(NUMERIC_SHORT_FLAG.to_le_bytes().to_vec());
    }

    let weight = int_group_count - 1 - (first_nonzero as i32);
    let mut payload = Vec::with_capacity(4 + trimmed_digits.len() * 2);

    if (-64..=63).contains(&weight) && dscale <= 63 {
        let sign_bit = if neg { NUMERIC_SHORT_SIGN_MASK } else { 0 };
        let dscale_bits = ((dscale as u16) & 0x003F) << 7;
        let weight_bits = (weight as i16 as u16) & 0x007F;
        let header = NUMERIC_SHORT_FLAG | sign_bit | dscale_bits | weight_bits;
        payload.extend_from_slice(&header.to_le_bytes());
    } else {
        let weight_i16 = i16::try_from(weight)
            .map_err(|_| PgBoltError::Bridge(format!("NUMERIC weight {weight} overflows i16")))?;
        let sign_bits = if neg { NUMERIC_NEG_FLAG } else { 0 };
        let header = sign_bits | ((dscale as u16) & 0x3FFF);
        payload.extend_from_slice(&header.to_le_bytes());
        payload.extend_from_slice(&weight_i16.to_le_bytes());
    }

    for &d in trimmed_digits {
        payload.extend_from_slice(&d.to_le_bytes());
    }
    Ok(payload)
}

fn resolve_dropped_attr_layout(spec: &PgColumnSpec) -> PgBoltResult<(i16, usize)> {
    if let Some(len) = spec.attlen {
        let align = match spec.attalign {
            Some(b'c') | Some(1) => 1usize,
            Some(b's') | Some(2) => 2usize,
            Some(b'i') | Some(4) => 4usize,
            Some(b'd') | Some(8) => 8usize,
            Some(other) => {
                return Err(PgBoltError::Bridge(format!(
                    "unsupported attalign {other} on dropped column '{}'",
                    spec.meta.name
                )));
            }
            None => match len {
                -1 => 4,
                -2 | 1 => 1,
                2 => 2,
                4 => 4,
                8 => 8,
                _ => 1,
            },
        };
        return Ok((len, align));
    }
    if spec.meta.type_oid != 0 {
        let (pg_ty, _) = map_pg_oid_to_arrow(spec.meta.type_oid)?;
        return Ok(match pg_ty {
            SupportedPgType::Bool => (1, 1),
            SupportedPgType::Int2 => (2, 2),
            SupportedPgType::Int4 | SupportedPgType::Float4 | SupportedPgType::Date => (4, 4),
            SupportedPgType::Int8
            | SupportedPgType::Float8
            | SupportedPgType::Time
            | SupportedPgType::Timestamp
            | SupportedPgType::Timestamptz => (8, 8),
            SupportedPgType::Uuid => (16, 1),
            SupportedPgType::Binary | SupportedPgType::Text | SupportedPgType::Numeric => (-1, 4),
        });
    }
    Err(PgBoltError::Bridge(format!(
        "dropped column '{}' is non-null in heap tuple without attlen or type_oid",
        spec.meta.name
    )))
}

fn skip_dropped_attribute(
    spec: &PgColumnSpec,
    tuple: &[u8],
    cursor: &mut usize,
) -> PgBoltResult<()> {
    let (attlen, attalign) = resolve_dropped_attr_layout(spec)?;
    if attlen == -1 {
        return advance_varlena_cursor(tuple, cursor);
    }
    if attlen == -2 {
        *cursor = align_up(*cursor, attalign);
        let rel_nul = tuple
            .get(*cursor..)
            .and_then(|s| s.iter().position(|&b| b == 0))
            .ok_or_else(|| {
                PgBoltError::Bridge(format!(
                    "unterminated cstring in dropped column '{}'",
                    spec.meta.name
                ))
            })?;
        *cursor += rel_nul + 1;
        return Ok(());
    }
    if attlen <= 0 {
        return Err(PgBoltError::Bridge(format!(
            "invalid attlen {attlen} for dropped column '{}'",
            spec.meta.name
        )));
    }
    *cursor = align_up(*cursor, attalign);
    let len = attlen as usize;
    if cursor.checked_add(len).is_none_or(|end| end > tuple.len()) {
        return Err(PgBoltError::Bridge(format!(
            "truncated dropped column '{}' in heap tuple",
            spec.meta.name
        )));
    }
    *cursor += len;
    Ok(())
}

/// Decodes one `LP_NORMAL` tuple slice using extended `PgColumnSpec` metadata
/// (`attisdropped`, `attmissing`, `typmod`, PGLZ `4B_C`, and optional external TOAST detoaster).
fn decode_tuple_slice_ext(
    specs: &[PgColumnSpec],
    tuple: &[u8],
    detoaster: Option<&dyn ToastDetoaster>,
) -> PgBoltResult<DecodedPageTuple> {
    if tuple.len() < MIN_HEAP_TUPLE_HEADER_SIZE {
        return Err(PgBoltError::Bridge(format!(
            "heap tuple length {} smaller than minimum header size {MIN_HEAP_TUPLE_HEADER_SIZE}",
            tuple.len()
        )));
    }

    let t_xmin = read_u32_le(tuple, 0);
    let t_xmax = read_u32_le(tuple, 4);
    let _t_cid = read_u32_le(tuple, 8);
    let t_infomask2 = read_u16_le(tuple, 18);
    let t_infomask = read_u16_le(tuple, 20);
    let t_hoff = tuple[22];
    let natts = t_infomask2 & 0x07FF;

    let hoff = t_hoff as usize;
    if hoff < PG_PAGE_HEADER_SIZE || hoff % 8 != 0 || hoff > tuple.len() {
        return Err(PgBoltError::Bridge(format!(
            "invalid heap tuple t_hoff={t_hoff} for tuple length {}",
            tuple.len()
        )));
    }

    let natts_usize = natts as usize;
    if natts_usize > specs.len() {
        return Err(PgBoltError::Bridge(format!(
            "heap tuple natts ({natts_usize}) exceeds provided schema width ({})",
            specs.len()
        )));
    }

    let has_null = (t_infomask & HEAP_HASNULL) != 0;
    if has_null {
        let bitmap_bytes = natts_usize.div_ceil(8);
        if MIN_HEAP_TUPLE_HEADER_SIZE + bitmap_bytes > hoff {
            return Err(PgBoltError::Bridge(format!(
                "heap tuple null bitmap ({bitmap_bytes} bytes) exceeds t_hoff ({hoff})"
            )));
        }
    }

    let visible_count = specs.iter().filter(|s| !s.attisdropped).count();
    let mut values = Vec::with_capacity(visible_count);
    let mut cursor = hoff;

    for (col_idx, spec) in specs.iter().enumerate() {
        if col_idx >= natts_usize {
            if spec.attisdropped {
                continue;
            }
            values.push(spec.attmissing.clone().unwrap_or(PgDatumValue::Null));
            continue;
        }
        if has_null {
            let bit = 1_u8 << (col_idx & 7);
            let is_not_null = (tuple[MIN_HEAP_TUPLE_HEADER_SIZE + (col_idx >> 3)] & bit) != 0;
            if !is_not_null {
                if !spec.attisdropped {
                    values.push(PgDatumValue::Null);
                }
                continue;
            }
        }
        if spec.attisdropped {
            skip_dropped_attribute(spec, tuple, &mut cursor)?;
            continue;
        }

        let (pg_ty, _) = map_pg_oid_to_arrow(spec.meta.type_oid)?;
        let val = match pg_ty {
            SupportedPgType::Bool => {
                if cursor + 1 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated BOOL column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let b = tuple[cursor];
                cursor += 1;
                PgDatumValue::Bool(b != 0)
            }
            SupportedPgType::Int2 => {
                cursor = align_up(cursor, 2);
                if cursor + 2 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated INT2 column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let v = read_i16_le(tuple, cursor);
                cursor += 2;
                PgDatumValue::Int16(v)
            }
            SupportedPgType::Int4 => {
                cursor = align_up(cursor, 4);
                if cursor + 4 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated INT4 column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let v = read_i32_le(tuple, cursor);
                cursor += 4;
                PgDatumValue::Int32(v)
            }
            SupportedPgType::Float4 => {
                cursor = align_up(cursor, 4);
                if cursor + 4 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated FLOAT4 column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let v = read_f32_le(tuple, cursor);
                cursor += 4;
                PgDatumValue::Float32(v)
            }
            SupportedPgType::Date => {
                cursor = align_up(cursor, 4);
                if cursor + 4 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated DATE column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let pg_days = read_i32_le(tuple, cursor);
                cursor += 4;
                PgDatumValue::DateDays(pg_date_to_unix_days(pg_days)?)
            }
            SupportedPgType::Int8 => {
                cursor = align_up(cursor, 8);
                if cursor + 8 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated INT8 column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let v = read_i64_le(tuple, cursor);
                cursor += 8;
                PgDatumValue::Int64(v)
            }
            SupportedPgType::Float8 => {
                cursor = align_up(cursor, 8);
                if cursor + 8 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated FLOAT8 column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let v = read_f64_le(tuple, cursor);
                cursor += 8;
                PgDatumValue::Float64(v)
            }
            SupportedPgType::Time => {
                cursor = align_up(cursor, 8);
                if cursor + 8 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated TIME column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let pg_usec = read_i64_le(tuple, cursor);
                cursor += 8;
                if !(0..=PG_TIME_MAX_USEC).contains(&pg_usec) {
                    return Err(PgBoltError::Bridge(format!(
                        "TIME value {pg_usec} us in column '{}' out of range [0, {PG_TIME_MAX_USEC}]",
                        spec.meta.name
                    )));
                }
                PgDatumValue::TimeMicros(pg_usec)
            }
            SupportedPgType::Timestamp | SupportedPgType::Timestamptz => {
                cursor = align_up(cursor, 8);
                if cursor + 8 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated TIMESTAMP column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let pg_usec = read_i64_le(tuple, cursor);
                cursor += 8;
                PgDatumValue::TimestampMicros(pg_usec_to_unix_usec(pg_usec)?)
            }
            SupportedPgType::Uuid => {
                // PostgreSQL `uuid` uses `typalign = 'c'` (1-byte alignment, 16-byte raw storage).
                if cursor + 16 > tuple.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "truncated UUID column '{}' in heap tuple",
                        spec.meta.name
                    )));
                }
                let mut uuid_bytes = [0_u8; 16];
                uuid_bytes.copy_from_slice(&tuple[cursor..cursor + 16]);
                cursor += 16;
                PgDatumValue::Uuid(uuid_bytes)
            }
            SupportedPgType::Binary => {
                let payload = read_varlena_payload_cow(tuple, &mut cursor, detoaster)?;
                PgDatumValue::Bytes(payload.into_owned())
            }
            SupportedPgType::Text => {
                let payload = read_varlena_payload_cow(tuple, &mut cursor, detoaster)?;
                let text = std::str::from_utf8(payload.as_ref()).map_err(|e| {
                    PgBoltError::Bridge(format!(
                        "invalid UTF-8 in TEXT column '{}': {e}",
                        spec.meta.name
                    ))
                })?;
                PgDatumValue::Text(text.to_owned())
            }
            SupportedPgType::Numeric => {
                let payload = read_varlena_payload_cow(tuple, &mut cursor, detoaster)?;
                let dec_str = decode_pg_numeric_bytes(payload.as_ref())?;
                PgDatumValue::Numeric(dec_str)
            }
        };
        values.push(val);
    }

    Ok(DecodedPageTuple {
        header: HeapTupleHeaderMeta {
            t_xmin,
            t_xmax,
            t_infomask,
            t_infomask2,
            t_hoff,
            natts,
        },
        values,
    })
}

fn decode_heap_page_tuples_with_specs(
    specs: &[PgColumnSpec],
    page: &[u8],
    detoaster: Option<&dyn ToastDetoaster>,
) -> PgBoltResult<Vec<DecodedPageTuple>> {
    if page.len() != PG_BLCKSZ {
        return Err(PgBoltError::Bridge(format!(
            "invalid PostgreSQL heap page length {}: expected {PG_BLCKSZ}",
            page.len()
        )));
    }

    let pd_lower = read_u16_le(page, 12) as usize;
    let pd_upper = read_u16_le(page, 14) as usize;
    let pd_special = read_u16_le(page, 16) as usize;

    if pd_lower < PG_PAGE_HEADER_SIZE
        || pd_lower > pd_upper
        || pd_upper > pd_special
        || pd_special > PG_BLCKSZ
        || (pd_lower - PG_PAGE_HEADER_SIZE) % 4 != 0
    {
        return Err(PgBoltError::Bridge(format!(
            "corrupt PageHeaderData bounds: pd_lower={pd_lower}, pd_upper={pd_upper}, pd_special={pd_special}"
        )));
    }

    let n_items = (pd_lower - PG_PAGE_HEADER_SIZE) / 4;
    let mut tuples = Vec::with_capacity(n_items);
    let mut emitted = vec![false; n_items];

    let read_item = |idx: usize| -> (usize, u16, usize) {
        let raw_item = read_u32_le(page, PG_PAGE_HEADER_SIZE + idx * 4);
        let lp_off = (raw_item & 0x7FFF) as usize;
        let lp_flags = ((raw_item >> 15) & 0x0003) as u16;
        let lp_len = ((raw_item >> 17) & 0x7FFF) as usize;
        (lp_off, lp_flags, lp_len)
    };

    for item_idx in 0..n_items {
        if emitted[item_idx] {
            continue;
        }
        let (mut lp_off, mut lp_flags, mut lp_len) = read_item(item_idx);
        let mut target_idx = item_idx;

        if lp_flags == LP_REDIRECT {
            let mut visited = Vec::new();
            loop {
                if lp_off == 0 || lp_off > n_items {
                    return Err(PgBoltError::Bridge(format!(
                        "LP_REDIRECT at item {target_idx} targets out-of-range OffsetNumber {lp_off} (n_items={n_items})"
                    )));
                }
                let next_idx = lp_off - 1;
                if next_idx == target_idx || visited.contains(&next_idx) {
                    return Err(PgBoltError::Bridge(format!(
                        "cycle detected in heap page LP_REDIRECT chain starting at item {item_idx}"
                    )));
                }
                visited.push(target_idx);
                target_idx = next_idx;
                let next = read_item(target_idx);
                lp_off = next.0;
                lp_flags = next.1;
                lp_len = next.2;
                if lp_flags != LP_REDIRECT {
                    break;
                }
            }
        }

        if lp_flags != LP_NORMAL || lp_len < MIN_HEAP_TUPLE_HEADER_SIZE || emitted[target_idx] {
            continue;
        }
        let end = lp_off.checked_add(lp_len).ok_or_else(|| {
            PgBoltError::Bridge(format!("item {target_idx} offset/length overflow"))
        })?;
        if lp_off < pd_upper || end > pd_special {
            return Err(PgBoltError::Bridge(format!(
                "item {target_idx} span [{lp_off}..{end}) outside page tuple region [{pd_upper}..{pd_special})"
            )));
        }

        let decoded = decode_tuple_slice_ext(specs, &page[lp_off..end], detoaster)?;
        emitted[target_idx] = true;
        tuples.push(decoded);
    }

    Ok(tuples)
}

/// Parses a single 8KB PostgreSQL heap page into `DecodedPageTuple` entries.
pub fn decode_heap_page_tuples(
    schema: &[PgColumnMeta],
    page: &[u8],
) -> PgBoltResult<Vec<DecodedPageTuple>> {
    let specs: Vec<PgColumnSpec> = schema
        .iter()
        .cloned()
        .map(PgColumnSpec::from_meta)
        .collect();
    decode_heap_page_tuples_ext(&specs, page, None)
}

/// Parses a single 8KB PostgreSQL heap page with full `PgColumnSpec` evolution metadata
/// (`attisdropped`, `attmissing`, `typmod`), intra-page `LP_REDIRECT` HOT chains, and an
/// optional external TOAST detoaster.
pub fn decode_heap_page_tuples_ext(
    specs: &[PgColumnSpec],
    page: &[u8],
    detoaster: Option<&dyn ToastDetoaster>,
) -> PgBoltResult<Vec<DecodedPageTuple>> {
    for spec in specs {
        if !spec.attisdropped {
            map_pg_column_to_arrow(spec)?;
        }
    }
    decode_heap_page_tuples_with_specs(specs, page, detoaster)
}

fn toast_table_specs() -> [PgColumnSpec; 3] {
    [
        PgColumnSpec::from_meta(PgColumnMeta {
            name: "chunk_id".into(),
            type_oid: INT4OID,
            nullable: false,
        }),
        PgColumnSpec::from_meta(PgColumnMeta {
            name: "chunk_seq".into(),
            type_oid: INT4OID,
            nullable: false,
        }),
        PgColumnSpec::from_meta(PgColumnMeta {
            name: "chunk_data".into(),
            type_oid: BYTEAOID,
            nullable: false,
        }),
    ]
}

/// Standalone pure-Rust reader and reassembler for PostgreSQL TOAST relation pages (`pg_toast_<oid>`).
///
/// Implements [`ToastDetoaster`] so offline 8KB page decoders can detoast external `1B_E` on-disk
/// TOAST pointers without an active PostgreSQL backend connection.
#[derive(Debug, Default, Clone)]
pub struct StandaloneToastReader {
    /// Mapping from `(va_valueid, chunk_seq)` to the chunk bytes.
    chunks: std::collections::BTreeMap<(u32, i32), Vec<u8>>,
}

impl StandaloneToastReader {
    /// Creates an empty `StandaloneToastReader`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a single TOAST chunk directly.
    pub fn add_chunk(&mut self, value_id: u32, chunk_seq: i32, data: Vec<u8>) {
        self.chunks.insert((value_id, chunk_seq), data);
    }

    /// Reads all tuples from an 8KB TOAST heap page and adds their chunks to the reader.
    ///
    /// In a PostgreSQL TOAST table, the columns are:
    /// - col 0 (`chunk_id`): `INT4OID` / `Oid`
    /// - col 1 (`chunk_seq`): `INT4OID`
    /// - col 2 (`chunk_data`): `BYTEAOID`
    ///
    /// Returns the number of chunks successfully extracted from the page.
    pub fn read_page(&mut self, page: &[u8]) -> PgBoltResult<usize> {
        let specs = toast_table_specs();
        let tuples = decode_heap_page_tuples_ext(&specs, page, None)?;
        let count = tuples.len();
        for tuple in tuples {
            if tuple.values.len() >= 3 {
                let chunk_id = match tuple.values[0] {
                    PgDatumValue::Int32(v) => v as u32,
                    _ => continue,
                };
                let chunk_seq = match tuple.values[1] {
                    PgDatumValue::Int32(v) => v,
                    _ => continue,
                };
                let chunk_data = match tuple.values[2] {
                    PgDatumValue::Bytes(ref b) => b.clone(),
                    _ => continue,
                };
                self.add_chunk(chunk_id, chunk_seq, chunk_data);
            }
        }
        Ok(count)
    }

    /// Constructs a `StandaloneToastReader` by scanning a slice of raw 8KB TOAST relation pages.
    pub fn from_pages(pages: &[&[u8]]) -> PgBoltResult<Self> {
        let mut reader = Self::new();
        for page in pages {
            reader.read_page(page)?;
        }
        Ok(reader)
    }
}

impl ToastDetoaster for StandaloneToastReader {
    fn detoast(&self, raw_varlena: &[u8]) -> PgBoltResult<Vec<u8>> {
        if raw_varlena.len() < 18 {
            return Err(PgBoltError::Bridge(format!(
                "invalid external TOAST varlena length {}: expected >= 18",
                raw_varlena.len()
            )));
        }
        let va_tag = raw_varlena[1];
        if va_tag != 18 {
            return Err(PgBoltError::Bridge(format!(
                "StandaloneToastReader only supports VARTAG_ONDISK (tag 18), got {va_tag}"
            )));
        }
        let va_rawsize_word = read_u32_le(raw_varlena, 2);
        let expected_raw_len = (va_rawsize_word & 0x3FFF_FFFF) as usize;
        let va_extsize_word = read_u32_le(raw_varlena, 6);
        let va_cmid = (va_extsize_word >> 30) & 0x03;
        let va_extsize = (va_extsize_word & 0x3FFF_FFFF) as usize;
        let va_valueid = read_u32_le(raw_varlena, 10);

        // Collect matching chunks sorted by chunk_seq
        let mut chunks_for_val = Vec::new();
        for (&(val_id, seq), chunk) in &self.chunks {
            if val_id == va_valueid {
                chunks_for_val.push((seq, chunk));
            }
        }
        if chunks_for_val.is_empty() {
            return Err(PgBoltError::Bridge(format!(
                "no TOAST chunks found for va_valueid {va_valueid}"
            )));
        }
        chunks_for_val.sort_by_key(|&(seq, _)| seq);

        // Verify sequence is contiguous starting from 0
        let mut total_ext_bytes = 0usize;
        for (expected_seq, &(seq, chunk)) in chunks_for_val.iter().enumerate() {
            if seq != expected_seq as i32 {
                return Err(PgBoltError::Bridge(format!(
                    "missing or out-of-order TOAST chunk seq {expected_seq} for va_valueid {va_valueid} (found {seq})"
                )));
            }
            total_ext_bytes = total_ext_bytes.saturating_add(chunk.len());
        }

        if total_ext_bytes > MAX_VARLENA_RAW_BYTES {
            return Err(PgBoltError::Bridge(format!(
                "assembled TOAST data length {total_ext_bytes} exceeds MAX_VARLENA_RAW_BYTES"
            )));
        }

        let mut assembled = Vec::with_capacity(total_ext_bytes);
        for (_, chunk) in chunks_for_val {
            assembled.extend_from_slice(chunk);
        }

        // Decompress if compressed
        if assembled.len() >= 8 {
            let word = read_u32_le(&assembled, 0);
            if (word & 0x03) == 0x02 {
                let va_tcinfo = read_u32_le(&assembled, 4);
                let method = (va_tcinfo >> 30) & 0x03;
                let raw_len = (va_tcinfo & 0x3FFF_FFFF) as usize;
                let payload = &assembled[8..];
                return match method {
                    0 => pglz_decompress(payload, raw_len),
                    1 => lz4_decompress(payload, raw_len),
                    other => Err(PgBoltError::Bridge(format!(
                        "unsupported TOAST compression method {other}"
                    ))),
                };
            }
        }

        // Check if compressed without inline 4B_C header
        if (va_cmid != 0 || (va_extsize > 0 && va_extsize < expected_raw_len))
            && assembled.len() == va_extsize
        {
            return match va_cmid {
                0 => pglz_decompress(&assembled, expected_raw_len),
                1 => lz4_decompress(&assembled, expected_raw_len),
                other => Err(PgBoltError::Bridge(format!(
                    "unsupported TOAST compression method {other} in external pointer"
                ))),
            };
        }

        Ok(assembled)
    }
}

fn decode_and_filter_page(
    specs: &[PgColumnSpec],
    page_idx: usize,
    page: &[u8],
    snapshot: Option<&MvccSnapshot>,
    vm: Option<&VisibilityMap>,
    detoaster: Option<&dyn ToastDetoaster>,
) -> PgBoltResult<Vec<DecodedPageTuple>> {
    let mut tuples = decode_heap_page_tuples_with_specs(specs, page, detoaster)?;
    if let Some(snap) = snapshot {
        let all_visible = vm.is_some_and(|v| v.is_all_visible(page_idx));
        if !all_visible {
            tuples.retain(|t| is_tuple_visible(&t.header, snap));
        }
    }
    Ok(tuples)
}

/// Decodes one or more PostgreSQL 8KB heap pages into an Arrow `RecordBatch`, applying
/// optional MVCC snapshot filtering and `VisibilityMap` all-visible fast-path bypassing.
///
/// Decodes pages in parallel across CPU cores via `std::thread::scope` when `pages.len() >= 4`.
pub fn decode_heap_pages_to_arrow(
    schema: &[PgColumnMeta],
    pages: &[&[u8]],
    snapshot: Option<&MvccSnapshot>,
    vm: Option<&VisibilityMap>,
) -> PgBoltResult<RecordBatch> {
    let specs: Vec<PgColumnSpec> = schema
        .iter()
        .cloned()
        .map(PgColumnSpec::from_meta)
        .collect();
    decode_heap_pages_to_arrow_with_specs(&specs, pages, snapshot, vm, None)
}

/// Decodes one or more PostgreSQL 8KB heap pages into an Arrow `RecordBatch` using full
/// `PgColumnSpec` definitions (`typmod`, `attisdropped`, `attmissing`) and optional TOAST resolver.
pub fn decode_heap_pages_to_arrow_with_specs(
    specs: &[PgColumnSpec],
    pages: &[&[u8]],
    snapshot: Option<&MvccSnapshot>,
    vm: Option<&VisibilityMap>,
    detoaster: Option<&(dyn ToastDetoaster + Sync)>,
) -> PgBoltResult<RecordBatch> {
    let (arrow_schema, plans) = build_arrow_schema_and_plans_from_specs(specs)?;
    if pages.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    let mut page_batches: Vec<Vec<DecodedPageTuple>> = Vec::with_capacity(pages.len());
    if pages.len() >= 4 {
        let n_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(pages.len())
            .max(1);
        let chunk_size = pages.len().div_ceil(n_workers);

        let chunk_results: Vec<PgBoltResult<Vec<Vec<DecodedPageTuple>>>> =
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for (chunk_idx, chunk) in pages.chunks(chunk_size).enumerate() {
                    handles.push(scope.spawn(move || {
                        let mut local = Vec::with_capacity(chunk.len());
                        for (local_idx, &page) in chunk.iter().enumerate() {
                            let page_idx = chunk_idx * chunk_size + local_idx;
                            local.push(decode_and_filter_page(
                                specs,
                                page_idx,
                                page,
                                snapshot,
                                vm,
                                detoaster.map(|d| d as &dyn ToastDetoaster),
                            )?);
                        }
                        Ok(local)
                    }));
                }
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().map_err(|_| {
                            PgBoltError::Bridge(
                                "heap page worker thread panicked during parallel decode".into(),
                            )
                        })?
                    })
                    .collect()
            });

        for res in chunk_results {
            page_batches.extend(res?);
        }
    } else {
        for (page_idx, &page) in pages.iter().enumerate() {
            page_batches.push(decode_and_filter_page(
                specs,
                page_idx,
                page,
                snapshot,
                vm,
                detoaster.map(|d| d as &dyn ToastDetoaster),
            )?);
        }
    }

    let total_rows: usize = page_batches.iter().map(Vec::len).sum();
    if total_rows == 0 {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    let mut builders = plans
        .iter()
        .map(|(plan, _)| PgColumnBuilder::with_plan(plan, total_rows))
        .collect::<PgBoltResult<Vec<_>>>()?;

    for tuples in page_batches {
        for tuple in tuples {
            for (&(_, spec), builder, val) in itertools_zip3(&plans, &mut builders, &tuple.values) {
                builder.append_datum(&spec.meta.name, spec.meta.nullable, val)?;
            }
        }
    }

    finish_column_builders(arrow_schema, builders, total_rows)
}

#[inline]
fn itertools_zip3<'a, 'b, A, B, C>(
    a: &'a [A],
    b: &'b mut [B],
    c: &'a [C],
) -> impl Iterator<Item = (&'a A, &'b mut B, &'a C)> {
    a.iter()
        .zip(b.iter_mut())
        .zip(c.iter())
        .map(|((x, y), z)| (x, y, z))
}

/// Test-page line-pointer entry for [`encode_test_heap_page_ext`].
#[derive(Clone, Debug)]
pub enum TestHeapPageItem {
    /// Active `LP_NORMAL` tuple, optionally truncated to `physical_natts` columns to
    /// test PostgreSQL 11+ `attmissingval` defaults (values length must equal `effective_natts`).
    Normal {
        header: HeapTupleHeaderMeta,
        values: Vec<PgDatumValue>,
        physical_natts: Option<usize>,
    },
    /// Intra-page `LP_REDIRECT` pointing to a 1-based `OffsetNumber` on the same page.
    Redirect(u16),
}

/// Encodes `(HeapTupleHeaderMeta, Vec<PgDatumValue>)` tuples into a valid PostgreSQL 8KB
/// binary heap page for tests and offline verification harnesses.
pub fn encode_test_heap_page(
    schema: &[PgColumnMeta],
    tuples: &[(HeapTupleHeaderMeta, Vec<PgDatumValue>)],
) -> PgBoltResult<[u8; PG_BLCKSZ]> {
    let specs: Vec<PgColumnSpec> = schema
        .iter()
        .cloned()
        .map(PgColumnSpec::from_meta)
        .collect();
    let items: Vec<TestHeapPageItem> = tuples
        .iter()
        .map(|(header, values)| TestHeapPageItem::Normal {
            header: *header,
            values: values.clone(),
            physical_natts: None,
        })
        .collect();
    encode_test_heap_page_ext(&specs, &items)
}

/// Encodes mixed `LP_NORMAL` (including dropped columns and truncated `physical_natts` for
/// `attmissing`) and `LP_REDIRECT` items into a valid PostgreSQL 8KB heap page for tests.
pub fn encode_test_heap_page_ext(
    specs: &[PgColumnSpec],
    items: &[TestHeapPageItem],
) -> PgBoltResult<[u8; PG_BLCKSZ]> {
    if specs.len() > MAX_TUPLE_ATTRS {
        return Err(PgBoltError::Bridge(format!(
            "schema column count {} exceeds PostgreSQL tuple limit {MAX_TUPLE_ATTRS}",
            specs.len()
        )));
    }
    let mut col_types = Vec::with_capacity(specs.len());
    for spec in specs {
        col_types.push(map_pg_oid_to_arrow(spec.meta.type_oid)?.0);
    }

    let mut page = [0_u8; PG_BLCKSZ];
    let mut pd_lower = PG_PAGE_HEADER_SIZE;
    let mut pd_upper = PG_BLCKSZ;
    let pd_special = PG_BLCKSZ;

    for (item_idx, item) in items.iter().enumerate() {
        match item {
            TestHeapPageItem::Redirect(target_offset) => {
                let new_pd_lower = pd_lower + 4;
                if new_pd_lower > pd_upper {
                    return Err(PgBoltError::Bridge(format!(
                        "redirect item {item_idx} exhausted free space on 8KB heap page"
                    )));
                }
                let item_word: u32 =
                    (u32::from(*target_offset) & 0x7FFF) | ((u32::from(LP_REDIRECT)) << 15);
                page[pd_lower..new_pd_lower].copy_from_slice(&item_word.to_le_bytes());
                pd_lower = new_pd_lower;
            }
            TestHeapPageItem::Normal {
                header: hdr,
                values,
                physical_natts,
            } => {
                let natts = physical_natts.unwrap_or(specs.len());
                if natts > specs.len() {
                    return Err(PgBoltError::Bridge(format!(
                        "tuple {item_idx} physical_natts ({natts}) exceeds specs length ({})",
                        specs.len()
                    )));
                }
                if values.len() != natts {
                    return Err(PgBoltError::Bridge(format!(
                        "tuple {item_idx} has {} values, expected {natts}",
                        values.len()
                    )));
                }

                let has_null = values.iter().any(|v| matches!(v, PgDatumValue::Null));
                let mut has_varwidth = false;

                let bitmap_bytes = if has_null { natts.div_ceil(8) } else { 0 };
                let t_hoff = align_up(MIN_HEAP_TUPLE_HEADER_SIZE + bitmap_bytes, 8);
                let t_hoff_u8 = u8::try_from(t_hoff).map_err(|_| {
                    PgBoltError::Bridge(format!("tuple {item_idx} t_hoff ({t_hoff}) overflows u8"))
                })?;

                let mut tuple_bytes = vec![0_u8; t_hoff];
                if has_null {
                    for (col_idx, v) in values.iter().enumerate() {
                        if !matches!(v, PgDatumValue::Null) {
                            tuple_bytes[MIN_HEAP_TUPLE_HEADER_SIZE + (col_idx >> 3)] |=
                                1_u8 << (col_idx & 7);
                        }
                    }
                }

                for ((spec, &pg_ty), val) in specs[..natts]
                    .iter()
                    .zip(col_types[..natts].iter())
                    .zip(values.iter())
                {
                    if matches!(val, PgDatumValue::Null) {
                        if !spec.meta.nullable && !spec.attisdropped {
                            return Err(PgBoltError::Bridge(format!(
                                "null value encoded for non-nullable column '{}'",
                                spec.meta.name
                            )));
                        }
                        continue;
                    }
                    match (pg_ty, val) {
                        (SupportedPgType::Bool, PgDatumValue::Bool(v)) => {
                            tuple_bytes.push(u8::from(*v));
                        }
                        (SupportedPgType::Int2, PgDatumValue::Int16(v)) => {
                            let off = align_up(tuple_bytes.len(), 2);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        (SupportedPgType::Int4, PgDatumValue::Int32(v)) => {
                            let off = align_up(tuple_bytes.len(), 4);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        (SupportedPgType::Float4, PgDatumValue::Float32(v)) => {
                            let off = align_up(tuple_bytes.len(), 4);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        (SupportedPgType::Date, PgDatumValue::DateDays(unix_days)) => {
                            let off = align_up(tuple_bytes.len(), 4);
                            tuple_bytes.resize(off, 0);
                            let pg_days = unix_days_to_pg_date(*unix_days)?;
                            tuple_bytes.extend_from_slice(&pg_days.to_le_bytes());
                        }
                        (SupportedPgType::Int8, PgDatumValue::Int64(v)) => {
                            let off = align_up(tuple_bytes.len(), 8);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        (SupportedPgType::Float8, PgDatumValue::Float64(v)) => {
                            let off = align_up(tuple_bytes.len(), 8);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        (SupportedPgType::Time, PgDatumValue::TimeMicros(us)) => {
                            if !(0..=PG_TIME_MAX_USEC).contains(us) {
                                return Err(PgBoltError::Bridge(format!(
                                    "TIME value {us} us out of range [0, {PG_TIME_MAX_USEC}]"
                                )));
                            }
                            let off = align_up(tuple_bytes.len(), 8);
                            tuple_bytes.resize(off, 0);
                            tuple_bytes.extend_from_slice(&us.to_le_bytes());
                        }
                        (
                            SupportedPgType::Timestamp | SupportedPgType::Timestamptz,
                            PgDatumValue::TimestampMicros(unix_usec),
                        ) => {
                            let off = align_up(tuple_bytes.len(), 8);
                            tuple_bytes.resize(off, 0);
                            let pg_usec = unix_usec_to_pg_usec(*unix_usec)?;
                            tuple_bytes.extend_from_slice(&pg_usec.to_le_bytes());
                        }
                        (SupportedPgType::Uuid, PgDatumValue::Uuid(bytes)) => {
                            // `typalign = 'c'`: no extra alignment padding before 16-byte UUID.
                            tuple_bytes.extend_from_slice(bytes);
                        }
                        (SupportedPgType::Binary, PgDatumValue::Bytes(bytes)) => {
                            has_varwidth = true;
                            write_varlena_payload(&mut tuple_bytes, bytes)?;
                        }
                        (SupportedPgType::Text, PgDatumValue::Text(s)) => {
                            has_varwidth = true;
                            write_varlena_payload(&mut tuple_bytes, s.as_bytes())?;
                        }
                        (SupportedPgType::Numeric, PgDatumValue::Numeric(s)) => {
                            has_varwidth = true;
                            let numeric_payload = encode_pg_numeric_bytes(s)?;
                            write_varlena_payload(&mut tuple_bytes, &numeric_payload)?;
                        }
                        (_, other) => {
                            return Err(PgBoltError::Bridge(format!(
                                "datum {other:?} mismatches column '{}' type {:?}",
                                spec.meta.name, pg_ty
                            )));
                        }
                    }
                }

                let mut eff_infomask = hdr.t_infomask & !(HEAP_HASNULL | HEAP_HASVARWIDTH);
                if has_null {
                    eff_infomask |= HEAP_HASNULL;
                }
                if has_varwidth {
                    eff_infomask |= HEAP_HASVARWIDTH;
                }
                let eff_infomask2 = (hdr.t_infomask2 & !0x07FF) | (natts as u16 & 0x07FF);

                tuple_bytes[0..4].copy_from_slice(&hdr.t_xmin.to_le_bytes());
                tuple_bytes[4..8].copy_from_slice(&hdr.t_xmax.to_le_bytes());
                tuple_bytes[18..20].copy_from_slice(&eff_infomask2.to_le_bytes());
                tuple_bytes[20..22].copy_from_slice(&eff_infomask.to_le_bytes());
                tuple_bytes[22] = t_hoff_u8;

                let tuple_len = tuple_bytes.len();
                if tuple_len > 0x7FFF || tuple_len > pd_upper {
                    return Err(PgBoltError::Bridge(format!(
                        "tuple {item_idx} size ({tuple_len} bytes) exceeds 8KB heap page capacity"
                    )));
                }

                let new_pd_upper = (pd_upper - tuple_len) & !7;
                let new_pd_lower = pd_lower + 4;
                if new_pd_lower > new_pd_upper {
                    return Err(PgBoltError::Bridge(format!(
                        "tuple {item_idx} ({tuple_len} bytes) exhausted free space on 8KB heap page"
                    )));
                }

                page[new_pd_upper..new_pd_upper + tuple_len].copy_from_slice(&tuple_bytes);
                let item_word: u32 = (new_pd_upper as u32)
                    | ((u32::from(LP_NORMAL)) << 15)
                    | ((tuple_len as u32) << 17);
                page[pd_lower..new_pd_lower].copy_from_slice(&item_word.to_le_bytes());

                pd_lower = new_pd_lower;
                pd_upper = new_pd_upper;
            }
        }
    }

    page[12..14].copy_from_slice(&(pd_lower as u16).to_le_bytes());
    page[14..16].copy_from_slice(&(pd_upper as u16).to_le_bytes());
    page[16..18].copy_from_slice(&(pd_special as u16).to_le_bytes());
    page[18..20].copy_from_slice(&((PG_BLCKSZ as u16) | PG_PAGE_LAYOUT_VERSION).to_le_bytes());

    Ok(page)
}

// ---------------------------------------------------------------------------
// Result serialization (`RecordBatch -> serde_json::Value / pgrx::JsonB`)
// ---------------------------------------------------------------------------

#[inline]
fn int_to_json<T>(array: &arrow_array::PrimitiveArray<T>, row_idx: usize) -> serde_json::Value
where
    T: arrow_array::types::ArrowPrimitiveType,
    T::Native: Into<serde_json::Number>,
{
    if array.is_null(row_idx) {
        serde_json::Value::Null
    } else {
        serde_json::Value::Number(array.value(row_idx).into())
    }
}

#[inline]
fn resolve_any_dict_idx(
    keys: &dyn Array,
    values_len: usize,
    row_idx: usize,
) -> PgBoltResult<Option<usize>> {
    if keys.is_null(row_idx) {
        return Ok(None);
    }
    let key_i128 = match keys.data_type() {
        DataType::Int8 => i128::from(
            keys.as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int8Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::Int16 => i128::from(
            keys.as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int16Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::Int32 => i128::from(
            keys.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int32Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::Int64 => i128::from(
            keys.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Int64Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::UInt8 => i128::from(
            keys.as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt8Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::UInt16 => i128::from(
            keys.as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt16Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::UInt32 => i128::from(
            keys.as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt32Array dictionary keys".into()))?
                .value(row_idx),
        ),
        DataType::UInt64 => i128::from(
            keys.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected UInt64Array dictionary keys".into()))?
                .value(row_idx),
        ),
        other => {
            return Err(PgBoltError::Bridge(format!(
                "unsupported dictionary key type {other:?} at row {row_idx}"
            )));
        }
    };
    let dict_idx = usize::try_from(key_i128).map_err(|_| {
        PgBoltError::Bridge(format!(
            "negative or overflowing dictionary key {key_i128} at row {row_idx}"
        ))
    })?;
    if dict_idx >= values_len {
        return Err(PgBoltError::Bridge(format!(
            "dictionary key {dict_idx} out of bounds (len={values_len}) at row {row_idx}"
        )));
    }
    Ok(Some(dict_idx))
}

#[inline]
fn dict_str_at<'a>(
    keys: &dyn Array,
    values: &'a dyn Array,
    row_idx: usize,
) -> PgBoltResult<Option<&'a str>> {
    let Some(idx) = resolve_any_dict_idx(keys, values.len(), row_idx)? else {
        return Ok(None);
    };
    if values.is_null(idx) {
        return Ok(None);
    }
    let val = match values.data_type() {
        DataType::Utf8 => values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| PgBoltError::Bridge("expected StringArray in dictionary values".into()))?
            .value(idx),
        DataType::LargeUtf8 => values
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(|| {
                PgBoltError::Bridge("expected LargeStringArray in dictionary values".into())
            })?
            .value(idx),
        DataType::Utf8View => values
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| {
                PgBoltError::Bridge("expected StringViewArray in dictionary values".into())
            })?
            .value(idx),
        other => {
            return Err(PgBoltError::Bridge(format!(
                "unsupported dictionary string value type {other:?}"
            )));
        }
    };
    Ok(Some(val))
}

#[cfg(any(feature = "pgrx", test))]
#[inline]
fn dict_bytes_at<'a>(
    keys: &dyn Array,
    values: &'a dyn Array,
    row_idx: usize,
) -> PgBoltResult<Option<&'a [u8]>> {
    let Some(idx) = resolve_any_dict_idx(keys, values.len(), row_idx)? else {
        return Ok(None);
    };
    if values.is_null(idx) {
        return Ok(None);
    }
    let val = match values.data_type() {
        DataType::Binary => values
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| PgBoltError::Bridge("expected BinaryArray in dictionary values".into()))?
            .value(idx),
        DataType::LargeBinary => values
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                PgBoltError::Bridge("expected LargeBinaryArray in dictionary values".into())
            })?
            .value(idx),
        DataType::BinaryView => values
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .ok_or_else(|| {
                PgBoltError::Bridge("expected BinaryViewArray in dictionary values".into())
            })?
            .value(idx),
        other => {
            return Err(PgBoltError::Bridge(format!(
                "unsupported dictionary binary value type {other:?}"
            )));
        }
    };
    Ok(Some(val))
}

#[inline]
fn float64_to_json(v: f64) -> serde_json::Value {
    if v.is_nan() {
        serde_json::Value::String("NaN".to_owned())
    } else if v.is_infinite() {
        serde_json::Value::String(if v.is_sign_positive() {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        })
    } else {
        serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

#[inline]
fn format_decimal128_json(raw: i128, scale: i8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    if scale < 0 {
        let abs = raw.unsigned_abs();
        if abs == 0 {
            return "0".to_owned();
        }
        let mut s = String::new();
        if raw < 0 {
            s.push('-');
        }
        s.push_str(&abs.to_string());
        s.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
        return s;
    }
    let sc = scale as usize;
    let digits = raw.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + sc + 2);
    if raw < 0 {
        out.push('-');
    }
    if digits.len() <= sc {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', sc - digits.len()));
        out.push_str(&digits);
    } else {
        let split = digits.len() - sc;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

#[inline]
fn format_decimal256_json(raw: i256, scale: i8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let s = raw.to_string();
    let (is_neg, digits) = if let Some(stripped) = s.strip_prefix('-') {
        (true, stripped)
    } else {
        (false, s.as_str())
    };
    if digits == "0" {
        return "0".to_owned();
    }
    if scale < 0 {
        let mut out = String::with_capacity(s.len() + scale.unsigned_abs() as usize);
        out.push_str(&s);
        out.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
        return out;
    }
    let sc = scale as usize;
    let mut out = String::with_capacity(digits.len() + sc + 2);
    if is_neg {
        out.push('-');
    }
    if digits.len() <= sc {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', sc - digits.len()));
        out.push_str(digits);
    } else {
        let split = digits.len() - sc;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

/// Converts Unix-epoch days (`1970-01-01 = 0`) to proleptic Gregorian `(year, month, day)`.
#[inline]
fn unix_days_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    (year, m as u32, d as u32)
}

#[inline]
fn push_formatted_ymd(out: &mut String, year: i64, month: u32, day: u32) {
    use std::fmt::Write as _;
    if year < 0 {
        let _ = write!(out, "-{:04}-{month:02}-{day:02}", year.unsigned_abs());
    } else if year <= 9999 {
        let _ = write!(out, "{year:04}-{month:02}-{day:02}");
    } else {
        let _ = write!(out, "+{year}-{month:02}-{day:02}");
    }
}

#[inline]
fn push_subsec_autosi(out: &mut String, sub_ns: u32) {
    use std::fmt::Write as _;
    if sub_ns == 0 {
        return;
    }
    if sub_ns % 1_000_000 == 0 {
        let _ = write!(out, ".{:03}", sub_ns / 1_000_000);
    } else if sub_ns % 1_000 == 0 {
        let _ = write!(out, ".{:06}", sub_ns / 1_000);
    } else {
        let _ = write!(out, ".{sub_ns:09}");
    }
}

fn format_date32_json(unix_days: i32) -> String {
    let (year, month, day) = unix_days_to_ymd(i64::from(unix_days));
    let mut s = String::with_capacity(12);
    push_formatted_ymd(&mut s, year, month, day);
    s
}

fn format_time_of_day_ns(ns: i128) -> PgBoltResult<String> {
    use std::fmt::Write as _;
    const MAX_DAY_NS: i128 = 86_400_000_000_000;
    if !(0..=MAX_DAY_NS).contains(&ns) {
        return Err(PgBoltError::Bridge(format!(
            "Arrow Time value {ns} ns out of range [0, {MAX_DAY_NS}]"
        )));
    }
    let total_secs = (ns / 1_000_000_000) as u32;
    let sub_ns = (ns % 1_000_000_000) as u32;
    let hh = total_secs / 3600;
    let mm = (total_secs % 3600) / 60;
    let ss = total_secs % 60;
    let mut s = String::with_capacity(20);
    let _ = write!(s, "{hh:02}:{mm:02}:{ss:02}");
    push_subsec_autosi(&mut s, sub_ns);
    Ok(s)
}

/// Parses UTC aliases (`"UTC"`, `"Etc/UTC"`, `"GMT"`, `"Z"`) and fixed numeric offsets
/// (`+00:00`, `-05:00`, `+0530`, `+02`) into seconds East of UTC.
fn parse_timestamp_tz_offset_secs(tz: &str) -> Option<(i64, bool)> {
    if tz.eq_ignore_ascii_case("UTC")
        || tz.eq_ignore_ascii_case("Etc/UTC")
        || tz.eq_ignore_ascii_case("GMT")
        || tz.eq_ignore_ascii_case("Z")
    {
        return Some((0, true));
    }
    let bytes = tz.as_bytes();
    if bytes.is_empty() || (bytes[0] != b'+' && bytes[0] != b'-') {
        return None;
    }
    let sign = if bytes[0] == b'-' { -1_i64 } else { 1_i64 };
    let rest = &tz[1..];
    let (hh, mm): (i64, i64) = if let Some((h_str, m_str)) = rest.split_once(':') {
        if h_str.len() != 2 || m_str.len() != 2 {
            return None;
        }
        (h_str.parse().ok()?, m_str.parse().ok()?)
    } else if rest.len() == 4 {
        (rest[..2].parse().ok()?, rest[2..].parse().ok()?)
    } else if rest.len() == 2 {
        (rest.parse().ok()?, 0)
    } else {
        return None;
    };
    if !(0..=23).contains(&hh) || !(0..=59).contains(&mm) {
        return None;
    }
    let secs = sign * (hh * 3600 + mm * 60);
    Some((secs, secs == 0))
}

fn format_timestamp_ns(unix_ns: i128, offset: Option<(i64, bool)>) -> String {
    use std::fmt::Write as _;
    let (shifted_ns, suffix) = match offset {
        None => (unix_ns, String::new()),
        Some((_, true)) => (unix_ns, "Z".to_owned()),
        Some((off_secs, false)) => {
            let shifted = unix_ns + i128::from(off_secs) * 1_000_000_000;
            let sign = if off_secs < 0 { '-' } else { '+' };
            let abs = off_secs.unsigned_abs();
            let hh = abs / 3600;
            let mm = (abs % 3600) / 60;
            (shifted, format!("{sign}{hh:02}:{mm:02}"))
        }
    };
    let total_secs = shifted_ns.div_euclid(1_000_000_000);
    let sub_ns = shifted_ns.rem_euclid(1_000_000_000) as u32;
    let days = total_secs.div_euclid(86_400) as i64;
    let sod = total_secs.rem_euclid(86_400) as u32;
    let (year, month, day) = unix_days_to_ymd(days);
    let hh = sod / 3600;
    let mm = (sod % 3600) / 60;
    let ss = sod % 60;

    let mut out = String::with_capacity(32);
    push_formatted_ymd(&mut out, year, month, day);
    let _ = write!(out, "T{hh:02}:{mm:02}:{ss:02}");
    push_subsec_autosi(&mut out, sub_ns);
    out.push_str(&suffix);
    out
}

#[inline]
fn encode_pg_bytea_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for &b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[inline]
fn format_uuid_hyphenated(bytes: &[u8]) -> PgBoltResult<String> {
    use std::fmt::Write as _;
    if bytes.len() != 16 {
        return Err(PgBoltError::Bridge(format!(
            "expected 16-byte UUID payload, got {} bytes",
            bytes.len()
        )));
    }
    let mut out = String::with_capacity(36);
    for (idx, &b) in bytes.iter().enumerate() {
        if matches!(idx, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{b:02x}");
    }
    Ok(out)
}

#[derive(Clone, Copy)]
enum TsUnitScale {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TsUnitScale {
    #[cfg(feature = "pgrx")]
    #[inline]
    fn from_time_unit(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }

    #[inline]
    fn to_ns(self, raw: i64) -> i128 {
        match self {
            Self::Second => i128::from(raw) * 1_000_000_000,
            Self::Millisecond => i128::from(raw) * 1_000_000,
            Self::Microsecond => i128::from(raw) * 1_000,
            Self::Nanosecond => i128::from(raw),
        }
    }
}

enum OwnedColumnJsonExtractor {
    Boolean(BooleanArray),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
    BinaryView(BinaryViewArray),
    FixedSizeBinary {
        array: FixedSizeBinaryArray,
        byte_width: i32,
    },
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Decimal128 {
        array: Decimal128Array,
        scale: i8,
    },
    Decimal256 {
        array: Decimal256Array,
        scale: i8,
    },
    Date32(Date32Array),
    Time32Second(Time32SecondArray),
    Time32Millisecond(Time32MillisecondArray),
    Time64Microsecond(Time64MicrosecondArray),
    Time64Nanosecond(Time64NanosecondArray),
    TimestampSecond {
        array: TimestampSecondArray,
        offset: Option<(i64, bool)>,
    },
    TimestampMillisecond {
        array: TimestampMillisecondArray,
        offset: Option<(i64, bool)>,
    },
    TimestampMicrosecond {
        array: TimestampMicrosecondArray,
        offset: Option<(i64, bool)>,
    },
    TimestampNanosecond {
        array: TimestampNanosecondArray,
        offset: Option<(i64, bool)>,
    },
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Utf8View(StringViewArray),
    DictString {
        keys: ArrayRef,
        values: ArrayRef,
    },
    Formatted(ArrayRef),
}

impl OwnedColumnJsonExtractor {
    fn try_new(col: &ArrayRef) -> PgBoltResult<Self> {
        let array = col.as_ref();
        let any = array.as_any();
        if let Some(a) = any.downcast_ref::<BooleanArray>() {
            return Ok(Self::Boolean(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<BinaryArray>() {
            return Ok(Self::Binary(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<LargeBinaryArray>() {
            return Ok(Self::LargeBinary(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
            return Ok(Self::BinaryView(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<FixedSizeBinaryArray>() {
            return Ok(Self::FixedSizeBinary {
                array: a.clone(),
                byte_width: a.value_length(),
            });
        }
        if let Some(a) = any.downcast_ref::<Int8Array>() {
            return Ok(Self::Int8(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Int16Array>() {
            return Ok(Self::Int16(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Int32Array>() {
            return Ok(Self::Int32(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<UInt8Array>() {
            return Ok(Self::UInt8(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<UInt16Array>() {
            return Ok(Self::UInt16(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<UInt32Array>() {
            return Ok(Self::UInt32(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<UInt64Array>() {
            return Ok(Self::UInt64(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Float32Array>() {
            return Ok(Self::Float32(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Float64Array>() {
            return Ok(Self::Float64(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Decimal128Array>() {
            let DataType::Decimal128(_, scale) = a.data_type() else {
                return Err(PgBoltError::Bridge("unexpected Decimal128 DataType".into()));
            };
            return Ok(Self::Decimal128 {
                array: a.clone(),
                scale: *scale,
            });
        }
        if let Some(a) = any.downcast_ref::<Decimal256Array>() {
            let DataType::Decimal256(_, scale) = a.data_type() else {
                return Err(PgBoltError::Bridge("unexpected Decimal256 DataType".into()));
            };
            return Ok(Self::Decimal256 {
                array: a.clone(),
                scale: *scale,
            });
        }
        if let Some(a) = any.downcast_ref::<Date32Array>() {
            return Ok(Self::Date32(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Time32SecondArray>() {
            return Ok(Self::Time32Second(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Time32MillisecondArray>() {
            return Ok(Self::Time32Millisecond(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Time64MicrosecondArray>() {
            return Ok(Self::Time64Microsecond(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<Time64NanosecondArray>() {
            return Ok(Self::Time64Nanosecond(a.clone()));
        }
        if let DataType::Timestamp(unit, tz) = array.data_type() {
            let parsed_offset = match tz.as_deref() {
                None => Some(None),
                Some(tz_str) => parse_timestamp_tz_offset_secs(tz_str).map(Some),
            };
            if let Some(offset) = parsed_offset {
                return match unit {
                    TimeUnit::Second => {
                        let a = any.downcast_ref::<TimestampSecondArray>().ok_or_else(|| {
                            PgBoltError::Bridge("expected TimestampSecondArray".into())
                        })?;
                        Ok(Self::TimestampSecond {
                            array: a.clone(),
                            offset,
                        })
                    }
                    TimeUnit::Millisecond => {
                        let a =
                            any.downcast_ref::<TimestampMillisecondArray>()
                                .ok_or_else(|| {
                                    PgBoltError::Bridge("expected TimestampMillisecondArray".into())
                                })?;
                        Ok(Self::TimestampMillisecond {
                            array: a.clone(),
                            offset,
                        })
                    }
                    TimeUnit::Microsecond => {
                        let a =
                            any.downcast_ref::<TimestampMicrosecondArray>()
                                .ok_or_else(|| {
                                    PgBoltError::Bridge("expected TimestampMicrosecondArray".into())
                                })?;
                        Ok(Self::TimestampMicrosecond {
                            array: a.clone(),
                            offset,
                        })
                    }
                    TimeUnit::Nanosecond => {
                        let a =
                            any.downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| {
                                    PgBoltError::Bridge("expected TimestampNanosecondArray".into())
                                })?;
                        Ok(Self::TimestampNanosecond {
                            array: a.clone(),
                            offset,
                        })
                    }
                };
            }
        }
        if let Some(a) = any.downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            return Ok(Self::LargeUtf8(a.clone()));
        }
        if let Some(a) = any.downcast_ref::<StringViewArray>() {
            return Ok(Self::Utf8View(a.clone()));
        }
        if let Some(dict) = array.as_any_dictionary_opt() {
            if matches!(
                dict.values().data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Ok(Self::DictString {
                    keys: make_array(dict.keys().to_data()),
                    values: dict.values().clone(),
                });
            }
        }

        // Validate upfront that `ArrayFormatter` accepts this exotic Arrow type.
        ArrayFormatter::try_new(array, &FormatOptions::default()).map_err(|e| {
            PgBoltError::Bridge(format!(
                "unsupported Arrow array type {:?} for JSON serialization: {e}",
                array.data_type()
            ))
        })?;
        Ok(Self::Formatted(Arc::clone(col)))
    }

    fn value_at(&self, row_idx: usize) -> PgBoltResult<serde_json::Value> {
        let val = match self {
            Self::Boolean(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::Bool(a.value(row_idx))
                }
            }
            Self::Binary(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(encode_pg_bytea_hex(a.value(row_idx)))
                }
            }
            Self::LargeBinary(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(encode_pg_bytea_hex(a.value(row_idx)))
                }
            }
            Self::BinaryView(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(encode_pg_bytea_hex(a.value(row_idx)))
                }
            }
            Self::FixedSizeBinary { array, byte_width } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else if *byte_width == 16 {
                    serde_json::Value::String(format_uuid_hyphenated(array.value(row_idx))?)
                } else {
                    serde_json::Value::String(encode_pg_bytea_hex(array.value(row_idx)))
                }
            }
            Self::Int8(a) => int_to_json(a, row_idx),
            Self::Int16(a) => int_to_json(a, row_idx),
            Self::Int32(a) => int_to_json(a, row_idx),
            Self::Int64(a) => int_to_json(a, row_idx),
            Self::UInt8(a) => int_to_json(a, row_idx),
            Self::UInt16(a) => int_to_json(a, row_idx),
            Self::UInt32(a) => int_to_json(a, row_idx),
            Self::UInt64(a) => int_to_json(a, row_idx),
            Self::Float32(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    float64_to_json(f64::from(a.value(row_idx)))
                }
            }
            Self::Float64(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    float64_to_json(a.value(row_idx))
                }
            }
            Self::Decimal128 { array, scale } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_decimal128_json(array.value(row_idx), *scale))
                }
            }
            Self::Decimal256 { array, scale } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_decimal256_json(array.value(row_idx), *scale))
                }
            }
            Self::Date32(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_date32_json(a.value(row_idx)))
                }
            }
            Self::Time32Second(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_time_of_day_ns(
                        TsUnitScale::Second.to_ns(i64::from(a.value(row_idx))),
                    )?)
                }
            }
            Self::Time32Millisecond(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_time_of_day_ns(
                        TsUnitScale::Millisecond.to_ns(i64::from(a.value(row_idx))),
                    )?)
                }
            }
            Self::Time64Microsecond(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_time_of_day_ns(
                        TsUnitScale::Microsecond.to_ns(a.value(row_idx)),
                    )?)
                }
            }
            Self::Time64Nanosecond(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_time_of_day_ns(
                        TsUnitScale::Nanosecond.to_ns(a.value(row_idx)),
                    )?)
                }
            }
            Self::TimestampSecond { array, offset } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_timestamp_ns(
                        TsUnitScale::Second.to_ns(array.value(row_idx)),
                        *offset,
                    ))
                }
            }
            Self::TimestampMillisecond { array, offset } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_timestamp_ns(
                        TsUnitScale::Millisecond.to_ns(array.value(row_idx)),
                        *offset,
                    ))
                }
            }
            Self::TimestampMicrosecond { array, offset } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_timestamp_ns(
                        TsUnitScale::Microsecond.to_ns(array.value(row_idx)),
                        *offset,
                    ))
                }
            }
            Self::TimestampNanosecond { array, offset } => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(format_timestamp_ns(
                        TsUnitScale::Nanosecond.to_ns(array.value(row_idx)),
                        *offset,
                    ))
                }
            }
            Self::Utf8(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(a.value(row_idx).to_owned())
                }
            }
            Self::LargeUtf8(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(a.value(row_idx).to_owned())
                }
            }
            Self::Utf8View(a) => {
                if a.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(a.value(row_idx).to_owned())
                }
            }
            Self::DictString { keys, values } => {
                match dict_str_at(keys.as_ref(), values.as_ref(), row_idx)? {
                    Some(s) => serde_json::Value::String(s.to_owned()),
                    None => serde_json::Value::Null,
                }
            }
            Self::Formatted(array) => {
                if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    let formatter =
                        ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
                            .map_err(|e| {
                                PgBoltError::Bridge(format!(
                                    "failed to initialize Arrow formatter at row {row_idx}: {e}"
                                ))
                            })?;
                    let formatted = formatter.value(row_idx).try_to_string().map_err(|e| {
                        PgBoltError::Bridge(format!(
                            "failed to format Arrow cell at row {row_idx}: {e}"
                        ))
                    })?;
                    serde_json::Value::String(formatted)
                }
            }
        };
        Ok(val)
    }
}

/// Validates that a JSON value (recursively across strings, arrays, and object keys) contains
/// no interior NUL (`'\0'`) byte, which PostgreSQL `jsonb` rejects.
pub fn validate_json_value_no_nul(
    col_name: &str,
    row_idx: usize,
    value: &serde_json::Value,
) -> PgBoltResult<()> {
    match value {
        serde_json::Value::String(s) => {
            if s.contains('\0') {
                return Err(PgBoltError::Bridge(format!(
                    "column '{col_name}' at row {row_idx} contains interior NUL (\\u0000) byte rejected by PostgreSQL jsonb"
                )));
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                validate_json_value_no_nul(col_name, row_idx, item)?;
            }
        }
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                if k.contains('\0') {
                    return Err(PgBoltError::Bridge(format!(
                        "JSONB key '{k}' in column '{col_name}' at row {row_idx} contains forbidden NUL (\\u0000) byte"
                    )));
                }
                validate_json_value_no_nul(k, row_idx, v)?;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(())
}

/// Lazy streaming row iterator over an Arrow `RecordBatch` yielding `serde_json::Value::Object`s
/// one row at a time without materializing a full `Vec<serde_json::Value>`.
pub struct RecordBatchJsonIter {
    field_names: Vec<String>,
    extractors: Vec<OwnedColumnJsonExtractor>,
    next_row: usize,
    num_rows: usize,
}

impl RecordBatchJsonIter {
    fn try_new(batch: &RecordBatch) -> PgBoltResult<Self> {
        let num_rows = batch.num_rows();
        let schema = batch.schema();
        let field_names = schema.fields().iter().map(|f| f.name().clone()).collect();
        let extractors = batch
            .columns()
            .iter()
            .map(OwnedColumnJsonExtractor::try_new)
            .collect::<PgBoltResult<Vec<_>>>()?;
        Ok(Self {
            field_names,
            extractors,
            next_row: 0,
            num_rows,
        })
    }
}

impl Iterator for RecordBatchJsonIter {
    type Item = PgBoltResult<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row >= self.num_rows {
            return None;
        }
        let row_idx = self.next_row;
        self.next_row += 1;

        let mut obj = serde_json::Map::with_capacity(self.extractors.len());
        for (name, extractor) in self.field_names.iter().zip(self.extractors.iter()) {
            match extractor.value_at(row_idx) {
                Ok(v) => {
                    obj.insert(name.clone(), v);
                }
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(serde_json::Value::Object(obj)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.num_rows.saturating_sub(self.next_row);
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for RecordBatchJsonIter {}

/// Creates a lazy `'static` streaming iterator yielding one `serde_json::Value::Object` per row.
pub fn record_batch_to_json_iter(batch: &RecordBatch) -> PgBoltResult<RecordBatchJsonIter> {
    RecordBatchJsonIter::try_new(batch)
}

/// Converts an Arrow `RecordBatch` into row-oriented `serde_json::Value::Object` values.
///
/// Uses fast typed extraction for primitives, strings, `Dictionary<Int32, Utf8>`, `Binary` (`\x...`),
/// `Time64`, `FixedSizeBinary(16)` UUID, `Decimal128`, `Date32`, and `Timestamp` (normalizing
/// `"UTC"` / `"Etc/UTC"` / `"+00:00"` cleanly without `chrono-tz`), safely encoding float
/// `NaN` / `±Infinity` as strings.
pub fn record_batch_to_json_rows(batch: &RecordBatch) -> PgBoltResult<Vec<serde_json::Value>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    record_batch_to_json_iter(batch)?.collect()
}

/// Lazy streaming row iterator yielding `pgrx::JsonB` values while guarding against
/// interior NUL (`\u0000`) bytes rejected by PostgreSQL `jsonb`.
#[cfg(feature = "pgrx")]
pub struct RecordBatchJsonbIter {
    inner: RecordBatchJsonIter,
    row_idx: usize,
}

#[cfg(feature = "pgrx")]
impl Iterator for RecordBatchJsonbIter {
    type Item = PgBoltResult<pgrx::JsonB>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_row = self.inner.next()?;
        let row_idx = self.row_idx;
        self.row_idx += 1;
        match next_row {
            Ok(val) => {
                if let Err(e) = validate_json_value_no_nul("row", row_idx, &val) {
                    return Some(Err(e));
                }
                Some(Ok(pgrx::JsonB(val)))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(feature = "pgrx")]
impl ExactSizeIterator for RecordBatchJsonbIter {}

/// Creates a lazy `'static` streaming iterator yielding `pgrx::JsonB` rows from `batch`,
/// rejecting any string or object key containing an interior NUL (`'\0'`) byte.
#[cfg(feature = "pgrx")]
pub fn record_batch_to_pgrx_jsonb_iter(batch: &RecordBatch) -> PgBoltResult<RecordBatchJsonbIter> {
    Ok(RecordBatchJsonbIter {
        inner: record_batch_to_json_iter(batch)?,
        row_idx: 0,
    })
}

/// Converts an Arrow `RecordBatch` into PostgreSQL `jsonb` row values via `pgrx::JsonB`,
/// rejecting any string or key containing an interior NUL (`'\0'`) byte.
#[cfg(feature = "pgrx")]
pub fn record_batch_to_pgrx_jsonb(batch: &RecordBatch) -> PgBoltResult<Vec<pgrx::JsonB>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    record_batch_to_pgrx_jsonb_iter(batch)?.collect()
}

#[cfg(feature = "pgrx")]
fn str_to_pg_numeric_datum(
    text: &str,
    source_kind: &str,
) -> PgBoltResult<Option<pgrx::pg_sys::Datum>> {
    use pgrx::datum::{AnyNumeric, IntoDatum};

    let num: AnyNumeric = text.parse().map_err(|e| {
        PgBoltError::Bridge(format!(
            "failed to parse {source_kind} '{text}' as PostgreSQL NUMERIC: {e:?}"
        ))
    })?;
    Ok(num.into_datum())
}

#[cfg(feature = "pgrx")]
fn float_to_pg_numeric_datum(val: PgFloatInput) -> PgBoltResult<Option<pgrx::pg_sys::Datum>> {
    let text = format_finite_float_for_numeric(val)?;
    str_to_pg_numeric_datum(&text, "float")
}

/// Converts one cell of an Arrow `RecordBatch` into a PostgreSQL `Datum` for `CustomScan` / slot emission.
#[cfg(feature = "pgrx")]
pub fn arrow_cell_to_pg_datum(
    col: &dyn Array,
    row_idx: usize,
    pg_type_oid: pgrx::pg_sys::Oid,
) -> PgBoltResult<Option<pgrx::pg_sys::Datum>> {
    use pgrx::datum::{IntoDatum, Time, Uuid};
    use pgrx::pg_sys;

    if row_idx >= col.len() {
        return Err(PgBoltError::Bridge(format!(
            "row_idx {row_idx} out of bounds for Arrow array len {}",
            col.len()
        )));
    }
    if col.is_null(row_idx) {
        return Ok(None);
    }

    match col.data_type() {
        DataType::Boolean => {
            if pg_type_oid != pg_sys::BOOLOID {
                return Err(PgBoltError::Bridge(format!(
                    "Boolean column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| PgBoltError::Bridge("expected BooleanArray".into()))?;
            Ok(arr.value(row_idx).into_datum())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            if pg_type_oid != pg_sys::BYTEAOID {
                return Err(PgBoltError::Bridge(format!(
                    "Binary column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let bytes = match col.data_type() {
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected BinaryArray".into()))?
                    .value(row_idx),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected LargeBinaryArray".into()))?
                    .value(row_idx),
                _ => col
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected BinaryViewArray".into()))?
                    .value(row_idx),
            };
            Ok(bytes.to_vec().into_datum())
        }
        DataType::FixedSizeBinary(len) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| PgBoltError::Bridge("expected FixedSizeBinaryArray".into()))?;
            let slice = arr.value(row_idx);
            if pg_type_oid == pg_sys::UUIDOID && *len == 16 {
                let bytes: [u8; 16] = slice.try_into().map_err(|_| {
                    PgBoltError::Bridge(format!("expected 16 UUID bytes, got {}", slice.len()))
                })?;
                Ok(Uuid::from_bytes(bytes).into_datum())
            } else if pg_type_oid == pg_sys::BYTEAOID {
                Ok(slice.to_vec().into_datum())
            } else {
                Err(PgBoltError::Bridge(format!(
                    "FixedSizeBinary({len}) column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )))
            }
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => match convert_int_for_oid(col, row_idx, pg_type_oid.to_u32())? {
            PgIntDatum::Int2(v) => Ok(v.into_datum()),
            PgIntDatum::Int4(v) => Ok(v.into_datum()),
            PgIntDatum::Int8(v) => Ok(v.into_datum()),
            PgIntDatum::Numeric(v) => str_to_pg_numeric_datum(&v.to_string(), "integer"),
        },
        DataType::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Float32Array".into()))?
                .value(row_idx);
            match pg_type_oid {
                pg_sys::FLOAT4OID => Ok(v.into_datum()),
                pg_sys::FLOAT8OID => Ok(f64::from(v).into_datum()),
                pg_sys::NUMERICOID => float_to_pg_numeric_datum(PgFloatInput::F32(v)),
                _ => Err(PgBoltError::Bridge(format!(
                    "Float32 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                ))),
            }
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Float64Array".into()))?
                .value(row_idx);
            match pg_type_oid {
                pg_sys::FLOAT8OID => Ok(v.into_datum()),
                pg_sys::FLOAT4OID => Ok((v as f32).into_datum()),
                pg_sys::NUMERICOID => float_to_pg_numeric_datum(PgFloatInput::F64(v)),
                _ => Err(PgBoltError::Bridge(format!(
                    "Float64 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                ))),
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if !matches!(
                pg_type_oid,
                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
            ) {
                return Err(PgBoltError::Bridge(format!(
                    "String column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let s = match col.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected StringArray".into()))?
                    .value(row_idx),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected LargeStringArray".into()))?
                    .value(row_idx),
                _ => col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected StringViewArray".into()))?
                    .value(row_idx),
            };
            if s.contains('\0') {
                return Err(PgBoltError::Bridge(format!(
                    "TEXT value at row {row_idx} contains interior NUL byte disallowed by PostgreSQL text"
                )));
            }
            Ok(s.into_datum())
        }
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            if !matches!(
                pg_type_oid,
                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID
            ) {
                return Err(PgBoltError::Bridge(format!(
                    "Dictionary string column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let dict = col
                .as_any_dictionary_opt()
                .ok_or_else(|| PgBoltError::Bridge("expected DictionaryArray".into()))?;
            let Some(s) = dict_str_at(dict.keys(), dict.values().as_ref(), row_idx)? else {
                return Ok(None);
            };
            if s.contains('\0') {
                return Err(PgBoltError::Bridge(format!(
                    "dictionary TEXT value at row {row_idx} contains interior NUL byte"
                )));
            }
            Ok(s.into_datum())
        }
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) =>
        {
            if pg_type_oid != pg_sys::BYTEAOID {
                return Err(PgBoltError::Bridge(format!(
                    "Dictionary binary column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let dict = col
                .as_any_dictionary_opt()
                .ok_or_else(|| PgBoltError::Bridge("expected DictionaryArray".into()))?;
            let Some(bytes) = dict_bytes_at(dict.keys(), dict.values().as_ref(), row_idx)? else {
                return Ok(None);
            };
            Ok(bytes.to_vec().into_datum())
        }
        DataType::Date32 => {
            if pg_type_oid != pg_sys::DATEOID {
                return Err(PgBoltError::Bridge(format!(
                    "Date32 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let unix_days = col
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Date32Array".into()))?
                .value(row_idx);
            let pg_days = unix_days_to_pg_date(unix_days)?;
            Ok(pg_days.into_datum())
        }
        DataType::Time32(unit) => {
            if pg_type_oid != pg_sys::TIMEOID {
                return Err(PgBoltError::Bridge(format!(
                    "Time32 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let pg_micros = match unit {
                TimeUnit::Second => i64::from(
                    col.as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .ok_or_else(|| PgBoltError::Bridge("expected Time32SecondArray".into()))?
                        .value(row_idx),
                )
                .checked_mul(1_000_000)
                .ok_or_else(|| PgBoltError::Bridge("Time32 second overflow".into()))?,
                TimeUnit::Millisecond => i64::from(
                    col.as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .ok_or_else(|| {
                            PgBoltError::Bridge("expected Time32MillisecondArray".into())
                        })?
                        .value(row_idx),
                )
                .checked_mul(1_000)
                .ok_or_else(|| PgBoltError::Bridge("Time32 millisecond overflow".into()))?,
                other => {
                    return Err(PgBoltError::Bridge(format!(
                        "invalid TimeUnit {other:?} for Time32"
                    )));
                }
            };
            let t = Time::try_from(pg_micros as pg_sys::TimeADT).map_err(|e| {
                PgBoltError::Bridge(format!(
                    "Time32 value {pg_micros} us invalid for PG TIME: {e:?}"
                ))
            })?;
            Ok(t.into_datum())
        }
        DataType::Time64(unit) => {
            if pg_type_oid != pg_sys::TIMEOID {
                return Err(PgBoltError::Bridge(format!(
                    "Time64 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let pg_micros = match unit {
                TimeUnit::Microsecond => col
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected Time64MicrosecondArray".into()))?
                    .value(row_idx),
                TimeUnit::Nanosecond => {
                    let raw = col
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| {
                            PgBoltError::Bridge("expected Time64NanosecondArray".into())
                        })?
                        .value(row_idx);
                    let max_nsec = PG_TIME_MAX_USEC * 1_000;
                    if !(0..=max_nsec).contains(&raw) {
                        return Err(PgBoltError::Bridge(format!(
                            "Time64 nanosecond value {raw} invalid for PG TIME [0, {max_nsec}]"
                        )));
                    }
                    raw / 1_000
                }
                other => {
                    return Err(PgBoltError::Bridge(format!(
                        "invalid TimeUnit {other:?} for Time64"
                    )));
                }
            };
            let t = Time::try_from(pg_micros as pg_sys::TimeADT).map_err(|e| {
                PgBoltError::Bridge(format!(
                    "Time64 value {pg_micros} us invalid for PG TIME: {e:?}"
                ))
            })?;
            Ok(t.into_datum())
        }
        DataType::Timestamp(unit, _) => {
            if !matches!(pg_type_oid, pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID) {
                return Err(PgBoltError::Bridge(format!(
                    "Timestamp column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let raw = match unit {
                TimeUnit::Second => col
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected TimestampSecondArray".into()))?
                    .value(row_idx),
                TimeUnit::Millisecond => col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        PgBoltError::Bridge("expected TimestampMillisecondArray".into())
                    })?
                    .value(row_idx),
                TimeUnit::Microsecond => col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        PgBoltError::Bridge("expected TimestampMicrosecondArray".into())
                    })?
                    .value(row_idx),
                TimeUnit::Nanosecond => col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| PgBoltError::Bridge("expected TimestampNanosecondArray".into()))?
                    .value(row_idx),
            };
            let unix_micros = match TsUnitScale::from_time_unit(*unit) {
                TsUnitScale::Second => raw
                    .checked_mul(1_000_000)
                    .ok_or_else(|| PgBoltError::Bridge("timestamp seconds overflow i64".into()))?,
                TsUnitScale::Millisecond => raw.checked_mul(1_000).ok_or_else(|| {
                    PgBoltError::Bridge("timestamp milliseconds overflow i64".into())
                })?,
                TsUnitScale::Microsecond => raw,
                TsUnitScale::Nanosecond => raw / 1_000,
            };
            Ok(unix_usec_to_pg_usec(unix_micros)?.into_datum())
        }
        DataType::Decimal128(_, scale) => {
            if pg_type_oid != pg_sys::NUMERICOID {
                return Err(PgBoltError::Bridge(format!(
                    "Decimal128 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let raw = col
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Decimal128Array".into()))?
                .value(row_idx);
            let text = format_decimal128_json(raw, *scale);
            str_to_pg_numeric_datum(&text, "Decimal128")
        }
        DataType::Decimal256(_, scale) => {
            if pg_type_oid != pg_sys::NUMERICOID {
                return Err(PgBoltError::Bridge(format!(
                    "Decimal256 column incompatible with PostgreSQL OID {pg_type_oid:?}"
                )));
            }
            let raw = col
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| PgBoltError::Bridge("expected Decimal256Array".into()))?
                .value(row_idx);
            let text = format_decimal256_json(raw, *scale);
            str_to_pg_numeric_datum(&text, "Decimal256")
        }
        other => Err(PgBoltError::Bridge(format!(
            "unsupported Arrow result type in PostgreSQL Datum bridge: {other}"
        ))),
    }
}

/// Writes one `RecordBatch` row directly into PostgreSQL `(tts_values, tts_isnull)` datum buffers.
#[cfg(feature = "pgrx")]
pub fn write_record_batch_row_datums(
    batch: &RecordBatch,
    row_idx: usize,
    pg_type_oids: &[pgrx::pg_sys::Oid],
    out_datums: &mut [pgrx::pg_sys::Datum],
    out_isnull: &mut [bool],
) -> PgBoltResult<()> {
    use pgrx::pg_sys;

    if row_idx >= batch.num_rows() {
        return Err(PgBoltError::Bridge(format!(
            "row_idx {row_idx} out of bounds for RecordBatch with {} rows",
            batch.num_rows()
        )));
    }
    let ncols = batch.num_columns();
    if pg_type_oids.len() != ncols || out_datums.len() != ncols || out_isnull.len() != ncols {
        return Err(PgBoltError::Bridge(format!(
            "datum buffer width mismatch: batch has {ncols} cols, oids={}, datums={}, isnull={}",
            pg_type_oids.len(),
            out_datums.len(),
            out_isnull.len()
        )));
    }

    for (((col, &oid), datum_slot), isnull_slot) in batch
        .columns()
        .iter()
        .zip(pg_type_oids.iter())
        .zip(out_datums.iter_mut())
        .zip(out_isnull.iter_mut())
    {
        match arrow_cell_to_pg_datum(col.as_ref(), row_idx, oid)? {
            Some(datum) => {
                *datum_slot = datum;
                *isnull_slot = false;
            }
            None => {
                *datum_slot = pg_sys::Datum::from(0usize);
                *isnull_slot = true;
            }
        }
    }
    Ok(())
}

/// Extracts target column PostgreSQL type OIDs once per scan from a `TupleTableSlot`'s
/// `TupleDesc` so callers can cache them across rows and avoid per-tuple `Vec` allocations.
///
/// # Safety
/// `slot` must be a non-null `TupleTableSlot` pointer with a valid `tts_tupleDescriptor`
/// whose `attrs` array covers `natts` entries.
#[cfg(feature = "pgrx")]
pub unsafe fn extract_tuple_desc_oids(
    slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> PgBoltResult<Vec<pgrx::pg_sys::Oid>> {
    if slot.is_null() {
        return Err(PgBoltError::Bridge("null TupleTableSlot passed".into()));
    }
    let tupdesc = (*slot).tts_tupleDescriptor;
    if tupdesc.is_null() {
        return Err(PgBoltError::Bridge(
            "TupleTableSlot missing tts_tupleDescriptor".into(),
        ));
    }
    let natts_i32 = (*tupdesc).natts;
    if natts_i32 < 0 {
        return Err(PgBoltError::Bridge(format!(
            "invalid TupleDesc natts={natts_i32}"
        )));
    }
    let natts = natts_i32 as usize;
    if natts == 0 {
        return Ok(Vec::new());
    }
    let attrs = (*tupdesc).attrs.as_slice(natts);
    Ok(attrs.iter().map(|a| a.atttypid).collect())
}

/// Populates a PostgreSQL `TupleTableSlot` for `row_idx` from an Arrow `RecordBatch` using
/// caller-cached target column `pg_type_oids` (zero per-tuple heap allocations).
///
/// # Safety
/// `slot` must be a non-null `TupleTableSlot` pointer whose `tts_values` and `tts_isnull`
/// arrays cover `pg_type_oids.len()` columns inside an active PostgreSQL execution memory context.
#[cfg(feature = "pgrx")]
pub unsafe fn populate_tuple_slot_with_oids(
    batch: &RecordBatch,
    row_idx: usize,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    pg_type_oids: &[pgrx::pg_sys::Oid],
) -> PgBoltResult<()> {
    if slot.is_null() {
        return Err(PgBoltError::Bridge("null TupleTableSlot passed".into()));
    }
    let ncols = pg_type_oids.len();
    if ncols != batch.num_columns() {
        return Err(PgBoltError::Bridge(format!(
            "RecordBatch has {} columns, cached slot OIDs expect {ncols}",
            batch.num_columns()
        )));
    }
    if ncols == 0 {
        if row_idx >= batch.num_rows() {
            return Err(PgBoltError::Bridge(format!(
                "row_idx {row_idx} out of bounds for RecordBatch with {} rows",
                batch.num_rows()
            )));
        }
        return Ok(());
    }
    if (*slot).tts_values.is_null() || (*slot).tts_isnull.is_null() {
        return Err(PgBoltError::Bridge(
            "TupleTableSlot missing tts_values or tts_isnull buffers".into(),
        ));
    }

    let values = std::slice::from_raw_parts_mut((*slot).tts_values, ncols);
    let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, ncols);
    write_record_batch_row_datums(batch, row_idx, pg_type_oids, values, isnull)
}

/// Populates a PostgreSQL `TupleTableSlot` for `row_idx` from an Arrow `RecordBatch`.
///
/// # Safety
/// `slot` must be a non-null `TupleTableSlot` pointer whose `tts_tupleDescriptor`,
/// `tts_values`, and `tts_isnull` arrays cover at least `natts` columns inside an
/// active PostgreSQL execution memory context.
#[cfg(feature = "pgrx")]
pub unsafe fn populate_tuple_slot_from_batch(
    batch: &RecordBatch,
    row_idx: usize,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> PgBoltResult<()> {
    let oids = extract_tuple_desc_oids(slot)?;
    populate_tuple_slot_with_oids(batch, row_idx, slot, &oids)
}

/// Populates a PostgreSQL `TupleTableSlot` for `row_idx` from an Arrow `RecordBatch`
/// and marks it as a virtual tuple via `ExecStoreVirtualTuple`.
///
/// Direct conversion helper used by `CustomScan` execution nodes (`src/custom_scan.rs`).
///
/// # Safety
/// `slot` must be a valid PostgreSQL `TupleTableSlot` pointer with allocated `tts_values`
/// and `tts_isnull` buffers inside an active backend execution memory context.
#[cfg(feature = "pgrx")]
pub unsafe fn arrow_batch_to_slot_row(
    batch: &RecordBatch,
    row_idx: usize,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> PgBoltResult<()> {
    populate_tuple_slot_from_batch(batch, row_idx, slot)?;
    pgrx::pg_sys::ExecStoreVirtualTuple(slot);
    Ok(())
}

/// Populates a PostgreSQL `TupleTableSlot` for `row_idx` from an Arrow `RecordBatch`
/// using caller-cached column OIDs and marks it as a virtual tuple via `ExecStoreVirtualTuple`.
///
/// # Safety
/// `slot` must be a valid PostgreSQL `TupleTableSlot` pointer with allocated `tts_values`
/// and `tts_isnull` buffers covering `pg_type_oids.len()` columns.
#[cfg(feature = "pgrx")]
pub unsafe fn arrow_batch_to_slot_row_with_oids(
    batch: &RecordBatch,
    row_idx: usize,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    pg_type_oids: &[pgrx::pg_sys::Oid],
) -> PgBoltResult<()> {
    populate_tuple_slot_with_oids(batch, row_idx, slot, pg_type_oids)?;
    pgrx::pg_sys::ExecStoreVirtualTuple(slot);
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::{
        HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_FROZEN,
        HEAP_XMIN_INVALID,
    };

    fn full_type_schema() -> Vec<PgColumnMeta> {
        vec![
            PgColumnMeta {
                name: "b".into(),
                type_oid: BOOLOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "s".into(),
                type_oid: INT2OID,
                nullable: true,
            },
            PgColumnMeta {
                name: "i".into(),
                type_oid: INT4OID,
                nullable: true,
            },
            PgColumnMeta {
                name: "l".into(),
                type_oid: INT8OID,
                nullable: true,
            },
            PgColumnMeta {
                name: "f".into(),
                type_oid: FLOAT4OID,
                nullable: true,
            },
            PgColumnMeta {
                name: "d".into(),
                type_oid: FLOAT8OID,
                nullable: true,
            },
            PgColumnMeta {
                name: "n".into(),
                type_oid: NUMERICOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "dt".into(),
                type_oid: DATEOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "ts".into(),
                type_oid: TIMESTAMPOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "tstz".into(),
                type_oid: TIMESTAMPTZOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "txt".into(),
                type_oid: TEXTOID,
                nullable: true,
            },
        ]
    }

    #[test]
    fn oid_mapping_covers_supported_and_rejects_unsupported() {
        assert_eq!(
            map_pg_oid_to_arrow(BOOLOID).unwrap(),
            (SupportedPgType::Bool, DataType::Boolean)
        );
        assert_eq!(
            map_pg_oid_to_arrow(BYTEAOID).unwrap(),
            (SupportedPgType::Binary, DataType::Binary)
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
            (SupportedPgType::Numeric, DataType::Decimal128(38, 10))
        );
        assert_eq!(
            map_pg_oid_to_arrow(DATEOID).unwrap(),
            (SupportedPgType::Date, DataType::Date32)
        );
        assert_eq!(
            map_pg_oid_to_arrow(TIMEOID).unwrap(),
            (
                SupportedPgType::Time,
                DataType::Time64(TimeUnit::Microsecond)
            )
        );
        assert_eq!(
            map_pg_oid_to_arrow(TIMESTAMPOID).unwrap(),
            (
                SupportedPgType::Timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, None)
            )
        );
        assert_eq!(
            map_pg_oid_to_arrow(TIMESTAMPTZOID).unwrap(),
            (
                SupportedPgType::Timestamptz,
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00")))
            )
        );
        for oid in [TEXTOID, VARCHAROID, BPCHAROID] {
            assert_eq!(
                map_pg_oid_to_arrow(oid).unwrap(),
                (SupportedPgType::Text, DataType::Utf8)
            );
        }
        assert_eq!(
            map_pg_oid_to_arrow(UUIDOID).unwrap(),
            (SupportedPgType::Uuid, DataType::FixedSizeBinary(16))
        );
        assert!(matches!(
            map_pg_oid_to_arrow(999_999),
            Err(PgBoltError::Bridge(_))
        ));
    }

    #[test]
    fn parse_decimal_str_handles_exactness_scientific_and_bounds() {
        assert_eq!(parse_decimal_str_to_i128("0", 10).unwrap(), 0);
        assert_eq!(parse_decimal_str_to_i128("-0.000000000000", 10).unwrap(), 0);
        assert_eq!(
            parse_decimal_str_to_i128("12.34", 10).unwrap(),
            123_400_000_000
        );
        assert_eq!(parse_decimal_str_to_i128("-0.0000000005", 10).unwrap(), -5);
        assert_eq!(
            parse_decimal_str_to_i128("1.23000000000000", 10).unwrap(),
            12_300_000_000
        );
        assert_eq!(parse_decimal_str_to_i128("1.5e3", 2).unwrap(), 150_000);
        assert_eq!(parse_decimal_str_to_i128("200e-2", 0).unwrap(), 2);
        assert_eq!(parse_decimal_str_to_i128("250e-2", 1).unwrap(), 25);
        assert!(parse_decimal_str_to_i128("250e-2", 0).is_err());
        assert_eq!(
            parse_decimal_str_to_i128(&i128::MIN.to_string(), 0).unwrap(),
            i128::MIN
        );
        assert!(parse_decimal_str_to_i128("1.00000000001", 10).is_err());
        assert!(parse_decimal_str_to_i128("NaN", 10).is_err());
        assert!(parse_decimal_str_to_i128("Infinity", 10).is_err());
        assert!(parse_decimal_str_to_i128("999999999999999999999999999999999", 10).is_err());
        assert!(parse_decimal_str_to_i128("", 10).is_err());
        assert!(parse_decimal_str_to_i128(".", 10).is_err());
    }

    #[test]
    fn rows_to_record_batch_all_types_and_empty() {
        let schema = full_type_schema();

        let empty = rows_to_record_batch(&schema, &[]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), schema.len());

        let rows = vec![
            vec![
                PgDatumValue::Bool(true),
                PgDatumValue::Int16(i16::MIN),
                PgDatumValue::Int32(42),
                PgDatumValue::Int64(9_000_000_000),
                PgDatumValue::Float32(1.5),
                PgDatumValue::Float64(2.25),
                PgDatumValue::Numeric("12345.6789".into()),
                PgDatumValue::DateDays(19_723), // 2024-01-01
                PgDatumValue::TimestampMicros(1_704_067_200_123_456),
                PgDatumValue::TimestampMicros(1_704_067_200_654_321),
                PgDatumValue::Text("alpha".into()),
            ],
            vec![PgDatumValue::Null; schema.len()],
        ];
        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let s_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(s_col.value(0), i32::from(i16::MIN));
        assert!(s_col.is_null(1));

        let n_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(n_col.value(0), 123_456_789_000_000);
        assert!(n_col.is_null(1));

        let dt_col = batch
            .column(7)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(dt_col.value(0), 19_723);

        let ts_col = batch
            .column(8)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1_704_067_200_123_456);

        // Zero-column non-empty batch works cleanly.
        let no_col_batch = rows_to_record_batch(&[], &[vec![], vec![]]).unwrap();
        assert_eq!(no_col_batch.num_columns(), 0);
        assert_eq!(no_col_batch.num_rows(), 2);

        // Row width mismatch fails cleanly.
        assert!(rows_to_record_batch(&schema, &[vec![PgDatumValue::Null]]).is_err());

        // Decimal128 precision (> 10^38 - 1) is rejected even though i128 can hold 1.5e38.
        let dec_schema = vec![PgColumnMeta {
            name: "x".into(),
            type_oid: NUMERICOID,
            nullable: false,
        }];
        assert!(rows_to_record_batch(
            &dec_schema,
            &[vec![PgDatumValue::Numeric("1.5e28".into())]] // 1.5e28 * 10^10 = 1.5e38 >= 10^38
        )
        .is_err());
    }

    #[test]
    fn dictionary_encoding_and_json_roundtrip() {
        let input = StringArray::from(vec![Some("us"), None, Some("eu"), Some("us")]);
        let dict = encode_dictionary_utf8(&input).unwrap();
        assert_eq!(dict.values().len(), 2);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "region",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new("metric", DataType::Float64, true),
                Field::new("amount", DataType::Decimal128(38, 10), true),
                Field::new("day", DataType::Date32, true),
            ])),
            vec![
                Arc::new(dict),
                Arc::new(Float64Array::from(vec![
                    Some(1.25),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(123_400_000_000), None, Some(-5), Some(0)])
                        .with_precision_and_scale(38, 10)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![
                    Some(0),
                    Some(19_723),
                    None,
                    Some(-1),
                ])),
            ],
        )
        .unwrap();

        let json_rows = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(json_rows.len(), 4);
        assert_eq!(json_rows[0]["region"], "us");
        assert_eq!(json_rows[0]["metric"], 1.25);
        assert_eq!(json_rows[0]["amount"], "12.3400000000");
        assert_eq!(json_rows[0]["day"], "1970-01-01");

        assert_eq!(json_rows[1]["region"], serde_json::Value::Null);
        assert_eq!(json_rows[1]["metric"], "NaN");
        assert_eq!(json_rows[1]["day"], "2024-01-01");

        assert_eq!(json_rows[2]["region"], "eu");
        assert_eq!(json_rows[2]["metric"], "Infinity");
        assert_eq!(json_rows[2]["amount"], "-0.0000000005");

        assert_eq!(json_rows[3]["metric"], "-Infinity");
        assert_eq!(json_rows[3]["day"], "1969-12-31");
    }

    #[test]
    fn heap_page_encode_decode_roundtrip_with_varlena_numeric_and_nulls() {
        let schema = full_type_schema();
        let long_text = "x".repeat(220); // forces 4-byte aligned varlena header after padding
        let hdr = HeapTupleHeaderMeta {
            t_xmin: 100,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        let row1 = vec![
            PgDatumValue::Bool(true),
            PgDatumValue::Int16(-1234),
            PgDatumValue::Int32(987_654),
            PgDatumValue::Int64(-9_876_543_210),
            PgDatumValue::Float32(3.5),
            PgDatumValue::Float64(-12.75),
            PgDatumValue::Numeric("-0.0005".into()),
            PgDatumValue::DateDays(19_723),
            PgDatumValue::TimestampMicros(1_704_067_200_123_456),
            PgDatumValue::TimestampMicros(946_684_800_000_000), // exact PG epoch 2000-01-01
            PgDatumValue::Text(long_text.clone()),
        ];
        let row2 = vec![
            PgDatumValue::Null,
            PgDatumValue::Int16(7),
            PgDatumValue::Null,
            PgDatumValue::Int64(8),
            PgDatumValue::Null,
            PgDatumValue::Float64(0.5),
            PgDatumValue::Numeric("100000000.123456".into()), // multi-group base-10000 integer + fractional
            PgDatumValue::Null,
            PgDatumValue::TimestampMicros(0), // 1970-01-01 -> negative PG epoch timestamp
            PgDatumValue::Null,
            PgDatumValue::Text("short".into()),
        ];

        let page =
            encode_test_heap_page(&schema, &[(hdr, row1.clone()), (hdr, row2.clone())]).unwrap();
        let decoded = decode_heap_page_tuples(&schema, &page).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].values, row1);
        assert_eq!(decoded[1].values, row2);

        let batch = decode_heap_pages_to_arrow(&schema, &[&page], None, None).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Exercise NumericLong (dscale > 63 and weight > 63) and scaled zero preservation.
        for sample in [
            "0.00",
            &format!("0.{}1", "0".repeat(69)),
            &format!("-1{}", "0".repeat(260)),
        ] {
            let encoded = encode_pg_numeric_bytes(sample).unwrap();
            assert_eq!(decode_pg_numeric_bytes(&encoded).unwrap(), sample);
        }
    }

    #[test]
    fn heap_page_parallel_mvcc_and_visibility_map_fast_path() {
        let schema = vec![
            PgColumnMeta {
                name: "id".into(),
                type_oid: INT4OID,
                nullable: false,
            },
            PgColumnMeta {
                name: "tag".into(),
                type_oid: TEXTOID,
                nullable: true,
            },
        ];
        let committed_hdr = HeapTupleHeaderMeta {
            t_xmin: 10,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        let aborted_hdr = HeapTupleHeaderMeta {
            t_xmin: 11,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_INVALID | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        let future_hdr = HeapTupleHeaderMeta {
            t_xmin: 250,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        let deleted_hdr = HeapTupleHeaderMeta {
            t_xmin: 12,
            t_xmax: 50,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_COMMITTED,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        let frozen_hdr = HeapTupleHeaderMeta {
            t_xmin: 2,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };

        // Build 5 pages (`>= 4` triggers `std::thread::scope` parallel path).
        let p0 = encode_test_heap_page(
            &schema,
            &[
                (
                    committed_hdr,
                    vec![PgDatumValue::Int32(1), PgDatumValue::Text("a".into())],
                ),
                (
                    aborted_hdr,
                    vec![PgDatumValue::Int32(2), PgDatumValue::Text("b".into())],
                ),
            ],
        )
        .unwrap();
        let p1 = encode_test_heap_page(
            &schema,
            &[
                (
                    future_hdr,
                    vec![PgDatumValue::Int32(3), PgDatumValue::Text("c".into())],
                ),
                (
                    deleted_hdr,
                    vec![PgDatumValue::Int32(4), PgDatumValue::Text("d".into())],
                ),
            ],
        )
        .unwrap();
        // Mark p2 all-visible in VM: snapshot check is skipped on p2 (even if xmin=250 in synthetic test, verifying VM bypass!).
        let p2 = encode_test_heap_page(
            &schema,
            &[(
                future_hdr,
                vec![PgDatumValue::Int32(5), PgDatumValue::Text("vm".into())],
            )],
        )
        .unwrap();
        let p3 = encode_test_heap_page(
            &schema,
            &[(frozen_hdr, vec![PgDatumValue::Int32(6), PgDatumValue::Null])],
        )
        .unwrap();
        let p4 = encode_test_heap_page(&schema, &[]).unwrap();

        let snapshot = MvccSnapshot::new(100, 200, vec![]).with_committed([10, 12, 50]);
        let mut vm = VisibilityMap::new(5);
        vm.set_page_flags(2, true, false);

        let pages: [&[u8]; 5] = [&p0, &p1, &p2, &p3, &p4];
        let batch =
            decode_heap_pages_to_arrow(&schema, &pages, Some(&snapshot), Some(&vm)).unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 5, 6]);
    }

    #[test]
    fn heap_page_rejects_toast_compressed_and_corrupt_pages() {
        let schema = vec![PgColumnMeta {
            name: "txt".into(),
            type_oid: TEXTOID,
            nullable: true,
        }];

        // Wrong page size
        assert!(decode_heap_page_tuples(&schema, &[0_u8; 100]).is_err());

        // Helper to craft a single-tuple page at pd_upper = 8128
        let make_single_tuple_page = |tuple_payload: &[u8]| -> [u8; PG_BLCKSZ] {
            let mut page = [0_u8; PG_BLCKSZ];
            let lp_off = 8128usize;
            let lp_len = 24 + tuple_payload.len();
            let pd_lower = 28usize;
            let pd_special = PG_BLCKSZ;
            page[12..14].copy_from_slice(&(pd_lower as u16).to_le_bytes());
            page[14..16].copy_from_slice(&(lp_off as u16).to_le_bytes());
            page[16..18].copy_from_slice(&(pd_special as u16).to_le_bytes());
            let item: u32 = (lp_off as u32) | ((LP_NORMAL as u32) << 15) | ((lp_len as u32) << 17);
            page[24..28].copy_from_slice(&item.to_le_bytes());
            // Tuple header: natts = 1, t_hoff = 24
            page[lp_off + 18..lp_off + 20].copy_from_slice(&1_u16.to_le_bytes());
            page[lp_off + 22] = 24;
            page[lp_off + 24..lp_off + 24 + tuple_payload.len()].copy_from_slice(tuple_payload);
            page
        };

        // 1B_E external TOAST pointer (`va_header = 0x01`, `va_tag = 18`).
        let mut ext_toast = [0_u8; 18];
        ext_toast[0] = 0x01;
        ext_toast[1] = 18;
        let toast_page = make_single_tuple_page(&ext_toast);
        let err = decode_heap_page_tuples(&schema, &toast_page).unwrap_err();
        assert!(err.to_string().contains("TOAST"));

        // 4B_C compressed inline varlena (`(len << 2) | 0x02`).
        let mut compressed = [0_u8; 16];
        compressed[0..4].copy_from_slice(&((16_u32 << 2) | 0x02).to_le_bytes());
        let comp_page = make_single_tuple_page(&compressed);
        let comp_err = decode_heap_page_tuples(&schema, &comp_page).unwrap_err();
        assert!(comp_err.to_string().contains("compressed"));

        // Corrupt t_hoff (misaligned)
        let mut bad_hoff = make_single_tuple_page(&[0x03]);
        bad_hoff[8128 + 22] = 25;
        assert!(decode_heap_page_tuples(&schema, &bad_hoff).is_err());

        // Invalid UTF-8
        let bad_utf8 = make_single_tuple_page(&[((3_u8) << 1) | 0x01, 0xFF, 0xFE]);
        assert!(decode_heap_page_tuples(&schema, &bad_utf8).is_err());
    }

    #[test]
    fn timestamp_timezone_formatting_without_chrono_tz() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "ts_utc",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "ts_etc_utc",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("Etc/UTC".into())),
                    true,
                ),
                Field::new(
                    "ts_offset",
                    DataType::Timestamp(TimeUnit::Second, Some("+05:30".into())),
                    true,
                ),
                Field::new(
                    "ts_naive",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(0),
                        Some(1_704_067_200_123_456),
                        None,
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_704_067_200_500), None, Some(0)])
                        .with_timezone("Etc/UTC"),
                ),
                Arc::new(
                    TimestampSecondArray::from(vec![Some(0), Some(1_704_067_200), None])
                        .with_timezone("+05:30"),
                ),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_704_067_200_000_000_789),
                    None,
                    Some(0),
                ])),
            ],
        )
        .unwrap();

        let rows = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["ts_utc"], "1970-01-01T00:00:00Z");
        assert_eq!(rows[1]["ts_utc"], "2024-01-01T00:00:00.123456Z");
        assert_eq!(rows[2]["ts_utc"], serde_json::Value::Null);

        assert_eq!(rows[0]["ts_etc_utc"], "2024-01-01T00:00:00.500Z");
        assert_eq!(rows[0]["ts_offset"], "1970-01-01T05:30:00+05:30");
        assert_eq!(rows[0]["ts_naive"], "2024-01-01T00:00:00.000000789");
        assert_eq!(rows[2]["ts_naive"], "1970-01-01T00:00:00");

        let streamed: Vec<serde_json::Value> = record_batch_to_json_iter(&batch)
            .unwrap()
            .collect::<PgBoltResult<Vec<_>>>()
            .unwrap();
        assert_eq!(streamed, rows);

        // Unknown IANA timezone names (without chrono-tz) still error explicitly rather than silently misformatting.
        let bad_tz_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("Mars/Olympus".into())),
                false,
            )])),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![0]).with_timezone("Mars/Olympus"),
            )],
        )
        .unwrap();
        assert!(record_batch_to_json_rows(&bad_tz_batch).is_err());
    }

    #[test]
    fn extended_types_bytea_time_uuid_roundtrip_and_json() {
        let schema = vec![
            PgColumnMeta {
                name: "flag".into(),
                type_oid: BOOLOID,
                nullable: false,
            },
            PgColumnMeta {
                name: "id".into(),
                type_oid: UUIDOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "t".into(),
                type_oid: TIMEOID,
                nullable: true,
            },
            PgColumnMeta {
                name: "raw".into(),
                type_oid: BYTEAOID,
                nullable: true,
            },
        ];
        let uuid_bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let time_usec = 14 * 3_600_000_000 + 30 * 60_000_000 + 5_123_456;

        let rows = vec![
            vec![
                PgDatumValue::Bool(true),
                PgDatumValue::Uuid(uuid_bytes),
                PgDatumValue::TimeMicros(time_usec),
                PgDatumValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ],
            vec![
                PgDatumValue::Bool(false),
                PgDatumValue::Null,
                PgDatumValue::TimeMicros(PG_TIME_MAX_USEC), // 24:00:00 valid in PostgreSQL TIME
                PgDatumValue::Bytes(Vec::new()),
            ],
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let hdr = HeapTupleHeaderMeta {
            t_xmin: 10,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            t_infomask2: 0,
            t_hoff: 0,
            natts: 0,
        };
        // Check heap page binary encode/decode (verifying UUID 1-byte `typalign='c'` right after Bool!).
        let page =
            encode_test_heap_page(&schema, &[(hdr, rows[0].clone()), (hdr, rows[1].clone())])
                .unwrap();
        let decoded_tuples = decode_heap_page_tuples(&schema, &page).unwrap();
        assert_eq!(decoded_tuples[0].values, rows[0]);
        assert_eq!(decoded_tuples[1].values, rows[1]);

        let page_batch = decode_heap_pages_to_arrow(&schema, &[&page], None, None).unwrap();
        let json_rows = record_batch_to_json_rows(&page_batch).unwrap();
        assert_eq!(json_rows[0]["flag"], true);
        assert_eq!(json_rows[0]["id"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(json_rows[0]["t"], "14:30:05.123456");
        assert_eq!(json_rows[0]["raw"], "\\xdeadbeef");

        assert_eq!(json_rows[1]["id"], serde_json::Value::Null);
        assert_eq!(json_rows[1]["t"], "24:00:00");
        assert_eq!(json_rows[1]["raw"], "\\x");

        // Reject out-of-range PG TIME microseconds cleanly.
        assert!(rows_to_record_batch(
            &[PgColumnMeta {
                name: "t".into(),
                type_oid: TIMEOID,
                nullable: false
            }],
            &[vec![PgDatumValue::TimeMicros(-1)]],
        )
        .is_err());
        assert!(rows_to_record_batch(
            &[PgColumnMeta {
                name: "t".into(),
                type_oid: TIMEOID,
                nullable: false
            }],
            &[vec![PgDatumValue::TimeMicros(PG_TIME_MAX_USEC + 1)]],
        )
        .is_err());
    }

    #[test]
    fn numeric_typmod_decoding_and_special_values() {
        let make_typmod = |p: i32, s: i32| -> i32 { ((p << 16) | (s & 0xFFFF)) + 4 };

        assert_eq!(decode_numeric_typmod(-1), None);
        assert_eq!(decode_numeric_typmod(0), None);
        assert_eq!(decode_numeric_typmod(3), None);
        assert_eq!(decode_numeric_typmod(make_typmod(38, 0)), Some((38, 0)));
        assert_eq!(decode_numeric_typmod(make_typmod(20, 14)), Some((20, 14)));
        assert_eq!(decode_numeric_typmod(make_typmod(10, 0)), Some((10, 0)));
        assert_eq!(decode_numeric_typmod(make_typmod(39, 10)), None);
        assert_eq!(decode_numeric_typmod(make_typmod(10, 11)), None);
        assert_eq!(decode_numeric_typmod(make_typmod(10, -2)), None);

        let specs = vec![
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "high_scale".into(),
                type_oid: NUMERICOID,
                nullable: false,
            })
            .with_typmod(make_typmod(20, 14)),
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "big_int".into(),
                type_oid: NUMERICOID,
                nullable: false,
            })
            .with_typmod(make_typmod(38, 0)),
        ];
        let rows = vec![vec![
            PgDatumValue::Numeric("0.00000000000001".into()),
            PgDatumValue::Numeric("12345678901234567890123456789012345".into()),
        ]];
        let batch = rows_to_record_batch_with_typmods(&specs, &rows).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(20, 14)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Decimal128(38, 0)
        );

        let json = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(json[0]["high_scale"], "0.00000000000001");
        assert_eq!(json[0]["big_int"], "12345678901234567890123456789012345");

        // Binary NUMERIC special values (`NaN`, `+Infinity`, `-Infinity`).
        let nan_err = decode_pg_numeric_bytes(&NUMERIC_NAN_HEADER.to_le_bytes()).unwrap_err();
        assert!(nan_err.to_string().contains("NaN"));
        let pinf_err = decode_pg_numeric_bytes(&NUMERIC_PINF_HEADER.to_le_bytes()).unwrap_err();
        assert!(pinf_err.to_string().contains("Infinity"));
        let ninf_err = decode_pg_numeric_bytes(&NUMERIC_NINF_HEADER.to_le_bytes()).unwrap_err();
        assert!(ninf_err.to_string().contains("-Infinity"));
        assert!(decode_pg_numeric_bytes(&0xE000_u16.to_le_bytes()).is_err());

        assert!(parse_decimal_str_to_i128("-Infinity", 4)
            .unwrap_err()
            .to_string()
            .contains("-Infinity"));
    }

    fn test_make_4bc(raw_len: usize, method: u32, payload: &[u8]) -> Vec<u8> {
        let total_len = 8 + payload.len();
        let va_header = ((total_len as u32) << 2) | 0x02;
        let va_tcinfo = ((method & 0x03) << 30) | ((raw_len as u32) & 0x3FFF_FFFF);
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&va_header.to_le_bytes());
        out.extend_from_slice(&va_tcinfo.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn test_make_single_tuple_page(tuple_payload: &[u8]) -> [u8; PG_BLCKSZ] {
        let mut page = [0_u8; PG_BLCKSZ];
        let lp_off = 8192 - ((24 + tuple_payload.len() + 7) & !7);
        let lp_len = 24 + tuple_payload.len();
        let pd_lower = 28usize;
        let pd_special = PG_BLCKSZ;
        page[12..14].copy_from_slice(&(pd_lower as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(lp_off as u16).to_le_bytes());
        page[16..18].copy_from_slice(&(pd_special as u16).to_le_bytes());
        let item: u32 = (lp_off as u32) | ((LP_NORMAL as u32) << 15) | ((lp_len as u32) << 17);
        page[24..28].copy_from_slice(&item.to_le_bytes());
        page[lp_off + 18..lp_off + 20].copy_from_slice(&1_u16.to_le_bytes());
        page[lp_off + 22] = 24;
        page[lp_off + 24..lp_off + 24 + tuple_payload.len()].copy_from_slice(tuple_payload);
        page
    }

    #[test]
    fn pglz_inline_4bc_decompression_toast_and_corruption_guards() {
        let make_4bc = test_make_4bc;
        let make_single_tuple_page = test_make_single_tuple_page;

        // Case 1: `"hello_hello_hello!"` (18 bytes) via PGLZ literals + overlapping backreference.
        let pglz_hello = [0x40, b'h', b'e', b'l', b'l', b'o', b'_', 0x08, 0x06, b'!'];
        assert_eq!(
            pglz_decompress(&pglz_hello, 18).unwrap(),
            b"hello_hello_hello!"
        );
        let text_schema = vec![PgColumnMeta {
            name: "txt".into(),
            type_oid: TEXTOID,
            nullable: false,
        }];
        let page1 = make_single_tuple_page(&make_4bc(18, 0, &pglz_hello));
        let tuples1 = decode_heap_page_tuples(&text_schema, &page1).unwrap();
        assert_eq!(
            tuples1[0].values,
            vec![PgDatumValue::Text("hello_hello_hello!".into())]
        );

        // Case 2: 30 x 'A' via extended match length nibble (15 -> 18 + 11 = 29, off=1).
        let pglz_rle = [0x02, b'A', 0x0F, 0x01, 11];
        assert_eq!(pglz_decompress(&pglz_rle, 30).unwrap(), vec![b'A'; 30]);
        let bin_schema = vec![PgColumnMeta {
            name: "raw".into(),
            type_oid: BYTEAOID,
            nullable: false,
        }];
        let page2 = make_single_tuple_page(&make_4bc(30, 0, &pglz_rle));
        let tuples2 = decode_heap_page_tuples(&bin_schema, &page2).unwrap();
        assert_eq!(tuples2[0].values, vec![PgDatumValue::Bytes(vec![b'A'; 30])]);

        // Corrupt PGLZ guards: truncated tag, zero offset, out-of-range offset, length mismatch, OOM rawsize bomb, LZ4 without detoaster.
        assert!(pglz_decompress(&[0x01, 0x00], 3).is_err());
        assert!(pglz_decompress(&[0x01, 0x00, 0x00], 3).is_err());
        assert!(pglz_decompress(&[0x02, b'x', 0x00, 0x05], 4).is_err());
        assert!(pglz_decompress(&[0x00, b'x'], 5).is_err());
        assert!(pglz_decompress(&[0x00, b'x'], MAX_VARLENA_RAW_BYTES).is_err());
        assert!(pglz_decompress(&[0x00, b'x'], MAX_VARLENA_RAW_BYTES + 1).is_err());

        let lz4_page = make_single_tuple_page(&make_4bc(5, 1, b"lz4!!"));
        assert!(decode_heap_page_tuples(&text_schema, &lz4_page)
            .unwrap_err()
            .to_string()
            .contains("compressed"));

        // External TOAST (1B_E) + LZ4 (4B_C method 1) work when a `ToastDetoaster` is supplied,
        // including across parallel >= 4-page Arrow decoding.
        struct MockDetoaster;
        impl ToastDetoaster for MockDetoaster {
            fn detoast(&self, raw_varlena: &[u8]) -> PgBoltResult<Vec<u8>> {
                if raw_varlena.first() == Some(&0x01) {
                    Ok(b"external_toast_ok".to_vec())
                } else {
                    Ok(b"lz4_ok".to_vec())
                }
            }
        }
        let mut ext_toast = [0_u8; 18];
        ext_toast[0] = 0x01;
        ext_toast[1] = 18;
        let toast_page = make_single_tuple_page(&ext_toast);
        let specs = vec![PgColumnSpec::from_meta(text_schema[0].clone())];
        let detoasted =
            decode_heap_page_tuples_ext(&specs, &toast_page, Some(&MockDetoaster)).unwrap();
        assert_eq!(
            detoasted[0].values,
            vec![PgDatumValue::Text("external_toast_ok".into())]
        );

        let batch = decode_heap_pages_to_arrow_with_specs(
            &specs,
            &[&toast_page, &lz4_page, &page1, &toast_page],
            None,
            None,
            Some(&MockDetoaster),
        )
        .unwrap();
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "external_toast_ok");
        assert_eq!(arr.value(1), "lz4_ok");
        assert_eq!(arr.value(2), "hello_hello_hello!");
        assert_eq!(arr.value(3), "external_toast_ok");
    }

    #[test]
    fn heap_page_hot_redirect_attisdropped_and_attmissing() {
        let write_specs = vec![
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "id".into(),
                type_oid: INT4OID,
                nullable: false,
            }),
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "dropped_txt".into(),
                type_oid: TEXTOID,
                nullable: true,
            })
            .dropped(-1, b'i'),
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "added_val".into(),
                type_oid: INT8OID,
                nullable: false,
            })
            .with_missing(PgDatumValue::Int64(42)),
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "added_opt".into(),
                type_oid: TEXTOID,
                nullable: true,
            }),
        ];

        // Reading specs clears type_oid=0 on the dropped column (`InvalidOid` like PostgreSQL does!)
        let read_specs = vec![
            write_specs[0].clone(),
            PgColumnSpec::from_meta(PgColumnMeta {
                name: "........pg.dropped.2........".into(),
                type_oid: 0,
                nullable: true,
            })
            .dropped(-1, b'i'),
            write_specs[2].clone(),
            write_specs[3].clone(),
        ];

        let root_hdr = HeapTupleHeaderMeta {
            t_xmin: 100,
            t_xmax: 0,
            t_infomask: HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID,
            t_infomask2: HEAP_ONLY_TUPLE,
            t_hoff: 0,
            natts: 0,
        };

        // Item 0: LP_REDIRECT -> OffsetNumber 2 (Item 1, HEAP_ONLY_TUPLE).
        // Item 1: LP_NORMAL with dropped_txt present (shifted alignments) AND full 4 cols.
        // Item 2: Old pre-ALTER tuple (`physical_natts = 2`) missing `added_val` -> fast default 42, `added_opt` -> Null.
        // Item 3: LP_NORMAL (`HEAP_ONLY_TUPLE`) BEFORE Item 4 redirect -> tests target-before-redirect deduplication.
        // Item 4: LP_REDIRECT -> OffsetNumber 4 (Item 3).
        let page = encode_test_heap_page_ext(
            &write_specs,
            &[
                TestHeapPageItem::Redirect(2),
                TestHeapPageItem::Normal {
                    header: root_hdr,
                    values: vec![
                        PgDatumValue::Int32(1),
                        PgDatumValue::Text("legacy_dropped_payload".into()),
                        PgDatumValue::Int64(99),
                        PgDatumValue::Text("present".into()),
                    ],
                    physical_natts: None,
                },
                TestHeapPageItem::Normal {
                    header: HeapTupleHeaderMeta {
                        t_infomask2: 0,
                        ..root_hdr
                    },
                    values: vec![PgDatumValue::Int32(2), PgDatumValue::Null],
                    physical_natts: Some(2),
                },
                TestHeapPageItem::Normal {
                    header: root_hdr,
                    values: vec![
                        PgDatumValue::Int32(3),
                        PgDatumValue::Text("dropped3".into()),
                        PgDatumValue::Int64(777),
                        PgDatumValue::Null,
                    ],
                    physical_natts: None,
                },
                TestHeapPageItem::Redirect(4),
            ],
        )
        .unwrap();

        let decoded = decode_heap_page_tuples_ext(&read_specs, &page, None).unwrap();
        // Must have exactly 3 tuples (Item 1 & Item 3 NOT double-counted!).
        assert_eq!(decoded.len(), 3);
        assert_eq!(
            decoded[0].values,
            vec![
                PgDatumValue::Int32(1),
                PgDatumValue::Int64(99),
                PgDatumValue::Text("present".into()),
            ]
        );
        assert_eq!(
            decoded[1].values,
            vec![
                PgDatumValue::Int32(2),
                PgDatumValue::Int64(42), // filled from `attmissing`!
                PgDatumValue::Null,
            ]
        );
        assert_eq!(
            decoded[2].values,
            vec![
                PgDatumValue::Int32(3),
                PgDatumValue::Int64(777),
                PgDatumValue::Null,
            ]
        );

        let batch =
            decode_heap_pages_to_arrow_with_specs(&read_specs, &[&page], None, None, None).unwrap();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 3);

        // Cycle in LP_REDIRECT fails cleanly instead of looping forever.
        let cycle_page = encode_test_heap_page_ext(
            &write_specs[..1],
            &[TestHeapPageItem::Redirect(2), TestHeapPageItem::Redirect(1)],
        )
        .unwrap();
        assert!(
            decode_heap_page_tuples_ext(&write_specs[..1], &cycle_page, None)
                .unwrap_err()
                .to_string()
                .contains("cycle")
        );

        // Out-of-range LP_REDIRECT offset fails cleanly.
        let oob_page =
            encode_test_heap_page_ext(&write_specs[..1], &[TestHeapPageItem::Redirect(99)])
                .unwrap();
        assert!(decode_heap_page_tuples_ext(&write_specs[..1], &oob_page, None).is_err());
    }

    #[test]
    fn jsonb_nul_byte_guard_and_streaming_iter() {
        let dict = encode_dictionary_utf8(&StringArray::from(vec![Some("ok"), Some("bad\0dict")]))
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("msg", DataType::Utf8, true),
                Field::new(
                    "d",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("clean"), Some("has\0nul")])),
                Arc::new(dict),
            ],
        )
        .unwrap();

        let json_rows = record_batch_to_json_rows(&batch).unwrap();
        assert!(validate_json_value_no_nul("row", 0, &json_rows[0]).is_ok());
        let err = validate_json_value_no_nul("row", 1, &json_rows[1]).unwrap_err();
        assert!(err.to_string().contains("row 1"));
        assert!(err.to_string().contains("NUL"));
    }

    #[test]
    fn decimal128_format_scale_boundaries() {
        assert_eq!(format_decimal128_json(0, 0), "0");
        assert_eq!(format_decimal128_json(0, i8::MIN), "0");
        assert_eq!(format_decimal128_json(123, -2), "12300");

        let neg_min_scale = format_decimal128_json(-42, i8::MIN);
        assert!(neg_min_scale.starts_with("-42"));
        assert_eq!(neg_min_scale.len(), 3 + 128);
        assert!(neg_min_scale[3..].bytes().all(|b| b == b'0'));

        assert_eq!(format_decimal128_json(12345, 0), "12345");
        assert_eq!(format_decimal128_json(123_456_789_012, 10), "12.3456789012");

        let scale_39 = format_decimal128_json(12345, 39);
        assert_eq!(scale_39, format!("0.{}12345", "0".repeat(34)));

        assert_eq!(
            format_decimal128_json(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            format_decimal128_json(i128::MIN, 39),
            "-0.170141183460469231731687303715884105728"
        );
        assert_eq!(
            format_decimal128_json(i128::MIN, 40),
            "-0.0170141183460469231731687303715884105728"
        );
        let min_neg_scale = format_decimal128_json(i128::MIN, i8::MIN);
        assert_eq!(
            min_neg_scale,
            format!(
                "-170141183460469231731687303715884105728{}",
                "0".repeat(128)
            )
        );

        assert!(checked_max_decimal128_magnitude(38).is_ok());
        assert!(checked_max_decimal128_magnitude(39).is_err());
        assert!(checked_max_decimal128_magnitude(0).is_err());
    }

    #[test]
    fn convert_int_widening_narrowing_and_overflow() {
        let i8_arr = Int8Array::from(vec![-128_i8, 127_i8]);
        assert_eq!(
            convert_int_for_oid(&i8_arr, 0, INT2OID).unwrap(),
            PgIntDatum::Int2(-128)
        );
        assert_eq!(
            convert_int_for_oid(&i8_arr, 1, INT4OID).unwrap(),
            PgIntDatum::Int4(127)
        );
        assert_eq!(
            convert_int_for_oid(&i8_arr, 1, INT8OID).unwrap(),
            PgIntDatum::Int8(127)
        );

        let u8_arr = UInt8Array::from(vec![0_u8, 255_u8]);
        assert_eq!(
            convert_int_for_oid(&u8_arr, 1, INT2OID).unwrap(),
            PgIntDatum::Int2(255)
        );

        let u16_arr = UInt16Array::from(vec![32_767_u16, 32_768_u16, 65_535_u16]);
        assert_eq!(
            convert_int_for_oid(&u16_arr, 0, INT2OID).unwrap(),
            PgIntDatum::Int2(32_767)
        );
        assert!(convert_int_for_oid(&u16_arr, 1, INT2OID).is_err());
        assert_eq!(
            convert_int_for_oid(&u16_arr, 2, INT4OID).unwrap(),
            PgIntDatum::Int4(65_535)
        );

        let u32_arr = UInt32Array::from(vec![2_147_483_647_u32, 2_147_483_648_u32, u32::MAX]);
        assert_eq!(
            convert_int_for_oid(&u32_arr, 0, INT4OID).unwrap(),
            PgIntDatum::Int4(2_147_483_647)
        );
        assert!(convert_int_for_oid(&u32_arr, 1, INT4OID).is_err());
        assert_eq!(
            convert_int_for_oid(&u32_arr, 2, INT8OID).unwrap(),
            PgIntDatum::Int8(i64::from(u32::MAX))
        );

        let u64_arr = UInt64Array::from(vec![i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX]);
        assert_eq!(
            convert_int_for_oid(&u64_arr, 0, INT8OID).unwrap(),
            PgIntDatum::Int8(i64::MAX)
        );
        let overflow_err = convert_int_for_oid(&u64_arr, 1, INT8OID).unwrap_err();
        assert!(overflow_err.to_string().contains("overflows INT8"));
        assert!(convert_int_for_oid(&u64_arr, 2, INT8OID).is_err());

        let i64_arr =
            Int64Array::from(vec![i64::from(i16::MAX), i64::from(i16::MAX) + 1, i64::MIN]);
        assert_eq!(
            convert_int_for_oid(&i64_arr, 0, INT2OID).unwrap(),
            PgIntDatum::Int2(i16::MAX)
        );
        assert!(convert_int_for_oid(&i64_arr, 1, INT2OID).is_err());
        assert!(convert_int_for_oid(&i64_arr, 2, INT4OID).is_err());
        assert_eq!(
            convert_int_for_oid(&i64_arr, 2, INT8OID).unwrap(),
            PgIntDatum::Int8(i64::MIN)
        );
        assert!(convert_int_for_oid(&i8_arr, 0, TEXTOID).is_err());
    }

    #[test]
    fn convert_int_and_float_for_numeric_oid() {
        let i8_arr = Int8Array::from(vec![i8::MIN, i8::MAX]);
        assert_eq!(
            convert_int_for_oid(&i8_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i8::MIN))
        );
        let i16_arr = Int16Array::from(vec![i16::MIN, i16::MAX]);
        assert_eq!(
            convert_int_for_oid(&i16_arr, 1, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i16::MAX))
        );
        let i32_arr = Int32Array::from(vec![i32::MIN, i32::MAX]);
        assert_eq!(
            convert_int_for_oid(&i32_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i32::MIN))
        );
        let i64_arr = Int64Array::from(vec![i64::MIN, 0, i64::MAX]);
        assert_eq!(
            convert_int_for_oid(&i64_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i64::MIN))
        );
        assert_eq!(
            convert_int_for_oid(&i64_arr, 1, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(0)
        );
        assert_eq!(
            convert_int_for_oid(&i64_arr, 2, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i64::MAX))
        );

        let u8_arr = UInt8Array::from(vec![u8::MAX]);
        assert_eq!(
            convert_int_for_oid(&u8_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(u8::MAX))
        );
        let u16_arr = UInt16Array::from(vec![u16::MAX]);
        assert_eq!(
            convert_int_for_oid(&u16_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(u16::MAX))
        );
        let u32_arr = UInt32Array::from(vec![u32::MAX]);
        assert_eq!(
            convert_int_for_oid(&u32_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(u32::MAX))
        );
        let u64_arr = UInt64Array::from(vec![(i64::MAX as u64) + 1, u64::MAX]);
        assert_eq!(
            convert_int_for_oid(&u64_arr, 0, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(i64::MAX) + 1)
        );
        assert_eq!(
            convert_int_for_oid(&u64_arr, 1, NUMERICOID).unwrap(),
            PgIntDatum::Numeric(i128::from(u64::MAX))
        );

        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F32(0.0)).unwrap(),
            "0"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F32(-0.0)).unwrap(),
            "0"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F32(0.1_f32)).unwrap(),
            "0.1"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F64(0.0)).unwrap(),
            "0"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F64(-0.0)).unwrap(),
            "0"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F64(123.45)).unwrap(),
            "123.45"
        );
        assert_eq!(
            format_finite_float_for_numeric(PgFloatInput::F64(-42.5)).unwrap(),
            "-42.5"
        );

        for extreme in [1e25_f64, 1e-12_f64] {
            let formatted = format_finite_float_for_numeric(PgFloatInput::F64(extreme)).unwrap();
            assert!(parse_decimal_parts(&formatted).is_ok());
        }

        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(format_finite_float_for_numeric(PgFloatInput::F64(bad)).is_err());
        }
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(format_finite_float_for_numeric(PgFloatInput::F32(bad)).is_err());
        }
    }

    #[test]
    fn dictionary_all_key_widths_and_views() {
        use arrow_array::types::{
            Int16Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        };

        let utf8_vals: ArrayRef =
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
        let large_vals: ArrayRef =
            Arc::new(LargeStringArray::from(vec![Some("l0"), None, Some("l2")]));
        let view_vals: ArrayRef = Arc::new(StringViewArray::from(&StringArray::from(vec![
            Some("v0"),
            None,
            Some("v2"),
        ])));

        let d_i8 = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(2)]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );
        let d_i16 = Arc::new(
            DictionaryArray::<Int16Type>::try_new(
                Int16Array::from(vec![Some(2), Some(0), None, Some(1)]),
                large_vals,
            )
            .unwrap(),
        );
        let d_i32 = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(2), Some(1), None]),
                view_vals,
            )
            .unwrap(),
        );
        let d_i64 = Arc::new(
            DictionaryArray::<Int64Type>::try_new(
                Int64Array::from(vec![Some(2), Some(0), None, Some(2)]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );
        let d_u8 = Arc::new(
            DictionaryArray::<UInt8Type>::try_new(
                UInt8Array::from(vec![Some(0), Some(1), Some(2), None]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );
        let d_u16 = Arc::new(
            DictionaryArray::<UInt16Type>::try_new(
                UInt16Array::from(vec![Some(2), Some(0), Some(0), Some(1)]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );
        let d_u32 = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![Some(0), None, Some(2), Some(2)]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );
        let d_u64 = Arc::new(
            DictionaryArray::<UInt64Type>::try_new(
                UInt64Array::from(vec![Some(2), Some(1), Some(0), None]),
                utf8_vals.clone(),
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("i8", d_i8.data_type().clone(), true),
                Field::new("i16", d_i16.data_type().clone(), true),
                Field::new("i32", d_i32.data_type().clone(), true),
                Field::new("i64", d_i64.data_type().clone(), true),
                Field::new("u8", d_u8.data_type().clone(), true),
                Field::new("u16", d_u16.data_type().clone(), true),
                Field::new("u32", d_u32.data_type().clone(), true),
                Field::new("u64", d_u64.data_type().clone(), true),
            ])),
            vec![d_i8, d_i16, d_i32, d_i64, d_u8, d_u16, d_u32, d_u64],
        )
        .unwrap();

        let rows = record_batch_to_json_rows(&batch).unwrap();
        assert_eq!(rows[0]["i8"], serde_json::json!("alpha"));
        assert_eq!(rows[0]["i16"], serde_json::json!("l2"));
        assert_eq!(rows[0]["i32"], serde_json::json!("v0"));
        assert_eq!(rows[0]["i64"], serde_json::json!("gamma"));
        assert_eq!(rows[0]["u8"], serde_json::json!("alpha"));
        assert_eq!(rows[0]["u16"], serde_json::json!("gamma"));
        assert_eq!(rows[0]["u32"], serde_json::json!("alpha"));
        assert_eq!(rows[0]["u64"], serde_json::json!("gamma"));

        assert_eq!(rows[1]["i8"], serde_json::Value::Null);
        assert_eq!(rows[2]["i8"], serde_json::Value::Null);
        assert_eq!(rows[1]["i32"], serde_json::json!("v2"));
        assert_eq!(rows[2]["i32"], serde_json::Value::Null);

        assert!(dict_str_at(&Int8Array::from(vec![-1_i8]), utf8_vals.as_ref(), 0).is_err());
        assert!(dict_str_at(&UInt64Array::from(vec![99_u64]), utf8_vals.as_ref(), 0).is_err());

        let bin_view = BinaryViewArray::from(&BinaryArray::from(vec![
            Some(b"\x01\x02".as_slice()),
            None,
            Some(b"\xFE\xFF".as_slice()),
        ]));
        let u16_keys = UInt16Array::from(vec![Some(0_u16), Some(1_u16), None, Some(2_u16)]);
        assert_eq!(
            dict_bytes_at(&u16_keys, &bin_view, 0).unwrap(),
            Some(b"\x01\x02".as_slice())
        );
        assert_eq!(dict_bytes_at(&u16_keys, &bin_view, 1).unwrap(), None);
        assert_eq!(dict_bytes_at(&u16_keys, &bin_view, 2).unwrap(), None);
        assert_eq!(
            dict_bytes_at(&u16_keys, &bin_view, 3).unwrap(),
            Some(b"\xFE\xFF".as_slice())
        );
    }

    #[test]
    fn lz4_4bc_offline_decompress_and_corrupt_guards() {
        let text_schema = vec![PgColumnMeta {
            name: "t".into(),
            type_oid: TEXTOID,
            nullable: false,
        }];

        // Pure literals: token = 0x50 (5 literals), followed by "hello".
        let lit_only = [0x50, b'h', b'e', b'l', b'l', b'o'];
        assert_eq!(lz4_decompress(&lit_only, 5).unwrap(), b"hello");
        let page1 = test_make_single_tuple_page(&test_make_4bc(5, 1, &lit_only));
        let tuples1 = decode_heap_page_tuples(&text_schema, &page1).unwrap();
        assert_eq!(tuples1[0].values, vec![PgDatumValue::Text("hello".into())]);

        // Overlapping match + extended match_len nibble (15 -> 4 + 15 + 0 = 19, offset = 3).
        let overlap_rle = [0x3F, b'a', b'b', b'c', 0x03, 0x00, 0x00];
        assert_eq!(
            lz4_decompress(&overlap_rle, 22).unwrap(),
            b"abcabcabcabcabcabcabca"
        );
        let page2 = test_make_single_tuple_page(&test_make_4bc(22, 1, &overlap_rle));
        let tuples2 = decode_heap_page_tuples(&text_schema, &page2).unwrap();
        assert_eq!(
            tuples2[0].values,
            vec![PgDatumValue::Text("abcabcabcabcabcabcabca".into())]
        );

        // Extended literal length (15 + 2 = 17 literals).
        let mut ext_lit = vec![0xF0, 2];
        ext_lit.extend_from_slice(&[b'Z'; 17]);
        assert_eq!(lz4_decompress(&ext_lit, 17).unwrap(), vec![b'Z'; 17]);

        // Corrupt LZ4 guards: truncated ext literal length, zero offset, OOB offset, size mismatch, > 1 GiB.
        assert!(lz4_decompress(&[0xF0], 20).is_err());
        assert!(lz4_decompress(&[0x10, b'a', 0x00, 0x00], 5).is_err());
        assert!(lz4_decompress(&[0x10, b'a', 0x05, 0x00], 5).is_err());
        assert!(lz4_decompress(&lit_only, 6).is_err());
        assert!(lz4_decompress(&[], MAX_VARLENA_RAW_BYTES + 1).is_err());
    }

    #[test]
    fn bpchar_trailing_spaces_helper() {
        let (pg_ty, arrow_ty) = map_pg_oid_to_arrow(BPCHAROID).unwrap();
        assert_eq!(pg_ty, SupportedPgType::Text);
        assert_eq!(arrow_ty, DataType::Utf8);
        assert_eq!(trim_bpchar_trailing_spaces("abc   "), "abc");
        assert_eq!(normalize_bpchar_trailing_spaces("abc   "), "abc");
        assert_eq!(trim_bpchar_trailing_spaces("   "), "");
        assert_eq!(trim_bpchar_trailing_spaces("  x "), "  x");

        let schema = Arc::new(Schema::new(vec![
            Field::new("bp", DataType::Utf8, false),
            Field::new("other", DataType::Utf8, false),
        ]));
        let col1: ArrayRef = Arc::new(StringArray::from(vec!["hello   ", "world  ", "test"]));
        let col2: ArrayRef = Arc::new(StringArray::from(vec!["keep   ", "spaces ", "intact "]));
        let batch = RecordBatch::try_new(schema, vec![col1, col2]).unwrap();

        let normalized = normalize_bpchar_batch(&batch, &[0]).unwrap();
        let bp_res = normalized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let other_res = normalized
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(bp_res.value(0), "hello");
        assert_eq!(bp_res.value(1), "world");
        assert_eq!(bp_res.value(2), "test");
        assert_eq!(other_res.value(0), "keep   ");
        assert_eq!(other_res.value(1), "spaces ");
        assert_eq!(other_res.value(2), "intact ");
    }

    #[test]
    fn numeric_unconstrained_fallback_and_dynamic_inference() {
        let make_typmod = |p: i32, s: i32| -> i32 { ((p << 16) | (s & 0xFFFF)) + 4 };

        assert_eq!(decode_raw_numeric_typmod(-1), None);
        assert_eq!(decode_raw_numeric_typmod(0), None);
        assert_eq!(decode_raw_numeric_typmod(3), None);
        assert_eq!(decode_raw_numeric_typmod(make_typmod(38, 0)), Some((38, 0)));
        assert_eq!(decode_raw_numeric_typmod(make_typmod(40, 2)), Some((40, 2)));
        assert_eq!(
            decode_raw_numeric_typmod(make_typmod(76, 10)),
            Some((76, 10))
        );
        assert_eq!(decode_raw_numeric_typmod(make_typmod(10, 11)), None);

        // parse_decimal_str_to_i256
        let i256_val = parse_decimal_str_to_i256("123.456", 3).unwrap();
        assert_eq!(i256_val, i256::from_i128(123456));
        let i256_neg = parse_decimal_str_to_i256("-0.00000000000001", 14).unwrap();
        assert_eq!(i256_neg, i256::from_i128(-1));
        assert!(parse_decimal_str_to_i256("NaN", 2).is_err());
        assert!(parse_decimal_str_to_i256("Infinity", 2).is_err());

        // parse_numeric_str_to_f64
        assert_eq!(parse_numeric_str_to_f64("123.45", "test").unwrap(), 123.45);
        assert!(parse_numeric_str_to_f64("NaN", "test").unwrap().is_nan());
        assert_eq!(
            parse_numeric_str_to_f64("Infinity", "test").unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            parse_numeric_str_to_f64("-Infinity", "test").unwrap(),
            f64::NEG_INFINITY
        );

        // infer_numeric_precision_scale
        let scale_14 = infer_numeric_precision_scale(&["0.00000000000001"])
            .unwrap()
            .unwrap();
        assert_eq!(scale_14, (14, 14));
        let int_33 = infer_numeric_precision_scale(&["100000000000000000000000000000000"])
            .unwrap()
            .unwrap();
        assert_eq!(int_33, (33, 0));
        assert!(infer_numeric_precision_scale(&["NaN"]).unwrap().is_none());
        assert!(infer_numeric_precision_scale(&["Infinity"])
            .unwrap()
            .is_none());

        // Dynamic inference for unconstrained NUMERIC with scale > 10
        let unconstrained_spec = vec![PgColumnSpec::from_meta(PgColumnMeta {
            name: "unconstrained".into(),
            type_oid: NUMERICOID,
            nullable: false,
        })];
        let rows_scale14 = vec![vec![PgDatumValue::Numeric("0.00000000000001".into())]];
        let batch_inferred =
            rows_to_record_batch_with_typmods(&unconstrained_spec, &rows_scale14).unwrap();
        assert_eq!(
            batch_inferred.schema().field(0).data_type(),
            &DataType::Decimal128(14, 14)
        );
        let json = record_batch_to_json_rows(&batch_inferred).unwrap();
        assert_eq!(json[0]["unconstrained"], "0.00000000000001");

        // Dynamic inference for unconstrained NUMERIC with magnitude >= 10^28
        let rows_bigint = vec![vec![PgDatumValue::Numeric(
            "100000000000000000000000000000000".into(),
        )]];
        let batch_big =
            rows_to_record_batch_with_typmods(&unconstrained_spec, &rows_bigint).unwrap();
        assert_eq!(
            batch_big.schema().field(0).data_type(),
            &DataType::Decimal128(33, 0)
        );

        // Fallback to Float64 on special values (NaN / Infinity) or precision > 38
        crate::guc::with_test_config(
            crate::guc::BoltConfig {
                numeric_unconstrained_fallback: NumericUnconstrainedFallback::Float64,
                ..Default::default()
            },
            || {
                let rows_nan = vec![vec![PgDatumValue::Numeric("NaN".into())]];
                let batch_nan =
                    rows_to_record_batch_with_typmods(&unconstrained_spec, &rows_nan).unwrap();
                assert_eq!(batch_nan.schema().field(0).data_type(), &DataType::Float64);
                let f_arr = batch_nan
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert!(f_arr.value(0).is_nan());
            },
        );

        // Fallback to Decimal256 on precision > 38
        crate::guc::with_test_config(
            crate::guc::BoltConfig {
                numeric_unconstrained_fallback: NumericUnconstrainedFallback::Decimal256,
                ..Default::default()
            },
            || {
                let p40_spec = vec![PgColumnSpec::from_meta(PgColumnMeta {
                    name: "p40".into(),
                    type_oid: NUMERICOID,
                    nullable: false,
                })
                .with_typmod(make_typmod(40, 2))];
                let rows_p40 = vec![vec![PgDatumValue::Numeric(
                    "12345678901234567890123456789012345678.90".into(),
                )]];
                let batch_p40 = rows_to_record_batch_with_typmods(&p40_spec, &rows_p40).unwrap();
                assert_eq!(
                    batch_p40.schema().field(0).data_type(),
                    &DataType::Decimal256(40, 2)
                );
                let json_p40 = record_batch_to_json_rows(&batch_p40).unwrap();
                assert_eq!(
                    json_p40[0]["p40"],
                    "12345678901234567890123456789012345678.90"
                );
            },
        );

        // Fallback Error rejects precision > 38
        crate::guc::with_test_config(
            crate::guc::BoltConfig {
                numeric_unconstrained_fallback: NumericUnconstrainedFallback::Error,
                ..Default::default()
            },
            || {
                let p40_spec = vec![PgColumnSpec::from_meta(PgColumnMeta {
                    name: "p40".into(),
                    type_oid: NUMERICOID,
                    nullable: false,
                })
                .with_typmod(make_typmod(40, 2))];
                assert!(rows_to_record_batch_with_typmods(&p40_spec, &[]).is_err());
            },
        );
    }

    #[test]
    fn standalone_toast_reader_and_detoast() {
        let mut reader = StandaloneToastReader::new();

        // 1. Direct chunks (uncompressed "Hello, World!")
        let val_id: u32 = 42;
        reader.add_chunk(val_id, 0, b"Hello, ".to_vec());
        reader.add_chunk(val_id, 1, b"World!".to_vec());

        // Construct 1B_E on-disk varlena header (18 bytes):
        // byte 0: 0x01
        // byte 1: 18 (VARTAG_ONDISK)
        // bytes 2..5: va_rawsize = 13 (raw size)
        // bytes 6..9: va_extsize = 13 (external size)
        // bytes 10..13: va_valueid = 42
        // bytes 14..17: va_toastrelid = 999
        let mut ondisk_hdr = vec![0u8; 18];
        ondisk_hdr[0] = 0x01;
        ondisk_hdr[1] = 18;
        ondisk_hdr[2..6].copy_from_slice(&13_u32.to_le_bytes());
        ondisk_hdr[6..10].copy_from_slice(&13_u32.to_le_bytes());
        ondisk_hdr[10..14].copy_from_slice(&val_id.to_le_bytes());
        ondisk_hdr[14..18].copy_from_slice(&999_u32.to_le_bytes());

        let detoasted = reader.detoast(&ondisk_hdr).unwrap();
        assert_eq!(detoasted, b"Hello, World!");

        // 2. Out-of-order / missing chunk check
        let mut bad_reader = StandaloneToastReader::new();
        bad_reader.add_chunk(100, 1, b"missing seq 0".to_vec());
        let mut bad_hdr = ondisk_hdr.clone();
        bad_hdr[10..14].copy_from_slice(&100_u32.to_le_bytes());
        assert!(bad_reader.detoast(&bad_hdr).is_err());

        // 3. Unknown valueid
        bad_hdr[10..14].copy_from_slice(&9999_u32.to_le_bytes());
        assert!(bad_reader.detoast(&bad_hdr).is_err());

        // 4. LZ4 compressed chunk detoast
        let lz4_val_id: u32 = 77;
        let lit_only = [0x50, b'h', b'e', b'l', b'l', b'o'];
        let lz4_4bc = test_make_4bc(5, 1, &lit_only);
        let mut lz4_reader = StandaloneToastReader::new();
        lz4_reader.add_chunk(lz4_val_id, 0, lz4_4bc.clone());

        let mut lz4_hdr = ondisk_hdr.clone();
        lz4_hdr[2..6].copy_from_slice(&5_u32.to_le_bytes());
        lz4_hdr[6..10].copy_from_slice(&(lz4_4bc.len() as u32).to_le_bytes());
        lz4_hdr[10..14].copy_from_slice(&lz4_val_id.to_le_bytes());
        let lz4_detoasted = lz4_reader.detoast(&lz4_hdr).unwrap();
        assert_eq!(lz4_detoasted, b"hello");

        // 5. PG 14+ external TOAST with compression method in va_extsize high bits (without 4B_C header)
        // va_cmid = 1 (LZ4) -> bit 30 = 0x4000_0000
        let lz4_raw_id: u32 = 88;
        let mut lz4_raw_reader = StandaloneToastReader::new();
        lz4_raw_reader.add_chunk(lz4_raw_id, 0, lit_only.to_vec());

        let mut lz4_raw_hdr = ondisk_hdr.clone();
        lz4_raw_hdr[2..6].copy_from_slice(&5_u32.to_le_bytes()); // rawsize = 5
        let ext_word_lz4: u32 = (1_u32 << 30) | (lit_only.len() as u32);
        lz4_raw_hdr[6..10].copy_from_slice(&ext_word_lz4.to_le_bytes());
        lz4_raw_hdr[10..14].copy_from_slice(&lz4_raw_id.to_le_bytes());
        let lz4_raw_detoasted = lz4_raw_reader.detoast(&lz4_raw_hdr).unwrap();
        assert_eq!(lz4_raw_detoasted, b"hello");

        // va_cmid = 0 (PGLZ) without 4B_C header
        let pglz_raw_id: u32 = 89;
        let pglz_hello = [0x40, b'h', b'e', b'l', b'l', b'o', b'_', 0x08, 0x06, b'!'];
        let mut pglz_raw_reader = StandaloneToastReader::new();
        pglz_raw_reader.add_chunk(pglz_raw_id, 0, pglz_hello.to_vec());

        let mut pglz_raw_hdr = ondisk_hdr.clone();
        pglz_raw_hdr[2..6].copy_from_slice(&18_u32.to_le_bytes()); // rawsize = 18
        let ext_word_pglz: u32 = pglz_hello.len() as u32; // va_cmid = 0
        pglz_raw_hdr[6..10].copy_from_slice(&ext_word_pglz.to_le_bytes());
        pglz_raw_hdr[10..14].copy_from_slice(&pglz_raw_id.to_le_bytes());
        let pglz_raw_detoasted = pglz_raw_reader.detoast(&pglz_raw_hdr).unwrap();
        assert_eq!(pglz_raw_detoasted, b"hello_hello_hello!");
    }

    #[test]
    fn extreme_exponents_and_negation_overflow_guards() {
        // "1e-2147483648" has exp = -2147483648 = i32::MIN.
        // Must reject cleanly without panic or negation overflow!
        assert!(parse_decimal_str_to_i128("1e-2147483648", 0).is_err());
        assert!(parse_decimal_str_to_i128("1e-2147483648", 10).is_err());
        assert!(parse_decimal_str_to_i256("1e-2147483648", 0).is_err());
        assert!(parse_decimal_str_to_i256("1e-2147483648", 10).is_err());

        // Dynamic inference handles extreme negative exponent without panic
        assert_eq!(
            infer_numeric_precision_scale(&["1e-2147483648"]).unwrap(),
            None
        );

        // Encoding extreme exponent into PG binary format rejects cleanly without panic
        assert!(encode_pg_numeric_bytes("1e-2147483648").is_err());

        // Extreme positive exponents reject without panic
        assert!(parse_decimal_str_to_i128("1e2147483647", 10).is_err());
        assert!(parse_decimal_str_to_i256("1e2147483647", 10).is_err());
    }

    #[test]
    fn decimal256_precision_enforcement() {
        crate::guc::with_test_config(
            crate::guc::BoltConfig {
                numeric_unconstrained_fallback: NumericUnconstrainedFallback::Decimal256,
                ..current_config()
            },
            || {
                let make_typmod = |p: i32, s: i32| -> i32 { ((p << 16) | (s & 0xFFFF)) + 4 };
                let spec = vec![PgColumnSpec::from_meta(PgColumnMeta {
                    name: "val".into(),
                    type_oid: NUMERICOID,
                    nullable: false,
                })
                .with_typmod(make_typmod(40, 2))];

                // 40 total digits (38 integer digits + 2 fractional digits) matches Decimal256(40, 2)
                let exact_40_val = format!("{}.90", "1".repeat(38));
                let rows_valid = vec![vec![PgDatumValue::Numeric(exact_40_val)]];
                let batch_ok = rows_to_record_batch_with_typmods(&spec, &rows_valid);
                assert!(batch_ok.is_ok());

                // 41 total digits (39 integer digits + 2 fractional digits) exceeds Decimal256(40, 2)
                let exceed_40_val = format!("{}.90", "1".repeat(39));
                let rows_invalid = vec![vec![PgDatumValue::Numeric(exceed_40_val)]];
                let err = rows_to_record_batch_with_typmods(&spec, &rows_invalid).unwrap_err();
                assert!(err
                    .to_string()
                    .contains("exceeds Decimal256(40, 2) precision"));
            },
        );
    }

    #[test]
    fn normalize_bpchar_batch_large_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "lstr",
            DataType::LargeUtf8,
            false,
        )]));
        let col: ArrayRef = Arc::new(LargeStringArray::from(vec![
            "bp_padded   ",
            "no_pad",
            "   ",
        ]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let normalized = normalize_bpchar_batch(&batch, &[0]).unwrap();
        let res = normalized
            .column(0)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(res.value(0), "bp_padded");
        assert_eq!(res.value(1), "no_pad");
        assert_eq!(res.value(2), "");
    }
}
