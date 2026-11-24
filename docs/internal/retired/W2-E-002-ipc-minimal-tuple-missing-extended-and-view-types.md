<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-E-002: `MinimalTuple` IPC codec omits `Time`, `FixedSizeBinary` (UUID), `Utf8View`/`BinaryView`, and `Dictionary<Int64, Utf8>` decode

| Field | Value |
| :--- | :--- |
| Title | `MinimalTuple` IPC codec omits `Time`, `FixedSizeBinary` (UUID), `Utf8View`/`BinaryView`, and `Dictionary<Int64, Utf8>` decode |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/ipc.rs:113-122,175-228,350-445` |

## Description
`src/ipc.rs` implements `record_batch_to_minimal_tuples` and `minimal_tuples_to_record_batch` for shared-memory `MinimalTuple` IPC transport. Several Arrow types produced by `craton-bolt` or `W1-B-006` are asymmetric or unsupported:
1. **Encode/decode asymmetry on `Dictionary<Int64, Utf8>`**: `encode_cell_into` (`src/ipc.rs:217-220`) delegates to `dictionary_string_at` (`src/ipc.rs:124-161`), which explicitly accepts `Int8..Int64` and `UInt8..UInt64` dictionary keys. However, `MinimalColBuilder::new` (`src/ipc.rs:384-389`) only accepts `DataType::Int32` keys, rejecting round-trip decoding of `Int64`-keyed dictionary columns.
2. **Missing extended & view types**: Both `encode_cell_into` (`src/ipc.rs:175-228`) and `MinimalColBuilder::new` (`src/ipc.rs:325-396`) reject `DataType::Time32(_)`, `DataType::Time64(_)` (`TIMEOID`), `DataType::FixedSizeBinary(n)` (`UUIDOID`), `DataType::Utf8View`, and `DataType::BinaryView` (also omitted from `is_varwidth_type` at lines 113-122).

## Reproduction
1. Call `record_batch_to_minimal_tuples` on a `RecordBatch` containing a `Time64(Microsecond)`, `FixedSizeBinary(16)`, or `StringViewArray` column -> fails with `PgBoltError::Ipc("unsupported Arrow DataType ... for MinimalTuple framing")`.
2. Encode a `RecordBatch` with a `Dictionary<Int64, Utf8>` column (succeeds) and call `minimal_tuples_to_record_batch` with the same schema -> fails on decode.

## Suggested fix
- Add `Time32`, `Time64`, `FixedSizeBinary(n)`, `Utf8View`, and `BinaryView` support to `is_varwidth_type`, `encode_cell_into`, and `MinimalColBuilder`.
- Generalize `MinimalColBuilder` dictionary decoding to handle `Dictionary(Int64, Utf8)` (or parameterize on `Int32`/`Int64` key types).
- Add round-trip unit tests in `src/ipc.rs` for `Time64(Microsecond)`, `FixedSizeBinary(16)`, `Utf8View`, `BinaryView`, and `Dictionary<Int64, Utf8>`.

## Resolution (Wave 3)
- Extended `is_varwidth_type`, `encode_cell_into`, and `TupleColBuilder` (`src/ipc.rs`) with symmetric support for `DataType::Time32(Second | Millisecond)`, `DataType::Time64(Microsecond | Nanosecond)`, `DataType::FixedSizeBinary(len)` (validated non-negative), `DataType::Utf8View`, `DataType::BinaryView`, and `DataType::Dictionary(Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Utf8)` via `DictUtf8Builder` / `new_dict_utf8_builder`.
- Added round-trip unit test `minimal_tuple_roundtrip_time_fixed_binary_views_and_dict_widths` in `src/ipc.rs`.
