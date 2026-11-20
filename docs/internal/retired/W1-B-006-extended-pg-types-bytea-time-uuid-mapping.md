<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-006: Wire `BYTEAOID`, `TIMEOID`, and `UUIDOID` to `craton-bolt` `Extended` Arrow types (`Binary`, `Time64`, `FixedSizeBinary`)

| Field | Value |
| :--- | :--- |
| Title | Wire `BYTEAOID`, `TIMEOID`, and `UUIDOID` to `craton-bolt` `Extended` Arrow types (`Binary`, `Time64`, `FixedSizeBinary`) |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs:33,43,47,107-143`, `../bolt/src/plan/extended_types.rs` |

## Description
`src/arrow_bridge.rs` defines PostgreSQL OID constants `BYTEAOID = 17`, `TIMEOID = 1083`, and `UUIDOID = 2950`, and `craton-bolt` 0.12.0 supports `DataType::Binary`, `DataType::Time64(TimeUnit::Microsecond)`, and `DataType::FixedSizeBinary(16)` through its `Extended` schema bridge (`../bolt/src/plan/extended_types.rs`), yet `map_pg_oid_to_arrow` currently rejects these OIDs with `PgBoltError::Bridge`.

Supporting these types unlocks real-world PostgreSQL analytical schemas (event payloads, time-of-day windows, and UUID foreign/partition keys) without forcing users to cast columns to `text`.

## Reproduction
1. Call `map_pg_oid_to_arrow(UUIDOID)` (or `BYTEAOID` / `TIMEOID`), or load a PostgreSQL table containing a `uuid` column via `bolt.load_table`.
2. Ingestion fails with `unsupported PostgreSQL type OID 2950 (uuid)`.

## Suggested fix
- Add `Binary`, `Time`, and `Uuid` variants to `SupportedPgType` and `PgDatumValue` (`Bytes(Vec<u8>)`, `TimeMicros(i64)`, `Uuid([u8; 16])`), mapping:
  - `BYTEAOID` -> `DataType::Binary`
  - `TIMEOID` -> `DataType::Time64(TimeUnit::Microsecond)` (note: no epoch offset needed; PG `TimeADT` is microseconds since `00:00:00`)
  - `UUIDOID` -> `DataType::FixedSizeBinary(16)` (16-byte raw `attlen = 16`, `attalign = 'c'`)
- Verify `craton-bolt` planner/GPU fallback (`ExecutionTier::Host` / `Hybrid`) and `record_batch_to_json_rows` formatting (`hex` / canonical UUID `8-4-4-4-12` formatting instead of raw binary string fallback) handle all three types cleanly.
- Add unit tests for OID mapping, SPI/8KB page decoding, and JSON serialization.

## Resolution (Wave 2 Agent B)
- Wired `BYTEAOID -> DataType::Binary`, `TIMEOID -> DataType::Time64(TimeUnit::Microsecond)`, and `UUIDOID -> DataType::FixedSizeBinary(16)` across `SupportedPgType`, `PgDatumValue`, `map_pg_oid_to_arrow`, `rows_to_record_batch*`, 8KB heap page encode/decode (16-byte pass-by-reference UUID with `attalign = 'c'`), `pgrx` SPI extraction & `arrow_cell_to_pg_datum`, and JSON formatting (`\x...` hex, ISO time, canonical hyphenated lowercase UUID).
- Added unit test `extended_types_bytea_time_uuid_roundtrip_and_json` in `src/arrow_bridge.rs` (`tests/arrow_bridge_and_heap_test.rs`, `src/planner.rs`, `src/ipc.rs`, and `src/custom_scan.rs` also updated in Wave 2).
