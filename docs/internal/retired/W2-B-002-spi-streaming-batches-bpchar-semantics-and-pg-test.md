<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-B-002: Streaming multi-batch SPI extraction, `BPCHAR` trailing-space semantics, and live `pg_test` verification

| Field | Value |
| :--- | :--- |
| Title | Streaming multi-batch SPI extraction, `BPCHAR` trailing-space semantics, and live `pg_test` verification |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs`, `src/lib.rs`, `src/ipc.rs` |

## Description
Wave 2 replaced full `client.select` SPI materialization in `extract_relation_to_arrow` with bounded 8,192-row cursor chunks (`SPI_cursor_open_with_args`, `SPI_cursor_fetch`, `SPI_freetuptable`, `SPI_cursor_close`) and added safe server-to-UTF-8 transcoding (`GetDatabaseEncoding` + `pg_server_to_any`). Three follow-up items were addressed:
1. Multi-batch streaming cursor API to feed chunked IPC directly without materializing full relations in host memory.
2. `BPCHAROID` (`char(n)`) blank normalization for Bolt equality/join comparisons.
3. Transcoding for non-UTF-8 databases (`SQL_ASCII`, `LATIN1`) with lossy conversion fallback.

## Reproduction
1. Inspect `extract_relation_to_arrow` in `src/arrow_bridge.rs`: the single `builders` vector accumulated every 8,192-row SPI chunk until `finish_column_builders`.
2. Load a multi-GB relation into `craton-bolt` or compare `char(10)` keys against `varchar` keys.

## Resolution (Wave 5)
- **Streaming cursor API**: Provided `extract_relation_to_arrow_stream` and `extract_relation_to_arrow_batches` invoking an `on_batch` callback with bounded `RecordBatch` chunks (default 8,192 rows), freeing `SPITupleTable` per chunk to maintain constant memory footprint.
- **BPCHAR normalization**: Added `trim_bpchar_trailing_spaces`, `normalize_bpchar_trailing_spaces`, and `normalize_bpchar_batch` to trim trailing spaces across designated `BPCHAR` columns in an Arrow `RecordBatch`, aligning Bolt string join/hash/equality comparisons with PostgreSQL blank-padded semantics.
- **Lossless/lossy non-UTF-8 conversion**: Transcodes server encodings via `pg_server_to_any(..., PG_UTF8)` and falls back to `String::from_utf8_lossy` for unencoded `SQL_ASCII` or malformed byte sequences, completely avoiding panics or errors.
- **Unit test coverage**: Added `bpchar_trailing_spaces_helper` testing blank trimming and batch normalization in `src/arrow_bridge.rs`.
