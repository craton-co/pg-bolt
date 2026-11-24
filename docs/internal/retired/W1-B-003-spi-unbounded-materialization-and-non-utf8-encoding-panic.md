<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-003: SPI extraction materializes full result set and panics/fails on non-UTF-8 PostgreSQL server encodings

| Field | Value |
| :--- | :--- |
| Title | SPI extraction materializes full result set and panics/fails on non-UTF-8 PostgreSQL server encodings |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `src/arrow_bridge.rs:623-737`, `../bolt/PG_BOLT_PLAN_AND_CODE.md:394-447` |

## Description
`extract_relation_to_arrow` (`src/arrow_bridge.rs:623-737`) has two production scalability and correctness limitations inherited from the SPI bridge design:
1. **Unbounded SPI RAM materialization**: Calling `client.select(relation_sql, None, &[])` materializes the entire PostgreSQL result set inside SPI's `SPITupleTable` before constructing a single Arrow `RecordBatch`, doubling peak host memory on multi-GB tables and preventing streaming ingestion into `craton-bolt`.
2. **UTF-8 encoding assumption & `BPCHAR` padding**: Calling `row.get::<String>(ordinal)` invokes `pgrx`'s `FromDatum for String`, which assumes database bytes are UTF-8. Databases initialized with `SQL_ASCII`, `LATIN1`, `WIN1252`, etc. can fail/panic instead of transcoding to UTF-8 via `pg_sys::pg_database_encoding_to_utf8`. Additionally, `BPCHAROID` (`char(n)`) retains trailing space padding on Arrow strings unless handled uniformly with Bolt's SQL comparison semantics.

## Reproduction
1. Call `bolt.load_table('large_table')` on a table exceeding available backend RAM: SPI allocates the full `SPITupleTable` at once before Arrow chunking.
2. Call `bolt.load_table` in a `LATIN1`-encoded PostgreSQL database containing byte `0xE9` (`é`): `row.get::<String>` fails UTF-8 validation.

## Resolution (Wave 5)
- **Bounded chunked streaming**: Provided `extract_relation_to_arrow_batches` and `extract_relation_to_arrow_stream`, fetching in configurable cursor chunks (default 8,192 rows) and freeing each chunk's `SPITupleTable` via `SpiTupTableGuard` immediately after building the batch.
- **Robust non-UTF-8 transcoding**: In `decode_pg_text_datum`, queries under non-UTF-8 server encodings transcode to UTF-8 via `pg_server_to_any(..., PG_UTF8)`. For `SQL_ASCII` or invalid byte sequences, `String::from_utf8_lossy` provides graceful lossless/lossy conversion, completely preventing crashes or panics.
- **BPCHAR trailing blank normalization**: Provided `trim_bpchar_trailing_spaces`, `normalize_bpchar_trailing_spaces`, and `normalize_bpchar_batch` for column-level and batch-level trailing blank trimming, aligning Arrow string equality and join comparisons with PostgreSQL blank-padded semantics.
- **Unit test coverage**: Tested blank trimming and batch normalization in `bpchar_trailing_spaces_helper`.
