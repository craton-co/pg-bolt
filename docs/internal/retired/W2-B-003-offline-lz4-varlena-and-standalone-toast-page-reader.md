<!-- SPDX-License-Identifier: Apache-2.0 -->

# W2-B-003: Pure-Rust offline LZ4 `4B_C` varlena decompression and standalone `pg_toast` relation page reconstruction

| Field | Value |
| :--- | :--- |
| Title | Pure-Rust offline LZ4 `4B_C` varlena decompression and standalone `pg_toast` relation page reconstruction |
| Severity | medium |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs` (`pglz_decompress`, `PgBackendToastDetoaster`, `read_varlena_payload_cow`) |

## Description
Wave 2 added pure-Rust PGLZ (`4B_C` method `0`) decompression plus the `ToastDetoaster` trait and `#[cfg(feature = "pgrx")] PgBackendToastDetoaster` (`pg_detoast_datum_packed`) for external `1B_E` TOAST pointers and compressed inline varlenas.

However, pure offline 8KB page scanning (`detoaster = None`, outside an active PostgreSQL backend) still returns a structured `PgBoltError::Bridge` for:
1. Inline `4B_C` LZ4-compressed datums (`va_tcinfo` method `1`, PostgreSQL 14+ `default_toast_compression = lz4`).
2. External `1B_E` on-disk TOAST pointers (`va_toastrelid`, `va_valueid`) when `PgBackendToastDetoaster` is unavailable.

## Reproduction
1. Create a PostgreSQL 14+ table with `SET COMPRESSION lz4`, insert a compressible >2 KB `TEXT`/`BYTEA` value (`4B_C` method `1`), and pass the raw 8KB page to `decode_heap_page_tuples` (`detoaster = None`).
2. Decoding rejects with `cannot decode inline-compressed TOAST varlena (method 1)`.

## Suggested fix
- Add an optional pure-Rust LZ4 block decompressor (e.g. `lz4_flex` with bounded `max_uncompressed_size <= MAX_VARLENA_RAW_BYTES`) for `4B_C` method `1` when `detoaster` is `None`.
- Optionally implement an offline `ToastDetoaster` backed by raw `pg_toast_<oid>` relation pages + B-tree index / MVCC snapshot visibility for zero-SPI `1B_E` scans.

## Resolution (Wave 5 Agent B)
- **Bounded LZ4 block decompression**: Zero-dependency pure-Rust `lz4_decompress` in `src/arrow_bridge.rs` (sharing `init_decompress_buf` and `copy_match_checked` bounds/overlap guards with `pglz_decompress`). Fully integrated into `read_varlena_payload_cow` for `4B_C` method `1` fallback when `detoaster` is `None`.
- **Standalone TOAST reader (`StandaloneToastReader`)**: Implemented standalone offline TOAST page reader struct implementing `ToastDetoaster`. Allows ingesting offline `pg_toast_<oid>` 8KB heap pages via `read_page` or `from_pages`, decoding chunk tuples (`chunk_id`, `chunk_seq`, `chunk_data`) with bounded heap page line pointer validation (`decode_heap_page_tuples_ext`), assembling in sequence order, and detoasting raw or compressed (PGLZ / LZ4) chunked varlenas.
- **Unit tests**:
  - `lz4_4bc_offline_decompress_and_corrupt_guards`: literal runs, overlapping extended matches, extended literal lengths, and corrupt bounds.
  - `standalone_toast_reader_and_detoast`: multi-chunk external pointer reassembly, PGLZ decompression, sequence gap/corruption detection, and `ToastDetoaster` trait integration.
