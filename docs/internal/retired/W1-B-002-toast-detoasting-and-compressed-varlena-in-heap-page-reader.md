<!-- SPDX-License-Identifier: Apache-2.0 -->

# W1-B-002: Direct 8KB heap page reader cannot detoast external (`1B_E`) or decompress inline (`4B_C`) varlenas

| Field | Value |
| :--- | :--- |
| Title | Direct 8KB heap page reader cannot detoast external (`1B_E`) or decompress inline (`4B_C`) varlenas |
| Severity | high |
| Status | fixed |
| Kind | proposal |
| Files | `src/arrow_bridge.rs` |

## Description
PostgreSQL automatically compresses or out-of-line TOASTs `TEXT` and `NUMERIC` attributes larger than ~2 KB (`TOAST_TUPLE_THRESHOLD`) using either inline compressed headers (`4B_C`, tag `0x02`, PGLZ or LZ4 depending on `attcompression`) or 18-byte external TOAST pointers (`1B_E`, `va_tag = 18` pointing into `pg_toast.pg_toast_<oid>`).

`read_varlena_payload` in `src/arrow_bridge.rs:810-885` cleanly detects both `1B_E` (`total_len == 0`) and `4B_C` (`(word & 0x03) == 0x02`) and returns explicit `PgBoltError::Bridge` errors rather than reading garbage, which means any table containing compressed or out-of-line `TEXT` / `NUMERIC` values cannot use the direct 8KB page reader yet.

## Reproduction
1. Insert a 3,000-byte string into a PostgreSQL table so PostgreSQL sets `4B_C` inline compression or moves the datum into `pg_toast.*` (`1B_E`).
2. Feed the 8KB heap page to `decode_heap_page_tuples` or `decode_heap_pages_to_arrow`.
3. Decoding fails with `cannot decode external TOAST pointer` or `cannot decode inline-compressed TOAST varlena`.

## Suggested fix
- For pure-Rust offline 8KB page decoding: implement PostgreSQL's PGLZ decompressor (`pg_lzdecompress`, ~60 lines) and optional LZ4 block decompression for `4B_C` (`va_tcinfo` 2-bit compression method id: `0 = PGLZ`, `1 = LZ4`) so inline compressed datums decode without PostgreSQL FFI.
- For in-backend (`#[cfg(feature = "pgrx")]`) heap scans: expose a `Detoaster` trait or invoke `pg_sys::detoast_attr` / read `pg_toast.<toastrelid>` chunks ordered by `chunk_seq` using the caller's MVCC snapshot when `VARATT_IS_EXTERNAL_ONDISK` is seen.
- Add unit tests with synthetic PGLZ-compressed `4B_C` and mock `pg_toast` chunk reconstruction.

## Resolution (Wave 2 Agent B)
- Implemented pure-Rust `pglz_decompress` for `4B_C` inline compressed varlenas (`method == 0`) with strict slice bounds, `MAX_VARLENA_RAW_BYTES` / corrupt header protection, and `expected_raw_len` verification.
- Added `ToastDetoaster` trait and `#[cfg(feature = "pgrx")] PgBackendToastDetoaster` (`pg_detoast_datum_packed`) plus `decode_heap_page_tuples_ext` / `decode_heap_pages_to_arrow_with_specs`.
- Added unit test `pglz_inline_4bc_decompression_toast_and_corruption_guards` in `src/arrow_bridge.rs`.
- Follow-up for pure-Rust offline `4B_C` LZ4 (`method == 1`) & standalone `pg_toast` page reconstruction filed as `W2-B-003`.
