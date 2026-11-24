# W1-E-012: `quote_qualified_identifier` splits naively on `.`, breaking quoted dots and pre-quoted PostgreSQL identifiers

| Field | Value |
| :--- | :--- |
| Title | `quote_qualified_identifier` splits naively on `.`, breaking quoted dots and pre-quoted PostgreSQL identifiers |
| Severity | medium |
| Status | fixed |
| Kind | bug |
| Files | `src/runtime.rs:47-88`, `src/lib.rs:57-73` |

## Description
`quote_qualified_identifier` (`src/runtime.rs:60-85`) splits the input string unconditionally with `trimmed.split('.')`, checks `parts.len() > 3`, rejects whitespace around `.`, and wraps every split segment in `"..."` while doubling internal `"` characters.

This breaks two legitimate PostgreSQL relation name forms accepted by `regclass`:
1. **Quoted identifiers containing dots**: `"my.schema"."my.table"` (or `public."sales.2026"`) splits on internal dots into 4 segments and errors with `"has more than 3 dot-separated segments"`, or turns `public."sales.2026"` into `"public"."\"sales"."2026\""`.
2. **Pre-quoted relation names**: `"\"MySchema\".\"Sales\""` (as formatted by `regclass::text` or `quote_ident()`) has its outer quotes escaped to `"""MySchema"""."""Sales"""` instead of targeting `"MySchema"."Sales"`.

## Reproduction
1. Call `pg_bolt::runtime::quote_qualified_identifier(r#""my.schema"."my.tbl""#)`.
2. Expected: `Ok("\"my.schema\".\"my.tbl\"")`.
3. Actual: `Err(PgBoltError::Validation("qualified identifier ... has more than 3 dot-separated segments"))`.

## Suggested fix
Parse PostgreSQL qualified identifier segments with a small quote-aware scanner (or via `pgrx::pg_sys::makeRangeVarFromNameList` / PostgreSQL `textToQualifiedNameList` on the SPI side) that tracks `""` escapes inside double quotes, splits only on unquoted `.`, unquotes/normalizes each segment, and re-emits canonical `"seg1"."seg2"` quoting. Cover `"my.schema"."my.tbl"` and `public."v1.sales"` in unit and integration tests.

## Resolution (Wave 2)
Fixed in `src/runtime.rs` via `parse_qualified_segments`: properly scans PostgreSQL double-quoted segments (preserving dots inside quotes, unescaping `""`, and canonicalizing pre-quoted identifiers) while keeping legacy unquoted embedded-quote escaping (`my"schema.my"tbl` -> `"my""schema"."my""tbl"`) and rejecting empty, unclosed, control-char, whitespace-around-dot, or >3-segment inputs. Unit tests updated in `runtime::tests`.
