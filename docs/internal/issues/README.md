<!-- SPDX-License-Identifier: Apache-2.0 -->

# Issue tracker (in-repo)

Open defects, proposals, documentation problems, and missing tests for `pg_bolt`
are tracked as one Markdown page per item in this directory. Fixed items are
retired to [`../retired/`](../retired/).

## File naming

```text
<WAVE>-<AGENT>-<NNN>-<slug>.md
```

| Part | Meaning | Example |
| :--- | :--- | :--- |
| `WAVE` | Review / fix wave that filed the item (`W1`, `W2`, ...). | `W1` |
| `AGENT` | Author or work-stream id inside the wave (`A`..`E`). | `E` |
| `NNN` | Three-digit sequence number, unique per `WAVE-AGENT`. Never reused. | `001` |
| `slug` | A few lowercase words joined with `-`. | `arrow-version-and-numeric-oid-mismatch` |

## Page format

```markdown
# W1-E-001: Short title

| Field | Value |
| :--- | :--- |
| Title | Short title (same as the heading) |
| Severity | critical | high | medium | low |
| Status | open | in-progress | blocked-upstream | fixed | wontfix |
| Kind | bug | proposal | docs | test |
| Files | `src/arrow_bridge.rs:10-40`, ... |

## Description
What is wrong and why it matters.

## Reproduction
Steps, expected result, and actual result.

## Suggested fix
The smallest change that fixes it, plus the test that should cover it.
```

## Index (Open)

*There are currently 0 open issues. All 66 issues filed across Waves 1–6 have been fixed/resolved and retired to [`../retired/`](../retired/).*

## Retired (66 Issues)

| ID | Severity | Status | Kind | Summary |
| :--- | :--- | :--- | :--- | :--- |
| [W1-A-001](../retired/W1-A-001-cargo-optional-dev-dependency-and-feature-propagation.md) | high | fixed | bug | Cargo optional dev-dependency error and crate dependency alignment |
| [W1-A-002](../retired/W1-A-002-bgworker-and-custom-scan-integration-in-pg-init.md) | high | fixed | bug | Wire background worker, IPC arena dispatch, and CustomScan into `_PG_init` |
| [W1-A-003](../retired/W1-A-003-mvcc-snapshot-sync-delta-and-stale-cache-wiring.md) | high | fixed | proposal | Wire PostgreSQL DML invalidation and incremental MVCC deltas into cached tables |
| [W1-A-004](../retired/W1-A-004-pgrx-strict-null-default-and-extschema-control-trap.md) | high | fixed | bug | Fix pgrx STRICT NULL-default trap, extension control schema error, type aliases, and GUC bugs |
| [W1-A-005](../retired/W1-A-005-memtableprovider-set-sql-semantics-pub-crate-upstream.md) | medium | resolved | proposal | Upstream craton-bolt ergonomics for MemTableProvider::set_sql_semantics and serde derives |
| [W1-A-006](../retired/W1-A-006-query-timeout-cancellation-not-cooperative-during-ptx-or-ipc.md) | medium | fixed | bug | Bridge PostgreSQL SIGINT query cancellation and statement_timeout into IPC CancellationToken |
| [W1-A-007](../retired/W1-A-007-per-session-bolt-runtime-ignores-pg-xact-subxact-and-ipc-dispatch.md) | medium | fixed | bug | Reconcile bolt cache state across PostgreSQL transaction rollback and DDL |
| [W1-A-008](../retired/W1-A-008-missing-cross-platform-pg-bolt-ci-and-pg-test-coverage.md) | medium | fixed | test | Add CI build/test matrix and full pgrx pg_test integration coverage |
| [W1-B-001](../retired/W1-B-001-lib-query-jsonb-return-type-mismatch.md) | critical | fixed | bug | `lib.rs` `bolt.query` / `bolt.query_json` expects `Vec<pgrx::JsonB>` from `record_batch_to_json_rows` |
| [W1-B-002](../retired/W1-B-002-toast-detoasting-and-compressed-varlena-in-heap-page-reader.md) | high | fixed | proposal | Direct 8KB heap page reader cannot detoast external (`1B_E`) or decompress inline (`4B_C`) varlenas |
| [W1-B-003](../retired/W1-B-003-spi-unbounded-materialization-and-non-utf8-encoding-panic.md) | high | fixed | bug | SPI extraction materializes full result set and panics/fails on non-UTF-8 PostgreSQL server encodings |
| [W1-B-004](../retired/W1-B-004-numeric-typmod-scale-overflow-and-special-values.md) | high | fixed | bug | Static `Decimal128(38, 10)` `NUMERICOID` mapping rejects `> 10` fractional digits, `> 10^28` integers, and PG `NaN`/`Infinity` |
| [W1-B-005](../retired/W1-B-005-heap-page-hot-redirect-and-schema-evolution-dropped-attmissing.md) | high | fixed | bug | 8KB heap page reader lacks HOT `LP_REDIRECT`, `attisdropped` alignment, and PG 11+ `attmissingval` defaults |
| [W1-B-006](../retired/W1-B-006-extended-pg-types-bytea-time-uuid-mapping.md) | medium | fixed | proposal | Wire `BYTEAOID`, `TIMEOID`, and `UUIDOID` to `craton-bolt` `Extended` Arrow types (`Binary`, `Time64`, `FixedSizeBinary`) |
| [W1-B-007](../retired/W1-B-007-jsonb-nul-byte-and-minimal-tuple-custom-scan-bridge.md) | medium | fixed | proposal | `jsonb` `\u0000` NUL-byte failure and missing direct `MinimalTuple` / `TupleTableSlot` result path |
| [W1-C-001](../retired/W1-C-001-mvcc-clog-lookup-for-ancient-xids-and-wraparound-horizon.md) | high | fixed | bug | Missing `pg_xact` clog lookup for hint-bit-less `< xmin` tuples and XID wraparound horizon guard |
| [W1-C-002](../retired/W1-C-002-multixact-member-resolution-missing-pg-multixact.md) | high | fixed | bug | `HEAP_XMAX_IS_MULTI` requires PostgreSQL `MultiXactIdGetUpdateXid` member resolution |
| [W1-C-003](../retired/W1-C-003-mvcc-own-xid-suboverflow-and-buffer-lock.md) | high | fixed | bug | Missing own-transaction (`cmin`/`cmax`), `suboverflowed`, and buffer-lock atomicity in MVCC visibility |
| [W1-C-004](../retired/W1-C-004-cross-process-pg-dsm-shmem-ipc-wiring.md) | high | fixed | proposal | Cross-process PostgreSQL DSM / `ShmemInitStruct` / `LWLock` / `Latch` wiring for `SharedMemoryArena` |
| [W1-C-005](../retired/W1-C-005-chunked-ipc-streaming-for-oversized-batches.md) | medium | fixed | proposal | Chunked IPC streaming / DSM sideband for >`slot_bytes` Arrow batches |
| [W1-C-006](../retired/W1-C-006-bgworker-cancellation-guc-sighup-and-ipc-header-guard.md) | medium | fixed | bug | BGWorker query cancellation propagation, `SIGHUP` GUC reload, and Arrow IPC header bounds guard |
| [W1-D-001](../retired/W1-D-001-lib-query-json-wrong-arrow-bridge-fn.md) | critical | fixed | bug | `lib.rs` calls `record_batch_to_json_rows` expecting `Vec<pgrx::JsonB>` instead of `record_batch_to_pgrx_jsonb` |
| [W1-D-002](../retired/W1-D-002-bolt-schema-qualifier-stripping-breaks-default-load-table.md) | high | fixed | bug | `craton-bolt` schema-qualifier stripping breaks default `bolt.load_table('public.sales')` alias and cross-schema resolution |
| [W1-D-003](../retired/W1-D-003-planner-hook-raw-query-string-vs-pg-ast-rewrites-and-params.md) | high | fixed | bug | `planner_hook` raw `query_string` forwarding cannot handle `$1` bound parameters, view/rule expansion, or `search_path` |
| [W1-D-004](../retired/W1-D-004-custom-scan-pg-runtime-bgworker-and-cpu-subplan-fallback-wiring.md) | high | fixed | proposal | `CustomScan` execution needs shared `BgWorkerClient` wiring, `Direct` `timeout_ms` support, and fallback child `Plan` execution |
| [W1-D-005](../retired/W1-D-005-cross-backend-planner-catalog-sync-and-mvcc-snapshot-staleness.md) | high | fixed | bug | `PLANNER_CATALOG` is process-local and blind to cross-backend mutations and active MVCC transaction visibility |
| [W1-D-006](../retired/W1-D-006-planner-hook-upper-rel-subquery-and-resjunk-projection-support.md) | medium | fixed | proposal | `planner_hook` root replacement skips `ORDER BY` `resjunk` targets and mixed cached/uncached path injection |
| [W1-E-001](../retired/W1-E-001-arrow-version-and-feature-gate-mismatch.md) | high | fixed | bug | Arrow crate version and feature-gate mismatch between `PG_BOLT_PLAN_AND_CODE.md` and `craton-bolt` 0.12.0 |
| [W1-E-002](../retired/W1-E-002-sql-injection-in-load-table.md) | critical | fixed | bug | SQL injection via unquoted relation name interpolation in `bolt.load_table` |
| [W1-E-003](../retired/W1-E-003-pg-type-mapping-truncation-and-missing-temporal-types.md) | high | fixed | bug | PostgreSQL-to-Arrow type bugs (`INT2`, `NUMERIC`), missing temporal OIDs, and JSON conversion gaps |
| [W1-E-004](../retired/W1-E-004-runtime-queryhandle-stub-and-fork-safety-bugs.md) | high | fixed | bug | `runtime.rs` `QueryHandle::into()` compile failure, missing `cuda-stub` fallback, and CUDA fork-safety gap |
| [W1-E-005](../retired/W1-E-005-noop-planner-hook-and-missing-custom-scan-eligibility.md) | high | fixed | bug | No-op `planner_hook` and missing CustomScan eligibility, CTE handling, fallback, and cancellation |
| [W1-E-006](../retired/W1-E-006-postgresql-vs-bolt-sql-semantics-divergence.md) | medium | fixed | bug | PostgreSQL vs `craton-bolt` SQL semantics divergence (`NaN`, integer overflow, `NULLS FIRST/LAST`) |
| [W1-E-007](../retired/W1-E-007-missing-mvcc-vram-lru-and-ipc-bgworker-architecture.md) | high | fixed | gap | Missing MVCC visibility, VRAM LRU eviction, BGWorker IPC, and incorrect license note in architecture plan |
| [W1-E-008](../retired/W1-E-008-bolt-sql-on-stream-and-pg-overflow-semantics-proposal.md) | low | resolved | proposal | Upstream `craton-bolt` proposal — PostgreSQL `DESC NULLS FIRST` semantics and query/DDL cancel safety |
| [W1-E-009](../retired/W1-E-009-unit-tests-expect-gpu-tier-for-aggregate-and-dead-host-arm.md) | high | fixed | bug | Unit tests in `runtime.rs`, `planner.rs`, and `bgworker.rs` expect `ExecutionTier::Gpu` for `GROUP BY` aggregates |
| [W1-E-010](../retired/W1-E-010-eager-vram-eviction-corrupts-cache-before-register-succeeds.md) | high | fixed | bug | Eager LRU eviction and `plan_provider` mutation occur before `engine` table registration succeeds |
| [W1-E-011](../retired/W1-E-011-cte-scope-prepopulation-masks-self-referencing-base-table.md) | medium | fixed | bug | `SqlInspector` pre-populates all CTE aliases before inspecting CTE bodies, masking self-referencing base tables |
| [W1-E-012](../retired/W1-E-012-quote-qualified-identifier-breaks-quoted-dots-and-already-quoted-names.md) | medium | fixed | bug | `quote_qualified_identifier` splits naively on `.`, breaking quoted dots and pre-quoted PostgreSQL identifiers |
| [W2-A-001](../retired/W2-A-001-custom-scan-shared-bgworker-result-type-mismatch.md) | critical | fixed | bug | `custom_scan.rs` treats `shared_bgworker_client()` as `Option` instead of `PgBoltResult` |
| [W2-A-002](../retired/W2-A-002-ipc-missing-explain-sql-and-worker-dirty-sync-commands.md) | medium | fixed | proposal | Missing `ExplainSql` and worker dirty-flag sync commands in `ipc.rs` / `bgworker.rs` |
| [W2-B-001](../retired/W2-B-001-unconstrained-numeric-fallback-guc-and-decimal256.md) | medium | fixed | proposal | Unconstrained `NUMERIC` (`atttypmod == -1`) dynamic precision/scale and `Decimal256` / `Float64` fallback GUC |
| [W2-B-002](../retired/W2-B-002-spi-streaming-batches-bpchar-semantics-and-pg-test.md) | medium | fixed | proposal | Streaming multi-batch SPI extraction, `BPCHAR` trailing-space semantics, and live `pg_test` verification |
| [W2-B-003](../retired/W2-B-003-offline-lz4-varlena-and-standalone-toast-page-reader.md) | medium | fixed | proposal | Pure-Rust offline LZ4 `4B_C` varlena decompression and standalone `pg_toast` relation page reconstruction |
| [W2-B-004](../retired/W2-B-004-custom-scan-per-tuple-memory-context-and-cached-oids.md) | high | fixed | bug | `CustomScan` per-tuple `MemoryContext` leak and per-row OID `Vec` allocation in `populate_tuple_slot_from_batch` |
| [W2-C-001](../retired/W2-C-001-runtime-execute-sql-with-timeout-and-reconfigure.md) | medium | fixed | bug | Add `BoltSessionRuntime::execute_sql_with_timeout` and `reconfigure` for `pgrx` BGWorker timeout/SIGHUP parity |
| [W2-C-002](../retired/W2-C-002-cross-process-dsm-payload-ring-and-pg-init-wiring.md) | high | fixed | proposal | Wire `_PG_init` shmem/bgworker hooks and DSM segment payload transport across PG processes |
| [W2-C-003](../retired/W2-C-003-chunked-ipc-response-and-stream-ttl.md) | medium | fixed | proposal | Chunked IPC response streaming (`IpcResponse::BatchChunk`) and partial-stream TTL cleanup |
| [W2-D-001](../retired/W2-D-001-lib-pg-test-expects-gpu-for-aggregate-and-invalidations.md) | high | fixed | bug | `lib.rs` `pg_test` expects `"Gpu"` for `GROUP BY` aggregate and lacks OID invalidation wiring |
| [W2-D-002](../retired/W2-D-002-planner-rewrite-3part-column-qualifiers-and-order-by-table-prefix.md) | medium | fixed | bug | `planner::rewrite_sql_for_bolt` omits 3-part column qualifier collapse and `ORDER BY` table prefix stripping |
| [W2-D-003](../retired/W2-D-003-load-table-bypasses-bgworker-catalog-when-use-bgworker-enabled.md) | high | fixed | bug | Control-plane APIs (`load_table`/`query`/`evict_table`) bypass `BoltBgWorker` when `pg_bolt.use_bgworker` is enabled |
| [W2-E-001](../retired/W2-E-001-custom-scan-missing-extended-and-unsigned-arrow-slot-types.md) | high | fixed | bug | CustomScan slot bridge omits unsigned ints, `Dictionary<Int64, Utf8>`, extended OIDs, and checked decimal scale |
| [W2-E-002](../retired/W2-E-002-ipc-minimal-tuple-missing-extended-and-view-types.md) | medium | fixed | bug | `MinimalTuple` IPC codec omits `Time`, `FixedSizeBinary` (UUID), `Utf8View`/`BinaryView`, and `Dictionary<Int64, Utf8>` decode |
| [W2-E-003](../retired/W2-E-003-custom-scan-query-cxt-varlena-growth.md) | medium | fixed | bug | `CustomScan` allocates `text`/`numeric` tuple varlenas in `es_query_cxt` for the entire query lifetime |
| [W3-A-001](../retired/W3-A-001-static-sql-and-docs-missing-bolt-explain-sql.md) | low | fixed | docs | Update `sql/pg_bolt--0.1.0.sql` and docs for `bolt.explain_sql(sql text)` |
| [W3-A-002](../retired/W3-A-002-engine-runtime-memory-budget-reconfigure.md) | low | resolved | proposal | `BoltSessionRuntime::reconfigure` cannot dynamically resize `craton_bolt::Engine` OOC memory budget |
| [W3-A-003](../retired/W3-A-003-custom-scan-bgworker-ignores-pg-statement-timeout.md) | medium | fixed | bug | `CustomScan` execution ignores client `statement_timeout` and `QueryCancelPending` over BGWorker IPC |
| [W3-D-001](../retired/W3-D-001-custom-scan-aggregate-avg-sum-int8-numeric-oid-mismatch.md) | high | fixed | bug | `CustomScan` aggregate `AVG`/`SUM(bigint)`/`STDDEV` output `Float64`/`Int64` fails on PostgreSQL `NUMERICOID` |
| [W3-D-002](../retired/W3-D-002-uncommitted-dml-and-rollback-corrupt-shared-bgworker-dirty-state.md) | high | fixed | bug | Uncommitted DML eager sync and `ROLLBACK` reset corrupt global `BgWorker` `dirty` state across backends |
| [W3-D-003](../retired/W3-D-003-custom-scan-fails-hard-instead-of-fallback-when-bgworker-unavailable.md) | medium | fixed | bug | `CustomScan` aborts query instead of falling back when `use_bgworker = on` and `shared_bgworker_client()` fails with `PgBoltError::Ipc` |
| [W3-E-001](../retired/W3-E-001-missing-bolt-explain-sql-wrapper.md) | low | fixed | proposal | Expose `bolt.explain_sql(sql text)` PostgreSQL SQL wrapper matching `api::explain_sql` |
| [W3-E-002](../retired/W3-E-002-custom-scan-fallback-missing-slot-getallattrs.md) | high | fixed | bug | `CustomScan` CPU fallback `copy_non_resjunk_slot_attrs` skips `slot_getallattrs` |
| [W3-E-003](../retired/W3-E-003-custom-scan-plan-targetlist-not-rewritten-to-index-var.md) | high | fixed | bug | `attach_custom_scan_plan` keeps `Aggref`/`OpExpr` in `scan.plan.targetlist` instead of `INDEX_VAR` references |
| [W3-E-004](../retired/W3-E-004-arrow-bridge-datum-missing-uint-int8-and-int64-dict.md) | medium | fixed | bug | `arrow_bridge::arrow_cell_to_pg_datum` still omits `Int8`/`UInt8..=UInt64` and `Dictionary<Int64, Utf8>` |
| [W6-A-001 (BGWorker)](../retired/W6-A-001-bgworker-dead-backend-reclamation.md) | low | fixed | proposal | Cross-Backend Dead Client PID Reclamation in PG Shmem Slots |
| [W6-A-001 (CustomScan)](../retired/W6-A-001-custom-scan-missing-pending-invalidation-and-dirty-refresh.md) | high | fixed | bug | `CustomScan` execution omits `reconcile_pending_invalidations` and `refresh_dirty_referenced_tables` |
