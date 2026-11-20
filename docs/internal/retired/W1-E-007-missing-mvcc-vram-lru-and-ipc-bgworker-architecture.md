# W1-E-007: Missing MVCC visibility, VRAM LRU eviction, BGWorker IPC, and incorrect license note in architecture plan

| Field | Value |
| :--- | :--- |
| Title | Missing MVCC visibility, VRAM LRU eviction, BGWorker IPC, and incorrect license note in architecture plan |
| Severity | high |
| Status | fixed |
| Kind | gap |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:76,84-101,166-720`, `../bolt/Cargo.toml:6` |

## Description
Several critical design components listed in Sections 1–2 of `PG_BOLT_PLAN_AND_CODE.md` are absent from the code design in Section 3, and Section 1 contains stale license metadata:
1. **MVCC isolation gap**: Section 3 caches full table snapshots via `load_table` without `xmin`/`xmax`/`VisibilityMap` tuple filtering (`is_tuple_visible`, `HEAP_XMAX_LOCK_ONLY`, `build_visibility_bitmap`, `decode_heap_pages_to_arrow`), which would leak uncommitted or deleted tuples across concurrent transactions.
2. **Unbounded VRAM usage**: There is no `max_vram_mb` budget check, LRU table eviction, or `VramExhausted` handling when registering Arrow batches.
3. **Missing BGWorker & IPC layer**: Section 3 lacks the shared-memory arena / `MinimalTuple` IPC protocol and persistent CUDA Background Worker required to avoid per-backend 300 MB CUDA contexts and `fork()` corruption.
4. **Stale license statement**: `PG_BOLT_PLAN_AND_CODE.md:76` states `craton-bolt` is Apache-2.0, whereas `../bolt/Cargo.toml:6` licenses `craton-bolt` 0.12.0 under `AGPL-3.0-only`.

## Reproduction
Review `PG_BOLT_PLAN_AND_CODE.md:76-720` alongside `../bolt/Cargo.toml:6`.

## Suggested fix
Implement the `heap_page` + `mvcc` visibility filter, LRU VRAM budget manager, `bgworker` + `ipc` shared-memory worker, and update `NOTICE` and architecture documentation to accurately reflect `craton-bolt`'s `AGPL-3.0-only` license for combined binaries.
