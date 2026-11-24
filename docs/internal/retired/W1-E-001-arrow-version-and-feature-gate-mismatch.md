# W1-E-001: Arrow crate version and feature-gate mismatch between `PG_BOLT_PLAN_AND_CODE.md` and `craton-bolt` 0.12.0

| Field | Value |
| :--- | :--- |
| Title | Arrow crate version and feature-gate mismatch between `PG_BOLT_PLAN_AND_CODE.md` and `craton-bolt` 0.12.0 |
| Severity | high |
| Status | fixed |
| Kind | bug |
| Files | `../bolt/PG_BOLT_PLAN_AND_CODE.md:49,118-134`, `../bolt/Cargo.toml:47-50` |

## Description
The extension scaffold in `PG_BOLT_PLAN_AND_CODE.md` diverges from `craton-bolt` 0.12.0's dependency graph and build configuration in four ways:
1. Lines 131–133 pin `arrow-array = "54"`, `arrow-schema = "54"`, and `arrow-cast = "54"`, whereas `../bolt/Cargo.toml:47-50` pins Arrow `53` (`53` vs `54` produces incompatible `RecordBatch` / `Schema` types across crate boundaries).
2. Line 129 depends on `craton-bolt` without `default-features = false`, while line 121 defines `cuda-stub = ["craton-bolt/cuda-stub"]`. Because `craton-bolt` enables `cudarc` by default, passing `--no-default-features --features cuda-stub` in `pg_bolt` leaves CUDA driver dependencies active instead of isolating host-stub builds.
3. `Cargo.toml` omits an optional `pgrx` feature gate, preventing pure-Rust host integration tests (`cargo test`) from compiling without PostgreSQL / `pgrx` C linker setup.
4. Section 1 prose (`:49`) promises PostgreSQL 14–18 support, whereas `Cargo.toml:122-125` only defines `pg15` through `pg18`.

## Reproduction
1. Compare `PG_BOLT_PLAN_AND_CODE.md:118-134` with `../bolt/Cargo.toml:47-50`.
2. Passing an `arrow-array 54` `RecordBatch` into `craton_bolt::Engine::register_batch` fails to compile (`expected arrow_array::RecordBatch (53.x), found arrow_array::RecordBatch (54.x)`).

## Suggested fix
Pin `arrow-array`, `arrow-schema`, `arrow-cast`, and `arrow-json` to `"53"`, depend on `craton-bolt` with `default-features = false` (`cuda = ["craton-bolt/cudarc", "craton-bolt/storage"]`, `cuda-stub = ["craton-bolt/cuda-stub"]`), add an explicit `pgrx` feature gate (`#[cfg(feature = "pgrx")]`), and align the documented supported PostgreSQL versions (15–18 or add `pg14`).
