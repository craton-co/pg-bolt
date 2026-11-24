<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to pg_bolt

Thank you for your interest in contributing to `pg_bolt`.

## Development Prerequisites

- **Rust**: `>= 1.85.0` (edition `2021`, matching `Cargo.toml`) with `clippy` and `rustfmt`
- **Sibling `craton-bolt` checkout**: `Cargo.toml` references `craton-bolt = { path = "../bolt", version = "0.12.0" }`, so clone `bolt` alongside `pg_bolt` in the same parent directory:
  ```text
  workspace/
  ├── bolt/
  └── pg_bolt/
  ```
- **For GPU builds (`--features cuda`)**: NVIDIA CUDA Toolkit `>= 12.0` (`CUDA_PATH` or `CUDA_HOME` configured)
- **For PostgreSQL extension builds (`--features "pg17 cuda-stub"` etc.)**: `cargo-pgrx = "0.19.4"`, `clang`, `libclang-dev`, `pkg-config`, `libssl-dev`, and PostgreSQL `15`–`18` development headers

## Verification Before Opening a Pull Request

Run formatting, lints (`-D warnings`), and pure-Rust tests with `cuda-stub`:

```bash
cargo fmt --all -- --check
cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings
cargo test --no-default-features --features cuda-stub
```

If you have CUDA hardware and CUDA Toolkit >= 12 installed:

```bash
cargo test --features cuda
```

If you are working on PostgreSQL `pgrx` bindings (Linux/Docker):

```bash
cargo clippy --no-default-features --features "pg17 cuda-stub" --all-targets -- -D warnings
cargo pgrx test pg17 --no-default-features --features "pg17 cuda-stub pg_test"
```

Verify license and advisory compliance against [`deny.toml`](deny.toml):

```bash
cargo deny check
```

## Developer Certificate of Origin (DCO)

All commits must include a `Signed-off-by` trailer certifying the [Developer Certificate of Origin 1.1](DCO):

```bash
git commit -s -m "fix(planner): preserve self-referencing CTE base table"
```

## Licensing Note for Contributors

Source files contributed directly to `pg_bolt` carry the SPDX header `// SPDX-License-Identifier: Apache-2.0` (or `<!-- SPDX-License-Identifier: Apache-2.0 -->` for Markdown) and are licensed under [Apache-2.0](LICENSE). Note that `craton-bolt` is licensed under `AGPL-3.0-only` (see [NOTICE](NOTICE)).

## Code Owners & Conduct

Default code ownership is defined in [CODEOWNERS](CODEOWNERS) (`@victor-craton`). All contributors are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
