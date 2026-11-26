<!-- SPDX-License-Identifier: Apache-2.0 -->

# Installation & Build Guide

## Prerequisites

1. **Workspace layout**: [`Cargo.toml`](../Cargo.toml) references `craton-bolt` via a sibling path dependency (`path = "../bolt"`). Place both repositories in a common parent directory:
   ```text
   workspace/
   ├── bolt/
   └── pg_bolt/
   ```
2. **Rust toolchain**: Rust `>= 1.85.0` (edition `2021`).
3. **PostgreSQL & `pgrx`** (for PostgreSQL extension builds):
   - Supported major versions: **PostgreSQL 15, 16, 17, 18** (`pg15`, `pg16`, `pg17`, `pg18` Cargo features).
   - `cargo-pgrx` `0.19.4` matching `pgrx = "0.19"` in `Cargo.toml`.
   - Build tools: `clang`, `libclang-dev`, `pkg-config`, `libssl-dev`, C compiler toolchain.
4. **CUDA Toolkit `>= 12.0`** (for real GPU builds with `--features cuda`):
   - `build.rs` inspects `CUDA_PATH`, then `CUDA_HOME`, and then standard paths:
     - **Linux / Unix**: `/usr/local/cuda/lib64` (+ `/stubs`), `/usr/lib/x86_64-linux-gnu`, `/opt/cuda/lib64`.
     - **Windows**: `C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v*\lib\x64`.

## Cargo Feature Matrix

| Feature | Effect |
| :--- | :--- |
| `cuda` *(default)* | Enables `craton-bolt/cudarc` and `craton-bolt/storage` for live NVIDIA GPU PTX JIT execution |
| `cuda-stub` | Enables `craton-bolt/cuda-stub` and `craton-bolt/storage`; compiles and runs planner, MVCC, IPC, and bridge tests without GPU drivers/hardware |
| `pgrx` | Enables PostgreSQL `pgrx` C ABI hooks (automatically activated by `pg15`–`pg18`) |
| `pg15`, `pg16`, `pg17`, `pg18` | Targets PostgreSQL 15, 16, 17, or 18 via `pgrx` |
| `pg_test` | Enables `pgrx-tests` for `cargo pgrx test` |

## Building & Installing on Linux (PostgreSQL 17 + CUDA)

```bash
# 1. Install cargo-pgrx matching Cargo.toml (0.19.4)
cargo install cargo-pgrx --version 0.19.4 --locked
cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config

# 2. Install pg_bolt extension into PostgreSQL 17
cd pg_bolt
cargo pgrx install \
  --release \
  --pg-config /usr/lib/postgresql/17/bin/pg_config \
  --no-default-features \
  --features "pg17 cuda"
```

Then configure PostgreSQL (`postgresql.conf`) and restart:

```ini
shared_preload_libraries = 'pg_bolt'
```

And enable inside your database:

```sql
CREATE EXTENSION pg_bolt;
```

## Docker Build (PostgreSQL 17 + CUDA 12.6)

[`docker/Dockerfile.pg17-gpu`](../docker/Dockerfile.pg17-gpu) builds a multi-stage Ubuntu 24.04 image with CUDA 12.6, PostgreSQL 17, `cargo-pgrx 0.19.4`, and `shared_preload_libraries = 'pg_bolt'` preconfigured.

Build from the workspace root containing both `bolt/` and `pg_bolt/`:

```bash
docker build -f pg_bolt/docker/Dockerfile.pg17-gpu -t craton/pg_bolt:pg17-gpu .
docker run --rm -it --gpus all -e POSTGRES_PASSWORD=postgres -p 5432:5432 craton/pg_bolt:pg17-gpu
```

## Running Tests Without a GPU (`cuda-stub`)

```bash
cd pg_bolt
cargo test --no-default-features --features cuda-stub
cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings
```

For Linux `pgrx` PostgreSQL integration tests:

```bash
cargo pgrx test pg17 --no-default-features --features "pg17 cuda-stub pg_test"
```

## Windows Build Note

On `x86_64-pc-windows-msvc`, [`.cargo/config.toml`](../.cargo/config.toml) selects `linker=rust-lld` to avoid Windows `link.exe` COFF limits with Arrow/CUDA objects. Pure-Rust host suites (`cuda-stub` and `cuda`) run directly on Windows; full `pgrx` extension execution (`cargo pgrx test`) targets Linux PostgreSQL installations.
