<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing pg_bolt

Checklist for cutting a `pg_bolt` release.

## 1. Version & Metadata Sync

Ensure the release version (`X.Y.Z`) matches across:
- [`Cargo.toml`](Cargo.toml) (`package.version` and `craton-bolt` dependency pin)
- [`pg_bolt.control`](pg_bolt.control) (`default_version`)
- Upgrade/install SQL script under [`sql/`](sql/) (for example `sql/pg_bolt--0.1.1.sql` or migration scripts)
- [`CHANGELOG.md`](CHANGELOG.md) and [`NOTICE`](NOTICE)

## 2. Pre-Release Validation

All checks below must pass cleanly before tagging:

```bash
cargo fmt --all -- --check
cargo clippy --no-default-features --features cuda-stub --all-targets -- -D warnings
cargo test --no-default-features --features cuda-stub
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --no-default-features --features cuda-stub
```

Run Linux PostgreSQL matrix (`pg15`–`pg18`) and CUDA GPU validation (`--features "pg17 cuda pg_test"` + `docker/Dockerfile.pg17-gpu` build):

```bash
cargo pgrx test pg17 --no-default-features --features "pg17 cuda-stub pg_test"
cargo test --features cuda
```

Verify license headers and `NOTICE` compliance (`Apache-2.0` source + `AGPL-3.0-only` combined binary notice for `craton-bolt`).

## 3. Tag & Publish

```bash
git tag -s vX.Y.Z -m "Release pg_bolt vX.Y.Z"
git push origin vX.Y.Z
```

Create the GitHub Release attaching release notes from `CHANGELOG.md` and note whether binary container images carry `AGPL-3.0-only` combined-work obligations.
