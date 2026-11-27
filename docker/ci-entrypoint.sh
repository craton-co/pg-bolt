#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Ensure USER and HOME are set for pgrx-tests
export USER="${USER:-$(id -un)}"
export HOME="${HOME:-/home/${USER}}"

# Ensure git doesn't complain about mounted workspace ownership
git config --global --add safe.directory '*' >/dev/null 2>&1 || true

# Comma- or space-separated list of PostgreSQL versions for pgrx checks (e.g. "pg17" or "pg15 pg16 pg17 pg18")
PG_VERSIONS="${PG_VERSIONS:-pg17}"
PG_VERSIONS="${PG_VERSIONS//,/ }"
SKIP_PGRX="${SKIP_PGRX:-0}"
SKIP_DENY="${SKIP_DENY:-0}"

if [ ! -d /workspace/pg_bolt ]; then
  echo "ERROR: /workspace/pg_bolt not found." >&2
  exit 1
fi

if [ ! -d /workspace/bolt ]; then
  echo "ERROR: /workspace/bolt (sibling craton-bolt repo) not found." >&2
  exit 1
fi

cd /workspace/pg_bolt

step() {
  echo ""
  echo "================================================================"
  echo "==> $*"
  echo "================================================================"
}

step "1. Format check (cargo fmt)"
cargo +stable fmt -- --check

step "2. Pure-Rust cuda-stub suite (MSRV 1.85.0)"
cargo +1.85.0 clippy --no-default-features --features cuda-stub --all-targets -- -D warnings
cargo +1.85.0 test --no-default-features --features cuda-stub

step "3. Pure-Rust cuda-stub suite (stable)"
cargo +stable clippy --no-default-features --features cuda-stub --all-targets -- -D warnings
cargo +stable test --no-default-features --features cuda-stub

step "4. Rustdoc check (-D warnings)"
RUSTDOCFLAGS="-D warnings" cargo +stable doc --no-deps --no-default-features --features cuda-stub

if [ "${SKIP_DENY}" != "1" ]; then
  step "5. cargo-deny check"
  cargo deny check
else
  echo ""
  echo "==> Skipping cargo-deny (SKIP_DENY=1)"
fi

if [ "${SKIP_PGRX}" != "1" ]; then
  for pg in ${PG_VERSIONS}; do
    step "6. pgrx init & checks (${pg}, cuda-stub)"
    if [ ! -f "${PGRX_HOME}/config.toml" ] || ! grep -q -E "(^|[[:space:]])${pg}[[:space:]]*=" "${PGRX_HOME}/config.toml" 2>/dev/null; then
      cargo pgrx init "--${pg}" download
    fi
    cargo +stable clippy --no-default-features --features "${pg} cuda-stub" --all-targets -- -D warnings
    cargo +stable pgrx test "${pg}" --no-default-features --features "${pg} cuda-stub pg_test"
  done
else
  echo ""
  echo "==> Skipping pgrx matrix (SKIP_PGRX=1)"
fi

echo ""
echo "================================================================"
echo "ALL LOCAL CI CHECKS PASSED"
echo "================================================================"
