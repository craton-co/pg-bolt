#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Builds a .deb package for postgresql-<PG_MAJOR>-pg-bolt inside Ubuntu/Debian.
set -euo pipefail

PG_MAJOR="${1:?Usage: $0 <pg_major> <distro_codename> <version> <out_dir>}"
DISTRO="${2:?}"
VERSION="${3:?}"
OUT_DIR="${4:?}"

export DEBIAN_FRONTEND=noninteractive
export CUDA_HOME=/usr/local/cuda
export PATH="/root/.cargo/bin:/usr/lib/postgresql/${PG_MAJOR}/bin:${CUDA_HOME}/bin:${PATH}"

apt-get update
apt-get install -y --no-install-recommends \
    build-essential ca-certificates clang curl git gnupg \
    libclang-dev libssl-dev lsb-release pkg-config dpkg-dev

# Install PGDG repository
install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${DISTRO}-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list
apt-get update
apt-get install -y --no-install-recommends "postgresql-${PG_MAJOR}" "postgresql-server-dev-${PG_MAJOR}"

# Install CUDA toolkit headers/nvcc/nvrtc if not using an nvidia/cuda base image (e.g. debian:bookworm)
if [ ! -d "${CUDA_HOME}" ]; then
    curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/cuda-keyring_1.1-1_all.deb \
        -o /tmp/cuda-keyring.deb
    dpkg -i /tmp/cuda-keyring.deb
    rm -f /tmp/cuda-keyring.deb
    apt-get update
    apt-get install -y --no-install-recommends cuda-nvcc-12-6 cuda-nvrtc-dev-12-6 cuda-cudart-dev-12-6
fi

# Install Rust + cargo-pgrx
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal
fi
if ! command -v cargo-pgrx >/dev/null 2>&1; then
    cargo install cargo-pgrx --version 0.19.4 --locked
fi
cargo pgrx init "--pg${PG_MAJOR}" "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"

# Use crates.io craton-bolt if ../bolt is absent
if [ ! -d ../bolt ]; then
    sed -i.bak 's|path = "../bolt", ||g' Cargo.toml && rm -f Cargo.toml.bak
fi

cargo pgrx package \
    --pg-config "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" \
    --no-default-features \
    --features "pg${PG_MAJOR} cuda"

PKG_NAME="postgresql-${PG_MAJOR}-pg-bolt"
STAGE="/tmp/${PKG_NAME}_${VERSION}_amd64"
rm -rf "${STAGE}"
mkdir -p "${STAGE}/DEBIAN"
mkdir -p "${STAGE}/usr/lib/postgresql/${PG_MAJOR}/lib"
mkdir -p "${STAGE}/usr/share/postgresql/${PG_MAJOR}/extension"

PKG_OUT="target/release/pg_bolt-pg${PG_MAJOR}"
cp "${PKG_OUT}/usr/lib/postgresql/${PG_MAJOR}/lib/pg_bolt.so" "${STAGE}/usr/lib/postgresql/${PG_MAJOR}/lib/"
cp "${PKG_OUT}/usr/share/postgresql/${PG_MAJOR}/extension/"pg_bolt* "${STAGE}/usr/share/postgresql/${PG_MAJOR}/extension/"

cat > "${STAGE}/DEBIAN/control" <<EOF
Package: ${PKG_NAME}
Version: ${VERSION}
Section: database
Priority: optional
Architecture: amd64
Depends: postgresql-${PG_MAJOR}
Maintainer: Craton Software Company <opensource@craton.com.ar>
Homepage: https://github.com/craton-co/pg-bolt
Description: GPU-accelerated analytical SQL extension for PostgreSQL ${PG_MAJOR}
 pg_bolt caches PostgreSQL relations as Apache Arrow RecordBatch snapshots
 in NVIDIA GPU VRAM and executes analytical SQL via craton-bolt.
EOF

mkdir -p "${OUT_DIR}"
dpkg-deb --build --root-owner-group "${STAGE}" "${OUT_DIR}/${PKG_NAME}_${VERSION}~${DISTRO}_amd64.deb"
