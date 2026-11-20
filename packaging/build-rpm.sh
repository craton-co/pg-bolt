#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Builds an .rpm package for pg_bolt_<PG_MAJOR> inside Rocky Linux 9.
set -euo pipefail

PG_MAJOR="${1:?Usage: $0 <pg_major> <version> <out_dir>}"
VERSION="${2:?}"
OUT_DIR="${3:?}"

export CUDA_HOME=/usr/local/cuda
export PATH="/root/.cargo/bin:/usr/pgsql-${PG_MAJOR}/bin:${CUDA_HOME}/bin:${PATH}"

dnf install -y -q epel-release
dnf install -y -q \
    gcc gcc-c++ clang clang-devel openssl-devel pkgconfig \
    rpm-build git curl make \
    "https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
dnf -qy module disable postgresql || true
dnf install -y -q "postgresql${PG_MAJOR}-server" "postgresql${PG_MAJOR}-devel"

if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain 1.85.0 --profile minimal
fi
if ! command -v cargo-pgrx >/dev/null 2>&1; then
    cargo install cargo-pgrx --version 0.19.4 --locked
fi
cargo pgrx init "--pg${PG_MAJOR}" "/usr/pgsql-${PG_MAJOR}/bin/pg_config"

if [ ! -d ../bolt ]; then
    sed -i.bak 's|path = "../bolt", ||g' Cargo.toml && rm -f Cargo.toml.bak
fi

cargo pgrx package \
    --pg-config "/usr/pgsql-${PG_MAJOR}/bin/pg_config" \
    --no-default-features \
    --features "pg${PG_MAJOR} cuda"

PKG_OUT="target/release/pg_bolt-pg${PG_MAJOR}"
RPM_TOP="/tmp/rpmbuild"
rm -rf "${RPM_TOP}"
mkdir -p "${RPM_TOP}"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

cat > "${RPM_TOP}/SPECS/pg_bolt_${PG_MAJOR}.spec" <<EOF
Name:           pg_bolt_${PG_MAJOR}
Version:        ${VERSION}
Release:        1%{?dist}
Summary:        GPU-accelerated analytical SQL extension for PostgreSQL ${PG_MAJOR}
License:        AGPL-3.0-only
URL:            https://github.com/craton-co/pg_bolt
Requires:       postgresql${PG_MAJOR}-server

%description
pg_bolt caches PostgreSQL relations as Apache Arrow RecordBatch snapshots
in NVIDIA GPU VRAM and executes analytical SQL via craton-bolt.

%install
mkdir -p %{buildroot}/usr/pgsql-${PG_MAJOR}/lib
mkdir -p %{buildroot}/usr/pgsql-${PG_MAJOR}/share/extension
cp -a ${PWD}/${PKG_OUT}/usr/pgsql-${PG_MAJOR}/lib/pg_bolt.so %{buildroot}/usr/pgsql-${PG_MAJOR}/lib/
cp -a ${PWD}/${PKG_OUT}/usr/pgsql-${PG_MAJOR}/share/extension/pg_bolt* %{buildroot}/usr/pgsql-${PG_MAJOR}/share/extension/

%files
/usr/pgsql-${PG_MAJOR}/lib/pg_bolt.so
/usr/pgsql-${PG_MAJOR}/share/extension/pg_bolt*
EOF

rpmbuild --define "_topdir ${RPM_TOP}" -bb "${RPM_TOP}/SPECS/pg_bolt_${PG_MAJOR}.spec"
mkdir -p "${OUT_DIR}"
cp "${RPM_TOP}/RPMS/x86_64/"*.rpm "${OUT_DIR}/"
