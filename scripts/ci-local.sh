#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BOLT_ROOT="$(cd "${REPO_ROOT}/../bolt" && pwd)"

if [ ! -d "${BOLT_ROOT}" ]; then
  echo "ERROR: Sibling craton-bolt repository not found at '${BOLT_ROOT}'." >&2
  exit 1
fi

IMAGE_NAME="pg_bolt-ci:latest"
DOCKERFILE="${REPO_ROOT}/docker/Dockerfile.ci"
PG_VERSIONS="${PG_VERSIONS:-pg17}"
SKIP_PGRX="${SKIP_PGRX:-0}"
SKIP_DENY="${SKIP_DENY:-0}"
REBUILD_IMAGE="${REBUILD_IMAGE:-0}"

if [ "${REBUILD_IMAGE}" = "1" ] || ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
  echo "==> Building Docker image ${IMAGE_NAME}..."
  docker build -f "${DOCKERFILE}" -t "${IMAGE_NAME}" "${REPO_ROOT}"
fi

for vol in pg_bolt_ci_cargo pg_bolt_ci_pgrx pg_bolt_ci_target; do
  docker volume inspect "${vol}" >/dev/null 2>&1 || docker volume create "${vol}" >/dev/null
done

docker run --rm --user root --entrypoint /bin/bash \
  -v pg_bolt_ci_cargo:/cache/cargo \
  -v pg_bolt_ci_pgrx:/cache/pgrx \
  -v pg_bolt_ci_target:/cache/target \
  "${IMAGE_NAME}" -c "chown -R ubuntu:ubuntu /cache"

echo "==> Running local CI in Docker (${IMAGE_NAME})..."
docker run --rm \
  --shm-size=1g \
  -e "PG_VERSIONS=${PG_VERSIONS}" \
  -e "SKIP_PGRX=${SKIP_PGRX}" \
  -e "SKIP_DENY=${SKIP_DENY}" \
  -v "${REPO_ROOT}:/workspace/pg_bolt" \
  -v "${BOLT_ROOT}:/workspace/bolt" \
  -v pg_bolt_ci_cargo:/cache/cargo \
  -v pg_bolt_ci_pgrx:/cache/pgrx \
  -v pg_bolt_ci_target:/cache/target \
  "${IMAGE_NAME}"
