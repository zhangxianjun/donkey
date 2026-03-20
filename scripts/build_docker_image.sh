#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOCAL_IMAGE_REPO="${LOCAL_IMAGE_REPO:-donkey}"
LOCAL_IMAGE_TAG="${LOCAL_IMAGE_TAG:-latest}"
IMAGE_REF="${LOCAL_IMAGE_REPO}:${LOCAL_IMAGE_TAG}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-${REPO_ROOT}/Dockerfile}"
BUILD_CONTEXT="${BUILD_CONTEXT:-${REPO_ROOT}}"
PIP_PACKAGES="${PIP_PACKAGES:-duckdb}"
BUILD_PLATFORM="${BUILD_PLATFORM:-}"
NO_CACHE="${NO_CACHE:-0}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found" >&2
  exit 127
fi

build_args=(
  build
  -f "${DOCKERFILE_PATH}"
  -t "${IMAGE_REF}"
  --build-arg "PIP_PACKAGES=${PIP_PACKAGES}"
)

if [[ -n "${BUILD_PLATFORM}" ]]; then
  build_args+=(--platform "${BUILD_PLATFORM}")
fi

if [[ "${NO_CACHE}" == "1" ]]; then
  build_args+=(--no-cache)
fi

build_args+=("${BUILD_CONTEXT}")

echo "[docker-build] image=${IMAGE_REF} dockerfile=${DOCKERFILE_PATH}" >&2
exec docker "${build_args[@]}"
