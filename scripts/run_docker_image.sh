#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_REPO="${IMAGE_REPO:-${REMOTE_IMAGE_REPO:-${LOCAL_IMAGE_REPO:-donkey}}}"
IMAGE_TAG="${IMAGE_TAG:-${REMOTE_IMAGE_TAG:-${LOCAL_IMAGE_TAG:-latest}}}"
IMAGE_REF="${IMAGE_REPO}:${IMAGE_TAG}"
CONTAINER_NAME="${CONTAINER_NAME:-donkey}"
HOST_PORT="${HOST_PORT:-8866}"
CONTAINER_PORT="${CONTAINER_PORT:-8866}"
DETACH="${DETACH:-0}"
REMOVE_ON_EXIT="${REMOVE_ON_EXIT:-1}"
RUN_AS_CALLER="${RUN_AS_CALLER:-1}"
PULL_BEFORE_RUN="${PULL_BEFORE_RUN:-0}"
DOCKER_NETWORK="${DOCKER_NETWORK:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found" >&2
  exit 127
fi

mkdir -p \
  "${REPO_ROOT}/data" \
  "${REPO_ROOT}/db" \
  "${REPO_ROOT}/logs"

if [[ "${PULL_BEFORE_RUN}" == "1" ]]; then
  echo "[docker-pull] image=${IMAGE_REF}" >&2
  docker pull "${IMAGE_REF}"
fi

run_args=(run)

if [[ "${REMOVE_ON_EXIT}" == "1" ]]; then
  run_args+=(--rm)
fi

if [[ "${DETACH}" == "1" ]]; then
  run_args+=(-d)
else
  run_args+=(-it)
fi

run_args+=(
  --name "${CONTAINER_NAME}"
  -e PYTHONDONTWRITEBYTECODE=1
  -e PYTHONUNBUFFERED=1
  -v "${REPO_ROOT}/data:/app/data"
  -v "${REPO_ROOT}/db:/app/db"
  -v "${REPO_ROOT}/logs:/app/logs"
  -v "${REPO_ROOT}/config:/app/config:ro"
  -v "${REPO_ROOT}/sql:/app/sql:ro"
)

if [[ -n "${HOST_PORT}" && -n "${CONTAINER_PORT}" ]]; then
  run_args+=(-p "${HOST_PORT}:${CONTAINER_PORT}")
fi

if [[ -n "${DOCKER_NETWORK}" ]]; then
  run_args+=(--network "${DOCKER_NETWORK}")
fi

if [[ "${RUN_AS_CALLER}" == "1" ]]; then
  run_args+=(--user "$(id -u):$(id -g)")
fi

if [[ "$#" -gt 0 ]]; then
  cmd=("$@")
else
  cmd=(sh)
fi

echo "[docker-run] image=${IMAGE_REF} command=${cmd[*]}" >&2
exec docker "${run_args[@]}" "${IMAGE_REF}" "${cmd[@]}"
