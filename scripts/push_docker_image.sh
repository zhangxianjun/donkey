#!/usr/bin/env bash
set -euo pipefail

LOCAL_IMAGE_REPO="${LOCAL_IMAGE_REPO:-donkey}"
LOCAL_IMAGE_TAG="${LOCAL_IMAGE_TAG:-latest}"
REMOTE_IMAGE_REPO="${REMOTE_IMAGE_REPO:-${LOCAL_IMAGE_REPO}}"
REMOTE_IMAGE_TAG="${REMOTE_IMAGE_TAG:-${LOCAL_IMAGE_TAG}}"

LOCAL_IMAGE_REF="${LOCAL_IMAGE_REPO}:${LOCAL_IMAGE_TAG}"
REMOTE_IMAGE_REF="${REMOTE_IMAGE_REPO}:${REMOTE_IMAGE_TAG}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found" >&2
  exit 127
fi

if ! docker image inspect "${LOCAL_IMAGE_REF}" >/dev/null 2>&1; then
  echo "local image not found: ${LOCAL_IMAGE_REF}" >&2
  echo "build it first with scripts/build_docker_image.sh" >&2
  exit 1
fi

if [[ "${LOCAL_IMAGE_REF}" != "${REMOTE_IMAGE_REF}" ]]; then
  echo "[docker-tag] ${LOCAL_IMAGE_REF} -> ${REMOTE_IMAGE_REF}" >&2
  docker tag "${LOCAL_IMAGE_REF}" "${REMOTE_IMAGE_REF}"
fi

echo "[docker-push] image=${REMOTE_IMAGE_REF}" >&2
exec docker push "${REMOTE_IMAGE_REF}"
