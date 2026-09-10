#!/usr/bin/env bash
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the object store WAL restart test of `tests-integration` against a
# MinIO server started in Docker and prints a manifest of the run.
#
# The script reuses a running MinIO container, stores under a root of its
# own in the bucket so it never touches the objects of other runs or users,
# exports the `GT_S3_*` environment the S3-backed tests expect, fails unless
# the test log carries every field of the manifest, and removes exactly its
# root once the test has run, however it ended.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${0}")" >/dev/null 2>&1 && pwd)
ROOT_DIR=$(dirname "${SCRIPT_DIR}")

MINIO_IMAGE="${MINIO_IMAGE:-minio/minio}"
MC_IMAGE="${MC_IMAGE:-minio/mc}"
MINIO_CONTAINER="${MINIO_CONTAINER:-greptimedb-object-store-wal-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_BUCKET="${MINIO_BUCKET:-greptime-object-store-wal}"
MINIO_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID:-superpower_ci_user}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-superpower_password}"
MINIO_REGION="${MINIO_REGION:-us-west-2}"
TEST_NAME="test_standalone_object_store_wal_survives_restarts_on_s3"
LOG_FILE="${LOG_FILE:-$(mktemp -t object-store-wal-minio.XXXXXX)}"

log() {
  echo "[object-store-wal-minio] $*" >&2
}

if ! docker ps --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}"; then
    log "starting the existing container ${MINIO_CONTAINER}"
    docker start "${MINIO_CONTAINER}" >/dev/null
  else
    log "creating container ${MINIO_CONTAINER} from ${MINIO_IMAGE}"
    docker run -d --name "${MINIO_CONTAINER}" \
      -p "${MINIO_PORT}:9000" \
      -e "MINIO_ROOT_USER=${MINIO_ACCESS_KEY_ID}" \
      -e "MINIO_ROOT_PASSWORD=${MINIO_ACCESS_KEY}" \
      "${MINIO_IMAGE}" server /data >/dev/null
  fi
fi

log "waiting for MinIO on port ${MINIO_PORT}"
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${MINIO_PORT}/minio/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "http://127.0.0.1:${MINIO_PORT}/minio/health/live" >/dev/null

# Runs a MinIO client command in the network namespace of the server, so it
# reaches it without host networking. The credentials reach the inner shell
# as environment variables and the command as positional parameters, so no
# value is parsed as shell source.
mc_run() {
  docker run --rm --network "container:${MINIO_CONTAINER}" \
    -e "MC_ACCESS_KEY_ID=${MINIO_ACCESS_KEY_ID}" -e "MC_SECRET_KEY=${MINIO_ACCESS_KEY}" \
    --entrypoint sh "${MC_IMAGE}" -c '
    mc alias set local http://127.0.0.1:9000 "${MC_ACCESS_KEY_ID}" "${MC_SECRET_KEY}" >/dev/null && "$@"
  ' sh "$@"
}

# The bucket name becomes part of the object paths handed to mc, so it is
# limited to the characters a bucket name may hold before the first command
# runs.
if ! [[ "${MINIO_BUCKET}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
  log "invalid bucket name: ${MINIO_BUCKET}"
  exit 1
fi
# Every run stores under its own root of the bucket, so nothing outside the
# root is read, counted or removed; the root is random so that concurrent
# runs, on this host or another, never share one.
STORE_ROOT="object-store-wal-$(uuidgen | tr '[:upper:]' '[:lower:]')"
log "creating bucket ${MINIO_BUCKET} and checking that ${STORE_ROOT} is empty"
mc_run mc mb --ignore-existing "local/${MINIO_BUCKET}" >/dev/null
REMAINING=$(mc_run mc ls --recursive "local/${MINIO_BUCKET}/${STORE_ROOT}/")
if [ -n "${REMAINING}" ]; then
  log "root ${STORE_ROOT} of bucket ${MINIO_BUCKET} already holds objects:"
  echo "${REMAINING}" >&2
  exit 1
fi

export GT_S3_BUCKET="${MINIO_BUCKET}"
export GT_S3_ROOT="${STORE_ROOT}"
export GT_S3_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID}"
export GT_S3_ACCESS_KEY="${MINIO_ACCESS_KEY}"
export GT_S3_REGION="${MINIO_REGION}"
export GT_S3_ENDPOINT_URL="http://127.0.0.1:${MINIO_PORT}"

# The root is removed however the test ended, and only the root: the
# removal is verified by listing it, since removing an empty prefix reports
# an error. It runs once, whether the test finished or the run was
# interrupted.
CLEANUP=""
cleanup_root() {
  if [ -n "${CLEANUP}" ]; then
    return
  fi
  mc_run mc rm --recursive --force "local/${MINIO_BUCKET}/${STORE_ROOT}/" >/dev/null 2>&1 || true
  if REMAINING=$(mc_run mc ls --recursive "local/${MINIO_BUCKET}/${STORE_ROOT}/") && [ -z "${REMAINING}" ]; then
    CLEANUP="root ${STORE_ROOT} removed and verified empty"
  else
    echo "${REMAINING}" >&2
    CLEANUP="root ${STORE_ROOT} still holds objects"
  fi
}
# An interrupted run stops the test before the root is removed, so that no
# writer is left behind to recreate it.
TEST_PID=""
on_interrupt() {
  trap '' INT TERM
  if [ -n "${TEST_PID}" ]; then
    pkill -TERM -P "${TEST_PID}" 2>/dev/null || true
    wait "${TEST_PID}" 2>/dev/null || true
  fi
  cleanup_root
  log "interrupted, ${CLEANUP}"
  exit 130
}
trap on_interrupt INT TERM

log "running ${TEST_NAME}, log in ${LOG_FILE}"
cd "${ROOT_DIR}"
# The test runs in the background so that a signal reaches the trap while
# it runs; the subshell inherits pipefail, so its status is the test's.
(
  cargo nextest run -p tests-integration --test main \
    -E "test(${TEST_NAME})" --no-capture 2>&1 | tee "${LOG_FILE}"
) &
TEST_PID=$!
if wait "${TEST_PID}"; then
  STATUS=0
else
  STATUS=$?
fi
TEST_PID=""
trap - INT TERM

cleanup_root
if [ "${CLEANUP}" != "root ${STORE_ROOT} removed and verified empty" ]; then
  STATUS=1
fi

# The manifest is evidence, so every field it reports must be present in
# the log; a missing field fails the run even when the test passed.
MISSING=()
require_line() {
  if ! grep -qE "${2}" "${LOG_FILE}"; then
    MISSING+=("${1}")
  fi
}
for phase in before-writes after-writes after-restart-1 after-flush after-restart-2; do
  require_line "phase ${phase}" "object_store_wal phase=${phase} objects=[0-9]+ bytes=[0-9]+"
done
for restart in 1 2; do
  require_line "restart ${restart}" "object_store_wal restart=${restart} wall_ms=[0-9]+"
done
# The datanode opens regions once per instance: the first build and two restarts.
OPEN_PATTERN='Opened [0-9]+ regions in [^[:space:]]+'
OPENED=$(grep -cE "${OPEN_PATTERN}" "${LOG_FILE}" || true)
if [ "${OPENED}" -lt 3 ]; then
  MISSING+=("region open timings (found ${OPENED}, expected 3)")
fi
# The test lists the WAL objects recursively under the configured root prefix
# and logs every key, which is what shows the layout the store derived below it.
WAL_OBJECTS=$(grep -oE 'object_store_wal object=[^ ]+ bytes=[0-9]+' "${LOG_FILE}" | sort -u || true)
if [ -z "${WAL_OBJECTS}" ]; then
  MISSING+=("WAL object keys")
fi

if [ "${STATUS}" -eq 0 ] && [ "${#MISSING[@]}" -eq 0 ]; then
  RESULT=PASS
else
  RESULT=FAIL
  STATUS=1
fi

# The image is the one the container runs, which can differ from what the
# tag resolves to when an older container is reused.
MINIO_IMAGE_ID=$(docker inspect --format '{{.Image}}' "${MINIO_CONTAINER}")
MINIO_IMAGE_DIGESTS=$(docker inspect --format '{{join .RepoDigests ","}}' "${MINIO_IMAGE_ID}")

echo
echo "== object store WAL MinIO manifest =="
# The commit checked out while the script ran.
echo "base commit: $(git -C "${ROOT_DIR}" rev-parse HEAD)"
echo "minio image: ${MINIO_IMAGE_ID} (${MINIO_IMAGE_DIGESTS:-no repo digest}) in container ${MINIO_CONTAINER}"
echo "bucket: ${MINIO_BUCKET} root ${STORE_ROOT} at ${GT_S3_ENDPOINT_URL}"
grep -oE 'object_store_wal (phase|restart)=[^"]*' "${LOG_FILE}" | sed -E 's/^object_store_wal //' || true
grep -oE "${OPEN_PATTERN}" "${LOG_FILE}" | sed -E 's/^/replay: /' || true
printf '%s\n' "${WAL_OBJECTS}" | sed -E 's/^object_store_wal object=/object: /'
for field in ${MISSING[@]+"${MISSING[@]}"}; do
  echo "missing: ${field}"
done
echo "cleanup: ${CLEANUP}"
echo "test exit status: ${STATUS}"
echo "result: ${RESULT}"
exit "${STATUS}"
