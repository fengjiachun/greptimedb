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

export GT_S3_BUCKET="${MINIO_BUCKET}"
export GT_S3_ROOT="${STORE_ROOT}"
export GT_S3_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID}"
export GT_S3_ACCESS_KEY="${MINIO_ACCESS_KEY}"
export GT_S3_REGION="${MINIO_REGION}"
export GT_S3_ENDPOINT_URL="http://127.0.0.1:${MINIO_PORT}"

# Once the root has been found empty, the run ends through `finalize`,
# however it ends: the test is stopped if it still runs, the root is removed
# exactly once and verified by listing it (removing an empty prefix reports
# an error, so the listing is the evidence), and the manifest is printed. A
# failed test keeps its exit status; an interrupted run exits 130. The
# functions are defined before the check so that the traps can be installed
# as the first thing after it.
TEST_PID=""
TEST_STATUS=""
# Written by the test wrapper the moment the test exits, independently of
# the log copy, so that the test's own status survives whatever happens to
# the pipeline afterwards.
STATUS_FILE=$(mktemp -t object-store-wal-minio-status.XXXXXX)
INTERRUPTED=0
CLEANUP=""
FINALIZED=0

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

# A signal stops the test if it is running, and the run then ends through
# `finalize`. A signal that arrives before the test is registered is
# remembered and acted on right after; one that arrives once the test has
# ended changes nothing.
on_signal() {
  if [ -n "${TEST_STATUS}" ] || [ -s "${STATUS_FILE}" ]; then
    return
  fi
  INTERRUPTED=1
  if [ -n "${TEST_PID}" ]; then
    kill -TERM -- "-${TEST_PID}" 2>/dev/null || true
  fi
}

# The test runs in a process group of its own (job control is on), so
# stopping it reaches the test binary as well as the runner; the group is
# waited for until every member is gone, since only the subshell is a child.
# Returns non-zero if a member survived the KILL, in which case the root
# must not be touched.
group_gone() {
  local i
  for i in $(seq 1 "${1}"); do
    if ! kill -0 -- "-${TEST_PID}" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  ! kill -0 -- "-${TEST_PID}" 2>/dev/null
}
stop_test() {
  kill -TERM -- "-${TEST_PID}" 2>/dev/null || true
  wait "${TEST_PID}" 2>/dev/null || true
  if group_gone 100; then
    return 0
  fi
  kill -KILL -- "-${TEST_PID}" 2>/dev/null || true
  group_gone 100
}

# The manifest is evidence, so every field it reports must be present in
# the log; a missing field fails the run even when the test passed.
MISSING=()
require_line() {
  if ! grep -qE "${2}" "${LOG_FILE}"; then
    MISSING+=("${1}")
  fi
}
OPEN_PATTERN='Opened [0-9]+ regions in [^[:space:]]+'
WAL_OBJECTS=""
collect_evidence() {
  for phase in before-writes after-writes after-restart-1 after-flush after-restart-2; do
    require_line "phase ${phase}" "object_store_wal phase=${phase} objects=[0-9]+ bytes=[0-9]+"
  done
  for restart in 1 2; do
    require_line "restart ${restart}" "object_store_wal restart=${restart} wall_ms=[0-9]+"
  done
  # The datanode opens regions once per instance: the first build and two restarts.
  local opened
  opened=$(grep -cE "${OPEN_PATTERN}" "${LOG_FILE}" || true)
  if [ "${opened}" -lt 3 ]; then
    MISSING+=("region open timings (found ${opened}, expected 3)")
  fi
  # The test lists the WAL objects recursively under the configured root prefix
  # and logs every key, which is what shows the layout the store derived below it.
  WAL_OBJECTS=$(grep -oE 'object_store_wal object=[^ ]+ bytes=[0-9]+' "${LOG_FILE}" | sort -u || true)
  if [ -z "${WAL_OBJECTS}" ]; then
    MISSING+=("WAL object keys")
  fi
}

print_manifest() {
  # The image is the one the container runs, which can differ from what the
  # tag resolves to when an older container is reused.
  local image_id image_digests
  image_id=$(docker inspect --format '{{.Image}}' "${MINIO_CONTAINER}" 2>/dev/null || echo unknown)
  image_digests=$(docker inspect --format '{{join .RepoDigests ","}}' "${image_id}" 2>/dev/null || true)
  echo
  echo "== object store WAL MinIO manifest =="
  # The commit checked out while the script ran.
  echo "base commit: $(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "minio image: ${image_id} (${image_digests:-no repo digest}) in container ${MINIO_CONTAINER}"
  echo "bucket: ${MINIO_BUCKET} root ${STORE_ROOT} at ${GT_S3_ENDPOINT_URL}"
  grep -oE 'object_store_wal (phase|restart)=[^"]*' "${LOG_FILE}" | sed -E 's/^object_store_wal //' || true
  grep -oE "${OPEN_PATTERN}" "${LOG_FILE}" | sed -E 's/^/replay: /' || true
  if [ -n "${WAL_OBJECTS}" ]; then
    printf '%s\n' "${WAL_OBJECTS}" | sed -E 's/^object_store_wal object=/object: /'
  fi
  for field in ${MISSING[@]+"${MISSING[@]}"}; do
    echo "missing: ${field}"
  done
  echo "cleanup: ${CLEANUP}"
  echo "test exit status: ${TEST_STATUS:-not run}"
  echo "result: ${RESULT}"
}

finalize() {
  local exit_status=$?
  trap '' INT TERM
  if [ "${FINALIZED}" -eq 1 ]; then
    return
  fi
  FINALIZED=1
  set +e
  # Whatever the wrapper reported, no process of the test may outlive the
  # root: a member of its group still there is stopped first, and if it
  # survives that, the root is left alone.
  if [ -n "${TEST_PID}" ] && ! group_gone 1 && ! stop_test; then
    CLEANUP="root ${STORE_ROOT} not removed, a process of the test survived"
  fi
  cleanup_root
  collect_evidence
  # An interruption is only recorded while the test has not ended, so it
  # decides the status; a test that ended keeps its own.
  if [ "${INTERRUPTED}" -eq 1 ]; then
    STATUS=130
  elif [ -n "${TEST_STATUS}" ] && [ "${TEST_STATUS}" -ne 0 ]; then
    STATUS=${TEST_STATUS}
  elif [ -z "${TEST_STATUS}" ] && [ "${exit_status}" -ne 0 ]; then
    STATUS=${exit_status}
  elif [ "${CLEANUP}" != "root ${STORE_ROOT} removed and verified empty" ] || [ "${#MISSING[@]}" -ne 0 ]; then
    STATUS=1
  else
    STATUS=0
  fi
  if [ "${STATUS}" -eq 0 ]; then
    RESULT=PASS
  else
    RESULT=FAIL
  fi
  print_manifest
  rm -f "${STATUS_FILE}"
  exit "${STATUS}"
}

log "creating bucket ${MINIO_BUCKET} and checking that ${STORE_ROOT} is empty"
mc_run mc mb --ignore-existing "local/${MINIO_BUCKET}" >/dev/null
REMAINING=$(mc_run mc ls --recursive "local/${MINIO_BUCKET}/${STORE_ROOT}/")
if [ -n "${REMAINING}" ]; then
  log "root ${STORE_ROOT} of bucket ${MINIO_BUCKET} already holds objects:"
  echo "${REMAINING}" >&2
  exit 1
fi
trap finalize EXIT
trap on_signal INT TERM

log "running ${TEST_NAME}, log in ${LOG_FILE}"
cd "${ROOT_DIR}"
# An interruption that arrived before the test starts ends the run here.
if [ "${INTERRUPTED}" -eq 1 ]; then
  exit 130
fi
# The test runs in the background, in a process group of its own, so that
# a signal reaches the trap while it runs and stopping it reaches every
# process of the test. The wrapper records the test's own status the moment
# it exits and ends with it, whatever happened to the log copy.
set -m
(
  set +e
  {
    cargo nextest run -p tests-integration --test main \
      -E "test(${TEST_NAME})" --no-capture
    echo "$?" > "${STATUS_FILE}"
  } 2>&1 | tee "${LOG_FILE}"
  exit "$(cat "${STATUS_FILE}" 2>/dev/null || echo 1)"
) &
TEST_PID=$!
set +m
if [ "${INTERRUPTED}" -eq 1 ]; then
  kill -TERM -- "-${TEST_PID}" 2>/dev/null || true
fi
# A signal makes the wait return with a status above 128 without reaping
# the wrapper, so such a status is checked by waiting again: a wrapper that
# is no longer a child was reaped by the first wait, and the status was its
# own; otherwise the second wait returns it.
WRAPPER_STATUS=""
while true; do
  if wait "${TEST_PID}"; then
    WRAPPER_STATUS=0
    break
  else
    WRAPPER_STATUS=$?
  fi
  if [ "${WRAPPER_STATUS}" -le 128 ]; then
    break
  fi
  if wait "${TEST_PID}" 2>/dev/null; then
    WRAPPER_STATUS=0
    break
  else
    AGAIN=$?
  fi
  if [ "${AGAIN}" -eq 127 ]; then
    break
  fi
  WRAPPER_STATUS=${AGAIN}
  if [ "${AGAIN}" -le 128 ]; then
    break
  fi
done
if [ -s "${STATUS_FILE}" ]; then
  TEST_STATUS=$(cat "${STATUS_FILE}")
else
  TEST_STATUS=${WRAPPER_STATUS}
fi
