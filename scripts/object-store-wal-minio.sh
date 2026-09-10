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
# Runs a command in a process group of its own, so that a signal sent to the
# script's whole group, which the script may ignore or handle, does not end
# the command; a wait the signal cut short is repeated while the command
# lives. Every command whose result decides the outcome of the run goes
# through it: the client, and the tools that read the log for the manifest.
isolated() {
  local pid status
  set -m
  "$@" &
  pid=$!
  set +m
  while true; do
    if wait "${pid}" 2>/dev/null; then
      return 0
    else
      status=$?
    fi
    if [ "${status}" -le 128 ] || ! kill -0 "${pid}" 2>/dev/null; then
      return "${status}"
    fi
  done
}
# Captures the output of an isolated command through a file, since a
# command substitution would run it in a subshell without job control.
CAPTURE_FILE=$(mktemp -t object-store-wal-minio-capture.XXXXXX)
CAPTURED=""
# Until `finalize` takes over the exit, the temporary files are the only
# thing to clean up.
trap 'rm -f "${STATUS_FILE:-}" "${CAPTURE_FILE}"' EXIT
capture() {
  local status=0
  : > "${CAPTURE_FILE}"
  isolated "$@" > "${CAPTURE_FILE}" || status=$?
  # Read by the shell itself: a command substitution would fork a subshell
  # a signal to the group could end before it returns.
  CAPTURED=""
  IFS= read -r -d '' CAPTURED < "${CAPTURE_FILE}" || true
  CAPTURED=${CAPTURED%$'\n'}
  return "${status}"
}
mc_run() {
  isolated docker run --rm --network "container:${MINIO_CONTAINER}" \
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
# functions are defined before the check because the traps are installed
# before it.
ROOT_OWNED=0
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
  if [ "${ROOT_OWNED}" -eq 0 ]; then
    CLEANUP="root ${STORE_ROOT} not touched, never found empty by this run"
    return
  fi
  mc_run mc rm --recursive --force "local/${MINIO_BUCKET}/${STORE_ROOT}/" >/dev/null 2>&1 || true
  if capture mc_run mc ls --recursive "local/${MINIO_BUCKET}/${STORE_ROOT}/" && [ -z "${CAPTURED}" ]; then
    CLEANUP="root ${STORE_ROOT} removed and verified empty"
  elif [ -n "${CAPTURED}" ]; then
    echo "${CAPTURED}" >&2
    CLEANUP="root ${STORE_ROOT} still holds objects"
  else
    CLEANUP="root ${STORE_ROOT} removed but not verified, the listing failed"
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
    sleep 0.1 || true
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
# Prints the lines of its argument once each, in order of first appearance,
# and prints the lines of a text with a prefix removed from their start and
# another put in front; both are the shell's own work, so no reader needs a
# pipeline or a shell other than the running one.
unique_lines() {
  local line seen="" out=""
  while IFS= read -r line; do
    [ -n "${line}" ] || continue
    case "${seen}" in
      *"|${line}|"*) continue ;;
    esac
    seen="${seen}|${line}|"
    out="${out}${line}
"
  done <<LINES
${1}
LINES
  printf '%s' "${out%
}"
}
prefix_lines() {
  local line out=""
  while IFS= read -r line; do
    [ -n "${line}" ] || continue
    out="${out}${1}${line#"${2}"}
"
  done <<LINES
${3}
LINES
  printf '%s' "${out%
}"
}

MISSING=()
require_line() {
  if ! isolated grep -qE "${2}" "${LOG_FILE}"; then
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
  # A reader that fails is a missing field, whatever it printed; grep
  # reports no match as 1, which is not a failure of the reader.
  local opened status=0
  capture grep -cE "${OPEN_PATTERN}" "${LOG_FILE}" || status=$?
  opened=${CAPTURED:-0}
  if [ "${status}" -gt 1 ]; then
    MISSING+=("region open timings (the reader failed with status ${status})")
  elif [ "${opened}" -lt 3 ]; then
    MISSING+=("region open timings (found ${opened}, expected 3)")
  fi
  # The test lists the WAL objects recursively under the configured root prefix
  # and logs every key, which is what shows the layout the store derived below it.
  status=0
  # Every reader is a single grep, so that a status of 1 can only mean no
  # match; the keys are made unique by the shell itself.
  capture grep -oE 'object_store_wal object=[^ ]+ bytes=[0-9]+' "${LOG_FILE}" || status=$?
  WAL_OBJECTS=$(unique_lines "${CAPTURED}")
  if [ "${status}" -gt 1 ]; then
    MISSING+=("WAL object keys (the reader failed with status ${status})")
  elif [ -z "${WAL_OBJECTS}" ]; then
    MISSING+=("WAL object keys")
  fi
}

# The manifest's own lines are collected before the verdict, so that a
# reader or a provenance command that fails counts against the run, and
# printed after it with nothing left to run.
IMAGE_ID=unknown
IMAGE_DIGESTS=""
BASE_COMMIT=unknown
PHASE_LINES=""
REPLAY_LINES=""
collect_manifest() {
  local status
  # The image is the one the container runs, which can differ from what the
  # tag resolves to when an older container is reused.
  status=0
  capture docker inspect --format '{{.Image}}' "${MINIO_CONTAINER}" 2>/dev/null || status=$?
  if [ "${status}" -eq 0 ]; then
    IMAGE_ID=${CAPTURED:-unknown}
  else
    MISSING+=("manifest: minio image (docker inspect failed with status ${status})")
  fi
  if [ "${IMAGE_ID}" != unknown ]; then
    status=0
    capture docker inspect --format '{{join .RepoDigests ","}}' "${IMAGE_ID}" 2>/dev/null || status=$?
    if [ "${status}" -eq 0 ]; then
      IMAGE_DIGESTS=${CAPTURED}
    else
      MISSING+=("manifest: minio image digests (docker inspect failed with status ${status})")
    fi
  fi
  # The commit checked out while the script ran.
  status=0
  capture git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || status=$?
  if [ "${status}" -eq 0 ]; then
    BASE_COMMIT=${CAPTURED:-unknown}
  else
    MISSING+=("manifest: base commit (git failed with status ${status})")
  fi
  status=0
  # The formatting is the shell's own, so a reader is a single grep whose
  # status of 1 can only mean no match.
  capture grep -oE 'object_store_wal (phase|restart)=[^"]*' "${LOG_FILE}" || status=$?
  PHASE_LINES=$(prefix_lines "" "object_store_wal " "${CAPTURED}")
  if [ "${status}" -gt 1 ]; then
    MISSING+=("manifest: phase lines (the reader failed with status ${status})")
  fi
  status=0
  capture grep -oE "${OPEN_PATTERN}" "${LOG_FILE}" || status=$?
  REPLAY_LINES=$(prefix_lines "replay: " "" "${CAPTURED}")
  if [ "${status}" -gt 1 ]; then
    MISSING+=("manifest: replay lines (the reader failed with status ${status})")
  fi
}

print_manifest() {
  local line
  echo
  echo "== object store WAL MinIO manifest =="
  echo "base commit: ${BASE_COMMIT}"
  echo "minio image: ${IMAGE_ID} (${IMAGE_DIGESTS:-no repo digest}) in container ${MINIO_CONTAINER}"
  echo "bucket: ${MINIO_BUCKET} root ${STORE_ROOT} at ${GT_S3_ENDPOINT_URL}"
  if [ -n "${PHASE_LINES}" ]; then
    printf '%s\n' "${PHASE_LINES}"
  fi
  if [ -n "${REPLAY_LINES}" ]; then
    printf '%s\n' "${REPLAY_LINES}"
  fi
  if [ -n "${WAL_OBJECTS}" ]; then
    while IFS= read -r line; do
      echo "object: ${line#object_store_wal object=}"
    done <<OBJECTS
${WAL_OBJECTS}
OBJECTS
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
  if [ -z "${TEST_STATUS}" ] && [ -s "${STATUS_FILE}" ]; then
    TEST_STATUS=$(cat "${STATUS_FILE}")
  fi
  # Whatever the wrapper reported, no process of the test may outlive the
  # root: a member of its group still there is stopped first, and if it
  # survives that, the root is left alone.
  if [ -n "${TEST_PID}" ] && ! group_gone 1 && ! stop_test; then
    CLEANUP="root ${STORE_ROOT} not removed, a process of the test survived"
  fi
  cleanup_root
  collect_evidence
  collect_manifest
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
  rm -f "${STATUS_FILE}" "${CAPTURE_FILE}"
  exit "${STATUS}"
}

# The traps are installed before the root is checked, so that no statement
# after a successful check runs unprotected; until the check has passed,
# `finalize` leaves the root alone.
trap finalize EXIT
trap on_signal INT TERM
log "creating bucket ${MINIO_BUCKET} and checking that ${STORE_ROOT} is empty"
mc_run mc mb --ignore-existing "local/${MINIO_BUCKET}" >/dev/null
capture mc_run mc ls --recursive "local/${MINIO_BUCKET}/${STORE_ROOT}/"
REMAINING=${CAPTURED}
if [ -n "${REMAINING}" ]; then
  log "root ${STORE_ROOT} of bucket ${MINIO_BUCKET} already holds objects:"
  echo "${REMAINING}" >&2
  exit 1
fi
ROOT_OWNED=1

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
# The wrapper is watched rather than waited for, so that a signal never
# leaves the script blocked and the test's own end is noticed as soon as it
# records its status. From then on the log copy may drain for a bounded
# time; a process of the test that keeps the copy from ending is stopped
# when that time is up, and the recorded status is kept. Liveness comes
# from the builtin kill, which no signal can interrupt; ps only tells a
# zombie apart, and a ps that a signal cut short leaves the wrapper counted
# as alive until the next look. The clock is the shell's own.
DRAIN_SECONDS=30
DRAIN_DEADLINE=""
while kill -0 "${TEST_PID}" 2>/dev/null; do
  # The substitution's own shell can be ended by a signal to the group; an
  # empty state then counts as alive, like a cut-short ps.
  STATE=$(ps -o stat= -p "${TEST_PID}" 2>/dev/null | tr -d ' ' || true) || STATE=""
  case "${STATE}" in
    Z*) break ;;
  esac
  if [ -s "${STATUS_FILE}" ]; then
    if [ -z "${DRAIN_DEADLINE}" ]; then
      DRAIN_DEADLINE=$((SECONDS + DRAIN_SECONDS))
    elif [ "${SECONDS}" -ge "${DRAIN_DEADLINE}" ]; then
      log "the test ended ${DRAIN_SECONDS}s ago and its output has not drained, stopping what is left of it"
      if ! stop_test; then
        CLEANUP="root ${STORE_ROOT} not removed, a process of the test survived"
      fi
      break
    fi
  fi
  # A signal to the whole foreground group reaches this sleep as well; the
  # handler decides what it means, the interrupted sleep does not.
  sleep 0.1 || true
done
if wait "${TEST_PID}" 2>/dev/null; then
  WRAPPER_STATUS=0
else
  WRAPPER_STATUS=$?
fi
if [ -s "${STATUS_FILE}" ]; then
  TEST_STATUS=$(cat "${STATUS_FILE}")
else
  TEST_STATUS=${WRAPPER_STATUS}
fi
