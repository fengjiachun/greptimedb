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

# Crash gate of the object store WAL: kills a standalone server with SIGKILL
# while clients are inserting, restarts it on the same bucket and data home,
# and checks that every acknowledged row came back exactly once.
#
# Usage: scripts/object-store-wal-crash-gate.sh
#
# Needs Docker (MinIO runs in a container that is reused across runs), curl
# and python3. The binary is built with `cargo build --bin greptime` unless
# GREPTIME_BIN points at one. Knobs, all optional:
#   CYCLES          kill/restart cycles on the same bucket, default 5
#   WRITERS         concurrent writers, default 8; each owns a stripe of the
#                   sequence numbers (writer w inserts w, w+WRITERS, ...), and
#                   a sequential writer is acknowledged about once per WAL
#                   flush interval, so one writer alone yields few rows
#   KILL_MIN_SECS, KILL_MAX_SECS
#                   the kill fires after a random delay in this window once
#                   the writers started, default 2 and 6
#   GREPTIME_PORT_BASE
#                   HTTP port of the server; gRPC, MySQL and PostgreSQL take
#                   the next three ports, default 24000
#   RUN_DIR         where the config, logs, acked files and manifest go,
#                   default a fresh temporary directory
#   MINIO_*         same as scripts/object-store-wal-minio.sh
#
# Every cycle writes with WRITERS clients that append a sequence number to
# acked.log only after the server acknowledged its INSERT, kills the server,
# restarts it, and reads the table back. The cycle passes when every
# acknowledged sequence number is present exactly once, no sequence number
# is present twice, and every present sequence number that was not
# acknowledged is a write that was in flight at the kill (the last attempt of
# a writer). Nothing is cleaned between cycles, so replay accumulates.
#
# PASS means every cycle passed and the manifest carries every required
# field; anything else is FAIL with a non-zero exit status. The run directory
# is printed and kept in both cases; the objects of a passed run are removed
# from the bucket, those of a failed run are kept as evidence.
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
CYCLES="${CYCLES:-5}"
WRITERS="${WRITERS:-8}"
KILL_MIN_SECS="${KILL_MIN_SECS:-2}"
KILL_MAX_SECS="${KILL_MAX_SECS:-6}"
GREPTIME_PORT_BASE="${GREPTIME_PORT_BASE:-24000}"
RUN_DIR="${RUN_DIR:-$(mktemp -d -t object-store-wal-crash-gate.XXXXXX)}"
WAL_PREFIX="wal"
READY_TIMEOUT_SECS=300

HTTP_ADDR="127.0.0.1:${GREPTIME_PORT_BASE}"
SQL_URL="http://${HTTP_ADDR}/v1/sql"
MANIFEST="${RUN_DIR}/manifest.txt"
ACKED="${RUN_DIR}/acked.log"
ATTEMPTED="${RUN_DIR}/attempted.log"
SERVER_PID=""
WRITER_PIDS=""
RESULT=FAIL
STORE_ROOT=""
BINARY_COMMIT=""
BINARY_SHA256=""
MISSING=()

log() {
  echo "[object-store-wal-crash-gate] $*" >&2
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

cleanup() {
  if [ -n "${WRITER_PIDS}" ]; then
    # shellcheck disable=SC2086
    kill -KILL ${WRITER_PIDS} 2>/dev/null || true
  fi
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "killing the leftover server ${SERVER_PID}"
    kill -KILL "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  log "run directory: ${RUN_DIR}"
}
trap cleanup EXIT

mkdir -p "${RUN_DIR}"
: > "${MANIFEST}"
: > "${ACKED}"
: > "${ATTEMPTED}"

# Prints the manifest: the run identity, then every field collected so far.
print_manifest() {
  # The image is the one the container runs, which can differ from what the
  # tag resolves to when an older container is reused.
  local image_id="" digests=""
  if docker ps -a --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}"; then
    image_id=$(docker inspect --format '{{.Image}}' "${MINIO_CONTAINER}")
    digests=$(docker inspect --format '{{join .RepoDigests ","}}' "${image_id}")
  fi
  {
    echo
    echo "== object store WAL crash gate manifest =="
    echo "base commit: $(git -C "${ROOT_DIR}" rev-parse HEAD)"
    echo "binary: ${GREPTIME_BIN:-not built} commit ${BINARY_COMMIT:-unknown} sha256 ${BINARY_SHA256:-unknown}"
    echo "minio image: ${image_id:-unknown} (${digests:-no repo digest}) in container ${MINIO_CONTAINER}"
    echo "bucket: ${MINIO_BUCKET} root ${STORE_ROOT:-unknown} wal prefix ${WAL_PREFIX} at http://127.0.0.1:${MINIO_PORT}"
    echo "cycles: ${CYCLES} writers: ${WRITERS} kill window: ${KILL_MIN_SECS}s to ${KILL_MAX_SECS}s"
    cat "${MANIFEST}"
    for field in ${MISSING[@]+"${MISSING[@]}"}; do
      echo "missing: ${field}"
    done
    echo "result: ${RESULT}"
  } | tee "${RUN_DIR}/manifest-final.txt"
}

# Fails the run right away; the manifest keeps what was collected so far.
fail() {
  log "FAIL: $*"
  echo "fail: $*" >> "${MANIFEST}"
  print_manifest
  exit 1
}

if curl -sf "http://${HTTP_ADDR}/health" >/dev/null 2>&1; then
  fail "something already answers on http://${HTTP_ADDR}; set GREPTIME_PORT_BASE"
fi

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
# reaches it without host networking.
mc_run() {
  docker run --rm --network "container:${MINIO_CONTAINER}" --entrypoint sh "${MC_IMAGE}" -c "
    mc alias set local http://127.0.0.1:9000 '${MINIO_ACCESS_KEY_ID}' '${MINIO_ACCESS_KEY}' >/dev/null &&
    $*
  "
}

# Every run stores under its own root of the bucket, so runs never see each
# other's objects and a failed run keeps its evidence.
STORE_ROOT="crash-gate-$(date -u +%Y%m%dT%H%M%SZ)-$$"
log "creating bucket ${MINIO_BUCKET} and checking that ${STORE_ROOT} is empty"
mc_run "mc mb --ignore-existing local/${MINIO_BUCKET}" >/dev/null
mc_run "mc rm --recursive --force local/${MINIO_BUCKET}/${STORE_ROOT}/" >/dev/null 2>&1 || true
REMAINING=$(mc_run "mc ls --recursive local/${MINIO_BUCKET}/${STORE_ROOT}/")
if [ -n "${REMAINING}" ]; then
  echo "${REMAINING}" >&2
  fail "root ${STORE_ROOT} of bucket ${MINIO_BUCKET} still holds objects"
fi

# Prints `objects=N bytes=N` of the WAL objects under the prefix.
wal_objects() {
  mc_run "mc ls --recursive --json local/${MINIO_BUCKET}/${STORE_ROOT}/${WAL_PREFIX}/objects/" 2>/dev/null |
    python3 -c '
import json, sys
count = 0
size = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    entry = json.loads(line)
    if entry.get("type") == "file":
        count += 1
        size += int(entry.get("size", 0))
print(f"objects={count} bytes={size}")
'
}

if [ -z "${GREPTIME_BIN:-}" ]; then
  log "building the greptime binary"
  (cd "${ROOT_DIR}" && cargo build --bin greptime >"${RUN_DIR}/cargo-build.log" 2>&1) ||
    fail "cargo build failed, see ${RUN_DIR}/cargo-build.log"
  GREPTIME_BIN="${ROOT_DIR}/target/debug/greptime"
fi
[ -x "${GREPTIME_BIN}" ] || fail "${GREPTIME_BIN} is not executable"
BINARY_COMMIT=$("${GREPTIME_BIN}" --version | sed -nE 's/^commit: *//p' | head -1)
BINARY_SHA256=$(shasum -a 256 "${GREPTIME_BIN}" | cut -d' ' -f1)

CONFIG="${RUN_DIR}/standalone.toml"
DATA_HOME="${RUN_DIR}/data"
LOG_DIR="${RUN_DIR}/logs"
cat > "${CONFIG}" <<EOF
[http]
addr = "${HTTP_ADDR}"

[grpc]
bind_addr = "127.0.0.1:$((GREPTIME_PORT_BASE + 1))"

[mysql]
addr = "127.0.0.1:$((GREPTIME_PORT_BASE + 2))"

[postgres]
addr = "127.0.0.1:$((GREPTIME_PORT_BASE + 3))"

[wal]
provider = "experimental_object_store"
prefix = "${WAL_PREFIX}"

[storage]
data_home = "${DATA_HOME}"
type = "S3"
bucket = "${MINIO_BUCKET}"
root = "${STORE_ROOT}"
access_key_id = "${MINIO_ACCESS_KEY_ID}"
secret_access_key = "${MINIO_ACCESS_KEY}"
endpoint = "http://127.0.0.1:${MINIO_PORT}"
region = "${MINIO_REGION}"

[logging]
dir = "${LOG_DIR}"
append_stdout = true
EOF

# Starts the server with its output in `server-<n>.log` and returns once the
# health endpoint answers, which happens after the regions were opened, so
# replay is complete by then. Sets START_WALL_MS.
start_server() {
  local name=$1
  SERVER_LOG="${RUN_DIR}/server-${name}.log"
  local started
  started=$(now_ms)
  "${GREPTIME_BIN}" standalone start -c "${CONFIG}" >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  local deadline=$((SECONDS + READY_TIMEOUT_SECS))
  while ! curl -sf "http://${HTTP_ADDR}/health" >/dev/null 2>&1; do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      fail "server ${name} exited before it became ready, see ${SERVER_LOG}"
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      fail "server ${name} not ready after ${READY_TIMEOUT_SECS}s, see ${SERVER_LOG}"
    fi
    sleep 0.2
  done
  START_WALL_MS=$(($(now_ms) - started))
}

sql() {
  curl -sf -X POST "${SQL_URL}?format=csv" --data-urlencode "sql=$1"
}

# One writer owns the stripe `first, first + WRITERS, ...` of the sequence
# numbers and stops at the first statement that is not acknowledged, so the
# stripe has at most one attempted but unacknowledged sequence number: the
# write that was in flight at the kill.
writer() {
  local cycle=$1 index=$2 seq=$3
  local attempted="${RUN_DIR}/cycle-${cycle}/attempted-${index}.log"
  local acked="${RUN_DIR}/cycle-${cycle}/acked-${index}.log"
  local failed="${RUN_DIR}/cycle-${cycle}/failed-${index}.log"
  local code
  while :; do
    echo "${seq}" >> "${attempted}"
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 60 -X POST "${SQL_URL}" \
      --data-urlencode "sql=INSERT INTO t VALUES (${seq}, ${seq})" || true)
    if [ "${code}" != "200" ]; then
      echo "${seq} ${code}" >> "${failed}"
      break
    fi
    echo "${seq}" >> "${acked}"
    seq=$((seq + WRITERS))
  done
}

# Reads the table back and checks it against the acknowledged and attempted
# sequence numbers. Prints the counts for the manifest and fails the run on
# the first violated invariant.
verify() {
  local cycle=$1
  local found="${RUN_DIR}/cycle-${cycle}/found.csv"
  sql "SELECT seq FROM t ORDER BY seq" > "${found}" ||
    fail "cycle ${cycle}: reading the table back failed"
  python3 - "${cycle}" "${ACKED}" "${ATTEMPTED}" "${found}" <<'EOF' >> "${MANIFEST}" || fail "cycle ${cycle}: invariant violated, see ${RUN_DIR}/cycle-${cycle}"
import collections
import sys

cycle_no, acked_path, attempted_path, found_path = sys.argv[1:5]
acked = [int(line) for line in open(acked_path) if line.strip()]
attempted = collections.OrderedDict()
for line in open(attempted_path):
    if line.strip():
        cycle, writer, seq = line.split()
        attempted.setdefault((int(cycle), int(writer)), []).append(int(seq))
found = [int(line) for line in open(found_path) if line.strip()]

acked_set = set(acked)
attempted_set = {seq for stripe in attempted.values() for seq in stripe}
counts = collections.Counter(found)
errors = []

duplicated = sorted(seq for seq, n in counts.items() if n > 1)
if duplicated:
    errors.append(f"present more than once: {duplicated}")
missing = sorted(acked_set - set(counts))
if missing:
    errors.append(f"acknowledged but absent: {missing}")
unattempted = sorted(set(counts) - attempted_set)
if unattempted:
    errors.append(f"present but never attempted: {unattempted}")

# The unacknowledged tail of each stripe is what was in flight at the kill;
# every unacknowledged sequence number that survived must come from there.
in_flight = set()
for (cycle, writer), stripe in attempted.items():
    acked_prefix = 0
    while acked_prefix < len(stripe) and stripe[acked_prefix] in acked_set:
        acked_prefix += 1
    tail = stripe[acked_prefix:]
    if any(seq in acked_set for seq in tail):
        errors.append(f"cycle {cycle} writer {writer} acknowledged after a failure: {tail}")
    in_flight.update(tail)
survivors = sorted(set(counts) - acked_set)
stray = sorted(seq for seq in survivors if seq not in in_flight)
if stray:
    errors.append(f"present, unacknowledged and not in flight at the kill: {stray}")

if len(acked) != len(acked_set):
    errors.append("acked.log holds a sequence number twice")

for error in errors:
    print(f"cycle={cycle_no} violation: {error}")
print(
    f"cycle={cycle_no} found={len(found)} cumulative_acked={len(acked_set)} "
    f"unacked_survivors={len(survivors)} survivors={survivors}"
)
sys.exit(1 if errors else 0)
EOF
}

# Appends a required field of the cycle to the manifest.
record() {
  echo "cycle=$1 $2" >> "${MANIFEST}"
}

log "run directory ${RUN_DIR}, root ${STORE_ROOT}, ${CYCLES} cycles, ${WRITERS} writers"
start_server 0
sql "CREATE TABLE t (seq BIGINT, ts TIMESTAMP TIME INDEX) WITH (append_mode = 'true')" >/dev/null ||
  fail "creating the table failed"
NEXT_SEQ=1

for cycle in $(seq 1 "${CYCLES}"); do
  CYCLE_DIR="${RUN_DIR}/cycle-${cycle}"
  mkdir -p "${CYCLE_DIR}"

  WRITER_PIDS=""
  for index in $(seq 0 $((WRITERS - 1))); do
    writer "${cycle}" "${index}" $((NEXT_SEQ + index)) &
    WRITER_PIDS="${WRITER_PIDS} $!"
  done

  delay_ms=$((KILL_MIN_SECS * 1000 + RANDOM % ((KILL_MAX_SECS - KILL_MIN_SECS) * 1000 + 1)))
  log "cycle ${cycle}: writing from sequence ${NEXT_SEQ}, SIGKILL in ${delay_ms}ms"
  sleep "$(python3 -c "print(${delay_ms} / 1000)")"
  kill -KILL "${SERVER_PID}"
  wait "${SERVER_PID}" 2>/dev/null || true
  # shellcheck disable=SC2086
  wait ${WRITER_PIDS}
  WRITER_PIDS=""

  cycle_acked=0
  for index in $(seq 0 $((WRITERS - 1))); do
    if [ -f "${CYCLE_DIR}/acked-${index}.log" ]; then
      cat "${CYCLE_DIR}/acked-${index}.log" >> "${ACKED}"
      cycle_acked=$((cycle_acked + $(wc -l < "${CYCLE_DIR}/acked-${index}.log")))
    fi
    if [ -f "${CYCLE_DIR}/attempted-${index}.log" ]; then
      sed -E "s/^/${cycle} ${index} /" "${CYCLE_DIR}/attempted-${index}.log" >> "${ATTEMPTED}"
    fi
  done
  # A response other than a dropped connection came from a live server that
  # rejected a valid insert.
  rejected=$(cat "${CYCLE_DIR}"/failed-*.log 2>/dev/null | grep -vE ' 000$' || true)
  if [ -n "${rejected}" ]; then
    echo "${rejected}" >&2
    fail "cycle ${cycle}: the server rejected inserts"
  fi
  max_attempted=$(awk '{ if ($3 > max) max = $3 } END { print max + 0 }' "${ATTEMPTED}")
  NEXT_SEQ=$((max_attempted + 1))
  record "${cycle}" "kill_delay_ms=${delay_ms} acked=${cycle_acked}"

  start_server "${cycle}"
  record "${cycle}" "restart_wall_ms=${START_WALL_MS}"
  # The region open time covers the WAL replay of the restart.
  opened=$(grep -oE 'Opened [0-9]+ regions in [^[:space:]]+' "${SERVER_LOG}" | head -1 || true)
  [ -n "${opened}" ] && record "${cycle}" "replay: ${opened}"
  grep -oE 'Replay WAL for region: .*' "${SERVER_LOG}" | sed -E "s/^/cycle=${cycle} replay: /" >> "${MANIFEST}" || true
  verify "${cycle}"
  record "${cycle}" "wal_$(wal_objects)"
  log "cycle ${cycle}: $(grep -E "^cycle=${cycle} found=" "${MANIFEST}")"
done

kill -TERM "${SERVER_PID}"
wait "${SERVER_PID}" || true
SERVER_PID=""

# The manifest is evidence, so every field must be present for every cycle;
# a missing field fails the run even when every check passed.
require_line() {
  if ! grep -qE "${2}" "${MANIFEST}"; then
    MISSING+=("${1}")
  fi
}
for cycle in $(seq 1 "${CYCLES}"); do
  require_line "cycle ${cycle} acked" "^cycle=${cycle} kill_delay_ms=[0-9]+ acked=[0-9]+$"
  require_line "cycle ${cycle} restart wall time" "^cycle=${cycle} restart_wall_ms=[0-9]+$"
  require_line "cycle ${cycle} replay timing" "^cycle=${cycle} replay: Opened [0-9]+ regions in "
  require_line "cycle ${cycle} rows found" "^cycle=${cycle} found=[0-9]+ cumulative_acked=[0-9]+ unacked_survivors=[0-9]+ survivors=\\["
  require_line "cycle ${cycle} WAL objects" "^cycle=${cycle} wal_objects=[0-9]+ bytes=[0-9]+$"
done
[ -n "${BINARY_COMMIT}" ] || MISSING+=("binary commit")

if [ "${#MISSING[@]}" -eq 0 ] && ! grep -qE '^(cycle=[0-9]+ violation|fail):' "${MANIFEST}"; then
  RESULT=PASS
fi
print_manifest

if [ "${RESULT}" = PASS ]; then
  mc_run "mc rm --recursive --force local/${MINIO_BUCKET}/${STORE_ROOT}/" >/dev/null 2>&1 || true
  exit 0
fi
exit 1
