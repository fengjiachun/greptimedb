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
#   KILL_WINDOW_MAX_MS
#                   upper bound of the sampling window around the kill,
#                   default 100; see below
#   GREPTIME_PORT_BASE
#                   HTTP port of the server; gRPC, MySQL and PostgreSQL take
#                   the next three ports, default 24000
#   RUN_DIR         where the config, logs, acked files and manifest go; it
#                   must not exist yet or be an empty directory, so the
#                   evidence of one run is never mixed with another's,
#                   default a fresh temporary directory
#   MINIO_*         same as scripts/object-store-wal-minio.sh
#
# Every cycle writes with WRITERS clients that append a sequence number to
# acked.log only after the server acknowledged its INSERT, kills the server,
# restarts it, and reads the table back. The cycle passes when every
# acknowledged sequence number is present exactly once, no sequence number
# is present twice, and every present sequence number that was not
# acknowledged is a write that was in flight at the kill: the last attempt
# of a writer, whose connection dropped no earlier than the kill. A dropped
# connection before the kill or a kill that found no writer still running
# fails the cycle. Nothing is cleaned between cycles, so replay accumulates.
#
# The in-flight verdict is lenient inside the sampling window around the
# kill: the controller stamps the clock just before it sends SIGKILL and
# again just after the signal was sent, and a dropped connection that ended
# at or after the first stamp counts as in flight. The width of that window
# is written to the manifest and must stay under KILL_WINDOW_MAX_MS. This
# is the known precision boundary of the script.
#
# PASS means every cycle passed, the manifest carries every required field,
# and the objects of the run were removed from the bucket and the removal
# was verified; anything else is FAIL with a non-zero exit status and a
# final manifest, however the run ended. The manifest is written in two
# parts: the verdict part, with every field and a `verdict:` line, is
# persisted while the objects are still in the bucket, and the `cleanup:`
# and `result:` lines follow the removal. The run directory is printed and
# kept in both cases; the objects of a failed run stay in the bucket as
# evidence.
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
KILL_WINDOW_MAX_MS="${KILL_WINDOW_MAX_MS:-100}"
GREPTIME_PORT_BASE="${GREPTIME_PORT_BASE:-24000}"
RUN_DIR="${RUN_DIR:-}"
WAL_PREFIX="wal"
READY_TIMEOUT_SECS=300

HTTP_ADDR="127.0.0.1:${GREPTIME_PORT_BASE}"
SQL_URL="http://${HTTP_ADDR}/v1/sql"
# The evidence files are named only once the run directory was accepted;
# nothing is written to a directory before that.
MANIFEST=""
FINAL_MANIFEST=""
ACKED=""
ATTEMPTED=""
SERVER_PID=""
WRITER_PIDS=""
VERDICT=FAIL
VERDICT_PERSISTED=no
RESULT_APPENDED=no
FINAL_MANIFEST_OPEN=no
OBJECTS_REMOVED=no
UNRECORDED=""
STORE_ROOT=""
BINARY_COMMIT=""
BINARY_SHA256=""
CHECKED_OUT_COMMIT=""
MINIO_IMAGE_ID=""
MINIO_IMAGE_DIGESTS=""
MISSING=()

# The original stdout and stderr, so the manifest and the log reach the
# caller even when the EXIT handler runs inside a redirected command.
exec 3>&1 4>&2

log() {
  echo "[object-store-wal-crash-gate] $*" >&4
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

# The writers and the kill markers are stamped from this one clock.
now_ns() {
  python3 -c 'import time; print(time.time_ns())'
}

# Writes text to the final manifest file through an external process whose
# stdout is that file for its whole life, so a failed write cannot leak the
# text into the shell's own stdout buffer.
write_final() {
  printf '%s\n' "$1" | cat >&5
}

# Writes one trailing manifest line to the caller and, once it is open, to
# the final manifest file.
append_line() {
  printf '%s\n' "$1" >&3 || return 1
  if [ "${FINAL_MANIFEST_OPEN}" = yes ]; then
    write_final "$1" || return 1
  fi
}

# Writes the verdict part of the manifest: the run identity, every field
# collected so far, and the verdict. It renders only values collected
# earlier and never runs a command that can fail, so it works from the EXIT
# handler at any point of the run, before a run directory exists included.
# The verdict counts as persisted only once both copies were written.
print_verdict() {
  local text
  text=$(
    echo "== object store WAL crash gate manifest =="
    echo "base commit: ${CHECKED_OUT_COMMIT:-unknown}"
    echo "binary: ${GREPTIME_BIN:-not built} commit ${BINARY_COMMIT:-unknown} sha256 ${BINARY_SHA256:-unknown}"
    echo "minio image: ${MINIO_IMAGE_ID:-unknown} (${MINIO_IMAGE_DIGESTS:-no repo digest}) in container ${MINIO_CONTAINER}"
    echo "bucket: ${MINIO_BUCKET} root ${STORE_ROOT:-unknown} wal prefix ${WAL_PREFIX} at http://127.0.0.1:${MINIO_PORT}"
    echo "cycles: ${CYCLES} writers: ${WRITERS} kill window: ${KILL_MIN_SECS}s to ${KILL_MAX_SECS}s"
    [ -n "${MANIFEST}" ] && [ -f "${MANIFEST}" ] && cat "${MANIFEST}"
    [ -n "${UNRECORDED}" ] && printf '%s' "${UNRECORDED}"
    for field in ${MISSING[@]+"${MISSING[@]}"}; do
      echo "missing: ${field}"
    done
    echo "verdict: ${VERDICT}"
  )
  printf '\n%s\n' "${text}" >&3 || return 1
  if [ "${FINAL_MANIFEST_OPEN}" = yes ]; then
    write_final "${text}" || return 1
  fi
  VERDICT_PERSISTED=yes
}

# Fails the run right away with the reason in the manifest; the EXIT handler
# prints it. Until the run directory was accepted, and whenever the reason
# cannot be recorded there, it is kept in memory and printed with the
# manifest, so a rejected directory is never written to.
fail() {
  log "FAIL: $*"
  if [ -z "${MANIFEST}" ] || ! echo "fail: $*" 2>/dev/null >> "${MANIFEST}"; then
    UNRECORDED="${UNRECORDED}fail: $*"$'\n'
  fi
  exit 1
}

# Runs on every exit: stops whatever is still running, and completes the
# manifest as FAIL when the run ended before it was complete, so a command
# that failed under `set -e` still leaves a verdict. The objects are still
# in the bucket unless the manifest says they were removed.
# shellcheck disable=SC2329
cleanup() {
  local status=$?
  set +e
  if [ -n "${WRITER_PIDS}" ]; then
    # shellcheck disable=SC2086
    kill -KILL ${WRITER_PIDS} 2>/dev/null
  fi
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "killing the leftover server ${SERVER_PID}"
    kill -KILL "${SERVER_PID}" 2>/dev/null
    wait "${SERVER_PID}" 2>/dev/null
  fi
  if [ "${VERDICT_PERSISTED}" = no ]; then
    VERDICT=FAIL
    UNRECORDED="${UNRECORDED}fail: exited with status ${status} before the verdict was complete"$'\n'
    print_verdict
  fi
  if [ "${RESULT_APPENDED}" = no ]; then
    if [ "${OBJECTS_REMOVED}" = yes ]; then
      append_line "objects: removed from the bucket and verified empty after the verdict"
    else
      append_line "objects: kept in the bucket under root ${STORE_ROOT:-unknown}"
    fi
    append_line "result: FAIL"
    log "run directory: ${RUN_DIR:-none}"
    exit 1
  fi
  log "run directory: ${RUN_DIR:-none}"
}
# Installed before anything that can fail, the run directory included.
trap cleanup EXIT
# Read once early so a manifest printed before the checked collection still
# names the commit; the checked collection runs before the verdict.
CHECKED_OUT_COMMIT=$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || true)

# The knobs are checked before anything is started, so a bad value fails
# with a manifest instead of an empty run or an arithmetic error.
is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}
is_non_negative_number() {
  [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]]
}
is_positive_integer "${CYCLES}" || fail "CYCLES must be a positive integer, got '${CYCLES}'"
is_positive_integer "${WRITERS}" || fail "WRITERS must be a positive integer, got '${WRITERS}'"
is_non_negative_number "${KILL_MIN_SECS}" ||
  fail "KILL_MIN_SECS must be a non-negative number, got '${KILL_MIN_SECS}'"
is_non_negative_number "${KILL_MAX_SECS}" ||
  fail "KILL_MAX_SECS must be a non-negative number, got '${KILL_MAX_SECS}'"
if ! python3 -c "import sys; sys.exit(0 if ${KILL_MAX_SECS} >= ${KILL_MIN_SECS} else 1)"; then
  fail "KILL_MAX_SECS (${KILL_MAX_SECS}) must not be below KILL_MIN_SECS (${KILL_MIN_SECS})"
fi
is_positive_integer "${KILL_WINDOW_MAX_MS}" ||
  fail "KILL_WINDOW_MAX_MS must be a positive integer, got '${KILL_WINDOW_MAX_MS}'"
is_positive_integer "${GREPTIME_PORT_BASE}" ||
  fail "GREPTIME_PORT_BASE must be a positive integer, got '${GREPTIME_PORT_BASE}'"

# The run directory holds the evidence of exactly one run, so a given one
# must be new or empty and is not touched before that is known. The final
# manifest file is opened now and kept open, so a directory that cannot
# take the manifest fails before anything runs.
if [ -z "${RUN_DIR}" ]; then
  RUN_DIR=$(mktemp -d -t object-store-wal-crash-gate.XXXXXX) ||
    fail "cannot create a temporary run directory"
elif [ -e "${RUN_DIR}" ]; then
  if ! [ -d "${RUN_DIR}" ] || [ -n "$(ls -A "${RUN_DIR}" 2>/dev/null || echo occupied)" ]; then
    fail "RUN_DIR ${RUN_DIR} exists and is not an empty directory"
  fi
fi
mkdir -p "${RUN_DIR}" || fail "cannot create RUN_DIR ${RUN_DIR}"
MANIFEST="${RUN_DIR}/manifest.txt"
FINAL_MANIFEST="${RUN_DIR}/manifest-final.txt"
ACKED="${RUN_DIR}/acked.log"
ATTEMPTED="${RUN_DIR}/attempted.log"
: > "${FINAL_MANIFEST}" || fail "cannot write ${FINAL_MANIFEST}"
exec 5>>"${FINAL_MANIFEST}"
FINAL_MANIFEST_OPEN=yes
: > "${MANIFEST}" || fail "cannot write ${MANIFEST}"
: > "${ACKED}" || fail "cannot write ${ACKED}"
: > "${ATTEMPTED}" || fail "cannot write ${ATTEMPTED}"

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

# Collects the identity of the run: the checked-out commit and the image of
# the running MinIO container, which can differ from what the tag resolves
# to when an older container is reused. Every step is checked, and a
# missing value fails the run.
collect_identity() {
  local commit image_id digests
  commit=$(git -C "${ROOT_DIR}" rev-parse HEAD) && [ -n "${commit}" ] ||
    fail "cannot read the checked-out commit"
  docker ps --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}" ||
    fail "container ${MINIO_CONTAINER} is not running"
  image_id=$(docker inspect --format '{{.Image}}' "${MINIO_CONTAINER}") && [ -n "${image_id}" ] ||
    fail "cannot read the image of container ${MINIO_CONTAINER}"
  digests=$(docker inspect --format '{{join .RepoDigests ","}}' "${image_id}") ||
    fail "cannot read the digests of image ${image_id}"
  CHECKED_OUT_COMMIT="${commit}"
  MINIO_IMAGE_ID="${image_id}"
  MINIO_IMAGE_DIGESTS="${digests}"
}
collect_identity

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
BINARY_COMMIT=$("${GREPTIME_BIN}" --version | sed -nE 's/^commit: *//p' | head -1) &&
  [ -n "${BINARY_COMMIT}" ] || fail "cannot read the commit of ${GREPTIME_BIN}"
BINARY_SHA256=$(shasum -a 256 "${GREPTIME_BIN}" | cut -d' ' -f1) &&
  [ -n "${BINARY_SHA256}" ] || fail "cannot hash ${GREPTIME_BIN}"

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
# write that was in flight at the kill. Every request is recorded with its
# status code and the time it ended, and the exit of the writer with its
# time, so the kill markers can be compared against them.
writer() {
  local cycle=$1 index=$2 seq=$3
  local dir="${RUN_DIR}/cycle-${cycle}"
  local code end_ns
  while :; do
    echo "${seq}" >> "${dir}/attempted-${index}.log"
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 60 -X POST "${SQL_URL}" \
      --data-urlencode "sql=INSERT INTO t VALUES (${seq}, ${seq})" || true)
    end_ns=$(now_ns)
    echo "${seq} ${code} ${end_ns}" >> "${dir}/requests-${index}.log"
    if [ "${code}" != "200" ]; then
      break
    fi
    echo "${seq}" >> "${dir}/acked-${index}.log"
    seq=$((seq + WRITERS))
  done
  now_ns > "${dir}/exit-${index}.log"
}

# Checks the requests of a cycle against the kill markers: a dropped
# connection counts as a write in flight at the kill when it ended at or
# after the marker taken before the signal, an earlier one is a transport
# failure, any other status is a rejected insert, at least one writer must
# have been running at the kill, and the window between the two markers
# must stay under the bound. An acknowledged request is never judged by its
# time; the restart oracle decides whether it was durable. Prints the counts
# for the manifest and fails the run on the first violation.
check_kill() {
  local cycle=$1 before_ns=$2 after_ns=$3
  python3 - "${cycle}" "${before_ns}" "${after_ns}" "${KILL_WINDOW_MAX_MS}" "${WRITERS}" "${RUN_DIR}/cycle-${cycle}" <<'EOF' >> "${MANIFEST}" || fail "cycle ${cycle}: the writes around the kill are not consistent, see ${RUN_DIR}/cycle-${cycle}"
import os
import sys

cycle_no = sys.argv[1]
before_ns, after_ns, window_max_ms, writers = (int(value) for value in sys.argv[2:6])
cycle_dir = sys.argv[6]
errors = []
in_flight = 0
running_at_kill = 0
window_ns = after_ns - before_ns
if window_ns > window_max_ms * 1_000_000:
    errors.append(f"kill marker not tight: window {window_ns}ns exceeds {window_max_ms}ms")
for index in range(writers):
    requests_path = os.path.join(cycle_dir, f"requests-{index}.log")
    exit_path = os.path.join(cycle_dir, f"exit-{index}.log")
    if not os.path.exists(exit_path):
        errors.append(f"writer {index} left no exit time")
        continue
    exit_ns = int(open(exit_path).read())
    if exit_ns >= before_ns:
        running_at_kill += 1
    requests = []
    if os.path.exists(requests_path):
        for line in open(requests_path):
            if line.strip():
                seq, code, end_ns = line.split()
                requests.append((int(seq), code, int(end_ns)))
    for seq, code, end_ns in requests:
        if code == "200":
            continue
        if code == "000":
            if end_ns < before_ns:
                errors.append(f"writer {index}: sequence {seq} lost its connection {before_ns - end_ns}ns before the kill")
            else:
                in_flight += 1
        else:
            errors.append(f"writer {index}: sequence {seq} rejected with HTTP {code}")
if running_at_kill == 0:
    errors.append("no writer was running at the kill")

for error in errors:
    print(f"cycle={cycle_no} kill violation: {error}")
print(
    f"cycle={cycle_no} kill_before_ns={before_ns} kill_after_ns={after_ns} "
    f"kill_window_ns={window_ns} in_flight={in_flight} writers_at_kill={running_at_kill}"
)
sys.exit(1 if errors else 0)
EOF
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

  delay_ms=$(python3 -c "import random; print(random.randint(int(${KILL_MIN_SECS} * 1000), int(${KILL_MAX_SECS} * 1000)))")
  log "cycle ${cycle}: writing from sequence ${NEXT_SEQ}, SIGKILL in ${delay_ms}ms"
  sleep "$(python3 -c "print(${delay_ms} / 1000)")"
  # The two markers bound the moment of the kill; the window between them
  # is the precision of the in-flight verdict and goes into the manifest.
  kill_before_ns=$(now_ns)
  kill -KILL "${SERVER_PID}"
  kill_after_ns=$(now_ns)
  wait "${SERVER_PID}" 2>/dev/null || true
  # A writer exits zero only through its loop; anything else means its
  # acked and failed files cannot be trusted.
  index=0
  for pid in ${WRITER_PIDS}; do
    if ! wait "${pid}"; then
      fail "cycle ${cycle}: writer ${index} (pid ${pid}) exited unexpectedly"
    fi
    index=$((index + 1))
  done
  WRITER_PIDS=""
  check_kill "${cycle}" "${kill_before_ns}" "${kill_after_ns}"

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
  max_attempted=$(awk '{ if ($3 > max) max = $3 } END { print max + 0 }' "${ATTEMPTED}")
  NEXT_SEQ=$((max_attempted + 1))
  record "${cycle}" "kill_delay_ms=${delay_ms} acked=${cycle_acked}"

  start_server "${cycle}"
  record "${cycle}" "restart_wall_ms=${START_WALL_MS}"
  # The replay of every region is reported by the engine; the region open
  # time of the datanode covers all of them.
  grep -oE 'Replay WAL for region: .*' "${SERVER_LOG}" | sed -E "s/^/cycle=${cycle} replay: /" >> "${MANIFEST}" || true
  grep -oE 'Opened [0-9]+ regions in [^[:space:]]+' "${SERVER_LOG}" | head -1 |
    sed -E "s/^/cycle=${cycle} open: /" >> "${MANIFEST}" || true
  verify "${cycle}"
  objects=$(wal_objects) || fail "cycle ${cycle}: listing the WAL objects failed"
  record "${cycle}" "wal_${objects}"
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
  require_line "cycle ${cycle} kill" "^cycle=${cycle} kill_before_ns=[0-9]+ kill_after_ns=[0-9]+ kill_window_ns=[0-9]+ in_flight=[0-9]+ writers_at_kill=[1-9][0-9]*$"
  require_line "cycle ${cycle} acked" "^cycle=${cycle} kill_delay_ms=[0-9]+ acked=[0-9]+$"
  require_line "cycle ${cycle} restart wall time" "^cycle=${cycle} restart_wall_ms=[0-9]+$"
  require_line "cycle ${cycle} replay" "^cycle=${cycle} replay: Replay WAL for region: .* rows recovered: [0-9]+, replay from entry id: [0-9]+, last entry id: [0-9]+, .*elapsed: "
  require_line "cycle ${cycle} region open time" "^cycle=${cycle} open: Opened [0-9]+ regions in "
  require_line "cycle ${cycle} rows found" "^cycle=${cycle} found=[0-9]+ cumulative_acked=[0-9]+ unacked_survivors=[0-9]+ survivors=\\["
  require_line "cycle ${cycle} WAL objects" "^cycle=${cycle} wal_objects=[0-9]+ bytes=[0-9]+$"
done
[ -n "${BINARY_COMMIT}" ] || MISSING+=("binary commit")
[ -n "${BINARY_SHA256}" ] || MISSING+=("binary sha256")
collect_identity

# The verdict part is persisted while the objects are still in the bucket:
# `verdict: PASS` states that every durability check passed. Only then are
# the objects removed, and the removal is verified before `result: PASS` is
# appended. Every failure before the removal keeps the objects.
if [ "${#MISSING[@]}" -eq 0 ] && ! grep -qE '^(cycle=[0-9]+ (kill )?violation|fail):' "${MANIFEST}"; then
  VERDICT=PASS
fi
print_verdict
if [ "${VERDICT}" != PASS ]; then
  append_line "result: FAIL"
  RESULT_APPENDED=yes
  exit 1
fi
mc_run "mc rm --recursive --force local/${MINIO_BUCKET}/${STORE_ROOT}/" >/dev/null 2>&1 || true
if REMAINING=$(mc_run "mc ls --recursive local/${MINIO_BUCKET}/${STORE_ROOT}/") && [ -z "${REMAINING}" ]; then
  OBJECTS_REMOVED=yes
  append_line "cleanup: root ${STORE_ROOT} removed and verified empty"
  append_line "result: PASS"
  RESULT_APPENDED=yes
  exit 0
fi
echo "${REMAINING}" >&2
append_line "cleanup: root ${STORE_ROOT} still holds objects"
append_line "result: FAIL"
RESULT_APPENDED=yes
exit 1
