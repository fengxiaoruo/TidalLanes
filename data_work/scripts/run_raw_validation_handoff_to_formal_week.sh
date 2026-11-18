#!/usr/bin/env bash
# Wait for d01 catchup to finish, then start formal week crawl from START_DAY (default 2).
#
# Usage (already running in background after d01 catchup was launched):
#   nohup ./scripts/run_raw_validation_handoff_to_formal_week.sh \
#     >> outputs/raw_baidu_validation_beijing_v1/logs/handoff_nohup.out 2>&1 &

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DATA_WORK}"

ENV_FILE="${RAW_WEEK_ENV:-${SCRIPT_DIR}/raw_validation_week.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  set -a && source "${ENV_FILE}" && set +a
fi

WEEK_LABEL="${FORMAL_WEEK_LABEL:-formal_week_$(TZ=Asia/Shanghai date +%Y%m%d)}"
START_DAY="${HANDOFF_START_DAY:-2}"
CATCHUP_LABEL="${CATCHUP_LABEL:-${WEEK_LABEL}_d01}"
POLL_SEC="${HANDOFF_POLL_SEC:-60}"
MIN_PM_ROWS="${HANDOFF_MIN_PM_ROWS:-23000}"

LOG_DIR="${DATA_WORK}/outputs/raw_baidu_validation_beijing_v1/logs"
mkdir -p "${LOG_DIR}"
HANDOFF_LOG="${LOG_DIR}/${WEEK_LABEL}_handoff.log"
LOCK_FILE="${LOG_DIR}/${WEEK_LABEL}_handoff.lock"

log() {
  echo "[raw_handoff] $* $(TZ=Asia/Shanghai date -Iseconds)" | tee -a "${HANDOFF_LOG}"
}

if [[ -f "${LOCK_FILE}" ]]; then
  log "lock exists (${LOCK_FILE}) — handoff already scheduled or done; exit"
  exit 0
fi
echo "$$" >"${LOCK_FILE}"

log "handoff watcher start pid=$$ week=${WEEK_LABEL} catchup=${CATCHUP_LABEL} -> formal day ${START_DAY}-${FORMAL_DAYS:-5}"

_wait_catchup_process() {
  while pgrep -f "run_raw_validation_d01_catchup.sh" >/dev/null 2>&1; do
    log "catchup still running; sleep ${POLL_SEC}s"
    sleep "${POLL_SEC}"
  done
}

_verify_pm_rows() {
  local pm_csv
  pm_csv="$(find "${DATA_WORK}/outputs/raw_baidu_validation_beijing_v1/api/crawl_sessions" -name "PM_*${CATCHUP_LABEL}" -type d 2>/dev/null | head -1)/results_flat.csv"
  if [[ ! -f "${pm_csv}" ]]; then
    log "WARN: PM results not found yet (${pm_csv})"
    return 0
  fi
  local n
  n=$(( $(wc -l < "${pm_csv}") - 1 ))
  log "PM rows=${n} (min expected ${MIN_PM_ROWS})"
  if [[ "${n}" -lt "${MIN_PM_ROWS}" ]]; then
    log "WARN: PM row count below threshold; proceeding anyway"
  fi
}

_wait_catchup_process
_verify_pm_rows

log "catchup finished — launching formal week from day ${START_DAY}"
export START_DAY="${START_DAY}"
exec ./scripts/run_raw_validation_week.sh 2>&1 | tee -a "${LOG_DIR}/${WEEK_LABEL}_master.log"
