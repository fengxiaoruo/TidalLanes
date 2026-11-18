#!/usr/bin/env bash
# Formal centerline × Baidu week crawl (serial, scheduled, waypoints always on).
#
# - Day 1: FREE + AM + PM with --steps-info (route geometry for spatial QA)
# - Day 2+: same panels with --no-steps-info (duration/distance only)
# - Each panel starts at configured Asia/Shanghai HH:MM via wait_until_shanghai_hhmm_then_exec.py
#
# Usage:
#   cp scripts/centerline_validation_week.env.example scripts/centerline_validation_week.env
#   # edit .env (AK, times, FORMAL_DAYS, FORMAL_WEEK_LABEL)
#   cd data_work && nohup ./scripts/run_centerline_validation_week.sh >> outputs/.../logs/formal_week_nohup.out 2>&1 &
#
# Stop after current panel: touch outputs/centerline_baidu_validation_beijing_v1/logs/${FORMAL_WEEK_LABEL}.stop

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DATA_WORK}"

ENV_FILE="${CENTERLINE_WEEK_ENV:-${SCRIPT_DIR}/centerline_validation_week.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck source=/dev/null
  set -a && source "${ENV_FILE}" && set +a
fi

PYTHON="${PYTHON:-python3.11}"
if ! command -v "${PYTHON}" >/dev/null 2>&1; then
  PYTHON=python
fi

CONFIG="${CENTERLINE_CONFIG:-config/centerline_baidu_validation_beijing_v1.json}"
START_FREE="${START_FREE_HHMM:-02:30}"
START_AM="${START_AM_HHMM:-07:30}"
START_PM="${START_PM_HHMM:-17:30}"
SLEEP_SEC="${CRAWL_SLEEP_SEC:-0.25}"
FORMAL_DAYS="${FORMAL_DAYS:-5}"
WEEK_LABEL="${FORMAL_WEEK_LABEL:-formal_week_$(TZ=Asia/Shanghai date +%Y%m%d)}"

LOG_DIR="${DATA_WORK}/outputs/centerline_baidu_validation_beijing_v1/logs"
mkdir -p "${LOG_DIR}"
MASTER_LOG="${LOG_DIR}/${WEEK_LABEL}_master.log"
STOP_FILE="${LOG_DIR}/${WEEK_LABEL}.stop"

log() {
  echo "[formal_week] $* $(TZ=Asia/Shanghai date -Iseconds)" | tee -a "${MASTER_LOG}"
}

if [[ "${CONFIG}" == /* ]]; then
  CFG_PATH="${CONFIG}"
else
  CFG_PATH="${DATA_WORK}/${CONFIG}"
fi
if [[ ! -f "${CFG_PATH}" ]]; then
  echo "Config not found: ${CFG_PATH}" >&2
  exit 1
fi

log "start pid=$$ label=${WEEK_LABEL} days=${FORMAL_DAYS}"
log "config=${CONFIG} sleep=${SLEEP_SEC}s"
log "schedule FREE=${START_FREE} AM=${START_AM} PM=${START_PM}"
log "stop file: ${STOP_FILE}"
log "master log: ${MASTER_LOG}"

_run_panel() {
  local day="$1"
  local panel="$2"
  local hhmm="$3"
  local steps_flag="$4"
  local label="${WEEK_LABEL}_d$(printf '%02d' "${day}")"
  local day_log="${LOG_DIR}/${label}_${panel}.log"

  if [[ -f "${STOP_FILE}" ]]; then
    log "stop file found — exiting before ${label} ${panel}"
    exit 0
  fi

  log "day ${day}/${FORMAL_DAYS} panel=${panel} wait=${hhmm} ${steps_flag} label=${label}"
  {
    echo "===== ${label} ${panel} begin $(TZ=Asia/Shanghai date -Iseconds) ====="
    "${PYTHON}" scripts/wait_until_shanghai_hhmm_then_exec.py "${hhmm}" -- \
      "${PYTHON}" -m src.centerline_baidu_validation.query_baidu_direction \
      --config "${CONFIG}" \
      --panel "${panel}" \
      --label "${label}" \
      --sleep-sec "${SLEEP_SEC}" \
      ${steps_flag}
    echo "===== ${label} ${panel} end $(TZ=Asia/Shanghai date -Iseconds) ====="
  } 2>&1 | tee -a "${day_log}" >>"${MASTER_LOG}"
}

for ((day = 1; day <= FORMAL_DAYS; day++)); do
  if [[ "${day}" -eq 1 ]]; then
    STEPS_FLAG="--steps-info"
    log "day ${day}: route geometry ON (steps_info=1)"
  else
    STEPS_FLAG="--no-steps-info"
    log "day ${day}: route geometry OFF (duration/distance only)"
  fi

  _run_panel "${day}" FREE "${START_FREE}" "${STEPS_FLAG}"
  _run_panel "${day}" AM "${START_AM}" "${STEPS_FLAG}"
  _run_panel "${day}" PM "${START_PM}" "${STEPS_FLAG}"

  log "day ${day}/${FORMAL_DAYS} complete"
done

log "formal week complete label=${WEEK_LABEL} days=${FORMAL_DAYS}"
