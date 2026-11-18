#!/usr/bin/env bash
# Grid-pair × Baidu route benchmark (nextstep D step 4).
# Runs FREE 02:30, AM 07:30, PM 17:30 (Asia/Shanghai) on N weekdays; weekend slots are skipped.
# Sample: outputs/${VERSION}/data/sample_manifest.csv. Default v2 (600 directed pairs);
# v3 (2026-11-09, 1392 rows, about 4200 requests per day under the 5000 daily quota):
#   GP_VERSION=grid_pair_baidu_validation_v3 ./scripts/run_grid_pair_validation_week.sh
# After each panel src.raw_baidu_validation.check_grid_pair_panel appends to logs/panel_checks.jsonl;
# a success rate below 50% writes the stop file.
# FIRST_DAY_SKIP_FREE=1 starts day 1 at the AM panel when launched after 02:30.
# START_DAY=k resumes at day k (labels dk..); WEEKDAYS_TO_RUN stays the last day number.
#
#   cd data_work && nohup ./scripts/run_grid_pair_validation_week.sh \
#     >> outputs/grid_pair_baidu_validation_v2/logs/week_nohup.out 2>&1 &
# Stop before the next panel: touch outputs/grid_pair_baidu_validation_v2/logs/week.stop
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_WORK="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${DATA_WORK}"

PYTHON="${PYTHON:-python3}"
CONFIG="config/raw_baidu_validation_beijing_v1.json"
VERSION="${GP_VERSION:-grid_pair_baidu_validation_v2}"
MANIFEST="outputs/${VERSION}/data/sample_manifest.csv"
DAYS="${WEEKDAYS_TO_RUN:-5}"
SLEEP_SEC="${CRAWL_SLEEP_SEC:-0.35}"
LOG_DIR="outputs/${VERSION}/logs"
mkdir -p "${LOG_DIR}"
LOG="${LOG_DIR}/week_master.log"
STOP="${LOG_DIR}/week.stop"
log() { echo "[grid_pair_week] $* $(TZ=Asia/Shanghai date -Iseconds)" | tee -a "${LOG}"; }

run_panel() {
  local panel="$1" hhmm="$2" day="$3"
  [[ -f "${STOP}" ]] && { log "stop file found"; exit 0; }
  "${PYTHON}" scripts/wait_until_shanghai_hhmm_then_exec.py "${hhmm}" -- true
  local dow; dow="$(TZ=Asia/Shanghai date +%u)"
  if [[ "${dow}" -ge 6 ]]; then
    log "skip ${panel}: weekend (dow=${dow})"; return 1
  fi
  log "day ${day} ${panel} start"
  "${PYTHON}" -m src.raw_baidu_validation.query_baidu_direction \
    --config "${CONFIG}" --output-dir outputs --version-id "${VERSION}" \
    --sample-manifest "${MANIFEST}" --panel "${panel}" --no-steps-info \
    --sleep-sec "${SLEEP_SEC}" --label "d$(printf '%02d' "${day}")" >> "${LOG_DIR}/d$(printf '%02d' "${day}")_${panel}.log" 2>&1
  log "day ${day} ${panel} done"
  local lab; lab="d$(printf '%02d' "${day}")"
  if ! "${PYTHON}" -m src.raw_baidu_validation.check_grid_pair_panel --version-id "${VERSION}" \
      --panel "${panel}" --label "${lab}" >> "${LOG_DIR}/${lab}_${panel}.log" 2>&1; then
    log "check failed for ${lab} ${panel}; writing stop file"; touch "${STOP}"
  fi
  return 0
}

day="${START_DAY:-1}"
log "start pid=$$ weekdays=${DAYS} start_day=${day}"
while [[ "${day}" -le "${DAYS}" ]]; do
  ran=0
  if [[ "${day}" -eq 1 && "${FIRST_DAY_SKIP_FREE:-0}" == "1" ]]; then
    log "day 1 FREE skipped (FIRST_DAY_SKIP_FREE=1, started after 02:30)"
  else
    run_panel FREE 02:30 "${day}" && ran=1 || true
  fi
  run_panel AM 07:30 "${day}" && ran=1 || true
  run_panel PM 17:30 "${day}" && ran=1 || true
  if [[ "${ran}" -eq 1 ]]; then day=$((day + 1)); fi
done
log "complete"
