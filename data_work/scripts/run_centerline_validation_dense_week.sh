#!/usr/bin/env bash
# Dense formal week: anchored FREE/AM/PM + gap-fill extra rounds (no path geometry from day 2).
#
# Usage:
#   cp scripts/centerline_validation_week.env.example scripts/centerline_validation_week.env
#   nohup /path/to/data_work/scripts/run_centerline_validation_dense_week.sh \
#     >> outputs/centerline_baidu_validation_beijing_v1/logs/formal_week_dense_nohup.out 2>&1 &

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
WEEK_LABEL="${FORMAL_WEEK_LABEL:-formal_week_$(TZ=Asia/Shanghai date +%Y%m%d)}"
START_DAY="${DENSE_START_DAY:-2}"
DENSE_DAYS="${DENSE_DAYS:-4}"
SLEEP_SEC="${CRAWL_SLEEP_SEC:-0.25}"
START_FREE="${START_FREE_HHMM:-02:30}"
START_AM="${START_AM_HHMM:-07:30}"
START_PM="${START_PM_HHMM:-17:30}"
PANEL_EST="${PANEL_ESTIMATE_SEC:-8000}"
BUFFER_MIN="${BUFFER_BEFORE_ANCHOR_MIN:-30}"
OVERNIGHT_BUF="${OVERNIGHT_BUFFER_FREE_HHMM:-02:00}"

IMMEDIATE_FLAG=()
if [[ "${DENSE_START_IMMEDIATELY:-0}" == "1" ]]; then
  IMMEDIATE_FLAG=(--start-immediately)
fi

RESUME_FLAG=()
if [[ "${DENSE_RESUME:-0}" == "1" ]]; then
  RESUME_FLAG=(--resume)
fi

ORCH_ARGS=(
  --config "${CONFIG}"
  --week-label "${WEEK_LABEL}"
  --start-day "${START_DAY}"
  --days "${DENSE_DAYS}"
  --start-free-hhmm "${START_FREE}"
  --start-am-hhmm "${START_AM}"
  --start-pm-hhmm "${START_PM}"
  --buffer-before-anchor-min "${BUFFER_MIN}"
  --overnight-buffer-free-hhmm "${OVERNIGHT_BUF}"
  --panel-estimate-sec "${PANEL_EST}"
  --sleep-sec "${SLEEP_SEC}"
  --python "${PYTHON}"
)
if ((${#IMMEDIATE_FLAG[@]})); then
  ORCH_ARGS+=("${IMMEDIATE_FLAG[@]}")
fi
if ((${#RESUME_FLAG[@]})); then
  ORCH_ARGS+=("${RESUME_FLAG[@]}")
fi

exec "${PYTHON}" -m src.centerline_baidu_validation.dense_week_orchestrator "${ORCH_ARGS[@]}"
