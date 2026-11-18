#!/usr/bin/env bash
# 示例：在「北京时间」到达 WAIT_HHMM 后启动晚高峰单档爬取（须落在 17:00–19:30 严格窗内）。

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-/Users/fxr/Dropbox/Projects/TidalLanes}"
WAIT_HHMM="${WAIT_HHMM:-17:05}"
CONFIG="${BAIDU_NATIONAL_CONFIG:-config/national_tidal_commuting_baidu2025_top100_congestion_v1.json}"
LABEL_PREFIX="${CRAWL_LABEL_PREFIX:-pm_strict}"

cd "${REPO_ROOT}/data_work"

exec python3.11 scripts/wait_until_shanghai_hhmm_then_exec.py "${WAIT_HHMM}" -- \
  python3.11 -m src.national_tidal_commuting.run_full_crawl \
  --config "${CONFIG}" \
  --panels PM \
  --strict-live-window \
  --label "${LABEL_PREFIX}_$(date +%Y%m%d)" \
  --sleep-sec "${CRAWL_SLEEP_SEC:-0.2}"
