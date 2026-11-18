#!/usr/bin/env bash
set -euo pipefail

cd /Users/fxr/Dropbox/Projects/TidalLanes
source flowvedio/.venv-video/bin/activate

STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="flowvedio/batch_outputs/20260709_identification_qa_video"
LOG_DIR="flowvedio/batch_outputs/logs"
mkdir -p "$OUT" "$LOG_DIR"

python flowvedio/run_traffic_plan.py \
  --input flowvedio/20260709彩田路 \
  --segments flowvedio/segments_20260709_identification_qa.csv \
  --lines flowvedio/line_plan_20260709_identification.csv \
  --output "$OUT" \
  --model flowvedio/models/yolo11s.pt \
  --imgsz 960 \
  --conf 0.16 \
  --stride 4 \
  --device mps \
  --save-video \
  2>&1 | tee "$LOG_DIR/run_20260709_identification_qa_video_${STAMP}.log"

echo
echo "Done."
echo "Summary: /Users/fxr/Dropbox/Projects/TidalLanes/${OUT}/summary_plan.csv"
echo "Panel:   /Users/fxr/Dropbox/Projects/TidalLanes/${OUT}/panel_by_line.csv"
