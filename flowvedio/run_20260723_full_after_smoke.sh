#!/bin/zsh

set -euo pipefail

smoke_pid="${1:?smoke PID required}"
project_dir="/Users/fxr/Dropbox/Projects/TidalLanes"
python_bin="/Users/fxr/miniconda3/envs/research/bin/python3"
log_dir="$project_dir/flowvedio/batch_outputs/20260723_caitian_empirical_v1/logs"

mkdir -p "$log_dir"
while kill -0 "$smoke_pid" 2>/dev/null; do
  sleep 10
done

cd "$project_dir"
"$python_bin" flowvedio/run_20260723_measurements.py \
  >"$log_dir/full_measurement.log" 2>&1
