#!/bin/zsh
set -euo pipefail

ROOT="/Users/fxr/Desktop/TidalLanes"
cd "$ROOT"

export MPLCONFIGDIR=/tmp/mpl

while [[ ! -f data_work/outputs/match_projection_complete_v7/data/raw_to_centerline_match_master.parquet ]]; do
  sleep 60
done

python data_work/src/stages/stage10_generate_figures.py \
  --version-id match_projection_complete_v7 \
  --output-dir data_work/outputs \
  --figure-group match

python data_work/src/stages/run_outer_native_centerline_experiment.py \
  --version-id match_projection_complete_v8 \
  --output-dir data_work/outputs \
  --native-min-length 5000 \
  --native-min-center-dist 10000 \
  --split-sample-step 200 \
  --split-search-dist 80 \
  --cut-buf 40 \
  --snap-tol 30 \
  --min-seg-gap 5 \
  --proj-search-dist 120

python data_work/src/stages/stage10_generate_figures.py \
  --version-id match_projection_complete_v8 \
  --output-dir data_work/outputs \
  --figure-group match

python data_work/src/stages/run_outer_native_centerline_experiment.py \
  --version-id match_projection_complete_v9 \
  --output-dir data_work/outputs \
  --native-min-length 3000 \
  --native-min-center-dist 10000 \
  --split-sample-step 120 \
  --split-search-dist 100 \
  --cut-buf 50 \
  --snap-tol 35 \
  --min-seg-gap 5 \
  --proj-search-dist 180

python data_work/src/stages/stage10_generate_figures.py \
  --version-id match_projection_complete_v9 \
  --output-dir data_work/outputs \
  --figure-group match
