# Temp Stage02 Projection Debug

This is a temporary note for debugging only the `389` baseline unmatched rows in stage02.

Goal:
- do not rerun the first-pass baseline match
- only tune the projection fallback for the unmatched subset
- quickly regenerate stage02 outputs and match figures

## Scope

Use this when you are only changing projection-related logic in:

- `data_work/src/stages/stage02_match_raw_to_centerline.py`

Specifically:
- projection scoring
- projection acceptance thresholds
- distance / area / length-ratio rules
- candidate filtering for projection fallback

Do not use this if you changed:
- baseline first-pass matching logic
- raw splitting logic
- stage01 centerline construction

## New Helper Script

Use:

- `data_work/src/stages/stage02_projection_debug.py`

What it does:
- reads an existing stage02 result as the baseline source
- keeps the old first-pass `matched_old` results
- only reruns `projection_fallback_segments()` for the baseline unmatched subset
- rebuilds `raw_to_centerline_match_master.parquet`
- rebuilds `raw_split_centerline.parquet`
- rewrites stage02 metrics for the new target version

## Recommended Source Version

Use this as the baseline source:

```bash
match_projection_complete_v2
```

Reason:
- its baseline first-pass matching has already been aligned back to the notebook-style threshold logic
- its baseline unmatched count is `389`

## Fast Workflow

### 1. Edit projection logic

Edit:

- `data_work/src/stages/stage02_match_raw_to_centerline.py`

Typical parameters / functions to tune:

- `PROJ_SEARCH_DIST`
- `PROJ_BUF`
- `PROJ_MIN_LENGTH_RATIO`
- `PROJ_DIST_PENALTY`
- `projection_fallback_segments()`
- `score_projection()`
- `projection_area_metrics()`

### 2. Rerun only projection fallback

Example:

```bash
python data_work/src/stages/stage02_projection_debug.py \
  --source-version match_projection_complete_v2 \
  --target-version match_projection_complete_v4 \
  --output-dir data_work/outputs \
  --copy-stage01
```

This does not rerun the first-pass baseline match.

### 3. Regenerate match figures

```bash
python data_work/src/stages/stage10_generate_figures.py \
  --version-id match_projection_complete_v4 \
  --output-dir data_work/outputs \
  --figure-group match
```

## Files To Check

After each debug rerun, check:

- `data_work/outputs/{version_id}/metrics/stage02_match_summary.csv`
- `data_work/outputs/{version_id}/data/raw_to_centerline_match_master.parquet`
- `data_work/outputs/{version_id}/figures/map_raw_split_matched_vs_unmatched.png`
- `data_work/outputs/{version_id}/figures/map_undirected_centerline_coverage_4classes.png`
- `data_work/outputs/{version_id}/figures/match_quality_centerline.png`

## Key Metrics To Compare

Main fields in `stage02_match_summary.csv`:

- `split_match_rate_old`
- `split_match_rate`
- `projection_uplift_pp`
- `projection_added_matches`
- `unmatched_split_segments`
- `projection_review_flagged`

## Useful Quick Checks

Count how many projection matches were added:

```bash
python - <<'PY'
import pandas as pd
df = pd.read_parquet('data_work/outputs/match_projection_complete_v4/data/raw_to_centerline_match_master.parquet')
use = df[df['keep_baseline'].fillna(False)].copy()
print(int(((use['matched_old'] == 0) & (use['matched_proj'] == 1)).sum()))
print(use['match_method_final'].value_counts(dropna=False).to_string())
PY
```

Inspect only the rows recovered by projection:

```bash
python - <<'PY'
import pandas as pd
df = pd.read_parquet('data_work/outputs/match_projection_complete_v4/data/raw_to_centerline_match_master.parquet')
use = df[(df['keep_baseline'].fillna(False)) & (df['match_method_final'] == 'projection_fallback')].copy()
cols = [c for c in [
    'split_id', 'raw_edge_id', 'score_proj', 'proj_area', 'proj_area_per_length',
    'proj_length_ratio', 'dist_mean_proj', 'dist_max_proj', 'angle_diff_proj',
    'candidate_count_proj', 'review_flag'
] if c in use.columns]
print(use[cols].sort_values('score_proj', ascending=False).to_string(index=False))
PY
```

## Current Best Practice

For projection debugging:

1. Keep baseline source fixed at `match_projection_complete_v2`.
2. Create a new target version for every experiment.
3. Only rerun `stage02_projection_debug.py`.
4. Then rerun `stage10_generate_figures.py --figure-group match`.

That keeps the experiments comparable and avoids redoing the first-pass match.

