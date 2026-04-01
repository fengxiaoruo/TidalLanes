# Manual Match Override Workflow

Use this when a small set of long raw segments still fail to match automatically and you want to fix them by hand in GIS.

Recommended base version:

- `data_work/outputs/match_projection_complete_v7`

Prepared review files for `v7`:

- Template:
  - `data_work/outputs/match_projection_complete_v7/metrics/manual_override_template.csv`
- Unmatched raw:
  - `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/raw_split_unmatched_v7.shp`
- Faster review subset:
  - `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/raw_split_unmatched_top80_v7.shp`
- Full directed centerline:
  - `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/centerline_directed_all_v7.shp`
- Full undirected centerline:
  - `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/centerline_undirected_all_v7.shp`

How to fill the CSV:

- Required:
  - `cline_id`
  - `dir`
- Also provide one of:
  - `split_id`
  - or `raw_edge_id`
- Optional:
  - `s_from`
  - `s_to`
  - `note`

Notes:

- `dir` should be `AB` or `BA`.
- If you only fill `raw_edge_id`, the script will apply the override automatically only when that raw edge has exactly one split segment.
- For multi-part raw edges, fill `split_id` directly.

Run stage02 with manual overrides:

```bash
python data_work/src/stages/stage02_match_raw_to_centerline.py \
  --version-id match_projection_complete_v7_manual \
  --output-dir data_work/outputs \
  --manual-overrides data_work/outputs/match_projection_complete_v7/metrics/manual_override_template.csv
```

Outputs from the manual-override run:

- `data/raw_to_centerline_match_master.parquet`
- `data/raw_split_centerline.parquet`
- `metrics/stage02_match_summary.csv`
- `metrics/manual_overrides_applied.csv`
- `metrics/manual_override_review_candidates.csv`

Recommended usage:

1. Review the top-80 unmatched shapefile first.
2. Fill only the clear cases in the CSV.
3. Run a new manual version.
4. Re-open the new unmatched shapefile and continue only if needed.
