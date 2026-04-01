# New Machine Setup Checklist

This checklist is for moving the project to a new computer and getting it into a usable state quickly.

## 1. Copy the project

- Copy the full project directory:
  - `/Users/fxr/Desktop/TidalLanes`
- Keep the existing output folders if possible, especially:
  - `data_work/outputs/raw_rebuild_validation`
  - `data_work/outputs/match_projection_complete_v7`
  - `Documents/todo_outputs`

These saved outputs are important because some steps are expensive and do not need to be rerun immediately.

## 2. Install Python

- Recommended: Python `3.11` or `3.12`
- Confirm:

```bash
python --version
```

## 3. Create an environment

Conda is the safest option for the geospatial stack.

Example:

```bash
conda create -n tidallanes python=3.11 -y
conda activate tidallanes
```

## 4. Install core Python packages

At minimum, install:

```bash
pip install pandas numpy scipy matplotlib tqdm pyarrow
pip install shapely geopandas rasterio scikit-image h3
```

If `geopandas` or `rasterio` fail with `pip`, use conda-forge:

```bash
conda install -c conda-forge geopandas rasterio shapely pyproj fiona gdal scikit-image h3 -y
```

## 5. Check geospatial imports

Run:

```bash
python - <<'PY'
import pandas
import numpy
import scipy
import geopandas
import shapely
import rasterio
import skimage
import h3
print("ok")
PY
```

If this fails, do not start rerunning the pipeline yet.

## 6. Check that raw data exists

Confirm these main inputs still exist:

- `data_work/raw_data/gis/roads_baidu/beijing_roads.shp`
- `data_work/raw_data/speed_Beijing_all_wgs84.csv`
- `data_work/raw_data/gis/map/北京市边界.shp`
- `data_work/raw_data/commute_202211.csv`

Quick check:

```bash
find data_work/raw_data -maxdepth 3 -type f | sed -n '1,80p'
```

## 7. Check that baseline outputs exist

Confirm the main baseline version is still present:

```bash
find data_work/outputs/raw_rebuild_validation -maxdepth 2 -type f | sed -n '1,80p'
```

Important baseline files include:

- `data_work/outputs/raw_rebuild_validation/data/centerline_master.parquet`
- `data_work/outputs/raw_rebuild_validation/data/centerline_dir_master.parquet`
- `data_work/outputs/raw_rebuild_validation/data/centerline_speed_master.parquet`
- `data_work/outputs/raw_rebuild_validation/data/grid_links_square_long.csv`
- `data_work/outputs/raw_rebuild_validation/data/qsm_input_nodes_square.parquet`
- `data_work/outputs/raw_rebuild_validation/data/qsm_input_edges_square.parquet`
- `data_work/outputs/raw_rebuild_validation/data/qsm_input_od_square.parquet`

## 8. Check that the main docs are present

These are the most useful handoff files:

- `data_work/docs/CODEX_HANDOFF_NOTE.md`
- `data_work/docs/REPRODUCIBILITY_SNAPSHOT.md`
- `data_work/docs/QUICK_RERUN_CHEATSHEET.md`
- `data_work/docs/STRUCTURAL_MODEL_WORKFLOW.md`

## 9. Verify the descriptive paper assets

If copied correctly, these should already exist:

- `Documents/todo_outputs/figs/figure1_spatial_distribution_residents_jobs.png`
- `Documents/todo_outputs/figs/figure2_average_commuting_speed_by_hour.png`
- `Documents/todo_outputs/figs/figure3_tidal_commuting_by_hour.png`
- `Documents/todo_outputs/figs/figure4_spatial_distribution_tidal_commuting.png`
- `Documents/todo_outputs/tables/table1_summary_statistics.tex`

## 10. Rebuild the paper assets if needed

If `Documents/todo_outputs` is missing, rerun:

```bash
python data_work/src/diagnostics/generate_todo_paper_assets.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --paper-dir Documents/todo_outputs
```

## 11. Recompute shortest-path travel costs if needed

```bash
python data_work/src/diagnostics/generate_reasonable_travel_costs.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --paper-dir Documents/todo_outputs
```

## 12. Check the structural model prototype

Run a quick baseline test:

```bash
python data_work/src/model/run_spatial_equilibrium.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --top-n 5 \
  --max-iter 15
```

Check outputs:

- `data_work/outputs/raw_rebuild_validation/model_square_baseline/equilibrium_summary.csv`
- `data_work/outputs/raw_rebuild_validation/model_square_baseline/calibration_summary.json`

## 13. Check the counterfactual suite

Run a light suite first:

```bash
python data_work/src/model/run_counterfactual_suite.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --grid-type square \
  --lambda-list 0.15 \
  --topn-list 5 \
  --max-iter 10
```

Then plot:

```bash
python data_work/src/diagnostics/plot_model_counterfactuals.py \
  --version-id raw_rebuild_validation \
  --output-dir data_work/outputs \
  --model-subdir model_square_suite
```

## 14. Check LaTeX

The project includes paper drafts:

- `Documents/TidalLanes_0203.tex`
- `Documents/TidalLanes_EconDraft_0318.tex`

If you need PDF compilation, check for `xelatex`:

```bash
xelatex --version
```

If missing, install a TeX distribution such as MacTeX or TeX Live.

Quick compile test:

```bash
cd Documents
xelatex TidalLanes_EconDraft_0318.tex
```

## 15. GIS-side check

If manual spatial inspection is needed, confirm that `shp` outputs open correctly in GIS software.

Useful review layers:

- `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/raw_split_unmatched_v7.shp`
- `data_work/outputs/match_projection_complete_v7/gis_exports/manual_override_review_v7/centerline_directed_all_v7.shp`

## 16. Common machine-specific issues

- `matplotlib` cache errors
  - set `MPLCONFIGDIR` to a writable folder
- `geopandas` import failures
  - usually caused by `gdal / fiona / pyproj` installation mismatch
- `rasterio` install errors
  - safer through `conda-forge`
- slow or failing `stage01`
  - do not rerun immediately unless necessary; it is computationally heavy
- missing Chinese path support
  - check terminal locale and filesystem permissions

## 17. What to tell Codex on the new machine

Use this prompt:

```text
Please first read data_work/docs/CODEX_HANDOFF_NOTE.md and data_work/docs/NEW_MACHINE_SETUP_CHECKLIST.md, then continue taking over this project.
```

## 18. Practical recommendation

Before doing any heavy rerun:

1. Confirm the copied outputs are intact.
2. Confirm the geospatial Python stack imports cleanly.
3. Confirm the structural-model scripts run on the copied baseline.
4. Only then consider rerunning stage-level pipeline steps.
