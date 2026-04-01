# Diagnostics

This directory contains reusable diagnostics and figure-generation scripts for a versioned run under `outputs/{version_id}`.

Current scripts:

- `plot_match_diagnostics.py`
  - restores raw / centerline / matching figures
- `plot_speed_diagnostics.py`
  - restores raw vs centerline speed distribution and diurnal figures
- `plot_asymmetry_diagnostics.py`
  - restores centerline asymmetry overlay maps
- `plot_grid_diagnostics.py`
  - restores grid-link, asymmetry, tidal-intensity, mode-share, and AM two-panel figures
- `generate_all_figures.py`
  - compatibility wrapper; prefer `stage10_generate_figures.py`

Example:

```bash
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --from-stage stage10 --to-stage stage10
```

Related baseline metrics script:

- `analysis/compute_baseline_metrics.py`

Matplotlib cache:

- plotting entry points automatically use `outputs/.mpl_cache` as a writable cache directory
