# Validation Record, 2026-09-26

## What changed

The package had fallen behind the main repository on one point. On 2026-07-16 the main repository corrected `tidal_lane_diagnostics.py` so that the all-route bilateral cost index `tau_ij` is no longer converted to seconds; experienced commuting time is now the additive link-passage average `Mbar = (1/Lbar) sum_kl Xi_kl m_kl`. The package still carried the June code and the June reference tables, and its `tidal.tex` still reports the June figures (ST direct `-1.63 s`, LT total `+0.065 s`, `+95 person-min/day`).

This update brings the code and the reference tables in line with the main repository. The slide source is unchanged; see the open item below.

## Files changed

- `src/model/full_estimation/equilibrium_solve.py`, `tidal_lane_diagnostics.py`, `run_tidal_lane_diagnostics_gate5.py`: copied from `data_work/src/model/full_estimation/`. All 20 model files now match the main repository byte for byte.
- `outputs/reference_results/tidal_st_lt_compact.csv`, `tidal_st_lt_congestion_summary.csv`: from `data_work/outputs/tidal_lane_diagnostics_gate5_bpr_pos05_physical_time_v2/` (four specifications, physical minutes).
- `outputs/reference_results/alpha_beta_sensitivity_grid.csv`, `alpha_beta_sensitivity_grid_converged.csv`: from `data_work/outputs/alpha_beta_sensitivity_bpr_pos05_physical_time_v1/`.
- `scripts/package_manifest.py` and `docs/FILE_MAP.csv`: the four reference entries above now point to the physical-time directories.
- `src/model/full_estimation/ESTIMATION_NOTES.md`: a status section at the top records the five statements in the May text that the gate5 mainline supersedes (welfare object, parameters, congestion form, physical time versus cost index, sample size).

## Commands

```bash
make check
make smoke-model
python -m src.model.full_estimation.run_tidal_lane_diagnostics_gate5 --repo-root . --output-name validation_tidal_diagnostics --max-outer 500
python -m src.model.full_estimation.run_alpha_beta_sensitivity_bpr_gate5 --repo-root . --output-name validation_alpha_beta_sensitivity --sweep both
```

## Results

- `make check`: 25 slide graphics, one run-linked asset, 98 file-map rows, descriptive metrics unchanged (grid Duncan `0.260`, national SAR `0.878`, Shenzhen flow SAR `0.819`, centerline SAR `0.884`).
- `make smoke-model`: `Wbar=3.200092`, `median_tau_obs_ratio=1.0001`, converged.
- Tidal diagnostics, four specifications: maximum absolute difference against the reference is `0` on both tables.
- Alpha-beta grid, nine cells (eight converged): maximum absolute difference against the reference is `0` on both tables.

The Stage 2, policy comparison, and symmetry-suite tables were not rerun. Their code did not change, and their reference tables do not depend on `tidal_lane_diagnostics.py`.

## Headline values in the new units (top-100, alpha=+0.05, beta=-0.10)

| Quantity | Value |
|---|---|
| Baseline expected physical commuting time | 11.298 min |
| Fixed-location (short-run) time | 10.596 min, a saving of 42.1 s per commuter, 1,031 person-hours per AM peak |
| Full-equilibrium (long-run) time | 11.538 min, an increase of 14.4 s per commuter, 352 person-hours |
| Expected-utility welfare | `+0.8714%` short run, `+0.3243%` long run (unchanged) |

Across the alpha-beta grid the long-run commuting time rises in every converged cell (between `+9,837` and `+31,663` person-minutes per day). The June statement that person-minutes cross zero near `alpha=0.04` to `0.05` was an artefact of the cost-index units and no longer holds.

## Deck update, 2026-09-28

The package deck is now `Documents/PreSlides/Tidal0715.tex` (copied as `tidal.tex`, compiled from `Tidal0718.pdf` as `tidal.pdf`, speaker script `tidal_speaker_script_Tidal0715_20260718.md` copied as `tidal_speaker_script.md`). The manifest lists its 27 graphics and the linked video; the OD-validation figure that the main repository reads from `data_work/outputs/` is copied under `figs/` and the path is rewritten on sync. `make check` passes with 27 references and 102 file-map rows; `make slides` compiles 43 pages with no errors. The reduced July deck is no longer part of the package; it remains in the main repository as `Documents/PreSlides/tidal.tex`.
