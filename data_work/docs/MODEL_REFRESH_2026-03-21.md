# Model Refresh 2026-03-21

This note records the overnight refresh that updates the current model run to the latest implemented:

- grid-edge travel-cost construction,
- free-flow cost definition,
- congestion primitive,
- parameter-estimation workflow,
- output versions used by the paper drafts.

## 1. Version and Scope

- Working output version: `data_work/outputs/match_projection_complete_v7_latest`
- Main model file: [`spatial_equilibrium.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py)
- Main equation notes:
  - [`CURRENT_MODEL_EQUATIONS.md`](/Users/fxr/Desktop/TidalLanes/data_work/docs/CURRENT_MODEL_EQUATIONS.md)
  - [`CURRENT_MODEL_EQUATIONS.tex`](/Users/fxr/Desktop/TidalLanes/data_work/docs/CURRENT_MODEL_EQUATIONS.tex)

## 2. Travel-Cost Construction

### Observed edge cost

The current observed grid-edge cost is still:

\[
t_{kl}^{obs} = 60 \cdot \frac{d_{kl}^{grid}}{v_{kl}^{harm}}
\]

where:

- `d_{kl}^{grid}` is the centroid-to-centroid distance between adjacent grids,
- `v_{kl}^{harm}` is the harmonic mean speed across crossing centerline pieces:

\[
v_{kl}^{harm}
=
\frac{\sum_e \ell_e}{\sum_e \ell_e / v_e}.
\]

### Free-flow edge cost

The new free-flow speed uses the raw speed data in the `22:00` to `05:00` window:

\[
v_{kl}^{ff}
=
\frac{\sum_e \ell_e}{\sum_e \ell_e / v_e^{ff}}
\]

where `v_e^{ff}` is built from centerline observations in the `22:00`--`05:00` window.

The free-flow edge cost is:

\[
t_{kl}^{ff} = 60 \cdot \frac{d_{kl}^{grid}}{v_{kl}^{ff}}.
\]

## 3. Congestion Primitive

The solver now uses:

\[
t_{kl} = t_{kl}^{ff}\left(\frac{\phi_{kl}}{n_{kl}}\right)^\lambda
\]

instead of the older observed-time-anchored relative-density update.

## 4. Parameter Workflow

- `theta`, `alpha`, `beta`: baseline external calibration
- `lambda`: cross-sectional estimate from

\[
\log\left(\frac{t_{kl}^{obs}}{t_{kl}^{ff}}\right)
=
\eta_{\text{lane-bin}(kl)} + \lambda \log\left(\frac{\phi_{kl}^{obs}}{n_{kl}^{obs}}\right) + u_{kl}
\]

with shortest-path all-or-nothing observed-flow proxy.

## 5. Files Touched in This Refresh

- [`stage03_attach_speed.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/stage03_attach_speed.py)
- [`stage06_build_grid_links.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/stage06_build_grid_links.py)
- [`stage08_build_qsm_inputs.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/stages/stage08_build_qsm_inputs.py)
- [`spatial_equilibrium.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/model/spatial_equilibrium.py)
- [`run_spatial_equilibrium.py`](/Users/fxr/Desktop/TidalLanes/data_work/src/model/run_spatial_equilibrium.py)

## 6. Run Log

- `stage03`: rerun on `match_projection_complete_v7_latest` with free-flow window tags and retained `total_dist_m`, `total_time_h`
- `stage06`: rebuilt grid links with `t_min` and `t_ff_min`
- `stage08`: rebuilt QSM edge inputs with `t_ff_min`
- `stage03b`: reran asymmetry outputs after the updated speed aggregation
- `stage10`:
  - reran `speed` figures
  - reran `grid` figures
- `model` baseline runs completed for:
  - `model_square_ff2205_estlambda`
  - `model_hex_ff2205_estlambda`
  - `model_voronoi_ff2205_estlambda`
  - `model_square_suite_ff2205`

## 7. Key Calibration Outputs

### Square baseline

- output dir: [`model_square_ff2205_estlambda`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/match_projection_complete_v7_latest/model_square_ff2205_estlambda)
- `theta` used in solver: `6.83` (`external_default`)
- `lambda` used in solver: `0.01` (`estimated_cross_section`)
- diagnostic `theta_hat`: `0.6194`
- diagnostic `lambda_hat`: `0.01`

Main summary:

- baseline welfare: `21.2232`
- baseline average edge time: `4.2962` minutes
- baseline weighted average commute time: `8.5928` minutes
- top-5 tidal-lane counterfactual welfare change: `+0.0499%`
- top-5 tidal-lane counterfactual commute-time change: `-0.0103` minutes

### Hex baseline

- output dir: [`model_hex_ff2205_estlambda`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/match_projection_complete_v7_latest/model_hex_ff2205_estlambda)
- `lambda` used in solver: `0.01`
- diagnostic `theta_hat`: `0.6988`
- diagnostic `lambda_hat`: `0.01`

Main summary:

- baseline welfare: `22.2334`
- baseline weighted average commute time: `3.0556` minutes
- top-5 tidal-lane counterfactual welfare change: `-0.0089%`

### Voronoi baseline

- output dir: [`model_voronoi_ff2205_estlambda`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/match_projection_complete_v7_latest/model_voronoi_ff2205_estlambda)
- `lambda` used in solver: `0.01`
- diagnostic `theta_hat`: `0.4307`
- diagnostic `lambda_hat`: `0.01`

Main summary:

- baseline welfare: `8.7435`
- baseline weighted average commute time: `11.5208` minutes
- top-5 tidal-lane counterfactual welfare change: `+0.0153%`

## 8. Square Robustness Suite

- output dir: [`model_square_suite_ff2205`](/Users/fxr/Desktop/TidalLanes/data_work/outputs/match_projection_complete_v7_latest/model_square_suite_ff2205)
- lambdas run: `0.01`, `0.05`, `0.15`
- scenarios included:
  - baseline
  - symmetric
  - congestion-top-5 / top-10
  - tidal-top-5 / top-10

Selected results:

- at `lambda = 0.01`:
  - `congestion_top_5`: welfare `+0.0164%`, commute time `-0.0002` min vs baseline
  - `tidal_top_5`: welfare `+0.0001%`, commute time `-0.0000` min vs baseline
- at `lambda = 0.05`:
  - `congestion_top_5`: welfare `+0.1219%`, commute time `-0.0223` min vs baseline
  - `tidal_top_5`: welfare `+0.0842%`, commute time `-0.0136` min vs baseline
- at `lambda = 0.15`:
  - `congestion_top_5`: welfare `+0.0611%`, commute time `-0.0100` min vs baseline
  - `tidal_top_5`: welfare `-0.1905%`, commute time `-0.0176` min vs baseline

## 9. Data Refresh Snapshots

- `stage03_speed_summary.csv`:
  - matched speed observations: `23,261,328`
  - centerline-time rows: `398,474`
  - mean speed: `36.43` km/h
  - median speed: `33.48` km/h
- `stage03b_asymmetry_summary.csv`:
  - AM unsym1 count: `407`
  - PM unsym1 count: `360`
  - reversed count: `1,045`
  - tidal candidates: `13`
- `stage07_square_summary.csv`:
  - reachable OD pairs: `374,850`
  - reachable pair share: `84.7%`
  - reachable population share: `95.3%`

## 10. Draft Update Targets

- [`TidalLanes_0320.tex`](/Users/fxr/Desktop/TidalLanes/Documents/TidalLanes_0320.tex)
- [`TidalLanes_EconDraft_0318.tex`](/Users/fxr/Desktop/TidalLanes/Documents/TidalLanes_EconDraft_0318.tex)

The paper drafts should be updated to match:

- the current implemented travel-cost definition,
- the current free-flow definition (`22:00`--`05:00` mean speed),
- the current congestion primitive,
- the current parameter workflow,
- the latest output paths and figures.

## 11. Compile Status

- [`TidalLanes_EconDraft_0318.pdf`](/Users/fxr/Desktop/TidalLanes/Documents/TidalLanes_EconDraft_0318.pdf) compiled successfully with `xelatex`
- [`TidalLanes_0320.pdf`](/Users/fxr/Desktop/TidalLanes/Documents/TidalLanes_0320.pdf) now compiles successfully with `xelatex`; remaining messages are float-placement and overfull-box warnings rather than fatal errors
