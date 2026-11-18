# Minimal Package Manifest

This package is the smallest self-contained unit that can:

1. compile the current model slides;
2. verify slide descriptive metrics against compact reference tables;
3. rerun the main structural pipelines and match the slide headline numbers.

## Included By Design

| Layer | Count | Purpose |
| --- | ---: | --- |
| Slide source | 4 | `tidal.tex` (the Tidal0715 seminar deck), `references.bib`, compiled `tidal.pdf`, `tidal_speaker_script.md` |
| Slide figures | 28 | every `\includegraphics` / run-linked asset in `tidal.tex` (27 graphics, one linked video) |
| Aggregate model inputs | 7 files | gate5 square-grid nodes, edges, OD, QA/config snapshots |
| Reference result tables | 22 files | slide numbers + `make validate-full` comparison targets |
| Model code | 20 `.py` files | only modules imported by the validation runners |

## Model Code Kept

**`src/model/full_estimation/` (16 files)**

- Core library: `build_primitives`, `parameters`, `invert_fundamentals`, `equilibrium_solve`, `lambda_diagnostic`, `stage2_counterfactuals`, `counterfactual_levels`, `phase_metrics_common`, `tidal_lane_diagnostics`
- Runners used by validation: `run_stage2_gate5`, `run_tidal_lane_diagnostics_gate5`, `run_policy_comparison_gate5`, `run_alpha_beta_sensitivity_bpr_gate5`, `run_symmetry_directionality_suite_gate5`, `run_alpha_robustness_quick_gate5`

**`src/model/directional_cost/` (4 files)**

- `fixed_tau_model`, `policy_counterfactuals`, `tidal_lane_counterfactuals`, package marker

## Explicitly Excluded

- Raw Baidu speed / OD files
- Plotting-only scripts and stage-1 exploratory runners
- Figure-generation pipelines (maps are copied as static `figs/`)
- Audit JSON not referenced by slides or validation (`wbar_theta_decomposition`, `pi_inv_decomposition`, `phase_a_st_metrics`)
- Unused slide assets (`pat_commute_direction_evidence.png`, `fieldvisit_3.jpg`)
- Validation reruns under `outputs/validation_*` and visual QA renders

## Main-Project Link

- `MAIN_REPO` text file holds a **relative** path to the main repo (default: `..`)
- `docs/FILE_MAP.csv` maps every managed package file to a relative main-project path
- Refresh: `make sync-from-main`
- Push back one file: `make push-to-main FILE='tidal.tex'`

See `docs/SYNC_WORKFLOW.md` for the full rhythm.
