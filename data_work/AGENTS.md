# AGENTS: data_work

Working rules for code and outputs under `data_work/`. Project-wide rules are in the root `AGENTS.md`.

## Before running anything

The current model specification, input bundle, and run commands are stated once, in the "Current mainline" table of [`DOCUMENT_INDEX.md`](../DOCUMENT_INDEX.md). Read it first. Do not take the mainline from script defaults, from older READMEs, or from file names.

Script defaults in `src/model/full_estimation/` still describe the old gate5 mainline (anchored form, λ = 1.1, 2022 OD). Every mainline run passes its flags explicitly, as in the commands in `outputs/beta_c_rerun_b035_summary_v1/README.md`. A `gate5` in a script name does not mean the script only runs gate5.

Python environment: `/Users/fxr/miniconda3/envs/research/bin/python`, run from `data_work/` with `--repo-root` set to this directory.

## Where code lives

| Path | Role |
|---|---|
| `src/model/full_estimation/` | Mainline model: inversion, equilibrium solver, counterfactuals, policy comparison. Entry scripts are named in the index table |
| `src/model/travel_cost_bundle/` | Builds model input bundles (`build_osm_model_bundle.py` for 1B and 1A) |
| `src/network/` | OSM networks and grid-pair travel times |
| `src/validation/` | Checks against external data (background traffic, flow validation) |
| `src/raw_baidu_validation/`, `src/centerline_baidu_validation/` | Baidu route collection and validation |
| `src/stages/` | March stage pipeline (centreline match through grid inputs); upstream of the old gate5 bundles, not rerun for the current mainline |
| `src/model/directional_cost/`, `estimation_v2/`, `full_estimation_flat/`, `road_level/` | Parallel or earlier model lines; see each README before use |
| `src/raw_connected_v1/` to `v3/` | Earlier grid travel-cost builds, kept for reproducing old bundles |

## Outputs

- Every run writes a new folder under `outputs/`. Never overwrite an existing folder; bump the `_vN` suffix instead.
- A folder that ends with only a failed `run.log` is recorded as void in `docs/WORK_LOG.md`.
- A run that becomes part of the mainline is added to [`docs/MAINLINE_MANIFEST.md`](docs/MAINLINE_MANIFEST.md), which lists what must be backed up.
- [`docs/OUTPUTS_REGISTRY.md`](docs/OUTPUTS_REGISTRY.md) sorts every output folder by category; rerun `python src/diagnostics/build_outputs_registry.py --repo-root .` after adding outputs.
- Give each retained result folder a `README.md` with the conclusion, the numbers, and the exact command.

## Recording work

- Completed work, locked decisions, and new output folders go in `docs/WORK_LOG.md`, newest entry at the top. Entries before October 2026 are in `docs/WORK_LOG_2026Q3.md` and `docs/WORK_LOG_2026Q2.md`.
- `nextstep.md` at the root holds only current status and next actions.
- Do not modify `raw_data/`.
