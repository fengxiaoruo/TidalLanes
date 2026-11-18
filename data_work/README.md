# TidalLanes Data Work

Code, configuration, and versioned outputs for the TidalLanes data pipeline and structural model.

## Start here

- **Current mainline** (specification, input bundle, run commands, what the numbers measure): the "Current mainline" table in [`../DOCUMENT_INDEX.md`](../DOCUMENT_INDEX.md). It is the only place that states it.
- **Working rules and code map:** [`AGENTS.md`](AGENTS.md).
- **Mainline output folders to back up:** [`docs/MAINLINE_MANIFEST.md`](docs/MAINLINE_MANIFEST.md).
- **History of completed work:** [`docs/WORK_LOG.md`](docs/WORK_LOG.md).

`outputs/` holds about 480 run folders. Most belong to earlier specifications (gate5, 2022 OD, anchored λ = 1.1) or to experiments. A folder name alone does not say whether it is current; check the manifest.

## Layout

| Path | Contents |
|---|---|
| `raw_data/` | Source datasets; do not modify |
| `src/` | Pipeline and model code; see the code map in `AGENTS.md` |
| `config/` | Run configuration in JSON |
| `scripts/` | Shell launchers for Baidu collection and backup |
| `docs/` | Pipeline documentation, evidence notes, `WORK_LOG.md` |
| `outputs/` | One folder per run, never overwritten; excluded from Git |
| `analysis/`, `crosscheck/`, `notebooks/` | Early baseline metrics and an equilibrium cross-check |

## Stage pipeline (March 2026)

This chain builds the centreline match, speeds, grids, and the old gate5 model inputs. The current mainline does not rerun it; its inputs come from `src/network/` and `src/model/travel_cost_bundle/`.

Stages:

1. `stage01_build_centerline.py`
2. `stage02_match_raw_to_centerline.py`
3. `stage03_attach_speed.py`
4. `stage03b_centerline_asymmetry.py`
5. `stage04_estimate_lanes.py`
6. `stage05_build_grids.py`
7. `stage06_build_grid_links.py`
8. `stage07_build_od_and_population.py`
9. `stage08_build_qsm_inputs.py`
10. `stage09_compare_versions.py`
11. `stage10_generate_figures.py`

These live in [`src/stages/`](src/stages).

Unified execution entry point:

- [`src/stages/run_full_pipeline.py`](src/stages/run_full_pipeline.py)
  - runs the stage chain for a `version_id`
  - supports partial runs with `--from-stage` / `--to-stage`
  - can include or skip figure generation

The reference run of this chain is `outputs/raw_rebuild_validation`, completed end to end in March:

- centerline outputs match the prior working baseline
- raw-to-centerline match rates match the prior working baseline
- speed aggregation matches the prior working baseline
- centerline asymmetry matches the prior working baseline
- square and hex grid results are aligned with the prior working baseline up to minor ID-cleaning differences
- voronoi results now use normalized `grid_id` handling; differences from older notebook outputs reflect previous ID inconsistency in the notebook-era outputs

Notes:

- Notebook-based production logic is no longer stored in this directory.
- `outputs/` is versioned by run. Each top-level folder under `outputs/` should represent one retained pipeline run.
- `config_snapshot.stage0x.json` files record the effective parameters and inputs used by each stage for a given run.

## Reproducing the old gate5 mainline

The July deck `Tidal0715` and the April paper use the old gate5 mainline. Its specification and commands are in [`docs/STAGE2_MAINLINE_GATE5.md`](docs/STAGE2_MAINLINE_GATE5.md); its input bundle is `outputs/model_input_edgeproj_v2_lcc_gate5_v1`. Partial reruns of the stage chain are described in [`docs/HOW_TO_RERUN_PARTIAL_RESULTS.md`](docs/HOW_TO_RERUN_PARTIAL_RESULTS.md).

Stage-chain commands:

```bash
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --from-stage stage06 --to-stage stage10
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation
```

The retained output of the manual centreline matching line is `outputs/manual_centerline_rules_v11`; its version history is in `outputs/comparison/raw_centerline_match_version_summary.md`.
