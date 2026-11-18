# full_estimation — level-form scaffold (v1)

Level-form implementation of the TidalLanes structural model in
`Documents/L1_manuscripts/TidalLanes_Model_v4.tex` §6 (Estimation and
Algorithms). v1 is a **scaffold**, agreed at the 2026-05-02 meeting:

- no parameter estimation — `(theta, alpha, beta, lambda)` use the
  AA(2022) Seattle calibration as a placeholder.
- no exact-hat algebra — every counterfactual re-solves the
  equilibrium in levels.
- grid travel-cost convention stays an open design choice — the
  loader takes a `--version-id` so we can swap pipelines without
  changing the model code.

The exact-hat replication path is unchanged in
`Replication_apply/python_commuting/`. This package lives alongside it.

## Layout

```
data_work/src/model/full_estimation/
├── __init__.py
├── build_primitives.py        # Step 1
├── parameters.py              # Step 2  (placeholder calibration)
├── lambda_diagnostic.py       # Step 3  (diagnostic only)
├── invert_fundamentals.py     # Step 4
├── equilibrium_solve.py       # level-form fixed point (used by all CFs)
├── counterfactual_levels.py   # CF1 / CF2 / CF3
├── stage2_counterfactuals.py  # Stage 2 policy + tidal CFs (exog/endog)
├── run_full_estimation.py     # CLI entry point (legacy bundle default)
├── run_stage2_gate5.py        # **Stage 2 gate5 entry point**
├── README.md
├── WORKFLOW.md                # inputs -> outputs schema per step
└── ESTIMATION_NOTES.md        # per-step derivation + code thinking
```

## How to run

### Stage 2 (gate5, endogenous congestion — **mainline 2026-06-29**)

```bash
cd data_work
python3 src/model/full_estimation/run_stage2_gate5.py
python3 src/model/full_estimation/run_stage2_gate5.py --quick
python3 src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py
```

**Mainline defaults:** `congestion_form=anchored`, `lambda_phys=1.1`, `damping=0.25`, tidal pairs by **asym**.

Legacy diagnostic two-phase: `--legacy-diagnostic-two-phase --output-name stage2_endogenous_edgeproj_v2_lcc_gate5_v1`

Full spec: `data_work/docs/STAGE2_MAINLINE_GATE5.md`

Output (mainline): `data_work/outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1/`

Write-up: `Documents/L1_manuscripts/EndogenousCongestion_Stage2_gate5.tex`  
Handoff: `Documents/L2_notes/stage2_endogenous_congestion_handoff_20260628.md`

### Legacy scaffold (manual_centerline_rules_v11)

From the project root (`/Users/fxr/Dropbox/Projects/TidalLanes`):

```bash
python -m data_work.src.model.full_estimation.run_full_estimation \
  --version-id manual_centerline_rules_v11 \
  --grid-type square --period AM \
  --output-name full_estimation_square_v1
```

Useful flags:

- `--skip-counterfactual` — only run Step 1/2/4 + baseline check.
- `--cf2-top-k 100` — number of pairs treated under CF2.
- `--cf3-factors 1.10,1.25,1.50` — uniform-expansion sweep for CF3.
- `--damping 0.5 --tol 1e-5 --max-outer 60` — outer-loop tuning.

## Outputs

Everything lands under
`data_work/outputs/<output-name>/`:

```
parameters.json
lambda_diagnostic.json
primitives/
    primitives_meta.json
    primitives_nodes.csv
    primitives_edges.csv
fundamentals/
    fundamentals_arrays.npz   # ubar_theta, abar_theta, residuals
    fundamentals_meta.json
baseline_check/
    equilibrium_arrays.npz
    equilibrium_meta.json
counterfactuals/
    CF1_symmetric/           summary.csv + equilibrium_*
    CF2_tidal_lane/          summary.csv + selected_pairs.csv + equilibrium_*
    CF3_uniform_1.10/        summary.csv + equilibrium_*
    CF3_uniform_1.25/        ...
    CF3_uniform_1.50/        ...
run.log
```

## Pointer to detailed notes

- `WORKFLOW.md` — schema of inputs/outputs per step.
- `ESTIMATION_NOTES.md` — equations, algorithm choices, parameter sources,
  TODOs by step.
