# Stage 2 gate5 — mainline specification (2026-06-29)

> **Historical (note added 2026-11-18).** This was the locked specification of the old gate5 mainline (June 2026, anchored λ = 1.1, 2022 OD). It still describes how to reproduce `Tidal0715` and the April paper. The current mainline (OSM 1B, all-day 2025 OD with 07:00 to 09:00 link times, `ce_floor` β_c = 0.35) is stated in the "Current mainline" table of `DOCUMENT_INDEX.md`. Text below is unchanged.

**Travel cost:** `raw_connected_v1` → `edgeproj_v2` → `model_input_edgeproj_v2_lcc_gate5_v1` (**not centerline**)

## Locked mainline

| Item | Value |
|------|--------|
| Congestion form | **anchored (E6)** — semi-lock level at `τ_obs` |
| `lambda_phys` | **1.1** (documented / Barwick calibration) |
| Outer damping | **0.25** (anchored) |
| Tidal pair selection (default) | **asym** (pure τ asymmetry); optional `score_st` / `score_lt` |
| `theta, alpha, beta` | AA Seattle placeholders (unchanged) |

**Robustness (do not overwrite mainline outputs):**

- `lambda_phys = 0.572` — P0/P1 lower bound (`p1_congestion_calibration_gate5_v1.json`)
- `congestion_form = legacy` + diagnostic λ — old two-phase pipeline (`--legacy-diagnostic-two-phase`)

## Commands

```bash
cd data_work

# Stage 2 full mainline
python3 src/model/full_estimation/run_stage2_gate5.py

# Quick smoke
python3 src/model/full_estimation/run_stage2_gate5.py --quick

# Tidal ST/LT diagnostics (mainline λ=1.1)
python3 src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py

# Tidal with corridor-score selection (robustness / policy shortlist)
python3 src/model/full_estimation/run_tidal_lane_diagnostics_gate5.py \
  --pair-selection score_st \
  --output-name tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_score_st_v1

# Legacy diagnostic pipeline (preserved for comparison)
python3 src/model/full_estimation/run_stage2_gate5.py \
  --legacy-diagnostic-two-phase \
  --output-name stage2_endogenous_edgeproj_v2_lcc_gate5_v1
```

## Default outputs

| Run | Directory |
|-----|-----------|
| Stage 2 mainline | `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_anchored_v1/` |
| Tidal diagnostics | `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_v1/` |
| Legacy diagnostic | `outputs/stage2_endogenous_edgeproj_v2_lcc_gate5_v1/` (old) |
| λ=0.57 robustness tidal | `outputs/tidal_lane_diagnostics_edgeproj_v2_lcc_gate5_p1_calib_v1/` |

## Related notes

- P1 QA + corridor scoring: `Documents/L2_notes/p1_edgeproj_qa_tidal_corridor_handoff_20260629.md`
- λ=1.1 form comparison: `Documents/L2_notes/congestion_lambda11_density_vs_anchored_20260629.md`
- Stage 2 handoff: `Documents/L2_notes/stage2_endogenous_congestion_handoff_20260628.md`
