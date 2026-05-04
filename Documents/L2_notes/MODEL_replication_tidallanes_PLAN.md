# PLAN: MODEL_replication_tidallanes.tex

**Date:** 2026-04-29  
**Target file:** `Documents/L1_manuscripts/MODEL_replication_tidallanes.tex`  
**Purpose:** Self-contained LaTeX document describing the AA (2022) replication package as applied to Beijing tidal-lane counterfactual (Spec C only).

---

## Document Structure

### Section 1 — Replication Package Introduction

Content sources: `README.md`, `README_minimal_commuting.md`, `commuting_code_walkthrough.md`

Topics to cover:
1. **Framework**: AA (2022) exact-hat algebra — welfare and population changes from iceberg cost shocks, without observing unobservable amenities/productivities
2. **Four-step workflow**: data prep → baseline equilibrium → OD validation → counterfactual
3. **Three input files**:
   - `sparse_adjmat_seattle.csv` — bilateral iceberg commuting cost τ_{ij}  
   - `node_lr_lf_seattle.csv` — l_R (residents) and l_F (workers) per node  
   - `sparse_commute_seattle.csv` — OD commute shares π_{ij}
4. **Parameters**: θ=6.83, δ₁=0.488, λ=(1/θ)·δ₁≈0.0714, α=−0.12, β=−0.1
5. **Solver architecture**:
   - `static_eqm.py`: baseline equilibrium via bisection on χ
   - `counterfactual.py`: hat-algebra solver — inner loop (iterate x̂₁, x̂₂), outer loop (bisect χ̂)
   - `data_io.py`: loads inputs, computes ξ_{ij} from observed τ and π
6. **Python file map**: data_io → static_eqm → counterfactual → tidal_lane_specC

---

### Section 2 — Application: Beijing Data to AA Inputs

Content sources: `build_inputs_from_data_work_square_v2.py`, `qsm_input_edges_square.parquet` schema, `node_index_mapping_square.csv`

Topics to cover:
1. **Data source**: Beijing road centerline network, manual lane-rule pipeline v11 (`manual_centerline_rules_v11`)
2. **Grid**: Square spatial grid; AM-peak period only
3. **Edge data** (`qsm_input_edges_square.parquet` columns used):
   - `tau_obs_min` — observed travel time (minutes)  
   - `tau_ff_min` — free-flow travel time (minutes)  
   - `lanes_directional` — directional lane count (from centerline rules)  
   - `total_link_len_m` — total link length in meters  
   - `grid_o`, `grid_d` — origin/destination grid cell IDs  
   - `i`, `j` — raw node indices
4. **Node mapping**: `node_index_mapping_square.csv` — maps raw `node_i` → `node_i_aa_1based` (1-indexed for AA solver), with `grid_id`
5. **BPR inversion** to get observed flows:
   ξ_{ij} = lanes_{ij} · (τ_{obs,ij} / τ_{ff,ij})^{1/δ₁}
6. **v2 vs v1**: v2 uses square grid cells with manually validated centerline lane counts; v1 used earlier rule set

---

### Section 3 — Counterfactual: Tidal Lane Policy (Spec C)

Content sources: `tidal_lane_specC.py`, results in `results/tidal_lane/`

#### 3.1 Policy Motivation
- Tidal lanes convert one lane from the light-traffic direction (AM inbound) to the heavy-traffic direction (AM outbound) during peak hours
- Physically: +1 lane to slow direction, −1 lane from fast direction
- Restriction: fast direction must retain ≥ 1 lane after reallocation

#### 3.2 Selection of Treated Links
- Filter: both directions must have total_link_len_m > 200 m
- Asymmetry metric: asym_ratio = max(τ_{ij}, τ_{ji}) / min(τ_{ij}, τ_{ji})
- Selection: top 100 pairs by asym_ratio
- Skip if fast direction has < 2 lanes (cannot give 1 away)
- Result: 99 pairs treated, 1 skipped

#### 3.3 BPR-Based Travel Time Change (Fixed-Flow Assumption)

Given observed flows (from BPR inversion):
  ξ_{slow} = lanes_{slow} · (τ_{obs,slow} / τ_{ff,slow})^{1/δ₁}

New travel times after Δ=1 lane reallocation:
  τ_{slow,new} = max[ τ_{ff,slow} · (ξ_{slow} / (lanes_{slow} + 1))^{δ₁},  τ_{ff,slow} ]
  τ_{fast,new} = max[ τ_{ff,fast} · (ξ_{fast} / (lanes_{fast} − 1))^{δ₁},  τ_{ff,fast} ]

Iceberg cost change matrix:
  t̄_hat[slow direction] = τ_{slow,new} / τ_{obs,slow}  < 1  (cost falls)
  t̄_hat[fast direction] = τ_{fast,new} / τ_{obs,fast}  > 1  (cost rises)

Fixed-flow assumption note: ξ is held at observed baseline; GE flow adjustment is handled endogenously by the counterfactual solver.

#### 3.4 Welfare and Equilibrium Metric

Welfare is measured by χ (CES aggregate cost index):
  χ^{−1} = Σ_{ij} (ū_i · v̄_j / t̄_{ij})^θ

χ̂ = χ_new / χ_baseline
  - χ̂ < 1: welfare IMPROVES (commuting costs fall on net)
  - χ̂ > 1: welfare WORSENS

#### 3.5 Results (Sandbox run, tol=5e-4)

| Metric | Value |
|---|---|
| Pairs treated | 99 |
| Asym ratio range | 1.05 – 6.41 |
| t̄_hat slow direction | [0.82, 0.92] |
| t̄_hat fast direction | [1.09, 1.75] |
| τ change slow (avg) | −10.5% |
| τ change fast (avg) | +18.2% |
| χ̂ | 0.9996314 |
| (χ̂ − 1) × 10⁶ | −368.6 |
| Welfare direction | **IMPROVES** |
| l_R max gain | sq_36_72: +50.3% |
| l_R max loss | sq_14_10: −27.6% |
| l_F max gain | sq_36_65: +73.2% |
| l_F max loss | sq_53_46: −21.8% |

#### 3.6 Figures (to generate from CSVs)

1. **Figure 1**: Scatter plot of t̄_hat values — slow direction (x-axis) vs fast direction (y-axis), colored by asym_ratio. Shows the asymmetric policy shock.
2. **Figure 2**: Bar chart or sorted scatter of l_R_hat − 1 across grid nodes (resident welfare change), highlighting max gain/loss nodes.
3. **Figure 3**: Bar chart or sorted scatter of l_F_hat − 1 across grid nodes (worker welfare change), highlighting max gain/loss nodes.
4. **Figure 4** (optional): Scatter of τ_slow_obs vs τ_fast_obs for the 99 treated pairs, with size ∝ asym_ratio.

---

## Execution Steps

1. [x] Write this plan file
2. [ ] Run `tidal_lane_specC.py` to generate `chi_lr_lf_specC.csv` and `selected_pairs_specC.csv`
3. [ ] Generate figures (Python matplotlib) from CSVs → save as PDF/PNG to `Documents/L3_figs/`
4. [ ] Read skill files (pdf or docx) if needed
5. [ ] Write `MODEL_replication_tidallanes.tex` to `Documents/L1_manuscripts/`
6. [ ] Compile LaTeX (pdflatex) and verify output

---

## Key Numbers to Include

- N = number of AA nodes (to be read from node file)
- Parameters: θ=6.83, δ₁=0.488, λ≈0.0714, α=−0.12, β=−0.1
- Data: Beijing, AM-peak, square grid, v11 centerline rules
- Spec C: Δ=1 lane, top-100 pairs, len>200m
- Welfare: (χ̂−1)×10⁶ = −368.6 (IMPROVES)
- Note: fixed-flow assumption; local run with tol=1e-4 gives fully converged result
