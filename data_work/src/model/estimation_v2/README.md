# estimation_v2 — directional-congestion QSE estimation, rebuilt from scratch

A clean, from-scratch reimplementation of the structural estimation framework,
built **function by function**, confirming each step. Goal: every line is
understood, not inherited.

## Working agreement

- Each step: (1) explain the goal + the math/intuition in plain language, (2) write
  one small function, (3) walk through it line by line, (4) run a tiny check, (5) confirm,
  then move on. No skipping ahead.
- Python, minimal dependencies (numpy, pandas, scipy). No repo-wide side effects.
- Inputs: the model-ready QSM files (we do NOT redo the upstream GIS/speed ETL).

## Roadmap

1. **Primitives** — load the network (nodes, directed edges: ℓ, v₀, v_obs, n, τ_ff, τ_obs)
   and commuting OD (L_ij, marginals L_R/L_F, Lbar). Understand every object.
2. **Parameters** — θ, α, β, λ: meaning, sources, the λ=1.1 decision.
3. **Bilateral commuting cost / route aggregator** — A_kl = t_kl^{-θ},
   τ_ij^{-θ} = (I−A)^{-1}; spectral-radius condition and κ. (Conceptual core.)
4. **Invert fundamentals** — recover ū_i, Ā_j so the observed allocation is an equilibrium.
5. **Equilibrium solver** — market access, gravity, congestion fixed point; with the
   stability lessons baked in (in-loop κ, free-flow cap / observed-anchored congestion).
6. **Counterfactuals** — symmetric vs directional, tidal-lane, uniform expansion; welfare.
7. **Diagnostics & validation** — convergence, λ-robustness, sign checks.

## Inputs (Step 1) — files and column → model-symbol mapping

Source dir: **`data_work/outputs/model_input_edgeproj_v2_lcc_v1/data/`** (locked 2026-06-28; see `Documents/L2_notes/travel_cost_lock_in_20260628.md`), grid = square, period = AM.

**`qsm_input_nodes_square.parquet`** — one row per grid cell (N = 2736):
| column | model symbol | meaning |
|---|---|---|
| `node_i` | i | integer node index (0…N−1) |
| `grid_id` | — | human-readable cell id (e.g. sq_10_16) |
| `residents` | (L_i^R, raw) | observed residents in cell |
| `jobs` | (L_j^F, raw) | observed jobs in cell |
| `keep_for_model` | — | 1 = used in model |
| `component` | — | connected-component id |

**`qsm_input_edges_square.parquet`** — one row per directed grid→grid link (period AM):
| column | model symbol | meaning |
|---|---|---|
| `i`, `j` | (k,l) | directed edge from node i to node j |
| `tau_obs_min` | τ_obs (min) | observed peak travel time = distance / harmonic-mean speed |
| `tau_ff_min` | τ_ff (min) | free-flow travel time = distance / free-flow speed |
| `total_link_len_m` | ℓ (m) | physical length |
| `lanes_directional` / `lane_est_length_weighted` | n | directed lane count |
| `capacity_proxy` | — | lane-based capacity proxy |

**`qsm_input_od_square.parquet`** — one row per home→work grid pair (377,708 rows):
| column | model symbol | meaning |
|---|---|---|
| `home_i`, `work_i` | (i,j) | residence node, workplace node |
| `commuters_road` | L_ij | road-mode commuting flow (the bilateral flow we model) |
| `commuters_total/car/car_bus` | — | alternative flow definitions (kept for robustness) |
| `pop`, `type_*` | — | raw survey pop and mode shares |

**Design note to settle in Step 1:** the model's marginals L_R_i, L_F_j can come from
either (a) the node table's `residents`/`jobs`, or (b) the OD row/column sums of
`commuters_road`. These need not be identical (population raster vs commuting survey).
The previous code used (b). We will decide and document this explicitly.

## Status

- [x] Step 0 — folder + roadmap (this file)
- [ ] Step 1 — primitives (in progress: inputs inspected)
