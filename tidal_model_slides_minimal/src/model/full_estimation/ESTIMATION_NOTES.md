# Estimation Notes — full_estimation v1

## Status update, 2026-09-26 (read this first)

The step-by-step text below was written for the May 2026 scaffold. The code in
this package is the gate5 mainline, and five statements below are superseded.
Where the two disagree, this section wins.

1. **Welfare.** The main welfare object is the gauge-free Frechet denominator,
   `Wbar^theta = sum_ij tau_ij^{-theta} u_i^theta w_j^theta`, with
   `u_i^theta = ubar_i^theta (L_i^R)^{beta theta}` and
   `w_j^theta = abar_j^theta (L_j^F)^{alpha theta}`. The market-access form
   `sum_i u_i^theta Pi_i^{-theta}` in Step 5 is kept only as `Wbar_pi_raw`, a
   diagnostic. It carries a normalisation drift in `(Pi, P)` across scenarios
   and its sign is not a welfare result (see
   `Documents/L2_notes/estimation_audit_welfare_gauge_20260630.md`).
2. **Parameters.** Mainline counterfactuals use `alpha = +0.05`,
   `beta = -0.10`, `theta = 6.83`, and the physical congestion elasticity
   `lambda_phys = 1.1`. The `alpha = -0.12` and `lambda = 0.488/6.83` values in
   Step 2 are the AA Seattle placeholders still stored in `parameters.py`;
   `lambda_` is used only in the exogenous-tau diagnostics.
3. **Congestion form.** The mainline is `anchored`: physical minutes update as
   `m = max(m_ff, m_obs [(Xi/Xi^0)(n^0/n)]^lambda)` around the observed
   allocation, with log damping `0.25` (the `legacy` update in Step 5 is a
   robustness form). The baseline therefore starts at `m_obs`, not at `m_ff`.
4. **Physical time versus the cost index.** `tau_ij` is the all-route iceberg
   index and is never in minutes. Experienced commuting time is the additive
   link-passage average `Mbar = (1/Lbar) sum_kl Xi_kl m_kl`, and the ST/GE/sorting
   decomposition in `tidal_lane_diagnostics.py` is written in that space. The
   June reference tables that reported `tau_ij` differences in seconds were
   replaced on 2026-09-26 by the `physical_time` runs listed in
   `docs/FILE_MAP.csv`.
5. **Sample size.** The gate5 bundle has `N = 1104` nodes and `2642` directed
   links, not `N ~ 2700`. Row sums of `A = 1/m` stay below one at `kappa = 1`.

The CF1 symmetric default is `geometric`; `arithmetic` is the documented
alternative and is the one used for the physical-time symmetry result.

---

This file walks through every step of the level-form scaffold:
*target object*, *formula reference*, *algorithm and solver*,
*parameter values with source*, and *open TODOs*. The equation labels
follow `Documents/L1_manuscripts/TidalLanes_Model_v4.tex`.

## Conventions

| Symbol | Meaning |
|--------|---------|
| `i,j`  | spatial cells (`grid_id`, e.g. `sq_10_16`), indices in `0..N-1` |
| `(k,l)`| directed edge from cell `k` to cell `l` |
| minutes| physical units of `tau_ff_min`, `tau_obs_min` (from QSM stage 06) |
| iceberg| dimensionless cost `t_kl = (tau_min)^{1/theta}` (v4 §3.3) |

Iceberg vs. minutes — v4 §3.3 fixes
`t_kl = (ell_kl / v_kl)^{1/theta} = (tau_min_kl)^{1/theta}`.
So `A_kl ≡ t_kl^{-theta} = 1 / tau_min_kl`. We carry both
representations in the solver and always cite which we are using.

The 2026-05-02 meeting fixed three top-level rules for v1:

1. No parameter estimation — use AA(2022) Seattle calibration.
2. No exact-hat — counterfactuals re-solve in levels.
3. Grid travel-cost convention is open — `--version-id` controls it.

---

## Step 1 — `build_primitives`

**Target object**: the directed-edge and population primitives in
v4 Table 1 panel A and B, plus the dense OD matrix.

**Formula refs**: §3.1 (network), §3.4 (population shares).

**Algorithm**: pure pandas. Three things matter.

1. *Node reindexing.* The QSM parquet uses a non-contiguous `node_i`.
   We filter on `keep_for_model == 1`, sort by `node_i`, build a map
   to contiguous `0..N-1`. The map is applied to edges and to OD
   identically — without this the matrix solver gets misaligned.

2. *Lane fallback.* `lanes_directional` is missing on some edges
   (the QSM stage 06 export ships a `lane_quality_flag`); we fall
   back to `lane_est_length_weighted`, then to the mean
   `nanmean(n_kl)`. Documented in
   `data_work/docs/_superseded/STRUCTURAL_MODEL_WORKFLOW.md`.

3. *OD column choice.* Default `commuters_road` (car + bus
   on-road commuters). Switchable via `--od-pop-col`. The meeting
   note keeps the mode definition open for now.

**Parameter values**: none.

**TODOs**:

- Re-evaluate the OD column once the cross-city subsection adds
  transport-mode shares (Meeting note: "supply-side differences,
  including transport-mode shares").
- Reconcile against the alternative grid travel-cost methods in
  `data_work/outputs/comparison_grid_methods/`.

---

## Step 2 — `parameters`

**Target object**: the four-tuple `(theta, alpha, beta, lambda)`.

**Formula refs**: §3.5, §6.2, §6.3.

**Values and sources**:

- `theta = 6.83` — AA(2022) Seattle, taken from Ahlfeldt, Redding,
  Sturm, Wolf (2015). This is the Frechet shape that governs both
  the residence–workplace choice and (under §3.2) the route choice.
- `alpha = -0.12` — AA(2022) Seattle workplace externality.
  (v4 §3.4 writes `alpha > 0`; the negative sign convention used in
  `Replication_apply/PLAN.md` is the iceberg-cost mapping. We keep
  the AA replication-code sign here so the two pipelines line up.)
- `beta  = -0.10` — AA(2022) Seattle residential externality (same
  sign convention as above).
- `lambda = delta1 / theta = 0.488 / 6.83 ≈ 0.07144` — v4 §6.3
  `lambda = delta1 / theta` with delta1 imported from the AA Seattle
  congestion regression.

**TODOs**:

- Cross-check against Professor You Wei's related work; the meeting
  note flags this as a pre-requisite before locking calibration.
- The `alpha`/`beta` sign convention should be unified across the
  level-form path and the exact-hat path in
  `Replication_apply/python_commuting/`.

---

## Step 3 — `lambda_diagnostic`

**Target object**: a diagnostic estimate of the congestion elasticity
`lambda`. Not used to override the placeholder in Step 2.

**Formula ref**: v4 eq.(lambda_regression),

```
ln( tau_obs / tau_ff )_kl  =  + lambda * ln( Xi_hat_kl / n_kl ) + eta_kl + eps_kl
```

**Algorithm**: OLS after lane-bin demeaning.

- We do **not** observe directed traffic — `Xi_hat_kl` comes from the
  baseline equilibrium (Step 5 at observed-data inputs).
- Lane bins (integer `n_kl`) absorb the `eta_kl` heterogeneity term,
  i.e. the cross-section analogue of edge fixed effects.
- The slope on demeaned `ln(Xi_hat/n)` is the reported `lambda_hat`.

**Why diagnostic only**:

1. `Xi_hat_kl` is a model-implied quantity, not measured. Any
   reduced-form regression of speeds on model-implied traffic suffers
   from the same circularity v4 §6.3 flags.
2. Single cross-section, no IV, no panel — exactly the limitations
   v4 §6.3 already lists.

**TODOs**:

- IV for `Xi_kl / n_kl` (lane-capacity exogenous variation,
  upstream/downstream demand shocks).
- Once panel-frequency speed/flow data are available (Baidu crawl
  variants under `national_tidal_commuting_*`), replace with the
  panel specification in v4 eq.(lambda_panel).

---

## Step 4 — `invert_fundamentals`

**Target object**: `(ubar_i^theta, Abar_j^theta, chi)`.

**Formula refs**: v4 §6.4, eqs (invert_R)–(invert_F):

```
(l_i^R_obs)^{1-beta*theta}  = chi * sum_j tau_ij_obs^{-theta} ubar_i^theta abar_j^theta (l_j^F_obs)^{alpha*theta}
(l_j^F_obs)^{1-alpha*theta} = chi * sum_i tau_ij_obs^{-theta} ubar_i^theta abar_j^theta (l_i^R_obs)^{beta*theta}
```

with `A_kl_obs = t_kl_obs^{-theta} = 1 / tau_obs_min_kl` (§3.3) and
`tau_ij_obs^{-theta} = [(I - A_obs)^{-1}]_{ij}` (§3.2).

**Algorithm**: alternating updates of `(v_i, w_j) = (ubar^theta, abar^theta)`,
geometric-mean normalisation per step, `chi` recomputed as the
geometric mean of the implied per-equation residual constants:

```
a_i      = sum_j tau_ij^{-theta} (l_F)^{alpha*theta} w_j
v_i_new  = (l_R)^{1-beta*theta} / (chi * a_i)
v_i_new  <- v_i_new / gmean(v_i_new)

b_j      = sum_i tau_ij^{-theta} (l_R)^{beta*theta} v_i
w_j_new  = (l_F)^{1-alpha*theta} / (chi * b_j)
w_j_new  <- w_j_new / gmean(w_j_new)

chi_new  = exp( 0.5 * mean(log(chi_R)) + 0.5 * mean(log(chi_F)) )
```

Stop when `max(|log v_new - log v|, |log w_new - log w|) < tol`.

**Solver pick**: dense `scipy.linalg.solve(I - A, I)` for the
`(I - A_obs)^{-1}` step. At `N ≈ 2700` this costs ~few seconds; we
call it once outside the loop because `tau_obs` is fixed.

**Parameter values**: floors `floor_share = 1e-6` on `(L_R_obs, L_F_obs)`
before normalisation. This mirrors the AA-replication fix described
in `Replication_apply/PLAN.md` §5 (110 nodes with `l_R = 0`, 58 with
`l_F = 0`).

**Outputs**:

- `ubar_theta`, `abar_theta` — both with geometric mean 1.
- `chi` — scalar.
- `resid_R, resid_F` — log-share gaps; under Assumption ass:obs_eq
  these should be close to zero. Large residuals flag a mis-match
  between the observed allocation and the assumed equilibrium.

**TODOs**:

- Add a unit test that for synthetic primitives drawn from a known
  equilibrium, the inverter recovers `(v, w)` to floating tolerance.

---

## Step 5 — `equilibrium_solve` (level form)

**Target object**: the equilibrium tuple `Q` of Definition 4.1.

**Formula refs**: v4 (E1)–(E6) and the share-form (eq_R)–(eq_F).

**Algorithm — outer loop on `tau_min_kl`** with damping:

```
Initialise tau_min_kl = tau_ff_kl   (no congestion start)
for it in 1..max_outer:
    A_kl            = 1 / tau_min_kl                          # (E1)
    tau_ij^{-theta} = (I - A)^{-1}                             # (E1)
    (l_R, l_F)      = solve_lR_lF(tau, ubar, abar, params)    # (eq_R)(eq_F)
    (Pi_i, P_j)     = market_access(tau, l_R, l_F, Lbar, theta)# (E4)
    Xi_kl           = t_kl^{-theta} * P_k^{-theta} * Pi_l^{-theta}  # (E5)
    tau_min_new     = tau_ff * (Xi_kl / n_kl)^lambda          # (E6)
    tau_min         = exp( d * log(tau_min_new) + (1-d) * log(tau_min) )
    if max|log change| < tol: stop
```

`solve_lR_lF` is itself an alternating fixed-point in shares,
analogous to the inversion routine in Step 4 but with `(v, w)`
fixed and `(l_R, l_F)` unknown:

```
rhs_R_i  = chi * v_i * sum_j tau_ij^{-theta} w_j (l_F)^{alpha*theta}
l_R_new  = rhs_R_i ^ (1 / (1 - beta*theta))
l_R_new <- l_R_new / sum(l_R_new)
rhs_F_j  = chi * w_j * sum_i tau_ij^{-theta} v_i (l_R_new)^{beta*theta}
l_F_new  = rhs_F_j ^ (1 / (1 - alpha*theta))
l_F_new <- l_F_new / sum(l_F_new)
```

`market_access` solves (E4) by alternating updates on `(Pi, P)` —
each step rebuilds `Pi_i^{-theta}` and `P_j^{-theta}` from the
current other side and the (fixed) marginals.

**Solver pick & cost**:

- `(I - A)^{-1}` is recomputed every outer iteration because `A`
  changes with `tau_min`. At `N ≈ 2700`, each outer iter is a few
  seconds; we cap `max_outer = 60` (CLI flag).
- Damping `0.5` (CLI flag); the AA paper recommends `0.1–0.3`. We
  found `0.5` faster on smooth shocks and stable on the 1% test
  cases used in `Replication_apply/PLAN.md`.

**Welfare** (superseded, see the 2026-09-26 status update at the top):

```
Wbar^theta = sum_ij tau_ij^{-theta} * u_i^theta * w_j^theta     # main, gauge-free
Wbar_pi_raw^theta = sum_i u_i^theta * Pi_i^{-theta}             # diagnostic only
u_i^theta  = ubar_i^theta * (L_i^R)^{beta*theta}
w_j^theta  = abar_j^theta * (L_j^F)^{alpha*theta}
```

We report `Wbar` (not `Wbar / Wbar_baseline`)
because all comparisons in this scaffold are run side-by-side.

**Spectral-radius safety (`kappa`)**:

On the Beijing square grid (N≈2700), some nodes have many
outgoing edges with low `tau_obs_min`. The raw row sums of
`A_kl = 1/tau_obs_min_kl` reach ~1.4, so `(I - A)^{-1}` diverges. We
add a uniform `kappa` rescaling: `A_kl ← 1 / (kappa * tau_min_kl)`.
`_pick_kappa` chooses `kappa = max(1, max_rowsum_raw / 0.9)`, which
guarantees row sums (and hence spectral radius) ≤ 0.9.

Why this is safe: a uniform `kappa` is equivalent to rescaling every
`t_kl` by `kappa`. The model is invariant in *counterfactual ratios*
to such a rescaling (τ_ij scales by `kappa`, but `τ_ij_cf / τ_ij_base`
does not). The *absolute level* of `tau_ij` and `Wbar` does shift.
This is acceptable for v1 — the meeting note prioritises the
workflow and CF comparisons, not absolute welfare numbers.

**TODOs**:

- Replace the dense solve with a sparse iterative Neumann series when
  `A` is sparse enough that the truncated series converges in fewer
  matvecs than the dense solve.
- Document `kappa` in any plot or table that reports level `Wbar`.

---

## Counterfactuals (CF1 / CF2 / CF3)

**Common architecture**: each counterfactual is a thin wrapper that
mutates either `tau_min_init` or `n_kl_override` and re-runs
`solve_equilibrium`. Fundamentals `(ubar_theta, abar_theta)` are
locked.

**CF1 — symmetric corridor cost** (`build_symmetric_tau_min`):

- For each bidirectional pair `(i,j), (j,i)`, replace both
  `tau_obs_min` with their arithmetic mean.
- Unpaired one-way edges keep `tau_obs_min`.
- We pass this as `tau_min_init`; the outer loop then re-solves with
  the same congestion technology. The equilibrium we read off is the
  GE re-mixing under symmetric costs.

**CF2 — tidal-lane reallocation** (`select_tidal_lane_pairs`):

- Treated set: top-`top_k` bidirectional pairs by
  `asym = tau_obs[slow] / tau_obs[fast]`.
- Filter `min_length_m = 200 m` on both directions
  (`Replication_apply/PLAN.md §3.2`).
- Skip a pair if `n_kl[fast] - Delta < 1`.
- Apply `n_kl[slow] += Delta`, `n_kl[fast] -= Delta`, default
  `Delta = 1.0`.
- Saved sidecar `selected_pairs.csv` records each treated pair's
  pre-shock state for later inspection.

**CF3 — uniform expansion**:

- `n_kl_cf = factor * n_kl`, default sweep `1.10, 1.25, 1.50`.

**Reported metrics** (`summarise_diff`):

- `Wbar_baseline`, `Wbar_cf`, `Wbar_pct_change` (welfare)
- `tau_min_mean_*` (average congestion level)
- `l_R_max_abs_log_change`, `l_F_max_abs_log_change` (largest
  spatial reallocation in log shares)
- `converged_*` flags from both runs

**Why level-form, not exact-hat**:

The meeting note (2026-05-02) reads: "Do not focus on parameter
estimation for now. Build the full estimation framework first,
excluding exact-hat, and make the counterfactual workflow explicit."
A level-form re-solve is conceptually simpler (no hat algebra; no
need to derive ratio-form updates), and it does not require the
fundamentals to be inverted exactly — small residuals from Step 4
show up as small baseline-vs-data gaps rather than as accumulated
errors in the hats. The exact-hat path remains available under
`Replication_apply/python_commuting/` when needed.

**TODOs**:

- Add a CF4 hook for cross-city counterfactuals once the cross-city
  subsection lands (meeting note: jobs–housing separation, mode
  share, road-network structure).
- Add a baseline-vs-CF1 comparison plot to quantify the welfare
  contribution of observed directionality.

---

## Run order summary

```
build_primitives ──► parameters ──► invert_fundamentals ──► equilibrium_solve (baseline)
                                                                      │
                                          lambda_diagnostic ◄─────────┘
                                                                      │
                            counterfactual_levels.run_cf1_symmetric ◄─┤
                            counterfactual_levels.run_cf2_tidal    ◄──┤
                            counterfactual_levels.run_cf3_uniform  ◄──┘
```

`run_full_estimation.py` is the canonical driver and writes all
outputs to `data_work/outputs/full_estimation_square_v1/`.
