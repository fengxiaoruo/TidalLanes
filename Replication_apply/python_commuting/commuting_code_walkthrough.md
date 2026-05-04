# Seattle Commuting Code Walkthrough

This note summarizes the Seattle commuting-mode code path in `AA_ReplicationFinal`, with the emphasis on practical replication order and direct correspondence to the paper.

## 1. Overall Framework

For the Seattle commuting application, the goal is not to run a standard traffic assignment model. The code implements a four-step workflow:

1. Prepare node-level and edge-level network data.
2. Use observed traffic to validate whether the model can recover commuting OD flows.
3. Set parameters and solve the baseline equilibrium.
4. Run a 1% infrastructure improvement counterfactual on each directed edge.

The original code path is:

- Data preparation: `analysis/seattle/*.do`
- OD validation: `analysis/seattle/predict_commuting.m`
- Baseline equilibrium core:
  - `analysis/figure1/fn_AA_calc_eqm_commuting_static.m`
  - `analysis/figure1/fn_AA_eqm_lr_lf.m`
- Counterfactual core:
  - `counterfactuals/seattle/fn_AA_calc_eqm_commuting_counterfactual.m`
  - `counterfactuals/seattle/fn_AA_eqm_lr_lf_counterfactual.m`
- Edge-by-edge batch run:
  - `counterfactuals/seattle/asym_counterfactual_berlin.m`

The current Python mapping is:

- `python_commuting/data_io.py`
- `python_commuting/predict_commuting.py`
- `python_commuting/static_eqm.py`
- `python_commuting/counterfactual.py`
- `python_commuting/run_all_counterfactual_edges.py`

## 2. Data Preparation

At the minimum, the Seattle commuting workflow only needs three prepared inputs:

- `counterfactuals/seattle/sparse_adjmat_seattle.csv`
- `counterfactuals/seattle/node_lr_lf_seattle.csv`
- `counterfactuals/seattle/sparse_commute_seattle.csv`

These are produced from the Stata preparation pipeline. Their practical meanings are:

- `sparse_adjmat_seattle.csv`
  - edge-level observed traffic input
  - exported in `analysis/seattle/generate_sparse.do`
- `node_lr_lf_seattle.csv`
  - node-level residence and workplace mass
  - exported in `analysis/seattle/generate_sparse.do`
- `sparse_commute_seattle.csv`
  - observed commuting OD matrix
  - exported in `analysis/seattle/generate_sparse.do`

In the full original project, the preparation order is:

1. `gridcity_popinc.do`
   - builds grid-level population and workplace measures
2. `gridcity_LODES.do`
   - builds commuting OD flows from LODES
3. `gridcity_adjmat.do`
   - builds adjacency and edge-level network objects
4. `generate_sparse.do`
   - exports Matlab-ready sparse inputs

So the Seattle commuting model does not start from raw GIS files directly. It starts from these already-networked sparse CSV objects.

## 3. Estimation and Validation Come Before Counterfactuals

The empirical order matters. The original code does not jump straight into counterfactuals.

The first validation layer is whether the model can recover commuting OD flows from observed traffic.

The entry file is:

- `analysis/seattle/predict_commuting.m`

The Python counterpart is:

- `python_commuting/predict_commuting.py`

This step does the following:

1. Load observed link traffic `Xi_ij`.
2. Load node masses `l_R` and `l_F`.
3. Apply the Matlab population-consistency normalization.
4. Construct
   - `B = inv(diag(avg_pop + avg_flow) - Xi_ij)`
5. Recover predicted commuting flows:
   - `Lij_pred = B .* l_R .* l_F'`

Then `analysis/results.do` compares the predicted commuting matrix against observed `grid_commuters` to produce the Seattle Figure 2 validation scatter.

Economically, this is a crucial check: the model should not merely solve its own equations, but should also invert observed network traffic into a plausible commuting OD matrix.

## 4. Parameter Choice

The baseline Seattle counterfactual parameterization is set in:

- `counterfactuals/seattle/asym_counterfactual_berlin.m`

The main parameters are:

- `theta = 6.83`
- `delta1 = 0.488`
- `lambda = (1/theta) * delta1`
- `alpha = -0.12`
- `beta = -0.1`

Interpretation:

- `theta`
  - commuting elasticity
- `delta1`
  - congestion slope estimated from the traffic-time regression
- `lambda`
  - congestion parameter entering the equilibrium system
- `alpha`
  - productivity/workplace externality
- `beta`
  - residential/amenity externality

So the empirical logic is not “estimate everything jointly in one black box.” Instead:

1. Take or calibrate `theta`.
2. Estimate `delta1` from the congestion regression.
3. Combine them into `lambda`.

## 5. Baseline Equilibrium Solver

The baseline equilibrium core is:

- `analysis/figure1/fn_AA_eqm_lr_lf.m`

The practical solution order is:

1. Map `(l_R, l_F)` into transformed variables `(x1, x2)`.
2. Inner loop:
   - for a fixed `chi`, iterate on `(x1, x2)` until the fixed point converges.
3. Outer loop:
   - adjust `chi` until the scale condition is satisfied.

The transformed-variable system is helpful because it makes the nonlinear equilibrium equations easier to iterate on while preserving the adding-up constraints.

The key scalar is `chi`. In both the trade and commuting versions of the model, it plays the role of the equilibrium scale or welfare-related closure term.

After convergence, the code recovers:

- `tau`
  - bilateral effective transport cost between all node pairs
- `Lij`
  - commuting OD flows
- `Xi_ij`
  - edge-level traffic

The Python baseline counterpart is:

- `python_commuting/static_eqm.py`

## 6. Exact-Hat Counterfactual Logic

The counterfactual solver is:

- `counterfactuals/seattle/fn_AA_eqm_lr_lf_counterfactual.m`

This is not a re-estimation step. It takes the baseline equilibrium as given and solves for hat variables.

The practical order is:

1. Hold baseline `l_R`, `l_F`, `L_bar`, and `Xi_ij`.
2. Feed in a counterfactual set of hats:
   - `t_bar_hat`
   - `T_bar_hat`
   - `u_bar_hat`
   - `L_bar_hat`
3. Run the inner iteration on `x1hat`, `x2hat`.
4. Search over `chihat`.
5. Recover:
   - `l_Rhat`
   - `l_Fhat`
   - `t_hat`
   - `Xi_ij_hat`

In the Seattle application, almost all counterfactuals are simple edge improvements:

- one directed edge gets `t_bar_hat = 0.99`
- everything else stays at 1:
  - `T_bar_hat = 1`
  - `u_bar_hat = 1`
  - `L_bar_hat = 1`

So this is a clean “reduce one edge’s iceberg cost by 1%” experiment.

The Python counterpart is:

- `python_commuting/counterfactual.py`

## 7. Why the Batch Driver Looks the Way It Does

The batch driver is:

- `counterfactuals/seattle/asym_counterfactual_berlin.m`

Its logic is simple:

1. Read the list of directed edges from `sparse_adjmat_seattle.csv`.
2. For each edge, construct a fresh `t_bar_hat_iter`.
3. Set only that edge to `0.99`.
4. Call the counterfactual solver once.
5. Save the full set of node-level hats for that edge.

The Python version follows the same design:

- `python_commuting/run_all_counterfactual_edges.py`

The current Python driver is sequential first, because that keeps the code path closest to the original Matlab logic and is easier to explain line by line.

## 8. The Main Replication Lines to Track

If the goal is to understand and replicate the Seattle commuting code in practice, the most useful way to read the code is to track four lines:

- Data line
  - grid population and employment plus LODES plus HERE traffic
  - then exported into sparse CSVs
- Validation line
  - `Xi_ij -> predicted Lij -> compare with observed commuting`
- Equilibrium line
  - `t_bar, T_bar, u_bar, L_bar, theta, lambda, alpha, beta -> chi, l_R, l_F, Xi_ij`
- Counterfactual line
  - one-edge 0.99 hat
  - then `chi_hat, l_Rhat, l_Fhat, Xi_ij_hat`

## 9. Recommended Function-by-Function Reading Order

For detailed study, the most practical order is:

1. `analysis/seattle/generate_sparse.do`
2. `analysis/seattle/predict_commuting.m`
3. `analysis/figure1/fn_AA_calc_eqm_commuting_static.m`
4. `analysis/figure1/fn_AA_eqm_lr_lf.m`
5. `counterfactuals/seattle/fn_AA_calc_eqm_commuting_counterfactual.m`
6. `counterfactuals/seattle/fn_AA_eqm_lr_lf_counterfactual.m`
7. `counterfactuals/seattle/asym_counterfactual_berlin.m`

This reading order matches the empirical workflow, the computational workflow, and the paper’s logic much better than starting from the batch counterfactual script.
