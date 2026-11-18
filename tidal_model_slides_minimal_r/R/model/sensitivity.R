source(file.path("R", "model", "diagnostics.R"))
source(file.path("R", "model", "solver_presets.R"))

sensitivity_spec_label <- function(alpha, beta) {
  sprintf("alpha_%+.2f_beta_%+.2f", alpha, beta)
}

run_sensitivity_spec <- function(prim,
                                 params,
                                 label,
                                 top_k,
                                 max_outer,
                                 out_dir,
                                 pair_selection = "score_bpr_st") {
  require_pkg("data.table")
  solver_kw <- solver_overrides_for_params(params, max_outer = max_outer)
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  kw <- build_congestion_solver_kwargs(
    prim = prim,
    fund = fund,
    params = params,
    congestion_form = "anchored",
    congestion_lambda = 1.1,
    max_outer = solver_kw$max_outer,
    tol = solver_kw$tol,
    damping = solver_kw$damping
  )
  base <- solve_baseline_endogenous(prim, fund, params, kw)
  st <- run_tidal_endogenous(
    prim, fund, params, base, kw,
    top_k = top_k,
    delta = 1.0,
    horizon = "short_run",
    pair_selection = pair_selection
  )
  lt <- run_tidal_endogenous(
    prim, fund, params, base, kw,
    top_k = top_k,
    delta = 1.0,
    horizon = "long_run",
    pair_selection = pair_selection
  )

  spec_dir <- file.path(out_dir, label)
  dir.create(spec_dir, recursive = TRUE, showWarnings = FALSE)
  write_equilibrium(base, file.path(spec_dir, "baseline"))
  write_equilibrium(lt$eq, file.path(spec_dir, "tidal_lt"))
  write_json(
    list(
      label = label,
      alpha = params$alpha,
      beta = params$beta,
      chi = fund$chi,
      kappa = fund$kappa,
      n_iter = fund$n_iter,
      fund_converged = fund$converged,
      resid_R_max_abs = max(abs(fund$resid_R)),
      resid_F_max_abs = max(abs(fund$resid_F)),
      solver = solver_kw
    ),
    file.path(spec_dir, "fund_inversion_meta.json")
  )
  write_json(
    list(
      baseline = list(converged = base$converged, n_outer = base$n_outer, max_log_change = base$max_log_change),
      tidal_st = list(converged = st$eq$converged, n_outer = st$eq$n_outer, max_log_change = st$eq$max_log_change),
      tidal_lt = list(converged = lt$eq$converged, n_outer = lt$eq$n_outer, max_log_change = lt$eq$max_log_change)
    ),
    file.path(spec_dir, "convergence_meta.json")
  )

  decomp <- od_cost_decomposition(base, st$eq, lt$eq, prim, params, fund$abar_theta)
  sort <- sorting_stats_alpha(base, lt$eq)
  corr <- corridor_pct_alpha(base, lt$eq, lt$selection)
  row <- c(
    list(
      spec_label = label,
      alpha = params$alpha,
      beta = params$beta,
      solver_max_outer = solver_kw$max_outer,
      solver_damping = solver_kw$damping,
      solver_tol = solver_kw$tol,
      baseline_converged = base$converged,
      tidal_st_converged = st$eq$converged,
      tidal_lt_converged = lt$eq$converged,
      baseline_max_log_change = base$max_log_change,
      tidal_lt_max_log_change = lt$eq$max_log_change,
      top_k = top_k,
      n_pairs = nrow(lt$selection),
      baseline_Wbar = base$Wbar,
      baseline_median_tau_ratio_vs_obs = stats::median(base$tau_min_kl / pmax(prim$tau_obs_min, EPS)),
      Wbar_pct_change_st = 100.0 * (st$eq$Wbar / base$Wbar - 1.0),
      Wbar_pct_change_lt = 100.0 * (lt$eq$Wbar / base$Wbar - 1.0)
    ),
    corr,
    decomp,
    sort,
    list(
      person_min_day_lt_total = decomp$total_lt_cost_change_sec / 60.0 * prim$Lbar,
      person_min_day_sorting_wedge = decomp$sorting_wedge_sec / 60.0 * prim$Lbar,
      one_minus_alpha_theta = 1.0 - params$alpha * params$theta,
      one_minus_beta_theta = 1.0 - params$beta * params$theta,
      pair_selection = pair_selection
    )
  )
  row$all_converged <- row$baseline_converged && row$tidal_st_converged && row$tidal_lt_converged
  row
}
