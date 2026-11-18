source(file.path("R", "model", "counterfactuals.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_stage2_full",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    quick = FALSE,
    skip_tidal_grid = FALSE,
    max_outer = 500,
    tol = 1e-4
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("--quick", "--skip-tidal-grid")) {
      out[[gsub("-", "_", sub("^--", "", key))]] <- TRUE
      i <- i + 1L
    } else if (key %in% c("--repo-root", "--output-name", "--version-id", "--max-outer", "--tol")) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      val <- args[[i + 1L]]
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- val
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$max_outer <- as.integer(out$max_outer)
  out$tol <- as.numeric(out$tol)
  out
}

main <- function() {
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  out_dir <- file.path(root, "outputs", args$output_name)
  cf_dir <- file.path(out_dir, "counterfactuals")
  dir.create(cf_dir, recursive = TRUE, showWarnings = FALSE)

  params <- params_default(alpha = 0.05, beta = -0.10)
  solver_max_outer <- if (params$alpha > 0) max(args$max_outer, 500L) else args$max_outer
  solver_damping <- if (params$alpha > 0) 0.10 else 0.25
  solver_tol <- if (params$alpha > 0) 1e-4 else args$tol
  prim <- load_primitives(root = root, version_id = args$version_id)
  write_primitives_summary(prim, file.path(out_dir, "primitives"))
  write_json(params, file.path(out_dir, "parameters.json"))

  message("Inverting fundamentals")
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  write_fundamentals(fund, file.path(out_dir, "fundamentals"))

  message("Building solver kwargs")
  kw <- build_congestion_solver_kwargs(
    prim = prim,
    fund = fund,
    params = params,
    congestion_form = "anchored",
    congestion_lambda = 1.1,
    max_outer = solver_max_outer,
    tol = solver_tol,
    damping = solver_damping
  )

  message("Solving baselines")
  baseline_exog <- solve_baseline_exogenous(prim, fund, params, kw)
  write_equilibrium(baseline_exog, file.path(out_dir, "baseline_exogenous_tau"))
  baseline_endog <- solve_baseline_endogenous(prim, fund, params, kw)
  write_equilibrium(baseline_endog, file.path(out_dir, "baseline_endogenous"))

  baseline_summary <- list(
    version_id = args$version_id,
    engine = "R",
    congestion_form = "anchored",
    congestion_lambda = 1.1,
    pair_selection = "score_bpr_st",
    baseline_exogenous = list(Wbar = baseline_exog$Wbar, converged = baseline_exog$converged),
    baseline_endogenous = list(
      Wbar = baseline_endog$Wbar,
      converged = baseline_endog$converged,
      n_outer = baseline_endog$n_outer,
      max_log_change = baseline_endog$max_log_change,
      mean_tau_vs_obs_pct = 100 * (mean(baseline_endog$tau_min_kl) / mean(prim$tau_obs_min) - 1),
      median_tau_ratio_vs_obs = stats::median(baseline_endog$tau_min_kl / prim$tau_obs_min)
    )
  )
  write_json(baseline_summary, file.path(out_dir, "baseline_summary.json"))

  message("Running policy CFs, exogenous and endogenous")
  policy_all <- run_all_policy_counterfactuals(
    prim, fund, params,
    baseline_exog = baseline_exog,
    baseline_endog = baseline_endog,
    kw = kw
  )
  data.table::fwrite(policy_all, file.path(cf_dir, "policy_cf_all_modes.csv"))

  cf_top_k <- if (args$quick) 25L else 50L
  cf3 <- if (args$quick) c(1.10) else c(1.10, 1.25, 1.50)

  message("Running structural endogenous CFs")
  structural <- run_structural_counterfactuals(
    prim, fund, params, baseline_endog, kw,
    cf2_top_k = cf_top_k,
    cf3_factors = cf3,
    sym_method = "geometric",
    pair_selection = "score_bpr_st"
  )
  if (!args$quick) {
    arith <- run_structural_counterfactuals(
      prim, fund, params, baseline_endog, kw,
      cf2_top_k = 0,
      cf3_factors = c(),
      sym_method = "arithmetic",
      pair_selection = "score_bpr_st"
    )
    structural <- data.table::rbindlist(list(structural, arith[grepl("^CF1", cf)]), fill = TRUE)
  }
  data.table::fwrite(structural, file.path(cf_dir, "structural_cf_endogenous.csv"))

  message("Running structural exogenous CFs")
  exog <- run_structural_exogenous(
    prim, fund, params, baseline_exog, kw,
    cf2_top_k = cf_top_k,
    cf3_factors = if (args$quick) c(1.10) else cf3,
    sym_method = "geometric",
    pair_selection = "score_bpr_st"
  )
  data.table::fwrite(exog, file.path(cf_dir, "structural_cf_exogenous.csv"))

  message("Running endogenous tidal summary")
  tidal_specs <- if (args$quick) list(c(cf_top_k, 1.0)) else list(c(cf_top_k, 1.0), c(50, 0.5), c(100, 1.0))
  tidal_rows <- list()
  for (sp in tidal_specs) {
    top_k <- as.integer(sp[1])
    delta <- as.numeric(sp[2])
    for (horizon in c("short_run", "long_run")) {
      res <- run_tidal_endogenous(
        prim, fund, params, baseline_endog, kw,
        top_k = top_k,
        delta = delta,
        horizon = horizon,
        pair_selection = "score_bpr_st"
      )
      row <- summarise_vs_baseline(baseline_endog, res$eq, prim, params, fund$abar_theta)
      row$cf <- "tidal_lane_endogenous"
      row$top_k <- top_k
      row$delta_lanes <- delta
      row$horizon <- horizon
      row$n_pairs <- nrow(res$selection)
      row$pair_selection <- "score_bpr_st"
      row$congestion_form <- "anchored"
      row$congestion_lambda <- 1.1
      tidal_rows[[length(tidal_rows) + 1L]] <- row
    }
  }
  data.table::fwrite(
    data.table::rbindlist(lapply(tidal_rows, data.table::as.data.table), fill = TRUE),
    file.path(cf_dir, "tidal_lane_endogenous_summary.csv")
  )

  if (!args$skip_tidal_grid) {
    message("Running tidal exogenous BPR grid")
    texog <- run_tidal_exogenous_bpr_grid(prim, fund, params, baseline_exog, kw, quick = args$quick)
    data.table::fwrite(texog$rows, file.path(cf_dir, "tidal_lane_exogenous_bpr_grid.csv"))
    if (nrow(texog$details)) {
      data.table::fwrite(texog$details, file.path(cf_dir, "tidal_lane_exogenous_edge_detail.csv"))
    }
  }

  message(sprintf(
    "stage2 R ok: Wbar=%.6f, median_tau_obs_ratio=%.4f",
    baseline_endog$Wbar,
    baseline_summary$baseline_endogenous$median_tau_ratio_vs_obs
  ))
}

main()
