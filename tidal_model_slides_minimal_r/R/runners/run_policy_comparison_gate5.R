source(file.path("R", "model", "counterfactuals.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_policy_comparison",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    horizons = "short_run,long_run"
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("--repo-root", "--output-name", "--version-id", "--horizons", "--stage2-dir")) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      val <- args[[i + 1L]]
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- val
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out
}

main <- function() {
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  out_dir <- file.path(root, "outputs", args$output_name)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  horizons <- strsplit(args$horizons, ",", fixed = TRUE)[[1]]
  horizons <- trimws(horizons[nzchar(trimws(horizons))])

  params <- params_default(alpha = 0.05, beta = -0.10)
  prim <- load_primitives(root = root, version_id = args$version_id)
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  kw <- build_congestion_solver_kwargs(
    prim = prim,
    fund = fund,
    params = params,
    congestion_form = "anchored",
    congestion_lambda = 1.1,
    max_outer = 500,
    tol = 1e-4,
    damping = 0.10
  )
  baseline <- solve_baseline_endogenous(prim, fund, params, kw)

  rows <- list()
  p2_lane_km <- 0.0
  for (horizon in horizons) {
    message(sprintf("Policy horizon: %s", horizon))
    p1 <- run_tidal_endogenous(
      prim, fund, params, baseline, kw,
      top_k = 100,
      delta = 1.0,
      horizon = horizon,
      pair_selection = "score_bpr_st"
    )
    row <- summarise_vs_baseline(baseline, p1$eq, prim, params, fund$abar_theta)
    row$policy_id <- "P1_tidal"
    row$horizon <- horizon
    row$lane_km_added <- 0.0
    row$pair_selection <- "score_bpr_st"
    row$congestion_lambda <- 1.1
    rows[[length(rows) + 1L]] <- row

    p2 <- run_road_expansion_same_corridor(
      prim, fund, params, baseline, kw,
      top_k = 100,
      pair_selection = "score_bpr_st",
      horizon = horizon
    )
    p2_lane_km <- lane_km_added(prim, prim$n_kl, p2$n_cf)
    row <- summarise_vs_baseline(baseline, p2$eq, prim, params, fund$abar_theta)
    row$policy_id <- "P2_expansion_same_corridor"
    row$horizon <- horizon
    row$lane_km_added <- p2_lane_km
    row$congestion_lambda <- 1.1
    rows[[length(rows) + 1L]] <- row

    p3 <- run_road_expansion_qdelay_topk(
      prim, fund, params, baseline, kw,
      top_k = 100,
      horizon = horizon
    )
    p3_lane_km <- lane_km_added(prim, prim$n_kl, p3$n_cf)
    row <- summarise_vs_baseline(baseline, p3$eq, prim, params, fund$abar_theta)
    row$policy_id <- "P3_expansion_qdelay"
    row$horizon <- horizon
    row$lane_km_added <- p3_lane_km
    row$congestion_lambda <- 1.1
    rows[[length(rows) + 1L]] <- row

    fac <- uniform_expansion_factor_for_lane_budget(prim, p2_lane_km)
    p4 <- run_cf3_uniform(
      prim, fund, params, kw,
      factor = fac,
      horizon = horizon,
      baseline = baseline,
      label = "equilibrium"
    )
    row <- summarise_vs_baseline(baseline, p4, prim, params, fund$abar_theta)
    row$policy_id <- "P4_expansion_uniform_matched"
    row$horizon <- horizon
    row$lane_km_added <- p2_lane_km
    row$expansion_factor <- fac
    row$congestion_lambda <- 1.1
    rows[[length(rows) + 1L]] <- row

    p5 <- run_cf3_uniform(
      prim, fund, params, kw,
      factor = 1.10,
      horizon = horizon,
      baseline = baseline,
      label = "equilibrium"
    )
    row <- summarise_vs_baseline(baseline, p5, prim, params, fund$abar_theta)
    row$policy_id <- "P5_uniform_speed_1.10"
    row$horizon <- horizon
    row$lane_km_added <- NA_real_
    row$congestion_lambda <- 1.1
    rows[[length(rows) + 1L]] <- row

    p6 <- run_congestion_pricing_first_best(
      prim, fund, params, baseline, kw,
      horizon = horizon,
      congestion_lambda = 1.1
    )
    row <- summarise_vs_baseline(baseline, p6$eq, prim, params, fund$abar_theta)
    w_real <- realized_welfare_excl_toll(
      prim, fund, params, p6$eq,
      congestion_lambda = 1.1,
      xi_obs_kl = kw$xi_obs_kl,
      n_obs_kl = kw$n_obs_kl,
      tau_obs_anchor = kw$tau_obs_anchor
    )
    row$policy_id <- "P6_first_best_pricing"
    row$horizon <- horizon
    row$Wbar_pct_change_realized <- 100 * (w_real / baseline$Wbar - 1)
    for (nm in names(p6$meta)) row[[nm]] <- p6$meta[[nm]]
    rows[[length(rows) + 1L]] <- row

    p7 <- run_cordon_pricing(
      prim, fund, params, baseline, kw,
      horizon = horizon
    )
    row <- summarise_vs_baseline(baseline, p7$eq, prim, params, fund$abar_theta)
    w_real <- realized_welfare_excl_toll(
      prim, fund, params, p7$eq,
      congestion_lambda = 1.1,
      xi_obs_kl = kw$xi_obs_kl,
      n_obs_kl = kw$n_obs_kl,
      tau_obs_anchor = kw$tau_obs_anchor
    )
    row$policy_id <- "P7_cordon_pricing"
    row$horizon <- horizon
    row$Wbar_pct_change_realized <- 100 * (w_real / baseline$Wbar - 1)
    for (nm in names(p7$meta)) row[[nm]] <- p7$meta[[nm]]
    rows[[length(rows) + 1L]] <- row
  }

  df <- data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE)
  if (!("Wbar_pct_change_realized" %in% names(df))) df[, Wbar_pct_change_realized := NA_real_]
  df[, Wbar_pct_change_perceived := Wbar_pct_change]
  df[is.na(Wbar_pct_change_realized), Wbar_pct_change_realized := Wbar_pct_change]
  data.table::fwrite(df, file.path(out_dir, "results_by_policy.csv"))

  lt <- df[horizon == "long_run"]
  if (!nrow(lt)) lt <- df
  ce <- lt[, .(
    Wbar_pct_change_lt = mean(Wbar_pct_change_realized),
    Wbar_pct_change_lt_perceived = mean(Wbar_pct_change_perceived),
    lane_km_added = lane_km_added[1]
  ), by = policy_id]
  ce[, Wbar_per_lane_km := Wbar_pct_change_lt / ifelse(lane_km_added == 0, NA_real_, lane_km_added)]
  data.table::fwrite(ce, file.path(out_dir, "cost_effectiveness.csv"))

  manifest <- list(
    version_id = args$version_id,
    pair_selection = "score_bpr_st",
    alpha = params$alpha,
    beta = params$beta,
    congestion_lambda = 1.1,
    policies = c("P1", "P2", "P3", "P4", "P5", "P6", "P7")
  )
  write_json(manifest, file.path(out_dir, "policies_manifest.json"))
  print(df[, .(policy_id, horizon, Wbar_pct_change_realized, Wbar_pct_change_perceived)])
}

main()
