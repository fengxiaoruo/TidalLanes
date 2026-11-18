source(file.path("R", "model", "diagnostics.R"))
source(file.path("R", "model", "solver_presets.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_tidal_diagnostics",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    alpha = 0.05,
    beta = -0.10,
    congestion_lambda = 1.1,
    pair_selection = "score_bpr_st",
    max_outer = 500L,
    only_top100 = FALSE
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key == "--only-top100") {
      out$only_top100 <- TRUE
      i <- i + 1L
    } else if (key %in% c(
      "--repo-root", "--output-name", "--version-id", "--alpha", "--beta",
      "--congestion-lambda", "--pair-selection", "--max-outer"
    )) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- args[[i + 1L]]
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$alpha <- as.numeric(out$alpha)
  out$beta <- as.numeric(out$beta)
  out$congestion_lambda <- as.numeric(out$congestion_lambda)
  out$max_outer <- as.integer(out$max_outer)
  out
}

main <- function() {
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  out_dir <- file.path(root, "outputs", args$output_name)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  prim <- load_primitives(root = root, version_id = args$version_id)
  params <- params_default(alpha = args$alpha, beta = args$beta)
  write_json(params, file.path(out_dir, "parameters.json"))
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  skw <- solver_overrides_for_params(params, max_outer = args$max_outer)
  kw <- build_congestion_solver_kwargs(
    prim = prim,
    fund = fund,
    params = params,
    congestion_form = "anchored",
    congestion_lambda = args$congestion_lambda,
    max_outer = skw$max_outer,
    tol = skw$tol,
    damping = skw$damping
  )
  write_json(
    list(xi_obs_median = stats::median(kw$xi_obs_kl)),
    file.path(out_dir, "observation_anchors.json")
  )
  baseline <- solve_baseline_endogenous(prim, fund, params, kw)
  write_equilibrium(baseline, file.path(out_dir, "baseline"))

  specs <- if (args$only_top100) list(c(100, 1.0)) else list(c(25, 1.0), c(50, 1.0), c(50, 0.5), c(100, 1.0))
  rows <- list()
  detail_parts <- list()
  for (sp in specs) {
    top_k <- as.integer(sp[1])
    delta <- as.numeric(sp[2])
    label <- sprintf("top%s_d%.1f", top_k, delta)
    message(sprintf("Tidal diagnostics: %s", label))
    st <- run_tidal_endogenous(
      prim, fund, params, baseline, kw,
      top_k = top_k,
      delta = delta,
      horizon = "short_run",
      pair_selection = args$pair_selection
    )
    lt <- run_tidal_endogenous(
      prim, fund, params, baseline, kw,
      top_k = top_k,
      delta = delta,
      horizon = "long_run",
      pair_selection = args$pair_selection
    )
    write_equilibrium(st$eq, file.path(out_dir, label, "short_run"))
    write_equilibrium(lt$eq, file.path(out_dir, label, "long_run"))
    data.table::fwrite(lt$selection, file.path(out_dir, label, "selected_pairs.csv"))
    one <- summarise_tidal_pair(
      spec_label = label,
      top_k = top_k,
      delta_lanes = delta,
      baseline = baseline,
      short_run = st$eq,
      long_run = lt$eq,
      prim = prim,
      params = params,
      abar_theta = fund$abar_theta,
      selection = lt$selection
    )
    row <- one$row
    row$congestion_form <- "anchored"
    row$lambda <- args$congestion_lambda
    row$pair_selection <- args$pair_selection
    rows[[length(rows) + 1L]] <- data.table::as.data.table(row)
    if (nrow(one$detail)) detail_parts[[length(detail_parts) + 1L]] <- one$detail
  }

  df <- data.table::rbindlist(rows, fill = TRUE)
  data.table::fwrite(df, file.path(out_dir, "tidal_st_lt_congestion_summary.csv"))
  if (length(detail_parts)) {
    data.table::fwrite(data.table::rbindlist(detail_parts, fill = TRUE), file.path(out_dir, "tidal_treated_edge_detail.csv"))
  }
  if ("st_treated_mean_delta_tau_min" %in% names(df)) {
    df[, st_treated_mean_delta_tau_sec := 60.0 * st_treated_mean_delta_tau_min]
    df[, lt_treated_mean_delta_tau_sec := 60.0 * lt_treated_mean_delta_tau_min]
  }
  compact_cols <- c(
    "spec", "top_k", "n_pairs",
    "Wbar_pct_change_st", "Wbar_pct_change_lt",
    "avg_physical_time_base_min", "avg_physical_time_st_min", "avg_physical_time_lt_min",
    "physical_time_saved_st_sec_per_commuter", "physical_time_saved_lt_sec_per_commuter",
    "physical_time_saved_st_person_hours", "physical_time_saved_lt_person_hours",
    "direct_st_cost_change_sec", "ge_cost_wedge_sec", "sorting_wedge_sec",
    "total_st_cost_change_sec", "total_lt_cost_change_sec",
    "st_treated_mean_delta_tau_sec", "lt_treated_mean_delta_tau_min",
    "st_treated_xi_weighted_delta_tau_min", "lt_treated_xi_weighted_delta_tau_min"
  )
  compact_cols <- compact_cols[compact_cols %in% names(df)]
  data.table::fwrite(df[, ..compact_cols], file.path(out_dir, "tidal_st_lt_compact.csv"))
  print(df[, .(
    spec, Wbar_pct_change_st, Wbar_pct_change_lt,
    avg_physical_time_base_min,
    physical_time_saved_st_sec_per_commuter, physical_time_saved_lt_sec_per_commuter,
    physical_time_saved_st_person_hours, physical_time_saved_lt_person_hours
  )])
}

main()
