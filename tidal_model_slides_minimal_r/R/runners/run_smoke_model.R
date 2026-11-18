source(file.path("R", "model", "core.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "smoke_model",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    max_outer = 500,
    tol = 1e-4
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("--repo-root", "--output-name", "--version-id", "--max-outer", "--tol")) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      val <- args[[i + 1L]]
      nm <- sub("^--", "", key)
      nm <- gsub("-", "_", nm)
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
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  params <- params_default(alpha = 0.05, beta = -0.10)
  solver_max_outer <- if (params$alpha > 0) max(args$max_outer, 500L) else args$max_outer
  solver_damping <- if (params$alpha > 0) 0.10 else 0.25
  solver_tol <- if (params$alpha > 0) 1e-4 else args$tol
  prim <- load_primitives(root = root, version_id = args$version_id)
  write_primitives_summary(prim, file.path(out_dir, "primitives"))

  message("Inverting fundamentals")
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  write_fundamentals(fund, file.path(out_dir, "fundamentals"))

  message("Building anchored-congestion observation anchors")
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

  message("Solving exogenous baseline")
  base_exog <- do.call_solve_equilibrium(
    prim, fund, params,
    label = "baseline_exogenous_tau",
    endogenous = FALSE,
    kw = kw
  )
  write_equilibrium(base_exog, file.path(out_dir, "baseline_exogenous_tau"))

  message("Solving anchored endogenous baseline")
  base_endog <- do.call_solve_equilibrium(
    prim, fund, params,
    label = "baseline_endogenous",
    endogenous = TRUE,
    kw = kw
  )
  write_equilibrium(base_endog, file.path(out_dir, "baseline_endogenous"))

  summary <- list(
    version_id = args$version_id,
    engine = "R",
    input = "package-local parquet",
    parameters = params,
    baseline_exogenous = list(
      Wbar = base_exog$Wbar,
      converged = base_exog$converged,
      n_outer = base_exog$n_outer
    ),
    baseline_endogenous = list(
      Wbar = base_endog$Wbar,
      converged = base_endog$converged,
      n_outer = base_endog$n_outer,
      max_log_change = base_endog$max_log_change,
      mean_tau_vs_obs_pct = 100 * (mean(base_endog$tau_min_kl) / mean(prim$tau_obs_min) - 1),
      median_tau_ratio_vs_obs = stats::median(base_endog$tau_min_kl / prim$tau_obs_min)
    ),
    fundamentals = list(
      chi = fund$chi,
      kappa = fund$kappa,
      n_iter = fund$n_iter,
      converged = fund$converged,
      max_log_change = fund$max_log_change
    )
  )
  write_json(summary, file.path(out_dir, "baseline_summary.json"))
  message(sprintf(
    "smoke model ok: Wbar=%.6f, median_tau_obs_ratio=%.4f, converged=%s",
    base_endog$Wbar,
    summary$baseline_endogenous$median_tau_ratio_vs_obs,
    base_endog$converged
  ))
}

main()
