source(file.path("R", "model", "diagnostics.R"))
source(file.path("R", "model", "solver_presets.R"))

centroid_gap <- function(prim, l_R, l_F) {
  xy <- grid_xy(prim)
  wr <- pmax(l_R, EPS)
  wf <- pmax(l_F, EPS)
  rx <- stats::weighted.mean(xy$x, wr)
  ry <- stats::weighted.mean(xy$y, wr)
  fx <- stats::weighted.mean(xy$x, wf)
  fy <- stats::weighted.mean(xy$y, wf)
  sqrt((rx - fx)^2 + (ry - fy)^2)
}

sorting_stats_symmetry <- function(prim, base, scen, selection = NULL) {
  lr0 <- pmax(base$l_R, EPS)
  lf0 <- pmax(base$l_F, EPS)
  lr1 <- pmax(scen$l_R, EPS)
  lf1 <- pmax(scen$l_F, EPS)
  dlr <- log(lr1) - log(lr0)
  dlf <- log(lf1) - log(lf0)
  hub <- employment_hub_mask(prim)
  outer <- outer_ring_mask(prim)
  out <- list(
    centroid_gap_base = centroid_gap(prim, base$l_R, base$l_F),
    centroid_gap_scenario = centroid_gap(prim, scen$l_R, scen$l_F),
    centroid_gap_change = centroid_gap(prim, scen$l_R, scen$l_F) - centroid_gap(prim, base$l_R, base$l_F),
    mean_log_lR_change_hub = if (any(hub)) mean(dlr[hub]) else NA_real_,
    mean_log_lF_change_hub = if (any(hub)) mean(dlf[hub]) else NA_real_,
    mean_log_lR_change_outer = if (any(outer)) mean(dlr[outer]) else NA_real_,
    mean_log_lF_change_outer = if (any(outer)) mean(dlf[outer]) else NA_real_,
    max_abs_log_lR_change = max(abs(dlr)),
    max_abs_log_lF_change = max(abs(dlf))
  )
  if (!is.null(selection) && nrow(selection)) {
    nodes <- sort(unique(c(prim$edge_i[selection$slow], prim$edge_j[selection$slow])))
    out$n_slow_endpoint_nodes <- length(nodes)
    out$mean_log_lR_change_slow_endpoints <- if (length(nodes)) mean(dlr[nodes]) else NA_real_
    out$mean_log_lF_change_slow_endpoints <- if (length(nodes)) mean(dlf[nodes]) else NA_real_
    out$sum_delta_lR_slow_endpoints <- if (length(nodes)) sum(scen$l_R[nodes] - base$l_R[nodes]) else NA_real_
    out$sum_delta_lF_slow_endpoints <- if (length(nodes)) sum(scen$l_F[nodes] - base$l_F[nodes]) else NA_real_
  }
  out
}

symmetry_compare_row <- function(prim,
                                 params,
                                 base,
                                 scen,
                                 world,
                                 scenario,
                                 family,
                                 abar_base,
                                 abar_scen = NULL,
                                 selection = NULL) {
  row <- summarise_vs_baseline(
    baseline = base,
    scenario = scen,
    prim = prim,
    params = params,
    abar_theta = abar_base,
    abar_theta_scenario = if (is.null(abar_scen)) abar_base else abar_scen
  )
  row$world <- world$name
  row$mode <- world$mode
  row$scenario <- scenario
  row$family <- family
  c(row, sorting_stats_symmetry(prim, base, scen, selection = selection))
}

solve_world <- function(prim,
                        params,
                        world,
                        ubar,
                        abar,
                        n_kl = NULL,
                        endogenous = NULL,
                        label = "world_scenario") {
  args <- c(
    list(
      prim = prim,
      ubar_theta = ubar,
      abar_theta = abar,
      params = params,
      n_kl_override = n_kl,
      tau_ff_override = world$tau_ff,
      tau_min_init = world$tau_obs_anchor,
      endogenous_congestion = if (is.null(endogenous)) world$mode == "endogenous" else isTRUE(endogenous),
      label = label
    ),
    world$kwargs
  )
  do.call(solve_equilibrium, args)
}

make_symmetry_worlds <- function(prim,
                                 fund,
                                 params,
                                 sym_method = "geometric",
                                 congestion_lambda = 1.1,
                                 solver_kw = solver_overrides_for_params(params)) {
  tau_obs_dir <- prim$tau_obs_min
  tau_ff_dir <- prim$tau_ff_min
  tau_obs_sym <- symmetric_edge_values(prim, prim$tau_obs_min, method = sym_method)
  tau_ff_sym <- symmetric_edge_values(prim, prim$tau_ff_min, method = sym_method)

  base_kwargs <- function(tau_obs_anchor, tau_ff) {
    anchor <- solve_equilibrium(
      prim = prim,
      ubar_theta = fund$ubar_theta,
      abar_theta = fund$abar_theta,
      params = params,
      tau_ff_override = tau_ff,
      tau_min_init = tau_obs_anchor,
      endogenous_congestion = FALSE,
      chi = fund$chi,
      kappa = fund$kappa,
      label = "world_anchor"
    )
    list(
      chi = fund$chi,
      kappa = fund$kappa,
      max_outer = solver_kw$max_outer,
      tol = solver_kw$tol,
      damping = solver_kw$damping,
      congestion_form = "anchored",
      congestion_lambda = congestion_lambda,
      xi_obs_kl = anchor$Xi_kl,
      n_obs_kl = resolve_lanes(prim$n_kl),
      tau_obs_anchor = tau_obs_anchor
    )
  }

  worlds <- list()
  for (item in list(
    list(name = "directed", tau_obs = tau_obs_dir, tau_ff = tau_ff_dir),
    list(name = "symmetric", tau_obs = tau_obs_sym, tau_ff = tau_ff_sym)
  )) {
    kw <- base_kwargs(item$tau_obs, item$tau_ff)
    exog <- solve_world(
      prim, params,
      world = list(name = item$name, mode = "exogenous_tau", tau_obs_anchor = item$tau_obs, tau_ff = item$tau_ff, kwargs = kw),
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      endogenous = FALSE,
      label = sprintf("%s_exog_baseline", item$name)
    )
    endog <- solve_world(
      prim, params,
      world = list(name = item$name, mode = "endogenous", tau_obs_anchor = item$tau_obs, tau_ff = item$tau_ff, kwargs = kw),
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      endogenous = TRUE,
      label = sprintf("%s_endog_baseline", item$name)
    )
    worlds[[paste(item$name, "exogenous_tau", sep = "_")]] <- list(
      name = item$name,
      mode = "exogenous_tau",
      tau_obs_anchor = item$tau_obs,
      tau_ff = item$tau_ff,
      kwargs = kw,
      baseline = exog
    )
    worlds[[paste(item$name, "endogenous", sep = "_")]] <- list(
      name = item$name,
      mode = "endogenous",
      tau_obs_anchor = item$tau_obs,
      tau_ff = item$tau_ff,
      kwargs = kw,
      baseline = endog
    )
  }
  worlds
}

run_symmetry_suite <- function(root,
                               version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
                               output_name = "validation_r_symmetry_directionality",
                               sym_method = "geometric",
                               top_k = 100L) {
  require_pkg("data.table")
  out_dir <- file.path(root, "outputs", output_name)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  prim <- load_primitives(root = root, version_id = version_id)
  params <- params_default(alpha = 0.05, beta = -0.10)
  fund <- invert_fundamentals(prim, params, tol = 1.5e-4, max_iter = 1800, kappa = 1.0)
  solver_kw <- solver_overrides_for_params(params)
  worlds <- make_symmetry_worlds(
    prim, fund, params,
    sym_method = sym_method,
    congestion_lambda = 1.1,
    solver_kw = solver_kw
  )

  rows <- list()
  for (mode in c("exogenous_tau", "endogenous")) {
    directed <- worlds[[paste("directed", mode, sep = "_")]]
    symmetric <- worlds[[paste("symmetric", mode, sep = "_")]]
    rows[[length(rows) + 1L]] <- symmetry_compare_row(
      prim, params,
      base = directed$baseline,
      scen = symmetric$baseline,
      world = symmetric,
      scenario = "symmetric_cost_surface",
      family = "cost_surface",
      abar_base = fund$abar_theta
    )
  }

  place_specs <- list(
    policy_spec("hub_productivity_10pct", abar_mult_on_hubs = 1.10),
    policy_spec("outer_ring_amenity_10pct", ubar_mult_on_outer_ring = 1.10)
  )
  for (world in worlds) {
    for (spec in place_specs) {
      ubar <- apply_policy_ubar(fund$ubar_theta, prim, spec)
      abar <- apply_policy_abar(fund$abar_theta, prim, spec)
      eq <- solve_world(
        prim, params, world,
        ubar = ubar,
        abar = abar,
        label = sprintf("%s_%s_%s", world$name, world$mode, spec$name)
      )
      rows[[length(rows) + 1L]] <- symmetry_compare_row(
        prim, params,
        base = world$baseline,
        scen = eq,
        world = world,
        scenario = spec$name,
        family = "place_shock",
        abar_base = fund$abar_theta,
        abar_scen = abar
      )
    }
  }

  for (key in c("directed_endogenous", "symmetric_endogenous")) {
    world <- worlds[[key]]
    tidal_sel <- select_tidal_lane_pairs_by_rank(
      prim, top_k = top_k, delta = 1.0, rank = "score_bpr_st", xi_kl = world$baseline$Xi_kl
    )
    n_tidal <- apply_tidal_lanes(prim, tidal_sel, delta = 1.0)
    eq <- solve_world(
      prim, params, world,
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      n_kl = n_tidal,
      label = sprintf("%s_tidal_top%s", world$name, top_k)
    )
    rows[[length(rows) + 1L]] <- symmetry_compare_row(
      prim, params,
      base = world$baseline,
      scen = eq,
      world = world,
      scenario = sprintf("tidal_top%s", top_k),
      family = "transport_policy",
      abar_base = fund$abar_theta,
      selection = tidal_sel
    )

    n_same <- apply_bidirectional_lane_delta(prim, tidal_sel, delta = 1.0)
    p2_lane_km <- lane_km_added(prim, prim$n_kl, n_same)
    eq <- solve_world(
      prim, params, world,
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      n_kl = n_same,
      label = sprintf("%s_same_corridor_expansion_top%s", world$name, top_k)
    )
    row <- symmetry_compare_row(
      prim, params,
      base = world$baseline,
      scen = eq,
      world = world,
      scenario = sprintf("same_corridor_expansion_top%s", top_k),
      family = "transport_policy",
      abar_base = fund$abar_theta,
      selection = tidal_sel
    )
    row$lane_km_added <- p2_lane_km
    rows[[length(rows) + 1L]] <- row

    qdelay_sel <- select_tidal_lane_pairs_by_qdelay(prim, top_k = top_k, delta = 1.0, xi_kl = world$baseline$Xi_kl)
    n_qdelay <- apply_bidirectional_lane_delta(prim, qdelay_sel, delta = 1.0)
    eq <- solve_world(
      prim, params, world,
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      n_kl = n_qdelay,
      label = sprintf("%s_qdelay_expansion_top%s", world$name, top_k)
    )
    row <- symmetry_compare_row(
      prim, params,
      base = world$baseline,
      scen = eq,
      world = world,
      scenario = sprintf("qdelay_expansion_top%s", top_k),
      family = "transport_policy",
      abar_base = fund$abar_theta,
      selection = qdelay_sel
    )
    row$lane_km_added <- lane_km_added(prim, prim$n_kl, n_qdelay)
    row$overlap_with_tidal_pairs <- length(intersect(
      paste(qdelay_sel$slow, qdelay_sel$fast),
      paste(tidal_sel$slow, tidal_sel$fast)
    ))
    rows[[length(rows) + 1L]] <- row

    factor <- uniform_expansion_factor_for_lane_budget(prim, p2_lane_km)
    eq <- solve_world(
      prim, params, world,
      ubar = fund$ubar_theta,
      abar = fund$abar_theta,
      n_kl = prim$n_kl * factor,
      label = sprintf("%s_uniform_expansion_matched", world$name)
    )
    row <- symmetry_compare_row(
      prim, params,
      base = world$baseline,
      scen = eq,
      world = world,
      scenario = "uniform_expansion_matched",
      family = "transport_policy",
      abar_base = fund$abar_theta
    )
    row$lane_km_added <- p2_lane_km
    row$expansion_factor <- factor
    rows[[length(rows) + 1L]] <- row
  }

  df <- data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE)
  data.table::fwrite(df, file.path(out_dir, "summary_by_scenario.csv"))
  write_json(
    list(
      version_id = version_id,
      output_name = output_name,
      sym_method = sym_method,
      top_k = top_k,
      alpha = params$alpha,
      beta = params$beta,
      theta = params$theta,
      congestion_form = "anchored",
      congestion_lambda = 1.1,
      audit = list(
        same_primitives = TRUE,
        same_fundamentals = TRUE,
        same_welfare_formula = "primitive Frechet denominator",
        exogenous_vs_endogenous_difference = "endogenous_congestion flag and anchored tau update only",
        symmetric_world_difference = "reciprocal-pair symmetric tau_obs_anchor and tau_ff"
      )
    ),
    file.path(out_dir, "suite_meta.json")
  )
  df
}
