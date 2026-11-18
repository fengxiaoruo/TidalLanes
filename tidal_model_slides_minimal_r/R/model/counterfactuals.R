source(file.path("R", "model", "core.R"))

build_symmetric_tau_min <- function(prim, method = "geometric") {
  symmetric_edge_values(prim, prim$tau_obs_min, method)
}

symmetric_edge_values <- function(prim, values, method = "geometric") {
  key <- paste(prim$edge_i, prim$edge_j, sep = "_")
  idx_map <- seq_along(key)
  names(idx_map) <- key
  out <- as.numeric(values)
  visited <- rep(FALSE, length(out))
  for (idx in seq_along(out)) {
    if (visited[idx]) next
    rev_key <- paste(prim$edge_j[idx], prim$edge_i[idx], sep = "_")
    if (!(rev_key %in% names(idx_map))) next
    rev <- idx_map[[rev_key]]
    visited[idx] <- TRUE
    visited[rev] <- TRUE
    if (method == "geometric") {
      sym <- sqrt(pmax(out[idx], EPS) * pmax(out[rev], EPS))
    } else if (method == "arithmetic") {
      sym <- 0.5 * (out[idx] + out[rev])
    } else {
      stop(sprintf("unknown symmetrisation method: %s", method), call. = FALSE)
    }
    out[idx] <- sym
    out[rev] <- sym
  }
  out
}

eligible_reciprocal_pairs <- function(prim,
                                      min_length_m = 200.0,
                                      delta = 1.0,
                                      min_asym_ratio = 0.0) {
  require_pkg("data.table")
  key <- paste(prim$edge_i, prim$edge_j, sep = "_")
  idx_map <- seq_along(key)
  names(idx_map) <- key
  seen <- rep(FALSE, length(key))
  rows <- vector("list", 0)
  for (idx in seq_along(key)) {
    if (seen[idx]) next
    rev_key <- paste(prim$edge_j[idx], prim$edge_i[idx], sep = "_")
    if (!(rev_key %in% names(idx_map))) next
    rev <- idx_map[[rev_key]]
    seen[idx] <- TRUE
    seen[rev] <- TRUE
    if (prim$tau_obs_min[idx] >= prim$tau_obs_min[rev]) {
      slow <- idx
      fast <- rev
    } else {
      slow <- rev
      fast <- idx
    }
    len_m <- min(prim$ell_kl_km[slow], prim$ell_kl_km[fast]) * 1000
    if (len_m < min_length_m) next
    asym <- prim$tau_obs_min[slow] / max(prim$tau_obs_min[fast], 1e-9)
    if (asym < min_asym_ratio) next
    if (prim$n_kl[fast] - delta < 1.0) next
    rows[[length(rows) + 1L]] <- data.table::data.table(
      slow = slow,
      fast = fast,
      asym_ratio = asym
    )
  }
  if (!length(rows)) return(data.table::data.table(slow = integer(), fast = integer(), asym_ratio = numeric()))
  data.table::rbindlist(rows)
}

bpr_st_pair_score <- function(prim, slow, fast, xi_kl) {
  delay_s <- max(prim$tau_obs_min[slow] - prim$tau_ff_min[slow], 0)
  delay_f <- max(prim$tau_obs_min[fast] - prim$tau_ff_min[fast], 0)
  xi_kl[slow] * delay_s - xi_kl[fast] * delay_f
}

qdelay_pair_score <- function(prim, slow, fast, xi_kl) {
  delay_s <- max(prim$tau_obs_min[slow] - prim$tau_ff_min[slow], 0)
  delay_f <- max(prim$tau_obs_min[fast] - prim$tau_ff_min[fast], 0)
  xi_kl[slow] * delay_s + xi_kl[fast] * delay_f
}

select_tidal_lane_pairs_by_rank <- function(prim,
                                            top_k = 100,
                                            min_length_m = 200.0,
                                            delta = 1.0,
                                            rank = "score_bpr_st",
                                            xi_kl = NULL) {
  require_pkg("data.table")
  rows <- eligible_reciprocal_pairs(prim, min_length_m, delta)
  if (!nrow(rows)) return(rows)
  if (rank == "asym") {
    rows[, score := asym_ratio]
  } else if (rank == "score_bpr_st") {
    if (is.null(xi_kl)) stop("score_bpr_st requires xi_kl", call. = FALSE)
    rows[, score := mapply(function(s, f) bpr_st_pair_score(prim, s, f, xi_kl), slow, fast)]
  } else {
    stop(sprintf("Unsupported rank in R replica: %s", rank), call. = FALSE)
  }
  rows <- rows[order(-score, method = "radix")]
  rows[seq_len(min(top_k, .N))]
}

select_tidal_lane_pairs_by_qdelay <- function(prim,
                                              top_k = 100,
                                              min_length_m = 200.0,
                                              delta = 1.0,
                                              xi_kl) {
  rows <- eligible_reciprocal_pairs(prim, min_length_m = min_length_m, delta = delta)
  if (!nrow(rows)) return(rows)
  rows[, score := mapply(function(s, f) qdelay_pair_score(prim, s, f, xi_kl), slow, fast)]
  rows <- rows[order(-score, method = "radix")]
  rows[seq_len(min(top_k, .N))]
}

apply_tidal_lanes <- function(prim, selection, delta = 1.0) {
  n_cf <- prim$n_kl
  if (nrow(selection)) {
    for (ii in seq_len(nrow(selection))) {
      n_cf[selection$slow[ii]] <- n_cf[selection$slow[ii]] + delta
      n_cf[selection$fast[ii]] <- n_cf[selection$fast[ii]] - delta
    }
  }
  n_cf
}

apply_bidirectional_lane_delta <- function(prim, selection, delta = 1.0) {
  n_cf <- prim$n_kl
  if (nrow(selection)) {
    for (ii in seq_len(nrow(selection))) {
      n_cf[selection$slow[ii]] <- n_cf[selection$slow[ii]] + delta
      n_cf[selection$fast[ii]] <- n_cf[selection$fast[ii]] + delta
    }
  }
  n_cf
}

lane_km_added <- function(prim, n_base, n_cf) {
  sum((n_cf - n_base) * prim$ell_kl_km)
}

uniform_expansion_factor_for_lane_budget <- function(prim, target_lane_km) {
  base <- sum(prim$n_kl * prim$ell_kl_km)
  if (base <= 0) return(1.0)
  1.0 + target_lane_km / base
}

predicted_od_shares <- function(eq, abar_theta, params) {
  dest_factor <- abar_theta * (eq$l_F ^ (params$alpha * params$theta))
  weights <- eq$tau_ij_inv_theta * matrix(dest_factor, nrow = length(dest_factor), ncol = length(dest_factor), byrow = TRUE)
  row_sum <- pmax(rowSums(weights), EPS)
  prob <- weights / row_sum
  matrix(eq$l_R, nrow = length(eq$l_R), ncol = length(eq$l_R)) * prob
}

weighted_bilateral_cost <- function(eq, od_shares, theta) {
  tau_ij_min <- pmax(eq$tau_ij_inv_theta, EPS) ^ (-1.0 / theta)
  sum(od_shares * tau_ij_min) / pmax(sum(od_shares), EPS)
}

comparison_summary <- function(base, scenario, prim, params, abar_theta = NULL, abar_theta_scenario = NULL) {
  valid_R <- base$l_R > EPS & scenario$l_R > EPS
  valid_F <- base$l_F > EPS & scenario$l_F > EPS
  out <- list(
    base_label = base$label,
    scenario_label = scenario$label,
    Wbar_base = base$Wbar,
    Wbar_scenario = scenario$Wbar,
    Wbar_pct_change = 100 * (scenario$Wbar / base$Wbar - 1),
    residence_dissimilarity_pct = 100 * 0.5 * sum(abs(scenario$l_R - base$l_R)),
    workplace_dissimilarity_pct = 100 * 0.5 * sum(abs(scenario$l_F - base$l_F)),
    max_abs_log_residence_change = max(abs(log(scenario$l_R[valid_R]) - log(base$l_R[valid_R]))),
    p95_abs_log_residence_change = as.numeric(stats::quantile(abs(log(scenario$l_R[valid_R]) - log(base$l_R[valid_R])), 0.95, names = FALSE)),
    max_abs_log_workplace_change = max(abs(log(scenario$l_F[valid_F]) - log(base$l_F[valid_F]))),
    p95_abs_log_workplace_change = as.numeric(stats::quantile(abs(log(scenario$l_F[valid_F]) - log(base$l_F[valid_F])), 0.95, names = FALSE)),
    mean_edge_tau_pct_change = 100 * mean(scenario$tau_min_kl / pmax(base$tau_min_kl, EPS) - 1),
    p95_abs_edge_log_tau_change = as.numeric(stats::quantile(abs(log(scenario$tau_min_kl / pmax(base$tau_min_kl, EPS))), 0.95, names = FALSE)),
    base_converged = base$converged,
    scenario_converged = scenario$converged,
    base_iter = as.integer(base$n_outer),
    scenario_iter = as.integer(scenario$n_outer),
    kappa = scenario$kappa
  )
  if (!is.null(abar_theta)) {
    abar_scen <- if (is.null(abar_theta_scenario)) abar_theta else abar_theta_scenario
    od_base <- predicted_od_shares(base, abar_theta, params)
    od_scen <- predicted_od_shares(scenario, abar_scen, params)
    out$od_dissimilarity_pct <- 100 * 0.5 * sum(abs(od_scen - od_base))
    out$base_od_weighted_bilateral_cost_min <- weighted_bilateral_cost(base, od_base, params$theta)
    out$scenario_od_weighted_bilateral_cost_min <- weighted_bilateral_cost(scenario, od_scen, params$theta)
    out$od_weighted_bilateral_cost_pct_change <- 100 * (
      out$scenario_od_weighted_bilateral_cost_min / out$base_od_weighted_bilateral_cost_min - 1
    )
  }
  out
}

summarise_vs_baseline <- function(baseline, scenario, prim, params, abar_theta, abar_theta_scenario = NULL) {
  row <- comparison_summary(baseline, scenario, prim, params, abar_theta, abar_theta_scenario)
  row$label_baseline <- baseline$label
  row$label_scenario <- scenario$label
  row$endogenous_congestion <- scenario$endogenous_congestion
  row$mean_Xi_pct_change <- 100 * mean(scenario$Xi_kl / pmax(baseline$Xi_kl, EPS) - 1)
  row$p95_abs_log_Pi_change <- as.numeric(stats::quantile(
    abs(log(pmax(scenario$Pi_i, EPS) / pmax(baseline$Pi_i, EPS))),
    0.95,
    names = FALSE
  ))
  row
}

solve_baseline_exogenous <- function(prim, fund, params, kw) {
  do.call_solve_equilibrium(prim, fund, params, "baseline_exogenous_tau", FALSE, kw)
}

solve_baseline_endogenous <- function(prim, fund, params, kw) {
  do.call_solve_equilibrium(prim, fund, params, "baseline_endogenous", TRUE, kw)
}

run_cf1_symmetric <- function(prim, fund, params, sym_method = "geometric", kw, endogenous = TRUE, label_suffix = "") {
  tau_sym <- build_symmetric_tau_min(prim, sym_method)
  label <- if (label_suffix == "") sprintf("CF1_symmetric_%s", sym_method) else sprintf("CF1_symmetric_%s_%s", sym_method, label_suffix)
  do.call_solve_equilibrium(
    prim, fund, params,
    label = label,
    endogenous = endogenous,
    kw = kw,
    extra = list(tau_min_init = tau_sym)
  )
}

run_tidal_endogenous <- function(prim,
                                 fund,
                                 params,
                                 baseline,
                                 kw,
                                 top_k = 50,
                                 delta = 1.0,
                                 horizon = "long_run",
                                 pair_selection = "score_bpr_st") {
  sel <- select_tidal_lane_pairs_by_rank(
    prim,
    top_k = top_k,
    delta = delta,
    rank = pair_selection,
    xi_kl = if (pair_selection == "score_bpr_st") baseline$Xi_kl else NULL
  )
  n_cf <- apply_tidal_lanes(prim, sel, delta)
  extra <- list(n_kl_override = n_cf)
  if (horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  } else if (horizon != "long_run") {
    stop(sprintf("unknown tidal horizon: %s", horizon), call. = FALSE)
  }
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("tidal_top%s_d%s_%s", top_k, delta, horizon),
    endogenous = TRUE,
    kw = kw,
    extra = extra
  )
  list(eq = eq, selection = sel)
}

run_cf2_tidal_structural <- function(prim,
                                     fund,
                                     params,
                                     baseline,
                                     kw,
                                     top_k = 50,
                                     delta = 1.0,
                                     pair_selection = "score_bpr_st") {
  sel <- select_tidal_lane_pairs_by_rank(
    prim,
    top_k = top_k,
    delta = delta,
    rank = pair_selection,
    xi_kl = if (pair_selection == "score_bpr_st") baseline$Xi_kl else NULL
  )
  n_cf <- apply_tidal_lanes(prim, sel, delta)
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = "equilibrium",
    endogenous = TRUE,
    kw = kw,
    extra = list(n_kl_override = n_cf, tau_min_init_ff = TRUE)
  )
  list(eq = eq, selection = sel)
}

run_road_expansion_same_corridor <- function(prim,
                                             fund,
                                             params,
                                             baseline,
                                             kw,
                                             top_k = 100,
                                             delta = 1.0,
                                             pair_selection = "score_bpr_st",
                                             horizon = "long_run") {
  sel <- select_tidal_lane_pairs_by_rank(
    prim,
    top_k = top_k,
    delta = delta,
    rank = pair_selection,
    xi_kl = if (pair_selection == "score_bpr_st") baseline$Xi_kl else NULL
  )
  n_cf <- apply_bidirectional_lane_delta(prim, sel, delta)
  extra <- list(n_kl_override = n_cf)
  if (horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  }
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("expansion_same_corridor_top%s_%s", top_k, horizon),
    endogenous = TRUE,
    kw = kw,
    extra = extra
  )
  list(eq = eq, selection = sel, n_cf = n_cf)
}

run_road_expansion_qdelay_topk <- function(prim,
                                           fund,
                                           params,
                                           baseline,
                                           kw,
                                           top_k = 100,
                                           delta = 1.0,
                                           horizon = "long_run") {
  sel <- select_tidal_lane_pairs_by_qdelay(prim, top_k = top_k, delta = delta, xi_kl = baseline$Xi_kl)
  n_cf <- apply_bidirectional_lane_delta(prim, sel, delta)
  extra <- list(n_kl_override = n_cf)
  if (horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  }
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("expansion_qdelay_top%s_%s", top_k, horizon),
    endogenous = TRUE,
    kw = kw,
    extra = extra
  )
  list(eq = eq, selection = sel, n_cf = n_cf)
}

run_cf3_uniform <- function(prim, fund, params, kw, factor = 1.10, horizon = "long_run", baseline = NULL, label = NULL) {
  extra <- list(n_kl_override = prim$n_kl * factor, tau_min_init_ff = TRUE)
  if (!is.null(baseline) && horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  }
  if (is.null(label)) label <- sprintf("CF3_uniform_%.2f", factor)
  do.call_solve_equilibrium(prim, fund, params, label, TRUE, kw, extra)
}

run_structural_counterfactuals <- function(prim,
                                           fund,
                                           params,
                                           baseline,
                                           kw,
                                           cf2_top_k = 50,
                                           cf3_factors = c(1.10, 1.25),
                                           sym_method = "geometric",
                                           pair_selection = "score_bpr_st") {
  require_pkg("data.table")
  rows <- list()
  cf1 <- run_cf1_symmetric(prim, fund, params, sym_method, kw, TRUE)
  row <- summarise_vs_baseline(baseline, cf1, prim, params, fund$abar_theta)
  row$cf <- sprintf("CF1_symmetric_%s", sym_method)
  row$congestion_mode <- "endogenous"
  rows[[length(rows) + 1L]] <- row

  if (cf2_top_k > 0) {
    tid <- run_cf2_tidal_structural(prim, fund, params, baseline, kw, cf2_top_k, 1.0, pair_selection)
    row <- summarise_vs_baseline(baseline, tid$eq, prim, params, fund$abar_theta)
    row$cf <- sprintf("CF2_tidal_top%s", cf2_top_k)
    row$congestion_mode <- "endogenous"
    row$n_pairs <- nrow(tid$selection)
    row$pair_selection <- pair_selection
    rows[[length(rows) + 1L]] <- row
  }

  for (fac in cf3_factors) {
    cf3 <- run_cf3_uniform(prim, fund, params, kw, factor = fac, label = "equilibrium")
    row <- summarise_vs_baseline(baseline, cf3, prim, params, fund$abar_theta)
    row$cf <- sprintf("CF3_uniform_%.2f", fac)
    row$congestion_mode <- "endogenous"
    rows[[length(rows) + 1L]] <- row
  }
  data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE)
}

run_structural_exogenous <- function(prim,
                                     fund,
                                     params,
                                     baseline,
                                     kw,
                                     cf2_top_k = 50,
                                     cf3_factors = c(1.10),
                                     sym_method = "geometric",
                                     pair_selection = "score_bpr_st") {
  require_pkg("data.table")
  rows <- list()
  tau_sym <- build_symmetric_tau_min(prim, sym_method)
  cf1 <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("CF1_symmetric_%s_exog", sym_method),
    endogenous = FALSE,
    kw = kw,
    extra = list(tau_min_init = tau_sym)
  )
  row <- summarise_vs_baseline(baseline, cf1, prim, params, fund$abar_theta)
  row$cf <- sprintf("CF1_symmetric_%s", sym_method)
  row$congestion_mode <- "exogenous_tau"
  rows[[length(rows) + 1L]] <- row

  if (cf2_top_k > 0) {
    sel <- select_tidal_lane_pairs_by_rank(
      prim,
      top_k = cf2_top_k,
      delta = 1.0,
      rank = pair_selection,
      xi_kl = if (pair_selection == "score_bpr_st") baseline$Xi_kl else NULL
    )
    tau_cf2 <- prim$tau_obs_min
    delta1 <- params$lambda_legacy * params$theta
    if (nrow(sel)) {
      for (ii in seq_len(nrow(sel))) {
        for (item in list(c(sel$slow[ii], 1.0), c(sel$fast[ii], -1.0))) {
          idx <- as.integer(item[1])
          d_n <- item[2]
          n_new <- max(prim$n_kl[idx] + d_n, 0.25)
          xi <- baseline$Xi_kl[idx]
          tau_cf2[idx] <- max(prim$tau_ff_min[idx] * (xi / max(n_new, EPS)) ^ delta1, prim$tau_ff_min[idx])
        }
      }
    }
    cf2 <- do.call_solve_equilibrium(
      prim, fund, params,
      label = sprintf("CF2_tidal_top%s_exog", cf2_top_k),
      endogenous = FALSE,
      kw = kw,
      extra = list(tau_min_init = tau_cf2)
    )
    row <- summarise_vs_baseline(baseline, cf2, prim, params, fund$abar_theta)
    row$cf <- sprintf("CF2_tidal_top%s", cf2_top_k)
    row$congestion_mode <- "exogenous_tau"
    row$n_pairs <- nrow(sel)
    row$pair_selection <- pair_selection
    rows[[length(rows) + 1L]] <- row
  }

  for (fac in cf3_factors) {
    tau_cf3 <- prim$tau_obs_min / fac
    cf3 <- do.call_solve_equilibrium(
      prim, fund, params,
      label = sprintf("CF3_uniform_%.2f_exog", fac),
      endogenous = FALSE,
      kw = kw,
      extra = list(tau_min_init = tau_cf3)
    )
    row <- summarise_vs_baseline(baseline, cf3, prim, params, fund$abar_theta)
    row$cf <- sprintf("CF3_uniform_%.2f", fac)
    row$congestion_mode <- "exogenous_tau"
    rows[[length(rows) + 1L]] <- row
  }
  data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE)
}

observed_xi <- function(lanes, tau_obs, tau_ff, delta1) {
  ratio <- max(tau_obs, tau_ff) / max(tau_ff, EPS)
  lanes * ratio ^ (1.0 / delta1)
}

bpr_tau_new <- function(tau_ff, tau_obs, lanes_obs, lanes_new, delta1) {
  xi <- observed_xi(lanes_obs, tau_obs, tau_ff, delta1)
  max(tau_ff * (xi / max(lanes_new, EPS)) ^ delta1, tau_ff)
}

build_reciprocal_pair_table <- function(prim, min_len_m = 200.0, min_asym_ratio = 1.0) {
  require_pkg("data.table")
  seen <- rep(FALSE, prim$n_edges)
  key <- paste(prim$edge_i, prim$edge_j, sep = "_")
  idx_map <- seq_along(key)
  names(idx_map) <- key
  rows <- list()
  for (idx in seq_along(key)) {
    if (seen[idx]) next
    rev_key <- paste(prim$edge_j[idx], prim$edge_i[idx], sep = "_")
    seen[idx] <- TRUE
    if (!(rev_key %in% names(idx_map))) next
    rev <- idx_map[[rev_key]]
    seen[rev] <- TRUE
    len_ij <- prim$ell_kl_km[idx] * 1000
    len_ji <- prim$ell_kl_km[rev] * 1000
    if (min(len_ij, len_ji) < min_len_m) next
    tau_ij <- prim$tau_obs_min[idx]
    tau_ji <- prim$tau_obs_min[rev]
    ratio <- max(tau_ij, tau_ji) / min(tau_ij, tau_ji)
    if (ratio < min_asym_ratio) next
    rows[[length(rows) + 1L]] <- data.table::data.table(
      edge_idx_ij = idx,
      edge_idx_ji = rev,
      tau_ij = tau_ij,
      tau_ji = tau_ji,
      tau_ff_ij = prim$tau_ff_min[idx],
      tau_ff_ji = prim$tau_ff_min[rev],
      lanes_ij = prim$n_kl[idx],
      lanes_ji = prim$n_kl[rev],
      asym_ratio = ratio
    )
  }
  if (!length(rows)) return(data.table::data.table())
  out <- data.table::rbindlist(rows)
  out[order(-asym_ratio, method = "radix")]
}

select_pairs_bpr_grid <- function(prim,
                                  top_n = 50,
                                  delta_lanes = 1.0,
                                  min_asym_ratio = 1.10,
                                  min_len_m = 200.0,
                                  min_fast_lanes = 2.0) {
  pool <- build_reciprocal_pair_table(prim, min_len_m, min_asym_ratio)
  if (!nrow(pool)) return(pool)
  picked <- list()
  for (ii in seq_len(nrow(pool))) {
    if (length(picked) >= top_n) break
    row <- pool[ii]
    if (row$tau_ij >= row$tau_ji) {
      edge_slow <- row$edge_idx_ij
      edge_fast <- row$edge_idx_ji
      lanes_slow <- row$lanes_ij
      lanes_fast <- row$lanes_ji
      tau_slow <- row$tau_ij
      tau_fast <- row$tau_ji
    } else {
      edge_slow <- row$edge_idx_ji
      edge_fast <- row$edge_idx_ij
      lanes_slow <- row$lanes_ji
      lanes_fast <- row$lanes_ij
      tau_slow <- row$tau_ji
      tau_fast <- row$tau_ij
    }
    if (lanes_fast < min_fast_lanes) next
    if (lanes_fast - delta_lanes < 1.0) next
    picked[[length(picked) + 1L]] <- cbind(
      row,
      data.table::data.table(
        edge_idx_slow = edge_slow,
        edge_idx_fast = edge_fast,
        lanes_slow = lanes_slow,
        lanes_fast = lanes_fast,
        tau_slow_obs = tau_slow,
        tau_fast_obs = tau_fast
      )
    )
  }
  if (!length(picked)) return(data.table::data.table())
  out <- data.table::rbindlist(picked)
  out[, rank := seq_len(.N)]
  out
}

apply_tidal_lane_shock_bpr <- function(prim, tau_base, spec, selected = NULL) {
  require_pkg("data.table")
  if (is.null(selected)) selected <- select_pairs_bpr_grid(
    prim,
    top_n = spec$top_n,
    delta_lanes = spec$delta_lanes,
    min_asym_ratio = spec$min_asym_ratio
  )
  tau_out <- tau_base
  detail <- list()
  if (nrow(selected)) {
    for (ii in seq_len(nrow(selected))) {
      row <- selected[ii]
      e_slow <- row$edge_idx_slow
      e_fast <- row$edge_idx_fast
      lanes_slow_new <- row$lanes_slow + spec$delta_lanes
      lanes_fast_new <- row$lanes_fast - spec$delta_lanes
      if (spec$method == "bpr") {
        tau_slow_new <- bpr_tau_new(prim$tau_ff_min[e_slow], row$tau_slow_obs, row$lanes_slow, lanes_slow_new, spec$delta1)
        tau_fast_new <- bpr_tau_new(prim$tau_ff_min[e_fast], row$tau_fast_obs, row$lanes_fast, lanes_fast_new, spec$delta1)
      } else {
        tau_slow_new <- row$tau_slow_obs * spec$slow_tau_scale
        tau_fast_new <- row$tau_fast_obs * spec$fast_tau_scale
      }
      tau_out[e_slow] <- tau_slow_new
      tau_out[e_fast] <- tau_fast_new
      detail[[length(detail) + 1L]] <- cbind(
        selected[ii],
        data.table::data.table(
          spec_name = spec$name,
          method = spec$method,
          delta_lanes = spec$delta_lanes,
          lanes_slow_new = lanes_slow_new,
          lanes_fast_new = lanes_fast_new,
          tau_slow_new = tau_slow_new,
          tau_fast_new = tau_fast_new,
          tau_slow_pct_change = 100 * (tau_slow_new / row$tau_slow_obs - 1),
          tau_fast_pct_change = 100 * (tau_fast_new / row$tau_fast_obs - 1)
        )
      )
    }
  }
  list(tau = tau_out, detail = if (length(detail)) data.table::rbindlist(detail, fill = TRUE) else data.table::data.table())
}

default_tidal_specs <- function(quick = FALSE) {
  py_num <- function(x) {
    if (abs(x - round(x)) < 1e-12) {
      sprintf("%.1f", x)
    } else {
      sub("0+$", "", sub("\\.$", "", sprintf("%.12f", x)))
    }
  }
  dotp <- function(x) gsub("\\.", "p", py_num(x))
  nodot <- function(x) gsub("\\.", "", py_num(x))
  mk <- function(name, top_n, delta_lanes, min_asym_ratio = 1.10, method = "bpr", slow_tau_scale = 0.90, fast_tau_scale = 1.10) {
    list(
      name = name,
      top_n = top_n,
      delta_lanes = delta_lanes,
      min_asym_ratio = min_asym_ratio,
      method = method,
      slow_tau_scale = slow_tau_scale,
      fast_tau_scale = fast_tau_scale,
      delta1 = 0.488
    )
  }
  if (quick) return(list(mk("bpr_top25_d1", 25, 1.0)))
  specs <- list()
  for (top_n in c(10, 25, 50, 100)) {
    for (delta in c(0.5, 1.0)) {
      specs[[length(specs) + 1L]] <- mk(sprintf("bpr_top%s_d%s_r1p10", top_n, dotp(delta)), top_n, delta)
    }
  }
  for (ratio in c(1.25, 1.50, 2.00)) {
    specs[[length(specs) + 1L]] <- mk(sprintf("bpr_top50_d1_r%s", dotp(ratio)), 50, 1.0, ratio)
  }
  for (scales in list(c(0.90, 1.10), c(0.85, 1.15), c(0.80, 1.20))) {
    specs[[length(specs) + 1L]] <- mk(
      sprintf("direct_top50_s%s_f%s", nodot(scales[1]), nodot(scales[2])),
      50,
      1.0,
      1.10,
      "direct_tau",
      scales[1],
      scales[2]
    )
  }
  specs
}

run_tidal_exogenous_bpr_grid <- function(prim, fund, params, baseline, kw, specs = NULL, quick = FALSE) {
  require_pkg("data.table")
  if (is.null(specs)) specs <- default_tidal_specs(quick)
  rows <- list()
  details <- list()
  for (spec in specs) {
    selected <- select_pairs_bpr_grid(
      prim,
      top_n = spec$top_n,
      delta_lanes = spec$delta_lanes,
      min_asym_ratio = spec$min_asym_ratio
    )
    shock <- apply_tidal_lane_shock_bpr(prim, prim$tau_obs_min, spec, selected)
    for (horizon in c("exogenous_tau", "short_run_fixed_shares")) {
      extra <- list(tau_min_init = shock$tau)
      if (horizon == "short_run_fixed_shares") {
        extra$l_R_fixed <- baseline$l_R
        extra$l_F_fixed <- baseline$l_F
      }
      eq <- do.call_solve_equilibrium(
        prim, fund, params,
        label = sprintf("%s_%s", spec$name, horizon),
        endogenous = FALSE,
        kw = kw,
        extra = extra
      )
      row <- summarise_vs_baseline(baseline, eq, prim, params, fund$abar_theta)
      row$policy_family <- "tidal_lane_bpr_fixed_xi"
      row$spec_name <- spec$name
      row$horizon <- horizon
      row$top_n <- spec$top_n
      row$delta_lanes <- spec$delta_lanes
      row$n_pairs <- nrow(selected)
      rows[[length(rows) + 1L]] <- row
    }
    if (nrow(shock$detail)) details[[length(details) + 1L]] <- shock$detail
  }
  list(
    rows = data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE),
    details = if (length(details)) data.table::rbindlist(details, fill = TRUE) else data.table::data.table()
  )
}

grid_xy <- function(prim) {
  parts <- strsplit(as.character(prim$node_grid_ids), "_")
  x <- vapply(parts, function(z) if (length(z) >= 3) as.numeric(z[[2]]) else 0, numeric(1))
  y <- vapply(parts, function(z) if (length(z) >= 3) as.numeric(z[[3]]) else 0, numeric(1))
  list(x = x, y = y)
}

employment_hub_mask <- function(prim, quantile = 0.75) {
  pos <- prim$L_F_obs > 0
  thr <- as.numeric(stats::quantile(prim$L_F_obs[pos], quantile, names = FALSE))
  prim$L_F_obs >= thr
}

outer_ring_mask <- function(prim, quantile = 0.75) {
  xy <- grid_xy(prim)
  w <- pmax(prim$L_F_obs, EPS)
  cx <- stats::weighted.mean(xy$x, w)
  cy <- stats::weighted.mean(xy$y, w)
  dist <- sqrt((xy$x - cx)^2 + (xy$y - cy)^2)
  valid <- prim$L_R_obs > 0
  thr <- as.numeric(stats::quantile(dist[valid], quantile, names = FALSE))
  dist >= thr
}

policy_spec <- function(name,
                        tau_scale = 1.0,
                        tau_scale_on_hub_edges = NA_real_,
                        tau_scale_on_outer_edges = NA_real_,
                        hub_quantile = 0.75,
                        outer_ring_dist_quantile = 0.75,
                        abar_mult_on_hubs = 1.0,
                        ubar_mult_on_outer_ring = 1.0) {
  list(
    name = name,
    tau_scale = tau_scale,
    tau_scale_on_hub_edges = tau_scale_on_hub_edges,
    tau_scale_on_outer_edges = tau_scale_on_outer_edges,
    hub_quantile = hub_quantile,
    outer_ring_dist_quantile = outer_ring_dist_quantile,
    abar_mult_on_hubs = abar_mult_on_hubs,
    ubar_mult_on_outer_ring = ubar_mult_on_outer_ring
  )
}

default_policies <- function() {
  list(
    policy_spec("uniform_speed_10pct", tau_scale = 0.90),
    policy_spec("hub_access_15pct", tau_scale_on_hub_edges = 0.85),
    policy_spec("hub_productivity_10pct", abar_mult_on_hubs = 1.10),
    policy_spec("outer_ring_amenity_10pct", ubar_mult_on_outer_ring = 1.10),
    policy_spec("fringe_transport_20pct", tau_scale_on_outer_edges = 0.80)
  )
}

apply_policy_tau <- function(tau, prim, spec) {
  out <- as.numeric(tau) * spec$tau_scale
  if (is.finite(spec$tau_scale_on_hub_edges)) {
    hub <- employment_hub_mask(prim, spec$hub_quantile)
    touch <- hub[prim$edge_i] | hub[prim$edge_j]
    out[touch] <- out[touch] * spec$tau_scale_on_hub_edges
  }
  if (is.finite(spec$tau_scale_on_outer_edges)) {
    outer <- outer_ring_mask(prim, spec$outer_ring_dist_quantile)
    touch <- outer[prim$edge_i] | outer[prim$edge_j]
    out[touch] <- out[touch] * spec$tau_scale_on_outer_edges
  }
  out
}

apply_policy_abar <- function(abar_theta, prim, spec) {
  out <- as.numeric(abar_theta)
  if (spec$abar_mult_on_hubs != 1.0) {
    out[employment_hub_mask(prim, spec$hub_quantile)] <- out[employment_hub_mask(prim, spec$hub_quantile)] * spec$abar_mult_on_hubs
  }
  out
}

apply_policy_ubar <- function(ubar_theta, prim, spec) {
  out <- as.numeric(ubar_theta)
  if (spec$ubar_mult_on_outer_ring != 1.0) {
    out[outer_ring_mask(prim, spec$outer_ring_dist_quantile)] <- out[outer_ring_mask(prim, spec$outer_ring_dist_quantile)] * spec$ubar_mult_on_outer_ring
  }
  out
}

apply_tau_ff_policy <- function(tau_ff, prim, spec) {
  apply_policy_tau(tau_ff, prim, spec)
}

run_policy_exogenous_tau <- function(prim, fund, params, baseline, spec, kw) {
  tau_cf <- apply_policy_tau(prim$tau_obs_min, prim, spec)
  ubar_cf <- apply_policy_ubar(fund$ubar_theta, prim, spec)
  abar_cf <- apply_policy_abar(fund$abar_theta, prim, spec)
  args <- c(
    list(
      prim = prim,
      ubar_theta = ubar_cf,
      abar_theta = abar_cf,
      params = params,
      tau_min_init = tau_cf,
      endogenous_congestion = FALSE,
      label = sprintf("%s_exogenous_tau", spec$name)
    ),
    kw
  )
  do.call(solve_equilibrium, args)
}

run_policy_endogenous <- function(prim, fund, params, spec, kw) {
  tau_ff <- apply_tau_ff_policy(prim$tau_ff_min, prim, spec)
  ubar_cf <- apply_policy_ubar(fund$ubar_theta, prim, spec)
  abar_cf <- apply_policy_abar(fund$abar_theta, prim, spec)
  args <- c(
    list(
      prim = prim,
      ubar_theta = ubar_cf,
      abar_theta = abar_cf,
      params = params,
      tau_ff_override = tau_ff,
      tau_min_init = prim$tau_obs_min,
      endogenous_congestion = TRUE,
      label = sprintf("%s_endogenous", spec$name)
    ),
    kw
  )
  do.call(solve_equilibrium, args)
}

run_all_policy_counterfactuals <- function(prim,
                                           fund,
                                           params,
                                           baseline_exog,
                                           baseline_endog,
                                           kw,
                                           policies = NULL) {
  require_pkg("data.table")
  if (is.null(policies)) policies <- default_policies()
  rows <- list()
  for (spec in policies) {
    eq_exog <- run_policy_exogenous_tau(prim, fund, params, baseline_exog, spec, kw)
    row <- summarise_vs_baseline(baseline_exog, eq_exog, prim, params, fund$abar_theta)
    row$policy <- spec$name
    row$congestion_mode <- "exogenous_tau"
    rows[[length(rows) + 1L]] <- row

    eq_endog <- run_policy_endogenous(prim, fund, params, spec, kw)
    row <- summarise_vs_baseline(baseline_endog, eq_endog, prim, params, fund$abar_theta)
    row$policy <- spec$name
    row$congestion_mode <- "endogenous"
    rows[[length(rows) + 1L]] <- row
  }
  data.table::rbindlist(lapply(rows, data.table::as.data.table), fill = TRUE)
}

realized_welfare_excl_toll <- function(prim,
                                       fund,
                                       params,
                                       scenario,
                                       congestion_lambda,
                                       xi_obs_kl,
                                       n_obs_kl,
                                       tau_obs_anchor,
                                       n_kl = NULL,
                                       congestion_form = "anchored") {
  n_used <- if (is.null(n_kl)) prim$n_kl else n_kl
  tau_real <- congestion_tau_update(
    form = congestion_form,
    lam = congestion_lambda,
    tau_ff = prim$tau_ff_min,
    tau_obs = tau_obs_anchor,
    Xi_kl = scenario$Xi_kl,
    n_kl = resolve_lanes(n_used),
    ell_km = prim$ell_kl_km,
    xi_obs_kl = xi_obs_kl,
    n_obs_kl = n_obs_kl
  )
  tau_inv <- compute_tau_inv_theta(build_A_matrix(prim, tau_real, scenario$kappa))
  theta <- params$theta
  L_R <- pmax(scenario$l_R * prim$Lbar, EPS)
  L_F <- pmax(scenario$l_F * prim$Lbar, EPS)
  u_theta <- fund$ubar_theta * (L_R ^ (params$beta * theta))
  w_theta <- fund$abar_theta * (L_F ^ (params$alpha * theta))
  Wbar_theta <- sum(tau_inv * tcrossprod(u_theta, w_theta))
  if (Wbar_theta > 0) Wbar_theta ^ (1.0 / theta) else NA_real_
}

run_congestion_pricing_first_best <- function(prim,
                                              fund,
                                              params,
                                              baseline,
                                              kw,
                                              horizon = "long_run",
                                              congestion_lambda = 1.1) {
  toll <- congestion_lambda * baseline$tau_min_kl
  tau_ff_cf <- prim$tau_ff_min + toll
  extra <- list(tau_ff_override = tau_ff_cf)
  if (horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  }
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("pricing_first_best_%s", horizon),
    endogenous = TRUE,
    kw = kw,
    extra = extra
  )
  revenue <- sum(eq$Xi_kl * toll)
  list(
    eq = eq,
    meta = list(
      toll_revenue_per_day = revenue,
      lump_sum_rebate_per_worker = revenue / max(prim$Lbar, EPS),
      policy_capex_lane_km = 0.0
    )
  )
}

run_cordon_pricing <- function(prim,
                               fund,
                               params,
                               baseline,
                               kw,
                               hub_quantile = 0.90,
                               toll_per_min = 2.0,
                               horizon = "long_run") {
  hub <- employment_hub_mask(prim, hub_quantile)
  touch <- hub[prim$edge_i] | hub[prim$edge_j]
  tau_ff_cf <- prim$tau_ff_min
  tau_ff_cf[touch] <- tau_ff_cf[touch] + toll_per_min
  extra <- list(tau_ff_override = tau_ff_cf)
  if (horizon == "short_run") {
    extra$l_R_fixed <- baseline$l_R
    extra$l_F_fixed <- baseline$l_F
  }
  eq <- do.call_solve_equilibrium(
    prim, fund, params,
    label = sprintf("pricing_cordon_q%s_%s", hub_quantile, horizon),
    endogenous = TRUE,
    kw = kw,
    extra = extra
  )
  revenue <- sum(baseline$Xi_kl[touch] * toll_per_min)
  list(
    eq = eq,
    meta = list(
      n_treated_edges = sum(touch),
      toll_revenue_per_day = revenue,
      lump_sum_rebate_per_worker = revenue / max(prim$Lbar, EPS),
      policy_capex_lane_km = 0.0
    )
  )
}
