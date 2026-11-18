source(file.path("R", "model", "counterfactuals.R"))

# All-route bilateral iceberg-cost index tau_ij. It is not measured in minutes
# and must never be multiplied by 60 to construct seconds (mirrors the Python
# `bilateral_composite_cost_matrix`, 2026-07-16 fix).
bilateral_composite_cost_matrix <- function(eq, theta) {
  pmax(eq$tau_ij_inv_theta, EPS) ^ (-1.0 / theta)
}

# Equilibrium OD shares from the gravity / market-access system
# (mirrors Python `equilibrium_od_shares`).
equilibrium_od_shares <- function(eq, prim, theta) {
  R <- eq$tau_ij_inv_theta
  L_R <- eq$l_R * prim$Lbar
  L_F <- eq$l_F * prim$Lbar
  origin <- L_R * pmax(eq$Pi_i, EPS) ^ theta
  destination <- L_F * pmax(eq$P_j, EPS) ^ theta
  shares <- R * tcrossprod(origin, destination) / prim$Lbar
  total <- sum(shares)
  if (!is.finite(total) || total <= 0) stop("equilibrium OD shares have non-positive mass")
  shares / total
}

# Expected additive physical route minutes conditional on each OD pair:
# M_ij = [R B R]_ij / R_ij with B_kl = sum_{e:k->l} a_e * m_e and a_e = 1/(kappa m_e)
# (mirrors Python `expected_additive_route_minutes`).
expected_additive_route_minutes <- function(eq, prim) {
  R <- eq$tau_ij_inv_theta
  m <- pmax(eq$tau_min_kl, EPS)
  kappa <- as.numeric(eq$kappa)
  B <- matrix(0.0, nrow = nrow(R), ncol = ncol(R))
  idx <- cbind(prim$edge_i, prim$edge_j)
  B[idx] <- B[idx] + (1.0 / (kappa * m)) * m
  numerator <- R %*% B %*% R
  out <- matrix(0.0, nrow = nrow(R), ncol = ncol(R))
  ok <- R > EPS
  out[ok] <- numerator[ok] / R[ok]
  out
}

# Physical-time decomposition. `M` is expected additive route time built from
# physical link minutes; the headline is the link-passage average
# Xi.m / (Lbar * kappa). The legacy field names are kept for downstream
# scripts, but their values now refer to physical time rather than tau_ij
# (mirrors the Python `od_cost_decomposition` after the 2026-07-16 fix).
od_cost_decomposition <- function(baseline,
                                  short_run,
                                  long_run,
                                  prim,
                                  params,
                                  abar_theta) {
  theta <- params$theta
  od0 <- equilibrium_od_shares(baseline, prim, theta)
  od_st <- equilibrium_od_shares(short_run, prim, theta)
  od_lt <- equilibrium_od_shares(long_run, prim, theta)
  M0 <- expected_additive_route_minutes(baseline, prim)
  Mst <- expected_additive_route_minutes(short_run, prim)
  Mlt <- expected_additive_route_minutes(long_run, prim)

  od_route_avg0 <- sum(od0 * M0)
  od_route_avg_st <- sum(od_st * Mst)
  od_route_avg_lt <- sum(od_lt * Mlt)

  composite0 <- bilateral_composite_cost_matrix(baseline, theta)
  composite_st <- bilateral_composite_cost_matrix(short_run, theta)
  composite_lt <- bilateral_composite_cost_matrix(long_run, theta)

  link_average <- function(eq) {
    # The solver's Xi omits the route-diffusion normalization when kappa != 1.
    sum(eq$Xi_kl * eq$tau_min_kl) / (prim$Lbar * as.numeric(eq$kappa))
  }
  avg0 <- link_average(baseline)
  avg_st <- link_average(short_run)
  avg_lt <- link_average(long_run)
  total_st <- avg_st - avg0
  total_lt <- avg_lt - avg0

  # Exact decomposition in link-passage space: ST term is the full ST change;
  # GE wedge is the LT-minus-ST link-time change at ST passages; sorting is
  # the passage reallocation evaluated at LT minutes.
  st_passages <- short_run$Xi_kl / as.numeric(short_run$kappa)
  lt_passages <- long_run$Xi_kl / as.numeric(long_run$kappa)
  direct_st <- total_st
  ge_wedge <- sum((long_run$tau_min_kl - short_run$tau_min_kl) * st_passages) / prim$Lbar
  sorting <- sum(long_run$tau_min_kl * (lt_passages - st_passages)) / prim$Lbar

  list(
    avg_physical_time_base_min = avg0,
    avg_physical_time_st_min = avg_st,
    avg_physical_time_lt_min = avg_lt,
    od_route_matrix_avg_time_base_min = od_route_avg0,
    od_route_matrix_avg_time_st_min = od_route_avg_st,
    od_route_matrix_avg_time_lt_min = od_route_avg_lt,
    avg_composite_cost_base_index = sum(od0 * composite0),
    avg_composite_cost_st_index = sum(od_st * composite_st),
    avg_composite_cost_lt_index = sum(od_lt * composite_lt),
    direct_st_cost_change_min = direct_st,
    direct_st_cost_change_sec = 60.0 * direct_st,
    ge_cost_wedge_min = ge_wedge,
    ge_cost_wedge_sec = 60.0 * ge_wedge,
    sorting_wedge_min = sorting,
    sorting_wedge_sec = 60.0 * sorting,
    total_lt_cost_change_min = total_lt,
    total_lt_cost_change_sec = 60.0 * total_lt,
    total_st_cost_change_min = total_st,
    total_st_cost_change_sec = 60.0 * total_st,
    physical_time_saved_st_sec_per_commuter = -60.0 * total_st,
    physical_time_saved_lt_sec_per_commuter = -60.0 * total_lt,
    physical_time_saved_st_person_hours = -total_st * prim$Lbar / 60.0,
    physical_time_saved_lt_person_hours = -total_lt * prim$Lbar / 60.0,
    od_route_matrix_minus_link_time_base_sec = 60.0 * (od_route_avg0 - avg0),
    od_route_matrix_minus_link_time_st_sec = 60.0 * (od_route_avg_st - avg_st),
    od_route_matrix_minus_link_time_lt_sec = 60.0 * (od_route_avg_lt - avg_lt),
    od_dissimilarity_st_vs_base_pct = 100.0 * 0.5 * sum(abs(od_st - od0)),
    od_dissimilarity_lt_vs_base_pct = 100.0 * 0.5 * sum(abs(od_lt - od0))
  )
}

edge_mask_stats <- function(tau, tau_ff, tau_obs, xi, mask, prefix) {
  if (!any(mask)) return(list())
  w <- xi / sum(xi)
  wm <- w[mask] / sum(w[mask])
  markup_ff <- tau / pmax(tau_ff, EPS) - 1.0
  markup_obs <- tau / pmax(tau_obs, EPS) - 1.0
  cong_min <- tau - tau_ff
  out <- list()
  out[[sprintf("%s_mean_tau_min", prefix)]] <- mean(tau[mask])
  out[[sprintf("%s_mean_congestion_min", prefix)]] <- mean(cong_min[mask])
  out[[sprintf("%s_xi_weighted_congestion_min", prefix)]] <- sum(wm * cong_min[mask])
  out[[sprintf("%s_mean_markup_ff", prefix)]] <- sum(wm * markup_ff[mask])
  out[[sprintf("%s_mean_markup_obs", prefix)]] <- sum(wm * markup_obs[mask])
  out[[sprintf("%s_total_xi_weighted_travel_min", prefix)]] <- sum(wm * tau[mask])
  out
}

edge_congestion_summary <- function(eq,
                                    prim,
                                    baseline = NULL,
                                    treated_edge_idx = NULL) {
  tau <- eq$tau_min_kl
  tau_ff <- prim$tau_ff_min
  tau_obs <- prim$tau_obs_min
  xi <- pmax(eq$Xi_kl, EPS)
  w <- xi / sum(xi)
  markup_ff <- tau / pmax(tau_ff, EPS) - 1.0
  markup_obs <- tau / pmax(tau_obs, EPS) - 1.0
  cong_min <- tau - tau_ff

  out <- list(
    network_xi_weighted_congestion_min = sum(w * cong_min),
    network_xi_weighted_travel_min = sum(w * tau),
    network_mean_markup_ff = sum(w * markup_ff),
    network_mean_markup_obs = sum(w * markup_obs),
    network_total_xi = sum(xi)
  )
  out <- c(out, edge_mask_stats(tau, tau_ff, tau_obs, xi, rep(TRUE, length(tau)), "network"))

  if (!is.null(treated_edge_idx) && length(treated_edge_idx)) {
    mask <- rep(FALSE, length(tau))
    mask[as.integer(treated_edge_idx)] <- TRUE
    out <- c(out, edge_mask_stats(tau, tau_ff, tau_obs, xi, mask, "treated"))
  }

  if (!is.null(baseline)) {
    dtau <- tau - baseline$tau_min_kl
    dxi <- eq$Xi_kl - baseline$Xi_kl
    out$network_mean_delta_tau_min <- mean(dtau)
    out$network_xi_weighted_delta_tau_min <- sum(w * dtau)
    out$network_mean_delta_xi_pct <- 100.0 * mean(eq$Xi_kl / pmax(baseline$Xi_kl, EPS) - 1.0)
    if (!is.null(treated_edge_idx) && length(treated_edge_idx)) {
      mask <- rep(FALSE, length(tau))
      mask[as.integer(treated_edge_idx)] <- TRUE
      wm <- w[mask] / sum(w[mask])
      out$treated_mean_delta_tau_min <- mean(dtau[mask])
      out$treated_xi_weighted_delta_tau_min <- sum(wm * dtau[mask])
      out$treated_mean_delta_xi_pct <- 100.0 * mean(eq$Xi_kl[mask] / pmax(baseline$Xi_kl[mask], EPS) - 1.0)
    }
  }
  out
}

treated_edge_frame <- function(prim, baseline, scenario, selection) {
  require_pkg("data.table")
  rows <- list()
  if (nrow(selection)) {
    for (kk in seq_len(nrow(selection))) {
      for (role in c("slow", "fast")) {
        idx <- selection[[role]][kk]
        rows[[length(rows) + 1L]] <- data.table::data.table(
          pair_rank = kk,
          role = role,
          edge_idx = as.integer(idx - 1L),
          node_i = as.integer(prim$edge_i[idx] - 1L),
          node_j = as.integer(prim$edge_j[idx] - 1L),
          tau_base_min = baseline$tau_min_kl[idx],
          tau_scenario_min = scenario$tau_min_kl[idx],
          delta_tau_min = scenario$tau_min_kl[idx] - baseline$tau_min_kl[idx],
          delta_tau_sec = 60.0 * (scenario$tau_min_kl[idx] - baseline$tau_min_kl[idx]),
          delta_tau_pct = 100.0 * (scenario$tau_min_kl[idx] / pmax(baseline$tau_min_kl[idx], EPS) - 1.0),
          tau_ff_min = prim$tau_ff_min[idx],
          congestion_markup_ff_base = baseline$tau_min_kl[idx] / pmax(prim$tau_ff_min[idx], EPS) - 1.0,
          congestion_markup_ff_scenario = scenario$tau_min_kl[idx] / pmax(prim$tau_ff_min[idx], EPS) - 1.0,
          Xi_base = baseline$Xi_kl[idx],
          Xi_scenario = scenario$Xi_kl[idx],
          delta_Xi_pct = 100.0 * (scenario$Xi_kl[idx] / pmax(baseline$Xi_kl[idx], EPS) - 1.0),
          n_lanes_base = prim$n_kl[idx]
        )
      }
    }
  }
  if (!length(rows)) return(data.table::data.table())
  data.table::rbindlist(rows)
}

summarise_tidal_pair <- function(spec_label,
                                 top_k,
                                 delta_lanes,
                                 baseline,
                                 short_run,
                                 long_run,
                                 prim,
                                 params,
                                 abar_theta,
                                 selection) {
  require_pkg("data.table")
  treated_idx <- unique(c(selection$slow, selection$fast))
  decomp <- od_cost_decomposition(baseline, short_run, long_run, prim, params, abar_theta)
  row <- c(
    list(
      spec = spec_label,
      top_k = top_k,
      delta_lanes = delta_lanes,
      n_pairs = nrow(selection),
      Wbar_base = baseline$Wbar,
      Wbar_st = short_run$Wbar,
      Wbar_lt = long_run$Wbar,
      Wbar_pct_change_st = 100.0 * (short_run$Wbar / baseline$Wbar - 1.0),
      Wbar_pct_change_lt = 100.0 * (long_run$Wbar / baseline$Wbar - 1.0),
      sorting_welfare_wedge_pct = 100.0 * (
        (long_run$Wbar / baseline$Wbar - 1.0) -
          (short_run$Wbar / baseline$Wbar - 1.0)
      )
    ),
    decomp
  )
  for (pair in list(list(prefix = "st", eq = short_run), list(prefix = "lt", eq = long_run))) {
    prefix <- pair$prefix
    eq <- pair$eq
    edge <- edge_congestion_summary(eq, prim, baseline = baseline, treated_edge_idx = treated_idx)
    for (nm in names(edge)) row[[sprintf("%s_%s", prefix, nm)]] <- edge[[nm]]
  }

  detail_st <- treated_edge_frame(prim, baseline, short_run, selection)
  if (nrow(detail_st)) {
    detail_st[, spec := spec_label]
    detail_st[, horizon := "short_run"]
  }
  detail_lt <- treated_edge_frame(prim, baseline, long_run, selection)
  if (nrow(detail_lt)) {
    detail_lt[, spec := spec_label]
    detail_lt[, horizon := "long_run"]
  }
  detail <- data.table::rbindlist(list(detail_st, detail_lt), fill = TRUE)
  list(row = row, decomp = decomp, detail = detail)
}

sorting_stats_alpha <- function(base, lt) {
  dl_r <- log(pmax(lt$l_R, EPS)) - log(pmax(base$l_R, EPS))
  dl_f <- log(pmax(lt$l_F, EPS)) - log(pmax(base$l_F, EPS))
  d_pi <- log(pmax(lt$Pi_i, EPS)) - log(pmax(base$Pi_i, EPS))
  list(
    max_abs_log_l_R_change = max(abs(dl_r)),
    max_abs_log_l_F_change = max(abs(dl_f)),
    mean_abs_log_l_R_change = mean(abs(dl_r)),
    mean_abs_log_l_F_change = mean(abs(dl_f)),
    p95_abs_log_l_R_change = as.numeric(stats::quantile(abs(dl_r), 0.95, names = FALSE)),
    p95_abs_log_l_F_change = as.numeric(stats::quantile(abs(dl_f), 0.95, names = FALSE)),
    max_abs_log_Pi_change = max(abs(d_pi)),
    residence_dissimilarity_pct = 100.0 * 0.5 * sum(abs(lt$l_R - base$l_R)),
    workplace_dissimilarity_pct = 100.0 * 0.5 * sum(abs(lt$l_F - base$l_F))
  )
}

corridor_pct_alpha <- function(base, lt, selection) {
  pct <- 100.0 * (1.0 - lt$tau_min_kl[selection$slow] / pmax(base$tau_min_kl[selection$slow], EPS))
  list(
    corridor_slow_median_pct_save_lt = stats::median(pct),
    corridor_slow_mean_pct_save_lt = mean(pct)
  )
}
