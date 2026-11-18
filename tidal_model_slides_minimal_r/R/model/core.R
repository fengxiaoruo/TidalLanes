EPS <- 1e-12

params_default <- function(alpha = 0.05, beta = -0.10) {
  list(
    theta = 6.83,
    alpha = alpha,
    beta = beta,
    lambda_phys = 1.1,
    lambda_legacy = 0.488 / 6.83
  )
}

require_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Required R package is missing: %s", pkg), call. = FALSE)
  }
}

read_parquet_dt <- function(path) {
  require_pkg("nanoparquet")
  data.table::as.data.table(nanoparquet::read_parquet(path))
}

safe_numeric <- function(x) {
  suppressWarnings(as.numeric(x))
}

load_primitives <- function(root,
                            version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
                            grid_type = "square",
                            period = "AM",
                            od_pop_col = "commuters_road") {
  require_pkg("data.table")
  data_dir <- file.path(root, "outputs", version_id, "data")
  nodes <- read_parquet_dt(file.path(data_dir, sprintf("qsm_input_nodes_%s.parquet", grid_type)))
  edges <- read_parquet_dt(file.path(data_dir, sprintf("qsm_input_edges_%s.parquet", grid_type)))
  od <- read_parquet_dt(file.path(data_dir, sprintf("qsm_input_od_%s.parquet", grid_type)))

  nodes[, node_i := as.integer(node_i)]
  if ("keep_for_model" %in% names(nodes)) {
    nodes <- nodes[keep_for_model == 1]
  }
  data.table::setorder(nodes, node_i)
  n_nodes <- nrow(nodes)
  raw_nodes <- nodes$node_i
  dense <- seq_len(n_nodes)

  map_node <- function(x) dense[match(as.integer(x), raw_nodes)]

  period_arg <- period
  if ("period" %in% names(edges)) {
    edges <- edges[toupper(as.character(period)) == toupper(period_arg)]
  }
  edges[, i_new := map_node(i)]
  edges[, j_new := map_node(j)]
  edges <- edges[!is.na(i_new) & !is.na(j_new)]

  tau_obs <- safe_numeric(edges$tau_obs_min)
  tau_ff <- safe_numeric(edges$tau_ff_min)
  keep_edge <- is.finite(tau_obs) & tau_obs > 0 & is.finite(tau_ff) & tau_ff > 0
  edges <- edges[keep_edge]
  tau_obs <- tau_obs[keep_edge]
  tau_ff <- tau_ff[keep_edge]

  lanes <- safe_numeric(edges$lanes_directional)
  if (any(!is.finite(lanes) | lanes <= 0)) {
    fb <- if ("lane_est_length_weighted" %in% names(edges)) {
      safe_numeric(edges$lane_est_length_weighted)
    } else {
      rep(NA_real_, length(lanes))
    }
    lanes <- ifelse(is.finite(lanes) & lanes > 0, lanes, fb)
  }
  lane_mean <- mean(lanes[is.finite(lanes) & lanes > 0])
  lanes <- ifelse(is.finite(lanes) & lanes > 0, lanes, lane_mean)

  ell_km <- safe_numeric(edges$total_link_len_m) / 1000
  edge_i <- as.integer(edges$i_new)
  edge_j <- as.integer(edges$j_new)

  origin_col <- if ("origin_node" %in% names(od)) "origin_node" else "home_i"
  dest_col <- if ("destination_node" %in% names(od)) "destination_node" else "work_i"
  if (!(od_pop_col %in% names(od))) {
    stop(sprintf("OD population column missing: %s", od_pop_col), call. = FALSE)
  }
  o_i <- map_node(od[[origin_col]])
  o_j <- map_node(od[[dest_col]])
  o_pop <- safe_numeric(od[[od_pop_col]])
  ok_od <- !is.na(o_i) & !is.na(o_j) & is.finite(o_pop)
  o_i <- as.integer(o_i[ok_od])
  o_j <- as.integer(o_j[ok_od])
  o_pop <- o_pop[ok_od]

  L_ij <- matrix(0, n_nodes, n_nodes)
  L_ij[cbind(o_i, o_j)] <- L_ij[cbind(o_i, o_j)] + o_pop
  L_R <- rowSums(L_ij)
  L_F <- colSums(L_ij)

  list(
    version_id = version_id,
    grid_type = grid_type,
    period = period,
    node_grid_ids = as.character(nodes$grid_id),
    edge_i = edge_i,
    edge_j = edge_j,
    ell_kl_km = ell_km,
    v0_kl = ell_km / tau_ff,
    v_obs_kl = ell_km / tau_obs,
    tau_ff_min = tau_ff,
    tau_obs_min = tau_obs,
    n_kl = lanes,
    L_R_obs = L_R,
    L_F_obs = L_F,
    L_ij_obs = L_ij,
    Lbar = sum(L_R),
    n_nodes = n_nodes,
    n_edges = length(edge_i)
  )
}

write_json <- function(x, path) {
  require_pkg("jsonlite")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(x, path, pretty = TRUE, auto_unbox = TRUE, digits = NA)
}

write_primitives_summary <- function(prim, out_dir) {
  require_pkg("data.table")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  write_json(
    list(
      version_id = prim$version_id,
      grid_type = prim$grid_type,
      period = prim$period,
      n_nodes = prim$n_nodes,
      n_edges = prim$n_edges,
      Lbar = prim$Lbar
    ),
    file.path(out_dir, "primitives_meta.json")
  )
  data.table::fwrite(
    data.table::data.table(
      node_idx = seq_len(prim$n_nodes) - 1L,
      grid_id = prim$node_grid_ids,
      L_R_obs = prim$L_R_obs,
      L_F_obs = prim$L_F_obs
    ),
    file.path(out_dir, "primitives_nodes.csv")
  )
  data.table::fwrite(
    data.table::data.table(
      edge_i = prim$edge_i - 1L,
      edge_j = prim$edge_j - 1L,
      ell_kl_km = prim$ell_kl_km,
      tau_ff_min = prim$tau_ff_min,
      tau_obs_min = prim$tau_obs_min,
      n_kl = prim$n_kl
    ),
    file.path(out_dir, "primitives_edges.csv")
  )
}

gmean_normalize <- function(x, mask = NULL, floor = 1e-30) {
  x <- pmax(as.numeric(x), floor)
  sel <- if (is.null(mask)) x else x[mask]
  g <- exp(mean(log(sel)))
  if (!is.finite(g) || g <= 0) g <- 1.0
  list(x = x / g, g = g)
}

pick_kappa <- function(edge_i, edge_j, tau_min_kl, n_nodes, target_rowsum = 0.9) {
  raw <- numeric(n_nodes)
  inv <- 1 / pmax(tau_min_kl, EPS)
  raw <- rowsum(inv, edge_i, reorder = FALSE)
  max_rs <- max(raw)
  max(1.0, max_rs / target_rowsum)
}

build_A_matrix <- function(prim, tau_min_kl, kappa = 1.0) {
  A <- matrix(0, prim$n_nodes, prim$n_nodes)
  idx <- cbind(prim$edge_i, prim$edge_j)
  A[idx] <- A[idx] + 1 / pmax(kappa * tau_min_kl, EPS)
  A
}

compute_tau_inv_theta <- function(A) {
  n <- nrow(A)
  X <- solve(diag(n) - A)
  pmax(X, EPS)
}

invert_fundamentals <- function(prim,
                                params,
                                max_iter = 1800,
                                tol = 1.5e-4,
                                floor_share = 1e-6,
                                kappa = 1.0,
                                kappa_target_rowsum = 0.9,
                                tau_min_kl = NULL) {
  theta <- params$theta
  alpha <- params$alpha
  beta <- params$beta
  n <- prim$n_nodes
  tau_edge <- if (is.null(tau_min_kl)) prim$tau_obs_min else tau_min_kl
  if (is.null(kappa)) {
    kappa <- pick_kappa(prim$edge_i, prim$edge_j, tau_edge, n, kappa_target_rowsum)
  }
  A <- build_A_matrix(prim, tau_edge, kappa)
  tau_inv <- compute_tau_inv_theta(A)

  L_R <- pmax(prim$L_R_obs, floor_share)
  L_F <- pmax(prim$L_F_obs, floor_share)
  l_R <- L_R / sum(L_R)
  l_F <- L_F / sum(L_F)
  valid <- prim$L_R_obs > 0 & prim$L_F_obs > 0

  v <- rep(1.0, n)
  w <- rep(1.0, n)
  p_R <- 1.0 - beta * theta
  p_F <- 1.0 - alpha * theta
  q_R <- beta * theta
  q_F <- alpha * theta
  last_delta <- Inf
  converged <- FALSE

  for (it in seq_len(max_iter)) {
    a <- as.numeric(tau_inv %*% (w * (l_F ^ q_F)))
    v_new <- (l_R ^ p_R) / pmax(a, EPS)
    v_new <- gmean_normalize(v_new, valid)$x

    b <- as.numeric(t(tau_inv) %*% (v_new * (l_R ^ q_R)))
    w_new <- (l_F ^ p_F) / pmax(b, EPS)
    w_new <- gmean_normalize(w_new, valid)$x

    a_chk <- as.numeric(tau_inv %*% (w_new * (l_F ^ q_F)))
    b_chk <- as.numeric(t(tau_inv) %*% (v_new * (l_R ^ q_R)))
    chi_R <- (l_R ^ p_R) / pmax(v_new * a_chk, EPS)
    chi_F <- (l_F ^ p_F) / pmax(w_new * b_chk, EPS)
    last_delta <- max(stats::sd(log(pmax(chi_R[valid], EPS))),
                      stats::sd(log(pmax(chi_F[valid], EPS))))
    v <- v_new
    w <- w_new
    if (last_delta < tol) {
      converged <- TRUE
      break
    }
  }

  a <- as.numeric(tau_inv %*% (w * (l_F ^ q_F)))
  b <- as.numeric(t(tau_inv) %*% (v * (l_R ^ q_R)))
  chi_R <- (l_R ^ p_R) / pmax(v * a, EPS)
  chi_F <- (l_F ^ p_F) / pmax(w * b, EPS)
  chi <- exp(0.5 * (mean(log(pmax(chi_R[valid], EPS))) +
                      mean(log(pmax(chi_F[valid], EPS)))))

  l_R_pred <- (chi * v * a) ^ (1.0 / p_R)
  l_F_pred <- (chi * w * b) ^ (1.0 / p_F)
  l_R_pred <- l_R_pred / sum(l_R_pred)
  l_F_pred <- l_F_pred / sum(l_F_pred)
  resid_R <- numeric(n)
  resid_F <- numeric(n)
  resid_R[valid] <- log(pmax(l_R_pred[valid], EPS)) - log(pmax(l_R[valid], EPS))
  resid_F[valid] <- log(pmax(l_F_pred[valid], EPS)) - log(pmax(l_F[valid], EPS))

  list(
    ubar_theta = v,
    abar_theta = w,
    chi = chi,
    kappa = kappa,
    tau_obs_inv_theta = tau_inv,
    n_iter = it,
    converged = converged,
    max_log_change = last_delta,
    resid_R = resid_R,
    resid_F = resid_F
  )
}

write_fundamentals <- function(fund, out_dir) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(
    list(
      ubar_theta = fund$ubar_theta,
      abar_theta = fund$abar_theta,
      resid_R = fund$resid_R,
      resid_F = fund$resid_F
    ),
    file.path(out_dir, "fundamentals_arrays.rds")
  )
  write_json(
    list(
      chi = fund$chi,
      kappa = fund$kappa,
      n_iter = fund$n_iter,
      converged = fund$converged,
      max_log_change = fund$max_log_change
    ),
    file.path(out_dir, "fundamentals_meta.json")
  )
}

resolve_lanes <- function(n_kl) {
  fallback <- stats::median(n_kl[is.finite(n_kl) & n_kl > 0])
  out <- ifelse(is.finite(n_kl) & n_kl > 0, n_kl, fallback)
  pmax(out, 1.0)
}

edge_lengths_km <- function(prim) {
  ell <- prim$ell_kl_km
  med <- stats::median(ell[is.finite(ell) & ell > 0])
  pmax(ifelse(is.finite(ell) & ell > 0, ell, med), EPS)
}

solve_lR_lF_given_tau <- function(tau_inv_theta,
                                  ubar_theta,
                                  abar_theta,
                                  params,
                                  chi = 1.0,
                                  max_iter = 400,
                                  tol = 1e-8) {
  theta <- params$theta
  alpha <- params$alpha
  beta <- params$beta
  n <- nrow(tau_inv_theta)
  tau_inv_t <- t(tau_inv_theta)
  l_R <- rep(1 / n, n)
  l_F <- rep(1 / n, n)
  for (it in seq_len(max_iter)) {
    rhs_R <- chi * ubar_theta * as.numeric(tau_inv_theta %*% (abar_theta * (l_F ^ (alpha * theta))))
    l_R_new <- pmax(rhs_R, EPS) ^ (1.0 / (1.0 - beta * theta))
    l_R_new <- l_R_new / sum(l_R_new)

    rhs_F <- chi * abar_theta * as.numeric(tau_inv_t %*% (ubar_theta * (l_R_new ^ (beta * theta))))
    l_F_new <- pmax(rhs_F, EPS) ^ (1.0 / (1.0 - alpha * theta))
    l_F_new <- l_F_new / sum(l_F_new)

    delta <- max(max(abs(log(pmax(l_R_new, EPS)) - log(pmax(l_R, EPS)))),
                 max(abs(log(pmax(l_F_new, EPS)) - log(pmax(l_F, EPS)))))
    l_R <- l_R_new
    l_F <- l_F_new
    if (delta < tol) {
      return(list(l_R = l_R, l_F = l_F, n_iter = it))
    }
  }
  list(l_R = l_R, l_F = l_F, n_iter = max_iter)
}

market_access <- function(tau_inv_theta,
                          l_R,
                          l_F,
                          Lbar,
                          theta,
                          max_iter = 400,
                          tol = 1e-9) {
  L_R <- l_R * Lbar
  L_F <- l_F * Lbar
  n <- length(l_R)
  tau_inv_t <- t(tau_inv_theta)
  Pi <- rep(1.0, n)
  Pj <- rep(1.0, n)
  for (it in seq_len(max_iter)) {
    Pi_inv_theta <- as.numeric(tau_inv_theta %*% (L_F * (Pj ^ theta)))
    Pi_new <- pmax(Pi_inv_theta, EPS) ^ (-1.0 / theta)
    Pj_inv_theta <- as.numeric(tau_inv_t %*% (L_R * (Pi_new ^ theta)))
    Pj_new <- pmax(Pj_inv_theta, EPS) ^ (-1.0 / theta)
    delta <- max(max(abs(log(Pi_new / pmax(Pi, EPS)))),
                 max(abs(log(Pj_new / pmax(Pj, EPS)))))
    Pi <- Pi_new
    Pj <- Pj_new
    if (delta < tol) break
  }
  list(Pi_i = Pi, P_j = Pj)
}

congestion_tau_update <- function(form,
                                  lam,
                                  tau_ff,
                                  tau_obs,
                                  Xi_kl,
                                  n_kl,
                                  ell_km,
                                  xi_obs_kl = NULL,
                                  n_obs_kl = NULL) {
  if (form == "legacy") {
    ratio <- pmax(Xi_kl / pmax(n_kl, EPS), EPS)
    return(pmax(tau_ff, tau_ff * ratio ^ lam))
  }
  if (form == "density") {
    density <- Xi_kl / pmax(n_kl * ell_km, EPS)
    return(pmax(tau_ff, tau_ff * pmax(density, EPS) ^ lam))
  }
  if (form == "anchored") {
    if (is.null(xi_obs_kl) || is.null(n_obs_kl)) {
      stop("anchored congestion requires xi_obs_kl and n_obs_kl", call. = FALSE)
    }
    dev <- pmax(Xi_kl / pmax(xi_obs_kl, EPS), EPS) * pmax(n_obs_kl / pmax(n_kl, EPS), EPS)
    return(pmax(tau_ff, tau_obs * dev ^ lam))
  }
  stop(sprintf("unknown congestion form: %s", form), call. = FALSE)
}

solve_equilibrium <- function(prim,
                              ubar_theta,
                              abar_theta,
                              params,
                              n_kl_override = NULL,
                              tau_ff_override = NULL,
                              tau_min_init = NULL,
                              chi = 1.0,
                              damping = 0.5,
                              max_outer = 60,
                              tol = 1e-5,
                              kappa = NULL,
                              kappa_target_rowsum = 0.9,
                              endogenous_congestion = TRUE,
                              label = "equilibrium",
                              l_R_fixed = NULL,
                              l_F_fixed = NULL,
                              congestion_form = "legacy",
                              congestion_lambda = NULL,
                              xi_obs_kl = NULL,
                              n_obs_kl = NULL,
                              tau_obs_anchor = NULL) {
  theta <- params$theta
  lam <- if (is.null(congestion_lambda)) params$lambda_legacy else congestion_lambda
  n_kl <- resolve_lanes(if (is.null(n_kl_override)) prim$n_kl else n_kl_override)
  tau_ff <- if (is.null(tau_ff_override)) prim$tau_ff_min else tau_ff_override
  tau_obs <- if (is.null(tau_obs_anchor)) prim$tau_obs_min else tau_obs_anchor
  tau_min <- if (is.null(tau_min_init)) tau_ff else as.numeric(tau_min_init)
  ell_km <- edge_lengths_km(prim)
  if (is.null(kappa)) {
    kappa <- pick_kappa(prim$edge_i, prim$edge_j, tau_min, prim$n_nodes, kappa_target_rowsum)
  }

  pin_shares <- !is.null(l_R_fixed) && !is.null(l_F_fixed)
  if (pin_shares) {
    l_R_pin <- pmax(as.numeric(l_R_fixed), EPS)
    l_F_pin <- pmax(as.numeric(l_F_fixed), EPS)
    l_R_pin <- l_R_pin / sum(l_R_pin)
    l_F_pin <- l_F_pin / sum(l_F_pin)
  }

  converged <- FALSE
  last_delta <- Inf
  outer_it <- 0L
  for (outer_it in seq_len(max_outer)) {
    A <- build_A_matrix(prim, tau_min, kappa)
    tau_inv <- compute_tau_inv_theta(A)

    if (pin_shares) {
      l_R <- l_R_pin
      l_F <- l_F_pin
    } else {
      shares <- solve_lR_lF_given_tau(tau_inv, ubar_theta, abar_theta, params, chi)
      l_R <- shares$l_R
      l_F <- shares$l_F
    }
    ma <- market_access(tau_inv, l_R, l_F, prim$Lbar, theta)
    Pi <- ma$Pi_i
    Pj <- ma$P_j

    t_kl <- tau_min ^ (1.0 / theta)
    Xi_kl <- (t_kl ^ (-theta)) * (Pj[prim$edge_i] ^ (-theta)) * (Pi[prim$edge_j] ^ (-theta))

    if (!endogenous_congestion) {
      converged <- TRUE
      last_delta <- 0.0
      break
    }

    tau_new <- congestion_tau_update(
      form = congestion_form,
      lam = lam,
      tau_ff = tau_ff,
      tau_obs = tau_obs,
      Xi_kl = Xi_kl,
      n_kl = n_kl,
      ell_km = ell_km,
      xi_obs_kl = xi_obs_kl,
      n_obs_kl = n_obs_kl
    )
    log_old <- log(pmax(tau_min, EPS))
    log_new <- log(pmax(tau_new, EPS))
    tau_damped <- exp(damping * log_new + (1 - damping) * log_old)
    last_delta <- max(abs(log(tau_damped) - log_old))
    tau_min <- tau_damped
    if (last_delta < tol) {
      converged <- TRUE
      break
    }
  }

  A <- build_A_matrix(prim, tau_min, kappa)
  tau_inv <- compute_tau_inv_theta(A)
  if (pin_shares) {
    l_R <- l_R_pin
    l_F <- l_F_pin
  } else {
    shares <- solve_lR_lF_given_tau(tau_inv, ubar_theta, abar_theta, params, chi)
    l_R <- shares$l_R
    l_F <- shares$l_F
  }
  ma <- market_access(tau_inv, l_R, l_F, prim$Lbar, theta)
  Pi <- ma$Pi_i
  Pj <- ma$P_j
  t_kl <- tau_min ^ (1.0 / theta)
  Xi_kl <- (t_kl ^ (-theta)) * (Pj[prim$edge_i] ^ (-theta)) * (Pi[prim$edge_j] ^ (-theta))

  L_R <- l_R * prim$Lbar
  L_F <- l_F * prim$Lbar
  u_theta <- ubar_theta * (pmax(L_R, EPS) ^ (params$beta * theta))
  w_theta <- abar_theta * (pmax(L_F, EPS) ^ (params$alpha * theta))
  Wbar_theta <- sum(tau_inv * tcrossprod(u_theta, w_theta))
  Wbar <- if (Wbar_theta > 0) Wbar_theta ^ (1.0 / theta) else NA_real_
  Wbar_pi_theta <- sum(u_theta * (pmax(Pi, EPS) ^ (-theta)))
  Wbar_pi_raw <- if (Wbar_pi_theta > 0) Wbar_pi_theta ^ (1.0 / theta) else NA_real_

  list(
    label = label,
    tau_min_kl = tau_min,
    t_kl_iceberg = t_kl,
    Xi_kl = Xi_kl,
    tau_ij_inv_theta = tau_inv,
    l_R = l_R,
    l_F = l_F,
    Pi_i = Pi,
    P_j = Pj,
    Wbar = Wbar,
    Wbar_pi_raw = Wbar_pi_raw,
    n_outer = outer_it,
    converged = converged,
    max_log_change = last_delta,
    endogenous_congestion = endogenous_congestion,
    kappa = kappa,
    congestion_form = congestion_form,
    congestion_lambda = lam
  )
}

write_equilibrium <- function(eq, out_dir) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  saveRDS(
    list(
      tau_min_kl = eq$tau_min_kl,
      t_kl_iceberg = eq$t_kl_iceberg,
      Xi_kl = eq$Xi_kl,
      tau_ij_inv_theta = eq$tau_ij_inv_theta,
      l_R = eq$l_R,
      l_F = eq$l_F,
      Pi_i = eq$Pi_i,
      P_j = eq$P_j
    ),
    file.path(out_dir, "equilibrium_arrays.rds")
  )
  write_json(
    list(
      label = eq$label,
      Wbar = eq$Wbar,
      Wbar_pi_raw = eq$Wbar_pi_raw,
      n_outer = eq$n_outer,
      converged = eq$converged,
      max_log_change = eq$max_log_change,
      endogenous_congestion = eq$endogenous_congestion,
      kappa = eq$kappa,
      congestion_form = eq$congestion_form,
      congestion_lambda = eq$congestion_lambda
    ),
    file.path(out_dir, "equilibrium_meta.json")
  )
}

compute_observation_anchors <- function(prim, fund, params) {
  eq <- solve_equilibrium(
    prim = prim,
    ubar_theta = fund$ubar_theta,
    abar_theta = fund$abar_theta,
    params = params,
    tau_min_init = prim$tau_obs_min,
    endogenous_congestion = FALSE,
    chi = fund$chi,
    kappa = fund$kappa,
    label = "observation_anchor"
  )
  list(
    xi_obs_kl = eq$Xi_kl,
    n_obs_kl = resolve_lanes(prim$n_kl),
    tau_obs_anchor = prim$tau_obs_min,
    l_R_obs_model = eq$l_R,
    l_F_obs_model = eq$l_F
  )
}

build_congestion_solver_kwargs <- function(prim,
                                           fund,
                                           params,
                                           congestion_form = "anchored",
                                           congestion_lambda = 1.1,
                                           max_outer = 500,
                                           tol = 1e-4,
                                           damping = 0.10) {
  kw <- list(
    chi = fund$chi,
    kappa = fund$kappa,
    max_outer = max_outer,
    tol = tol,
    damping = damping,
    congestion_form = congestion_form,
    congestion_lambda = congestion_lambda
  )
  if (congestion_form == "anchored") {
    anchors <- compute_observation_anchors(prim, fund, params)
    kw$xi_obs_kl <- anchors$xi_obs_kl
    kw$n_obs_kl <- anchors$n_obs_kl
    kw$tau_obs_anchor <- anchors$tau_obs_anchor
  }
  kw
}

do.call_solve_equilibrium <- function(prim, fund, params, label, endogenous, kw, extra = list()) {
  args <- list(
    prim = prim,
    ubar_theta = fund$ubar_theta,
    abar_theta = fund$abar_theta,
    params = params,
    tau_min_init = prim$tau_obs_min,
    endogenous_congestion = endogenous,
    label = label
  )
  if (isTRUE(extra$tau_min_init_ff)) {
    args$tau_min_init <- NULL
    extra$tau_min_init_ff <- NULL
  }
  for (nm in names(extra)) args[[nm]] <- extra[[nm]]
  for (nm in names(kw)) args[[nm]] <- kw[[nm]]
  do.call(solve_equilibrium, args)
}
