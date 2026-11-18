source(file.path("R", "model", "sensitivity.R"))

alpha_grid <- c(-0.12, 0.00, 0.02, 0.03, 0.05, 0.10)
beta_grid <- c(-0.10, 0.00, 0.05, 0.10)

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_alpha_beta_sensitivity",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    pair_selection = "score_bpr_st",
    top_k = 100L,
    fix_beta = -0.10,
    fix_alpha = 0.05,
    sweep = "both",
    skip_existing = FALSE
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key == "--skip-existing") {
      out$skip_existing <- TRUE
      i <- i + 1L
    } else if (key %in% c(
      "--repo-root", "--output-name", "--version-id", "--pair-selection",
      "--top-k", "--fix-beta", "--fix-alpha", "--sweep"
    )) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- args[[i + 1L]]
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$top_k <- as.integer(out$top_k)
  out$fix_beta <- as.numeric(out$fix_beta)
  out$fix_alpha <- as.numeric(out$fix_alpha)
  out
}

converged_meta_exists <- function(out_dir, label) {
  require_pkg("jsonlite")
  path <- file.path(out_dir, label, "convergence_meta.json")
  if (!file.exists(path)) return(FALSE)
  meta <- tryCatch(jsonlite::read_json(path, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(meta)) return(FALSE)
  isTRUE(meta$baseline$converged) && isTRUE(meta$tidal_st$converged) && isTRUE(meta$tidal_lt$converged)
}

main <- function() {
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  out_dir <- file.path(root, "outputs", args$output_name)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  prim <- load_primitives(root = root, version_id = args$version_id)

  specs <- list()
  if (args$sweep %in% c("alpha", "both")) {
    for (alpha in alpha_grid) specs[[length(specs) + 1L]] <- c(alpha, args$fix_beta)
  }
  if (args$sweep %in% c("beta", "both")) {
    for (beta in beta_grid) {
      if (args$sweep == "both" && abs(beta - args$fix_beta) < 1e-12) next
      specs[[length(specs) + 1L]] <- c(args$fix_alpha, beta)
    }
  }
  if (!length(specs)) stop(sprintf("Unsupported sweep: %s", args$sweep), call. = FALSE)

  rows <- list()
  for (sp in specs) {
    alpha <- sp[1]
    beta <- sp[2]
    label <- sensitivity_spec_label(alpha, beta)
    if (args$skip_existing && converged_meta_exists(out_dir, label)) {
      message(sprintf("Skip converged: %s", label))
      next
    }
    message(sprintf("Sensitivity spec: %s", label))
    params <- params_default(alpha = alpha, beta = beta)
    rows[[length(rows) + 1L]] <- data.table::as.data.table(run_sensitivity_spec(
      prim = prim,
      params = params,
      label = label,
      top_k = args$top_k,
      max_outer = 500L,
      out_dir = out_dir,
      pair_selection = args$pair_selection
    ))
  }

  grid_path <- file.path(out_dir, "sensitivity_grid.csv")
  if (file.exists(grid_path)) {
    prev <- data.table::fread(grid_path)
    if (length(rows)) {
      df <- data.table::rbindlist(c(list(prev), rows), fill = TRUE)
      data.table::setorder(df, spec_label)
      df <- df[!duplicated(spec_label, fromLast = TRUE)]
    } else {
      df <- prev
    }
  } else {
    df <- data.table::rbindlist(rows, fill = TRUE)
  }
  df[, all_converged := baseline_converged & tidal_st_converged & tidal_lt_converged]
  data.table::fwrite(df, grid_path)
  data.table::fwrite(df[all_converged == TRUE], file.path(out_dir, "sensitivity_grid_converged.csv"))
  pos <- df[all_converged == TRUE & Wbar_pct_change_lt > 0]
  if (nrow(pos)) data.table::fwrite(pos, file.path(out_dir, "wbar_lt_positive_candidates.csv"))
  print(df[order(-Wbar_pct_change_lt), .(spec_label, alpha, beta, sorting_wedge_sec, person_min_day_lt_total, Wbar_pct_change_lt)])
}

main()
