source(file.path("R", "model", "core.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_report",
    stage2_output = "validation_r_stage2_full",
    policy_output = "validation_r_policy_comparison",
    tidal_output = "validation_r_tidal_diagnostics",
    sensitivity_output = "validation_r_alpha_beta_sensitivity",
    symmetry_output = "validation_r_symmetry_directionality",
    tolerance = 1e-6
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c(
      "--repo-root", "--output-name", "--stage2-output", "--policy-output",
      "--tidal-output", "--sensitivity-output", "--symmetry-output", "--tolerance"
    )) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- args[[i + 1L]]
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$tolerance <- as.numeric(out$tolerance)
  out
}

collapse_key <- function(dt, keys) {
  if (!length(keys)) return(as.character(dt$.__row_id__))
  apply(dt[, ..keys], 1, paste, collapse = "|")
}

compare_csv <- function(name, ref_path, cand_path, keys, tolerance, converged_only = FALSE) {
  require_pkg("data.table")
  if (!file.exists(ref_path) || !file.exists(cand_path)) {
    return(data.table::data.table(
      table = name,
      status = "missing_file",
      ref_rows = if (file.exists(ref_path)) nrow(data.table::fread(ref_path)) else NA_integer_,
      candidate_rows = if (file.exists(cand_path)) nrow(data.table::fread(cand_path)) else NA_integer_,
      common_rows = NA_integer_,
      max_abs_diff = NA_real_,
      worst_column = NA_character_,
      worst_key = NA_character_
    ))
  }

  ref <- data.table::fread(ref_path)
  cand <- data.table::fread(cand_path)
  if (converged_only && "all_converged" %in% names(ref) && "all_converged" %in% names(cand)) {
    ref <- ref[all_converged == TRUE]
    cand <- cand[all_converged == TRUE]
  }
  keys_ok <- length(keys) && all(keys %in% names(ref)) && all(keys %in% names(cand))
  if (!keys_ok) {
    ref[, .__row_id__ := seq_len(.N)]
    cand[, .__row_id__ := seq_len(.N)]
    keys <- ".__row_id__"
  }

  common_cols <- setdiff(intersect(names(ref), names(cand)), keys)
  numeric_cols <- common_cols[vapply(common_cols, function(nm) {
    (is.numeric(ref[[nm]]) || is.logical(ref[[nm]])) &&
      (is.numeric(cand[[nm]]) || is.logical(cand[[nm]]))
  }, logical(1))]

  merged <- merge(ref, cand, by = keys, suffixes = c("__ref", "__r"))
  if (!nrow(merged) || !length(numeric_cols)) {
    return(data.table::data.table(
      table = name,
      status = "no_common_numeric",
      ref_rows = nrow(ref),
      candidate_rows = nrow(cand),
      common_rows = nrow(merged),
      max_abs_diff = NA_real_,
      worst_column = NA_character_,
      worst_key = NA_character_
    ))
  }

  best <- list(diff = -Inf, col = NA_character_, key = NA_character_)
  for (nm in numeric_cols) {
    rv <- as.numeric(merged[[paste0(nm, "__ref")]])
    cv <- as.numeric(merged[[paste0(nm, "__r")]])
    diff <- abs(rv - cv)
    diff[is.na(diff) & is.na(rv) & is.na(cv)] <- 0
    if (!length(diff) || all(is.na(diff))) next
    idx <- which.max(diff)
    if (is.finite(diff[idx]) && diff[idx] > best$diff) {
      best$diff <- diff[idx]
      best$col <- nm
      best$key <- collapse_key(merged[idx], keys)
    }
  }
  status <- if (is.finite(best$diff) && best$diff <= tolerance) "pass" else "fail"
  data.table::data.table(
    table = name,
    status = status,
    ref_rows = nrow(ref),
    candidate_rows = nrow(cand),
    common_rows = nrow(merged),
    max_abs_diff = best$diff,
    worst_column = best$col,
    worst_key = best$key
  )
}

write_report <- function(summary, out_dir, tolerance) {
  path <- file.path(out_dir, "VALIDATION_REPORT.md")
  md_cell <- function(x) {
    x <- ifelse(is.na(x), "", as.character(x))
    gsub("\\|", "\\\\|", x)
  }
  lines <- c(
    "# R Replica Validation Report",
    "",
    sprintf("Tolerance: `%s`.", format(tolerance, scientific = TRUE)),
    "",
    "This report compares R outputs against the Python reference CSV files copied into `outputs/reference_results`.",
    "",
    "## Table Checks",
    "",
    "| table | status | common rows | max abs diff | worst column | worst key |",
    "|---|---:|---:|---:|---|---|"
  )
  for (ii in seq_len(nrow(summary))) {
    lines <- c(lines, sprintf(
      "| %s | %s | %s | %s | %s | %s |",
      summary$table[ii],
      summary$status[ii],
      ifelse(is.na(summary$common_rows[ii]), "", as.character(summary$common_rows[ii])),
      ifelse(is.na(summary$max_abs_diff[ii]), "", format(summary$max_abs_diff[ii], scientific = TRUE, digits = 6)),
      md_cell(summary$worst_column[ii]),
      md_cell(summary$worst_key[ii])
    ))
  }
  lines <- c(
    lines,
    "",
    "## Interpretation",
    "",
    "A `pass` status means every shared numeric column in the matched rows is within tolerance. Missing files or failed rows identify the next debugging target."
  )
  failed <- summary[status != "pass"]
  if (nrow(failed)) {
    lines <- c(
      lines,
      "",
      "## Failed Rows",
      "",
      "| table | max abs diff | worst column | worst key |",
      "|---|---:|---|---|"
    )
    for (ii in seq_len(nrow(failed))) {
      lines <- c(lines, sprintf(
        "| %s | %s | %s | %s |",
        failed$table[ii],
        ifelse(is.na(failed$max_abs_diff[ii]), "", format(failed$max_abs_diff[ii], scientific = TRUE, digits = 6)),
        md_cell(failed$worst_column[ii]),
        md_cell(failed$worst_key[ii])
      ))
    }
  }
  writeLines(lines, path)
}

main <- function() {
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  ref <- file.path(root, "outputs", "reference_results")
  out_dir <- file.path(root, "outputs", args$output_name)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  items <- list(
    list("structural_cf_endogenous", file.path(ref, "structural_cf_endogenous.csv"), file.path(root, "outputs", args$stage2_output, "counterfactuals", "structural_cf_endogenous.csv"), c("cf", "congestion_mode")),
    list("structural_cf_exogenous", file.path(ref, "structural_cf_exogenous.csv"), file.path(root, "outputs", args$stage2_output, "counterfactuals", "structural_cf_exogenous.csv"), c("cf", "congestion_mode")),
    list("policy_cf_all_modes", file.path(ref, "policy_cf_all_modes.csv"), file.path(root, "outputs", args$stage2_output, "counterfactuals", "policy_cf_all_modes.csv"), c("policy", "congestion_mode")),
    list("tidal_lane_exogenous_bpr_grid", file.path(ref, "tidal_lane_exogenous_bpr_grid.csv"), file.path(root, "outputs", args$stage2_output, "counterfactuals", "tidal_lane_exogenous_bpr_grid.csv"), c("spec_name", "horizon")),
    list("policy_results_by_policy", file.path(ref, "policy_results_by_policy.csv"), file.path(root, "outputs", args$policy_output, "results_by_policy.csv"), c("policy_id", "horizon")),
    list("policy_cost_effectiveness", file.path(ref, "policy_cost_effectiveness.csv"), file.path(root, "outputs", args$policy_output, "cost_effectiveness.csv"), c("policy_id")),
    list("tidal_st_lt_congestion_summary", file.path(ref, "tidal_st_lt_congestion_summary.csv"), file.path(root, "outputs", args$tidal_output, "tidal_st_lt_congestion_summary.csv"), c("spec")),
    list("tidal_st_lt_compact", file.path(ref, "tidal_st_lt_compact.csv"), file.path(root, "outputs", args$tidal_output, "tidal_st_lt_compact.csv"), c("spec")),
    list("alpha_beta_sensitivity_grid_converged_rows", file.path(ref, "alpha_beta_sensitivity_grid.csv"), file.path(root, "outputs", args$sensitivity_output, "sensitivity_grid.csv"), c("spec_label"), TRUE),
    list("alpha_beta_sensitivity_grid_converged", file.path(ref, "alpha_beta_sensitivity_grid_converged.csv"), file.path(root, "outputs", args$sensitivity_output, "sensitivity_grid_converged.csv"), c("spec_label"), FALSE),
    list("symmetry_directionality_summary", file.path(ref, "symmetry_directionality_summary.csv"), file.path(root, "outputs", args$symmetry_output, "summary_by_scenario.csv"), c("family", "world", "mode", "scenario"))
  )
  rows <- lapply(items, function(item) {
    compare_csv(item[[1]], item[[2]], item[[3]], item[[4]], args$tolerance, if (length(item) >= 5) isTRUE(item[[5]]) else FALSE)
  })
  summary <- data.table::rbindlist(rows, fill = TRUE)
  data.table::fwrite(summary, file.path(out_dir, "reference_comparison.csv"))
  write_report(summary, out_dir, args$tolerance)
  print(summary)
  if (any(summary$status %in% c("fail", "missing_file", "no_common_numeric"))) {
    stop("Reference comparison has failing or incomplete checks.", call. = FALSE)
  }
}

main()
