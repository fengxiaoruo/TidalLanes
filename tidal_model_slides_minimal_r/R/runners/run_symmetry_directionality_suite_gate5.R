source(file.path("R", "model", "symmetry_suite.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    output_name = "validation_r_symmetry_directionality",
    version_id = "model_input_edgeproj_v2_lcc_gate5_v1",
    sym_method = "geometric",
    top_k = 100L
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("--repo-root", "--output-name", "--version-id", "--sym-method", "--top-k")) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- args[[i + 1L]]
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$top_k <- as.integer(out$top_k)
  out
}

main <- function() {
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  df <- run_symmetry_suite(
    root = root,
    version_id = args$version_id,
    output_name = args$output_name,
    sym_method = args$sym_method,
    top_k = args$top_k
  )
  print(df[, .(
    family, world, mode, scenario,
    Wbar_pct_change,
    od_weighted_bilateral_cost_pct_change,
    residence_dissimilarity_pct,
    workplace_dissimilarity_pct
  )])
}

main()
