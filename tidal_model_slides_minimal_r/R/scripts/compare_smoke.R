source(file.path("R", "model", "core.R"))

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  out <- list(
    repo_root = ".",
    python_mini = "../tidal_model_slides_minimal",
    output_name = "smoke_model",
    tolerance = 1e-6
  )
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (key %in% c("--repo-root", "--python-mini", "--output-name", "--tolerance")) {
      if (i == length(args)) stop(sprintf("Missing value for %s", key), call. = FALSE)
      val <- args[[i + 1L]]
      nm <- gsub("-", "_", sub("^--", "", key))
      out[[nm]] <- val
      i <- i + 2L
    } else {
      stop(sprintf("Unknown argument: %s", key), call. = FALSE)
    }
  }
  out$tolerance <- as.numeric(out$tolerance)
  out
}

get_nested <- function(x, path) {
  cur <- x
  for (key in strsplit(path, ".", fixed = TRUE)[[1]]) {
    cur <- cur[[key]]
  }
  cur
}

main <- function() {
  require_pkg("jsonlite")
  require_pkg("data.table")
  args <- parse_args()
  root <- normalizePath(args$repo_root, mustWork = TRUE)
  py <- normalizePath(args$python_mini, mustWork = TRUE)
  r_path <- file.path(root, "outputs", args$output_name, "baseline_summary.json")
  p_path <- file.path(py, "outputs", "smoke_model", "baseline_summary.json")
  r <- jsonlite::read_json(r_path, simplifyVector = TRUE)
  p <- jsonlite::read_json(p_path, simplifyVector = TRUE)
  checks <- c(
    "baseline_endogenous.Wbar",
    "baseline_endogenous.median_tau_ratio_vs_obs",
    "baseline_endogenous.mean_tau_vs_obs_pct",
    "baseline_endogenous.n_outer"
  )
  rows <- lapply(checks, function(item) {
    rv <- as.numeric(get_nested(r, item))
    pv <- as.numeric(get_nested(p, item))
    data.table::data.table(
      metric = item,
      r_value = rv,
      python_value = pv,
      abs_diff = abs(rv - pv)
    )
  })
  tab <- data.table::rbindlist(rows)
  out_path <- file.path(root, "outputs", args$output_name, "smoke_compare_python.csv")
  data.table::fwrite(tab, out_path)
  print(tab)
  worst <- max(tab$abs_diff, na.rm = TRUE)
  if (worst > args$tolerance) {
    stop(sprintf("Smoke comparison exceeds tolerance: %.12g", worst), call. = FALSE)
  }
  message(sprintf("smoke comparison ok: max_abs_diff=%.12g", worst))
}

main()

