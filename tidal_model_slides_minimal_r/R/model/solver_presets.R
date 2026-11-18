source(file.path("R", "model", "core.R"))

solver_overrides_for_params <- function(params, max_outer = NULL) {
  if (params$alpha > 0) {
    out <- list(max_outer = 500L, damping = 0.10, tol = 1e-4)
  } else {
    out <- list(max_outer = 120L, damping = 0.25, tol = 1e-5)
  }
  if (!is.null(max_outer)) {
    out$max_outer <- max(as.integer(max_outer), out$max_outer)
  }
  out
}
