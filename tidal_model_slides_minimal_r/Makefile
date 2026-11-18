RSCRIPT ?= Rscript

.PHONY: smoke-model compare-smoke stage2-quick stage2-full policy-long policy-full tidal-diagnostics alpha-beta-sensitivity symmetry-suite compare-reference validate-full clean-outputs

smoke-model:
	$(RSCRIPT) R/runners/run_smoke_model.R

compare-smoke:
	$(RSCRIPT) R/scripts/compare_smoke.R

stage2-quick:
	$(RSCRIPT) R/runners/run_stage2_gate5.R --output-name validation_r_stage2_quick --quick --skip-tidal-grid --max-outer 80 --tol 1e-5

stage2-full:
	$(RSCRIPT) R/runners/run_stage2_gate5.R --output-name validation_r_stage2_full --max-outer 500 --tol 1e-4

policy-long:
	$(RSCRIPT) R/runners/run_policy_comparison_gate5.R --output-name validation_r_policy_comparison_long --horizons long_run

policy-full:
	$(RSCRIPT) R/runners/run_policy_comparison_gate5.R --output-name validation_r_policy_comparison --horizons short_run,long_run

tidal-diagnostics:
	$(RSCRIPT) R/runners/run_tidal_lane_diagnostics_gate5.R --output-name validation_r_tidal_diagnostics

alpha-beta-sensitivity:
	$(RSCRIPT) R/runners/run_alpha_beta_sensitivity_bpr_gate5.R --output-name validation_r_alpha_beta_sensitivity

symmetry-suite:
	$(RSCRIPT) R/runners/run_symmetry_directionality_suite_gate5.R --output-name validation_r_symmetry_directionality

compare-reference:
	$(RSCRIPT) R/scripts/compare_reference_results.R

validate-full: stage2-full policy-full tidal-diagnostics alpha-beta-sensitivity symmetry-suite compare-reference

clean-outputs:
	rm -rf outputs/smoke_model outputs/validation_r_*
