# Documentation Index

This directory stores project-level reference documents for the current refactor.

Current documents:

- `CODEX_HANDOFF_NOTE.md`
  Compact handoff memo for a new Codex session on another machine.
- `DATASET_SCHEMA_SNAPSHOT.md`
  Intermediate dataset schema snapshot inferred from the existing pipeline.
- `HOW_TO_RERUN_PARTIAL_RESULTS.md`
  Practical guide for manually changing stage parameters and rerunning only the affected outputs or figures.
- `MANUAL_MATCH_OVERRIDE_WORKFLOW.md`
  Short workflow for fixing the remaining unmatched raw segments by hand and rerunning stage02 with a manual override CSV.
- `NEW_MACHINE_SETUP_CHECKLIST.md`
  Environment and verification checklist for moving the project to a new computer.
- `QUICK_RERUN_CHEATSHEET.md`
  Short command lookup table for common parameter edits and partial reruns.
- `REPRODUCIBILITY_SNAPSHOT.md`
  Current working pipeline, required inputs, final outputs, and workflow order.
- `REFACTOR_BLUEPRINT.md`
  Target structure for refactoring the project into stage-based scripts, versioned outputs, and comparison-friendly data products.
- `TEMP_STAGE02_PROJECTION_DEBUG.md`
  Temporary note for tuning only the stage02 projection fallback on the baseline unmatched subset.

Migration status:

- Documentation migration has started.
- Core documentation is now centralized here.
- A first stage-based production chain has been created under `src/stages/`.
- Existing notebooks in `code/` remain important as the baseline source logic during migration.
