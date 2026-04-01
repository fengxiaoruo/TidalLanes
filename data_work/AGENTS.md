# AGENTS

This file is a project-level coordination document for future work.

## Purpose

- Provide a stable place for project-wide working rules
- Keep coding style and workflow guidance consistent across refactor stages
- Allow future additions without changing the overall structure

## Current Guidance

No strict project-specific rules have been added yet.

Default expectations:

- Prefer simple, clear code
- Keep structure consistent across stages
- Avoid unnecessary complexity
- Make pipeline inputs and outputs explicit
- Preserve comparability across versions

## Common Commands

Use these as the default entry points for routine work:

```bash
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --from-stage stage06 --to-stage stage10
python src/stages/run_full_pipeline.py --version-id raw_rebuild_validation --skip-figures
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --figure-group match
python src/stages/stage10_generate_figures.py --version-id raw_rebuild_validation --figure-group grid --grid-type voronoi
```

Notes:

- `outputs/{version_id}` is the working directory for one retained run
- plotting entry points automatically use `outputs/.mpl_cache` and `outputs/.cache`
- prefer stage scripts and `run_full_pipeline.py` over ad hoc notebook execution

## Reserved Sections

These sections are intentionally left open for future use:

- Coding conventions
- Naming conventions
- Data schema rules
- Version comparison rules
- Output directory rules
- Diagnostic requirements
- QSM integration notes
