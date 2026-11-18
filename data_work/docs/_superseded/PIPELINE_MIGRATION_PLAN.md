# Pipeline Migration Plan

This document maps the current notebook-based workflow to the target refactored stage-based structure.

## Purpose

- Preserve the current pipeline logic while moving toward a script-first structure
- Clarify which notebooks are production logic versus diagnostics or exploratory analysis
- Make future algorithm changes easier to isolate and compare

## Migration Principles

1. Freeze current behavior before changing logic.
2. Move production logic to stage scripts first.
3. Keep diagnostics and visualization in notebooks where useful.
4. Replace row deletion with explicit flags.
5. Preserve baseline and upgraded methods side by side where possible.

## Current Notebook to Target Stage Mapping

### `code/01_Match_Final.ipynb`

Current role:

- Build centerline from raw roads
- Construct directed centerline geometry
- Split raw roads
- Match split segments to directed centerlines
- Export crosswalks and matching diagnostics

Target split:

- `src/stages/stage01_build_centerline.py`
- `src/stages/stage02_match_raw_to_centerline.py`
- diagnostics move to:
  - `src/diagnostics/match_diagnostics.py`
  - `notebooks/01_match_diagnostics.ipynb`

Migration notes:

- Separate geometry construction from matching logic
- Preserve current matching method as baseline
- Convert current pruning/removal logic into flags

### `code/02_Centerline_Match_Speed.ipynb`

Current role:

- Read raw speed observations
- Match speed records to directed centerlines through split-road crosswalks
- Aggregate centerline speed by time
- Export centerline speed panel and speed summary outputs

Target split:

- `src/stages/stage03_attach_speed.py`
- diagnostics move to:
  - `src/diagnostics/speed_diagnostics.py`
  - `notebooks/02_speed_diagnostics.ipynb`

Migration notes:

- Keep time aggregation logic in the stage script
- Move plots and descriptive summaries out of the production stage

### `code/02_2_DefineAsymmetricCenterliens.ipynb`

Current role:

- Compute AM/PM directional asymmetry at centerline level
- Identify candidate tidal-lane segments

Target split:

- Optional production stage:
  - `src/stages/stage03b_centerline_asymmetry.py`
- or diagnostics path:
  - `src/diagnostics/centerline_asymmetry.py`
  - `notebooks/01_match_diagnostics.ipynb`

Migration notes:

- Keep only if centerline-level asymmetry remains a maintained analysis branch
- Otherwise archive as a secondary analysis endpoint

### `code/03_GridConstruct.ipynb`

Current role:

- Construct square, hex, and Voronoi grid systems
- Compare their coverage and geometry

Target split:

- `src/stages/stage05_build_grids.py`
- diagnostics move to:
  - `src/diagnostics/grid_diagnostics.py`
  - `notebooks/03_grid_diagnostics.ipynb`

Migration notes:

- Keep output schema stable across all grid systems
- Keep comparison tables and figures outside the production stage

### `code/04_GirdlevelData.ipynb`

Current role:

- Merge centerline geometry, centerline speed, grids, and commute data
- Build grid links
- Build graph inputs
- Match commute data to grids
- Build OD and population summaries
- Generate multiple figures

Target split:

- `src/stages/stage06_build_grid_links.py`
- `src/stages/stage07_build_od_and_population.py`
- `src/stages/stage08_build_qsm_inputs.py`
- diagnostics move to:
  - `src/diagnostics/grid_diagnostics.py`
  - `src/diagnostics/qsm_diagnostics.py`
  - `notebooks/03_grid_diagnostics.ipynb`
  - `notebooks/05_qsm_results.ipynb`

Migration notes:

- Separate data generation from plotting
- Preserve alternative travel-time definitions in output tables
- Make QSM input export a distinct stage

## Exploratory and Archive Mapping

### Keep as exploratory only

- `code/00_1_Data_description_0911.ipynb`
- `code/00_2_graph_asymetric_flows_commutingflow.ipynb`

Recommended future location:

- `notebooks/exploratory/`
  or
- `archive/notebooks/`

### Legacy archive

- `code/archive/*`
- `code/.ipynb_checkpoints/*`

Recommended future location:

- keep as-is for now
- later move to a dedicated `archive/legacy_code/` structure if needed

## Recommended Migration Order

### Phase 1: Structure freeze

- Create target directories
- Freeze snapshots and baseline diagnostics
- Add project-level documentation and `AGENTS.md`

### Phase 2: Move production logic

- Extract centerline build logic into stage scripts
- Extract matching logic into stage scripts
- Extract speed aggregation into stage scripts
- Extract grid construction into stage scripts

### Phase 3: Normalize outputs

- Introduce stable master-table schemas
- Replace row dropping with flags
- Introduce versioned output directories

### Phase 4: Upgrade methods

- Add projection-based matching fallback
- Add lane estimation
- Fix travel-time aggregation
- Add QSM export stage

### Phase 5: Comparison workflow

- Add version comparison metrics
- Add comparison report generation
- Keep notebooks only for diagnostics and interpretation

## Immediate Next Refactor Targets

The initial production files have now been created:

1. `src/stages/stage01_build_centerline.py`
2. `src/stages/stage02_match_raw_to_centerline.py`
3. `src/stages/stage03_attach_speed.py`

Additional stage files now exist for:

4. `src/stages/stage05_build_grids.py`
5. `src/stages/stage06_build_grid_links.py`
6. `src/stages/stage07_build_od_and_population.py`

These stages now provide the backbone of the migrated production chain. Remaining work is concentrated in:

- lane estimation
- centerline asymmetry branch migration
- QSM export
- version comparison
