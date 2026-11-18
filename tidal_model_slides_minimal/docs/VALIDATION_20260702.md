# Validation Record, 2026-07-02

This package was synchronized from `/Users/fxr/Dropbox/Projects/TidalLanes` and validated as a standalone slide and model replication unit.

## Commands

```bash
make sync-from-main MAIN=/Users/fxr/Dropbox/Projects/TidalLanes
make check
make slides
make smoke-model
make validate-full
python scripts/validate_full_model.py --compare-only
```

Visual QA used:

```bash
pdftoppm -png -r 90 tidal.pdf outputs/qa_render/pages/slide
```

## Result Checks

- `make check` resolved 23 slide graphics and one run-linked video asset, then verified the aggregate model inputs.
- The descriptive metric checker verified the jobs-housing Duncan statistic, radial inbound statistic, national AM asymmetry statistic, Shenzhen grid-level flow exposure statistic, and Beijing centerline asymmetry statistic from package-local reference files.
- `make slides` compiled `tidal.pdf` to 42 pages from package-local paths.
- `make smoke-model` ran the fixed-input model smoke test and converged with `Wbar=3.200092` and `median_tau_obs_ratio=1.0001`.
- `make validate-full` reran Stage 2, tidal diagnostics, policy comparison, alpha-beta sensitivity, and the symmetry-to-directionality suite inside the package.
- `python scripts/validate_full_model.py --compare-only` matched every checked model table with maximum absolute difference `0`.
- Visual QA rendered a 42-page contact sheet at `outputs/qa_render/contact_sheet.png`; no missing images, blank pages, or obvious slide-level overlap were detected.

## Headline Reproduced Values

- Tidal top100: ST welfare `+0.871400%`; LT welfare `+0.324315%`.
- Policy comparison, LT welfare: P1 tidal `+0.324315%`; P2 same-corridor expansion `+0.381536%`; P3 qdelay expansion `+0.444483%`; P4 uniform expansion `+0.099513%`.
- Alpha-beta grid: 9 rows ran; 8 converged; the retained main cell is `alpha=+0.05, beta=-0.10`.
- Symmetry diagnostic, reciprocal geometric costs with fixed fundamentals: anchored GE welfare `+1.103376%`; residence dissimilarity `3.363766%`; workplace dissimilarity `6.147019%`.
- Descriptive checks: grid Duncan `0.260`; random-allocation floor `0.004`; radial inbound share `74.5%`; national AM flow-weighted SAR `0.878`; Shenzhen observed-flow SAR `0.819`; Beijing AM centerline SAR `0.884`.

## Scope

The validation reads only package-local aggregate inputs, compact reference results, and copied slide assets. It does not read raw speed files, raw OD files, or the main project directory after synchronization.

The LaTeX log retains one small overfull vbox warning, `1.392pt` on the radial-inbound appendix page. The compile has no missing figures, undefined references, fatal errors, or undefined citations.
