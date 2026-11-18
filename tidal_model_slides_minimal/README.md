# Tidal Lanes Model Slides Minimal Package

Standalone working copy for the current model slides: slide source, speaker script, slide figures, aggregate model inputs, reference result tables, and the **minimal** model code needed to rerun the main structural pipelines.

## Quick Start

```bash
make slides
make check
make smoke-model          # fast fixed-input convergence check
make validate-full        # full structural rerun + numeric compare
```

## Main-Project Link (Relative Path)

This package lives inside the main repo by default. `MAIN_REPO` is a one-line text file:

```text
..
```

All sync/push scripts resolve the main project as `(package_root / MAIN_REPO)`. No symlinks, no absolute paths — safe for Dropbox.

If you move this folder elsewhere, edit `MAIN_REPO` to the relative path back to TidalLanes, e.g. `../TidalLanes`.

## Sync With Main Project

Refresh from main (default `MAIN=..`):

```bash
make sync-from-main
# or explicitly:
make sync-from-main MAIN=..
```

Push one edited file back:

```bash
make push-to-main FILE='tidal.tex'
make push-to-main FILE='tidal_speaker_script.md'
```

`docs/FILE_MAP.csv` stores **relative** main-project paths (e.g. `Documents/PreSlides/Tidal0715.tex`, which the package carries as `tidal.tex`).

## What Is Included

See `docs/MANIFEST.md` for the exact file list.

- `tidal.tex` (= `Documents/PreSlides/Tidal0715.tex`, the current seminar deck since 2026-09-28), `tidal.pdf`, `references.bib`, `tidal_speaker_script.md`
- `figs/`, aggregate inputs, reference tables, minimal model code
- `MAIN_REPO` + `docs/FILE_MAP.csv`

## Git / Dropbox

```bash
make init-git
make slides && make check
git add .
git commit -m "Update minimal slides package"
```

Recommended ignores: LaTeX aux files, `outputs/smoke_model/`, `outputs/validation_*`.
