# Sync Workflow

This package aligns with the main TidalLanes repo through:

- `MAIN_REPO` — one-line **relative** path to the main repo (default: `..`)
- `docs/FILE_MAP.csv` — maps each package file to a **relative** main-project path

No symlinks. Works across machines and Dropbox.

## Default Layout

```text
TidalLanes/                          # main repo
  tidal_model_slides_minimal/        # this package
    MAIN_REPO                        # contains: ..
    tidal.tex
    ...
```

## Refresh From Main

```bash
make sync-from-main
```

Uses `MAIN=..` by default. Override if needed:

```bash
make sync-from-main MAIN=../TidalLanes
```

This copies managed files, rewrites `docs/FILE_MAP.csv` with relative paths, updates `MAIN_REPO`, and optionally prunes extras.

## Move Package Edits Back

```bash
make push-to-main FILE='tidal.tex'
python scripts/push_to_main.py --dry-run tidal.tex
```

Then rerun the relevant pipeline or slide compile in the main project.

## After Moving The Folder

If the package is no longer a direct child of the main repo, edit `MAIN_REPO`:

```text
../TidalLanes
```

Run `make check` to confirm resolution.

## Recommended Checks Before Sharing

```bash
make slides
make check
make smoke-model
git status
```

Run `make validate-full` after model-logic changes.
