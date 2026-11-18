# brad_202611: discussion deck for the meeting with Brad Ross

| File | Notes |
|---|---|
| `slides_brad_202611.tex` / `.pdf` | English Beamer, same theme as `platform_20261104`. 17 main pages, 5 appendix pages. |
| `speaker_notes.md` | Page-by-page script: what to say, what to ask, likely follow-ups, and the source of each number. |
| `make_figs.py` | Builds the figures from pipeline outputs into `Documents/L3_figs/brad_202611/`. |

- Build figures: `/Users/fxr/miniconda3/envs/research/bin/python make_figs.py`
- Build deck: `latexmk -pdf -interaction=nonstopmode -outdir=build slides_brad_202611.tex`, then copy `build/slides_brad_202611.pdf` here.
- Numbers use the main specification as of 2026-11-15 (congestion elasticity 0.35). Background and the literature are in `Documents/L2_notes/brad_meeting_prep_20261113.md`.
