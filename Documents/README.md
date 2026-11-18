# Documents

Writing output and research memos, split into four layers.

The catalogue of every document, with its status and what supersedes it, lives in [`DOCUMENT_INDEX.md`](../DOCUMENT_INDEX.md) at the repository root.

| Layer | Contents |
|---|---|
| `L1_manuscripts/` | Paper and model sources. `drafts/` holds earlier iterations of the paper, model, and abstract lines. |
| `L2_notes/` | Research memos, handoffs, meeting notes, audits. `_superseded/` holds retired temporary checklists. |
| `L3_figs/` | Figures used by manuscripts. Slide-only figures belong in `PreSlides/figs/` instead. |
| `L4_pipeline_outputs/` | Tables, validation reports, and exhibits promoted out of the pipeline. |
| `PreSlides/` | Decks, speaker scripts, and presentation media. `_snapshots/` holds the manual backups taken during the July rewrite. |
| `archive/` | Frozen historical material. Do not modify. |

## Current state

The only material that uses current-mainline numbers is the November discussion deck `PreSlides/brad_202611/`. The seminar deck `PreSlides/Tidal0715.tex` (compiled as `Tidal0718.pdf`, July) and the paper draft `L1_manuscripts/TidalLanes_0429.tex` (April) still quote the old gate5 mainline. Which results each document uses is listed in the "Materials and the results they use" table of `DOCUMENT_INDEX.md`; updating the deck and paper waits until the specification is frozen (`nextstep.md`).

## Conventions

LaTeX build artefacts are not kept. They are regenerated on compile and are excluded by `.gitignore`; 180 of them were cleared on 2026-07-27 and 14 more on 2026-11-18. Decks with their own `build/` subfolder keep build files there.
