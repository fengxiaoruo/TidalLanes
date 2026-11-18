# data_work/docs

Pipeline documentation, evidence memos, and the work log.

The catalogue of every document in the project, with its status and what supersedes it, lives in [`DOCUMENT_INDEX.md`](../../DOCUMENT_INDEX.md) at the repository root. That file is maintained; this one is not a second list.

## The three that matter most

| File | Use |
|---|---|
| `WORK_LOG.md` | Completed work, locked parameters, retained decisions, from 2026-10 onward. Add new entries at the top; do not edit history. Earlier entries are in `WORK_LOG_2026Q3.md` and `WORK_LOG_2026Q2.md`. |
| `MAINLINE_MANIFEST.md` | Output folders of the current mainline and its input chain. The specification itself is the "Current mainline" table in `DOCUMENT_INDEX.md`. |
| `OUTPUTS_REGISTRY.md` | Every output folder sorted by category. |
| `STAGE2_MAINLINE_GATE5.md` | Specification of the old gate5 mainline (June 2026); reproduces `Tidal0715` and the April paper, not the current mainline. |
| `HOW_TO_RERUN_PARTIAL_RESULTS.md` | How to change a parameter and rerun only what depends on it. |

## `_superseded/`

Documents from the March pipeline refactor and the prototype model line, moved aside on 2026-07-27. They record how the project reached its current structure and are kept for provenance. Nothing in that directory describes current behaviour; read it only to understand a historical decision.
