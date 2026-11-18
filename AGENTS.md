# AGENTS — TidalLanes (project root)

This file is the project-root coordination document for any AI agent working in this repo (Claude, subagents, code assistants, or any future model that opens the project).

It is intentionally short. Operational guidance for the data pipeline lives in `data_work/AGENTS.md`; current task state lives in `nextstep.md`; this file fixes the rules that apply *across* tasks.

For the lowest-token restart or handoff, read [`agent_quickstart.md`](./agent_quickstart.md) first. The current model specification, input bundle, and run commands are stated in one place only: the "Current mainline" table at the top of [`DOCUMENT_INDEX.md`](./DOCUMENT_INDEX.md). Script defaults, older READMEs, and folder names may describe the old gate5 mainline. To find which document covers a topic, and which one wins when two disagree, use [`DOCUMENT_INDEX.md`](./DOCUMENT_INDEX.md).

---

## Mandatory: writing rules

Any task that produces English prose for the project — paper text, abstracts, slide content, referee responses, project memos, or long-form documentation — **must** be performed under the rules in [`writing.md`](./writing.md).

This is non-negotiable and applies to every agent, regardless of which subagent or tool produced the prose. Concretely:

1. Before generating prose, the agent reads `writing.md` in full.
2. The agent applies the rules during generation, not as a post-hoc rewrite.
3. Before returning the output, the agent runs the final-output checklist in `writing.md` Section 8.
4. If a request asks for prose that would violate these rules, the agent follows `writing.md` rather than the request, and notes the deviation back to the user.
5. Documents shared with coauthors must lead with the conclusion and use plain, direct language. Technical terms, process detail, and formal phrasing are included only when they help the reader understand or assess the result.

The rules in `writing.md` override any other style preference the agent has been trained on.

This rule does **not** apply to:

- code, code comments, commit messages, or shell output
- chat-style replies to the user (clarifications, confirmations, summaries of work done)
- machine-readable artefacts (JSON, YAML, CSV, log files)

---

## Repository layout

Top-level directories and what belongs in each. Directory names are load-bearing: several hundred hard-coded paths in `nextstep.md`, `data_work/docs/WORK_LOG.md`, and `Documents/L2_notes/` refer to them, so they are not renamed even where the spelling is imperfect (`flowvedio`).

| Path | Contents | Reproducible? |
|---|---|---|
| `Platform/` | 2025 platform extraction specifications, probes, queue, and local run records | source; generated references and downloads excluded from Git |
| `data_work/src/` | pipeline and model code | source |
| `data_work/raw_data/` | Baidu, Gaode, GIS, and Shenzhen source data | no, irreplaceable |
| `data_work/outputs/` | every generated run, one directory per version id | yes, from code plus inputs |
| `data_work/docs/` | pipeline documentation and `WORK_LOG.md` | source |
| `data_work/config/` | run configuration in JSON | source |
| `flowvedio/` | Caitian Road traffic video, counting code, batch outputs | raw video: no |
| `fieldvisit/` | field photographs from site visits | no, irreplaceable |
| `Documents/L1_manuscripts/` | paper and model `.tex` sources | source |
| `Documents/L2_notes/` | handoff memos, design notes, meeting notes | source |
| `Documents/L3_figs/` | figures used by manuscripts and slides | mostly yes |
| `Documents/L4_pipeline_outputs/` | tables and exhibits promoted out of the pipeline | yes |
| `Documents/PreSlides/` | slide decks, speaker scripts, presentation video | source |
| `Replication_apply/` | workspace adapting the Allen-Arkolakis Seattle replication code to Beijing data | mixed |
| `tidal_model_slides_minimal/` | self-contained Python replication package shared through Dropbox | yes |
| `tidal_model_slides_minimal_r/` | the same package reimplemented in R | yes |
| `sample_data/` | small public sample of the data structure | source |

The three replication directories serve different ends and are not duplicates of one another. `Replication_apply/` is an inward-facing research tool: it reshapes pipeline outputs into Allen-Arkolakis input format so their Seattle procedure can be run against Beijing data as a cross-check. `tidal_model_slides_minimal/` is outward-facing, the package sent to coauthors through Dropbox, where it appears as `tidal_model_replication`. Its name understates it, since it reproduces every structural number rather than a minimal subset. The `_r` variant exists so the results do not depend on one language implementation. None of the three is renamed, because `Replication_apply` alone is referenced from 21 files including runtime paths inside the Python package.
| `00archive/`, `Documents/archive/` | frozen historical material, read-only | archived |

Two conventions follow from the reproducibility column. Anything marked reproducible is excluded from backup and from Git, because it is recoverable from code plus inputs. Anything marked irreplaceable is backed up before any cleanup touches it; the procedure is fixed in [`data_work/docs/BACKUP_WORKFLOW.md`](./data_work/docs/BACKUP_WORKFLOW.md).

---

## Other project-wide rules

- Do not modify files under `00archive/` or `Documents/archive/`.
- Do not delete or rewrite raw data under `data_work/raw_data/`.
- Pipeline-specific guidance: see [`data_work/AGENTS.md`](./data_work/AGENTS.md).
- Platform extraction guidance: see [`Platform/AGENTS.md`](./Platform/AGENTS.md), [`Platform/current-spec.md`](./Platform/current-spec.md), and the batch queue/run log.
- Current operational priorities: see [`nextstep.md`](./nextstep.md).
- Workflow split:
  - `nextstep.md` is the live plan and must stay short.
  - Keep only current status, the immediate next action, and short future plan bullets in `nextstep.md`.
  - Move completed work, locked parameters, historical comparisons, and retained decisions into `data_work/docs/WORK_LOG.md`.
  - When a task is completed, remove it from `nextstep.md` rather than letting the live plan become a running diary.
  - After any substantial completed work, the agent checks whether `nextstep.md` and `data_work/docs/WORK_LOG.md` need updating before finishing.
  - Update `nextstep.md` only if current status, immediate next action, or priorities changed.
  - Update `WORK_LOG.md` only for completed work, retained outputs, locked decisions, or reusable lessons; do not log low-value intermediate churn.

---

## What to do if uncertain

If the agent is unsure whether the writing rules apply to a given output, the default is that they apply. If the agent is unsure of a factual claim, the rule in `writing.md` Section 7 applies: leave a visible placeholder rather than filling with plausible invention.
