# AGENTS — TidalLanes (project root)

This file is the project-root coordination document for any AI agent working in this repo (Claude, subagents, code assistants, or any future model that opens the project).

It is intentionally short. Operational guidance for the data pipeline lives in `data_work/AGENTS.md`; current task state lives in `nextstep.md`; this file fixes the rules that apply *across* tasks.

For the lowest-token restart or handoff, read [`agent_quickstart.md`](./agent_quickstart.md) first.

---

## Mandatory: writing rules

Any task that produces English prose for the project — paper text, abstracts, slide content, referee responses, project memos, or long-form documentation — **must** be performed under the rules in [`writing.md`](./writing.md).

This is non-negotiable and applies to every agent, regardless of which subagent or tool produced the prose. Concretely:

1. Before generating prose, the agent reads `writing.md` in full.
2. The agent applies the rules during generation, not as a post-hoc rewrite.
3. Before returning the output, the agent runs the final-output checklist in `writing.md` Section 8.
4. If a request asks for prose that would violate these rules, the agent follows `writing.md` rather than the request, and notes the deviation back to the user.

The rules in `writing.md` override any other style preference the agent has been trained on.

This rule does **not** apply to:

- code, code comments, commit messages, or shell output
- chat-style replies to the user (clarifications, confirmations, summaries of work done)
- machine-readable artefacts (JSON, YAML, CSV, log files)

---

## Other project-wide rules

- Do not modify files under `00archive/` or `Documents/archive/`.
- Do not delete or rewrite raw data under `data_work/raw_data/`.
- Pipeline-specific guidance: see [`data_work/AGENTS.md`](./data_work/AGENTS.md).
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
