# Agent Quickstart

用途：给新接入的 agent 一个最低 token 成本的对齐入口。

## 最小阅读顺序

1. `AGENTS.md`：跨任务规则与目录
2. `DOCUMENT_INDEX.md` 只读 “Current mainline” 一节：当前口径、输入包、运行命令、数字能怎么引用。**这是当前口径的唯一出处**，其他 README、脚本默认值、目录名都可能是旧主线
3. `nextstep.md`：当前状态与下一步
4. `data_work/docs/WORK_LOG.md` 只含 2026-10 以后，新条目在最上面；更早的在 `WORK_LOG_2026Q3.md`、`WORK_LOG_2026Q2.md`。**不要通读**，只按任务搜日期（`## 2026-MM-DD`）或输出目录名
5. `data_work/AGENTS.md` 仅在要改 pipeline / 跑模型时再读
6. `writing.md` 仅在要写英文正文 / memo / slides / appendix prose 时再读

## 当前项目一句话

这个项目研究北京方向性拥堵不对称与潮汐车道式容量重分配：用有向路网上的量化空间模型（路径选择＋居住就业选址）评估潮汐车道，并与扩建、定价等政策比较。

## 最容易犯的三个错

- 按脚本默认值跑模型。`src/model/full_estimation/` 的默认值仍是旧主线（anchored、λ = 1.1），主线运行必须照索引表里的命令显式传参
- 引用旧数字。论文（4 月）、`Tidal0715`（7 月）、复现包都是旧主线；`results_update_and_specifications_20261110.md` 是 β_c = 0.25。当前数字只看索引表
- 把“潮汐达到扩建的 96%”说成全部道路使用者的结果。它只对通勤者福利成立

## 新 agent 默认做法

- 不要先读很多旧文件；先按上面的顺序建立上下文
- 给合作者分享的文档，默认先写结论，再用简单、直白的话说明证据和局限；不要堆术语、流程和“AI腔”
- 不要重写 `nextstep.md`；只在当前状态 / 下一步 / 优先级变化时更新
- 已完成事项写入 `data_work/docs/WORK_LOG.md`；新的主线输出登记到 `data_work/docs/MAINLINE_MANIFEST.md`
- 主线口径变了，先改 `DOCUMENT_INDEX.md` 的 “Current mainline” 表
- 不要修改 `00archive/` 或 `Documents/archive/`；不要改 raw data

## 可直接复制给新 agent 的 prompt

```text
You are joining the TidalLanes project. Minimize token use and align quickly.

Read only these, in order:
1. AGENTS.md
2. The "Current mainline" section of DOCUMENT_INDEX.md (the only authority on the current specification and run commands)
3. nextstep.md

Do not read data_work/docs/WORK_LOG.md in full; search it only for entries your task needs.
If the task touches pipeline code or model runs, also read data_work/AGENTS.md.
If the task requires English project prose, also read writing.md.

After reading, give a short alignment note with:
- current project goal
- current mainline specification and its run command
- current bottleneck
- immediate next step for the requested task

Do not produce a long recap. Do not read broadly unless needed.
```

中文版本（同上意思）：先读 `AGENTS.md`、`DOCUMENT_INDEX.md` 的 “Current mainline” 一节和 `nextstep.md`；WORK_LOG 只按任务检索；涉及代码再读 `data_work/AGENTS.md`，写英文再读 `writing.md`。用很短的话对齐当前状态、任务和下一步，然后直接开始工作。
