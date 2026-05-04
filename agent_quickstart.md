# Agent Quickstart

用途：给新接入的 agent 一个最低 token 成本的对齐入口。

## 最小阅读顺序

1. `AGENTS.md`
2. `nextstep.md`
3. `data_work/docs/WORK_LOG.md`
4. `data_work/AGENTS.md` 仅在要改 pipeline / data_work 时再读
5. `writing.md` 仅在要写英文正文 / memo / slides / appendix prose 时再读

## 当前项目一句话

这个项目研究北京方向性拥堵不对称与潮汐车道式容量重分配，当前主线是把 raw_connected_v1 原始有向路网构造成可信的 grid-to-grid travel cost，并决定后续 baseline 方法。

## 当前工作焦点

- `raw-centerline` 稳定版本：`manual_centerline_rules_v11`
- raw-grid 当前比较的主版本：
  - `raw_node_v4`
  - `raw_edgeproj_v2`
- 当前更偏好的推进方向：`raw_edgeproj_v2`
- 当前主要瓶颈：
  - square / hex grid coverage 仍偏低
  - `raw_edgeproj_v2` 仍有 long-detour tail 需要诊断

## 新 agent 默认做法

- 不要先读很多旧文件；先按上面的顺序建立上下文
- 不要重写 `nextstep.md`；只在当前状态 / 下一步 / 优先级变化时更新
- 已完成事项写入 `data_work/docs/WORK_LOG.md`
- 不要修改 `00archive/` 或 `Documents/archive/`
- 不要改 raw data

## 可直接复制给新 agent 的 prompt

```text
You are joining the TidalLanes project. Minimize token use and align quickly.

Read only these files first, in order:
1. AGENTS.md
2. nextstep.md
3. data_work/docs/WORK_LOG.md

If and only if the task touches pipeline code or data construction, then read:
4. data_work/AGENTS.md

If and only if the task requires English project prose, then read:
5. writing.md

After reading, give a short alignment note with:
- current project goal
- current working baseline / preferred version
- current bottleneck
- immediate next step for the requested task

Do not produce a long recap. Do not read broadly unless needed.
```
