# crosscheck/ — 独立交叉校验（不属于主线管线）

本目录的脚本**只用于验证** `src/model/full_estimation` 主线代码的数值与逻辑正确性，
不参与任何论文/slides 产出，也不被主线 import。可安全删除。

## 脚本

- `crosscheck_equilibrium.py` — 用当前主线代码重解 baseline + 潮汐 ST/LT，
  对每一个"有多种解法"的对象用独立方法交叉验证：
  1. 谱半径 ρ(A) < 1
  2. τ_ij^{-θ}：线性求解 (I−A)^{-1} vs Neumann 级数 vs 残差 ‖(I−A)X−I‖
  3. 福利 W̄^θ：gauge-free 原式 vs 两个 gauge-fixed market-access 恒等式 vs eq.Wbar；
     并演示旧 Π(ones-init) 版本的跨情景比值被污染
  4. 链路流量 Ξ_kl：代码式 t^{-θ}P_k^{-θ}Π_l^{-θ} vs 引力/κ 路径式
  5. 引力边际一致性：Σ_j L_ij = l_R·L̄，Σ_i L_ij = l_F·L̄
  6. inversion 往返：反演基本面在 τ_obs 下复现 l_R_obs, l_F_obs
  7. anchored baseline 不动点：median τ/τ_obs≈1，统计 free-flow floor 命中边
  8. 分解恒等式：通勤 ST+GE+sorting=total；福利 gauge-free 与 Π-gauge-fixed 一致
  9. 福利比值的 gauge/归一化不变性：缩放 ū、缩放 market-access 初值，比值不变

## 运行

```bash
cd data_work
python3 crosscheck/crosscheck_equilibrium.py
```

输出每项 PASS/FAIL 与关键数字。
