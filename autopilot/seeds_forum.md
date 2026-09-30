# Forum-distilled seeds (feed into miner prompt)

来源：`~/quant/quant-trading-system`（Alpha101/158/191 公式库、Magic Formula、PB-ROE 动量研究）+ WQ 中文 playbook + 官方论坛 BRAIN TIPS。已实测标注结果。

## 已验证有效的 FASTEXPR 模式（USA/TOP3000/delay1）

1. **隔夜缺口 + 价值腿组合**（2.33/1.53，已提交 KPr3Ld2E）：
   `add(rank(reverse(1 - open / close)), ts_rank(divide(ts_mean(cashflow_op, 20), enterprise_value), 60))`
   单腿 `rank(reverse(1-open/close))` 有 1.79/0.89，靠价值腿把换手从 0.84 稀释下来。
2. **三重筛选 + 价值腿组合**（2.02/1.53）：`(ts_rank(volume,32)*(1-ts_rank(close+high-low,16))) * (1-ts_rank(returns,32))` 单腿 1.66/0.96。
3. **符号序列 × 量比**（1.85/1.01，已提交 qM08g1b2，全新家族）：
   `(1-rank(sign(ts_delta(close,1))+sign(ts_delta(close,2))+sign(ts_delta(close,3)))) * ts_sum(volume,5)/ts_sum(volume,20)`
4. **group_rank(ts_rank(signal,N), subindustry)**：playbook 金牌组合。我方价值信号套用得 1.67/1.06（blOX70AR）。
5. **quantile(expr)**：包在冠军外得 2.0/1.58（88Pbk1va），但保序、自相关降不下来（0.97 FAIL），只uzzlesharpe 不解耦。
6. **SUBINDUSTRY 中性化**：冠军换 SUBINDUSTRY 得 1.84/1.35（GrOP0bEZ），可作备用腿。

## FASTEXPR 禁区（实测 ERROR）

- `ts_max` / `ts_min` / `delay` 在 FASTEXPR 不可用（delay 用 `ts_delta` 替代）。
- event 字段（anl4_*/actual_*）禁算术、禁 densify。
- 纯 `fscore_*` / beta 绝对值方向弱（±0.7 以内），只配做反转取反素材。

## 待挖方向

- Alpha101 `_alpha030` 家族变体（符号窗口/量比窗口扫描）。
- 隔夜缺口家族：`open/close` 换 `vwap`、`high/low` 位置；缺口 × 不同慢腿（ebit/enterprise_value、cashflow_op/assets）。
- `trade_when(signal, volume_spike, 1)` 条件暴露（未试过）。
- `bucket(rank(x), range="0,1,0.1")` 分桶（未试过）。
- Magic Formula 原教旨：`add(rank(ebit/EV), rank(ebitda/assets))` 只有 0.18——说明单调加权不行，要换非线性合成（ts_rank 外包、quantile 外包）。
