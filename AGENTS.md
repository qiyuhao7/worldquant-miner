# AGENTS.md — worldquant-miner

WorldQuant Brain alpha 因子挖掘工具集。AI agent 在此仓库工作时遵守以下约定。

## 目录结构

- `autopilot/` — 24/7 无人值守挖掘系统（当前主力，见下）。
- `generation_two/` — 最完整的一代：自优化模板 + 遗传进化 + GUI（`gui/run_gui.py`）。
- `generation_one/` / `stone_age/` / `mini-quant/` — 早期单脚本矿工（`alpha_expression_miner.py --expression` 可直接回测手写表达式，无需 LLM）。
- `credential.example.txt` — 凭证格式说明。真凭证 `credential.txt` / `generation_two/credential.txt` 内容为 `["邮箱","密码"]`。

## autopilot 子系统（重点）

- `miner_loop.py` — 主循环：muse-spark（经本地 OCG 网关 `http://127.0.0.1:9042/v1`，OpenAI 兼容）生成 FASTEXPR → WorldQuant `/simulations` 回测 → 全过线自动提交。`--hours 0` = 无限跑。
- `results.db`（SQLite）— 唯一真实数据源：`sims` 表（每次仿真，崩溃安全：先写 PENDING 行再 UPDATE）、`kv` 表（submitted 名单、每日计数、deadline）。
- `dashboard.py` — 进度看板 `http://127.0.0.1:8899/`（纯标准库）。
- `run_forever.sh` / `stop.sh` — 启停脚本。线上常驻进程，不要误杀（`miner_loop.py --hours`、`dashboard.py`）。
- 历史经验沉淀在 prompt 里：OCG 必须用 `max_completion_tokens`；event 字段禁算术/densify；仅 USA 可用；保序变换降不了自相关（self-corr 上限 0.7）。

## 铁律

1. **绝不提交秘密**：`credential.txt`、`results.db*`、`*.log`、`daemon_status.json` 已在 `.gitignore`，提交前用 `git status` 复核 staged 列表。
2. **不动线上**：改 `autopilot/` 代码前先确认 daemon 状态；重启 daemon 必须验活（`pgrep` + 日志 tail + 看板 200）。
3. **只读优先**：先读文件/查 DB 再动手；`cksum` 式猜测不允许。
4. **用执行验证**：改完跑语法检查 + 小规模冒烟（单次仿真或 `--rounds 1`），贴证据再报结论。
5. **WQ 接口礼仪**：轮询间隔 ≥8s（看 `Retry-After`），429/代理断连退避重试；批量提交间隔 ≥10s。

## 常用命令

```bash
python3 autopilot/miner_loop.py --hours 0 --auto-submit --max-submits 3
bash autopilot/run_forever.sh   # 后台常驻（setsid nohup）
bash autopilot/stop.sh
python3 -c "import sqlite3;con=sqlite3.connect('autopilot/results.db');print(con.execute('SELECT COUNT(*) FROM sims').fetchone())"
python stone_age/python/pre_consultant/alpha_expression_miner.py --credentials ./credential.txt --expression "ts_rank(close,20)"
```
