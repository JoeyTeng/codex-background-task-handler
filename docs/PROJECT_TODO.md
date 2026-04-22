# Project TODO

- [x] 确认测试 thread `019db49a-de4e-7d61-93ab-5d70a8905cc3` 已落盘并可定位到 rollout 文件。
- [x] 确认桌面端私有 `app-server` 当前正持有该 rollout 文件。
- [x] 实现最小 PoC 脚本，通过外部独立 `codex app-server` 对该 thread 执行 `read` / `resume` / `inject_items`。
- [x] 运行 PoC，验证 marker 是否写入 rollout。
- [x] 扩展 PoC，验证外部独立 `turn/start` 能否在 desktop-originated thread 上生成完整新 turn。
- [x] 让用户在 desktop UI 中确认：完整外部 turn 在当前 loaded session 中仍不可见，且后续本地 turn 也未将其纳入上下文。
- [x] 验证 `codex exec resume` 能否作为不同于裸 app-server 的“特殊入口”向同一 desktop thread 追加 turn。
- [x] 验证独立 `codex exec` 会话是否实际暴露 agent mailbox 投递工具。
- [x] 基于结果收敛结论：哪些能力可以靠 wrapper/sidecar 实现，哪些 desktop 行为仍缺少公开 attach 面。
- [ ] 如果需要，把 Desktop heartbeat 做一个定时实测，区分“app 保持打开”与“app 完全退出”两种情况下是否按时触发。
- [ ] 建一个真实 heartbeat automation 样本，确认 thread 目标等字段在 `automation.toml` / `codex-dev.db` 中的持久化形状。
- [ ] 验证外部进程在 Desktop 运行时改写 automation 调度状态（尤其是 `next_run_at` / 状态切换）后，caller thread heartbeat 是否会被及时触发。
- [x] 验证 bridge automation thread 是否能通过 `automation_update` 稳定为别的 caller thread 创建/更新 heartbeat automation，而无需外部直接改 Codex automation DB。
- [x] 单独沉淀 Desktop background-task bridge 技术方案文档。
- [ ] 实现 `background-taskctl` 最小共享状态接口，优先考虑本地 helper CLI。
- [ ] 定义 bridge heartbeat prompt 与 caller heartbeat prompt 的最小稳定合约。
- [ ] 设计 caller heartbeat 的清理策略，避免残留重复 heartbeat automation。
- [ ] 为 bridge thread 设计一个最小共享状态面（文件 / socket / helper CLI 其一），让它能读取 sidecar 任务状态而不依赖 Codex thread 之间的 live push。
