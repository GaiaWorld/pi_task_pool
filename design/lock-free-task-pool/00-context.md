# lock-free-task-pool · context（L0）

## 地形

- Rust 2021 库 pi_task_pool 0.2.0，依赖 crossbeam-queue 0.3、crossbeam-utils 0.8（Cargo.toml:1–13）。
- 当前源码集中于 src/lib.rs；类型采用 PascalCase，方法/字段采用 snake_case；现有名称保留 TaskWight、TaskExector、task_exector。
- src/lib.rs:1–20 是方案素材：64 个串行队列、5 个并发优先级、概率权重、快速执行。
- src/lib.rs:28–81 是声明骨架；Drop、插入、弹出尚为 todo!()。TaskHandle 保存内部池裸指针，pop 有 &self，exec 接收句柄并返回 io::Result<()>。
- 尚有现存语法问题：task_exector 字段以分号结束；尚未编译验证。
- 未发现测试文件或 AGENTS.md。
- 工作区已有用户修改：Cargo.toml、README.md、src/lib.rs，以及三个旧源码文件删除和 .opencode/ 未跟踪内容。设计不得覆盖这些既有工作。

## 素材与来源

| 来源 | 已读情况 | 用途 |
|---|---|---|
| 用户对话及五点修正 | 已读 | 当前意图与已确认的技术约束 |
| src/lib.rs | 全文已读 | 方案与骨架，不将未实现内容当作已具备行为 |
| Cargo.toml | 已读 | 语言和依赖 |
| README.md | 已读 | 仍有历史废弃标记；不能据此推翻当前开发指令 |
| 组织既有方案 | 未提供 | 当前不依赖外部项目或历史实现 |

## 待验证

- 方案注释中 max 阈值判断前后相反，需 L1 由用户确定。
- 执行器拒绝任务后的所有权与任务去向、串行完成时点、低优先级公平性、快速执行顺序需澄清。
- 饱和计数的调度语义、serial_used 状态定义与并发交接在详细设计阶段确定。
- 尚无吞吐或延迟量化要求，不虚构指标。
