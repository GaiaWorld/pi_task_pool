# queue-length-estimates 设计索引（design-lite）

恢复入口：`index/status.md` 的当前阶段。

| 分册 | 内容 |
|---|---|
| [状态](index/status.md) | 模式与阶段 |
| [变更日志](index/changes.md) | 确认及变更记录 |
| [词汇](index/glossary.md) | 统计术语 |
| [上下文](00-context.md) | 项目事实与来源 |
| [问题](01-requirements/problem.md) | 范围与排除项 |
| [需求](01-requirements/needs.md) | 验收标准 |
| [流程](01-requirements/flow.md) | 单次查询说明 |
| [约束](01-requirements/notes.md) | 并发边界及澄清 |

L5 已确认跳过；L7 精简档已落声明壳芯。

- [预期调用链与实现切片](07-design/call-chain.md)
- [公开声明与类型](../../src/queue_length_estimates.rs)
- [内部声明与待实现桩](../../src/queue_length_estimates_inner.rs)

新增方法尚未实现，不能调用。[L9独立轻审](09-review.md)已放行，用户接受审查结论及修正后交付。
