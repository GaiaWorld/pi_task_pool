# lock-free-task-pool 设计索引（design-lite）

恢复入口：`index/status.md` 的当前阶段。

| 分册 | 内容 |
|---|---|
| [index/status.md](index/status.md) | 模式与阶段 |
| [index/changes.md](index/changes.md) | 变更日志 |
| [index/glossary.md](index/glossary.md) | 术语定义 |
| [00-context.md](00-context.md) | L0 地形与来源 |

## 需求分册

- [问题与范围](01-requirements/problem.md)
- [需求与验收](01-requirements/needs.md)
- [流程与源表](01-requirements/flow.md)
- [非功能与澄清](01-requirements/notes.md)
- [队列研究与比较素材](01-requirements/queue-research.md)

## 模块分册

- [模块职责](05-modules/modules.md)
- [依赖图与源表](05-modules/deps.md)
- [变更点](05-modules/change-points.md)
- [决定与可行性](05-modules/decisions.md)

## 详细设计（精简档）

- [调用链与实现切片](07-design/call-chain.md)
- 公开声明壳与契约：`src/lib.rs`
- 内部状态声明：`src/lib.rs`；实现模块：`src/task_pool_inner.rs`（`mod inner`）

## 当前审查与实现状态

当前设计处于L7同步后的待复审状态，旧报告仅作归档。前两切片已实现；移除TaskPoolInner/内部Arc后，3个有界Loom模型通过，4个私有确定性恢复窗口测试通过，普通测试共42通过。第三切片原技术阻塞已有解决及证据，待dev-impl重新审查；第四快速交接待实现。当前没有有效的09-review.md，不提前标记切片完成。
