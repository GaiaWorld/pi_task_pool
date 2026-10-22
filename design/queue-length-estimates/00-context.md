# queue-length-estimates · 上下文

- Rust 2021，生产依赖 Crossbeam；见 Cargo.toml:5,11-13。沿用 snake_case 方法名与 src/lib.rs 门面、src/task_pool_inner.rs 内部实现。
- src/lib.rs 的 TaskPool 持有 serial_states、parallel_states、21 个串行队列与 5 个并发队列；当前包含尚未提交的中文文档与 SerialQueueIndex 类型迁移，属于用户已批准工作，不覆盖。
- src/task_pool_inner.rs:503-512 已有总量估算：串行每档16、并发按提示值；此次需求为分队列结果，不能仅暴露总量函数。
- 测试布局：tests/*_test.rs 声明面与 tests/support/*_inner.rs 实现面；内部测试位于 src/task_pool_inner_test*.rs。当前会话上一轮 cargo test 58个常规测试及1个文档测试通过。设计阶段不重跑测试。
- 构建核验命令：cargo check；落声明前另行获取基线。
- 来源：本次对话；当前源码、Cargo.toml、README；用户确认无额外组织内部来源，用途为监控观察。
- 待验证：无外部来源待验证项；L7 API 名称与表示待确认。
