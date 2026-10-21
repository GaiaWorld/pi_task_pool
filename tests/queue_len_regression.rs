// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/queue_len_regression.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/queue_len_regression_inner.rs"]
mod queue_len_regression_inner;

/// 契约：src/lib.rs TaskPool::push_parallel/pop；needs.md 概率优先级、并发一致性
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: parallel_saturation_drains_after_len_based_repair
/// Given 五档分别在独立池提交编号0到9999共10000个任务，均超过该档提示上限
/// When 依次以random 0到9999获取任务
/// Then 各池按序取得0到9999且随后返回None
#[test]
fn parallel_saturation_drains_after_len_based_repair() {
    queue_len_regression_inner::parallel_saturation_drains_after_len_based_repair();
}

/// 契约：src/lib.rs TaskPool::push_serial/pop、TaskHandle::drop；needs.md 同组串行
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: serial_len_encoding_crosses_all_boundaries
/// Given 组20按序提交编号0到79共80个任务
/// When 反复以u64::MAX获取，每次持有句柄时再以0获取，然后释放句柄
/// Then 依次取得0到79，持有句柄时同组不可获取，排空后返回None
#[test]
fn serial_len_encoding_crosses_all_boundaries() {
    queue_len_regression_inner::serial_len_encoding_crosses_all_boundaries();
}

/// 契约：src/lib.rs TaskPool::push_parallel/pop；needs.md 并发一致性及恢复进展前提
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: concurrent_publications_and_consumers_leave_no_hidden_tasks
/// Given 最高档预填编号0到8999，两个生产者各准备3000个不重复任务
/// When 两生产者与两消费者同时起跑，各消费者尝试获取12000次，全部汇合后排空
/// Then 合计取得编号0到14999各一次；仅要求所有已开始发布与修复完成后的最终结果
#[test]
fn concurrent_publications_and_consumers_leave_no_hidden_tasks() {
    queue_len_regression_inner::concurrent_publications_and_consumers_leave_no_hidden_tasks();
}
