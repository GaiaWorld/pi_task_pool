#![cfg(loom)]
// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/recovery_loom_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/recovery_loom_test_inner.rs"]
mod recovery_loom_test_inner;

/// 契约：src/lib.rs:128-142；需求：needs.md 并发一致性、空返回
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: publication_racing_empty_consumer_preserves_task
/// Given 空池、一个提交者和一个消费者
/// When Loom 探索提交中一次消费，并等待两线程完成后排空
/// Then 编号7恰好出现一次，最终为空；允许提交中返回None
#[test]
fn publication_racing_empty_consumer_preserves_task() {
    recovery_loom_test_inner::publication_racing_empty_consumer_preserves_task();
}

/// 契约：src/lib.rs:118-142,158-161；需求：needs.md 同组串行、并发一致性
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: serial_cross_band_publication_preserves_fifo
/// Given 组0已有编号0到16共17个任务，下一次取出跨编码2到1
/// When Loom 探索同组追加17与消费者取出并释放首任务的交错，线程完成后排空
/// Then 编号0到17按序取得一次，最终为空
#[test]
fn serial_cross_band_publication_preserves_fifo() {
    recovery_loom_test_inner::serial_cross_band_publication_preserves_fifo();
}

/// 契约：src/lib.rs:128-142；需求：needs.md 概率优先级、并发一致性
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: saturated_publication_racing_repair_preserves_tasks
/// Given 最高档预填4095个唯一任务，提示已饱和
/// When Loom 探索新增4095与一次消费的交错，所有线程完成后排空
/// Then 编号0到4095各出现一次，最终为空
#[test]
fn saturated_publication_racing_repair_preserves_tasks() {
    recovery_loom_test_inner::saturated_publication_racing_repair_preserves_tasks();
}
