// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/parallel_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/parallel_pool_test_inner.rs"]
mod parallel_pool_test_inner;

/// 契约：src/lib.rs:135-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: empty_pool_returns_none
/// Given 新建空池
/// When 调用 pop
/// Then 返回 None
#[test]
fn empty_pool_returns_none() {
    parallel_pool_test_inner::empty_pool_returns_none();
}

/// 契约：src/lib.rs:29-37,128-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: all_weights_round_trip
/// Given 分别为五档创建池并提交值 42
/// When 各取出一次后再次 pop
/// Then 取得 42，随后返回 None
#[test]
fn all_weights_round_trip() {
    parallel_pool_test_inner::all_weights_round_trip();
}

/// 契约：src/lib.rs:145-148
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: clones_share_tasks_without_clone_bounds
/// Given 不实现 Clone 的任务和执行器，以及两个池克隆
/// When 交替经原池和克隆提交并获取任务
/// Then 共享同一批任务且每个只取出一次
#[test]
fn clones_share_tasks_without_clone_bounds() {
    parallel_pool_test_inner::clones_share_tasks_without_clone_bounds();
}

/// 契约：src/lib.rs:128-133
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: same_weight_handles_coexist
/// Given 同档提交值 11 和 22
/// When 保持第一个句柄存活并取第二个
/// Then 两个句柄同时存活并分别持有 11 和 22
#[test]
fn same_weight_handles_coexist() {
    parallel_pool_test_inner::same_weight_handles_coexist();
}

/// 契约：src/lib.rs:149-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: mutable_borrow_preserves_task_ownership
/// Given 携带析构计数器的任务值 7
/// When 借用并改为 9，然后释放句柄
/// Then 借用不析构，值为 9，释放恰好析构一次
#[test]
fn mutable_borrow_preserves_task_ownership() {
    parallel_pool_test_inner::mutable_borrow_preserves_task_ownership();
}

/// 契约：src/lib.rs:158-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: dropping_handle_does_not_block_parallel_queue
/// Given 同档三个任务且已取得前两个句柄
/// When 释放第一个并保持第二个，再取第三个
/// Then 第三个任务仍可获取且所有任务仅获取一次
#[test]
fn dropping_handle_does_not_block_parallel_queue() {
    parallel_pool_test_inner::dropping_handle_does_not_block_parallel_queue();
}

/// 契约：src/task_pool_inner.rs:158-188
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: saturated_fields_can_be_drained
/// Given 每档分别提交超过其提示上限的唯一编号任务
/// When 持续取出直至 None
/// Then 所有编号恰好出现一次，最终为空
#[test]
fn saturated_fields_can_be_drained() {
    parallel_pool_test_inner::saturated_fields_can_be_drained();
}

/// 契约：src/lib.rs:128-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: mixed_weights_preserve_every_task
/// Given 五档分别提交 31 个唯一编号任务
/// When 持续取出直至 None
/// Then 155 个任务无丢失无重复，不断言优先顺序
#[test]
fn mixed_weights_preserve_every_task() {
    parallel_pool_test_inner::mixed_weights_preserve_every_task();
}

/// 契约：src/lib.rs:135-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: concurrent_consumers_take_each_task_once
/// Given 预先提交 4000 个任务并克隆池给四个消费者
/// When 四线程同时取出直至 None
/// Then 合计恰好获取编号 0 到 3999 各一次
#[test]
fn concurrent_consumers_take_each_task_once() {
    parallel_pool_test_inner::concurrent_consumers_take_each_task_once();
}

/// 契约：src/lib.rs:128-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: concurrent_producers_and_consumers_preserve_tasks
/// Given 五档各一个提交者与四个消费者，共 5000 个任务
/// When 同时启动，各提交者等待消费者确认取出首个任务后才继续提交其余 999 个
/// Then 五个握手全部完成，生产者和消费者全部汇合后由最终消费者排空，合计所有编号各一次，最终为空
#[test]
fn concurrent_producers_and_consumers_preserve_tasks() {
    parallel_pool_test_inner::concurrent_producers_and_consumers_preserve_tasks();
}

/// 契约：src/lib.rs:99-108,145-148
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: handles_outlive_all_pool_facades
/// Given 已取出的任务句柄及两个池门面
/// When 丢弃全部门面后借用、修改并释放句柄
/// Then 句柄仍有效且任务恰好析构一次
#[test]
fn handles_outlive_all_pool_facades() {
    parallel_pool_test_inner::handles_outlive_all_pool_facades();
}

/// 契约：src/lib.rs:103-104,110-116
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: permanent_reference_retains_executor_and_queued_tasks
/// Given 池中有一个待取任务，执行器与任务均记录析构
/// When 丢弃全部池门面
/// Then 执行器和待取任务均不析构
#[test]
fn permanent_reference_retains_executor_and_queued_tasks() {
    parallel_pool_test_inner::permanent_reference_retains_executor_and_queued_tasks();
}

/// 契约：src/lib.rs:102-103
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: handle_send_sync_positive_contracts
/// Given T 为 Cell<u8> 或 u8，执行器为 Send + Sync
/// When 编译消费端 Send/Sync 泛型约束
/// Then Cell 任务句柄可 Send，u8 任务句柄可 Send + Sync
#[test]
fn handle_send_sync_positive_contracts() {
    parallel_pool_test_inner::handle_send_sync_positive_contracts();
}

/// 契约：src/lib.rs:102-103
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: handle_send_sync_negative_contracts
/// Given 任务为 Rc 或 Cell，或执行器不是 Send/Sync
/// When 编译九种不满足任务或执行器 Send/Sync 边界的句柄线程约束
/// Then 各探针均以 E0277 拒绝线程标记而非环境失败
#[test]
fn handle_send_sync_negative_contracts() {
    parallel_pool_test_inner::handle_send_sync_negative_contracts();
}

/// 契约：src/lib.rs:110-116
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: construction_requires_static_task_and_executor
/// Given 静态引用以及分别借用局部值的任务和执行器
/// When 编译三个公开构造调用
/// Then 静态引用编译成功，局部任务及执行器均以 E0597 被拒绝
#[test]
fn construction_requires_static_task_and_executor() {
    parallel_pool_test_inner::construction_requires_static_task_and_executor();
}
