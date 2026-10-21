// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/serial_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/serial_pool_test_inner.rs"]
mod serial_pool_test_inner;

/// 契约：src/lib.rs:118-126
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: all_serial_indices_round_trip
/// Given 为索引 0..21 分别创建空池
/// When 提交索引值并获取任务
/// Then 每组取得原值，释放后池为空
#[test]
fn all_serial_indices_round_trip() {
    serial_pool_test_inner::all_serial_indices_round_trip();
}

/// 契约：src/lib.rs:39-59,124-126
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @negative
/// Scenario: invalid_indices_are_rejected_before_publication
/// Given 索引 21、22、usize::MAX 及空池
/// When 分别尝试构造 SerialQueueIndex
/// Then 均返回 None 且不产生提交；随后组 20 提交的任务仍可取得
#[test]
fn invalid_indices_are_rejected_before_publication() {
    serial_pool_test_inner::invalid_indices_are_rejected_before_publication();
}

/// 契约：src/lib.rs:118-142,158-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: same_group_is_fifo_until_handle_release
/// Given 组 0、10、20 分别按顺序提交 11、22、33
/// When 保持当前句柄并尝试再取，然后释放
/// Then 存活期间返回 None，释放后依次取得 11、22、33
#[test]
fn same_group_is_fifo_until_handle_release() {
    serial_pool_test_inner::same_group_is_fifo_until_handle_release();
}

/// 契约：src/task_pool_inner.rs:241-265; src/lib.rs:118-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: serial_count_boundaries_drain_in_order
/// Given 组 20 分别提交 0、1、16、17、32、33、48、49、129 个递增编号
/// When 逐个获取并释放预定数量任务
/// Then 每个编号按序取得一次，全部释放后返回 None
#[test]
fn serial_count_boundaries_drain_in_order() {
    serial_pool_test_inner::serial_count_boundaries_drain_in_order();
}

/// 契约：src/lib.rs:149-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: borrowing_does_not_release_serial_ownership
/// Given 组 10 提交值 7 和 8 的带析构计数任务
/// When 持有首句柄并通过 DerefMut 将 7 改为 9
/// Then 借用后同组仍不可取且无析构，释放后析构一次并可取得 8
#[test]
fn borrowing_does_not_release_serial_ownership() {
    serial_pool_test_inner::borrowing_does_not_release_serial_ownership();
}

/// 契约：src/lib.rs:99-108,135-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: held_group_does_not_block_other_groups
/// Given 21 个组各有两个任务
/// When 保持所有组首句柄同时存活，再释放组 10
/// Then 可持有 21 个不同组句柄，仅组 10 的下个任务变为可取
#[test]
fn held_group_does_not_block_other_groups() {
    serial_pool_test_inner::held_group_does_not_block_other_groups();
}

/// 契约：src/lib.rs:128-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: held_serial_group_does_not_hide_parallel_task
/// Given 持有组 0 首句柄，同组尚有任务，并发中档有 99
/// When 获取下个可用任务
/// Then 取得并发任务 99，保持串行句柄时再取返回 None
#[test]
fn held_serial_group_does_not_hide_parallel_task() {
    serial_pool_test_inner::held_serial_group_does_not_hide_parallel_task();
}

/// 契约：src/lib.rs:158-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: normal_panic_unwind_releases_group
/// Given 组 20 按序提交 11 和 22
/// When 取得 11 后主动 panic 并正常展开至捕获点
/// Then 捕获指定任务 panic，随后可取得同组 22
#[test]
fn normal_panic_unwind_releases_group() {
    serial_pool_test_inner::normal_panic_unwind_releases_group();
}

/// 契约：src/lib.rs:99-108,158-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: serial_handle_release_can_happen_on_another_thread
/// Given 组 20 提交 Cell 值 7 和 8，首句柄已取出
/// When 将首句柄移至另一线程借用并释放
/// Then 释放前同组不可取，线程结束后原线程取得 8
#[test]
fn serial_handle_release_can_happen_on_another_thread() {
    serial_pool_test_inner::serial_handle_release_can_happen_on_another_thread();
}

/// 契约：src/lib.rs:99-108,149-161
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: serial_handle_remains_valid_after_facades_drop
/// Given 组 20 已取一个带析构计数句柄，池有两个门面
/// When 丢弃全部门面再借用修改并释放句柄
/// Then 句柄仍有效且任务析构一次，串行释放正常返回
#[test]
fn serial_handle_remains_valid_after_facades_drop() {
    serial_pool_test_inner::serial_handle_remains_valid_after_facades_drop();
}

/// 契约：src/lib.rs:135-142; design/lock-free-task-pool/01-requirements/needs.md:6,13
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: competing_consumers_never_overlap_same_group
/// Given 21 组各预先提交 40 个唯一任务，四消费者同时开始
/// When 竞争获取并在句柄存活期记录组占用，汇合后排空
/// Then 同组占用从不重叠，840 个任务全部恰好取得一次
#[test]
fn competing_consumers_never_overlap_same_group() {
    serial_pool_test_inner::competing_consumers_never_overlap_same_group();
}

/// 契约：src/lib.rs:74-76,118-142; design/lock-free-task-pool/01-requirements/needs.md:13
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: different_group_producers_and_consumers_preserve_tasks
/// Given 组 0、10、20 各一生产者和三个消费者同时开始
/// When 各生产者提交 100 个任务，消费者获取，汇合后排空
/// Then 300 个任务无重复无丢失，同组句柄从不重叠
#[test]
fn different_group_producers_and_consumers_preserve_tasks() {
    serial_pool_test_inner::different_group_producers_and_consumers_preserve_tasks();
}

/// 契约：src/lib.rs:102-103
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: serial_handle_send_sync_contracts_compile
/// Given Send+Sync 执行器，任务为 Cell<u8> 与 u8
/// When 通过消费端泛型约束检查句柄与池
/// Then Cell 句柄为 Send，u8 句柄为 Send+Sync，Cell 池为 Send+Sync
#[test]
fn serial_handle_send_sync_contracts_compile() {
    serial_pool_test_inner::serial_handle_send_sync_contracts_compile();
}
