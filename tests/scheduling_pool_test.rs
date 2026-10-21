// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/scheduling_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/scheduling_pool_test_inner.rs"]
mod scheduling_pool_test_inner;

/// 契约：src/lib.rs:128-142；design/lock-free-task-pool/01-requirements/needs.md:8,13
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: saturated_competing_consumers_preserve_all_tasks
/// Given 五档分别预填提示上限加257个唯一任务，四消费者同时开始
/// When 四消费者各自以ticket 0竞争获取，全部结束后由主线程最终排空
/// Then 每个已提交编号恰好取得一次，最终返回None；不要求每个消费者取得任务
#[test]
fn saturated_competing_consumers_preserve_all_tasks() {
    scheduling_pool_test_inner::saturated_competing_consumers_preserve_all_tasks();
}

/// 契约：src/lib.rs:128-142；design/lock-free-task-pool/01-requirements/needs.md:8,13,15
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: saturated_publications_and_consumers_eventually_drain
/// Given 五档分别预填提示上限加257个任务，每档另有一个生产者准备提交1024个任务
/// When 五生产者先各等待首个新增任务被取得确认再继续提交，四消费者同时竞争
/// Then 所有预填及新增任务恰好取得一次，最终返回None；不要求发布中持续可见
#[test]
fn saturated_publications_and_consumers_eventually_drain() {
    scheduling_pool_test_inner::saturated_publications_and_consumers_eventually_drain();
}

/// 契约：src/lib.rs:118-142,158-161；design/lock-free-task-pool/01-requirements/needs.md:6,8,13
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: serial_cross_band_publications_preserve_fifo_and_exclusion
/// Given 组0、10、20分别预填33个任务，各有唯一生产者准备再提交96个连续编号
/// When 各生产者等待编号33被取得并释放后继续提交，四消费者同时竞争
/// Then 每组按0到128顺序恰好取出一次，同组句柄从不重叠，最终返回None
#[test]
fn serial_cross_band_publications_preserve_fifo_and_exclusion() {
    scheduling_pool_test_inner::serial_cross_band_publications_preserve_fifo_and_exclusion();
}

/// 契约：src/lib.rs:118-142,158-161；design/lock-free-task-pool/01-requirements/needs.md:6,8,11
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: held_serial_groups_leave_saturated_parallel_candidates_available
/// Given 21组各有两个串行任务且首句柄全部存活，随后五档分别提交提示上限加1个任务
/// When 保持串行句柄并排空并发任务，再释放全部句柄并排空
/// Then 第一阶段只取得全部并发任务，第二阶段取得各组剩余任务，每个编号恰好一次，最终返回None
#[test]
fn held_serial_groups_leave_saturated_parallel_candidates_available() {
    scheduling_pool_test_inner::held_serial_groups_leave_saturated_parallel_candidates_available();
}

/// 契约：src/lib.rs:128-142；design/lock-free-task-pool/01-requirements/needs.md:8
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: fixed_ticket_intervals_match_queue_weights
/// Given 组0有17个任务、组5有1个、组12有33个、组9被占用且余1个，五档并发提示分别为3、2、1、1、1
/// When 以ticket从0到11722逐个调用pop并将任务放回原队列，再用u64::MAX调用一次pop
/// Then 各候选命中数等于其区间长度（串行编码乘100、并发提示乘档位权重，含编码3与High档），
///      占用组从不命中，ticket等于总权重时回绕到组0，u64::MAX按总权重取模后落入对应区间，
///      释放占用后组9重新计入
#[test]
fn fixed_ticket_intervals_match_queue_weights() {
    scheduling_pool_test_inner::fixed_ticket_intervals_match_queue_weights();
}

/// 契约：src/lib.rs:135-142
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: fixed_ticket_selection_repeats_after_total_weight
/// Given 最低档与中档各1个任务，总权重101
/// When 分别以0、1、101、201、0、0作为random调用pop
/// Then 0与101命中最低档，1与201命中中档，同输入重复命中同一队列；空池任意random均返回None
#[test]
fn fixed_ticket_selection_repeats_after_total_weight() {
    scheduling_pool_test_inner::fixed_ticket_selection_repeats_after_total_weight();
}

/// 契约：src/lib.rs:128-142；design/lock-free-task-pool/01-requirements/needs.md:8,11
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: saturated_fields_drain_under_fixed_tickets
/// Given 五档分别提交提示上限加17个唯一任务，提交量已超过字段提示上限
/// When 以ticket 0反复调用pop直至返回None
/// Then 每个编号恰好取得一次，排空后再次pop返回None
#[test]
fn saturated_fields_drain_under_fixed_tickets() {
    scheduling_pool_test_inner::saturated_fields_drain_under_fixed_tickets();
}
