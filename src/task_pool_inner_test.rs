//! 票据3私有恢复窗口的单元测试声明面。
#[path = "task_pool_inner_test_inner.rs"]
mod task_pool_inner_test_inner;
pub(super) use task_pool_inner_test_inner::checkpoint;

/// 契约：src/lib.rs:78-84；src/task_pool_inner.rs:345-349；design/lock-free-task-pool/01-requirements/needs.md:8,13,15
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法精确暂停旧快照下调CAS前并读取提示字段。
/// @critical
/// Scenario: saturated_same_value_publication_crosses_stale_nonzero_lowering
/// Given 最高档有4095个任务，饱和提示为4095
/// When 消费者取出一个任务并在下调前发布同档新任务
/// Then 命中前后窗口，旧下调中间为4094，恢复后补报4095，全部任务恰好取出一次
#[test]
fn saturated_same_value_publication_crosses_stale_nonzero_lowering() {
    task_pool_inner_test_inner::saturated_same_value_publication_crosses_stale_nonzero_lowering();
}

/// 契约：src/lib.rs:78-84；src/task_pool_inner.rs:345-349；design/lock-free-task-pool/01-requirements/needs.md:8,13,15
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法在下调CAS成功后、补报前插入一次发布并观察中间状态。
/// @critical
/// Scenario: publication_after_lowering_is_preserved_by_replenishment
/// Given 最高档有4095个任务且提示饱和
/// When 下调至4094后在补报前发布同档任务
/// Then 命中下调窗口，发布补报后提示为4095，所有4096个任务可排空
#[test]
fn publication_after_lowering_is_preserved_by_replenishment() {
    task_pool_inner_test_inner::publication_after_lowering_is_preserved_by_replenishment();
}

/// 契约：src/lib.rs:78-85；src/task_pool_inner.rs:223-227,287-315；design/lock-free-task-pool/01-requirements/needs.md:5,6,8
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法精确固定跨档下调前后窗口或观察used与编码的中间位。
/// @edgecase
/// Scenario: serial_cross_band_stale_lowering_replenishes_and_preserves_used
/// Given 串行组0排队17个任务，消费一个后剩16个
/// When 在2到1的跨档下调前发布第18个任务
/// Then 命中前后窗口，下调时used仍置位，补报后数量恢复为2；释放后按序排空
#[test]
fn serial_cross_band_stale_lowering_replenishes_and_preserves_used() {
    task_pool_inner_test_inner::serial_cross_band_stale_lowering_replenishes_and_preserves_used();
}

/// 契约：src/lib.rs:78-84；src/task_pool_inner.rs:106-184,345-349；design/lock-free-task-pool/01-requirements/needs.md:8,11
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法构造合法的暂时多报提示并断言取空时其他档位字段不变。
/// @edgecase
/// Scenario: empty_pop_repair_clears_residual_hint_and_keeps_other_fields
/// Given 最高档空队列暂时多报2且最低档有任务7
/// When 以落点1取空最高档并重新选择最低档
/// Then 命中取空窗口、最高档提示清零、最低档字段保留至其任务取出，最终为空
#[test]
fn empty_pop_repair_clears_residual_hint_and_keeps_other_fields() {
    task_pool_inner_test_inner::empty_pop_repair_clears_residual_hint_and_keeps_other_fields();
}

/// 契约：src/lib.rs:78-84；src/task_pool_inner.rs:345-349；design/lock-free-task-pool/01-requirements/needs.md:8,15
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法将恢复者暂停在下调CAS成功后、补报前并检查隐藏的提示。
/// @critical
/// Scenario: paused_repairer_republishes_hidden_task_on_resume
/// Given 最高档有4095个任务且提示饱和，下调前另发布一个同档任务
/// When 恢复者旧快照下调至4094后暂停，另一消费者耗尽当前提示，再恢复修复者
/// Then 暂停期间可暂时隐藏剩余任务，恢复者完成补报后剩余任务重新可见且最终排空
#[test]
fn paused_repairer_republishes_hidden_task_on_resume() {
    task_pool_inner_test_inner::paused_repairer_republishes_hidden_task_on_resume();
}

/// 契约：src/lib.rs:133-140；src/task_pool_inner.rs:106-184；design/lock-free-task-pool/07-design/call-chain.md:66-72
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法精确安排取空与另一消费者取走低档任务之间的重选窗口。
/// @critical
/// Scenario: competing_consumer_retry_reuses_nonzero_random
/// Given 最低档暂时多报1而队列空，低档排队7、8，中档排队9；原输入为121
/// When 首次落在取空的最低档，另一消费者在取空窗口取走低档任务7后恢复本次获取
/// Then 窗口命中、重选使用121按新总权重110取模而得到中档9；低档8仍可取得，最终排空
#[test]
fn competing_consumer_retry_reuses_nonzero_random() {
    task_pool_inner_test_inner::competing_consumer_retry_reuses_nonzero_random();
}

/// 契约：src/lib.rs:117-124；design/lock-free-task-pool/05-modules/decisions.md:19-21
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API不能在拒绝后任务已入队、提示尚未发布的窗口观察串行占用。
/// @critical
/// Scenario: rejected_serial_holds_group_until_hint_publication
/// Given 空组0阈值1、执行器拒绝未执行原句柄11
/// When 拒绝回退完成队列发布并在提示发布前暂停
/// Then 窗口内组0仍占用且11不可获取；完成提示发布后释放占用，11恰好可获取一次
#[test]
fn rejected_serial_holds_group_until_hint_publication() {
    task_pool_inner_test_inner::rejected_serial_holds_group_until_hint_publication();
}

/// 契约：src/lib.rs:97-105,117-124；src/task_pool_inner.rs:257-273；design/lock-free-task-pool/05-modules/decisions.md:19-21
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @kind: unit
/// @seam: inner；公共API无法在原句柄拆解后、回退入队前制造正常展开并检查守卫接管的占用。
/// @critical
/// Scenario: rejected_serial_unwind_before_publication_releases_once
/// Given 空组0已认领，执行器拒绝未执行的带析构计数任务11并交回原句柄
/// When 拆解句柄后在任务入队前正常panic展开
/// Then 展开前组仍占用，展开后组解除占用、任务恰好析构一次且队列为空；同组新任务可提交并获取
#[test]
fn rejected_serial_unwind_before_publication_releases_once() {
    task_pool_inner_test_inner::rejected_serial_unwind_before_publication_releases_once();
}
