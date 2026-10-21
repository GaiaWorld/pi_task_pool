// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/fast_path_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
#[path = "support/fast_path_test_inner.rs"]
mod fast_path_test_inner;

/// 契约：src/lib.rs:64-71,117-130; needs.md:9
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: threshold_zero_never_queries_idle
/// Given 阈值为0，执行器空闲
/// When 分别提交串行11及并发22
/// Then 均不查询空闲或交接，两任务可从池中获取
#[test]
fn threshold_zero_never_queries_idle() { fast_path_test_inner::threshold_zero_never_queries_idle(); }

/// 契约：src/lib.rs:64-71,117-130; needs.md:9
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: estimated_wait_at_threshold_skips_idle
/// Given 阈值为1且已排队一个并发任务，或阈值为16且已排队一个串行任务
/// When 分别提交第二个并发或另一组串行任务
/// Then 估计量达到阈值时不再查询空闲，两任务仍可从池获取
#[test]
fn estimated_wait_at_threshold_skips_idle() { fast_path_test_inner::estimated_wait_at_threshold_skips_idle(); }

/// 契约：src/lib.rs:64-71,117-124; design/lock-free-task-pool/01-requirements/needs.md:9
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @edgecase
/// Scenario: serial_estimate_below_threshold_checks_idle_and_hands_off
/// Given 组0排队一个任务11，阈值为17且执行器先非空闲后空闲
/// When 向空队列组1提交任务22
/// Then 串行估计16严格小于17，检查空闲并将22交给执行器；11仍在池中
#[test]
fn serial_estimate_below_threshold_checks_idle_and_hands_off() {
    fast_path_test_inner::serial_estimate_below_threshold_checks_idle_and_hands_off();
}

/// 契约：src/lib.rs:64-71,117-124; design/lock-free-task-pool/01-requirements/needs.md:5,9
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: nonempty_serial_queue_never_hands_off_successor
/// Given 组0已有等待任务11，阈值为17且执行器由非空闲变为空闲
/// When 向同组提交任务22
/// Then 22不被交接，依序从池中取得11、22，无插队或重复
#[test]
fn nonempty_serial_queue_never_hands_off_successor() {
    fast_path_test_inner::nonempty_serial_queue_never_hands_off_successor();
}

/// 契约：src/lib.rs:64-71,126-139; needs.md:9
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: idle_hint_controls_parallel_handoff
/// Given 空池阈值为2，执行器空闲提示先为假再为真
/// When 依次提交并发任务11和22
/// Then 11入队，22被执行器接收且不在队列
#[test]
fn idle_hint_controls_parallel_handoff() { fast_path_test_inner::idle_hint_controls_parallel_handoff(); }

/// 契约：src/lib.rs:68-71,97-105,117-124,133-154; needs.md:6,12
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// Scenario: accepted_serial_handle_owns_group_until_drop
/// Given 空池阈值为1，组0任务11已由执行器持有句柄
/// When 同组提交22并尝试获取，然后释放11
/// Then 22不提前交接或取出，释放后可取22且仅出现一次
#[test]
fn accepted_serial_handle_owns_group_until_drop() { fast_path_test_inner::accepted_serial_handle_owns_group_until_drop(); }

/// 契约：src/lib.rs:68-71,117-139; needs.md:9-10
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @critical
/// @negative
/// Scenario: rejected_serial_task_returns_once_and_preserves_fifo
/// Given 空池阈值为1，执行器拒绝组20原句柄11
/// When 随后提交同组22并依次获取释放
/// Then 依序仅获取11、22，最后为空
#[test]
fn rejected_serial_task_returns_once_and_preserves_fifo() { fast_path_test_inner::rejected_serial_task_returns_once_and_preserves_fifo(); }

/// 契约：src/lib.rs:68-71,126-139; needs.md:10
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @negative
/// Scenario: rejected_parallel_task_remains_visible_exactly_once
/// Given 空池阈值为1，执行器拒绝最高档原句柄42
/// When 从池中获取并释放，随后再次获取
/// Then 取到42且随后为空，任务仅析构一次
#[test]
fn rejected_parallel_task_remains_visible_exactly_once() { fast_path_test_inner::rejected_parallel_task_remains_visible_exactly_once(); }

/// 契约：src/lib.rs:68-71,117-124,133-154; needs.md:10
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @negative
/// Scenario: rejected_serial_task_releases_group_after_publication
/// Given 空池阈值为1且执行器拒绝组5任务11
/// When 获取并释放11后，再提交同组任务22
/// Then 22可再次快速交接，且11只取到一次
#[test]
fn rejected_serial_task_releases_group_after_publication() { fast_path_test_inner::rejected_serial_task_releases_group_after_publication(); }

/// 契约：src/lib.rs:68-71,117-124,133-154; needs.md:12
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// Scenario: accepted_serial_handle_unwind_releases_group
/// Given 执行器已接收组10任务11并持有句柄，同组任务22等待
/// When 持有11句柄的工作正常panic展开
/// Then 22可被获取，组10未永久占用
#[test]
fn accepted_serial_handle_unwind_releases_group() { fast_path_test_inner::accepted_serial_handle_unwind_releases_group(); }

/// 契约：src/lib.rs:68-71,117-124,133-154; needs.md:12
/// @api:src/lib.rs
/// @impl:src/task_pool_inner.rs
/// @negative
/// Scenario: executor_panic_unwinds_serial_handle_and_releases_group
/// Given 空池阈值为1，执行器收到组10任务11后正常panic展开
/// When 捕获panic后提交组10任务22
/// Then 22可被交接或取出，组10未永久占用
#[test]
fn executor_panic_unwinds_serial_handle_and_releases_group() { fast_path_test_inner::executor_panic_unwinds_serial_handle_and_releases_group(); }
