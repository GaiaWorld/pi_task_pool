// @api:src/queue_length_estimates.rs
// @impl:src/queue_length_estimates_inner.rs
// @test:tests/queue_length_estimates_test.rs
// @design-doc:design/queue-length-estimates/07-design/call-chain.md
#[path = "support/queue_length_estimates_test_inner.rs"]
mod queue_length_estimates_test_inner;

/// 契约：src/queue_length_estimates.rs:22
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: public_queries_do_not_allocate_on_the_calling_thread
/// Given 已准备空池和串行并发均非空的池，线程局部分配计数器已由真实分配校验有效
/// When 仅在每次公共查询调用期间开启当前线程的分配监测，各池查询100次
/// Then alloc、alloc_zeroed和realloc调用数均为零，其他线程分配不计入
#[test]
fn public_queries_do_not_allocate_on_the_calling_thread() {
    queue_length_estimates_test_inner::public_queries_do_not_allocate_on_the_calling_thread();
}

/// 契约：src/queue_length_estimates.rs:4-6,19-23
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: empty_pool_returns_all_zeroes_through_shared_borrow
/// Given 新建空池并将其借用为非static共享引用
/// When 查询全部队列估计
/// Then 返回serial的21个零和parallel的5个零
#[test]
fn empty_pool_returns_all_zeroes_through_shared_borrow() {
    queue_length_estimates_test_inner::empty_pool_returns_all_zeroes_through_shared_borrow();
}

/// 契约：src/queue_length_estimates.rs:5-6；design/queue-length-estimates/01-requirements/needs.md:8-9
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: serial_bands_and_all_group_positions_are_decoded
/// Given 每组按轮换排列提交0、1、16、17、32、33、49个任务，共七轮独立池
/// When 每轮查询全部队列估计
/// Then 对应估计分别为0、16、16、32、32、48、48，覆盖全部21组及首末组
#[test]
fn serial_bands_and_all_group_positions_are_decoded() {
    queue_length_estimates_test_inner::serial_bands_and_all_group_positions_are_decoded();
}

/// 契约：src/queue_length_estimates.rs:7,25
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: occupied_groups_keep_waiting_estimates_without_counting_used
/// Given 组0排队18个任务、组20排队1个任务，并各取出一个且持有句柄
/// When 重复查询占用中的组
/// Then 组0仍报32、组20报0，其余项为零；查询不解除占用，释放后组0余下17个任务可按序取得
#[test]
fn occupied_groups_keep_waiting_estimates_without_counting_used() {
    queue_length_estimates_test_inner::occupied_groups_keep_waiting_estimates_without_counting_used();
}

/// 契约：src/queue_length_estimates.rs:5-6；design/queue-length-estimates/01-requirements/needs.md:11-12
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: parallel_fields_preserve_order_and_saturate_independently
/// Given 五档依次为最低至最高，分别构造全零、不同非零值、上限减1、上限、上限加1五组独立池
/// When 查询每个池
/// Then 各档对应原提示，上限分别为8191、8191、8191、8191、4095，超过上限时截断且字段互不污染
#[test]
fn parallel_fields_preserve_order_and_saturate_independently() {
    queue_length_estimates_test_inner::parallel_fields_preserve_order_and_saturate_independently();
}

/// 契约：src/queue_length_estimates.rs:21-25；design/queue-length-estimates/01-requirements/needs.md:14-15
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: repeated_queries_preserve_tasks_and_never_call_executor
/// Given 组0有任务10、11、12，最低档有任务20、21，且执行器三个回调均计数
/// When 连续查询100次
/// Then 每次返回组0为16、最低档为2且其他项零；三个回调计数不变，全部5个任务仍恰好取得一次
#[test]
fn repeated_queries_preserve_tasks_and_never_call_executor() {
    queue_length_estimates_test_inner::repeated_queries_preserve_tasks_and_never_call_executor();
}

/// 契约：src/queue_length_estimates.rs:9-15,20
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: returned_arrays_are_independently_owned
/// Given 组20有任务7、最高档有任务8，并已取得一份查询结果
/// When 把该结果的两个数组全部改成usize最大值后重新查询
/// Then 新结果仍只有组20为16、最高档为1，原结果保持调用方写入值
#[test]
fn returned_arrays_are_independently_owned() {
    queue_length_estimates_test_inner::returned_arrays_are_independently_owned();
}

/// 契约：src/queue_length_estimates.rs:7-8,25
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: fast_accepted_tasks_are_not_added_to_estimates
/// Given 执行器阈值100且空闲并接受任务，组0提交7、最高档提交8，接受计数为2
/// When 重复查询
/// Then 全26项为零，接受计数仍为2且池无排队任务
#[test]
fn fast_accepted_tasks_are_not_added_to_estimates() {
    queue_length_estimates_test_inner::fast_accepted_tasks_are_not_added_to_estimates();
}

/// 契约：src/queue_length_estimates.rs:6,8,22,24
/// @api:src/queue_length_estimates.rs
/// @impl:src/queue_length_estimates_inner.rs
/// @kind: contract
/// @critical
/// Scenario: concurrent_queries_stay_in_range_and_preserve_all_tasks
/// Given 两个生产者和两个观察者同时起跑，生产者持续向组20提交偶数编号、向最高档提交奇数编号，直到两个观察者完成
/// When 每个观察者先查询并记录提交进度，再自由交错查询至少2000次，直到两个生产者的提交计数都超过该起点
/// Then 每次serial只取0、16、32、48且parallel前四项不超过8191、末项不超过4095；每个查询窗口内两生产者均有提交进展，回调仅来自提交，最终按提交计数核对全部任务各一次，不要求统一快照
#[test]
fn concurrent_queries_stay_in_range_and_preserve_all_tasks() {
    queue_length_estimates_test_inner::concurrent_queries_stay_in_range_and_preserve_all_tasks();
}
