//! 并发与串行队列闭环实现；快速交接、拒绝回退及释放。
// 设计：lock-free-task-pool
#![allow(dead_code, unused_variables)]
use crate::{TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use std::mem::ManuallyDrop;
use std::ptr;



use crate::sync::{AtomicU64, Ordering};

/// @internal 一次选择结果，索引分别为0..21与最低至最高0..5。
pub(crate) enum QueueSelection {
    Serial(usize),
    Parallel(usize),
}

// SAFETY: 句柄独占T，转移要求T:Send；静态池保持pool_ptr有效。
// 访问共享池要求执行器Send+Sync；并发Drop不访问任务以外的可变状态。
unsafe impl<T: Send, TE: TaskExecutor<T> + Send + Sync> Send for TaskHandle<T, TE> {}
// SAFETY: 共享句柄只暴露&T，故另外要求T:Sync；可变借用仍由&mut Self约束。
unsafe impl<T: Send + Sync, TE: TaskExecutor<T> + Send + Sync> Sync for TaskHandle<T, TE> {}

const PARALLEL_WEIGHTS: [u64; 5] = [1, 10, 100, 1_000, 10_000];

// 私有测试接缝：仅普通库单元测试存在，不进入生产或Loom构建。
#[cfg(all(test, not(loom)))]
#[path = "task_pool_inner_test.rs"]
mod recovery_tests;


fn parallel_field(queue_index: usize) -> (u32, u64) {
    let shift = queue_index as u32 * 13;
    let limit = if queue_index == 4 { 4095 } else { 8191 };
    (shift, limit)
}


/// @internal 唯一串行释放责任；持有到回退发布完成，展开也清used。
pub(super) struct SerialReleaseGuard<T, TE: TaskExecutor<T>> {
    pool_ptr: *const TaskPool<T, TE>,
    queue_index: usize,
}
impl<T, TE: TaskExecutor<T>> Drop for SerialReleaseGuard<T, TE> {
    fn drop(&mut self) {
        drop_serial_guard_inner(self)
    }
}
/// @internal
/// 只调用release_serial_inner清目标used，不动任务或数量提示，不panic。
pub(super) fn drop_serial_guard_inner<T, TE: TaskExecutor<T>>(
    guard: &mut SerialReleaseGuard<T, TE>,
) {
    // SAFETY: 守卫仅从原串行句柄转移释放责任，池由静态引用创建并永久存活。
    let pool = unsafe { &*guard.pool_ptr };
    release_serial_inner(pool, guard.queue_index);
}

/// @internal

/// 创建21+5空队列及零状态，保存executor；Box::leak直接返回静态池，无Arc或内部包装类型。分配失败沿用分配器。
pub(super) fn new_inner<T: Send + 'static, TE: TaskExecutor<T> + Send + Sync + 'static>(
    executor: TE,
) -> &'static TaskPool<T, TE> {
    Box::leak(Box::new(TaskPool {
        serial_states: CachePadded::new(AtomicU64::new(0)),
        parallel_states: CachePadded::new(AtomicU64::new(0)),
        serial: std::array::from_fn(|_| SegQueue::new()),
        parallel: std::array::from_fn(|_| SegQueue::new()),
        executor,
    }))
}

/// @internal

/// 先检查index<21，否则panic；估算等待量，小于阈值且idle时尝试空队列认领；成功交付返回；拒绝into_task_inner取得T与SerialReleaseGuard，持守卫入队并发布，然后Drop守卫清used。队列发布后不得主动panic或回调外部，必须完成提示发布。其余普通入队。同组push不得重叠。
pub(super) fn push_serial_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    task: T,
    queue_index: usize,
) {
    assert!(queue_index < 21, "serial queue index must be in 0..21");
    let serial_state = pool.serial_states.load(Ordering::Acquire);
    let parallel_states = pool.parallel_states.load(Ordering::Acquire);
    if estimate_waiting_inner(serial_state, parallel_states) < pool.executor.fast_path_threshold()
        && pool.executor.is_idle()
        && try_claim_serial_inner(pool, queue_index, true)
    {
        let handle = TaskHandle {
            task,
            pool_ptr: pool,
            serial_index: Some(queue_index),
        };
        match pool.executor.try_execute(handle) {
            Ok(()) => return,
            Err(handle) => {
                // 守卫一直持有used，直到队列和提示均发布完毕；预发布展开也会释放。
                let (task, guard) = into_task_inner(handle);
                enqueue_serial_inner(pool, task, queue_index);
                drop(guard);
                return;
            }
        }
    }
    enqueue_serial_inner(pool, task, queue_index);
}

/// @internal

/// 映射档位；估算等待量，少于阈值且idle则try_execute；接受返回，拒绝拆解返回任务；普通或拒绝任务走enqueue_parallel。
pub(super) fn push_parallel_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    task: T,
    weight: TaskWeight,
) {
    let queue_index = match weight {
        TaskWeight::Lowest => 0,
        TaskWeight::Low => 1,
        TaskWeight::Middle => 2,
        TaskWeight::High => 3,
        TaskWeight::Highest => 4,
    };
    let serial_state = pool.serial_states.load(Ordering::Acquire);
    let parallel_states = pool.parallel_states.load(Ordering::Acquire);
    if estimate_waiting_inner(serial_state, parallel_states) < pool.executor.fast_path_threshold()
        && pool.executor.is_idle()
    {
        let handle = TaskHandle {
            task,
            pool_ptr: pool,
            serial_index: None,
        };
        match pool.executor.try_execute(handle) {
            Ok(()) => return,
            Err(handle) => {
                let (task, _guard) = into_task_inner(handle);
                enqueue_parallel_inner(pool, task, queue_index);
                return;
            }
        }
    }
    enqueue_parallel_inner(pool, task, queue_index);
}

/// @internal

/// 建立本轮排除位图；Acquire读两状态，计算总权重，为零返回None；随机选候选。串行CAS认领used；并发普通提示CAS减1、饱和不减。失败重读；取空修复提示并排除候选，串行清used。成功后串行依据queue.len()下调编码，并发仅饱和分支修复；构造唯一句柄返回。
pub(super) fn pop_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    random: u64,
) -> Option<TaskHandle<T, TE>> {
    let mut excluded_serial = 0u32;
    let mut excluded_parallel = 0u8;
    loop {
        let serial_state = pool.serial_states.load(Ordering::Acquire);
        let parallel_states = pool.parallel_states.load(Ordering::Acquire);
        let total = total_weight_inner(serial_state, parallel_states, excluded_serial, excluded_parallel);
        if total == 0 {
            return None;
        }
        let selection = select_queue_inner(
            serial_state,
            parallel_states,
            excluded_serial,
            excluded_parallel,
            random % total,
        );
        let queue_index = match selection {
            Some(QueueSelection::Serial(index)) => {
                if !try_claim_serial_inner(pool, index, false) {
                    // 占用或提示已变，重新读取整个调度快照。
                    continue;
                }
                let queue = &pool.serial[index];
                if let Some(task) = queue.pop() {
                    // 数量修复期间持续持有used，剩余长度直接从队列采样。
                    lower_serial_hint_inner(pool, index);
                    return Some(TaskHandle {
                        task,
                        pool_ptr: pool,
                        serial_index: Some(index),
                    });
                }
                // 提示可能多报；取空校正后本轮不再尝试该组。
                lower_serial_hint_inner(pool, index);
                release_serial_inner(pool, index);
                excluded_serial |= 1 << index;
                continue;
            }
            Some(QueueSelection::Parallel(index)) => index,
            // 总权重与选择使用同一快照和排除集，正权重必有候选。
            None => unreachable!("positive snapshot must select a queue"),
        };
        let (shift, limit) = parallel_field(queue_index);
        let hint = (parallel_states >> shift) & limit;
        let saturated = hint == limit;
        if !saturated {
            let next = parallel_states - (1u64 << shift);
            if pool
                .parallel_states
                .compare_exchange_weak(parallel_states, next, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // 其他档位也可能竞争该字；重读并重新计算整个选择快照。
                continue;
            }
        }
        let queue = &pool.parallel[queue_index];
        if let Some(task) = queue.pop() {

            if saturated {
                repair_parallel_hint_inner(pool, queue_index);
            }
            return Some(TaskHandle {
                task,
                pool_ptr: pool,
                serial_index: None,
            });
        }
        #[cfg(all(test, not(loom)))]
        recovery_tests::checkpoint("parallel_empty_pop");
        // 提示可多报；取空校正后同一轮不再访问该候选。
        repair_parallel_hint_inner(pool, queue_index);
        excluded_parallel |= 1 << queue_index;
    }
}

/// @internal

/// 返回对task的共享借用，无状态变化。
pub(super) fn handle_ref_inner<T, TE: TaskExecutor<T>>(handle: &TaskHandle<T, TE>) -> &T {
    &handle.task
}

/// @internal

/// 返回对task的独占借用，无状态变化。
pub(super) fn handle_mut_inner<T, TE: TaskExecutor<T>>(handle: &mut TaskHandle<T, TE>) -> &mut T {
    &mut handle.task
}

/// @internal

/// 若serial_index有值，凭永久有效pool_ptr释放该串行used；否则无操作。不移动或显式析构task，交字段析构处理。
pub(super) fn drop_handle_inner<T, TE: TaskExecutor<T>>(handle: &mut TaskHandle<T, TE>) {
    if let Some(index) = handle.serial_index {
        // SAFETY: 句柄只由池构造，pool_ptr指向永久存活的静态池。
        // 句柄不可克隆，独占该组used；这里只借用共享池并清原子位，不移动任务。
        let pool = unsafe { &*handle.pool_ptr };
        release_serial_inner(pool, index);
    }
    // 并发没有占用位；返回后由Rust正常析构task，不解引用pool_ptr。
}

/// @internal

/// 读取元数据后ManuallyDrop包裹句柄；无分配、无外部回调、无panic的连续步骤内ptr::read转移T并建立串行SerialReleaseGuard，返回(T,守卫)，并发为None。原句柄不Drop；守卫唯一承接占用责任，正常发布后或预发布展开时释放，不提前解锁。
pub(super) fn into_task_inner<T, TE: TaskExecutor<T>>(
    handle: TaskHandle<T, TE>,
) -> (T, Option<SerialReleaseGuard<T, TE>>) {
    // 原句柄不再Drop；先承接串行释放责任，再将唯一的T移出。
    // 此连续区间无分配、外部回调或会主动panic的操作。
    let handle = ManuallyDrop::new(handle);
    let guard = handle.serial_index.map(|queue_index| SerialReleaseGuard {
        pool_ptr: handle.pool_ptr,
        queue_index,
    });
    // SAFETY: ManuallyDrop阻止原句柄析构；只读取T一次，唯一所有权随返回值转移。
    let task = unsafe { ptr::read(&handle.task) };
    (task, guard)
}

/// @internal

/// 以count/16加count%16非零指示计算向上取整，再min(3)；避免count+15溢出。
pub(super) fn encode_serial_count_inner(count: u64) -> u64 {
    (count / 16 + u64::from(count % 16 != 0)).min(3)
}


/// @internal

/// 先SegQueue.push，最后publish_serial_hint；发布后无外部回调/主动panic点，发布成功后必须完成提示发布。串行回退调用者持SerialReleaseGuard至此完成。
pub(super) fn enqueue_serial_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    task: T,
    queue_index: usize,
) {
    let queue = &pool.serial[queue_index];

    queue.push(task);
    #[cfg(all(test, not(loom)))]
    recovery_tests::checkpoint("serial_after_queue_push_before_hint");
    // 已发布任务必须完成提示发布；此后不调用执行器或设置主动panic点。
    publish_serial_hint_inner(pool, queue_index);
}

/// @internal

/// 先SegQueue.push，最后publish_parallel_hint；发布后无外部回调/主动panic点，发布成功后必须完成提示发布。
pub(super) fn enqueue_parallel_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    task: T,
    queue_index: usize,
) {
    let queue = &pool.parallel[queue_index];

    queue.push(task);
    // SegQueue负责任务可见性；队列发布后必须完成提示发布。
    publish_parallel_hint_inner(pool, queue_index);
}

/// @internal

/// 循环先Acquire读serial_states再采样queue.len()；目标编码max(原编码,encode(len))；AcqRel CAS保留used及其他字段，即使相同也做发布CAS；失败重读。
pub(super) fn publish_serial_hint_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
) {
    let shift = queue_index * 3 + 1;
    let mask = 3u64 << shift;
    let queue = &pool.serial[queue_index];
    loop {
        let state = pool.serial_states.load(Ordering::Acquire);
        let hint = (state >> shift) & 3;
        let target = hint.max(encode_serial_count_inner(queue.len() as u64));
        let next = (state & !mask) | (target << shift);
        // 同值也执行发布CAS；只改目标数量，保留used和其他组的全部位。
        if pool
            .serial_states
            .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

/// @internal

/// 循环先Acquire读状态再采样queue.len()；原编码<=encode(len)则返回；否则AcqRel CAS仅下调数量，失败重读。每次成功下调后调用publish_serial_hint_inner完成只增协调补报，不限于写零；避免同值发布被旧目标覆盖。不跨档不写。
pub(super) fn lower_serial_hint_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
) {
    let shift = queue_index * 3 + 1;
    let mask = 3u64 << shift;
    let queue = &pool.serial[queue_index];
    loop {
        let state = pool.serial_states.load(Ordering::Acquire);
        let hint = (state >> shift) & 3;
        let target = encode_serial_count_inner(queue.len() as u64);
        #[cfg(all(test, not(loom)))]
        recovery_tests::checkpoint("serial_before_lower_cas");
        if hint <= target {
            return;
        }
        let next = (state & !mask) | (target << shift);
        if pool
            .serial_states
            .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            #[cfg(all(test, not(loom)))]
            recovery_tests::checkpoint("serial_after_lower_cas");
            // 旧快照下调可覆盖同值发布，非零下调也必须重新读状态/队列长度补报。
            publish_serial_hint_inner(pool, queue_index);
            return;
        }
    }
}

/// @internal

/// 确定档位offset/mask，上限8191或最高4095；循环AcqRel CAS饱和加1，仅改目标字段，饱和相同值仍执行发布CAS，失败重读。
pub(super) fn publish_parallel_hint_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
) {
    let (shift, limit) = parallel_field(queue_index);
    let mask = limit << shift;
    loop {
        let state = pool.parallel_states.load(Ordering::Acquire);
        let hint = (state >> shift) & limit;
        let next = (state & !mask) | ((hint + 1).min(limit) << shift);
        // 饱和同值也发布，不能因值不变提前返回。
        if pool
            .parallel_states
            .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

/// @internal

/// 仅饱和取出或取空修复调用，普通提示减零不调用。下调循环：先Acquire读parallel_states，再采样queue.len()为n；target=min(n,LIMIT)。字段<=target则返回，否则AcqRel CAS仅下调目标字段，失败Acquire并从头重读。
/// 每次成功下调（含非零）后必须补报循环：先Acquire读parallel_states，再采样queue.len()为n；target=max(当前字段,min(n,LIMIT))；AcqRel CAS保留其他字段，即使同值也执行，失败Acquire从头重读，成功结束。补报不修改队列，不复用下调前快照。
/// 饱和同值发布可能穿过旧快照下调，因此不限写零复查。补报允许覆盖已认领未取出的任务为多报，由取空后修复收敛；任务唯一性由SegQueue保证。
pub(super) fn repair_parallel_hint_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
) {
    let (shift, limit) = parallel_field(queue_index);
    let mask = limit << shift;
    let queue = &pool.parallel[queue_index];
    loop {
        // 必须先读发布状态，再采样queue.len()，CAS失败全量重读。
        let state = pool.parallel_states.load(Ordering::Acquire);
        let hint = (state >> shift) & limit;
        let target = (queue.len() as u64).min(limit);
        #[cfg(all(test, not(loom)))]
        recovery_tests::checkpoint("parallel_before_lower_cas");
        if hint <= target {
            return;
        }
        let next = (state & !mask) | (target << shift);
        if pool
            .parallel_states
            .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }
    }
    #[cfg(all(test, not(loom)))]
    recovery_tests::checkpoint("parallel_after_lower_cas");
    // 每次下调后都补报：饱和同值发布可穿过旧快照下调，非零目标也不例外。
    // 不沿用下调前长度，且不降低其他恢复者/提交者已发布的提示。
    loop {
        let state = pool.parallel_states.load(Ordering::Acquire);
        let hint = (state >> shift) & limit;
        let target = hint.max((queue.len() as u64).min(limit));
        let next = (state & !mask) | (target << shift);
        if pool
            .parallel_states
            .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

/// @internal

/// Release fetch_and仅清目标used，保留所有数量字段与其他占用位。
pub(super) fn release_serial_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
) {
    pool.serial_states
        .fetch_and(!(1u64 << (queue_index * 3)), Ordering::Release);
}

/// @internal

/// Acquire读状态；used非零失败。快速路径require_empty还需队列为空（同组push不重叠）；普通路径要求编码非零。AcqRel CAS置used保留其余位，失败Acquire重读条件；无可认领条件返回false。
pub(super) fn try_claim_serial_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
    queue_index: usize,
    require_empty: bool,
) -> bool {
    let shift = queue_index * 3;
    let used = 1u64 << shift;
    loop {
        let state = pool.serial_states.load(Ordering::Acquire);
        if state & used != 0 {
            return false;
        }
        if require_empty {
            // 同组push不重叠；有在途或已入队任务时不能越过它快速交接。
            if !pool.serial[queue_index].is_empty() {
                return false;
            }
        } else if (state >> (shift + 1)) & 3 == 0 {
            return false;
        }
        if pool
            .serial_states
            .compare_exchange_weak(state, state | used, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
        // 即使其他组引起CAS失败，也重新检查本组占用和数量条件。
    }
}

/// @internal

/// 所有串行编码含占用组求和乘16，加五并发提示求和；压缩近似仅供快速阈值判断。
pub(super) fn estimate_waiting_inner(serial_state: u64, parallel_states: u64) -> usize {
    let mut waiting = 0usize;
    for index in 0..21 {
        waiting += (((serial_state >> (index * 3 + 1)) & 3) * 16) as usize;
    }
    for index in 0..5 {
        let (shift, limit) = parallel_field(index);
        waiting += ((parallel_states >> shift) & limit) as usize;
    }
    waiting
}

/// @internal

/// 按串行0..21、并发最低至最高排列区间，跳过排除/占用/零提示。串行编码*100，并发提示*档位值；ticket须在总权重区间内，无候选返回None。
pub(super) fn select_queue_inner(
    serial_state: u64,
    parallel_states: u64,
    excluded_serial: u32,
    excluded_parallel: u8,
    ticket: u64,
) -> Option<QueueSelection> {
    let mut remaining = ticket;
    for index in 0..21 {
        let field = (serial_state >> (index * 3)) & 7;
        if excluded_serial & (1 << index) != 0 || field & 1 != 0 {
            continue;
        }
        let interval = (field >> 1) * 100;
        if remaining < interval {
            return Some(QueueSelection::Serial(index));
        }
        remaining -= interval;
    }
    for (index, weight) in PARALLEL_WEIGHTS.into_iter().enumerate() {
        if excluded_parallel & (1 << index) != 0 {
            continue;
        }
        let (shift, limit) = parallel_field(index);
        let interval = ((parallel_states >> shift) & limit) * weight;
        if remaining < interval {
            return Some(QueueSelection::Parallel(index));
        }
        remaining -= interval;
    }
    None
}

/// @internal

/// 与选择芯使用相同顺序和过滤条件，u64累加编码*100及并发提示*档位权重，空集为0。
pub(super) fn total_weight_inner(
    serial_state: u64,
    parallel_states: u64,
    excluded_serial: u32,
    excluded_parallel: u8,
) -> u64 {
    let mut total = 0;
    for index in 0..21 {
        let field = (serial_state >> (index * 3)) & 7;
        if excluded_serial & (1 << index) == 0 && field & 1 == 0 {
            total += (field >> 1) * 100;
        }
    }
    for (index, weight) in PARALLEL_WEIGHTS.into_iter().enumerate() {
        if excluded_parallel & (1 << index) == 0 {
            let (shift, limit) = parallel_field(index);
            total += ((parallel_states >> shift) & limit) * weight;
        }
    }
    total
}



