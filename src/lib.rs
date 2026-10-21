// 设计：lock-free-task-pool
//! 概率任务池，提供21个串行队列与五档并发队列。
//! 普通调度路径已实现，快速交接尚待完成。

use std::ops::{Deref, DerefMut};
use crate::sync::{Arc, AtomicU64};

#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) mod sync {
    pub use std::cell::UnsafeCell;
    pub use std::sync::Arc;
    pub use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
}

#[cfg(loom)]
#[allow(unused_imports)]
pub(crate) mod sync {
    pub use loom::cell::UnsafeCell;
    pub use loom::sync::Arc;
    pub use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
}
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;

#[path = "task_pool_inner.rs"]
mod inner;

/// @public 概率调度的档位权重，不表示严格执行顺序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWeight {
    Highest = 10_000,
    High = 1_000,
    Middle = 100,
    Low = 10,
    Lowest = 1,
}

/// @public 合法的串行队列索引，取值范围为0..21。
/// 字段私有，外部安全代码只能通过校验构造；可复制并重复用于提交。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SerialQueueIndex(u8);

impl SerialQueueIndex {
    /// @public 校验索引；0..21返回Some，其他值返回None，无副作用且不panic。
    /// 可用于常量求值，在编译期构造固定队列索引。
    pub const fn new(index: usize) -> Option<Self> {
        if index < 21 {
            Some(Self(index as u8))
        } else {
            None
        }
    }

    /// @public 返回已校验的数组索引，始终小于21。
    pub const fn get(self) -> usize {
        self.0 as usize
    }
}

/// @public 外部执行器接收协议；线程及任务实际执行由实现者负责。
/// 回调不得重入同一串行队列的push；不得执行任务后再拒绝。
pub trait TaskExecutor<T>: Sized {
    /// 等待任务数阈值；估计等待量严格小于该值才检查空闲，0禁用快速交接。
    fn fast_path_threshold(&self) -> usize;
    /// 瞬时空闲提示，不保证随后交接一定接受；无池状态副作用。
    fn is_idle(&self) -> bool;
    /// Ok表示接收唯一句柄，持有至实际执行完成（含异步工作）。
    /// Err必须退回未执行的原句柄，池负责重新入队；不返回完成结果。
    /// panic不由池捕获，正常展开释放持有的句柄；不得接受后返回Err。
    fn try_execute(&self, task: TaskHandle<T, Self>) -> Result<(), TaskHandle<T, Self>>;
}

/// @public 静态任务池；通过共享静态引用跨线程使用，无需克隆池对象。
/// 构造返回静态引用，所有提交和获取操作要求静态接收者。
/// 无关闭、清空、取消、超时功能；同队列push调用必须不重叠。
pub struct TaskPool<T, TE: TaskExecutor<T>> {
    inner: Arc<TaskPoolInner<T, TE>>,
}

/// @internal 存储、独占交接及调度提示的状态边界。
/// serial_states:队列i的3*i位used，随后两位min(ceil(n/16),3)，bit63=0。
/// parallel_states:低到高最低/低/中/高各13位，最高12位，字段上限代表饱和。
/// 队列数量直接采样SegQueue::len()，不再维护独立计数。
/// 长度基于队首/队尾位置，可能包含尚未写完的位置，不包含已被出队认领的位置；不是后续操作的保证。
/// 提示可多报；不永久漏报以所有已开始的发布/修复最终继续并完成为前提。
/// 恢复者下调后暂停可使某档提示提前耗尽，依赖其补报后重新可见；无其他线程帮助/扫描兜底。
/// 无锁指不使用互斥锁且CAS竞争允许系统推进，不承诺任务可见性独立于暂停线程，也不承诺wait-free。
/// used是严格独占，完成Release清位。
/// 队列自行发布任务；状态Acquire读取，发布/认领CAS AcqRel、失败Acquire。
/// len内部读取SeqCst索引且可能重试；pop可能等待已预留槽位写入完成，不承诺wait-free。
/// 永久强引用保证句柄裸指针存活；无池回收。允许竞争重试，不保证wait-free。
pub(crate) struct TaskPoolInner<T, TE: TaskExecutor<T>> {
    serial_states: CachePadded<AtomicU64>,
    parallel_states: CachePadded<AtomicU64>,
    serial: [SegQueue<T>; 21],
    parallel: [SegQueue<T>; 5],
    executor: TE,
}

/// @public 唯一任务与串行占用的所有权凭据，不可克隆。
/// serial_index为Some(0..21)时持有相应used位；None代表并发任务。
/// pool_ptr指向永久存活内部池；不得解锁其他队列或二次转移任务。
/// Drop仅解除串行占用，字段T随后正常析构；执行器须持有到实际任务完成。
/// Send条件为T:Send、TE:Send+Sync；Sync另要求T:Sync，空芯阶段尚不实现unsafe标记。
pub struct TaskHandle<T, TE: TaskExecutor<T>> {
    task: T,
    pool_ptr: *const TaskPoolInner<T, TE>,
    serial_index: Option<usize>,
}

impl<T: Send + 'static, TE: TaskExecutor<T> + Send + Sync + 'static> TaskPool<T, TE> {
    /// @public 创建空池并返回永久有效的共享引用；无销毁回收能力。
    /// executor所有权转移至池；分配失败沿用分配器行为，不返回可恢复错误。
    pub fn new(executor: TE) -> &'static Self {
        inner::new_inner(executor)
    }
}

impl<T, TE: TaskExecutor<T>> TaskPool<T, TE> {
    /// @public 提交串行任务；queue_index须由SerialQueueIndex校验构造，同组调用不得重叠。
    /// 返回时任务入队或已被执行器接受；非幂等，不返回完成结果。
    /// 越界索引在SerialQueueIndex::new处返回None；不另设独立计数容量限制。
    /// 空队列可认领后快速交接；拒绝由内部守卫保持占用，入队发布后清占用，不插队。
    /// 拒绝回退由守卫承接释放责任；已发布任务必须完成提示发布后再清占用。
    pub fn push_serial(&'static self, task: T, queue_index: SerialQueueIndex) {
        inner::push_serial_inner(&self.inner, task, queue_index.get())
    }

    /// @public 提交指定档位任务；不限制同档任务同时执行。
    /// 返回时入队或已接受；拒绝原任务回退入队，不丢失、不重复，非幂等。
    /// 不另设独立计数容量限制；不管理工作线程，分配失败沿用分配器行为。
    pub fn push_parallel(&'static self, task: T, weight: TaskWeight) {
        inner::push_parallel_inner(&self.inner, task, weight)
    }

    /// @public 概率获取；Some转移唯一任务，串行used保持至句柄释放。
    /// None表示本轮无可取得候选，不保证并发精确空快照；底层队列可能等待进行中的槽位写入。
    /// CAS竞争重读，取空候选本轮排除；不保证有限等待或严格优先顺序。
    /// random接受整个u64范围，由调用方提供；按当前总权重取模选择。
    /// 重试复用同一random；允许取模偏差，同快照同输入选择一致。
    pub fn pop(&'static self, random: u64) -> Option<TaskHandle<T, TE>> {
        inner::pop_inner(&self.inner, random)
    }
}

impl<T, TE: TaskExecutor<T>> Deref for TaskHandle<T, TE> {
    type Target = T;
    /// @public 借用任务，不转移所有权或释放串行占用。
    fn deref(&self) -> &T { inner::handle_ref_inner(self) }
}
impl<T, TE: TaskExecutor<T>> DerefMut for TaskHandle<T, TE> {
    /// @public 独占借用任务，不转移所有权或释放串行占用。
    fn deref_mut(&mut self) -> &mut T { inner::handle_mut_inner(self) }
}
impl<T, TE: TaskExecutor<T>> Drop for TaskHandle<T, TE> {
    /// @public 串行Release清used，保留数量提示；并发不解锁，正常展开同样执行。
    fn drop(&mut self) { inner::drop_handle_inner(self) }
}
