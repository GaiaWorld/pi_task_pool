// 设计：lock-free-task-pool
//! 概率任务池，提供21个串行队列与五档并发队列。
//! 排队任务由 [`TaskPool::pop`] 按压缩数量提示和档位权重选择；快速交接不参与权重抽签。
//! 执行器接受快速交接后任务不入队，拒绝则回退入队。线程和实际执行由调用方负责。
//!
//! # 基本用法
//!
//! ```
//! use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
//!
//! struct QueuedOnly;
//! impl TaskExecutor<String> for QueuedOnly {
//!     fn fast_path_threshold(&self) -> usize { 0 }
//!     fn is_idle(&self) -> bool { false }
//!     fn try_execute(&self, task: TaskHandle<String, Self>)
//!         -> Result<(), TaskHandle<String, Self>> { Err(task) }
//! }
//!
//! let pool = TaskPool::new(QueuedOnly);
//! let group = SerialQueueIndex::new(0).expect("合法串行组");
//! pool.push_serial("串行任务".into(), group);
//! pool.push_parallel("并发任务".into(), TaskWeight::High);
//! // 示例固定输入；实际概率调度应由调用方提供覆盖整个 u64 范围的随机数。
//! while let Some(mut task) = pool.pop(0) {
//!     task.push_str("：已处理");
//!     // 实际完成后释放句柄，允许同组的下一个串行任务被获取。
//!     drop(task);
//! }
//! ```
//!
//! 池通过泄漏分配取得静态生命周期，没有销毁回收、关闭、取消或清空 API。
//! 同一串行组的提交调用不得重叠；不同组和并发任务允许多线程提交。

use std::ops::{Deref, DerefMut};
use crate::sync::AtomicU64;

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

mod queue_length_estimates;
mod queue_length_estimates_inner;
pub use queue_length_estimates::QueueLengthEstimates;

/// @public 概率调度的档位权重，不表示严格执行顺序。
/// 仅影响排队后的选择；并发档位的有效权重是数量提示乘以本档权重。
/// 快速交接不比较档位，低权重任务也可能先于已排队的高权重任务执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWeight {
    /// 最高档，单位提示权重为 10000。
    Highest = 10_000,
    /// 高档，单位提示权重为 1000。
    High = 1_000,
    /// 中档，单位提示权重为 100；串行队列的每个压缩数量档也使用此权重。
    Middle = 100,
    /// 低档，单位提示权重为 10。
    Low = 10,
    /// 最低档，单位提示权重为 1。
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
/// 这些回调用于提交时的快速交接，普通 `pop` 不会自动调用执行器。
/// 回调不得重入同一串行队列的push；不得执行任务后再拒绝。
pub trait TaskExecutor<T>: Sized {
    /// 等待任务数阈值；估计等待量严格小于该值才检查空闲，0禁用快速交接。
    /// 比较的是全池压缩估计量，允许高估或低估，不是精确排队长度。
    fn fast_path_threshold(&self) -> usize;
    /// 瞬时空闲提示，不保证随后交接一定接受；无池状态副作用。
    fn is_idle(&self) -> bool;
    /// Ok表示接收唯一句柄，持有至实际执行完成（含异步工作）。
    /// 接受后任务不入队、不参与权重抽签；可以同步完成，也可以把句柄移交工作线程。
    /// Err必须退回未执行的原句柄，池负责重新入队；不返回完成结果。
    /// panic不由池捕获，正常展开释放持有的句柄；不得接受后返回Err。
    fn try_execute(&self, task: TaskHandle<T, Self>) -> Result<(), TaskHandle<T, Self>>;
}

/// @public 静态任务池；通过共享静态引用跨线程使用，无需克隆池对象。
/// 构造返回静态引用，所有提交和获取操作要求静态接收者。
/// 无关闭、清空、取消、超时功能；同队列push调用必须不重叠。
/// 直接持有队列、状态和执行器，不使用引用计数间接层。
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
/// 静态池保证句柄裸指针存活；无池回收。允许竞争重试，不保证wait-free。
pub struct TaskPool<T, TE: TaskExecutor<T>> {
    serial_states: CachePadded<AtomicU64>,
    parallel_states: CachePadded<AtomicU64>,
    serial: [SegQueue<T>; 21],
    parallel: [SegQueue<T>; 5],
    executor: TE,
}

/// @public 唯一任务与串行占用的所有权凭据，不可克隆。
/// 通过解引用访问任务；释放句柄不会执行任务。不得在异步工作实际完成前丢弃句柄。
/// serial_index为Some时以合法的SerialQueueIndex持有相应used位；None代表并发任务。
/// pool_ptr指向永久存活的TaskPool；不得解锁其他队列或二次转移任务。
/// Drop仅解除串行占用，字段T随后正常析构；执行器须持有到实际任务完成。
/// Send条件为T:Send、TE:Send+Sync；Sync另要求T:Sync，标记及安全依据位于内部实现模块。
pub struct TaskHandle<T, TE: TaskExecutor<T>> {
    task: T,
    pool_ptr: *const TaskPool<T, TE>,
    serial_index: Option<SerialQueueIndex>,
}

impl<T: Send + 'static, TE: TaskExecutor<T> + Send + Sync + 'static> TaskPool<T, TE> {
    /// @public 创建空池并返回永久有效的共享引用；无销毁回收能力。
    /// executor所有权转移至池；分配失败沿用分配器行为，不返回可恢复错误。
    /// 每次调用都会保留一份池及执行器至进程结束，应复用返回的静态引用。
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
    /// 快速交接绕过权重抽签，但不会越过同组已有任务，也不会与同组存活句柄重叠。
    /// 快速接受时不入队：认领与完成释放分别更新一次串行 used 位，竞争时 CAS 可重试。
    pub fn push_serial(&'static self, task: T, queue_index: SerialQueueIndex) {
        inner::push_serial_inner(self, task, queue_index.get())
    }

    /// @public 提交指定档位任务；不限制同档任务同时执行。
    /// 返回时入队或已接受；拒绝原任务回退入队，不丢失、不重复，非幂等。
    /// 不另设独立计数容量限制；不管理工作线程，分配失败沿用分配器行为。
    /// 快速接受时不入队，也不更新两类状态字；此路径不按 weight 抽签。
    /// 因此可先于其他组或档位已排队的任务执行；weight 只影响回退或普通入队后的选择。
    pub fn push_parallel(&'static self, task: T, weight: TaskWeight) {
        inner::push_parallel_inner(self, task, weight)
    }

    /// @public 概率获取；Some转移唯一任务，串行used保持至句柄释放。
    /// None表示本轮无可取得候选，不保证并发精确空快照；底层队列可能等待进行中的槽位写入。
    /// CAS竞争重读，取空候选本轮排除；不保证有限等待或严格优先顺序。
    /// random接受整个u64范围，由调用方提供；按当前总权重取模选择。
    /// 重试复用同一random；允许取模偏差，同快照同输入选择一致。
    /// 只有调用方持续提供适当分布的随机输入时才形成概率调度，固定输入不保证公平性。
    /// 仅返回排队任务，不会返回已被执行器接受的快速任务，也不会自动执行任务。
    pub fn pop(&'static self, random: u64) -> Option<TaskHandle<T, TE>> {
        inner::pop_inner(self, random)
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
