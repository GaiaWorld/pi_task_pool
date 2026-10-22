// 设计：queue-length-estimates
use crate::{TaskExecutor, TaskPool};

/// @public 26个队列的粗略等待长度，单位为任务个数。
/// serial按组0至20排列；parallel按最低、低、中、高、最高档排列。
/// 池产生的serial值为0、16、32、48；parallel前四项不超过8191，最后一项不超过4095。
/// 包含占用中串行组的等待提示，但不把used位或执行中任务另计为一个任务。
/// 提示可暂时高估、低估或饱和截断；不是统一时刻快照，零值不保证没有任务。
/// 返回值由调用方独立持有和修改，修改它不会影响池。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLengthEstimates {
    /// 串行组0至20的粗略等待长度。
    pub serial: [usize; 21],
    /// 最低至最高五档的粗略等待长度。
    pub parallel: [usize; 5],
}

impl<T, TE: TaskExecutor<T>> TaskPool<T, TE> {
    /// @public 读取26个队列的粗略等待长度，供监控观察。
    /// 无额外参数；接收共享借用，返回独立拥有的QueueLengthEstimates。
    /// 两个状态字各作一次Relaxed原子读取，仅解码提示，不同步任务内容。
    /// 不保证读到最新值或跨状态字一致。
    /// 不分配堆内存、不重试、不扫描或修复队列、不调用执行器、不改变池状态。
    /// 无输入错误或可恢复失败；正常实现无主动panic路径。
    /// 可重复、并发调用，但并发更新时结果不保证相同。
    /// 不统计精确长度或完成状态，不额外统计执行中或已快速接受的任务。
    pub fn queue_length_estimates(&self) -> QueueLengthEstimates {
        crate::queue_length_estimates_inner::queue_length_estimates_inner(self)
    }
}
