// 按冻结契约只读解码两个状态字，返回分队列等待长度估计。
// 设计：queue-length-estimates
use crate::sync::Ordering;
use crate::{QueueLengthEstimates, TaskExecutor, TaskPool};

/// @internal 解码两个状态字，生成分队列等待长度估计。
pub(crate) fn queue_length_estimates_inner<T, TE: TaskExecutor<T>>(
    pool: &TaskPool<T, TE>,
) -> QueueLengthEstimates {
    // 输入前提：池状态遵循既有位布局。
    // 1. 两个状态字各作一次Relaxed原子读取；仅解码提示，不承担任务内存的发布同步。
    // 2. 按组0至20提取两位串行数量编码，忽略used，每项乘16。
    // 3. 从低至高提取五个并发数量字段，前四个13位，最后一个12位。
    // 4. 返回两个定长数组，不修改池，不调用外部代码。
    // 分支与失败：无输入校验、修复或重试；并发变化允许结果不同时。
    // 后置状态：池不变，结果各项在约定范围内。
    let serial_state = pool.serial_states.load(Ordering::Relaxed);
    let parallel_state = pool.parallel_states.load(Ordering::Relaxed);

    let serial = std::array::from_fn(|index| {
        (((serial_state >> (index * 3 + 1)) & 3) * 16) as usize
    });
    let parallel = std::array::from_fn(|index| {
        let mask = if index == 4 { 4095 } else { 8191 };
        ((parallel_state >> (index * 13)) & mask) as usize
    });

    QueueLengthEstimates { serial, parallel }
}
