# 登记分册：词汇表

## 词汇

| 术语 | 定义 | 出处 |
|---|---|---|
| 等待任务 | 已提交、尚未取出交执行器的任务，不含执行中的任务 | L1 needs |
| 串行队列 | 同组任务必须在前一任务完成释放后才能获取下一任务；同组push不重叠 | L1 needs/notes |
| 队列长度采样 | SegQueue::len基于队首/队尾索引计算；可能重试，包含在途写入位置，不含已认领位置；替代独立计数 | src/lib.rs、src/task_pool_inner.rs |
| 队列发布 | 将任务所有权交入SegQueue | src/task_pool_inner.rs |
| 提示发布 | 原子更新压缩字段使已发布任务参与选择，允许近似多报 | src/task_pool_inner.rs |
| 认领 | 以CAS取得串行used或消耗普通并发提示；任务实际所有权仍由pop确定 | src/task_pool_inner.rs |
| 饱和提示 | 并发字段达到上限，仅表示积压不可精确编码，取出不盲减 | src/task_pool_inner.rs |
| 快速交接阈值 | fast_path_threshold，早期称max；估计等待量少于该值才检查执行器空闲 | src/lib.rs |
| 任务句柄 | 唯一任务及串行占用责任，实际完成后释放，非执行线程 | src/lib.rs |
| 释放守卫 | 私有RAII对象，拒绝拆解后唯一承接串行used清除责任 | src/task_pool_inner.rs |
| 静态池 | new返回静态共享引用，提交和获取要求static self；没有池Clone或回收API | src/lib.rs |
| SerialQueueIndex | 字段私有的合法串行索引，const new仅接受0..21，否则None | src/lib.rs |
| 调用方随机数 | pop接收u64，对当前总权重取模，竞争重试复用，允许取模偏差 | src/lib.rs |
| serial_states | 21组占用位与2位压缩串行数量，最高位保留0 | src/lib.rs |
| parallel_states | 五档并发数量提示，最高12位，其余各13位，上限代表饱和 | src/lib.rs |
