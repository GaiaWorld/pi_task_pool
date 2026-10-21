# 登记分册：词汇表

## 词汇

| 术语 | 定义 | 出处 |
|---|---|---|
| 等待任务 | 已提交、尚未取出交执行器的任务，不含执行中的任务 | L1 needs |
| 串行队列 | 同组任务必须在前一任务完成释放后才能获取下一任务；同组push不重叠 | L1 needs/notes |
| 计数预留 | 先于队列发布增加的独立记账；静止时等于等待量，进行中可偏大 | src/task_pool_inner.rs |
| 队列发布 | 将任务所有权交入SegQueue | src/task_pool_inner.rs |
| 提示发布 | 原子更新压缩字段使已发布任务参与选择，允许近似多报 | src/task_pool_inner.rs |
| 认领 | 以CAS取得串行used或消耗普通并发提示；任务实际所有权仍由pop确定 | src/task_pool_inner.rs |
| 饱和提示 | 并发字段达到上限，仅表示积压不可精确编码，取出不盲减 | src/task_pool_inner.rs |
| 快速交接阈值 | fast_path_threshold，早期称max；估计等待量少于该值才检查执行器空闲 | src/lib.rs |
| 任务句柄 | 唯一任务及串行占用责任，实际完成后释放，非执行线程 | src/lib.rs |
| 释放守卫 | 私有RAII对象，拒绝拆解后唯一承接串行used清除责任 | src/task_pool_inner.rs |
