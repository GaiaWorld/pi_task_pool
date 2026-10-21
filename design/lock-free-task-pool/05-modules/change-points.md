# 变更点（L5）

| 变更方向 | 隔离位置 | 影响调用点 |
|---|---|---|
| 修改串行编码/并发位宽或饱和校正 | 队列状态（表示维护）/权重选择（快照解释） | publish_parallel_hint_inner、repair_parallel_hint_inner、encode_serial_count_inner、total_weight_inner、select_queue_inner、estimate_waiting_inner |
| 修改认领/发布/完成的状态协议 | 队列状态 | try_claim_serial_inner、publish_serial_hint_inner、lower_serial_hint_inner、pop_inner、release_serial_inner |
| 修改执行器接受/拒绝形式 | 执行交接 | TaskExecutor::try_execute、push_serial_inner、push_parallel_inner、into_task_inner、SerialReleaseGuard及其Drop芯 |
| 调整快速路径阈值估算 | 池门面/队列状态 | estimate_waiting_inner、push_serial_inner、push_parallel_inner、fast_path_threshold |

parallel与serial分别为[SegQueue<T>;5]和[SegQueue<T>;21]；恢复采样len()，没有独立计数、预留或回滚函数。静态生命周期调整涉及TaskPool::new、三个静态接收者方法及new_inner；TaskPool直接持有队列、状态和执行器，TaskPoolInner和内部Arc已删除；句柄与释放守卫直接指向静态TaskPool。第三切片新增仅普通单元测试启用的私有恢复检查点及src/recovery_tests_inner.rs，用于目标交错命中断言。
