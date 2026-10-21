# 变更点（L5）

| 变更方向 | 隔离位置 | 影响调用点 |
|---|---|---|
| 修改串行编码/并发位宽或饱和校正 | 队列状态（表示维护）/权重选择（快照解释） | publish_parallel_hint_inner、repair_parallel_hint_inner、encode_serial_count_inner、total_weight_inner、select_queue_inner、estimate_waiting_inner |
| 修改认领/发布/完成的状态协议 | 队列状态 | try_claim_serial_inner、publish_serial_hint_inner、lower_serial_hint_inner、pop_inner、release_serial_inner |
| 修改执行器接受/拒绝形式 | 执行交接 | TaskExecutor::try_execute、push_serial_inner、push_parallel_inner、into_task_inner、SerialReleaseGuard及其Drop芯 |
| 调整快速路径阈值估算 | 池门面/队列状态 | estimate_waiting_inner、push_serial_inner、push_parallel_inner、fast_path_threshold |

每队列独立精确预留计数；parallel与serial均采用(SegQueue<T>,AtomicU64)存储，分别5与21个。
