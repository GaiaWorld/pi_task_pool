# lock-free-task-pool · 队列调研：SegQueue 与 ubq 5.0.2

## 范围与来源

- 本仓库 [Cargo.toml](../../../Cargo.toml) 指定 crossbeam-queue 0.3，当前 [Cargo.lock](../../../Cargo.lock) 锁定 **0.3.14**；下文固定比较 [SegQueue 0.3.14](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html) 与 [ubq 5.0.2](https://docs.rs/ubq/5.0.2/ubq/)。所有外部链接均指向各自发布包的文档或源码。
- 两者都是可增长的 MPMC 队列；“无界”不表示固定内存或无需分配。[SegQueue 文档](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html)、[UBQ 文档](https://docs.rs/ubq/5.0.2/ubq/)。

## 算法与内存回收

| 对比 | Crossbeam SegQueue 0.3.14 | ubq UBQ 5.0.2 |
|---|---|---|
| 结构 | 链表分块，LAP=32、每块 31 个值槽；head/tail 各含原子索引及块指针，槽状态含 WRITE、READ、DESTROY。 | 链接的定长块；默认每块 511 槽，默认复用池 8 块；phead/chead 将块指针与索引打包，槽含原子状态和待初始化的值。 |
| 插入 | tail 索引 SeqCst CAS 抢槽，边界链接新块；值写入后以 Release 发布 WRITE。 | phead 常规以 SeqCst fetch_add 抢槽，边界链接新块；写值后 Release 发布 WRITE；push_batch 连续预留一批位置。 |
| 取出 | head 索引 SeqCst CAS 抢槽，Acquire 等待 WRITE，跨块时等待 next；读者使用结束后标记 READ。 | chead SeqCst CAS 抢槽，Acquire 等待 WRITE，跨块时等待 next；SKIP 和 faux_push 处理内部假插入路径。 |
| 回收 | 块尾消费者尝试销毁旧块；若其他读者仍在块中，DESTROY 位把销毁移交给后续离开的读者；没有内置空块池。 | 消费计数达到块长后清零状态并放入有限大小的原子空块池；池满释放，后续优先复用；队列 Drop 清理链接块及池中块。 |

来源：[SegQueue 块/状态/回收](https://docs.rs/crossbeam-queue/0.3.14/src/crossbeam_queue/seg_queue.rs.html#26-136)、[SegQueue push/pop](https://docs.rs/crossbeam-queue/0.3.14/src/crossbeam_queue/seg_queue.rs.html#223-458)、[SegQueue Drop](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html#impl-Drop-for-SegQueue%3CT%3E)、[UBQ block.rs（默认块长、reset、Drop）](https://docs.rs/crate/ubq/5.0.2/source/src/block.rs)、[UBQ queue.rs（池、占槽、跨块、Drop）](https://docs.rs/crate/ubq/5.0.2/source/src/queue.rs)。这里比较的是**块生命周期**，不要误称为逐元素 epoch/hazard-pointer 回收。两者在边界和未完成发布的槽上均可能自旋等待；UBQ 文档明确称操作不 park，同时承认跨块时可能等待在途生产者提交。[UBQ 文档](https://docs.rs/ubq/5.0.2/ubq/)。

## 顺序和并发语义

- 单线程依次 push(a)、push(b)，两者的官方示例均按 a、b 弹出；并发生产者的队列位置通过原子抢占决定，不能凭调用开始先后推断两个并发 push 的最终顺序；不同消费者的处理完成顺序也不是队列顺序。[SegQueue API 和源码](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html)、[UBQ API](https://docs.rs/ubq/5.0.2/ubq/struct.ConfiguredUBQ.html)、[UBQ 抢槽源码](https://docs.rs/crate/ubq/5.0.2/source/src/queue.rs)。
- 两者均先预留位置再发布槽值，消费者取得尚未发布的位置时可等待写入完成；因此无等待上限或暂停线程下的 wait-free 性质不能由“无界/lock-free”推断。[SegQueue push/pop](https://docs.rs/crossbeam-queue/0.3.14/src/crossbeam_queue/seg_queue.rs.html#223-458)、[UBQ push/pop](https://docs.rs/crate/ubq/5.0.2/source/src/queue.rs)。
- UBQ 的 push_batch 文档明确说整批按顺序且无间隙地放入队尾；SegQueue 0.3.14 没有对应批量方法。[UBQ push_batch](https://docs.rs/ubq/5.0.2/ubq/struct.ConfiguredUBQ.html#method.push_batch)、[SegQueue 方法表](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html)。

## 公开 API

| 项目 | SegQueue 0.3.14 | UBQ 5.0.2 |
|---|---|---|
| 构造 | const new() | new()（首次 push 才分配块）、new_arc() |
| 基本操作 | push(&self, T)、pop(&self) -> Option<T>、is_empty(&self) | 同样有 push、pop、is_empty |
| 附加操作 | len()、独占 push_mut/pop_mut、消费式 IntoIterator | push_batch（要求 ExactSizeIterator）、公开 POOL_SIZE/BLOCK_LENGTH 常量；API 方法表没有 len |
| 配置/平台 | 固定 31 槽的 SegQueue；见对应 crate 特性文档 | UBQ<T> 是 ConfiguredUBQ<T> 默认别名；可配置 backoff、POOL、BLOCK_SIZE、对齐；默认 std，可关闭默认特性使用 no_std + alloc（需全局分配器、8 位和指针宽原子） |

来源：[SegQueue API](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html)、[UBQ API](https://docs.rs/ubq/5.0.2/ubq/struct.ConfiguredUBQ.html)、[UBQ 默认别名](https://docs.rs/ubq/5.0.2/ubq/type.UBQ.html)、[UBQ 特性](https://docs.rs/crate/ubq/5.0.2/features)、[UBQ no_std 条件](https://docs.rs/ubq/5.0.2/ubq/)。对本仓库而言，如果调用点依赖 len 或独占方法，替换不能只换类型名。

## 第一方基准资料及限制

- [UBQ 5.0.2 README 的 Benchmarks 部分](https://docs.rs/crate/ubq/5.0.2/source/README.md) 描述包含 SegQueue 基线的矩阵、frontier 搜索、生产/消费线程组合及结果生成路径；[发布包 ubq_bench.rs](https://docs.rs/crate/ubq/5.0.2/source/benches/ubq_bench.rs) 有 SegQueue 对照、throughput 和 fill_drain 两种模式及 JSON 指标。这是**基准方法和代码**，不是本次运行的结果。
- [发布包 push_batch.rs](https://docs.rs/crate/ubq/5.0.2/source/benches/push_batch.rs) 仅对 UBQ 重复单条 push 与 UBQ push_batch 比较（默认百万元素、7 个样本，输出每项 ns 和比值），不能当作 UBQ 对 SegQueue 的批量胜负。[UBQ 应用基准说明](https://docs.rs/crate/ubq/5.0.2/source/docs/application_benchmarks.md) 还明确应用形态是受控合成实验而非完整生产负载。
- [SegQueue 官方文档](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.SegQueue.html) 只定性称其因分块动态分配而比**同库 ArrayQueue**略慢；[其 0.3.14 发布包目录](https://docs.rs/crate/crossbeam-queue/0.3.14/source/)未列 benches，不能推断上游仓库从无 benchmark。
- 所查的固定版本发布资料提供测试程序和说明，但未取得可归因于同机器、同配置的 UBQ 对 SegQueue 原始数值，因此**没有可信的“快 X 倍”结论**；比较数字需要同时标注机器、线程数、负载、版本、UBQ 块长/池大小与退避配置。[UBQ README](https://docs.rs/crate/ubq/5.0.2/source/README.md)、[UBQ 基准源码](https://docs.rs/crate/ubq/5.0.2/source/benches/ubq_bench.rs)。上述段落仅讨论第一方基准资料，下面为独立本机实测。

## 本机实测（2026-10-21）

Intel Core i7-6700K（4 物理核 / 8 逻辑线程）、Windows、rustc 1.100.0-nightly、Cargo release；固定 crossbeam-queue 0.3.14 和 ubq 5.0.2 默认配置，元素 u64。基准工程在工作区外 C:/Users/zmax/AppData/Local/Temp/opencode/queue-comparison-20261021。每线程 100,000 次循环，每组预热一次，取随后五次的吞吐中位数；两库运行顺序逐次交替。屏障放行到所有线程 join 计时；线程创建、只 push 后清空和只 pop 前填满不计时。mixed 为每线程交替 push/pop，空 pop 则自旋。单位 Mops/s = 总队列调用数 / 墙钟时间；mixed 每循环计两次调用。

| 场景 Mops/s（SegQueue / UBQ） | 1 线程 | 2 线程 | 4 线程 | 8 线程 |
|---|---:|---:|---:|---:|
| 只 push | 41.49 / 49.96 | 43.34 / 31.35 | 47.42 / 33.45 | 45.92 / 35.53 |
| 只 pop（预填满） | 46.92 / 55.60 | 44.66 / 53.43 | 39.48 / 49.10 | 38.18 / 50.19 |
| push→pop 交替（低占用） | 42.43 / 3.80 | 40.96 / 4.99 | 40.36 / 6.77 | 39.11 / 8.64 |

两轮完整运行的胜负方向一致；表为第二轮。mixed 常触及空队列；[UBQ pop 源码](https://docs.rs/crate/ubq/5.0.2/source/src/queue.rs)在取槽后如发现头尾相接，会走 faux_push 创建 SKIP 槽，遇 SKIP 再次 pop。此额外路径可能解释低占用差距，但尚无 profiling 定量归因，不能外推至普通高占用 MPMC。无 CPU 亲和性或进程隔离，数值仅代表该机器和测试形状。
