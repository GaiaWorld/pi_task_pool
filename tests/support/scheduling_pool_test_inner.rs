// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/scheduling_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Barrier,
};
use std::thread;
use std::time::Duration;

// Completion is published even if a producer fails its handshake, so consumers
// can terminate and the owning test reports the producer failure.
struct ProducerFinished(Arc<AtomicUsize>);
impl Drop for ProducerFinished {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

const WEIGHTS: [TaskWeight; 5] = [
    TaskWeight::Lowest,
    TaskWeight::Low,
    TaskWeight::Middle,
    TaskWeight::High,
    TaskWeight::Highest,
];
struct QueuedOnly;
impl<T> TaskExecutor<T> for QueuedOnly {
    fn fast_path_threshold(&self) -> usize {
        0
    }
    fn is_idle(&self) -> bool {
        panic!("disabled fast path queried idle")
    }
    fn try_execute(&self, _task: TaskHandle<T, Self>) -> Result<(), TaskHandle<T, Self>> {
        panic!("disabled fast path called executor")
    }
}

fn limit(index: usize) -> usize {
    if index == 4 {
        4095
    } else {
        8191
    }
}
fn assert_exact_ids(mut ids: Vec<usize>, total: usize) {
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..total).collect::<Vec<_>>(),
        "every submitted task must appear exactly once"
    );
}
fn drain(pool: &'static TaskPool<usize, QueuedOnly>, bound: usize) -> Vec<usize> {
    let mut ids = Vec::new();
    while let Some(task) = pool.pop(0) {
        ids.push(*task);
        assert!(
            ids.len() <= bound,
            "duplicate tasks or nonterminating drain"
        );
    }
    ids
}

fn saturated_race(extra: usize) {
    let pool = TaskPool::new(QueuedOnly);
    let mut next_id = 0;
    let mut additions = Vec::new();
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        for _ in 0..limit(index) + 257 {
            pool.push_parallel(next_id, weight);
            next_id += 1;
        }
        let start = next_id;
        next_id += extra;
        additions.push((weight, start..next_id));
    }
    let total = next_id;
    let producer_count = if extra == 0 { 0 } else { 5 };
    let start = Arc::new(Barrier::new(4 + producer_count));
    let finished = Arc::new(AtomicUsize::new(0));
    let mut acknowledgements = Vec::new();
    let mut producers = Vec::new();
    if extra != 0 {
        for (weight, mut ids) in additions {
            let first = ids.next().expect("nonempty producer range");
            let (ack, received) = mpsc::channel();
            acknowledgements.push((first, ack));
            let finished = Arc::clone(&finished);
                let start = Arc::clone(&start);
            producers.push(thread::spawn(move || {
                let _finished = ProducerFinished(finished);
                start.wait();
                pool.push_parallel(first, weight);
                // Watchdog only: no elapsed-time assertion is used as evidence.
                received
                    .recv_timeout(Duration::from_secs(10))
                    .expect("consumer must take the first new task before publication continues");
                for id in ids {
                    pool.push_parallel(id, weight);
                    thread::yield_now();
                }
            }));
        }
    }
    let acknowledgements = Arc::new(acknowledgements);
    let mut consumers = Vec::new();
    for _ in 0..4 {
        let start = Arc::clone(&start);
        let finished = Arc::clone(&finished);
        let acknowledgements = Arc::clone(&acknowledgements);
        consumers.push(thread::spawn(move || {
            start.wait();
            let mut ids = Vec::new();
            loop {
                if let Some(task) = pool.pop(0) {
                    let id = *task;
                    drop(task);
                    ids.push(id);
                    assert!(ids.len() <= total);
                    for (first, ack) in acknowledgements.iter() {
                        if id == *first {
                            ack.send(()).expect("producer awaiting acknowledgement");
                        }
                    }
                } else if finished.load(Ordering::Acquire) == producer_count {
                    break;
                } else {
                    thread::yield_now();
                }
            }
            ids
        }));
    }
    // None during publication is permitted. Join every publisher and repair owner
    // before the final drain; no assertion requires a paused thread to be helped.
    let producer_results: Vec<_> = producers
        .into_iter()
        .map(|producer| producer.join())
        .collect();
    let mut ids = Vec::new();
    for consumer in consumers {
        ids.extend(consumer.join().expect("consumer panicked"));
    }
    for result in producer_results {
        result.expect("producer panicked");
    }
    ids.extend(drain(&pool, total));
    assert_exact_ids(ids, total);
    assert!(pool.pop(0).is_none());
}
pub fn saturated_competing_consumers_preserve_all_tasks() {
    saturated_race(0);
}
pub fn saturated_publications_and_consumers_eventually_drain() {
    saturated_race(1024);
}

fn consume_serial(
    handle: TaskHandle<(usize, usize), QueuedOnly>,
    active: &[AtomicUsize; 21],
    next: &[AtomicUsize; 21],
) {
    let (group, id) = *handle;
    assert_eq!(
        active[group].fetch_add(1, Ordering::SeqCst),
        0,
        "same-group handles overlap"
    );
    assert_eq!(
        next[group].fetch_add(1, Ordering::SeqCst),
        id,
        "same-group FIFO changed"
    );
    thread::yield_now();
    assert_eq!(active[group].fetch_sub(1, Ordering::SeqCst), 1);
    drop(handle);
}
pub fn serial_cross_band_publications_preserve_fifo_and_exclusion() {
    let pool = TaskPool::new(QueuedOnly);
    let groups = [0, 10, 20];
    for group in groups {
        for id in 0..33 {
            pool.push_serial((group, id), SerialQueueIndex::new(group).unwrap());
        }
    }
    let active = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let next = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let start = Arc::new(Barrier::new(7));
    let finished = Arc::new(AtomicUsize::new(0));
    let mut acknowledgements = Vec::new();
    let mut producers = Vec::new();
    for group in groups {
        let (ack, received) = mpsc::channel();
        acknowledgements.push((group, ack));
        let finished = Arc::clone(&finished);
        let start = Arc::clone(&start);
        producers.push(thread::spawn(move || {
            let _finished = ProducerFinished(finished);
            start.wait();
            // Wait for a completed public-API acquisition/release before resuming.
            pool.push_serial((group, 33), SerialQueueIndex::new(group).unwrap());
            received
                .recv_timeout(Duration::from_secs(10))
                .expect("consumer must release serial task 33 before publication continues");
            // Exactly one producer per group; same-group push calls never overlap.
            for id in 34..129 {
                pool.push_serial((group, id), SerialQueueIndex::new(group).unwrap());
                thread::yield_now();
            }
        }));
    }
    let acknowledgements = Arc::new(acknowledgements);
    let mut consumers = Vec::new();
    for _ in 0..4 {
        let start = Arc::clone(&start);
        let finished = Arc::clone(&finished);
        let acknowledgements = Arc::clone(&acknowledgements);
        let active = Arc::clone(&active);
        let next = Arc::clone(&next);
        consumers.push(thread::spawn(move || {
            start.wait();
            let mut taken = 0;
            loop {
                if let Some(handle) = pool.pop(0) {
                    taken += 1;
                    assert!(taken <= 387, "too many serial tasks");
                    let (group, id) = *handle;
                    consume_serial(handle, &active, &next);
                    if id == 33 {
                        for (ack_group, ack) in acknowledgements.iter() {
                            if group == *ack_group {
                                ack.send(())
                                    .expect("serial producer awaiting acknowledgement");
                            }
                        }
                    }
                } else if finished.load(Ordering::Acquire) == 3 {
                    break;
                } else {
                    thread::yield_now();
                }
            }
        }));
    }
    let producer_results: Vec<_> = producers
        .into_iter()
        .map(|producer| producer.join())
        .collect();
    for consumer in consumers {
        consumer.join().expect("serial consumer panicked");
    }
    for result in producer_results {
        result.expect("serial producer panicked");
    }
    let mut taken = 0;
    while let Some(handle) = pool.pop(0) {
        taken += 1;
        assert!(taken <= 387);
        consume_serial(handle, &active, &next);
    }
    for group in 0..21 {
        assert_eq!(active[group].load(Ordering::SeqCst), 0);
        assert_eq!(
            next[group].load(Ordering::SeqCst),
            if groups.contains(&group) { 129 } else { 0 }
        );
    }
    assert!(pool.pop(0).is_none());
}

pub fn held_serial_groups_leave_saturated_parallel_candidates_available() {
    let pool = TaskPool::new(QueuedOnly);
    for group in 0..21 {
        pool.push_serial(group, SerialQueueIndex::new(group).unwrap());
        pool.push_serial(21 + group, SerialQueueIndex::new(group).unwrap());
    }
    let mut held = Vec::new();
    let mut first_ids = Vec::new();
    for _ in 0..21 {
        let handle = pool
            .pop(0)
            .expect("each unoccupied serial group must be available");
        first_ids.push(*handle);
        held.push(handle);
    }
    assert_exact_ids(first_ids, 21);
    assert!(pool.pop(0).is_none(), "only occupied serial groups remain");
    let mut total = 0;
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        for _ in 0..limit(index) + 1 {
            pool.push_parallel(42 + total, weight);
            total += 1;
        }
    }
    let ids = drain(&pool, total);
    assert!(
        ids.iter().all(|id| *id >= 42),
        "occupied serial group became selectable"
    );
    assert_exact_ids(ids.into_iter().map(|id| id - 42).collect(), total);
    drop(held);
    let remaining = drain(&pool, 21);
    assert!(remaining.iter().all(|id| (21..42).contains(id)));
    assert_exact_ids(remaining.into_iter().map(|id| id - 21).collect(), 21);
    assert!(pool.pop(0).is_none());
}

// 固定落点验收：pop 接收调用方提供的 random，并按当前总权重取模选择，
// 因此同一快照下每个 ticket 的选择可复现，无需随机源或内部快照访问。
type Task = (u8, usize, usize);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Candidate {
    Serial(usize),
    Parallel(usize),
}

fn serial_task(group: usize, sequence: usize) -> Task {
    (0, group, sequence)
}

fn parallel_task(index: usize, sequence: usize) -> Task {
    (1, index, sequence)
}

fn candidate(task: Task) -> Candidate {
    if task.0 == 0 {
        Candidate::Serial(task.1)
    } else {
        Candidate::Parallel(task.1)
    }
}

fn restore(pool: &'static TaskPool<Task, QueuedOnly>, task: Task) {
    if task.0 == 0 {
        let index = SerialQueueIndex::new(task.1).expect("valid serial group");
        pool.push_serial(task, index);
    } else {
        pool.push_parallel(task, WEIGHTS[task.1]);
    }
}

// 取出后立即放回同一队列：非饱和提示的扣减与发布相互抵消，快照复原。
fn take(pool: &'static TaskPool<Task, QueuedOnly>, random: u64) -> Candidate {
    let handle = pool
        .pop(random)
        .expect("ticket inside the total weight must yield a task");
    let task = *handle;
    drop(handle);
    restore(pool, task);
    candidate(task)
}

fn expected_candidate(ticket: u64) -> Candidate {
    match ticket {
        0..=199 => Candidate::Serial(0),
        200..=299 => Candidate::Serial(5),
        300..=599 => Candidate::Serial(12),
        600..=602 => Candidate::Parallel(0),
        603..=622 => Candidate::Parallel(1),
        623..=722 => Candidate::Parallel(2),
        723..=1722 => Candidate::Parallel(3),
        _ => Candidate::Parallel(4),
    }
}

pub fn fixed_ticket_intervals_match_queue_weights() {
    let pool = TaskPool::new(QueuedOnly);
    let group_nine = SerialQueueIndex::new(9).unwrap();
    pool.push_serial(serial_task(9, 0), group_nine);
    pool.push_serial(serial_task(9, 1), group_nine);
    let held = pool.pop(0).expect("group 9 head");
    assert_eq!(candidate(*held), Candidate::Serial(9));

    let group_zero = SerialQueueIndex::new(0).unwrap();
    for sequence in 0..17 {
        pool.push_serial(serial_task(0, sequence), group_zero);
    }
    pool.push_serial(serial_task(5, 0), SerialQueueIndex::new(5).unwrap());
    let group_twelve = SerialQueueIndex::new(12).unwrap();
    for sequence in 0..33 {
        pool.push_serial(serial_task(12, sequence), group_twelve);
    }
    for sequence in 0..3 {
        pool.push_parallel(parallel_task(0, sequence), TaskWeight::Lowest);
    }
    for sequence in 0..2 {
        pool.push_parallel(parallel_task(1, sequence), TaskWeight::Low);
    }
    pool.push_parallel(parallel_task(2, 0), TaskWeight::Middle);
    pool.push_parallel(parallel_task(3, 0), TaskWeight::High);
    pool.push_parallel(parallel_task(4, 0), TaskWeight::Highest);

    // 200(组0,编码2)+100(组5,编码1)+300(组12,编码3)+3+20+100+1000+10000。
    const TOTAL: u64 = 11_723;
    for ticket in 0..TOTAL {
        assert_eq!(
            take(&pool, ticket),
            expected_candidate(ticket),
            "ticket {ticket}"
        );
    }
    assert_eq!(take(&pool, TOTAL), expected_candidate(0));
    // 大输入：u64::MAX 按总权重取模后必须落入对应区间。
    let large = u64::MAX % TOTAL;
    assert_eq!(
        take(&pool, u64::MAX),
        expected_candidate(large),
        "u64::MAX must reduce by total weight"
    );

    drop(held);
    // 释放后组9重新计入并排在组12之前：300..399 命中组9。
    assert_eq!(take(&pool, 300), Candidate::Serial(9));
    // 枚举期间每个任务取出后都已放回，最终存货必须与初始快照完全一致。
    let mut remaining: std::collections::BTreeMap<(u8, usize), usize> =
        std::collections::BTreeMap::new();
    while let Some(handle) = pool.pop(0) {
        let candidate = candidate(*handle);
        *remaining
            .entry(match candidate {
                Candidate::Serial(index) => (0, index),
                Candidate::Parallel(index) => (1, index),
            })
            .or_insert(0) += 1;
    }
    let expected: std::collections::BTreeMap<(u8, usize), usize> = [
        ((0, 0), 17),
        ((0, 5), 1),
        ((0, 9), 1),
        ((0, 12), 33),
        ((1, 0), 3),
        ((1, 1), 2),
        ((1, 2), 1),
        ((1, 3), 1),
        ((1, 4), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(remaining, expected);
    assert!(pool.pop(0).is_none());
}

pub fn fixed_ticket_selection_repeats_after_total_weight() {
    let pool = TaskPool::new(QueuedOnly);
    pool.push_parallel(parallel_task(0, 0), TaskWeight::Lowest);
    pool.push_parallel(parallel_task(2, 0), TaskWeight::Middle);
    // 总权重 1 + 100 = 101。
    assert_eq!(take(&pool, 0), Candidate::Parallel(0));
    assert_eq!(take(&pool, 1), Candidate::Parallel(2));
    assert_eq!(take(&pool, 101), Candidate::Parallel(0));
    assert_eq!(take(&pool, 201), Candidate::Parallel(2));
    assert_eq!(take(&pool, 0), Candidate::Parallel(0));
    assert_eq!(
        take(&pool, 0),
        Candidate::Parallel(0),
        "same input, same queue"
    );

    // 大输入边界：u64::MAX 与总权重整数倍均须按 101 归入正确区间。
    let large = u64::MAX % 101;
    let expected_large = if large == 0 {
        Candidate::Parallel(0)
    } else {
        Candidate::Parallel(2)
    };
    assert_eq!(take(&pool, u64::MAX), expected_large);
    let aligned = 101 * 1_000_000;
    assert_eq!(take(&pool, aligned), Candidate::Parallel(0));
    assert_eq!(take(&pool, aligned + 1), Candidate::Parallel(2));

    let empty: &'static TaskPool<Task, QueuedOnly> = TaskPool::new(QueuedOnly);
    assert!(empty.pop(0).is_none());
    assert!(empty.pop(u64::MAX).is_none());
}

pub fn saturated_fields_drain_under_fixed_tickets() {
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        let count = limit(index) + 17;
        let pool = TaskPool::new(QueuedOnly);
        for id in 0..count {
            pool.push_parallel(id, weight);
        }
        let mut ids = Vec::new();
        while let Some(task) = pool.pop(0) {
            ids.push(*task);
            assert!(
                ids.len() <= count,
                "duplicate tasks or nonterminating drain"
            );
        }
        assert_exact_ids(ids, count);
        assert!(pool.pop(0).is_none());
    }
}

pub fn serial_count_band_boundaries_change_public_ticket_intervals() {
    for (count, initial_band, remaining_band) in [
        (16, 1u64, 1u64),
        (17, 2, 1),
        (32, 2, 2),
        (33, 3, 2),
        (48, 3, 3),
        (49, 3, 3),
    ] {
        let pool = TaskPool::new(QueuedOnly);
        let index = SerialQueueIndex::new(0).unwrap();
        for sequence in 0..count {
            pool.push_serial(serial_task(0, sequence), index);
        }
        pool.push_parallel(parallel_task(0, 0), TaskWeight::Lowest);
        let initial_total = initial_band * 100 + 1;
        for ticket in 0..initial_total {
            let expected = if ticket < initial_band * 100 {
                Candidate::Serial(0)
            } else {
                Candidate::Parallel(0)
            };
            assert_eq!(take(pool, ticket), expected, "count {count}, ticket {ticket}");
        }
        let held = pool.pop(0).expect("serial head");
        assert_eq!(candidate(*held), Candidate::Serial(0));
        let held_sequence = held.2;
        // 同一输入在占用后按新总权重取模，只能选最低档。
        assert_eq!(take(pool, 0), Candidate::Parallel(0));
        assert_eq!(take(pool, initial_total), Candidate::Parallel(0));
        drop(held);
        let remaining_total = remaining_band * 100 + 1;
        for ticket in 0..remaining_total {
            let expected = if ticket < remaining_band * 100 {
                Candidate::Serial(0)
            } else {
                Candidate::Parallel(0)
            };
            assert_eq!(take(pool, ticket), expected, "remaining {count}, ticket {ticket}");
        }
        let mut serial_sequences = vec![held_sequence];
        for _ in 1..count {
            let task = pool.pop(0).expect("serial task remains visible");
            assert_eq!(candidate(*task), Candidate::Serial(0));
            serial_sequences.push(task.2);
            drop(task);
        }
        serial_sequences.sort_unstable();
        assert_eq!(serial_sequences, (0..count).collect::<Vec<_>>());
        let last = pool.pop(0).expect("parallel task remains visible");
        assert_eq!(*last, parallel_task(0, 0));
        drop(last);
        assert!(pool.pop(0).is_none());
    }
}

// 验证分层：本文件验证公开API选择区间和并发结果。
// src/recovery_tests_inner.rs通过仅测试启用的私有检查点复用生产恢复循环，
// 对饱和旧快照覆盖、下调后发布、串行跨档及取空修复断言目标窗口命中。
// 三个Loom模型提供有界补充探索，不跟踪Crossbeam内部，不是穷尽证明。
// 独立计数预留已删除，不再列为当前恢复协议验收目标。
