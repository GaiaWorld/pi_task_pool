// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/queue_len_regression.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::sync::{Arc, Barrier};
use std::thread;

struct Queued;
impl TaskExecutor<usize> for Queued {
    fn fast_path_threshold(&self) -> usize {
        0
    }
    fn is_idle(&self) -> bool {
        false
    }
    fn try_execute(&self, task: TaskHandle<usize, Self>) -> Result<(), TaskHandle<usize, Self>> {
        Err(task)
    }
}

pub fn parallel_saturation_drains_after_len_based_repair() {
    for weight in [
        TaskWeight::Lowest,
        TaskWeight::Low,
        TaskWeight::Middle,
        TaskWeight::High,
        TaskWeight::Highest,
    ] {
        let pool = TaskPool::new(Queued);
        for value in 0..10_000 {
            pool.push_parallel(value, weight);
        }
        for expected in 0..10_000 {
            let task = pool
                .pop(expected as u64)
                .expect("published task must remain visible");
            assert_eq!(*task, expected);
        }
        assert!(pool.pop(0).is_none());
    }
}

pub fn serial_len_encoding_crosses_all_boundaries() {
    let pool = TaskPool::new(Queued);
    let index = SerialQueueIndex::new(20).unwrap();
    for value in 0..80 {
        pool.push_serial(value, index);
    }
    for expected in 0..80 {
        let task = pool.pop(u64::MAX).expect("serial task must remain visible");
        assert_eq!(*task, expected);
        assert!(
            pool.pop(0).is_none(),
            "held serial queue must remain occupied"
        );
        drop(task);
    }
    assert!(pool.pop(0).is_none());
}

pub fn concurrent_publications_and_consumers_leave_no_hidden_tasks() {
    let pool = TaskPool::new(Queued);
    // Start above saturation so concurrent consumption exercises recovery.
    for value in 0..9_000 {
        pool.push_parallel(value, TaskWeight::Highest);
    }
    let start = Arc::new(Barrier::new(5));
    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    for producer in 0..2 {
        let start = Arc::clone(&start);
        producers.push(thread::spawn(move || {
            start.wait();
            for offset in 0..3_000 {
                pool.push_parallel(9_000 + producer * 3_000 + offset, TaskWeight::Highest);
            }
        }));
    }
    for consumer in 0..2 {
        let start = Arc::clone(&start);
        consumers.push(thread::spawn(move || {
            let mut values = Vec::new();
            start.wait();
            for random in 0..12_000 {
                if let Some(task) = pool.pop(random + consumer) {
                    values.push(*task);
                }
            }
            values
        }));
    }
    start.wait();
    for producer in producers {
        producer.join().unwrap();
    }
    let mut values = Vec::new();
    // Join all consumers before final draining: every repair must finish first.
    for consumer in consumers {
        values.extend(consumer.join().unwrap());
    }
    while let Some(task) = pool.pop(0) {
        values.push(*task);
    }
    values.sort_unstable();
    assert_eq!(values, (0..15_000).collect::<Vec<_>>());
}
