// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/recovery_loom_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};

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

// Execution status: all three models currently stop at Loom's "Arc leaked"
// check because new_inner intentionally forgets one strong reference. They must
// not be treated as completed schedule exploration or recovery-counterexamples.
// The production Loom lifecycle needs a scoped owner for that permanent reference
// until every worker and handle has finished; do not suppress leak checks here.
// Bounded exploration, not an exhaustive proof. SegQueue remains Crossbeam:
// Loom controls the production hint/count atomics, not queue-internal operations.
// No target-interleaving hit counters exist: passing cannot certify a specific
// stale-snapshot window or an empty-pop repair branch was reached.
fn explore(permutations: usize, scenario: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_permutations = Some(permutations);
    model.max_branches = 100_000;
    // TaskPool owns 26 inline queues; construct it on a larger model-thread
    // stack rather than Loom's small default root coroutine stack.
    let scenario = std::sync::Arc::new(scenario);
    model.check(move || {
        let scenario = std::sync::Arc::clone(&scenario);
        loom::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(move || scenario())
            .unwrap()
            .join()
            .unwrap();
    });
}

fn finish(pool: &'static TaskPool<usize, QueuedOnly>, mut ids: Vec<usize>, count: usize) {
    while let Some(handle) = pool.pop(0) {
        ids.push(*handle);
        assert!(ids.len() <= count, "duplicate task or nonterminating drain");
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..count).collect::<Vec<_>>());
    assert!(pool.pop(u64::MAX).is_none());
}

pub fn publication_racing_empty_consumer_preserves_task() {
    explore(200, || {
        let pool = TaskPool::new(QueuedOnly);
        let producer = pool;
        let producer = loom::thread::spawn(move || producer.push_parallel(7, TaskWeight::Middle));
        let consumer = pool;
        let consumer = loom::thread::spawn(move || consumer.pop(0).map(|handle| *handle));
        producer.join().unwrap();
        let mut ids: Vec<_> = consumer.join().unwrap().into_iter().collect();
        while let Some(handle) = pool.pop(0) {
            ids.push(*handle);
            assert!(ids.len() <= 1);
        }
        assert_eq!(ids, vec![7]);
        assert!(pool.pop(0).is_none());
    });
}

pub fn serial_cross_band_publication_preserves_fifo() {
    explore(200, || {
        let pool = TaskPool::new(QueuedOnly);
        let index = SerialQueueIndex::new(0).unwrap();
        for id in 0..17 {
            pool.push_serial(id, index);
        }
        let producer = pool;
        let producer = loom::thread::spawn(move || producer.push_serial(17, index));
        let consumer = pool;
        let consumer = loom::thread::spawn(move || consumer.pop(0).map(|handle| *handle));
        producer.join().unwrap();
        let mut ids: Vec<_> = consumer.join().unwrap().into_iter().collect();
        while let Some(handle) = pool.pop(0) {
            ids.push(*handle);
            assert!(ids.len() <= 18);
        }
        assert_eq!(ids, (0..18).collect::<Vec<_>>());
        assert!(pool.pop(0).is_none());
    });
}

pub fn saturated_publication_racing_repair_preserves_tasks() {
    // Keep the real 12-bit limit: do not replace the algorithm with a small copy.
    // Eight schedules are an initial smoke bound, not target-window coverage.
    explore(8, || {
        let pool = TaskPool::new(QueuedOnly);
        for id in 0..4095 {
            pool.push_parallel(id, TaskWeight::Highest);
        }
        let producer = pool;
        let producer =
            loom::thread::spawn(move || producer.push_parallel(4095, TaskWeight::Highest));
        let consumer = pool;
        let consumer = loom::thread::spawn(move || consumer.pop(0).map(|handle| *handle));
        producer.join().unwrap();
        let ids = consumer.join().unwrap().into_iter().collect();
        finish(&pool, ids, 4096);
    });
}
