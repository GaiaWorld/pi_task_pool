// @api:src/queue_length_estimates.rs
// @impl:src/queue_length_estimates_inner.rs
// @test:tests/queue_length_estimates_test.rs
// @design-doc:design/queue-length-estimates/07-design/call-chain.md
use pi_task_pool::{QueueLengthEstimates, SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    // Const TLS has no allocating initializer; try_with also tolerates thread teardown.
    static ALLOCATIONS: Cell<Option<[usize; 3]>> = const { Cell::new(None) };
}
struct ThreadCountingAllocator;
fn record_allocation(kind: usize) {
    let _ = ALLOCATIONS.try_with(|state| {
        if let Some(mut counts) = state.get() {
            counts[kind] += 1;
            state.set(Some(counts));
        }
    });
}
// SAFETY: every allocation operation is forwarded unchanged to System;
// bookkeeping uses nonallocating thread-local Cells and never unwinds.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(0);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(1);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(2);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

struct AllocationWindow;
impl Drop for AllocationWindow {
    fn drop(&mut self) { ALLOCATIONS.with(|state| state.set(None)); }
}
fn measured<R>(action: impl FnOnce() -> R) -> (R, [usize; 3]) {
    ALLOCATIONS.with(|state| state.set(Some([0; 3])));
    let window = AllocationWindow;
    let result = action();
    let counts = ALLOCATIONS.with(|state| state.get().unwrap());
    drop(window);
    (result, counts)
}

pub fn public_queries_do_not_allocate_on_the_calling_thread() {
    // Positive control uses explicit allocator calls so optimization cannot elide them.
    let (_, counts) = measured(|| unsafe {
        let layout = Layout::from_size_align(16, 8).unwrap();
        let plain = std::alloc::alloc(layout);
        assert!(!plain.is_null());
        let grown = std::alloc::realloc(plain, layout, 32);
        assert!(!grown.is_null());
        std::alloc::dealloc(grown, Layout::from_size_align(32, 8).unwrap());
        let zeroed = std::alloc::alloc_zeroed(layout);
        assert!(!zeroed.is_null());
        std::alloc::dealloc(zeroed, layout);
    });
    assert_eq!(counts, [1, 1, 1], "all three allocation paths must be observable");
    // A separate thread allocates while this thread is monitored: it must not count.
    let isolation_start = Arc::new(Barrier::new(2));
    let worker_start = Arc::clone(&isolation_start);
    let worker = thread::spawn(move || {
        worker_start.wait();
        std::hint::black_box(Box::new(17))
    });
    let (value, isolated_counts) = measured(|| {
        isolation_start.wait();
        worker.join().unwrap()
    });
    assert_eq!(*value, 17);
    assert_eq!(isolated_counts, [0; 3]);
    let (empty, _) = new_pool(false);
    let (populated, _) = new_pool(false);
    populated.push_serial(7, index(20));
    populated.push_parallel(8, TaskWeight::Highest);
    for pool in [empty, populated] {
        for _ in 0..100 {
            // Pool preparation, black_box, assertions and draining are outside the window.
            let (result, counts) = measured(|| pool.queue_length_estimates());
            std::hint::black_box(result);
            assert_eq!(counts, [0; 3], "public query must not allocate");
        }
    }
    assert_eq!(drain(populated), vec![7, 8]);
}

#[derive(Default)]
struct Calls {
    threshold: AtomicUsize,
    idle: AtomicUsize,
    execute: AtomicUsize,
}
impl Calls {
    fn snapshot(&self) -> [usize; 3] {
        [self.threshold.load(Ordering::SeqCst), self.idle.load(Ordering::SeqCst), self.execute.load(Ordering::SeqCst)]
    }
}
struct Executor {
    calls: Arc<Calls>,
    accept: bool,
}
impl TaskExecutor<usize> for Executor {
    fn fast_path_threshold(&self) -> usize {
        self.calls.threshold.fetch_add(1, Ordering::SeqCst);
        if self.accept { 100 } else { 0 }
    }
    fn is_idle(&self) -> bool {
        self.calls.idle.fetch_add(1, Ordering::SeqCst);
        self.accept
    }
    fn try_execute(&self, task: TaskHandle<usize, Self>) -> Result<(), TaskHandle<usize, Self>> {
        self.calls.execute.fetch_add(1, Ordering::SeqCst);
        if self.accept { drop(task); Ok(()) } else { Err(task) }
    }
}
fn new_pool(accept: bool) -> (&'static TaskPool<usize, Executor>, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    let pool = TaskPool::new(Executor { calls: Arc::clone(&calls), accept });
    (pool, calls)
}
fn index(value: usize) -> SerialQueueIndex {
    SerialQueueIndex::new(value).unwrap()
}
const WEIGHTS: [TaskWeight; 5] = [TaskWeight::Lowest, TaskWeight::Low, TaskWeight::Middle, TaskWeight::High, TaskWeight::Highest];
fn expected(serial: [usize; 21], parallel: [usize; 5]) -> QueueLengthEstimates {
    QueueLengthEstimates { serial, parallel }
}
fn drain(pool: &'static TaskPool<usize, Executor>) -> Vec<usize> {
    let mut tasks = Vec::new();
    while let Some(task) = pool.pop(0) { tasks.push(*task); }
    tasks.sort_unstable();
    tasks
}
// Deliberately accepts an ordinary borrow, not a static receiver.
fn shared_query(pool: &TaskPool<usize, Executor>) -> QueueLengthEstimates {
    pool.queue_length_estimates()
}

pub fn empty_pool_returns_all_zeroes_through_shared_borrow() {
    let (pool, _) = new_pool(false);
    assert_eq!(shared_query(pool), expected([0; 21], [0; 5]));
}

pub fn serial_bands_and_all_group_positions_are_decoded() {
    // Explicit contract examples, not a copy of the bit-decoding implementation.
    let examples = [(0, 0), (1, 16), (16, 16), (17, 32), (32, 32), (33, 48), (49, 48)];
    for rotation in 0..examples.len() {
        let (pool, _) = new_pool(false);
        let mut serial = [0; 21];
        for (group, estimate) in serial.iter_mut().enumerate() {
            let (count, value) = examples[(group + rotation) % examples.len()];
            *estimate = value;
            for task in 0..count { pool.push_serial(task, index(group)); }
        }
        assert_eq!(pool.queue_length_estimates(), expected(serial, [0; 5]), "rotation {rotation}");
        drain(pool);
    }
}

pub fn occupied_groups_keep_waiting_estimates_without_counting_used() {
    let (pool, _) = new_pool(false);
    for task in 0..18 { pool.push_serial(task, index(0)); }
    pool.push_serial(100, index(20));
    let first = pool.pop(0).unwrap();
    assert_eq!(*first, 0);
    let last = pool.pop(0).unwrap();
    assert_eq!(*last, 100);
    let mut serial = [0; 21];
    serial[0] = 32;
    for _ in 0..10 { assert_eq!(pool.queue_length_estimates(), expected(serial, [0; 5])); }
    assert!(pool.pop(0).is_none(), "query must not release either held group");
    drop(first);
    drop(last);
    for value in 1..18 {
        let task = pool.pop(0).unwrap();
        assert_eq!(*task, value);
    }
    assert!(pool.pop(0).is_none());
}

pub fn parallel_fields_preserve_order_and_saturate_independently() {
    let examples = [
        ([0, 0, 0, 0, 0], [0, 0, 0, 0, 0]),
        ([1, 3, 17, 257, 2049], [1, 3, 17, 257, 2049]),
        ([8190, 8190, 8190, 8190, 4094], [8190, 8190, 8190, 8190, 4094]),
        ([8191, 8191, 8191, 8191, 4095], [8191, 8191, 8191, 8191, 4095]),
        ([8192, 8192, 8192, 8192, 4096], [8191, 8191, 8191, 8191, 4095]),
    ];
    for (counts, estimates) in examples {
        let (pool, _) = new_pool(false);
        for (weight, count) in WEIGHTS.into_iter().zip(counts) {
            for value in 0..count { pool.push_parallel(value, weight); }
        }
        assert_eq!(pool.queue_length_estimates(), expected([0; 21], estimates), "counts {counts:?}");
        drain(pool);
    }
}

pub fn repeated_queries_preserve_tasks_and_never_call_executor() {
    let (pool, calls) = new_pool(false);
    for value in 10..13 { pool.push_serial(value, index(0)); }
    for value in 20..22 { pool.push_parallel(value, TaskWeight::Lowest); }
    let before = calls.snapshot();
    let mut serial = [0; 21];
    serial[0] = 16;
    for _ in 0..100 {
        assert_eq!(pool.queue_length_estimates(), expected(serial, [2, 0, 0, 0, 0]));
    }
    assert_eq!(calls.snapshot(), before, "none of the three executor callbacks may run");
    assert_eq!(drain(pool), vec![10, 11, 12, 20, 21]);
}

pub fn returned_arrays_are_independently_owned() {
    let (pool, _) = new_pool(false);
    pool.push_serial(7, index(20));
    pool.push_parallel(8, TaskWeight::Highest);
    let mut result = pool.queue_length_estimates();
    result.serial.fill(usize::MAX);
    result.parallel.fill(usize::MAX);
    let mut serial = [0; 21];
    serial[20] = 16;
    assert_eq!(pool.queue_length_estimates(), expected(serial, [0, 0, 0, 0, 1]));
    assert_eq!(result, expected([usize::MAX; 21], [usize::MAX; 5]));
    assert_eq!(drain(pool), vec![7, 8]);
}

pub fn fast_accepted_tasks_are_not_added_to_estimates() {
    let (pool, calls) = new_pool(true);
    pool.push_serial(7, index(0));
    pool.push_parallel(8, TaskWeight::Highest);
    assert_eq!(calls.execute.load(Ordering::SeqCst), 2);
    let before = calls.snapshot();
    for _ in 0..10 { assert_eq!(pool.queue_length_estimates(), expected([0; 21], [0; 5])); }
    assert_eq!(calls.snapshot(), before);
    assert!(pool.pop(0).is_none());
}

pub fn concurrent_queries_stay_in_range_and_preserve_all_tasks() {
    let (pool, calls) = new_pool(false);
    let start = Barrier::new(4);
    let observed = AtomicUsize::new(0);
    let progress = [AtomicUsize::new(0), AtomicUsize::new(0)];
    thread::scope(|scope| {
        for producer in 0..2 {
            let start = &start;
            let observed = &observed;
            let progress = &progress;
            scope.spawn(move || {
                start.wait();
                let mut count = 0;
                // No observation barrier in this loop: submissions and queries may
                // proceed concurrently, including across the query's two loads.
                while observed.load(Ordering::SeqCst) < 2 {
                    let value = count * 2 + producer;
                    if producer == 0 {
                        pool.push_serial(value, index(20));
                    } else {
                        pool.push_parallel(value, TaskWeight::Highest);
                    }
                    count += 1;
                    progress[producer].store(count, Ordering::SeqCst);
                    thread::yield_now();
                }
            });
        }
        for _ in 0..2 {
            let start = &start;
            let observed = &observed;
            let progress = &progress;
            scope.spawn(move || {
                start.wait();
                let in_range = |result: QueueLengthEstimates| {
                    result.serial.into_iter().all(|value| [0, 16, 32, 48].contains(&value))
                        && result.parallel[..4].iter().all(|value| *value <= 8191)
                        && result.parallel[4] <= 4095
                };
                let mut valid = in_range(pool.queue_length_estimates());
                let before = [progress[0].load(Ordering::SeqCst), progress[1].load(Ordering::SeqCst)];
                let mut queries = 0;
                let after = loop {
                    valid &= in_range(pool.queue_length_estimates());
                    queries += 1;
                    let current = [progress[0].load(Ordering::SeqCst), progress[1].load(Ordering::SeqCst)];
                    if queries >= 2000 && current[0] > before[0] && current[1] > before[1] {
                        break current;
                    }
                    thread::yield_now();
                };
                // Signal completion before asserting so failed range checks do not
                // leave producers spinning. No exact snapshot is asserted.
                observed.fetch_add(1, Ordering::SeqCst);
                assert!(valid, "all observed estimates must stay in contract ranges");
                assert!(after[0] > before[0] && after[1] > before[1]);
                assert!(queries >= 2000);
            });
        }
    });
    let serial_count = progress[0].load(Ordering::SeqCst);
    let parallel_count = progress[1].load(Ordering::SeqCst);
    assert_eq!(observed.load(Ordering::SeqCst), 2);
    assert_eq!(calls.snapshot(), [serial_count + parallel_count, 0, 0]);
    let mut submitted = (0..serial_count).map(|value| value * 2)
        .chain((0..parallel_count).map(|value| value * 2 + 1)).collect::<Vec<_>>();
    submitted.sort_unstable();
    assert_eq!(drain(pool), submitted);
}
