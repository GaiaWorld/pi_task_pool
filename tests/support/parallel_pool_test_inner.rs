// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/parallel_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;

const WEIGHTS: [TaskWeight; 5] = [
    TaskWeight::Lowest,
    TaskWeight::Low,
    TaskWeight::Middle,
    TaskWeight::High,
    TaskWeight::Highest,
];

// Zero disables fast execution. Unexpected callbacks are fixture failures.
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

struct TrackedTask {
    value: usize,
    drops: Arc<AtomicUsize>,
}
impl Drop for TrackedTask {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct TrackedExecutor(Arc<AtomicUsize>);
impl<T> TaskExecutor<T> for TrackedExecutor {
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
impl Drop for TrackedExecutor {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_exact_ids(mut ids: Vec<usize>, count: usize) {
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..count).collect::<Vec<_>>(),
        "tasks must be neither duplicated nor lost"
    );
}

pub fn empty_pool_returns_none() {
    let pool = TaskPool::<usize, _>::new(QueuedOnly);
    assert!(pool.pop(0).is_none());
}

pub fn all_weights_round_trip() {
    assert_eq!(WEIGHTS.map(|w| w as u64), [1, 10, 100, 1000, 10000]);
    for weight in WEIGHTS {
        let pool = TaskPool::new(QueuedOnly);
        pool.push_parallel(42, weight);
        assert_eq!(*pool.pop(0).expect("submitted task"), 42);
        assert!(pool.pop(0).is_none());
    }
}

pub fn clones_share_tasks_without_clone_bounds() {
    struct NotClone(usize);
    let pool = TaskPool::new(QueuedOnly);
    let clone = pool;
    pool.push_parallel(NotClone(11), TaskWeight::Middle);
    assert_eq!(clone.pop(0).expect("shared task").0, 11);
    clone.push_parallel(NotClone(22), TaskWeight::Middle);
    assert_eq!(pool.pop(0).expect("shared task").0, 22);
    assert!(pool.pop(0).is_none());
    assert!(clone.pop(0).is_none());
}

pub fn same_weight_handles_coexist() {
    for weight in WEIGHTS {
        let pool = TaskPool::new(QueuedOnly);
        pool.push_parallel(11, weight);
        pool.push_parallel(22, weight);
        let first = pool.pop(0).expect("first task");
        let second = pool
            .pop(0)
            .expect("parallel task must not wait for first drop");
        assert_eq!((*first, *second), (11, 22));
        assert!(pool.pop(0).is_none());
    }
}

pub fn mutable_borrow_preserves_task_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(QueuedOnly);
    pool.push_parallel(
        TrackedTask {
            value: 7,
            drops: drops.clone(),
        },
        TaskWeight::Middle,
    );
    let mut handle = pool.pop(0).expect("tracked task");
    let borrowed: &TrackedTask = &handle;
    assert_eq!(borrowed.value, 7);
    let borrowed_mut: &mut TrackedTask = &mut handle;
    borrowed_mut.value = 9;
    assert_eq!(handle.value, 9);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

pub fn dropping_handle_does_not_block_parallel_queue() {
    let pool = TaskPool::new(QueuedOnly);
    for value in 0..3 {
        pool.push_parallel(value, TaskWeight::High);
    }
    let first = pool.pop(0).expect("first");
    let second = pool.pop(0).expect("second");
    assert_eq!(*first, 0);
    drop(first);
    let third = pool.pop(0).expect("third while second lives");
    assert_eq!((*second, *third), (1, 2));
    assert!(pool.pop(0).is_none());
}

pub fn saturated_fields_can_be_drained() {
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        let count = if index == 4 { 4095 + 257 } else { 8191 + 257 };
        let pool = TaskPool::new(QueuedOnly);
        for id in 0..count {
            pool.push_parallel(id, weight);
        }
        let mut ids = Vec::new();
        while let Some(task) = pool.pop(0) {
            ids.push(*task);
            assert!(ids.len() <= count, "duplicate task or nonterminating drain");
        }
        assert_exact_ids(ids, count);
        assert!(pool.pop(0).is_none());
    }
}

pub fn mixed_weights_preserve_every_task() {
    let pool = TaskPool::new(QueuedOnly);
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        for offset in 0..31 {
            pool.push_parallel(index * 31 + offset, weight);
        }
    }
    let mut ids = Vec::new();
    while let Some(task) = pool.pop(0) {
        ids.push(*task);
        assert!(ids.len() <= 155);
    }
    assert_exact_ids(ids, 155);
}

pub fn concurrent_consumers_take_each_task_once() {
    let pool = TaskPool::new(QueuedOnly);
    for id in 0..4000 {
        pool.push_parallel(id, WEIGHTS[id % 5]);
    }
    let barrier = Arc::new(Barrier::new(4));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            let mut ids = Vec::new();
            while let Some(task) = pool.pop(0) {
                ids.push(*task);
                assert!(ids.len() <= 4000);
            }
            ids
        }));
    }
    let mut ids = Vec::new();
    for worker in workers {
        ids.extend(worker.join().expect("consumer panicked"));
    }
    assert_exact_ids(ids, 4000);
    assert!(pool.pop(0).is_none());
}

pub fn concurrent_producers_and_consumers_preserve_tasks() {
    let pool = TaskPool::new(QueuedOnly);
    let barrier = Arc::new(Barrier::new(9));
    let remaining = Arc::new(AtomicUsize::new(5));
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Some(index) acknowledges a consumed first task; None reports worker failure.
    let (ack_tx, ack_rx) = std::sync::mpsc::channel();
    let mut permits = Vec::new();
    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    // One producer per queue respects the nonoverlapping same-queue push boundary.
    for (index, weight) in WEIGHTS.into_iter().enumerate() {
        let (permit_tx, permit_rx) = std::sync::mpsc::channel();
        permits.push(permit_tx);
        let barrier = barrier.clone();
        let remaining = remaining.clone();
        let cancelled = cancelled.clone();
        let ack_tx = ack_tx.clone();
        producers.push(thread::spawn(move || {
            barrier.wait();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if cancelled.load(Ordering::Acquire) {
                    return;
                }
                pool.push_parallel(index * 1000, weight);
                // Dropping all permits on cancellation also wakes blocked producers.
                if permit_rx.recv().is_err() {
                    return;
                }
                for offset in 1..1000 {
                    if cancelled.load(Ordering::Acquire) {
                        return;
                    }
                    pool.push_parallel(index * 1000 + offset, weight);
                }
            }));
            if result.is_err() {
                cancelled.store(true, Ordering::Release);
                let _ = ack_tx.send(None);
            }
            // Account for completion even when the producer panics.
            remaining.fetch_sub(1, Ordering::AcqRel);
            result
        }));
    }
    for _ in 0..4 {
        let ack_tx = ack_tx.clone();
        let barrier = barrier.clone();
        let remaining = remaining.clone();
        let cancelled = cancelled.clone();
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut ids = Vec::new();
                // This phase only exercises overlap. Completion is not an empty observation.
                while !cancelled.load(Ordering::Acquire) && remaining.load(Ordering::Acquire) != 0 {
                    if let Some(task) = pool.pop(0) {
                        if *task % 1000 == 0 {
                            ack_tx.send(Some(*task / 1000)).expect("coordinator alive");
                        }
                        ids.push(*task);
                        assert!(ids.len() <= 5000, "duplicate or nonterminating consumption");
                    } else {
                        thread::yield_now();
                    }
                }
                ids
            }));
            if result.is_err() {
                cancelled.store(true, Ordering::Release);
                let _ = ack_tx.send(None);
            }
            result
        }));
    }
    drop(ack_tx);
    let mut acknowledged = [false; 5];
    let mut handshake_error = None;
    for _ in 0..5 {
        // Watchdog for a broken pool, never a scheduling assertion.
        match ack_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(Some(index)) if index < 5 && !acknowledged[index] => {
                acknowledged[index] = true;
                if permits[index].send(()).is_err() {
                    handshake_error = Some(format!("producer {index} exited before permit"));
                    break;
                }
            }
            other => {
                handshake_error = Some(format!("invalid/missing acknowledgement: {other:?}"));
                break;
            }
        }
    }
    if handshake_error.is_some() {
        cancelled.store(true, Ordering::Release);
    }
    drop(permits);
    // Collect every join before asserting, including worker panic and handshake failures.
    let producer_results: Vec<_> = producers.into_iter().map(|p| p.join()).collect();
    let consumer_results: Vec<_> = consumers.into_iter().map(|c| c.join()).collect();
    assert!(handshake_error.is_none(), "{handshake_error:?}");
    for result in producer_results {
        result
            .expect("producer thread panicked")
            .expect("producer operation panicked");
    }
    let mut ids = Vec::new();
    for result in consumer_results {
        ids.extend(
            result
                .expect("consumer thread panicked")
                .expect("consumer operation panicked"),
        );
    }
    assert!(
        !cancelled.load(Ordering::Acquire),
        "worker cancelled the run"
    );
    assert_eq!(remaining.load(Ordering::Acquire), 0);
    assert_eq!(acknowledged, [true; 5]);
    // All producers AND consumers have joined: no publication or recovery is paused.
    // Only now does a fresh None justify finishing the final drain. A consumer thread
    // performs it; the coordinator never rescues a missing handshake or failed worker.
    let drain_pool = pool;
    let tail = thread::spawn(move || {
        let mut tail = Vec::new();
        while let Some(task) = drain_pool.pop(0) {
            tail.push(*task);
            assert!(tail.len() <= 5000, "nonterminating final drain");
        }
        tail
    })
    .join()
    .expect("final consumer panicked");
    ids.extend(tail);
    assert_exact_ids(ids, 5000);
    assert!(pool.pop(0).is_none());
}

pub fn handles_outlive_all_pool_facades() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(QueuedOnly);
    let clone = pool;
    pool.push_parallel(
        TrackedTask {
            value: 7,
            drops: drops.clone(),
        },
        TaskWeight::Low,
    );
    let mut handle = clone.pop(0).expect("task");
    assert_eq!(handle.value, 7);
    handle.value = 9;
    assert_eq!(handle.value, 9);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

pub fn permanent_reference_retains_executor_and_queued_tasks() {
    let task_drops = Arc::new(AtomicUsize::new(0));
    let executor_drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(TrackedExecutor(executor_drops.clone()));
    pool.push_parallel(
        TrackedTask {
            value: 7,
            drops: task_drops.clone(),
        },
        TaskWeight::Lowest,
    );
    assert_eq!(
        task_drops.load(Ordering::SeqCst),
        0,
        "queued tasks live with the permanent pool"
    );
    assert_eq!(
        executor_drops.load(Ordering::SeqCst),
        0,
        "executor lives with the permanent pool"
    );
}

// Compile probes are isolated consumer crates: they import only the public library.
// Internal counter rollback, exact CAS interleavings, empty-pop repair, and the
// permanent-Arc mechanism require implementation review; API outcomes do not prove them.
struct CompileProbe(std::path::PathBuf);
impl Drop for CompileProbe {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn compile_probe(body: &str, expected_error: Option<(&str, &str)>) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "pi-task-pool-contract-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(directory.join("src")).expect("create compile probe");
    let probe = CompileProbe(directory);
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .to_str()
        .expect("UTF-8 workspace")
        .replace('\\', "/");
    std::fs::write(probe.0.join("Cargo.toml"), format!("[package]\nname = \"pool_contract_probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n[dependencies]\npi_task_pool = {{ path = \"{root}\" }}\n[workspace]\n")).expect("write consumer manifest");
    let prelude = r#"
use pi_task_pool::{TaskExecutor, TaskHandle, TaskPool, SerialQueueIndex};
use std::{cell::Cell, rc::Rc, marker::PhantomData};
struct Executor<M>(PhantomData<M>);
impl<T, M> TaskExecutor<T> for Executor<M> {
    fn fast_path_threshold(&self) -> usize { 0 }
    fn is_idle(&self) -> bool { false }
    fn try_execute(&self, task: TaskHandle<T, Self>) -> Result<(), TaskHandle<T, Self>> { Err(task) }
}
fn needs_send<T: Send>() {}
fn needs_sync<T: Sync>() {}
"#;
    std::fs::write(probe.0.join("src/main.rs"), format!("{prelude}\n{body}\n"))
        .expect("write consumer source");
    let output = std::process::Command::new(env!("CARGO"))
        .args(["check", "--offline", "--quiet", "--manifest-path"])
        .arg(probe.0.join("Cargo.toml"))
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .expect("run cargo compile probe");
    let stderr = String::from_utf8_lossy(&output.stderr);
    match expected_error {
        None => assert!(
            output.status.success(),
            "valid public contract must compile:\n{stderr}"
        ),
        Some((code, subject)) => {
            assert!(
                !output.status.success(),
                "invalid public contract unexpectedly compiled: {body}"
            );
            assert!(
                stderr.contains(code) && stderr.contains(subject),
                "expected {code} for {subject}, not an unrelated build failure:\n{stderr}"
            );
        }
    }
}

pub fn handle_send_sync_positive_contracts() {
    // Cell is Send but not Sync: moving a handle must not unnecessarily require T: Sync.
    compile_probe("fn main() { needs_send::<TaskHandle<Cell<u8>, Executor<()>>>(); needs_send::<TaskHandle<u8, Executor<()>>>(); needs_sync::<TaskHandle<u8, Executor<()>>>(); let _ = TaskPool::<Cell<u8>, _>::new(Executor::<()>(PhantomData)); }", None);
}

pub fn handle_send_sync_negative_contracts() {
    // Validate the probe setup independently before accepting any negative diagnostic.
    compile_probe(
        "fn main() { let _ = TaskPool::<u8, _>::new(Executor::<()>(PhantomData)); }",
        None,
    );
    for body in [
        "fn main() { needs_send::<TaskHandle<Rc<u8>, Executor<()>>>(); }",
        "fn main() { needs_sync::<TaskHandle<Cell<u8>, Executor<()>>>(); }",
        "fn main() { needs_sync::<TaskHandle<Rc<u8>, Executor<()>>>(); }",
        "fn main() { needs_send::<TaskHandle<u8, Executor<std::sync::MutexGuard<'static, ()>>>>>(); }",
        "fn main() { needs_sync::<TaskHandle<u8, Executor<std::sync::MutexGuard<'static, ()>>>>>(); }",
        "fn main() { needs_send::<TaskHandle<u8, Executor<Rc<()>>>>(); }",
        "fn main() { needs_sync::<TaskHandle<u8, Executor<Rc<()>>>>(); }",
        "fn main() { needs_send::<TaskHandle<u8, Executor<Cell<()>>>>(); }",
        "fn main() { needs_sync::<TaskHandle<u8, Executor<Cell<()>>>>(); }",
    ] {
        compile_probe(body, Some(("E0277", "cannot be")));
    }
}

pub fn construction_requires_static_task_and_executor() {
    compile_probe("static VALUE: u8 = 7; fn main() { let _ = TaskPool::<&'static u8, _>::new(Executor::<&'static u8>(PhantomData)); let _ = &VALUE; }", None);
    compile_probe("fn main() { let value = 7u8; let pool = TaskPool::new(Executor::<()>(PhantomData)); pool.push_parallel(&value, pi_task_pool::TaskWeight::Low); }", Some(("E0597", "value")));
    compile_probe(
        r#"
struct BorrowedExecutor<'a>(&'a u8);
impl<T> TaskExecutor<T> for BorrowedExecutor<'_> {
    fn fast_path_threshold(&self) -> usize { *self.0 as usize }
    fn is_idle(&self) -> bool { false }
    fn try_execute(&self, task: TaskHandle<T, Self>) -> Result<(), TaskHandle<T, Self>> { Err(task) }
}
fn main() { let value = 7u8; let _ = TaskPool::<u8, _>::new(BorrowedExecutor(&value)); }
"#,
        Some(("E0597", "value")),
    );
}
