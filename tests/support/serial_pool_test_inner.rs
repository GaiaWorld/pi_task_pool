// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/serial_pool_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;

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

pub fn all_serial_indices_round_trip() {
    for index in 0..21 {
        let pool = TaskPool::new(QueuedOnly);
        pool.push_serial(index, SerialQueueIndex::new(index).unwrap());
        let handle = pool.pop(0).expect("legal serial group must be selectable");
        assert_eq!(*handle, index);
        drop(handle);
        assert!(pool.pop(0).is_none());
    }
}

pub fn invalid_indices_are_rejected_before_publication() {
    for index in [21, 22, usize::MAX] {
        assert!(
            SerialQueueIndex::new(index).is_none(),
            "index {index} must be rejected instead of panicking at submission"
        );
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(QueuedOnly);
    pool.push_serial(
        TrackedTask {
            value: 20,
            drops: Arc::clone(&drops),
        },
        SerialQueueIndex::new(20).expect("group 20 is valid"),
    );
    let handle = pool.pop(0).expect("valid group after rejected indices");
    assert_eq!(handle.value, 20);
    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(pool.pop(0).is_none());
}

pub fn same_group_is_fifo_until_handle_release() {
    for group in [0, 10, 20] {
        let pool = TaskPool::new(QueuedOnly);
        for value in [11, 22, 33] {
            pool.push_serial(value, SerialQueueIndex::new(group).unwrap());
        }
        for expected in [11, 22, 33] {
            let handle = pool.pop(0).expect("next FIFO task after release");
            assert_eq!(*handle, expected);
            assert!(pool.pop(0).is_none(), "live handle owns group {group}");
            drop(handle);
        }
        assert!(pool.pop(0).is_none());
    }
}

pub fn serial_count_boundaries_drain_in_order() {
    for count in [0, 1, 16, 17, 32, 33, 48, 49, 129] {
        let pool = TaskPool::new(QueuedOnly);
        for id in 0..count {
            pool.push_serial(id, SerialQueueIndex::new(20).unwrap());
        }
        for expected in 0..count {
            let handle = pool
                .pop(0)
                .expect("remaining serial task must stay visible");
            assert_eq!(*handle, expected, "initial count {count}");
            drop(handle);
        }
        assert!(pool.pop(0).is_none(), "must drain count {count}");
    }
}

pub fn borrowing_does_not_release_serial_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(QueuedOnly);
    for value in [7, 8] {
        pool.push_serial(
            TrackedTask {
                value,
                drops: Arc::clone(&drops),
            },
            SerialQueueIndex::new(10).unwrap(),
        );
    }
    let mut first = pool.pop(0).expect("first tracked serial task");
    let borrowed: &TrackedTask = &first;
    assert_eq!(borrowed.value, 7);
    let borrowed_mut: &mut TrackedTask = &mut first;
    borrowed_mut.value = 9;
    assert_eq!(first.value, 9);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(pool.pop(0).is_none(), "borrowing must not unlock");
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let second = pool.pop(0).expect("release preserves queued-task hint");
    assert_eq!(second.value, 8);
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(pool.pop(0).is_none());
}

pub fn held_group_does_not_block_other_groups() {
    let pool = TaskPool::new(QueuedOnly);
    for group in 0..21 {
        for sequence in 0..2 {
            pool.push_serial((group, sequence), SerialQueueIndex::new(group).unwrap());
        }
    }
    let mut held = Vec::new();
    let mut seen = [false; 21];
    for _ in 0..21 {
        let handle = pool
            .pop(0)
            .expect("other unoccupied group remains selectable");
        let (group, sequence) = *handle;
        assert_eq!(sequence, 0);
        assert!(!seen[group], "cannot claim a held group twice");
        seen[group] = true;
        held.push(handle);
    }
    assert!(seen.into_iter().all(|value| value));
    assert!(pool.pop(0).is_none(), "only occupied groups remain");
    let position = held
        .iter()
        .position(|handle| handle.0 == 10)
        .expect("held group 10");
    drop(held.swap_remove(position));
    let next = pool.pop(0).expect("released group must be selectable");
    assert_eq!(*next, (10, 1));
    assert!(pool.pop(0).is_none(), "other groups must remain owned");
    drop(next);
    drop(held);
    let mut remainder = Vec::new();
    while let Some(handle) = pool.pop(0) {
        remainder.push(*handle);
    }
    remainder.sort_unstable();
    assert_eq!(
        remainder,
        (0..21)
            .filter(|group| *group != 10)
            .map(|group| (group, 1))
            .collect::<Vec<_>>()
    );
}

pub fn held_serial_group_does_not_hide_parallel_task() {
    let pool = TaskPool::new(QueuedOnly);
    pool.push_serial(11, SerialQueueIndex::new(0).unwrap());
    let held = pool.pop(0).expect("serial head");
    pool.push_serial(22, SerialQueueIndex::new(0).unwrap());
    pool.push_parallel(99, TaskWeight::Middle);
    let parallel = pool.pop(0).expect("parallel remains available");
    assert_eq!(*parallel, 99);
    assert!(pool.pop(0).is_none());
    drop(parallel);
    drop(held);
    assert_eq!(*pool.pop(0).expect("serial tail after release"), 22);
    assert!(pool.pop(0).is_none());
}

pub fn normal_panic_unwind_releases_group() {
    let pool = TaskPool::new(QueuedOnly);
    pool.push_serial(11, SerialQueueIndex::new(20).unwrap());
    pool.push_serial(22, SerialQueueIndex::new(20).unwrap());
    // Obtain outside catch_unwind so a missing push/pop cannot masquerade as the expected task panic.
    let first = pool.pop(0).expect("first task before intentional panic");
    assert_eq!(*first, 11);
    let result = catch_unwind(AssertUnwindSafe(move || {
        let _owned_until_unwind = first;
        panic!("intentional serial task panic");
    }));
    let payload = result.expect_err("task panic must propagate");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("intentional serial task panic")
    );
    let second = pool.pop(0).expect("unwind must release group");
    assert_eq!(*second, 22);
    drop(second);
    assert!(pool.pop(0).is_none());
}

pub fn serial_handle_release_can_happen_on_another_thread() {
    let pool = TaskPool::new(QueuedOnly);
    pool.push_serial(Cell::new(7u8), SerialQueueIndex::new(20).unwrap());
    pool.push_serial(Cell::new(8u8), SerialQueueIndex::new(20).unwrap());
    let first = pool.pop(0).expect("first Cell task");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let receiver = thread::spawn(move || {
        assert_eq!(first.get(), 7);
        first.set(9);
        assert_eq!(first.get(), 9);
        held_tx.send(()).expect("confirm remote ownership");
        release_rx.recv().expect("wait for release permission");
        drop(first);
    });
    held_rx.recv().expect("receiving thread holds the handle");
    // Save the observation, then always release and join before asserting so an
    // exclusivity regression cannot strand the receiving thread.
    let blocked_while_remote_owned = pool.pop(0).is_none();
    release_tx.send(()).expect("permit remote release");
    receiver
        .join()
        .expect("receiving thread releases serial handle");
    assert!(
        blocked_while_remote_owned,
        "remote live handle must own its group"
    );
    assert_eq!(
        pool.pop(0)
            .expect("cross-thread release publishes availability")
            .get(),
        8
    );
    assert!(pool.pop(0).is_none());
}

pub fn serial_handle_remains_valid_after_facades_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(QueuedOnly);
    let clone = pool;
    pool.push_serial(
        TrackedTask {
            value: 7,
            drops: Arc::clone(&drops),
        },
        SerialQueueIndex::new(20).unwrap(),
    );
    let mut handle = clone.pop(0).expect("serial handle");
    assert_eq!(handle.value, 7);
    handle.value = 9;
    assert_eq!(handle.value, 9);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

// Observe ownership only through handles returned by the public API. Release our
// observation just before Drop; no second handle may enter while this one is held.
fn consume(
    handle: TaskHandle<(usize, usize), QueuedOnly>,
    active: &[AtomicUsize; 21],
) -> (usize, usize) {
    let value = *handle;
    assert_eq!(
        active[value.0].fetch_add(1, Ordering::SeqCst),
        0,
        "overlapping serial group {}",
        value.0
    );
    thread::yield_now();
    assert_eq!(active[value.0].fetch_sub(1, Ordering::SeqCst), 1);
    drop(handle);
    value
}

pub fn competing_consumers_never_overlap_same_group() {
    let pool = TaskPool::new(QueuedOnly);
    for group in 0..21 {
        for id in 0..40 {
            pool.push_serial((group, id), SerialQueueIndex::new(group).unwrap());
        }
    }
    let active = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let start = Arc::new(Barrier::new(4));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let active = Arc::clone(&active);
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            let mut values = Vec::new();
            while let Some(handle) = pool.pop(0) {
                values.push(consume(handle, &active));
            }
            values
        }));
    }
    let mut values = Vec::new();
    for worker in workers {
        values.extend(worker.join().expect("consumer must not panic"));
    }
    while let Some(handle) = pool.pop(0) {
        values.push(consume(handle, &active));
    }
    values.sort_unstable();
    assert_eq!(
        values,
        (0..21)
            .flat_map(|group| (0..40).map(move |id| (group, id)))
            .collect::<Vec<_>>()
    );
    assert!(pool.pop(0).is_none());
}

pub fn different_group_producers_and_consumers_preserve_tasks() {
    let pool = TaskPool::new(QueuedOnly);
    let active = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let start = Arc::new(Barrier::new(6));
    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    let finished = Arc::new(AtomicUsize::new(0));
    let acknowledged = Arc::new(AtomicUsize::new(0));
    let (ack_senders, ack_receivers): (Vec<_>, Vec<_>) =
        (0..3).map(|_| std::sync::mpsc::channel()).unzip();
    // Completion is published even if a producer panics, so consumers can exit
    // and all workers can be joined on the missing-implementation RED path.
    struct ProducerFinished(Arc<AtomicUsize>);
    impl Drop for ProducerFinished {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for (group, ack_rx) in [0, 10, 20].into_iter().zip(ack_receivers) {
        let start = Arc::clone(&start);
        let finished = Arc::clone(&finished);
        producers.push(thread::spawn(move || {
            let _completion = ProducerFinished(finished);
            start.wait();
            pool.push_serial((group, 0), SerialQueueIndex::new(group).unwrap());
            // This timeout diagnoses a broken handshake; elapsed time is not a
            // success condition. No tail submission can precede consumption.
            ack_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("consumer must acknowledge this group's first task before tail submission");
            for id in 1..100 {
                pool.push_serial((group, id), SerialQueueIndex::new(group).unwrap());
                thread::yield_now();
            }
        }));
    }
    for _ in 0..3 {
        let active = Arc::clone(&active);
        let start = Arc::clone(&start);
        let finished = Arc::clone(&finished);
        let acknowledged = Arc::clone(&acknowledged);
        let ack_senders = ack_senders.clone();
        consumers.push(thread::spawn(move || {
            start.wait();
            let mut values = Vec::new();
            loop {
                if let Some(handle) = pool.pop(0) {
                    let value = consume(handle, &active);
                    values.push(value);
                    if value.1 == 0 {
                        acknowledged.fetch_add(1, Ordering::SeqCst);
                        ack_senders[value.0 / 10]
                            .send(())
                            .expect("producer must wait for first-task acknowledgment");
                    }
                } else if finished.load(Ordering::SeqCst) == 3 {
                    break;
                } else {
                    // None is not completion while producers are active.
                    thread::yield_now();
                }
            }
            values
        }));
    }
    let production_ok = producers
        .into_iter()
        .map(|worker| worker.join().is_ok())
        .collect::<Vec<_>>();
    let mut values = Vec::new();
    for worker in consumers {
        values.extend(worker.join().expect("consumer must not panic"));
    }
    assert!(
        production_ok.into_iter().all(|ok| ok),
        "serial producers must succeed"
    );
    assert_eq!(
        acknowledged.load(Ordering::SeqCst),
        3,
        "all groups must be consumed before their producer submits the tail"
    );
    for group in [0, 10, 20] {
        assert!(
            values.contains(&(group, 0)),
            "worker consumer must take group {group}'s first task before final drain"
        );
    }
    while let Some(handle) = pool.pop(0) {
        values.push(consume(handle, &active));
    }
    values.sort_unstable();
    assert_eq!(
        values,
        [0, 10, 20]
            .into_iter()
            .flat_map(|group| (0..100).map(move |id| (group, id)))
            .collect::<Vec<_>>()
    );
    assert!(pool.pop(0).is_none());
}

pub fn serial_handle_send_sync_contracts_compile() {
    fn requires_send<T: Send>() {}
    fn requires_sync<T: Sync>() {}
    requires_send::<TaskHandle<Cell<u8>, QueuedOnly>>();
    requires_send::<TaskHandle<u8, QueuedOnly>>();
    requires_sync::<TaskHandle<u8, QueuedOnly>>();
    requires_send::<TaskPool<Cell<u8>, QueuedOnly>>();
    requires_sync::<TaskPool<Cell<u8>, QueuedOnly>>();
}
