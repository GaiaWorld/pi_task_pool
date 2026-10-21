// @api:src/lib.rs
// @impl:src/task_pool_inner.rs
// @test:tests/fast_path_test.rs
// @design-doc:design/lock-free-task-pool/07-design/call-chain.md
use pi_task_pool::{SerialQueueIndex, TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex};

struct TrackedTask { value: usize, drops: Arc<AtomicUsize> }
impl Drop for TrackedTask {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); }
}
#[derive(Clone, Copy)]
enum Response { Accept, Reject, Panic }
struct Control {
    idle: AtomicBool,
    idle_calls: AtomicUsize,
    attempts: AtomicUsize,
    held: Mutex<Vec<TaskHandle<TrackedTask, Probe>>>,
}
struct Probe { threshold: usize, response: Response, control: Arc<Control> }
impl TaskExecutor<TrackedTask> for Probe {
    fn fast_path_threshold(&self) -> usize { self.threshold }
    fn is_idle(&self) -> bool {
        self.control.idle_calls.fetch_add(1, Ordering::SeqCst);
        self.control.idle.load(Ordering::SeqCst)
    }
    fn try_execute(&self, task: TaskHandle<TrackedTask, Self>) -> Result<(), TaskHandle<TrackedTask, Self>> {
        self.control.attempts.fetch_add(1, Ordering::SeqCst);
        match self.response {
            Response::Accept => { self.control.held.lock().unwrap().push(task); Ok(()) }
            Response::Reject => Err(task),
            Response::Panic => { panic!("executor panic while holding original handle") }
        }
    }
}
fn setup(threshold: usize, response: Response, idle: bool) -> (&'static TaskPool<TrackedTask, Probe>, Arc<Control>, Arc<AtomicUsize>) {
    let control = Arc::new(Control {
        idle: AtomicBool::new(idle), idle_calls: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0), held: Mutex::new(Vec::new()),
    });
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = TaskPool::new(Probe { threshold, response, control: Arc::clone(&control) });
    (pool, control, drops)
}
fn task(value: usize, drops: &Arc<AtomicUsize>) -> TrackedTask {
    TrackedTask { value, drops: Arc::clone(drops) }
}
fn group(index: usize) -> SerialQueueIndex { SerialQueueIndex::new(index).unwrap() }
fn count(counter: &AtomicUsize) -> usize { counter.load(Ordering::SeqCst) }

pub fn threshold_zero_never_queries_idle() {
    let (serial, s, sd) = setup(0, Response::Accept, true);
    let (parallel, p, pd) = setup(0, Response::Accept, true);
    serial.push_serial(task(11, &sd), group(0));
    parallel.push_parallel(task(22, &pd), TaskWeight::Highest);
    assert_eq!(count(&s.idle_calls), 0);
    assert_eq!(count(&p.idle_calls), 0);
    assert_eq!(count(&s.attempts) + count(&p.attempts), 0);
    let first = serial.pop(0).expect("serial task queued");
    let second = parallel.pop(0).expect("parallel task queued");
    assert_eq!(first.value, 11);
    assert_eq!(second.value, 22);
    drop(first); drop(second);
    assert!(serial.pop(0).is_none());
    assert!(parallel.pop(0).is_none());
}

pub fn estimated_wait_at_threshold_skips_idle() {
    let (parallel, p, pd) = setup(1, Response::Accept, false);
    parallel.push_parallel(task(11, &pd), TaskWeight::Lowest);
    p.idle.store(true, Ordering::SeqCst);
    let before = count(&p.idle_calls);
    parallel.push_parallel(task(22, &pd), TaskWeight::Lowest);
    assert_eq!(count(&p.idle_calls), before);
    assert_eq!(count(&p.attempts), 0);
    for expected in [11, 22] {
        let handle = parallel.pop(0).expect("queued parallel task");
        assert_eq!(handle.value, expected);
    }
    assert!(parallel.pop(0).is_none());

    let (serial, s, sd) = setup(16, Response::Accept, false);
    serial.push_serial(task(33, &sd), group(0));
    s.idle.store(true, Ordering::SeqCst);
    let before = count(&s.idle_calls);
    serial.push_serial(task(44, &sd), group(1));
    assert_eq!(count(&s.idle_calls), before);
    assert_eq!(count(&s.attempts), 0);
    let first = serial.pop(0).expect("queued serial task");
    assert_eq!(first.value, 33);
    drop(first);
    let second = serial.pop(0).expect("second serial group queued");
    assert_eq!(second.value, 44);
    drop(second);
    assert!(serial.pop(0).is_none());
}

pub fn serial_estimate_below_threshold_checks_idle_and_hands_off() {
    let (pool, control, drops) = setup(17, Response::Accept, false);
    pool.push_serial(task(11, &drops), group(0));
    assert_eq!(count(&control.idle_calls), 1);
    control.idle.store(true, Ordering::SeqCst);
    pool.push_serial(task(22, &drops), group(1));
    assert_eq!(count(&control.idle_calls), 2, "16 is strictly less than threshold 17");
    assert_eq!(count(&control.attempts), 1);
    let accepted = control.held.lock().unwrap().pop().expect("empty group handed off");
    assert_eq!(accepted.value, 22);
    let queued = pool.pop(0).expect("prior serial task still queued");
    assert_eq!(queued.value, 11);
    drop(queued);
    assert!(pool.pop(0).is_none());
    drop(accepted);
    assert_eq!(count(&drops), 2);
}

pub fn nonempty_serial_queue_never_hands_off_successor() {
    let (pool, control, drops) = setup(17, Response::Accept, false);
    pool.push_serial(task(11, &drops), group(0));
    control.idle.store(true, Ordering::SeqCst);
    pool.push_serial(task(22, &drops), group(0));
    assert_eq!(count(&control.idle_calls), 2, "below threshold checks idle");
    assert_eq!(count(&control.attempts), 0, "occupied queue cannot hand off successor");
    for expected in [11, 22] {
        let handle = pool.pop(0).expect("same-group FIFO task");
        assert_eq!(handle.value, expected);
        assert!(pool.pop(0).is_none(), "group held until preceding handle releases");
        drop(handle);
    }
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}

pub fn idle_hint_controls_parallel_handoff() {
    let (pool, c, drops) = setup(2, Response::Accept, false);
    pool.push_parallel(task(11, &drops), TaskWeight::Low);
    c.idle.store(true, Ordering::SeqCst);
    pool.push_parallel(task(22, &drops), TaskWeight::Low);
    assert_eq!(count(&c.idle_calls), 2);
    assert_eq!(count(&c.attempts), 1);
    let queued = pool.pop(0).expect("non-idle submission stays queued");
    assert_eq!(queued.value, 11);
    drop(queued);
    assert!(pool.pop(0).is_none());
    let accepted = c.held.lock().unwrap().pop().expect("idle executor owns task");
    assert_eq!(accepted.value, 22);
    drop(accepted);
    assert_eq!(count(&drops), 2);
}

pub fn accepted_serial_handle_owns_group_until_drop() {
    let (pool, c, drops) = setup(1, Response::Accept, true);
    pool.push_serial(task(11, &drops), group(0));
    assert_eq!(count(&c.attempts), 1);
    pool.push_serial(task(22, &drops), group(0));
    assert_eq!(count(&c.attempts), 1, "occupied group cannot be handed off");
    assert!(pool.pop(0).is_none(), "queued successor cannot overtake live handle");
    let accepted = c.held.lock().unwrap().pop().expect("executor retains serial handle");
    assert_eq!(accepted.value, 11);
    drop(accepted);
    let successor = pool.pop(0).expect("successor visible after release");
    assert_eq!(successor.value, 22);
    drop(successor);
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}

pub fn rejected_serial_task_returns_once_and_preserves_fifo() {
    let (pool, c, drops) = setup(1, Response::Reject, true);
    pool.push_serial(task(11, &drops), group(20));
    assert_eq!(count(&c.attempts), 1);
    assert_eq!(count(&drops), 0, "rejected task remains owned by pool");
    pool.push_serial(task(22, &drops), group(20));
    assert_eq!(count(&c.attempts), 1);
    for expected in [11, 22] {
        let handle = pool.pop(0).expect("rejected task and successor must be published");
        assert_eq!(handle.value, expected);
        assert!(pool.pop(0).is_none(), "same group stays occupied while handle lives");
        drop(handle);
    }
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}

pub fn rejected_parallel_task_remains_visible_exactly_once() {
    let (pool, c, drops) = setup(1, Response::Reject, true);
    pool.push_parallel(task(42, &drops), TaskWeight::Highest);
    assert_eq!(count(&c.attempts), 1);
    assert_eq!(count(&drops), 0);
    let handle = pool.pop(0).expect("rejected task must have a published hint");
    assert_eq!(handle.value, 42);
    assert!(pool.pop(0).is_none());
    drop(handle);
    assert_eq!(count(&drops), 1);
}

pub fn rejected_serial_task_releases_group_after_publication() {
    let (pool, c, drops) = setup(1, Response::Reject, true);
    pool.push_serial(task(11, &drops), group(5));
    let first = pool.pop(0).expect("rejected task published and group released");
    assert_eq!(first.value, 11);
    drop(first);
    assert!(pool.pop(0).is_none());
    pool.push_serial(task(22, &drops), group(5));
    assert_eq!(count(&c.attempts), 2, "group may be claimed again after rollback");
    let second = pool.pop(0).expect("second rejection must also publish");
    assert_eq!(second.value, 22);
    drop(second);
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}

pub fn accepted_serial_handle_unwind_releases_group() {
    let (pool, c, drops) = setup(1, Response::Accept, true);
    pool.push_serial(task(11, &drops), group(10));
    pool.push_serial(task(22, &drops), group(10));
    assert!(pool.pop(0).is_none());
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let accepted = c.held.lock().unwrap().pop().expect("accepted handle");
        assert_eq!(accepted.value, 11);
        let _held_until_unwind = accepted;
        panic!("task panics during execution");
    }));
    assert!(outcome.is_err());
    assert_eq!(count(&drops), 1);
    let successor = pool.pop(0).expect("successor available after unwind");
    assert_eq!(successor.value, 22);
    drop(successor);
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}

pub fn executor_panic_unwinds_serial_handle_and_releases_group() {
    let (pool, c, drops) = setup(1, Response::Panic, true);
    let outcome = catch_unwind(AssertUnwindSafe(|| pool.push_serial(task(11, &drops), group(10))));
    assert!(outcome.is_err());
    assert_eq!(count(&drops), 1);
    c.idle.store(false, Ordering::SeqCst);
    pool.push_serial(task(22, &drops), group(10));
    let handle = pool.pop(0).expect("panic must release serial group");
    assert_eq!(handle.value, 22);
    drop(handle);
    assert!(pool.pop(0).is_none());
    assert_eq!(count(&drops), 2);
}
