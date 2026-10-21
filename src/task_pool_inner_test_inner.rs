//! 票据3私有恢复场景的实现面；复用生产恢复循环与测试专用检查点。
use super::super::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

type Hook = Box<dyn FnMut(&'static str)>;
thread_local! { static HOOK: RefCell<Option<Hook>> = RefCell::new(None); }

pub(crate) fn checkpoint(point: &'static str) {
    let hook = HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook(point);
        HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
    }
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) { HOOK.with(|slot| *slot.borrow_mut() = None); }
}
fn install(hook: Hook) -> Reset {
    HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
    Reset
}
struct Queued;
impl TaskExecutor<usize> for Queued {
    fn fast_path_threshold(&self) -> usize { 0 }
    fn is_idle(&self) -> bool { false }
    fn try_execute(&self, task: TaskHandle<usize, Self>) -> Result<(), TaskHandle<usize, Self>> { Err(task) }
}
fn parallel_hint(pool: &TaskPool<usize, Queued>) -> u64 {
    (pool.parallel_states.load(Ordering::Acquire) >> 52) & 4095
}

pub(super) fn saturated_same_value_publication_crosses_stale_nonzero_lowering() {
    let pool = TaskPool::new(Queued);
    for id in 0..4095 { pool.push_parallel(id, TaskWeight::Highest); }
    let hits = Rc::new(Cell::new(0));
    let recorded = Rc::clone(&hits);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_before_lower_cas" && recorded.get() == 0 {
            recorded.set(1);
            assert_eq!(pool.parallel[4].len(), 4094);
            assert_eq!(parallel_hint(pool), 4095);
            pool.push_parallel(4095, TaskWeight::Highest);
        } else if point == "parallel_after_lower_cas" {
            assert_eq!(parallel_hint(pool), 4094, "stale nonzero CAS must actually succeed");
            recorded.set(2);
        }
    }));
    assert_eq!(*pool.pop(0).unwrap(), 0);
    assert_eq!(hits.get(), 2);
    assert_eq!(parallel_hint(pool), 4095, "post-lowering repair must restore publication");
    drop(_reset);
    for id in 1..4096 { assert_eq!(*pool.pop(0).unwrap(), id); }
    assert!(pool.pop(0).is_none());
}

pub(super) fn publication_after_lowering_is_preserved_by_replenishment() {
    let pool = TaskPool::new(Queued);
    for id in 0..4095 { pool.push_parallel(id, TaskWeight::Highest); }
    let hit = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_after_lower_cas" && !recorded.replace(true) {
            assert_eq!(parallel_hint(pool), 4094);
            pool.push_parallel(4095, TaskWeight::Highest);
        }
    }));
    assert_eq!(*pool.pop(0).unwrap(), 0);
    assert!(hit.get());
    assert_eq!(parallel_hint(pool), 4095);
    drop(_reset);
    for id in 1..4096 { assert_eq!(*pool.pop(0).unwrap(), id); }
    assert!(pool.pop(0).is_none());
}

pub(super) fn serial_cross_band_stale_lowering_replenishes_and_preserves_used() {
    let pool = TaskPool::new(Queued);
    let index = crate::SerialQueueIndex::new(0).unwrap();
    for id in 0..17 { pool.push_serial(id, index); }
    let hits = Rc::new(Cell::new(0));
    let recorded = Rc::clone(&hits);
    let _reset = install(Box::new(move |point| {
        if point == "serial_before_lower_cas" && recorded.get() == 0 {
            recorded.set(1);
            assert_eq!(pool.serial[0].len(), 16);
            pool.push_serial(17, index);
        } else if point == "serial_after_lower_cas" {
            assert_eq!(pool.serial_states.load(Ordering::Acquire) & 7, 3);
            recorded.set(2);
        }
    }));
    let first = pool.pop(0).unwrap();
    assert_eq!(hits.get(), 2);
    assert_eq!(pool.serial_states.load(Ordering::Acquire) & 7, 5);
    assert!(pool.pop(0).is_none());
    drop(_reset);
    drop(first);
    for id in 1..18 { assert_eq!(*pool.pop(0).unwrap(), id); }
    assert!(pool.pop(0).is_none());
}

pub(super) fn empty_pop_repair_clears_residual_hint_and_keeps_other_fields() {
    let pool = TaskPool::new(Queued);
    // 合法的暂时多报状态：最高档提示为2而队列为空。
    pool.parallel_states.store(2 << 52, Ordering::Release);
    pool.push_parallel(7, TaskWeight::Lowest);
    let hit = Rc::new(Cell::new(0));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_empty_pop" {
            recorded.set(1);
            assert_eq!(parallel_hint(pool), 1);
            assert_eq!(pool.parallel_states.load(Ordering::Acquire) & 8191, 1);
        } else if point == "parallel_after_lower_cas" && recorded.get() == 1 {
            recorded.set(2);
            assert_eq!(parallel_hint(pool), 0);
            assert_eq!(pool.parallel_states.load(Ordering::Acquire) & 8191, 1);
        }
    }));
    assert_eq!(*pool.pop(1).unwrap(), 7);
    assert_eq!(hit.get(), 2);
    assert_eq!(parallel_hint(pool), 0);
    assert_eq!(pool.parallel_states.load(Ordering::Acquire) & 8191, 0);
    assert!(pool.pop(0).is_none());
}

pub(super) fn paused_repairer_republishes_hidden_task_on_resume() {
    let pool = TaskPool::new(Queued);
    for id in 0..4095 { pool.push_parallel(id, TaskWeight::Highest); }
    let hit = Rc::new(Cell::new(0));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_before_lower_cas" && recorded.get() == 0 {
            recorded.set(1);
            assert_eq!(parallel_hint(pool), 4095);
            assert_eq!(pool.parallel[4].len(), 4094);
            // 饱和同值发布先穿过恢复者的旧快照。
            pool.push_parallel(4095, TaskWeight::Highest);
        } else if point == "parallel_after_lower_cas" && recorded.get() == 1 {
            recorded.set(2);
            assert_eq!(parallel_hint(pool), 4094);
            assert_eq!(pool.parallel[4].len(), 4095);
            // 测试回调精确暂停恢复者；其他消费者耗尽已有提示，不是线程抢占证明。
            for id in 1..4095 { assert_eq!(*pool.pop(0).unwrap(), id); }
            assert_eq!(parallel_hint(pool), 0);
            assert_eq!(pool.parallel[4].len(), 1);
            assert!(pool.pop(0).is_none(), "unhelped paused repair may temporarily hide the task");
        }
    }));
    assert_eq!(*pool.pop(0).unwrap(), 0);
    assert_eq!(hit.get(), 2, "repair must reach both lowering checkpoints");
    assert_eq!(parallel_hint(pool), 1, "resumed repairer must republish the hidden task");
    drop(_reset);
    assert_eq!(*pool.pop(0).unwrap(), 4095);
    assert!(pool.pop(0).is_none());
}

pub(super) fn competing_consumer_retry_reuses_nonzero_random() {
    let pool = TaskPool::new(Queued);
    pool.push_parallel(7, TaskWeight::Low);
    pool.push_parallel(8, TaskWeight::Low);
    pool.push_parallel(9, TaskWeight::Middle);
    // 合法暂时多报：最低档没有任务，但其提示尚未被取空修复。
    pool.parallel_states.fetch_add(1, Ordering::AcqRel);
    let hit = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_empty_pop" && !recorded.replace(true) {
            // 首选最低档取空后提示已减零；另一消费者取走低档7。
            assert_eq!(pool.parallel_states.load(Ordering::Acquire) & 8191, 0);
            assert_eq!(*pool.pop(0).expect("competing consumer takes low queue"), 7);
        }
    }));
    // 初始权重1+2*10+100=121，121取模首先选取空的最低档。
    // 竞争消费后剩余10+100=110，原输入121取模为11，须选中中档。
    let chosen = pool.pop(121).expect("retry must select a remaining queue");
    assert!(hit.get(), "must hit the empty-pop competition window");
    assert_eq!(*chosen, 9, "retry must reuse the caller's nonzero random");
    drop(chosen);
    drop(_reset);
    assert_eq!(*pool.pop(0).expect("low task remains"), 8);
    assert!(pool.pop(121).is_none());
}

struct RejectFast;
impl TaskExecutor<usize> for RejectFast {
    fn fast_path_threshold(&self) -> usize { 1 }
    fn is_idle(&self) -> bool { true }
    fn try_execute(&self, task: TaskHandle<usize, Self>) -> Result<(), TaskHandle<usize, Self>> {
        Err(task)
    }
}

struct TrackedRejectedTask {
    value: usize,
    drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for TrackedRejectedTask {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); }
}
struct RejectTracked;
impl TaskExecutor<TrackedRejectedTask> for RejectTracked {
    fn fast_path_threshold(&self) -> usize { 1 }
    fn is_idle(&self) -> bool { true }
    fn try_execute(&self, task: TaskHandle<TrackedRejectedTask, Self>)
        -> Result<(), TaskHandle<TrackedRejectedTask, Self>> { Err(task) }
}

pub(super) fn rejected_serial_unwind_before_publication_releases_once() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let pool = TaskPool::new(RejectTracked);
    let index = crate::SerialQueueIndex::new(0).unwrap();
    let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    assert!(try_claim_serial_inner(pool, 0, true), "empty serial group must be claimed");
    let original = TaskHandle {
        task: TrackedRejectedTask { value: 11, drops: std::sync::Arc::clone(&drops) },
        pool_ptr: pool,
        serial_index: Some(0),
    };
    let rejected = pool.executor.try_execute(original).err().expect("executor rejects original handle");
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let (task, guard) = into_task_inner(rejected);
        assert_eq!(task.value, 11);
        assert!(guard.is_some(), "guard takes over the only release obligation");
        assert_eq!(pool.serial_states.load(Ordering::Acquire) & 1, 1,
            "group stays occupied before fallback publication");
        assert_eq!(drops.load(Ordering::SeqCst), 0, "original handle must not destroy transferred task");
        panic!("unwind after ownership transfer but before queue publication");
    }));
    assert!(outcome.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1, "transferred task must be destroyed exactly once");
    assert_eq!(pool.serial_states.load(Ordering::Acquire) & 1, 0,
        "guard must release group during unwind");
    assert!(pool.serial[0].is_empty(), "unpublished task must not appear in queue");
    assert!(pool.pop(0).is_none());
    pool.push_serial(TrackedRejectedTask { value: 22, drops: std::sync::Arc::clone(&drops) }, index);
    let next = pool.pop(0).expect("group available after guard unwind");
    assert_eq!(next.value, 22);
    drop(next);
    assert!(pool.pop(0).is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

pub(super) fn rejected_serial_holds_group_until_hint_publication() {
    let pool = TaskPool::new(RejectFast);
    let seen = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&seen);
    let _reset = install(Box::new(move |point| {
        if point == "serial_after_queue_push_before_hint" {
            assert!(!recorded.replace(true), "window must be visited exactly once");
            assert_eq!(pool.serial[0].len(), 1, "rejected task already in queue");
            assert_eq!(pool.serial_states.load(Ordering::Acquire) & 7, 1,
                "used must remain set while count hint is not published");
            assert!(pool.pop(0).is_none(), "same group cannot claim before hint publication");
        }
    }));
    pool.push_serial(11, crate::SerialQueueIndex::new(0).unwrap());
    assert!(seen.get(), "reject fallback must reach publication window");
    drop(_reset);
    assert_eq!(*pool.pop(0).expect("published rejected task must be available"), 11);
    assert!(pool.pop(0).is_none());
}
