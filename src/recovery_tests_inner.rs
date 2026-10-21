//! 私有单元测试接缝：复用生产循环，固定插入另一操作；不建复制算法模型。
use super::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::cell::Cell;

type Hook = Box<dyn FnMut(&'static str)>;
thread_local! { static HOOK: RefCell<Option<Hook>> = RefCell::new(None); }

pub(super) fn checkpoint(point: &'static str) {
    // 暂取出回调，允许回调执行真实发布路径而不重入借用。
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

#[test]
fn saturated_same_value_publication_crosses_stale_nonzero_lowering() {
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

#[test]
fn publication_after_lowering_is_preserved_by_replenishment() {
    let pool = TaskPool::new(Queued);
    for id in 0..4095 { pool.push_parallel(id, TaskWeight::Highest); }
    let hit = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_after_lower_cas" && !recorded.replace(true) {
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

#[test]
fn serial_cross_band_stale_lowering_replenishes_and_preserves_used() {
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

#[test]
fn empty_pop_repair_clears_residual_hint_and_keeps_other_fields() {
    let pool = TaskPool::new(Queued);
    // 构造合法的暂时多报状态，精确验证取空修复分支而非依赖调度概率。
    pool.parallel_states.store(2 << 52, Ordering::Release);
    pool.push_parallel(7, TaskWeight::Lowest);
    let hit = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&hit);
    let _reset = install(Box::new(move |point| {
        if point == "parallel_empty_pop" { recorded.set(true); }
    }));
    assert_eq!(*pool.pop(1).unwrap(), 7);
    assert!(hit.get());
    assert_eq!(parallel_hint(pool), 0);
    assert!(pool.pop(0).is_none());
}
