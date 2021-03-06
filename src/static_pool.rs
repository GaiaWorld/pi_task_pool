//! 不可删除的任务池
//! 单例任务使用快速的权重树（不可删除）作为数据结构；队列任务的每队列，使用双端队列作为数据结构

use std::collections::VecDeque;
use std::marker::Send;
use std::fmt;

use pi_wtree::wtree::WeightTree;
use pi_wtree::fast_wtree;

use pi_dyn_uint::{SlabFactory, UintFactory, ClassFactory};

use crate::enums:: {IndexType, FreeSign};

pub type AsyncPool<T> = fast_wtree::WeightTree<T>;

pub struct SyncPool<T: 'static>{
    weight_queues: WeightTree<()>,
    slab: SlabFactory<IndexType, WeightQueue<T>>,
    len: usize,
}

unsafe impl<T: Send> Send for SyncPool<T> {}
unsafe impl<T: Send> Sync for SyncPool<T> {}

impl<T: 'static> SyncPool<T>{
    #[inline]
    pub fn new() -> Self {
        SyncPool {
            weight_queues: WeightTree::new(),
            slab: SlabFactory::new(),
            len: 0,
        }
    }

    #[inline]
    pub fn queue_len(&self) -> usize {
        self.weight_queues.len()
    }

    //create queues, if id is exist, return false
    #[inline]
    pub fn create_queue(&mut self, weight: usize) -> usize {
        self.slab.create(0, IndexType::HalfLockQueue, WeightQueue::new(weight))
    }

    #[inline]
    pub fn try_remove_queue(&mut self, id: usize) -> bool {
        match self.slab.contains(id){
            true => {
                match self.slab.remove(id) {
                    (index, IndexType::Queue, _) => unsafe {self.weight_queues.delete(index, &mut self.slab);},
                    _ => ()
                }
                true
            },
            false => false
        }
    }

    #[inline]
    pub fn is_locked(&self, id: usize) -> bool {
        match self.slab.try_load(id) {
            Some(_i) => {
                let class = self.slab.get_class(id).clone();
                match class {
                    IndexType::HalfLockQueue => true,
                    IndexType::LockQueue => true,
                    _ => false,
                }
            },
            None => false,
        }
    }

    #[inline]
    pub fn lock_queue(&mut self, id: usize)-> bool {
        match self.slab.try_load(id) {
            Some(i) => {
                let class = self.slab.get_class(id).clone();
                match class {
                    IndexType::Queue => {
                        unsafe {self.weight_queues.delete(i, &mut self.slab)};
                        let mut e = unsafe{self.slab.get_unchecked_mut(id)};
                        e.1 = IndexType::LockQueue;
                        self.len -= e.2.len();
                        true
                    },
                    IndexType::HalfLockQueue => {
                        let mut e = unsafe { self.slab.get_unchecked_mut(id) };
                        e.1 = IndexType::LockQueue;
                        self.len -= e.2.len();
                        true
                    },
                    _ => true
                }
            },
            None => return false,
        }
    }

    #[inline]
    pub fn free_queue(&mut self, id: usize) -> FreeSign{
        let weight = match self.slab.get_mut(id) {
            Some(ref mut r) => {
                match r.1.clone() {
                    IndexType::LockQueue => {
                        if r.2.len() == 0 {
                            r.1 = IndexType::HalfLockQueue;
                            return FreeSign::Success;
                        }
                        self.len += r.2.len();
                        r.2.get_weight()
                    }
                    _ => return FreeSign::Ignore,
                }
                
            },
            _ => return FreeSign::Error,
        };
        self.weight_queues.push((), weight, id, &mut self.slab);
        self.slab.set_class(id, IndexType::Queue);
        FreeSign::Success
    }

    //Find a queue with weight, Removes the first element from the queue and returns it, Painc if weight >= get_weight().
    #[inline]
    pub fn pop_front(&mut self, weight: usize) -> Option<T> {
        let (r, weight, index) = {
            let i = unsafe {self.weight_queues.get_unchecked_mut_by_weight(weight).1};
            let r = unsafe { self.slab.get_unchecked_mut(i) };
            (r.2.pop_front(), r.2.get_weight(), r.0)
        };
        unsafe{ self.weight_queues.update_weight(weight, index, &mut self.slab)};
        self.len -= 1;
        r
    }

    //pop elements from specified queue, and not update weight, Painc if weight >= get_weight()
    #[inline]
    pub fn pop_front_with_lock(&mut self, weight: usize) -> (Option<T>, usize){
        let (r, index) = {
            let i = unsafe{ self.weight_queues.pop(weight, &mut self.slab).2 };
            let r = unsafe { self.slab.get_unchecked_mut(i) };
            (r.2.pop_front(), i)
        };
        self.slab.set_class(index, IndexType::LockQueue);
        self.len -= 1;
        (r, index)
    }

    //Append an element to the queue of the specified ID. return index, or None if the queue is FastQueue
    #[inline]
    pub fn push_back(&mut self, task: T, queue_id: usize) {
        match self.slab.get_class(queue_id) {
            IndexType::LockQueue => {
                unsafe {self.slab.get_unchecked_mut(queue_id) }.2.push_back(task);
                self.len += 1;
            },
            IndexType::HalfLockQueue => {
                let weight  = {
                    let q = unsafe { self.slab.get_unchecked_mut(queue_id) };
                    q.2.push_back(task);
                    q.2.get_weight()
                };
                self.weight_queues.push((), weight, queue_id, &mut self.slab);
                self.slab.set_class(queue_id, IndexType::Queue);
                self.len += 1;
            },
            IndexType::Queue => {
                let (weight, q_i) = {
                    let q = unsafe{self.slab.get_unchecked_mut(queue_id)};
                    q.2.push_back(task);
                    (q.2.get_weight(), q.0)
                };
                unsafe{self.weight_queues.update_weight(weight, q_i, &mut self.slab)};
                self.len += 1;
            },
            _ => {
                unreachable!();
            }
        }
    }

    //Append an element to the queue of the specified ID. return index, or None if the queue is FastQueue
    #[inline]
    pub fn push_front(&mut self, task: T, queue_id: usize) {
        match self.slab.get_class(queue_id) {
            IndexType::LockQueue => {
                unsafe {self.slab.get_unchecked_mut(queue_id).2.push_front(task)};
                self.len += 1;
            },
            IndexType::HalfLockQueue => {
                let weight  = {
                    let q = unsafe { self.slab.get_unchecked_mut(queue_id) };
                    q.2.push_front(task);
                    q.2.get_weight()
                };
                self.weight_queues.push((), weight, queue_id, &mut self.slab);
                self.len += 1;
            },
            IndexType::Queue => {
                let (weight, q_i) = {
                    let q = unsafe { self.slab.get_unchecked_mut(queue_id) };
                    q.2.push_front(task);
                    (q.2.get_weight(), q.0)
                };
                unsafe{self.weight_queues.update_weight(weight, q_i, &mut self.slab)};
                self.len += 1;
            },
            _ => {
                unreachable!();
            }
        }
    }

    //取队列的权重（所有任务的权重总值)
    #[inline]
    pub fn get_weight(&self) -> usize{
        self.weight_queues.amount()
    }

    //清空同步任务池
    #[inline]
    pub fn clear(&mut self) {
        self.weight_queues.clear();
        self.slab.clear();
        self.len = 0;
    }

    //长度
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T: fmt::Debug> fmt::Debug for SyncPool<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r##"SyncPool (
weight_queues: {:?},
slab: {:?}
len: {},
        )"##, self.weight_queues, self.slab, self.len)
    }
}

//权重队列， WeightQueue(权重, 队列)
pub struct WeightQueue<T>{
    weight_unit: usize, //单个任务权重
    queue: VecDeque<T>, //队列
}

impl<T> WeightQueue<T>{
    pub fn  new(weight_unit: usize) -> Self{
        WeightQueue{
            weight_unit: weight_unit,
            queue: VecDeque::new(),
        }
    }

    #[inline]
    pub fn  pop_front(&mut self) -> Option<T>{
        self.queue.pop_front()
    }

    #[inline]
    pub fn  push_back(&mut self, task: T){
        self.queue.push_back(task);
    }

    #[inline]
    pub fn  push_front(&mut self, task: T){
        self.queue.push_front(task);
    }

    //取队列的权重（所有任务的权重总值）
    #[inline]
    pub fn  get_weight(&self) -> usize {
        self.weight_unit * self.queue.len()
    }

    #[inline]
    pub fn  len(&self) -> usize{
       self.queue.len()
    }
}

impl<T: fmt::Debug> fmt::Debug for WeightQueue<T> {
    fn  fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r##"WeightQueue (
weight_unit: ({:?}),
queue: ({:?})
    )"##, self.weight_unit, self.queue)
    }
}

