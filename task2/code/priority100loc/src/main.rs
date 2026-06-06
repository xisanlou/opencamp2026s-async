use std::future::Future;

use std::pin::Pin;

use std::sync::atomic::Ordering::{Relaxed, Release};
use std::task::{Poll, Context};
use std::task::{RawWaker, RawWakerVTable, Waker};
use std::collections::{HashMap};
use std::thread::sleep;
//use std::thread::sleep;
use std::time::{Duration, Instant};
use core::sync::atomic::AtomicUsize;

enum State {
    Halted,
    Running,
}

struct Fib {
    state: State,
}

impl Fib {
    fn waiter<'a>(&'a mut self) -> Waiter<'a> {
        Waiter { fib: self }
    }
}

struct Waiter<'a> {
    fib: &'a mut Fib,
}

impl<'a> Future for Waiter<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        match self.fib.state {
            State::Halted => {
                self.fib.state = State::Running;
                Poll::Ready(())
            }
            State::Running => {
                self.fib.state = State::Halted;
                Poll::Pending
            }
        }
    }
}

#[derive(Clone)]
struct Cfstask(usize);
impl Cfstask {
    pub fn new(val: usize) -> Self {
        Self(val)
    }

    pub fn get(&self) -> usize {
        self.0
    }
}

struct Executor {
    id_pool: usize,
    current: usize,
    tasks: HashMap<usize, Pin<Box<dyn Future<Output=()>>>>,
    ready_queue: CFScheduler<Cfstask>,
}

impl Executor {
    fn new() -> Self {
        Executor {
            //fibs: VecDeque::new(),
            id_pool: 0,
            current: 0,
            tasks: HashMap::new(),
            ready_queue: CFScheduler::new(),
        }
    }

    fn init(&mut self) {
        self.id_pool += 1;
        self.tasks.insert(0, Box::pin(async {}));
    }

    fn push<C, F>(&mut self, closure: C, prio: isize)
    where
        F: Future<Output=()> + 'static,
        C: FnOnce(Fib) -> F,
    {
        let id = self.id_pool;
        self.id_pool += 1;
        let cfs_task = Cfstask::new(id);

        
        let fib = Fib { state: State::Running };
        self.tasks.insert(id, Box::pin(closure(fib)));
        let arc_cfs_task = Arc::new(CFSTask::new(cfs_task));
        
        self.ready_queue.set_priority(&arc_cfs_task, prio);
        println!("set priority to id= {} | weight={}", id, &arc_cfs_task.get_weight());
        self.ready_queue.add_task(arc_cfs_task);
    }

    fn run(&mut self) {
        let waker = create();
        let mut context = Context::from_waker(&waker);
        let exe_start_time = Instant::now();
        let mut exec_run_time = 0;

        //while let Some(mut fib) = self.fibs.pop_front() {
        loop {
            //sleep(Duration::from_millis(20));
            let run_secs = exe_start_time.elapsed().as_secs();
            //println!("Executor running {} seconds", run_secs);
            if run_secs > exec_run_time {
                println!("Executor running {} seconds", run_secs);
                exec_run_time = run_secs;
                if exec_run_time > 30 {
                    break;
                }
            }

            let task_start_time = Instant::now();

            match self.ready_queue.pick_next_task() {
                Some(arc_task) => {
                     
                    let current_task = arc_task.inner().get();
                    self.current = current_task;
                    //println!("current task id: {}", self.current);
                    let fut = self.tasks.get_mut(&current_task).unwrap().as_mut();
                    //println!("current task id: {}", self.current);
                    match fut.poll(&mut context) {
                    //match fib.as_mut().poll(&mut context) {
                        Poll::Pending => {
                            //println!("task id={} Pending", self.current);
                            let task_run_time = task_start_time.elapsed().as_nanos() as isize / 1000;
                            //println!("current: {} | vruntime={} | run_time={} | weight={}", current_task, &arc_task.get_vruntime(), task_run_time, &arc_task.get_weight());
                            self.ready_queue.task_tick(&arc_task, task_run_time);
                            self.ready_queue.put_prev_task(arc_task, false);
                            //self.fibs.push_back(fib);
                        },
                        Poll::Ready(()) => {
                            //println!("task id={} Ready", self.current);
                            let task_run_time = task_start_time.elapsed().as_nanos() as isize / 1000;
                            self.ready_queue.task_tick(&arc_task, task_run_time);
                        },
                    }
                },
                None => break,
            };
        }
    }
}



pub fn create() -> Waker {
    // Safety: The waker points to a vtable with functions that do nothing. Doing
    // nothing is memory-safe.
    unsafe { Waker::from_raw(RAW_WAKER) }
}

const RAW_WAKER: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(_: *const ()) -> RawWaker { RAW_WAKER }
unsafe fn wake(_: *const ()) { }
unsafe fn wake_by_ref(_: *const ()) { }
unsafe fn drop(_: *const ()) { }

fn main() {
    let mut exec = Executor::new();
    exec.init();

    let arc_task1 = Arc::new(AtomicUsize::new(0));
    let arc1_clone = arc_task1.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc1_clone.fetch_add(1, Release);
            sleep(Duration::from_millis(50));
            fib.waiter().await;
        }
    }, 0);   

    let arc_task2 = Arc::new(AtomicUsize::new(0));
    let arc2_clone = arc_task2.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc2_clone.fetch_add(1, Release);
            //sleep(Duration::from_millis(20));
            fib.waiter().await;
        }
    }, 0);

    let arc_task3 = Arc::new(AtomicUsize::new(0));
    let arc3_clone = arc_task3.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc3_clone.fetch_add(1, Release);
            fib.waiter().await;
        }
    }, 0);

    let arc_task4 = Arc::new(AtomicUsize::new(0));
    let arc4_clone = arc_task4.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc4_clone.fetch_add(1, Release);
            fib.waiter().await;
        }
    }, 0);

    let arc_task5 = Arc::new(AtomicUsize::new(0));
    let arc5_clone = arc_task5.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc5_clone.fetch_add(1, Release);
            fib.waiter().await;
        }
    }, 0);

    let arc_task6 = Arc::new(AtomicUsize::new(0));
    let arc6_clone = arc_task6.clone();
    exec.push(move |mut fib| async move {
        loop {
            arc6_clone.fetch_add(1, Release);
            fib.waiter().await;
        }
    }, 0);

    println!("Running");
    exec.run();

    println!("任务 1 运行次数： {}", &arc_task1.load(Relaxed));
    println!("任务 2 运行次数： {}", &arc_task2.load(Relaxed));
    println!("任务 3 运行次数： {}", &arc_task3.load(Relaxed));
    println!("任务 4 运行次数： {}", &arc_task4.load(Relaxed));
    println!("任务 5 运行次数： {}", &arc_task5.load(Relaxed));
    println!("任务 6 运行次数： {}", &arc_task6.load(Relaxed));

    println!("Done");
}


// ============================= CFS ==================================
// This file from arceos scheduler
// https://github.com/arceos-org/scheduler.git

use std::{collections::BTreeMap, sync::Arc};
use core::ops::Deref;
use core::sync::atomic::{AtomicIsize, Ordering};

//use crate::BaseScheduler;

/// task for CFS
pub struct CFSTask<T> {
    inner: T,
    init_vruntime: AtomicIsize,
    delta: AtomicIsize,
    nice: AtomicIsize,
    id: AtomicIsize,
}

// https://elixir.bootlin.com/linux/latest/source/include/linux/sched/prio.h

const NICE_RANGE_POS: usize = 19; // MAX_NICE in Linux
const NICE_RANGE_NEG: usize = 20; // -MIN_NICE in Linux, the range of nice is [MIN_NICE, MAX_NICE]

// https://elixir.bootlin.com/linux/latest/source/kernel/sched/core.c

const NICE2WEIGHT_POS: [isize; NICE_RANGE_POS + 1] = [
    1024, 820, 655, 526, 423, 335, 272, 215, 172, 137, 110, 87, 70, 56, 45, 36, 29, 23, 18, 15,
];
const NICE2WEIGHT_NEG: [isize; NICE_RANGE_NEG + 1] = [
    1024, 1277, 1586, 1991, 2501, 3121, 3906, 4904, 6100, 7620, 9548, 11916, 14949, 18705, 23254,
    29154, 36291, 46273, 56483, 71755, 88761,
];

impl<T> CFSTask<T> {
    /// new with default values
    pub const fn new(inner: T) -> Self {
        Self {
            inner,
            init_vruntime: AtomicIsize::new(0_isize),
            delta: AtomicIsize::new(0_isize),
            nice: AtomicIsize::new(0_isize),
            id: AtomicIsize::new(0_isize),
        }
    }

    fn get_weight(&self) -> isize {
        let nice = self.nice.load(Ordering::Acquire);
        if nice >= 0 {
            NICE2WEIGHT_POS[nice as usize]
        } else {
            NICE2WEIGHT_NEG[(-nice) as usize]
        }
    }

    fn get_id(&self) -> isize {
        self.id.load(Ordering::Acquire)
    }

    fn get_vruntime(&self) -> isize {
        if self.nice.load(Ordering::Acquire) == 0 {
            self.init_vruntime.load(Ordering::Acquire) + self.delta.load(Ordering::Acquire)
        } else {
            self.init_vruntime.load(Ordering::Acquire)
                + self.delta.load(Ordering::Acquire) * 1024 / self.get_weight()
        }
    }

    fn set_vruntime(&self, v: isize) {
        self.init_vruntime.store(v, Ordering::Release);
    }

    // Simple Implementation: no change in vruntime.
    // Only modifying priority of current process is supported currently.
    fn set_priority(&self, nice: isize) {
        let current_init_vruntime = self.get_vruntime();
        self.init_vruntime
            .store(current_init_vruntime, Ordering::Release);
        self.delta.store(0, Ordering::Release);
        self.nice.store(nice, Ordering::Release);
    }

    fn set_id(&self, id: isize) {
        self.id.store(id, Ordering::Release);
    }

    // 修改，将每次自增1变为传入一个数字
    fn task_tick(&self, val: isize) {
        self.delta.fetch_add(val, Ordering::Release);
    }

    /// Returns a reference to the inner task struct.
    pub const fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T> Deref for CFSTask<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A simple [Completely Fair Scheduler][1] (CFS).
///
/// [1]: https://en.wikipedia.org/wiki/Completely_Fair_Scheduler
pub struct CFScheduler<T> {
    ready_queue: BTreeMap<(isize, isize), Arc<CFSTask<T>>>, // (vruntime, taskid)
    min_vruntime: Option<AtomicIsize>,
    id_pool: AtomicIsize,
}

impl<T> CFScheduler<T> {
    /// Creates a new empty [`CFScheduler`].
    pub const fn new() -> Self {
        Self {
            ready_queue: BTreeMap::new(),
            min_vruntime: None,
            id_pool: AtomicIsize::new(0_isize),
        }
    }
    /// get the name of scheduler
    pub fn scheduler_name() -> &'static str {
        "Completely Fair"
    }
}

impl<T> BaseScheduler for CFScheduler<T> {
    type SchedItem = Arc<CFSTask<T>>;

    fn init(&mut self) {}

    fn add_task(&mut self, task: Self::SchedItem) {
        if self.min_vruntime.is_none() {
            self.min_vruntime = Some(AtomicIsize::new(0_isize));
        }
        let vruntime = self.min_vruntime.as_mut().unwrap().load(Ordering::Acquire);
        let taskid = self.id_pool.fetch_add(1, Ordering::Release);
        task.set_vruntime(vruntime);
        task.set_id(taskid);
        self.ready_queue.insert((vruntime, taskid), task);
        if let Some(((min_vruntime, _), _)) = self.ready_queue.first_key_value() {
            self.min_vruntime = Some(AtomicIsize::new(*min_vruntime));
        } else {
            self.min_vruntime = None;
        }
    }

    fn remove_task(&mut self, task: &Self::SchedItem) -> Option<Self::SchedItem> {
        if let Some((_, tmp)) = self
            .ready_queue
            .remove_entry(&(task.clone().get_vruntime(), task.clone().get_id()))
        {
            if let Some(((min_vruntime, _), _)) = self.ready_queue.first_key_value() {
                self.min_vruntime = Some(AtomicIsize::new(*min_vruntime));
            } else {
                self.min_vruntime = None;
            }
            Some(tmp)
        } else {
            None
        }
    }

    fn pick_next_task(&mut self) -> Option<Self::SchedItem> {
        if let Some((_, v)) = self.ready_queue.pop_first() {
            Some(v)
        } else {
            None
        }
    }

    fn put_prev_task(&mut self, prev: Self::SchedItem, _preempt: bool) {
        let taskid = self.id_pool.fetch_add(1, Ordering::Release);
        prev.set_id(taskid);
        self.ready_queue
            .insert((prev.clone().get_vruntime(), taskid), prev);
    }
    
    // 修改，将每次自增1修改为增加数字
    fn task_tick(&mut self, current: &Self::SchedItem, val: isize) -> bool {
        current.task_tick(val);
        if self.ready_queue.is_empty() {
            return false;
        }
        self.min_vruntime.is_none()
            || current.get_vruntime() > self.min_vruntime.as_mut().unwrap().load(Ordering::Acquire)
    }

    fn set_priority(&mut self, task: &Self::SchedItem, prio: isize) -> bool {
        if (-20..=19).contains(&prio) {
            task.set_priority(prio);
            true
        } else {
            false
        }
    }
}

/// The base scheduler trait that all schedulers should implement.
///
/// All tasks in the scheduler are considered runnable. If a task is go to
/// sleep, it should be removed from the scheduler.
pub trait BaseScheduler {
    /// Type of scheduled entities. Often a task struct.
    type SchedItem;

    /// Initializes the scheduler.
    fn init(&mut self);

    /// Adds a task to the scheduler.
    fn add_task(&mut self, task: Self::SchedItem);

    /// Removes a task by its reference from the scheduler. Returns the owned
    /// removed task with ownership if it exists.
    ///
    /// # Safety
    ///
    /// The caller should ensure that the task is in the scheduler, otherwise
    /// the behavior is undefined.
    fn remove_task(&mut self, task: &Self::SchedItem) -> Option<Self::SchedItem>;

    /// Picks the next task to run, it will be removed from the scheduler.
    /// Returns [`None`] if there is not runnable task.
    fn pick_next_task(&mut self) -> Option<Self::SchedItem>;

    /// Puts the previous task back to the scheduler. The previous task is
    /// usually placed at the end of the ready queue, making it less likely
    /// to be re-scheduled.
    ///
    /// `preempt` indicates whether the previous task is preempted by the next
    /// task. In this case, the previous task may be placed at the front of the
    /// ready queue.
    fn put_prev_task(&mut self, prev: Self::SchedItem, preempt: bool);

    /// Advances the scheduler state at each timer tick. Returns `true` if
    /// re-scheduling is required.
    ///
    /// `current` is the current running task.
    fn task_tick(&mut self, current: &Self::SchedItem, val: isize) -> bool;

    /// set priority for a task
    fn set_priority(&mut self, task: &Self::SchedItem, prio: isize) -> bool;
}


