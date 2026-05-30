use axsched::{CFScheduler, CFSTask, BaseScheduler};
use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake, Waker},
};
use std::{
    collections::HashMap, future::Future, pin::Pin, sync::{Arc, Condvar, Mutex}, task::{Context, Poll}, thread::{self, sleep}, time::Duration
};

// 定义执行器结构
struct Executor {
    scheduler: Arc<Mutex<CFScheduler<Arc<TaskInner>>>>,
    parker: Arc<Parker>,
    // 当任务不在scheduler内时，保持所有权
    tasks: Arc<Mutex<HashMap<usize, Arc<CFSTask<Arc<TaskInner>>>>>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
           scheduler: Arc::new(Mutex::new(CFScheduler::new())),
           parker: Arc::new(Parker::default()),
           tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // 向执行器中添加加任务并设置优先级
    fn spawn(&mut self, future: impl Future<Output = ()> + 'static + Send, prio: isize, id: usize) {
        let future = future.boxed();
        let task_inner = TaskInner {
            future: Mutex::new(Some(future)),
            scheduler: self.scheduler.clone(),
            parker_executor: self.parker.clone(),
            tasks: self.tasks.clone(),
            id: id,
        };
        let arc_task = Arc::new(CFSTask::new(Arc::new(task_inner)));
        
        

        self.tasks.lock().unwrap().insert(id, arc_task.clone());
        self.scheduler.lock().unwrap().set_priority(&arc_task, prio);
        self.scheduler.lock().unwrap().add_task(arc_task.clone());

        // ！！！测试配置！！！ 
        // 为在这个短期测试中，使优先级的设置在开始就能立刻体现出来，人为推动任务的task_tick
        for _ in 0..1024 {
            let _ = self.scheduler.lock().unwrap().task_tick(&arc_task);
        }
    }

    // 运行执行器
    fn run(&mut self) {
        // ！！！测试代码！！！
        // 为了使CFS的优先级生效，先把队列中所有任务取出再放入
        // 这纯粹是为了在这个实验中看到效果
        let mut arc_cfs_tasks = Vec::new();
        while let Some(arc_cfs_task) = self.scheduler.lock().unwrap().pick_next_task() {
            arc_cfs_tasks.push(arc_cfs_task);
        }

        while let Some(arc_cfs_task) = arc_cfs_tasks.pop() {
            self.scheduler.lock().unwrap().put_prev_task(arc_cfs_task, false);
        }

        // 通过循环，从调度器的等待队列不断读取任务,并进行处理，直到所有任务完成
        'outer: loop {
            // 为了能清楚看到执行的优先级顺序，人为制造任务堆积
            sleep(Duration::from_secs(1));

            let mut sche_guard = self.scheduler.lock().unwrap();
            let next_task = sche_guard.pick_next_task();
            drop(sche_guard);
            match next_task {
                None => {
                    //println!("执行器将被阻塞！！！");
                    self.parker.park();
                },
                Some(arc_task) => {
                    let task_id = arc_task.inner().id;
                    //println!("从就绪队列中取出任务：id={}", task_id);
                    let mut future_slot = arc_task.inner().future.lock().unwrap();
                    if let Some(mut future) = future_slot.take() {
                        // 创建localwaker和context
                        let waker = waker_ref(&arc_task.inner());
                        let context = &mut Context::from_waker(&*waker);
                        //println!("开始poll任务：{}", & task_id);
                        if future.as_mut().poll(context).is_pending() {
                            //println!("任务{}的poll返回pending！", &task_id);
                            // 推动CFS的task_tick
                            self.scheduler.lock().unwrap().task_tick(&arc_task);
                            //println!("已完成任务{}的task_tick推动！", &task_id);
                            
                            // 将未执行完的Future放回任务
                            *future_slot = Some(future);
                        } else {
                            // Future已执行完毕，将其从执行器的任务保持列表中删除
                            //let task_id = arc_task.inner().id;
                            //println!("任务{}已经执行完毕, 将从列表中删除！", &task_id);
                            self.tasks.lock().unwrap().remove(&task_id);
                            
                            // 如果保持列表都为空，则说明所有任务执行完毕，跳出循环
                            if self.tasks.lock().unwrap().is_empty() {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
    }
}

// 定义执行器实例
//static mut EXECUTOR: Executor = Executor::new();


struct TaskInner {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    scheduler: Arc<Mutex<CFScheduler<Arc<TaskInner>>>>,
    tasks: Arc<Mutex<HashMap<usize, Arc<CFSTask<Arc<TaskInner>>>>>>,
    parker_executor: Arc<Parker>,
    id: usize,
}

impl ArcWake for TaskInner {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if let Some(cfs_task) = arc_self.tasks.lock().unwrap().get(&arc_self.id) {
        //println!("开始唤醒：将任务 ‘{}’ 放入等待队列！", &arc_self.id);
        arc_self.scheduler.lock().unwrap().put_prev_task(cfs_task.clone(), false);
        arc_self.parker_executor.unpark();
        }
    }
}

// ============================= 计时器 ====================================
pub struct TimerFuture {
    shared_state: Arc<Mutex<SharedState>>,
}

/// 在Future和等待的线程间共享状态
struct SharedState {
    /// 定时(睡眠)是否结束
    completed: bool,

    /// 当睡眠结束后，线程可以用`waker`通知`TimerFuture`来唤醒任务
    waker: Option<Waker>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 通过检查共享状态，来确定定时器是否已经完成
        let mut shared_state = self.shared_state.lock().unwrap();
        if shared_state.completed {
            Poll::Ready(())
        } else {
            // 设置`waker`，这样新线程在睡眠(计时)结束后可以唤醒当前的任务，接着再次对`Future`进行`poll`操作,
            //
            // 下面的`clone`每次被`poll`时都会发生一次，实际上，应该是只`clone`一次更加合理。
            // 选择每次都`clone`的原因是： `TimerFuture`可以在执行器的不同任务间移动，如果只克隆一次，
            // 那么获取到的`waker`可能已经被篡改并指向了其它任务，最终导致执行器运行了错误的任务
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl TimerFuture {
    /// 创建一个新的`TimerFuture`，在指定的时间结束后，该`Future`可以完成
    pub fn new(duration: Duration) -> Self {
        let shared_state = Arc::new(Mutex::new(SharedState {
            completed: false,
            waker: None,
        }));

        // 创建新线程
        let thread_shared_state = shared_state.clone();
        thread::spawn(move || {
            // 睡眠指定时间实现计时功能
            //println!("反射器进程开始睡眠！");
            thread::sleep(duration);
            let mut shared_state = thread_shared_state.lock().unwrap();
            // 通知执行器定时器已经完成，可以继续`poll`对应的`Future`了
            shared_state.completed = true;
            if let Some(waker) = shared_state.waker.take() {
                //println!("唤醒！！");
                waker.wake()
            }
        });

        TimerFuture { shared_state }
    }
}


// ============================= 执行器阻塞器 ====================================
#[derive(Default)]
struct Parker(Mutex<bool>, Condvar);

impl Parker {
    fn park(&self) {
        let mut resumable = self.0.lock().unwrap();
            while !*resumable {
                resumable = self.1.wait(resumable).unwrap();
            }
        *resumable = false;
    }

    fn unpark(&self) {
        *self.0.lock().unwrap() = true;
        self.1.notify_one();
    }
}

fn main() {
    // 初始化一个执行器
    let mut executor = Executor::new();

    // 建立两个列表，记录future执行顺序
    let start_list: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let end_list: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let prio_list = vec![18, 17, -11, -10, 0, 1];

    // 孵化多个任务
    for id in 0..6 {
        let arc_s = start_list.clone();
        let arc_e = end_list.clone();
        let prio = prio_list[id];
        executor.spawn( async move {
            arc_s.lock().unwrap().push(id);
            println!("开始任务:{}, 优先级:{} ", id, prio);
            // 创建定时器任务
            TimerFuture::new(Duration::new(8, 0)).await;
            println!("结束任务:{}, 优先级:{} ", id, prio);
            arc_e.lock().unwrap().push(id);
        }, prio, id);
    }
    
    // 运行执行器
    executor.run();

    // 打印执行顺序
    println!("###############################################################");
    println!("任务优先级(序号为任务ID,值为优先级)：{:?}", prio_list);
    println!("任务开始的顺序（值为任务ID）：{:?}", start_list.lock().unwrap());
    println!("任务结束的顺序（值为任务ID）：{:?}", end_list.lock().unwrap());

}