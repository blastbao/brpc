// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// bthread - An M:N threading library to make applications more concurrent.

// Date: Tue Jul 10 17:40:58 CST 2012

#ifndef BTHREAD_TASK_GROUP_H
#define BTHREAD_TASK_GROUP_H

#include "butil/time.h"                             // cpuwide_time_ns
#include "bthread/task_control.h"
#include "bthread/task_meta.h"                     // bthread_t, TaskMeta
#include "bthread/work_stealing_queue.h"           // WorkStealingQueue
#include "bthread/remote_task_queue.h"             // RemoteTaskQueue
#include "butil/resource_pool.h"                    // ResourceId
#include "bthread/parking_lot.h"

namespace bthread {

// For exiting a bthread.
class ExitException : public std::exception {
public:
    explicit ExitException(void* value) : _value(value) {}
    ~ExitException() throw() {}
    const char* what() const throw() override {
        return "ExitException";
    }
    void* value() const {
        return _value;
    }
private:
    void* _value;
};

// Thread-local group of tasks.
// Notice that most methods involving context switching are static otherwise
// pointer `this' may change after wakeup. The **pg parameters in following
// function are updated before returning.
class TaskGroup {
public:
    // Create task `fn(arg)' with attributes `attr' in TaskGroup *pg and put
    // the identifier into `tid'. Switch to the new task and schedule old task
    // to run.
    // Return 0 on success, errno otherwise.
    static int start_foreground(TaskGroup** pg,
                                bthread_t* __restrict tid,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg);

    // Create task `fn(arg)' with attributes `attr' in this TaskGroup, put the
    // identifier into `tid'. Schedule the new thread to run.
    //   Called from worker: start_background<false>
    //   Called from non-worker: start_background<true>
    // Return 0 on success, errno otherwise.
    template <bool REMOTE>
    int start_background(bthread_t* __restrict tid,
                         const bthread_attr_t* __restrict attr,
                         void * (*fn)(void*),
                         void* __restrict arg);

    // Suspend caller and run next bthread in TaskGroup *pg.
    // 阻塞当前 bthread ，执行下一个就绪 bthread 。
    // 按照调度顺序，调度到下一个 bthread (runqueue, 自己的remotequeue，从其他 TaskGroup steal)
    static void sched(TaskGroup** pg);
    static void ending_sched(TaskGroup** pg);

    // Suspend caller and run bthread `next_tid' in TaskGroup *pg.
    // Purpose of this function is to avoid pushing `next_tid' to _rq and
    // then being popped by sched(pg), which is not necessary.
    //
    //
    static void sched_to(TaskGroup** pg, TaskMeta* next_meta);
    // 调度到 next_tid 对应的 bthread 并进行执行
    static void sched_to(TaskGroup** pg, bthread_t next_tid);
    static void exchange(TaskGroup** pg, bthread_t next_tid);

    // The callback will be run in the beginning of next-run bthread.
    // Can't be called by current bthread directly because it often needs
    // the target to be suspended already.
    //
    // 在执行下一个任务之前调用的函数，比如前一个 bthread 切入到新的 bthread 之后，需要把前一个任务的 btread_id push 到 runqueue_ 等。
    //
    // 通常在使用 brpc 的协程编程时，可以通过 set_remained 函数向 _last_context_remained 和 _last_context_remained_arg 中添加要保存的 RemainedFn 回调函数和参数。
    // 当执行 yield 或者发生其他协程切换时，就会先执行 RemainedFn 回调函数并传入相应的参数，再进行协程切换。
    typedef void (*RemainedFn)(void*);
    void set_remained(RemainedFn cb, void* arg) {
        _last_context_remained = cb;
        _last_context_remained_arg = arg;
    }
    
    // Suspend caller for at least |timeout_us| microseconds.
    // If |timeout_us| is 0, this function does nothing.
    // If |group| is NULL or current thread is non-bthread, call usleep(3)
    // instead. This function does not create thread-local TaskGroup.
    // Returns: 0 on success, -1 otherwise and errno is set.
    static int usleep(TaskGroup** pg, uint64_t timeout_us);

    // Suspend caller and run another bthread. When the caller will resume
    // is undefined.
    static void yield(TaskGroup** pg);

    // Suspend caller until bthread `tid' terminates.
    static int join(bthread_t tid, void** return_value);

    // Returns true iff the bthread `tid' still exists. Notice that it is
    // just the result at this very moment which may change soon.
    // Don't use this function unless you have to. Never write code like this:
    //    if (exists(tid)) {
    //        Wait for events of the thread.   // Racy, may block indefinitely.
    //    }
    static bool exists(bthread_t tid);

    // Put attribute associated with `tid' into `*attr'.
    // Returns 0 on success, -1 otherwise and errno is set.
    static int get_attr(bthread_t tid, bthread_attr_t* attr);

    // Get/set TaskMeta.stop of the tid.
    static void set_stopped(bthread_t tid);
    static bool is_stopped(bthread_t tid);

    // The bthread running run_main_task();
    bthread_t main_tid() const { return _main_tid; }
    TaskStatistics main_stat() const;
    // Routine of the main task which should be called from a dedicated pthread.
    void run_main_task();

    // current_task is a function in macOS 10.0+
#ifdef current_task
#undef current_task
#endif
    // Meta/Identifier of current task in this group.
    TaskMeta* current_task() const { return _cur_meta; }
    bthread_t current_tid() const { return _cur_meta->tid; }
    // Uptime of current task in nanoseconds.
    int64_t current_uptime_ns() const
    { return butil::cpuwide_time_ns() - _cur_meta->cpuwide_start_ns; }

    // True iff current task is the one running run_main_task()
    bool is_current_main_task() const { return current_tid() == _main_tid; }
    // True iff current task is in pthread-mode.
    bool is_current_pthread_task() const
    { return _cur_meta->stack == _main_stack; }

    // Active time in nanoseconds spent by this TaskGroup.
    int64_t cumulated_cputime_ns() const { return _cumulated_cputime_ns; }

    // Push a bthread into the runqueue
    // 将 tid 对应的 bthread push 到该 TaskGroup 的 rq
    void ready_to_run(bthread_t tid, bool nosignal = false);
    // Flush tasks pushed to rq but signalled.
    void flush_nosignal_tasks();

    // Push a bthread into the runqueue from another non-worker thread.
    // 将 tid 对应的 bthread push 到该 TaskGroup 的 remote_queue
    void ready_to_run_remote(bthread_t tid, bool nosignal = false);
    void flush_nosignal_tasks_remote_locked(butil::Mutex& locked_mutex);
    void flush_nosignal_tasks_remote();

    // Automatically decide the caller is remote or local, and call
    // the corresponding function.
    void ready_to_run_general(bthread_t tid, bool nosignal = false);
    void flush_nosignal_tasks_general();

    // The TaskControl that this TaskGroup belongs to.
    TaskControl* control() const { return _control; }

    // Call this instead of delete.
    void destroy_self();

    // Wake up blocking ops in the thread.
    // Returns 0 on success, errno otherwise.
    static int interrupt(bthread_t tid, TaskControl* c);

    // Get the meta associate with the task.
    static TaskMeta* address_meta(bthread_t tid);

    // Push a task into _rq, if _rq is full, retry after some time. This
    // process make go on indefinitely.
    void push_rq(bthread_t tid);

private:
friend class TaskControl;

    // You shall use TaskControl::create_group to create new instance.
    explicit TaskGroup(TaskControl*);

    int init(size_t runqueue_capacity);

    // You shall call destroy_self() instead of destructor because deletion
    // of groups are postponed to avoid race.
    ~TaskGroup();

    // 如果有 RemainFunc 需要调用，首先调用 RemainFunc，然后执行 bthread 对应的func，完成之后，继续调用 ending_sched 调度执行其他任务
    static void task_runner(intptr_t skip_remained);

    // Callbacks for set_remained()
    static void _release_last_context(void*);
    static void _add_sleep_event(void*);
    struct ReadyToRunArgs {
        bthread_t tid;
        bool nosignal;
    };
    static void ready_to_run_in_worker(void*);
    static void ready_to_run_in_worker_ignoresignal(void*);

    // Wait for a task to run.
    // Returns true on success, false is treated as permanent error and the
    // loop calling this function should end.
    bool wait_task(bthread_t* tid);


    // [重要]
    // 创建 bthread 只有两种情况：
    //  1、本身在 bthread 内创建 bthread ：
    //      因为当前 bthread 肯定跑在某个 worker 里，那就把新创建的 bthread 加到该 worker 的 _rq 里，这样 locality 貌似好一些。
    //  2、在 pthread 里创建 bthread ：
    //      TaskControl 随机挑个 worker（也就是 TaskGroup ），放到该 worker 的 _remote_rq 里（存放所有非 worker 创建的 bthread ），
    //      这个队列是个一读者多写者的队列，难做 wait-free，用 mutex 保护。
    //
    // 调度优先级
    //  - 基本调度：bth 在每个先进先出的 rq 队列中逐个执行
    //  - 若本地（本 worker ）rq 没有了则去本地 _remote_rq 里 pop 一个放到 runq 里运行
    //  - 去其他 worker 的 _rq 里偷
    //  - 去其他 worker 的 _remote_rq 里偷
    //
    //【tips】
    //  worker 拿到 tid 如果无阻塞就一定会执行完，有阻塞就从 rq 先拿掉放到队尾，再运行下个 bthread ，唤醒有别的机制。
    //【tips】
    //  如果一个 worker 内有一些 pthread 级别的阻塞，相当于这个 worker 就被阻塞了，那么其他 worker 会偷走该 worker 内被阻塞的 bthread ，
    //  保证整个系统可以顺利地跑在多核上。

    // 获取就绪协程
    //
    // 消费队列的优先级：
    //  - 本 TaskGroup 中的 _remote_rq ；
    //  - 随机其他 TaskGroup 中的 _rq ；
    //  - 第 2 步中 TaskGroup 中的 _remote_rq 。
    //
    // 对于以上设计，一般会有两个疑问：
    //  - 能否置换第 2 和第 3 步？
    //  - 本 TaskGroup 中的 _rq 为什么不被消费？
    //
    // 对于问题 1：
    //  从设计上考虑，Work Stealing 调度的主要目的是让 bthread 被更快调度到更多核心，所以设计成优先 steal 其他 Worker push 到那个 Worker 所属的 _rq 队列中的 bthread，再去消费 non-Worker 线程的 bthread 任务。
    //  从我的理解来说，这套设计的思想是：每个 TaskGroup 先优先解决分内的问题（local _remote_rq）；再优先支援其他 TaskGroup 的重要公共问题（other's _rq），因为设计上需要被更快处理；
    //  最后支援其他 TaskGroup 的私有问题（other's _remote_rq），因为这个队列中都是优先级较低的来自 non-Worker 线程的任务。
    //
    // 对于问题 2：
    //  首先是避免了非必要的竞争，不难看出如果自己也获取本 TaskGroup 中 _rq 任务的话可能会与其他前来 steal 的 TaskGroup 形成冲突。
    //  同时，local _rq 的消费也并不是只能靠其他 TaskGroup 的steal_task() 才能完成，在 task_runner() 的 ending_sched() 中将消费 local _rq。
    //
    //
    // wait_task 优先消费本地 _remote_rq ，没有则 steal 其他 TaskGroup 中的 _rq、remote_rq。
    // 最后在 task_runner 的 ending_sched 中消费自己的 _rq 。
    // TaskGroup 之所以使用 2 种 queue 就是为了降低 steal 时的竞争。


    bool steal_task(bthread_t* tid) {
        // 先从自己的 _remote_rq 取 tid ，取到返回
        if (_remote_rq.pop(tid)) {
            return true;
        }
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
        // 获取不到 tid ，更新 _last_pl_state 取值以便于再次进入休眠。
        _last_pl_state = _pl->get_state();
#endif
        // 从全局 TC 来窃取任务
        return _control->steal_task(tid, &_steal_seed, _steal_offset);
    }

    // 当前正在执行的 bthread 的 TaskMeta 对象
    TaskMeta* _cur_meta;
    // the control that this group belongs to
    // 当前 Pthread（TaskGroup）所属的 TaskControl（线程池）
    TaskControl* _control;

    // push 任务到自身队列，未 signal 唤醒其他 TaskGroup(Pthread) 的次数
    int _num_nosignal;
    // push 任务到自身队列，signal 其他 TaskGroup(Pthread) 的次数（一旦 signal 置 0 ）
    int _nsignaled;

    // last scheduling time
    int64_t _last_run_ns;               // 上一次调度执行的时间
    int64_t _cumulated_cputime_ns;      // 总共累计的执行时间

    size_t _nswitch;                    // bthread 之间的切换次数



    // 在 brpc 框架中，g->_last_context_remained 是 TaskGroup 类中的一个成员变量，用于记录上一个任务（即切换之前正在执行的任务）中尚未完成的操作。
    // 如果在任务切换时发现有 _last_context_remained ，则需要先执行该函数指针指向的操作，然后再切换到下一个任务。
    //
    // 具体来说，_last_context_remained 是一个 RemainedFn 类型的函数指针，可以执行上一个任务中保存下来的一些需要继续执行的操作。
    // 在 TaskGroup::sched_to() 中，如果存在_last_context_remained，则需要循环处理直至完成所有操作才能返回切换后的任务。
    //
    // 通过使用 _last_context_remained ，brpc 确保了应用程序在每个任务之间的状态转换没有失序或漏掉任何操作，并且避免了挂起和错误等异常情况。
    // 在实际使用中，这种机制对提高应用程序的可靠性和稳定性非常重要。
    //
    /*
    * 在执行下一个任务(task)之前调用的函数，比如前一个 bthread 切入到新的 bthread 之后，
    * 需要把前一个任务的 btread_id push 到 runqueue 以备下次调用。
    */
    RemainedFn _last_context_remained;  // 执行函数
    void* _last_context_remained_arg;   // 执行参数

    // 没有任务在的时候停在停车场等待唤醒
    ParkingLot* _pl;
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
    // 停车场的上一次状态，用于 wakeup
    ParkingLot::State _last_pl_state;
#endif

    // 本 TaskGroup（pthread） 从其他 TaskGroup 抢占任务的随机因子信息（work-steal）
    size_t _steal_seed;     // 随机数
    size_t _steal_offset;

    // TG 单独持有的栈
    ContextualStack* _main_stack;    // 初始化执行该 TaskGroup 的 Pthread 的初始 _main_stack
    // TG 调度线程
    bthread_t _main_tid;             // 对应的 main thread 的 pthread 信息

    // WorkStealingQueue 用于存储由 task_group(worker) 自身生成的 bthread，RemoteTaskQueue 用于存储非 task_group 生成的 bthread 。
    // 因为 WorkStealingQueue 中的 push 和 pop 只会被本 worker 调用，仅与 steal 产生竞争，所以使用无锁实现减少锁的开销。
    // 而 RemoteTaskQueue 由于是由非 worker 调用，且是随机选择 task_group , 所以使用锁的方式实现。
    //
    // StealTask 是 brpc 里调度 bthread 的策略，即可以从其他 task_group(worker) 的队列里面去拿任务，是实现 M:N 模型的方法。

    // WorkingStealingQueue 是个固定大小的无锁循环队列，采用它来实现本地任务队列，用于存储本 worker pthread 创建的 bthread。
    // 只有该线程会向其中 push 和 pop 任务，其他线程调用的是 steal() 方法来窃取任务。
    //
    // push 和 pop 只在本线程中调用，不可能并行, 但是可能有一堆 steal 在和他们并行。
    // push 和 pop 操作开销很小，只在本线程中调用，但是其 pop 操作是从队列尾部 pop 的，其头部的任务的弹出是 steal() 方法。
    //
    // 这个队列的任务主要是本地来创建、获取可运行协程，只有当其他线程的本地任务已空、且本线程远程队列已空时，其他线程才会从中 steal() 。//
    WorkStealingQueue<bthread_t> _rq;
    // 由于远程任务队列不会那么频繁被本地线程访问，且更多的是被其他线程访问，所以采用一个加锁的有界循环队列实现远程任务队列。
    // 它用来存储 non-worker pthread 创建的 bthread 。
    RemoteTaskQueue _remote_rq;




    int _remote_num_nosignal;
    int _remote_nsignaled;

    int _sched_recursive_guard;
};

}  // namespace bthread

#include "task_group_inl.h"

#endif  // BTHREAD_TASK_GROUP_H
