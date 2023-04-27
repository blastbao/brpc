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
    static void sched(TaskGroup** pg);
    static void ending_sched(TaskGroup** pg);

    // Suspend caller and run bthread `next_tid' in TaskGroup *pg.
    // Purpose of this function is to avoid pushing `next_tid' to _rq and
    // then being popped by sched(pg), which is not necessary.
    //
    //
    //
    //
    //
    static void sched_to(TaskGroup** pg, TaskMeta* next_meta);
    static void sched_to(TaskGroup** pg, bthread_t next_tid);
    static void exchange(TaskGroup** pg, bthread_t next_tid);

    // The callback will be run in the beginning of next-run bthread.
    // Can't be called by current bthread directly because it often needs
    // the target to be suspended already.
    //
    // 通过调用 set_remained 方法，用户可以将一个需要延迟执行的函数注册到当前任务组中。
    // 当该任务组执行完成后，这个函数就会被放入到 g->_last_context_remained 列表中，等待下一次调度时执行。
    //
    // 一般情况下，set_remained 方法通常用于异步编程中。
    // 例如，在进行异步 I/O 操作时，我们通常需要等待 I/O 完成后才能继续处理剩余工作。
    // 在这种情况下，我们需要将剩余工作封装为一个函数，然后通过 set_remained 方法将这个函数注册到当前任务组中。
    // 这个函数将被存储到 g->_last_context_remained 列表中，在 I/O 完成后被调用以完成后续操作。
    //
    // 一般来说，用户可以在 bthread_async 的回调函数或者其他需要异步处理的地方调用 set_remained 方法。
    //
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
    void ready_to_run(bthread_t tid, bool nosignal = false);
    // Flush tasks pushed to rq but signalled.
    void flush_nosignal_tasks();

    // Push a bthread into the runqueue from another non-worker thread.
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

    // sched_to 和 task_runner 是 brpc 框架中两个不同的函数，分别用于调度任务和执行任务。
    // 它们之间的关系是 sched_to 负责将任务调度到工作线程中，而 task_runner 负责在工作线程中执行任务。
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

    // 获取就绪协程
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


    TaskMeta* _cur_meta;
    
    // the control that this group belongs to
    TaskControl* _control;
    int _num_nosignal;
    int _nsignaled;
    // last scheduling time
    int64_t _last_run_ns;
    int64_t _cumulated_cputime_ns;

    size_t _nswitch;

    // 在 brpc 框架中，g->_last_context_remained 是 TaskGroup 类中保存所有未处理的协程上下文切换回调函数的指针。
    //
    // 在 sched_to 函数中，如果切换协程之前存在 g->_last_context_remained ，则在切换完协程后需要调用这些回调函数。
    //
    // 具体来说，当一个协程中使用了协程库提供的 co_yield 函数进行协程切换时，当前协程将进入挂起状态并保存相应的上下文信息。
    // 然后，brpc 框架会将当前协程置于一个待处理列表中，并切换到下一个协程执行。
    // 在下一次协程切换之前，如果当前协程还没有得到继续执行的机会，那么它的上下文信息就会被加入到 g->_last_context_remained 列表中。
    //
    // 而在下一次协程切换时，如果 g->_last_context_remained 中存在待处理的上下文信息，那么这些信息会被还原，并且相应的回调函数会被执行。
    // 这样，程序就能够在多个协程之间正确地切换，并且保证上下文信息得到恢复和回调函数得到执行。

    // 在 brpc 框架中，g->_last_context_remained 是 TaskGroup 类中的一个成员变量，用于记录上一个任务（即切换之前正在执行的任务）中尚未完成的操作。
    // 如果在任务切换时发现有 _last_context_remained ，则需要先执行该函数指针指向的操作，然后再切换到下一个任务。
    //
    // 具体来说，_last_context_remained 是一个 RemainedFn 类型的函数指针，可以执行上一个任务中保存下来的一些需要继续执行的操作。
    // 在 TaskGroup::sched_to() 中，如果存在_last_context_remained，则需要循环处理直至完成所有操作才能返回切换后的任务。
    //
    // 通过使用 _last_context_remained ，brpc 确保了应用程序在每个任务之间的状态转换没有失序或漏掉任何操作，并且避免了挂起和错误等异常情况。
    // 在实际使用中，这种机制对提高应用程序的可靠性和稳定性非常重要。

    RemainedFn _last_context_remained;
    void* _last_context_remained_arg;

    ParkingLot* _pl;
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
    ParkingLot::State _last_pl_state;
#endif
    size_t _steal_seed;     // 随机数
    size_t _steal_offset;



    //
    //
    ContextualStack* _main_stack;



    bthread_t _main_tid;
    WorkStealingQueue<bthread_t> _rq;
    RemoteTaskQueue _remote_rq;
    int _remote_num_nosignal;
    int _remote_nsignaled;

    int _sched_recursive_guard;
};

}  // namespace bthread

#include "task_group_inl.h"

#endif  // BTHREAD_TASK_GROUP_H
