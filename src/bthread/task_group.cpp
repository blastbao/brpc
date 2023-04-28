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

#include <sys/types.h>
#include <stddef.h>                         // size_t
#include <gflags/gflags.h>
#include "butil/compat.h"                   // OS_MACOSX
#include "butil/macros.h"                   // ARRAY_SIZE
#include "butil/scoped_lock.h"              // BAIDU_SCOPED_LOCK
#include "butil/fast_rand.h"
#include "butil/unique_ptr.h"
#include "butil/third_party/murmurhash3/murmurhash3.h" // fmix64
#include "bthread/errno.h"                  // ESTOP
#include "bthread/butex.h"                  // butex_*
#include "bthread/sys_futex.h"              // futex_wake_private
#include "bthread/processor.h"              // cpu_relax
#include "bthread/task_control.h"
#include "bthread/task_group.h"
#include "bthread/timer_thread.h"
#include "bthread/errno.h"

namespace bthread {

static const bthread_attr_t BTHREAD_ATTR_TASKGROUP = {
    BTHREAD_STACKTYPE_UNKNOWN, 0, NULL };

static bool pass_bool(const char*, bool) { return true; }

DEFINE_bool(show_bthread_creation_in_vars, false, "When this flags is on, The time "
            "from bthread creation to first run will be recorded and shown "
            "in /vars");
const bool ALLOW_UNUSED dummy_show_bthread_creation_in_vars =
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_show_bthread_creation_in_vars,
                                    pass_bool);

DEFINE_bool(show_per_worker_usage_in_vars, false,
            "Show per-worker usage in /vars/bthread_per_worker_usage_<tid>");
const bool ALLOW_UNUSED dummy_show_per_worker_usage_in_vars =
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_show_per_worker_usage_in_vars,
                                    pass_bool);

BAIDU_VOLATILE_THREAD_LOCAL(TaskGroup*, tls_task_group, NULL);
// Sync with TaskMeta::local_storage when a bthread is created or destroyed.
// During running, the two fields may be inconsistent, use tls_bls as the
// groundtruth.
__thread LocalStorage tls_bls = BTHREAD_LOCAL_STORAGE_INITIALIZER;

// defined in bthread/key.cpp
extern void return_keytable(bthread_keytable_pool_t*, KeyTable*);

// [Hacky] This is a special TLS set by bthread-rpc privately... to save
// overhead of creation keytable, may be removed later.
BAIDU_VOLATILE_THREAD_LOCAL(void*, tls_unique_user_ptr, NULL);

const TaskStatistics EMPTY_STAT = { 0, 0 };

const size_t OFFSET_TABLE[] = {
#include "bthread/offset_inl.list"
};

int TaskGroup::get_attr(bthread_t tid, bthread_attr_t* out) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            *out = m->attr;
            return 0;
        }
    }
    errno = EINVAL;
    return -1;
}

void TaskGroup::set_stopped(bthread_t tid) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            m->stop = true;
        }
    }
}

bool TaskGroup::is_stopped(bthread_t tid) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            return m->stop;
        }
    }
    // If the tid does not exist or version does not match, it's intuitive
    // to treat the thread as "stopped".
    return true;
}

// 当前工作线程在窃取任务前会先取一次 ParkingLot 的状态，当状态发生改变时会直接跳过 wait 。
bool TaskGroup::wait_task(bthread_t* tid) {

    do {
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
        // 已结束，退出
        if (_last_pl_state.stopped()) {
            return false;
        }

        // [重要]
        //
        // _pl._pending_signal 的值和 _last_pl_state.val 相等则阻塞，不等则返回。
        //
        // 注意，这里 _pl._pending_signal 和 _last_pl_state.val 的具体取值没有意义，主要是用来检测变化，
        // 也就是说，如果自上次 steal_task() 执行之后，这个值没有变化，也就意味着过程中没有新增的协程，
        // 也就不需要马上再次执行 steal_task() ，而是进入阻塞状态，等待被新增协程的事件唤醒。
        _pl->wait(_last_pl_state);


        // 被唤醒后，意味着可能有新增协程等待执行，尝试执行 steal_task() 来获取。
        //
        // work stealing 不是协程的专利，更不是 Go 语言的专利。
        // work stealing 是一种通用的实现负载均衡的算法。
        // 这里的负载均衡说的不是像 Nginx 那种对于外部网络请求做负载均衡，此处指的是每个 CPU 处理任务时，每个核的负载均衡。
        // 不止协程，其实线程池也可以做 work stealing 。
        if (steal_task(tid)) {
            return true;
        }
#else
        const ParkingLot::State st = _pl->get_state();
        if (st.stopped()) {
            return false;
        }
        if (steal_task(tid)) {
            return true;
        }
        _pl->wait(st);
#endif
    } while (true);
}

static double get_cumulated_cputime_from_this(void* arg) {
    return static_cast<TaskGroup*>(arg)->cumulated_cputime_ns() / 1000000000.0;
}

// 主循环：
//  不断等待就绪协程，当获取到就绪协程 id 后，执行该协程。
//  当无法获取到协程（当协程执行完了）时，该线程进入休眠，等待唤醒。
//
// 主要步骤：
//  1. wait_task()：获取有效任务（涉及工作窃取），出参 tid 会记录这个任务的 ID 号；
//  2. sched_to()：进行栈、寄存器等运行时上下文的切换，为接下来运行的 tid 任务准备上下文。
//  3. task_runner()：执行任务。
void TaskGroup::run_main_task() {
    bvar::PassiveStatus<double> cumulated_cputime(get_cumulated_cputime_from_this, this);
    std::unique_ptr<bvar::PerSecond<bvar::PassiveStatus<double> > > usage_bvar;

    TaskGroup* dummy = this;
    bthread_t tid;

    // 每次获取一个待执行的 bthread ，若没有在 parkinglot 上睡眠等待唤醒
    while (wait_task(&tid)) {

        // 切换到 tid 标记的 bthread 的上下文
        TaskGroup::sched_to(&dummy, tid);

        DCHECK_EQ(this, dummy);
        DCHECK_EQ(_cur_meta->stack, _main_stack);

        // 执行任务
        if (_cur_meta->tid != _main_tid) {
            TaskGroup::task_runner(1/*skip remained*/);
        }

        if (FLAGS_show_per_worker_usage_in_vars && !usage_bvar) {
            char name[32];
#if defined(OS_MACOSX)
            snprintf(name, sizeof(name), "bthread_worker_usage_%" PRIu64,
                     pthread_numeric_id());
#else
            snprintf(name, sizeof(name), "bthread_worker_usage_%ld",(long)syscall(SYS_gettid));
#endif
            usage_bvar.reset(new bvar::PerSecond<bvar::PassiveStatus<double> >(name, &cumulated_cputime, 1));
        }
    }
    // 这里线程退出


    // Don't forget to add elapse of last wait_task.
    current_task()->stat.cputime_ns += butil::cpuwide_time_ns() - _last_run_ns;
}

TaskGroup::TaskGroup(TaskControl* c)
    :
    _cur_meta(NULL)
    , _control(c)
    , _num_nosignal(0)
    , _nsignaled(0)
    , _last_run_ns(butil::cpuwide_time_ns())
    , _cumulated_cputime_ns(0)
    , _nswitch(0)
    , _last_context_remained(NULL)
    , _last_context_remained_arg(NULL)
    , _pl(NULL)
    , _main_stack(NULL)
    , _main_tid(0)
    , _remote_num_nosignal(0)
    , _remote_nsignaled(0)
#ifndef NDEBUG
    , _sched_recursive_guard(0)
#endif
{
    _steal_seed = butil::fast_rand();
    _steal_offset = OFFSET_TABLE[_steal_seed % ARRAY_SIZE(OFFSET_TABLE)];

    // 根据线程号哈希到某个 ParkingLot 对象上
    _pl = &c->_pl[butil::fmix64(pthread_numeric_id()) % TaskControl::PARKING_LOT_NUM];
    CHECK(c);
}

TaskGroup::~TaskGroup() {
    if (_main_tid) {
        TaskMeta* m = address_meta(_main_tid);
        CHECK(_main_stack == m->stack);
        return_stack(m->release_stack());
        return_resource(get_slot(_main_tid));
        _main_tid = 0;
    }
}

int TaskGroup::init(size_t runqueue_capacity) {
    if (_rq.init(runqueue_capacity) != 0) {
        LOG(FATAL) << "Fail to init _rq";
        return -1;
    }
    if (_remote_rq.init(runqueue_capacity / 2) != 0) {
        LOG(FATAL) << "Fail to init _remote_rq";
        return -1;
    }
    ContextualStack* stk = get_stack(STACK_TYPE_MAIN, NULL);
    if (NULL == stk) {
        LOG(FATAL) << "Fail to get main stack container";
        return -1;
    }
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource<TaskMeta>(&slot);
    if (NULL == m) {
        LOG(FATAL) << "Fail to get TaskMeta";
        return -1;
    }
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = NULL;
    m->arg = NULL;
    m->local_storage = LOCAL_STORAGE_INIT;
    m->cpuwide_start_ns = butil::cpuwide_time_ns();
    m->stat = EMPTY_STAT;
    m->attr = BTHREAD_ATTR_TASKGROUP;
    m->tid = make_tid(*m->version_butex, slot);
    m->set_stack(stk);

    _cur_meta = m;
    _main_tid = m->tid;
    _main_stack = stk;
    _last_run_ns = butil::cpuwide_time_ns();
    return 0;
}

// 常见的协程调度策略：
//  - 星切：主线程 --> 协程1 --> 主线程 --> 协程2 --> ... -->主线程
//  - 环切：主线程 --> 协程1 --> 协程2 --> 协程3 --> ... -->主线程
// 可以看出，环切比星切少了一半的切换次数，效率更高。
// bthread 采用的是环切。

//  虽然调用 bthread_start_* 创建新的 bthread 传入了上下文函数及其参数，但是 bthread 上不是直接运行该上下文函数，
//  实际上运行的是 TaskGroup::task_runner 函数，步骤：
//
//   - 执行上一个 bthread 设置的 _last_context_remained 函数，可能是用来释放栈空间之类的。如果是在 worker pthread 的栈上运行 task ，这个步骤会跳过。
//   - 运行上下文函数。执行过程中可能有多次跳出、跳回过程，所在的 worker pthread 可能也变了。
//   - 执行完之后，执行一些清理动作，例如释放tls等。
//   - 设置下一个 bthread 需要运行的 _last_context_remained 函数。
//   - 找到下一个可以运行的 bthread ，查找次序依次为本地队列，远程队列，其它 TaskGroup 的本地队列或远程队列。
//   - 如果没有找到，就返回 worker pthread 。
//
//  要注意，有两个地方会调用到 TaskGroup::task_runner 函数：
//   - bthread 执行时，实际执行的是 TaskGroup::task_runner 函数，而在该函数里面调用上下文函数。
//   - worker pthread 执行 pthread task 时会调用 TaskGroup::task_runner 函数。
//
//  所以，两种情况下会回到 worker pthread ：
//   - 没有需要执行的 bthread 。返回后，可以调用 TaskGroup::wait_task 从其它 TaskGroup 偷取任务。如果仍然没有，就休眠等待唤醒。
//   - 下一个 task 使用的是 worker-pthread 的栈。
//
//  首先获取当前任务组对象 g，并检查是否有需要延迟执行的函数（即 RemainedFn 对象），如果有，就按顺序执行这些函数。
//  然后，进入 do-while 循环。
//      其中，每次循环会从 g 的任务队列中取出下一个任务，并执行它的 user function（即 m->fn(m->arg)）。
//      如果 user function 抛出了 ExitException 异常，则将异常中的值作为线程返回值；否则，返回值就是 user function 的返回值。
//      接着，在执行 user function 后，会进行一些清理工作。
//          具体来说，会清空 TLS 中存储的 KeyTable 对象，并将其归还给对象池；
//          同时，会增加版本号，唤醒所有等待 join 的线程。
//      最后，会将当前任务组对象 g 返回到其父级任务组中。
//  循环执行到任务队列中的下一个任务的 tid 等于 g->_main_tid 时，循环结束。

// bthread 工作线程在执行过程中会有以下几种状态：
//  执行主协程任务，负责获取任务或者挂起等待，此时：
//      _cur_meta->tid == _main_tid
//      _cur_meta->stack == _main_stack
//  执行 pthread 任务，直接在主协程中调用 TaskGroup::task_runner(1) 执行，此时：
//      _cur_meta->tid != _main_tid
//      _cur_meta->stack == _main_stack
//  执行 bthread 任务，通过在 TaskGroup::sched_to 中调用 jump_stack 切换到协程栈，此时：
//      _cur_meta->tid != _main_tid
//      _cur_meta->stack != _main_stack
//
// 上述三种状态可以相互切换。

void TaskGroup::task_runner(intptr_t skip_remained) {

    // NOTE: tls_task_group is volatile since tasks are moved around different groups.
    TaskGroup* g = tls_task_group;

    if (!skip_remained) {
        // 用户函数执行前，先执行可能存在的 remained 函数
        while (g->_last_context_remained) {
            RemainedFn fn = g->_last_context_remained;
            g->_last_context_remained = NULL;
            fn(g->_last_context_remained_arg);
            g = tls_task_group;
        }
#ifndef NDEBUG
        --g->_sched_recursive_guard;
#endif
    }

    do {
        // A task can be stopped before it gets running, in which case
        // we may skip user function, but that may confuse user:
        // Most tasks have variables to remember running result of the task,
        // which is often initialized to values indicating success. If an
        // user function is never called, the variables will be unchanged
        // however they'd better reflect failures because the task is stopped
        // abnormally.

        // Meta and identifier of the task is persistent in this run.
        //
        // 通过 TLS 获取当前线程待执行的任务 TaskMeta
        TaskMeta* const m = g->_cur_meta;

        if (FLAGS_show_bthread_creation_in_vars) {
            // NOTE: the thread triggering exposure of pending time may spend
            // considerable time because a single bvar::LatencyRecorder
            // contains many bvar.
            g->_control->exposed_pending_time() << (butil::cpuwide_time_ns() - m->cpuwide_start_ns) / 1000L;
        }

        // Not catch exceptions except ExitException which is for implementing
        // bthread_exit(). User code is intended to crash when an exception is
        // not caught explicitly. This is consistent with other threading
        // libraries.
        //
        // 执行 bthread 中的回调函数
        void* thread_return;
        try {
            // 执行用户执行的 bthread 任务，假设中途协程调用 yield 主动挂起
            thread_return = m->fn(m->arg);
        } catch (ExitException& e) {
            thread_return = e.value();
        }

        // Group is probably changed
        g = BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group);

        // TODO: Save thread_return
        (void)thread_return;

        // Logging must be done before returning the keytable, since the logging lib
        // use bthread local storage internally, or will cause memory leak.
        // FIXME: the time from quiting fn to here is not counted into cputime
        if (m->attr.flags & BTHREAD_LOG_START_AND_FINISH) {
            LOG(INFO) << "Finished bthread " << m->tid << ", cputime=" << m->stat.cputime_ns / 1000000.0 << "ms";
        }

        // Clean tls variables, must be done before changing version_butex
        // otherwise another thread just joined this thread may not see side
        // effects of destructing tls variables.
        //
        // 清理 bthread 局部变量
        KeyTable* kt = tls_bls.keytable;
        if (kt != NULL) {
            return_keytable(m->attr.keytable_pool, kt);
            // After deletion: tls may be set during deletion.
            tls_bls.keytable = NULL;
            m->local_storage.keytable = NULL; // optional
        }

        // Increase the version and wake up all joiners, if resulting version
        // is 0, change it to 1 to make bthread_t never be 0. Any access
        // or join to the bthread after changing version will be rejected.
        // The spinlock is for visibility of TaskGroup::get_attr.
        //
        // 累加版本号，且版本号不能为 0
        {
            BAIDU_SCOPED_LOCK(m->version_lock);
            if (0 == ++*m->version_butex) {
                ++*m->version_butex;
            }
        }

        // 唤醒 joiner
        butex_wake_except(m->version_butex, 0);

        // _nbthreads 减 1
        g->_control->_nbthreads << -1;
        g->set_remained(TaskGroup::_release_last_context, m);

        // 查找下一个任务，并切换到其对应的运行时上下文
        ending_sched(&g);

    } while (g->_cur_meta->tid != g->_main_tid);

    // Was called from a pthread and we don't have BTHREAD_STACKTYPE_PTHREAD
    // tasks to run, quit for more tasks.
}

void TaskGroup::_release_last_context(void* arg) {
    TaskMeta* m = static_cast<TaskMeta*>(arg);
    if (m->stack_type() != STACK_TYPE_PTHREAD) {
        return_stack(m->release_stack()/*may be NULL*/);
    } else {
        // it's _main_stack, don't return.
        m->set_stack(NULL);
    }
    return_resource(get_slot(m->tid));
}

int TaskGroup::start_foreground(TaskGroup** pg,
                                bthread_t* __restrict th,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg) {
    if (__builtin_expect(!fn, 0)) {
        return EINVAL;
    }
    const int64_t start_ns = butil::cpuwide_time_ns();
    const bthread_attr_t using_attr = (attr ? *attr : BTHREAD_ATTR_NORMAL);
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource(&slot);
    if (__builtin_expect(!m, 0)) {
        return ENOMEM;
    }
    CHECK(m->current_waiter.load(butil::memory_order_relaxed) == NULL);
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = fn;
    m->arg = arg;
    CHECK(m->stack == NULL);
    m->attr = using_attr;
    m->local_storage = LOCAL_STORAGE_INIT;
    if (using_attr.flags & BTHREAD_INHERIT_SPAN) {
        m->local_storage.rpcz_parent_span = tls_bls.rpcz_parent_span;
    }
    m->cpuwide_start_ns = start_ns;
    m->stat = EMPTY_STAT;
    m->tid = make_tid(*m->version_butex, slot);
    *th = m->tid;
    if (using_attr.flags & BTHREAD_LOG_START_AND_FINISH) {
        LOG(INFO) << "Started bthread " << m->tid;
    }

    TaskGroup* g = *pg;
    g->_control->_nbthreads << 1;
    if (g->is_current_pthread_task()) {
        // never create foreground task in pthread.
        g->ready_to_run(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    } else {
        // NOSIGNAL affects current task, not the new task.
        RemainedFn fn = NULL;
        if (g->current_task()->about_to_quit) {
            fn = ready_to_run_in_worker_ignoresignal;
        } else {
            fn = ready_to_run_in_worker;
        }
        ReadyToRunArgs args = {
            g->current_tid(),
            (bool)(using_attr.flags & BTHREAD_NOSIGNAL)
        };
        g->set_remained(fn, &args);
        TaskGroup::sched_to(pg, m->tid);
    }
    return 0;
}

template <bool REMOTE>
int TaskGroup::start_background(bthread_t* __restrict th,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg) {

    if (__builtin_expect(!fn, 0)) {
        return EINVAL;
    }

    const int64_t start_ns = butil::cpuwide_time_ns();
    const bthread_attr_t using_attr = (attr ? *attr : BTHREAD_ATTR_NORMAL);
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource(&slot);
    if (__builtin_expect(!m, 0)) {
        return ENOMEM;
    }
    CHECK(m->current_waiter.load(butil::memory_order_relaxed) == NULL);
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = fn;
    m->arg = arg;
    CHECK(m->stack == NULL);
    m->attr = using_attr;
    m->local_storage = LOCAL_STORAGE_INIT;
    if (using_attr.flags & BTHREAD_INHERIT_SPAN) {
        m->local_storage.rpcz_parent_span = tls_bls.rpcz_parent_span;
    }
    m->cpuwide_start_ns = start_ns;
    m->stat = EMPTY_STAT;
    m->tid = make_tid(*m->version_butex, slot);
    *th = m->tid;
    if (using_attr.flags & BTHREAD_LOG_START_AND_FINISH) {
        LOG(INFO) << "Started bthread " << m->tid;
    }
    _control->_nbthreads << 1;
    if (REMOTE) {
        ready_to_run_remote(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    } else {
        ready_to_run(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    }
    return 0;
}

// Explicit instantiations.
template int
TaskGroup::start_background<true>(bthread_t* __restrict th,
                                  const bthread_attr_t* __restrict attr,
                                  void * (*fn)(void*),
                                  void* __restrict arg);
template int
TaskGroup::start_background<false>(bthread_t* __restrict th,
                                   const bthread_attr_t* __restrict attr,
                                   void * (*fn)(void*),
                                   void* __restrict arg);

int TaskGroup::join(bthread_t tid, void** return_value) {
    if (__builtin_expect(!tid, 0)) {  // tid of bthread is never 0.
        return EINVAL;
    }
    TaskMeta* m = address_meta(tid);
    if (__builtin_expect(!m, 0)) {
        // The bthread is not created yet, this join is definitely wrong.
        return EINVAL;
    }
    TaskGroup* g = tls_task_group;
    if (g != NULL && g->current_tid() == tid) {
        // joining self causes indefinite waiting.
        return EINVAL;
    }
    const uint32_t expected_version = get_version(tid);
    while (*m->version_butex == expected_version) {
        if (butex_wait(m->version_butex, expected_version, NULL) < 0 &&
            errno != EWOULDBLOCK && errno != EINTR) {
            return errno;
        }
    }
    if (return_value) {
        *return_value = NULL;
    }
    return 0;
}

bool TaskGroup::exists(bthread_t tid) {
    if (tid != 0) {  // tid of bthread is never 0.
        TaskMeta* m = address_meta(tid);
        if (m != NULL) {
            return (*m->version_butex == get_version(tid));
        }
    }
    return false;
}

TaskStatistics TaskGroup::main_stat() const {
    TaskMeta* m = address_meta(_main_tid);
    return m ? m->stat : EMPTY_STAT;
}

void TaskGroup::ending_sched(TaskGroup** pg) {
    TaskGroup* g = *pg;
    bthread_t next_tid = 0;
    // Find next task to run, if none, switch to idle thread of the group.
#ifndef BTHREAD_FAIR_WSQ
    // When BTHREAD_FAIR_WSQ is defined, profiling shows that cpu cost of
    // WSQ::steal() in example/multi_threaded_echo_c++ changes from 1.9%
    // to 2.9%
    const bool popped = g->_rq.pop(&next_tid);
#else
    const bool popped = g->_rq.steal(&next_tid);
#endif
    if (!popped && !g->steal_task(&next_tid)) {
        // Jump to main task if there's no task to run.
        next_tid = g->_main_tid;
    }

    TaskMeta* const cur_meta = g->_cur_meta;
    TaskMeta* next_meta = address_meta(next_tid);
    if (next_meta->stack == NULL) {
        if (next_meta->stack_type() == cur_meta->stack_type()) {
            // also works with pthread_task scheduling to pthread_task, the
            // transfered stack is just _main_stack.
            next_meta->set_stack(cur_meta->release_stack());
        } else {

            ContextualStack* stk = get_stack(next_meta->stack_type(), task_runner);
            if (stk) {
                next_meta->set_stack(stk);
            } else {
                // stack_type is BTHREAD_STACKTYPE_PTHREAD or out of memory,
                // In latter case, attr is forced to be BTHREAD_STACKTYPE_PTHREAD.
                // This basically means that if we can't allocate stack, run
                // the task in pthread directly.
                next_meta->attr.stack_type = BTHREAD_STACKTYPE_PTHREAD;
                next_meta->set_stack(g->_main_stack);
            }
        }
    }
    sched_to(pg, next_meta);
}

void TaskGroup::sched(TaskGroup** pg) {
    TaskGroup* g = *pg;
    bthread_t next_tid = 0;
    // Find next task to run, if none, switch to idle thread of the group.
#ifndef BTHREAD_FAIR_WSQ
    const bool popped = g->_rq.pop(&next_tid);
#else
    const bool popped = g->_rq.steal(&next_tid);
#endif

    // 若本 _rq 不存在，且无法从其它 tg steal 一个就绪 bthread ，则切换到 _main 去执行
    if (!popped && !g->steal_task(&next_tid)) {
        // Jump to main task if there's no task to run.
        next_tid = g->_main_tid;
    }

    // 切换到目标协程或者 _main 协程
    sched_to(pg, next_tid);
}

// 这段代码是 TaskGroup 类中的调度方法 sched_to，用于切换当前正在执行的任务到另一个任务上，并且记录一些指标信息。
//
// 该函数会将当前线程从当前执行的 TaskMeta 对象切换到 next_meta 指向的 TaskMeta 对象。
//
// 接下来，函数通过 jump_stack() 函数执行协程栈之间的跳转，将当前的协程切换到下一个协程中。
// 如果当前任务的协程栈不为空，则检查当前任务的协程是否为下一个任务所在的协程栈。
// 如果两个任务运行在不同的协程栈中，则需要跳转到下一个协程栈中执行。
// 如果两个任务运行在相同的协程栈中，则会触发 FATAL 错误，因为同一协程内无法调用自身。
// 最后，函数执行 last_context_remained 列表中的所有回调，将其从列表中删除。
// 最后一步是恢复 errno 和 tls_unique_user_ptr 两个变量的值，然后将当前 TaskGroup 对象指针 g 更新到 pg 所指向的地址中。
//
void TaskGroup::sched_to(TaskGroup** pg, TaskMeta* next_meta) {
    TaskGroup* g = *pg;
#ifndef NDEBUG
    if ((++g->_sched_recursive_guard) > 1) {
        LOG(FATAL) << "Recursively(" << g->_sched_recursive_guard - 1
                   << ") call sched_to(" << g << ")";
    }
#endif
    // Save errno so that errno is bthread-specific.
    // 保存当前的 errno 和 tls_unique_user_ptr，以防止在后续操作中被修改。
    const int saved_errno = errno;
    void* saved_unique_user_ptr = tls_unique_user_ptr;

    // 获取当前正在执行的 bthread meta ，更新当前 BThread 的 CPU 时间、线程切换次数以及该 TaskGroup 线程池总共的线程切换次数
    TaskMeta* const cur_meta = g->_cur_meta;
    const int64_t now = butil::cpuwide_time_ns();
    const int64_t elp_ns = now - g->_last_run_ns;
    g->_last_run_ns = now;
    cur_meta->stat.cputime_ns += elp_ns;
    if (cur_meta->tid != g->main_tid()) {     // 如果当前任务不是主线程，需要累加该任务的 CPU 时间
        g->_cumulated_cputime_ns += elp_ns;
    }
    ++cur_meta->stat.nswitch; // 更新当前 bthread 切换次数
    ++ g->_nswitch; // 更新全局切换次数

    // Switch to the task
    // 如果下一个任务不是当前正在运行的任务，则切换到下一个任务对应的上下文。
    if (__builtin_expect(next_meta != cur_meta, 1)) {
        // 将当前的 meta 设置为下一个 bthread 的 meta；
        g->_cur_meta = next_meta;

        // Switch tls_bls
        // bthread 具备内存类似 pthread 的 tls
        // 将当前任务的本地存储（local_storage）赋值给当前 meta ，将下一个任务的本地存储赋值给 tls_bls ；
        cur_meta->local_storage = tls_bls;
        tls_bls = next_meta->local_storage;

        // Logging must be done after switching the local storage, since the logging lib
        // use bthread local storage internally, or will cause memory leak.
        if ((cur_meta->attr.flags & BTHREAD_LOG_CONTEXT_SWITCH) || (next_meta->attr.flags & BTHREAD_LOG_CONTEXT_SWITCH)) {
            LOG(INFO) << "Switch bthread: " << cur_meta->tid << " -> "<< next_meta->tid;
        }

        // 如果当前任务有栈空间（stack == NULL），意味着当前任务是主线程，则跳过此步操作。
        // 如果当前任务有栈空间（stack != NULL），则判断下一个任务是否与当前任务在相同的栈空间中。
        // 如果不是，则需要使用 jump_stack 函数切换到下一个任务的栈空间。
        // 需要注意的是，有可能被切换的任务属于其他 TaskGroup 对象，因此需要重新将 g 赋值为 BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group)；
        if (cur_meta->stack != NULL) {
            if (next_meta->stack != cur_meta->stack) {
                // 若 next_meta 有自己的栈，jump_stack 切换到 next_meta 的栈
                // jump_stack 是一段汇编代码，主要作用是栈和寄存器切换
                jump_stack(cur_meta->stack, next_meta->stack);

                // [重要]

                // cur_meta 再次得到调度，从这里开始执行，由于 bthread 允许偷任务，可能此时是其他工作线程在执行，需要切换
                // probably went to another group, need to assign g again.
                g = BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group);
            }
#ifndef NDEBUG
            else {
                // 一个 pthread_task 切换到另一个 pthread_task ，只能在工作线程的栈上发生

                // else pthread_task is switching to another pthread_task, sc
                // can only equal when they're both _main_stack
                CHECK(cur_meta->stack == g->_main_stack);
            }
#endif
        }

        // else because of ending_sched(including pthread_task->pthread_task)
        // 如果当前任务没有栈空间，则说明该任务已经被结束或者切换到了 pthread 线程中。

    } else {
        // 如果当前任务和下一个任务相同，则抛出 FATAL 错误。
        LOG(FATAL) << "bthread=" << g->current_tid() << " sched_to itself!";
    }


    // g->_last_context_remained 变量表示上一个 context 中剩余的任务
    while (g->_last_context_remained) {
        RemainedFn fn = g->_last_context_remained;
        g->_last_context_remained = NULL;
        fn(g->_last_context_remained_arg);
        g = BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group);
    }

    // 还原 errno 和 unique_user_ptr 等线程相关属性
    // Restore errno
    errno = saved_errno;
    // tls_unique_user_ptr probably changed.
    BAIDU_SET_VOLATILE_THREAD_LOCAL(tls_unique_user_ptr, saved_unique_user_ptr);

#ifndef NDEBUG
    --g->_sched_recursive_guard;
#endif

    // 更新 pg 指向新的任务组 TaskGroup 对象，将其置为当前线程所在的 TaskGroup。
    *pg = g;
}

void TaskGroup::destroy_self() {
    if (_control) {
        _control->_destroy_group(this);
        _control = NULL;
    } else {
        CHECK(false);
    }
}

void TaskGroup::ready_to_run(bthread_t tid, bool nosignal) {
    push_rq(tid);
    if (nosignal) {
        ++_num_nosignal; // 对于设定了不触发信号的任务，仅增加计数
    } else {
        const int additional_signal = _num_nosignal;
        _num_nosignal = 0;
        _nsignaled += 1 + additional_signal;
        // 调用 signal_task 唤醒
        _control->signal_task(1 + additional_signal);
    }
}

// 将还没有触发信号的任务统一触发信号（工作线程内调用）
void TaskGroup::flush_nosignal_tasks() {
    const int val = _num_nosignal;
    if (val) {
        _num_nosignal = 0;
        _nsignaled += val;
        _control->signal_task(val);
    }
}


// 本函数的作用是将 tid 标识的线程加入到远程任务队列中，如果任务队列已满，则进行等待，并在一定时间间隔后重试。
// 如果 nosignal 设置为 true ，表示当前线程不需要接收信号，否则会给其它线程发送信号。
void TaskGroup::ready_to_run_remote(bthread_t tid, bool nosignal) {
    _remote_rq._mutex.lock();



    // [重要]
    //
    // while 循环入队，失败就执行 flush_nosignal_tasks_remote_locked() 然后休眠 1ms ，然后重新尝试入队。
    // 这里入队失败的唯一原因就是 remote_rq 的容量满了。flush_nosignal_tasks_remote_locked() 的操作也无非就是发出一个信号，
    // 让 remote_rq 中的任务（TM/bthread) 尽快被消费掉，给新的任务入队留出空间。
    // 另外 flush_nosignal_tasks_remote_locked() 内会做解锁操作，所以休眠 1ms 之后需要重新加锁。
    while (!_remote_rq.push_locked(tid)) {
        flush_nosignal_tasks_remote_locked(_remote_rq._mutex);
        LOG_EVERY_SECOND(ERROR) << "_remote_rq is full, capacity=" << _remote_rq.capacity();
        ::usleep(1000);
        _remote_rq._mutex.lock();
    }
    // 在 while 结束之后，表示新任务已经入队。



    // 是否需要发送信号通知有新的任务可以处理。
    if (nosignal) {
        // 如果不需发送信号，则直接更新 _remote_num_nosignal 计数器，表示当前有一个新任务待处理。
        ++_remote_num_nosignal;
        _remote_rq._mutex.unlock();
    } else {
        // 如果需要发送信号，当前有 1 + additional_signal 个任务待处理，通知其他人来消费。
        const int additional_signal = _remote_num_nosignal;
        _remote_num_nosignal = 0;
        _remote_nsignaled += 1 + additional_signal;
        _remote_rq._mutex.unlock();  // 锁的生命周期内也保护了计数相关变量
        _control->signal_task(1 + additional_signal);
    }
}

void TaskGroup::flush_nosignal_tasks_remote_locked(butil::Mutex& locked_mutex) {
    const int val = _remote_num_nosignal;
    if (!val) {
        locked_mutex.unlock();
        return;
    }
    _remote_num_nosignal = 0;
    _remote_nsignaled += val;
    locked_mutex.unlock();

    //
    _control->signal_task(val);
}

void TaskGroup::ready_to_run_general(bthread_t tid, bool nosignal) {
    if (tls_task_group == this) {
        return ready_to_run(tid, nosignal);
    }
    return ready_to_run_remote(tid, nosignal);
}

void TaskGroup::flush_nosignal_tasks_general() {
    if (tls_task_group == this) {
        return flush_nosignal_tasks();
    }
    return flush_nosignal_tasks_remote();
}

void TaskGroup::ready_to_run_in_worker(void* args_in) {
    ReadyToRunArgs* args = static_cast<ReadyToRunArgs*>(args_in);
    return tls_task_group->ready_to_run(args->tid, args->nosignal);
}

void TaskGroup::ready_to_run_in_worker_ignoresignal(void* args_in) {
    ReadyToRunArgs* args = static_cast<ReadyToRunArgs*>(args_in);
    return tls_task_group->push_rq(args->tid);
}

// 睡眠参数
struct SleepArgs {
    uint64_t timeout_us;    // 休眠时间
    bthread_t tid;          // 协程 ID
    TaskMeta* meta;         // 协程 信息
    TaskGroup* group;       // 归属的 TaskGroup
};

// 定时器回调函数，用于将之前睡眠的协程唤醒并添加到可执行队列中等待下一次调度。
static void ready_to_run_from_timer_thread(void* arg) {
    CHECK(tls_task_group == NULL);
    const SleepArgs* e = static_cast<const SleepArgs*>(arg);
    e->group->control()->choose_one_group()->ready_to_run_remote(e->tid);
}


// 具体来说：
//
// 首先通过调用 get_global_timer_thread() 函数获取全局的 TimerThread 实例，并使用 schedule 函数将当前协程的信息和休眠时间封装成 SleepArgs 结构体，注册到定时器队列中等待唤醒。
// 同时，如果添加失败，则直接调用 ready_to_run 函数将当前协程唤醒并加入可执行队列，以保证程序正常运行。
//
// 对于添加成功的情况，如果当前协程没有被 interrupt 打断，并且在 version_butex 的版本号没有发生变化时，给当前协程的元数据文件 meta 设置 current_sleep 标记，表示当前协程处于睡眠状态。
// 另外，由于 version_butex 是一个原子操作，因此需要使用 BAIDU_SCOPED_LOCK 宏完成线程安全。
//
// 如果当前协程已经被中断或者停止，则 unschedule 函数从定时器队列中移除当前协程的 sleep_id 任务，并调用 ready_to_run 函数将当前协程重新加入可执行队列中，以确保程序正常退出。
// 如果该 sleep_id 任务正在执行中，则 ready_to_run_in_worker 函数也将再次调度之前的协程和任务来确保程序正常退出。
//
//
// 这段代码是 TaskGroup 类中的 _add_sleep_event 函数的实现，主要用于在定时器中添加一个睡眠事件，并设置当前任务的睡眠状态。
// 具体代码如下：
//  将给定的 SleepArgs 结构体对象复制一份，并获取对应的 TaskGroup 对象。
//  调用 TimerThread 的 schedule 函数，将处理函数 ready_to_run_from_timer_thread 和回调参数 void_args 以及超时时间 e.timeout_us 作为参数传入，添加一个定时器事件并返回该定时器的 ID。
//  如果添加失败，则调用 TaskGroup 的 ready_to_run 函数，将指定任务添加到就绪队列中，并返回。
//  设置当前任务的睡眠状态，即将定时器的 ID 赋值给 TaskMeta 的 current_sleep 字段。
//  若赋值失败，则说明当前任务已被打断或停止，则调用 TimerThread 的 unschedule 函数取消定时器事件，并调用 TaskGroup 的 ready_to_run 函数将指定任务添加到就绪队列中。
// 这段代码的主要作用是实现了在定时器中添加睡眠事件，并且考虑了线程的并发性和相应状态的同步问题，使得多个线程可以正确地使用 _add_sleep_event 函数。
// 同时，代码中还利用了 RAII 的技术，利用 BAIDU_SCOPED_LOCK 宏封装了锁的获取和析构操作，使得锁的使用更加便捷和安全。
void TaskGroup::_add_sleep_event(void* void_args) {

    // Must copy SleepArgs. After calling TimerThread::schedule(), previous
    // thread may be stolen by a worker immediately and the on-stack SleepArgs
    // will be gone.
    SleepArgs e = *static_cast<SleepArgs*>(void_args);
    TaskGroup* g = e.group;

    // 调用全局定时器线程，创建定时任务。
    TimerThread::TaskId sleep_id;   // 计时器任务 id，可将其用于取消或重新安排计时器任务
    sleep_id = get_global_timer_thread()->schedule(
            ready_to_run_from_timer_thread,                             // 定时回调函数
            void_args,                                                 // 定时回调参数
            butil::microseconds_from_now(e.timeout_us)  // 唤醒时间点
    );

    // 如果 sleep_id 为空，则说明定时器任务添加失败，直接唤醒之前阻塞的协程并返回。
    if (!sleep_id) {
        // fail to schedule timer, go back to previous thread.
        g->ready_to_run(e.tid);
        return;
    }

    // Set TaskMeta::current_sleep which is for interruption.
    const uint32_t given_ver = get_version(e.tid);
    {
        BAIDU_SCOPED_LOCK(e.meta->version_lock);
        if (given_ver == *e.meta->version_butex && !e.meta->interrupted) {
            e.meta->current_sleep = sleep_id;
            return;
        }
    }

    // The thread is stopped or interrupted.
    // interrupt() always sees that current_sleep == 0. It will not schedule
    // the calling thread. The race is between current thread and timer thread.
    if (get_global_timer_thread()->unschedule(sleep_id) == 0) {
        // added to timer, previous thread may be already woken up by timer and
        // even stopped. It's safe to schedule previous thread when unschedule()
        // returns 0 which means "the not-run-yet sleep_id is removed". If the
        // sleep_id is running(returns 1), ready_to_run_in_worker() will
        // schedule previous thread as well. If sleep_id does not exist,
        // previous thread is scheduled by timer thread before and we don't
        // have to do it again.
        g->ready_to_run(e.tid);
    }

}

// To be consistent with sys_usleep, set errno and return -1 on error.
//
// 首先，判断是否休眠时间为 0，如果是，则直接执行 yield(pg) 函数，让出当前协程的执行权，等待下一次唤醒。
// 否则，创建一个 SleepArgs 结构体，并保存当前协程的相关信息和休眠时间，在调用 _add_sleep_event 函数向定时器队列中添加该事件。
// 然后，调用 sched 函数，将当前协程加入可执行队列，并且切换到下一个要执行的协程。
// 最后，判断 sleep 期间是否已被中断，如果中断则返回 -1 并设置 errno 为 EINTR 或者 ESTOP，否则返回 0。
//
// 需要说明的是，sleep 过程中可能会被 interrupt 函数打断。
// 例如，在 RPC 系统中，当客户端或服务端输出大量数据时，可能会导致网络缓冲区满，从而触发应用程序发送阻塞，
// 此时 bthread_usleep 函数就会因为超时被中断而退出当前睡眠，并进行阻塞处理。
// 在这种情况下，可以通过设置 stop 标记来确保程序安全停止，并在 errno 中返回回递错误码 ESTOP，以提示应用程序正常退出。
//
//
// 在 brpc 中，taskgroup::usleep 函数实现时必须先切到其他线程再设置定时器的原因如下：
//
//  - 避免阻塞：在 brpc 中，每个线程都有一个任务组，负责调度该线程执行的所有任务。
//      如果某个任务调用了 usleep 函数进行休眠，而没有释放任务组锁，那么该线程将一直持有锁，导致其他的任务无法进入该线程运行，从而影响系统的并发性。
//      为了避免这种情况的发生，usleep 需要释放当前任务组的锁。
//
//  - 设置定时器：在 usleep 内部，我们通过 schedule 函数来设置该任务的定时器，以便在一定时间后唤醒该任务。
//      但是，由于当前线程持有了任务组的锁，因此不能直接操作其它的任务（包括定时器的设置）。
//      所以，我们需要先释放任务组锁，让其他线程进入，然后再设置该任务的定时器。
//
// 综上所述，在 brpc 中，taskgroup::usleep 函数为了保证并发性和正确性，必须先切换到其他线程再设置定时器。
//
int TaskGroup::usleep(TaskGroup** pg, uint64_t timeout_us) {
    if (0 == timeout_us) {
        yield(pg);
        return 0;
    }

    TaskGroup* g = *pg;

    // We have to schedule timer after we switched to next bthread otherwise
    // the timer may wake up(jump to) current still-running context.
    SleepArgs e = { timeout_us, g->current_tid(), g->current_task(), g };
    g->set_remained(_add_sleep_event, &e);

    // 然后，调用 sched 函数进行线程切换，将当前线程挂起等待。
    // 这里要注意，必须先切到其他线程再设置定时器，否则可能出现定时器在当前线程执行的情况。
    // 之后，再次获取 TaskGroup 对象 g ，并将 e.meta->current_sleep 设为 0 。

    // 找一个就绪协程，jmp 过去运行
    sched(pg);

    /// ... 恢复现场，继续执行

    g = *pg;
    e.meta->current_sleep = 0;

    // 如果 e.meta->interrupted 是否为 true ，如果为 true ，说明在休眠过程中被打断了（比如收到了信号），
    // 根据 e.meta->stop 的值，将 errno 设为 ESTOP 或 EINTR ，返回 -1 表示出错。
    // 需要注意的是，将 errno 设置为 ESTOP 而不是 EINTR ，是为了跟标准的 Unix 信号处理方式保持一致。
    //
    // 如果 e.meta->interrupted 为 false ，则说明休眠时间结束，返回 0 表示正常结束。
    if (e.meta->interrupted) {
        // Race with set and may consume multiple interruptions, which are OK.
        e.meta->interrupted = false;
        // NOTE: setting errno to ESTOP is not necessary from bthread's
        // pespective, however many RPC code expects bthread_usleep to set
        // errno to ESTOP when the thread is stopping, and print FATAL
        // otherwise. To make smooth transitions, ESTOP is still set instead
        // of EINTR when the thread is stopping.
        errno = (e.meta->stop ? ESTOP : EINTR);
        return -1;
    }

    return 0;
}

// Defined in butex.cpp
bool erase_from_butex_because_of_interruption(ButexWaiter* bw);

static int interrupt_and_consume_waiters(bthread_t tid, ButexWaiter** pw, uint64_t* sleep_id) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m == NULL) {
        return EINVAL;
    }
    const uint32_t given_ver = get_version(tid);
    BAIDU_SCOPED_LOCK(m->version_lock);
    if (given_ver == *m->version_butex) {
        *pw = m->current_waiter.exchange(NULL, butil::memory_order_acquire);
        *sleep_id = m->current_sleep;
        m->current_sleep = 0;  // only one stopper gets the sleep_id
        m->interrupted = true;
        return 0;
    }
    return EINVAL;
}

static int set_butex_waiter(bthread_t tid, ButexWaiter* w) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            // Release fence makes m->interrupted visible to butex_wait
            m->current_waiter.store(w, butil::memory_order_release);
            return 0;
        }
    }
    return EINVAL;
}

// The interruption is "persistent" compared to the ones caused by signals,
// namely if a bthread is interrupted when it's not blocked, the interruption
// is still remembered and will be checked at next blocking. This designing
// choice simplifies the implementation and reduces notification loss caused
// by race conditions.
// TODO: bthreads created by BTHREAD_ATTR_PTHREAD blocking on bthread_usleep()
// can't be interrupted.
int TaskGroup::interrupt(bthread_t tid, TaskControl* c) {
    // Consume current_waiter in the TaskMeta, wake it up then set it back.
    ButexWaiter* w = NULL;
    uint64_t sleep_id = 0;
    int rc = interrupt_and_consume_waiters(tid, &w, &sleep_id);
    if (rc) {
        return rc;
    }
    // a bthread cannot wait on a butex and be sleepy at the same time.
    CHECK(!sleep_id || !w);
    if (w != NULL) {
        erase_from_butex_because_of_interruption(w);
        // If butex_wait() already wakes up before we set current_waiter back,
        // the function will spin until current_waiter becomes non-NULL.
        rc = set_butex_waiter(tid, w);
        if (rc) {
            LOG(FATAL) << "butex_wait should spin until setting back waiter";
            return rc;
        }
    } else if (sleep_id != 0) {
        if (get_global_timer_thread()->unschedule(sleep_id) == 0) {
            bthread::TaskGroup* g = bthread::tls_task_group;
            if (g) {
                g->ready_to_run(tid);
            } else {
                if (!c) {
                    return EINVAL;
                }
                c->choose_one_group()->ready_to_run_remote(tid);
            }
        }
    }
    return 0;
}

void TaskGroup::yield(TaskGroup** pg) {
    // yield 时将当前的 bthread 任务打包为 remained 函数
    TaskGroup* g = *pg;
    ReadyToRunArgs args = { g->current_tid(), false };
    g->set_remained(ready_to_run_in_worker, &args);
    sched(pg);
}

void print_task(std::ostream& os, bthread_t tid) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m == NULL) {
        os << "bthread=" << tid << " : never existed";
        return;
    }
    const uint32_t given_ver = get_version(tid);
    bool matched = false;
    bool stop = false;
    bool interrupted = false;
    bool about_to_quit = false;
    void* (*fn)(void*) = NULL;
    void* arg = NULL;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bool has_tls = false;
    int64_t cpuwide_start_ns = 0;
    TaskStatistics stat = {0, 0};
    {
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            matched = true;
            stop = m->stop;
            interrupted = m->interrupted;
            about_to_quit = m->about_to_quit;
            fn = m->fn;
            arg = m->arg;
            attr = m->attr;
            has_tls = m->local_storage.keytable;
            cpuwide_start_ns = m->cpuwide_start_ns;
            stat = m->stat;
        }
    }
    if (!matched) {
        os << "bthread=" << tid << " : not exist now";
    } else {
        os << "bthread=" << tid << " :\nstop=" << stop
           << "\ninterrupted=" << interrupted
           << "\nabout_to_quit=" << about_to_quit
           << "\nfn=" << (void*)fn
           << "\narg=" << (void*)arg
           << "\nattr={stack_type=" << attr.stack_type
           << " flags=" << attr.flags
           << " keytable_pool=" << attr.keytable_pool
           << "}\nhas_tls=" << has_tls
           << "\nuptime_ns=" << butil::cpuwide_time_ns() - cpuwide_start_ns
           << "\ncputime_ns=" << stat.cputime_ns
           << "\nnswitch=" << stat.nswitch;
    }
}

}  // namespace bthread
