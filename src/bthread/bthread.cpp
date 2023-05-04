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

#include <gflags/gflags.h>
#include "butil/macros.h"                       // BAIDU_CASSERT
#include "butil/logging.h"
#include "bthread/task_group.h"                // TaskGroup
#include "bthread/task_control.h"              // TaskControl
#include "bthread/timer_thread.h"
#include "bthread/list_of_abafree_id.h"
#include "bthread/bthread.h"

// Q: 当前正在执行的 bthread 还没有执行完，会切换到下一个 bthread 上去吗？
// A:
//
//  只有在 bthread 内部回调函数中执行了 bthread 同步原语相关的操作，才会引发切换，比如 bthread_sleep 或者 bthread 的 mutex ，
//  或者执行了 channel 的异步 IO 等等。
//  正常纯计算逻辑，是不会被切换的。
//
//  只有被挂起的时候才会切换，一般就是因为 IO 。
//  比如在使用 brpc 的 channel 异步请求下游服务或 redis 的时候，在对方 response 返回之前，会切换 bthread 。
//  正常的如果在执行串行的计算逻辑不会切 bthread 。

// Q: 调用阻塞的 syscall 的时候，是怎么切换协程的呢?
// A: 不切，当前线程会被阻塞住。这也是为什么 golang 的协程更完善，从语言层面解决系统调用造成的阻塞。

//【Q1】如果在 callback 里阻塞整个 worker ，其他 worker 会偷过来运行，但是万一所有worker都被阻塞住，那就gg了。
//【Q2】如果在 callback 里发起 brpc ，只会阻塞当前 bthread ，底层的 worker 不受影响，他发现后就移出 rq ，
//  这时，这个 callback 可能一会儿在 worker1 里跑、一会儿在 worker2 里跑，所以如果用了 pthread 级别的变量，就会有逻辑错误。
//  在 bthread 中，尽量不要使用 pthread loca l数据，如果一定要使用，需要通过 pthread_key_create 和 pthread_getspecific（mempool.h）。
//【Q3】如果在 callback 里加锁后发起下游 rpc 请求，那么情况是线程 1 抢到 butex 发起 rpc 请求，它需要等 brpc 返回后才能解锁，
//  但是处理请求的返回是需要资源的，若资源都被占了就会死锁。

// 1、bthread 为啥会设计 rq、remote_rq 两个队列，一个不可以吗？
//  如果只有一个，非 worker 添加的 bthread 也要入 rq ，那么就变成多生产者了，当前的 WorkStealingQueue 是无法满足的，如果要支持多生产者势必会增加开销。
//
//  为什么 bthread 会设计这两个队列呢？主要原因有以下几点：
//  - 避免锁竞争
//      在并发编程中，锁竞争是一个普遍存在的问题，而锁的竞争会严重影响程序的性能。
//      由于 rq 队列只存储本地协程，所以在调度器内部对它进行操作时，并不需要获得任何锁。
//      而当需要从其它线程中传送协程时，bthread 就会将这些协程放入 remote_rq 中，然后唤醒调度器中的一个线程去处理这些协程。
//      这样做的好处是，在处理远程协程的时候，可以避免与本地协程对 rq 队列的访问产生竞争。
//
//  - 减少上下文切换
//      上下文切换是协程调度器必须要面对的一个问题。
//      如果在 rq 队列中既存储本地协程，又存储远程协程，那么当调度器在切换协程时，就需要遍历整个队列来找到下一个要执行的协程。
//      这样做会导致上下文切换时间过长，影响程序的性能。
//      如果将本地协程和远程协程分开存储，在调度器切换协程时就可以避免对远程协程的遍历，从而减少上下文切换的时间。
//
//  - 方便远程协程的管理
//      将本地协程和远程协程分开存储，也方便了对远程协程的管理。
//      bthread 中提供了一套完整的接口，用于传送、暂停、恢复和取消远程协程。
//      这些接口都是针对 remote_rq 队列中的协程设计的，所以使用起来更加方便和高效。
//
//  综上所述，bthread 之所以设计 rq 和 remote_rq 两个队列，是为了避免锁竞争、减少上下文切换和方便远程协程的管理。
//  这样做的好处是显而易见的，可以提高程序的性能和可维护性。
//
//
// 2、为啥 wait_task 唤醒后，要先去 remote_rq 里取 tid 执行呢，而不先从 rq 里取？
//  全局的 steal 优先 steal rq ，如果本地唤醒后不优先去 _remote_rq 里取的话 remote task 可能会长时间得不到调度。
//
// 3、bthread 在执行过程中需要创建另一个 bthread 时，会调用 TaskGroup::start_foreground() ，在 start_foreground() 内完成 bthread 2 的 TaskMeta 对象的创建，
// 并调用 sched_to() 让 worker 去执行 bthread 2 的任务函数，worker 在真正执行 bthread 2 的任务函数前会将 bthread 1 的 tid 重新压入 TaskGroup 的 rq 队尾，
// bthread 1 不久之后会再次被调度执行，但不一定在此 worker 内了。


// bthread 执行用户指定的 func 过程中，有以下两种情况：
//
//  - func 执行完毕：
//      此时，先去当前在跑的 TaskGroup 中的 _rq 中看是否有其他 bth ，如果有直接执行下一个，如果没有就去偷，偷不到最后会返回 phtread 的调度 bth ，也就是卡在 wait_task 处；
//  - func 中创建新 bth 或调用阻塞操作：
//      立即进入新 bth ，原 bth 加入 _rq 尾部（这个被迫中断的 bth 不久后会被运行，但不一定在原来的 pth 上执行，可能被偷）；

namespace bthread {

DEFINE_int32(bthread_concurrency, 8 + BTHREAD_EPOLL_THREAD_NUM,
             "Number of pthread workers");

DEFINE_int32(bthread_min_concurrency, 0,
            "Initial number of pthread workers which will be added on-demand."
            " The laziness is disabled when this value is non-positive,"
            " and workers will be created eagerly according to -bthread_concurrency and bthread_setconcurrency(). ");

static bool never_set_bthread_concurrency = true;

static bool validate_bthread_concurrency(const char*, int32_t val) {
    // bthread_setconcurrency sets the flag on success path which should
    // not be strictly in a validator. But it's OK for a int flag.
    return bthread_setconcurrency(val) == 0;
}
const int ALLOW_UNUSED register_FLAGS_bthread_concurrency = 
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_bthread_concurrency,
                                    validate_bthread_concurrency);

static bool validate_bthread_min_concurrency(const char*, int32_t val);

const int ALLOW_UNUSED register_FLAGS_bthread_min_concurrency =
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_bthread_min_concurrency,
                                    validate_bthread_min_concurrency);

BAIDU_CASSERT(sizeof(TaskControl*) == sizeof(butil::atomic<TaskControl*>), atomic_size_match);

pthread_mutex_t g_task_control_mutex = PTHREAD_MUTEX_INITIALIZER;
// Referenced in rpc, needs to be extern.
// Notice that we can't declare the variable as atomic<TaskControl*> which
// are not constructed before main().
TaskControl* g_task_control = NULL;

extern BAIDU_THREAD_LOCAL TaskGroup* tls_task_group;
extern void (*g_worker_startfn)();

inline TaskControl* get_task_control() {
    return g_task_control;
}

// 步骤:
//  首先判断 g_task_control 是否在全局范围内已被创建，存在直接返回；
//  加 pthread_mutex_t 类型锁，并且这里为了防止其他线程同时创建再次检测是否存在；
//  创建全局 TaskControl，并传入指定的 concurrency（实际上是 pthread）的数量，默认为 9（8 + 1 个 epoll 线程）个；
//  初始化 g_task_control 并返回。
inline TaskControl* get_or_new_task_control() {
    // 1. 全局变量 TC（g_task_control）初始化，原子变量
    butil::atomic<TaskControl*>* p = (butil::atomic<TaskControl*>*)&g_task_control;
    // 2.1 通过原子变量进行 load ，取出 TC 指针，如果不为空，直接返回
    TaskControl* c = p->load(butil::memory_order_consume);
    if (c != NULL) {
        return c;
    }
    // 2.2. 竞争加自旋锁，重复上一操作
    BAIDU_SCOPED_LOCK(g_task_control_mutex);  // 全局锁
    c = p->load(butil::memory_order_consume);
    if (c != NULL) {
        return c;
    }
    // 2. 走到这，说明TC确实为NULL，开始new一个
    c = new (std::nothrow) TaskControl;
    if (NULL == c) {
        return NULL;
    }
    // 3. 用并发度 concurrency 来初始化全局 TC
    int concurrency = FLAGS_bthread_min_concurrency > 0 ?
        FLAGS_bthread_min_concurrency :
        FLAGS_bthread_concurrency;
    if (c->init(concurrency) != 0) {
        LOG(ERROR) << "Fail to init g_task_control";
        delete c;
        return NULL;
    }
    // 4. 将全局 TC 存入原子变量中
    p->store(c, butil::memory_order_release);
    return c;
}

static bool validate_bthread_min_concurrency(const char*, int32_t val) {
    if (val <= 0) {
        return true;
    }
    if (val < BTHREAD_MIN_CONCURRENCY || val > FLAGS_bthread_concurrency) {
        return false;
    }
    TaskControl* c = get_task_control();
    if (!c) {
        return true;
    }
    BAIDU_SCOPED_LOCK(g_task_control_mutex);
    int concurrency = c->concurrency();
    if (val > concurrency) {
        int added = c->add_workers(val - concurrency);
        return added == (val - concurrency);
    } else {
        return true;
    }
}


// 这个 tls 变量表明当前线程所归属的 taskgroup ，如果为 null ，说明当前线程不是 bthread 。
__thread TaskGroup* tls_task_group_nosignal = NULL;

BUTIL_FORCE_INLINE int
start_from_non_worker(bthread_t* __restrict tid,
                      const bthread_attr_t* __restrict attr,
                      void * (*fn)(void*),
                      void* __restrict arg) {

    // 获取 TaskControl 全局单例
    TaskControl* c = get_or_new_task_control();
    if (NULL == c) {
        return ENOMEM;
    }

    if (attr != NULL && (attr->flags & BTHREAD_NOSIGNAL)) {
        // Remember the TaskGroup to insert NOSIGNAL tasks for 2 reasons:
        // 1. NOSIGNAL is often for creating many bthreads in batch,
        //    inserting into the same TaskGroup maximizes the batch.
        // 2. bthread_flush() needs to know which TaskGroup to flush.
        TaskGroup* g = tls_task_group_nosignal;
        if (NULL == g) {
            g = c->choose_one_group();
            tls_task_group_nosignal = g;
        }
        return g->start_background<true>(tid, attr, fn, arg);
    }

    // 随机选择一个 TaskGroup ，将当前协程加入其队列
    return c->choose_one_group()->start_background<true>(tid, attr, fn, arg);
}

struct TidTraits {
    static const size_t BLOCK_SIZE = 63;
    static const size_t MAX_ENTRIES = 65536;
    static const bthread_t ID_INIT;
    static bool exists(bthread_t id) { return bthread::TaskGroup::exists(id); }
};
const bthread_t TidTraits::ID_INIT = INVALID_BTHREAD;

typedef ListOfABAFreeId<bthread_t, TidTraits> TidList;

struct TidStopper {
    void operator()(bthread_t id) const { bthread_stop(id); }
};
struct TidJoiner {
    void operator()(bthread_t & id) const {
        bthread_join(id, NULL);
        id = INVALID_BTHREAD;
    }
};

}  // namespace bthread

extern "C" {

int bthread_start_urgent(bthread_t* __restrict tid,
                         const bthread_attr_t* __restrict attr,
                         void * (*fn)(void*),
                         void* __restrict arg) {

    // 取本线程的 tg
    bthread::TaskGroup* g = bthread::tls_task_group;
    // 如果不为 null，则表明当前就是 bthread 而且 tls_task_group 指明了对应的 TaskGroup，在对应 TaskGroup（worker）执行新 bthread 的启动即可。
    if (g) {
        // start from worker
        return bthread::TaskGroup::start_foreground(&g, tid, attr, fn, arg);
    }
    // 如果为 null，说明当前线程不是 bthread，此时会调用 start_from_non_worker 函数。
    // 该函数会调用 get_or_new_task_control 函数获取或者创建新的 TaskControl 单例。
    return bthread::start_from_non_worker(tid, attr, fn, arg);
}

// [重要]
//
// 如果创建这个 bthread 的调用者是外部的线程（非task_group），调用 start_from_non_worker 。
// start_from_non_worker 的工作就是从 task_control 随机选择一个 task_group 作为这个新 bthread 的归宿。
//
// 如果创建者是已经存在于 task_group 的 bthread ，则会以当前的 task_group 作为最终归宿。
//
// task_group 中的 start_background 做的工作就是为新的 bthread 创建资源 TaskMeta ，然后情况将其加入不同的对列：
//  - 情况 1 ：加入 remote_rq；
//  - 情况 2 ：加入 _rq 。
int bthread_start_background(bthread_t* __restrict tid,
                             const bthread_attr_t* __restrict attr,
                             void * (*fn)(void*),
                             void* __restrict arg) {

    // 取当前线程本地 task_group 变量
    bthread::TaskGroup* g = bthread::tls_task_group;

    // 若非空，意味着本线程 tg 有效，直接走 g->start_background()
    if (g) {
        // start from worker
        return g->start_background<false>(tid, attr, fn, arg);
    }

    // 否则，说明当前线程不是 bthread ，
    return bthread::start_from_non_worker(tid, attr, fn, arg);
}

void bthread_flush() {
    bthread::TaskGroup* g = bthread::tls_task_group;
    if (g) {
        return g->flush_nosignal_tasks();
    }
    g = bthread::tls_task_group_nosignal;
    if (g) {
        // NOSIGNAL tasks were created in this non-worker.
        bthread::tls_task_group_nosignal = NULL;
        return g->flush_nosignal_tasks_remote();
    }
}

int bthread_interrupt(bthread_t tid) {
    return bthread::TaskGroup::interrupt(tid, bthread::get_task_control());
}

int bthread_stop(bthread_t tid) {
    bthread::TaskGroup::set_stopped(tid);
    return bthread_interrupt(tid);
}

int bthread_stopped(bthread_t tid) {
    return (int)bthread::TaskGroup::is_stopped(tid);
}

bthread_t bthread_self(void) {
    bthread::TaskGroup* g = bthread::tls_task_group;
    // note: return 0 for main tasks now, which include main thread and
    // all work threads. So that we can identify main tasks from logs
    // more easily. This is probably questionable in future.
    if (g != NULL && !g->is_current_main_task()/*note*/) {
        return g->current_tid();
    }
    return INVALID_BTHREAD;
}

int bthread_equal(bthread_t t1, bthread_t t2) {
    return t1 == t2;
}

void bthread_exit(void* retval) {
    bthread::TaskGroup* g = bthread::tls_task_group;
    if (g != NULL && !g->is_current_main_task()) {
        throw bthread::ExitException(retval);
    } else {
        pthread_exit(retval);
    }
}

int bthread_join(bthread_t tid, void** thread_return) {
    return bthread::TaskGroup::join(tid, thread_return);
}

int bthread_attr_init(bthread_attr_t* a) {
    *a = BTHREAD_ATTR_NORMAL;
    return 0;
}

int bthread_attr_destroy(bthread_attr_t*) {
    return 0;
}

int bthread_getattr(bthread_t tid, bthread_attr_t* attr) {
    return bthread::TaskGroup::get_attr(tid, attr);
}

int bthread_getconcurrency(void) {
    return bthread::FLAGS_bthread_concurrency;
}

int bthread_setconcurrency(int num) {
    if (num < BTHREAD_MIN_CONCURRENCY || num > BTHREAD_MAX_CONCURRENCY) {
        LOG(ERROR) << "Invalid concurrency=" << num;
        return EINVAL;
    }
    if (bthread::FLAGS_bthread_min_concurrency > 0) {
        if (num < bthread::FLAGS_bthread_min_concurrency) {
            return EINVAL;
        }
        if (bthread::never_set_bthread_concurrency) {
            bthread::never_set_bthread_concurrency = false;
        }
        bthread::FLAGS_bthread_concurrency = num;
        return 0;
    }
    bthread::TaskControl* c = bthread::get_task_control();
    if (c != NULL) {
        if (num < c->concurrency()) {
            return EPERM;
        } else if (num == c->concurrency()) {
            return 0;
        }
    }
    BAIDU_SCOPED_LOCK(bthread::g_task_control_mutex);
    c = bthread::get_task_control();
    if (c == NULL) {
        if (bthread::never_set_bthread_concurrency) {
            bthread::never_set_bthread_concurrency = false;
            bthread::FLAGS_bthread_concurrency = num;
        } else if (num > bthread::FLAGS_bthread_concurrency) {
            bthread::FLAGS_bthread_concurrency = num;
        }
        return 0;
    }
    if (bthread::FLAGS_bthread_concurrency != c->concurrency()) {
        LOG(ERROR) << "CHECK failed: bthread_concurrency="
                   << bthread::FLAGS_bthread_concurrency
                   << " != tc_concurrency=" << c->concurrency();
        bthread::FLAGS_bthread_concurrency = c->concurrency();
    }
    if (num > bthread::FLAGS_bthread_concurrency) {
        // Create more workers if needed.
        bthread::FLAGS_bthread_concurrency +=
            c->add_workers(num - bthread::FLAGS_bthread_concurrency);
        return 0;
    }
    return (num == bthread::FLAGS_bthread_concurrency ? 0 : EPERM);
}

int bthread_about_to_quit() {
    bthread::TaskGroup* g = bthread::tls_task_group;
    if (g != NULL) {
        bthread::TaskMeta* current_task = g->current_task();
        if(!(current_task->attr.flags & BTHREAD_NEVER_QUIT)) {
            current_task->about_to_quit = true;
        }
        return 0;
    }
    return EPERM;
}

int bthread_timer_add(bthread_timer_t* id, timespec abstime,
                      void (*on_timer)(void*), void* arg) {
    bthread::TaskControl* c = bthread::get_or_new_task_control();
    if (c == NULL) {
        return ENOMEM;
    }
    bthread::TimerThread* tt = bthread::get_or_create_global_timer_thread();
    if (tt == NULL) {
        return ENOMEM;
    }
    bthread_timer_t tmp = tt->schedule(on_timer, arg, abstime);
    if (tmp != 0) {
        *id = tmp;
        return 0;
    }
    return ESTOP;
}

int bthread_timer_del(bthread_timer_t id) {
    bthread::TaskControl* c = bthread::get_task_control();
    if (c != NULL) {
        bthread::TimerThread* tt = bthread::get_global_timer_thread();
        if (tt == NULL) {
            return EINVAL;
        }
        const int state = tt->unschedule(id);
        if (state >= 0) {
            return state;
        }
    }
    return EINVAL;
}

int bthread_usleep(uint64_t microseconds) {
    // 取当前线程关联的 tg
    bthread::TaskGroup* tg = bthread::tls_task_group;
    // 如果非空且非 pthread 模式，就调用 usleep
    if (NULL != tg && !tg->is_current_pthread_task()) {
        return bthread::TaskGroup::usleep(&tg, microseconds);
    }
    // 否则，调用 os usleep
    return ::usleep(microseconds);
}

int bthread_yield(void) {
    bthread::TaskGroup* g = bthread::tls_task_group;
    if (NULL != g && !g->is_current_pthread_task()) {
        bthread::TaskGroup::yield(&g);
        return 0;
    }
    // pthread_yield is not available on MAC
    return sched_yield();
}

int bthread_set_worker_startfn(void (*start_fn)()) {
    if (start_fn == NULL) {
        return EINVAL;
    }
    bthread::g_worker_startfn = start_fn;
    return 0;
}

void bthread_stop_world() {
    bthread::TaskControl* c = bthread::get_task_control();
    if (c != NULL) {
        c->stop_and_join();
    }
}

int bthread_list_init(bthread_list_t* list,
                      unsigned /*size*/,
                      unsigned /*conflict_size*/) {
    list->impl = new (std::nothrow) bthread::TidList;
    if (NULL == list->impl) {
        return ENOMEM;
    }
    // Set unused fields to zero as well.
    list->head = 0;
    list->size = 0;
    list->conflict_head = 0;
    list->conflict_size = 0;
    return 0;
}

void bthread_list_destroy(bthread_list_t* list) {
    delete static_cast<bthread::TidList*>(list->impl);
    list->impl = NULL;
}

int bthread_list_add(bthread_list_t* list, bthread_t id) {
    if (list->impl == NULL) {
        return EINVAL;
    }
    return static_cast<bthread::TidList*>(list->impl)->add(id);
}

int bthread_list_stop(bthread_list_t* list) {
    if (list->impl == NULL) {
        return EINVAL;
    }
    static_cast<bthread::TidList*>(list->impl)->apply(bthread::TidStopper());
    return 0;
}

int bthread_list_join(bthread_list_t* list) {
    if (list->impl == NULL) {
        return EINVAL;
    }
    static_cast<bthread::TidList*>(list->impl)->apply(bthread::TidJoiner());
    return 0;
}
    
}  // extern "C"
