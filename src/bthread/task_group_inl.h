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

#ifndef BTHREAD_TASK_GROUP_INL_H
#define BTHREAD_TASK_GROUP_INL_H

namespace bthread {

// Utilities to manipulate bthread_t
inline bthread_t make_tid(uint32_t version, butil::ResourceId<TaskMeta> slot) {
    return (((bthread_t)version) << 32) | (bthread_t)slot.value;
}

inline butil::ResourceId<TaskMeta> get_slot(bthread_t tid) {
    butil::ResourceId<TaskMeta> id = { (tid & 0xFFFFFFFFul) };
    return id;
}
inline uint32_t get_version(bthread_t tid) {
    return (uint32_t)((tid >> 32) & 0xFFFFFFFFul);
}

inline TaskMeta* TaskGroup::address_meta(bthread_t tid) {
    // TaskMeta * m = address_resource<TaskMeta>(get_slot(tid));
    // if (m != NULL && m->version == get_version(tid)) {
    //     return m;
    // }
    // return NULL;
    return address_resource(get_slot(tid));
}

inline void TaskGroup::exchange(TaskGroup** pg, bthread_t next_tid) {
    TaskGroup* g = *pg;
    if (g->is_current_pthread_task()) {
        return g->ready_to_run(next_tid);
    }
    ReadyToRunArgs args = { g->current_tid(), false };
    g->set_remained((g->current_task()->about_to_quit
                     ? ready_to_run_in_worker_ignoresignal
                     : ready_to_run_in_worker),
                    &args);
    TaskGroup::sched_to(pg, next_tid);
}




// sched_to(tid) 中首先通过 tid 拿到该 tid 对应的 TaskMeta ，TaskMeta 为一个 bthread 的 meta 信息，如执行函数、参数、local storage 等；
// 如果已经为该 meta 分配过栈，则调用 sched_to(next_meta)，该函数的主要逻辑为通过 jump_stack(cur_meta->stack, next_meta->stack) 跳转至next_meta；
// 否则需分配栈，并设置该栈的执行入口为 task_runner 函数，然后就切过去开始执行 task_runner 函数。

// 这段代码是 TaskGroup 类中的一个方法，用于将当前任务调度到另一个任务上。
// 具体来说，这个方法会根据指定的任务 ID，找到该任务对应的 TaskMeta 对象，并为其分配一个栈空间，然后将当前任务切换到该任务上执行。
//
// 下面是具体操作：
//  1. 通过地址计算获取到下一个任务的 TaskMeta 对象。
//  2. 判断下一个任务是否已经有栈空间（即是否已经执行过），如果下一个任务没有栈空间，为其分配一个新的栈空间。
//  3. 如果能够成功分配栈空间，就将该空间设置到 TaskMeta 的 stack 属性中；
//     否则，则将任务的执行方式设为 “在 pthread 中直接运行” ，并将栈空间设置为 TaskGroup 对象的主栈空间。
//  4. 调用 sched_to 方法，将当前任务切换到下一个任务上执行。
//
// 注意：
// 在分配协程栈时，TaskMeta 对象的属性 stack_type 表示要使用的栈类型。
// 如果 TaskMeta 对象对应的线程不能申请到 bthread 的栈，或者没有足够的内存可用，就会强制使用 pthread 栈。
// 在这种情况下，任务将在 pthread 线程中执行，而不是在 bthread 协程中执行。
//
// 在 brpc 框架中，sched_to 函数调用时会将当前协程状态保存后立即切换到目标协程执行。
// 因此，sched_to 函数并不会在目标协程执行完成后返回。
// 因为调度的不确定，sched_to 函数返回的时机并不确定，取决于目标协程的执行情况以及后续任务的安排。
inline void TaskGroup::sched_to(TaskGroup** pg, bthread_t next_tid) {

    // 根据传入的 next_tid 取出对应 bthread 的 meta 信息
    TaskMeta* next_meta = address_meta(next_tid);

    // 如果对应 meta 的 stack 为空，说明这是一个新建的 bthread，
    // 调用 get_stack 从一个 object pool 类型的资源池里取出 stack 对象赋给 bthread，object pool 继承自 resource pool。
    if (next_meta->stack == NULL) {
        ContextualStack* stk = get_stack(next_meta->stack_type(), task_runner);
        if (stk) {
            next_meta->set_stack(stk);
        } else {
            // stack_type is BTHREAD_STACKTYPE_PTHREAD or out of memory,
            // In latter case, attr is forced to be BTHREAD_STACKTYPE_PTHREAD.
            // This basically means that if we can't allocate stack, run
            // the task in pthread directly.
            next_meta->attr.stack_type = BTHREAD_STACKTYPE_PTHREAD;
            next_meta->set_stack((*pg)->_main_stack);
        }
    }

    // Update now_ns only when wait_task did yield.
    // 确保 stack 就绪后再调用内部的 TaskGroup::sched_to 的重载函数去执行切换操作。
    sched_to(pg, next_meta);
}

inline void TaskGroup::push_rq(bthread_t tid) {
    while (!_rq.push(tid)) {
        // Created too many bthreads: a promising approach is to insert the
        // task into another TaskGroup, but we don't use it because:
        // * There're already many bthreads to run, inserting the bthread
        //   into other TaskGroup does not help.
        // * Insertions into other TaskGroups perform worse when all workers
        //   are busy at creating bthreads (proved by test_input_messenger in
        //   brpc)
        flush_nosignal_tasks();
        LOG_EVERY_SECOND(ERROR) << "_rq is full, capacity=" << _rq.capacity();
        // TODO(gejun): May cause deadlock when all workers are spinning here.
        // A better solution is to pop and run existing bthreads, however which
        // make set_remained()-callbacks do context switches and need extensive
        // reviews on related code.
        ::usleep(1000);
    }
}

inline void TaskGroup::flush_nosignal_tasks_remote() {
    if (_remote_num_nosignal) {
        _remote_rq._mutex.lock();
        flush_nosignal_tasks_remote_locked(_remote_rq._mutex);
    }
}

}  // namespace bthread

#endif  // BTHREAD_TASK_GROUP_INL_H
