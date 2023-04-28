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

// Date: Sun, 22 Jan 2017

#ifndef BTHREAD_REMOTE_TASK_QUEUE_H
#define BTHREAD_REMOTE_TASK_QUEUE_H

#include "butil/containers/bounded_queue.h"
#include "butil/macros.h"


// 在 brpc 中，每个工作线程维护一个 RemoteTaskQueue ，非工作线程没有自己的任务队列，因此非工作线程需要通过工作线程提交任务。
// 由于非工作线程随机选择一个工作线程的 RemoteTaskQueue 提交任务，本身就降低了访问同一个 RemoteTaskQueue 的冲突，
// 因此 RemoteTaskQueue 的实现比较简单，内置一把锁用于同步，支持 push 和 pop 两种操作。
namespace bthread {

class TaskGroup;

// A queue for storing bthreads created by non-workers. Since non-workers
// randomly choose a TaskGroup to push which distributes the contentions,
// this queue is simply implemented as a queue protected with a lock.
// The function names should be self-explanatory.
class RemoteTaskQueue {
public:
    RemoteTaskQueue() {}

    int init(size_t cap) {
        const size_t memsize = sizeof(bthread_t) * cap;
        void* q_mem = malloc(memsize);
        if (q_mem == NULL) {
            return -1;
        }
        butil::BoundedQueue<bthread_t> q(q_mem, memsize, butil::OWNS_STORAGE);
        _tasks.swap(q);
        return 0;
    }

    bool pop(bthread_t* task) {
        if (_tasks.empty()) {
            return false;
        }
        _mutex.lock();
        const bool result = _tasks.pop(task);
        _mutex.unlock();
        return result;
    }

    bool push(bthread_t task) {
        _mutex.lock();
        const bool res = push_locked(task);
        _mutex.unlock();
        return res;
    }

    bool push_locked(bthread_t task) {
        return _tasks.push(task);
    }

    size_t capacity() const { return _tasks.capacity(); }
    
private:
friend class TaskGroup;
    DISALLOW_COPY_AND_ASSIGN(RemoteTaskQueue);
    butil::BoundedQueue<bthread_t> _tasks;
    butil::Mutex _mutex;
};

}  // namespace bthread

#endif  // BTHREAD_REMOTE_TASK_QUEUE_H
