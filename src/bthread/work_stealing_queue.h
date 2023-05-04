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

#ifndef BTHREAD_WORK_STEALING_QUEUE_H
#define BTHREAD_WORK_STEALING_QUEUE_H

#include "butil/macros.h"
#include "butil/atomicops.h"
#include "butil/logging.h"

// WorkStealingQueue 是一种无锁队列，用做 bthread 中的任务队列。
// WorkStealingQueue 提供三种操作：push，pop，steal。
//
// 由于 push 和 pop 都是在本线程中完成的，因此 push 和 pop 操作不会并发，两个 push 和两个 pop 之间也不会并发；
// 但是 steal 是从其他线程发起的，因此会和 push 或 pop 并发。
//
// 队列 Queue 的头尾指针 _top 和 _bottom ，push 操作在 _bottom 侧执行，pop 操作在 _bottom 侧执行，steal 操作在 _top 侧执行。
//
// 由于对于此队列的 push 和 pop 操作发生在一次流程的两个阶段，所以不会产生竞争，push 和 steal 不需要对一个元素竞争，
// 那么竞争就发生在pop和steal以及steal和steal中。比如：当前只有一个bth在队列里，tg1想从rq里pop一个，tg2想从rq里steal一个。

// WorkStealingQueue 的 push 和 pop 都是在 bottom 一侧，而 steal 是在 top 一侧；
namespace bthread {

template <typename T>
class WorkStealingQueue {
public:

    WorkStealingQueue()
        : _bottom(1)
        , _capacity(0)
        , _buffer(NULL)
        , _top(1) {
    }

    ~WorkStealingQueue() {
        delete [] _buffer;
        _buffer = NULL;
    }

    int init(size_t capacity) {
        if (_capacity != 0) {
            LOG(ERROR) << "Already initialized";
            return -1;
        }
        if (capacity == 0) {
            LOG(ERROR) << "Invalid capacity=" << capacity;
            return -1;
        }
        if (capacity & (capacity - 1)) {
            LOG(ERROR) << "Invalid capacity=" << capacity
                       << " which must be power of 2";
            return -1;
        }
        _buffer = new(std::nothrow) T[capacity];
        if (NULL == _buffer) {
            return -1;
        }
        _capacity = capacity;
        return 0;
    }

    // Push an item into the queue.
    // Returns true on pushed.
    // May run in parallel with steal().
    // Never run in parallel with pop() or another push().
    bool push(const T& x) {
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        // 使用 acquire 是为保证这里读到的 t 一定是别的线程修改后或者修改前的值
        const size_t t = _top.load(butil::memory_order_acquire);
        if (b >= t + _capacity) { // Full queue.
            return false;
        }
        // 这里用了一个非常巧妙的 & 操作对下标做处理，使得取元素时下标一直小于 capacity（所以 _capacity 要求是偶数）
        _buffer[b & (_capacity - 1)] = x;
        // 这里 release 保证了写 _buffer 之后这个 b 是精确 +1 的，即对应了 steal 的 load acq
        _bottom.store(b + 1, butil::memory_order_release);
        return true;
    }

    // Pop an item from the queue.
    // Returns true on popped and the item is written to `val'.
    // May run in parallel with steal().
    // Never run in parallel with push() or another pop().
    //
    // pop 操作同样只和 steal 并发，核心逻辑是 b-1 之后锁定一个元素防止被 steal 取走，在只有一个元素的时候通过 CAS 语义和 steal 竞争。
    bool pop(T* val) {
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        size_t t = _top.load(butil::memory_order_relaxed);
        if (t >= b) {
            // fast check since we call pop() in each sched.
            // Stale _top which is smaller should not enter this branch.
            return false;
        }
        // 先让 b-1 ，意思是假设我已经取了一个元素，那么我希望各 bthread 周知
        const size_t newb = b - 1;
        _bottom.store(newb, butil::memory_order_relaxed);
        // 这里的作用是 atomic_thread_fence 之前的所有操作会全局可见（从 cpu 的 store_buffer 至少刷新到 L3 cache 使多核可见）
        butil::atomic_thread_fence(butil::memory_order_seq_cst);
        // 保证当前 t 读的是最新值？如果不新，可能存在有线程已经 steal 走了但是我还取的旧值的情况
        t = _top.load(butil::memory_order_relaxed);
        // 再次判断，此时说明队列为空，b 回到原来的值
        if (t > newb) {
            _bottom.store(b, butil::memory_order_relaxed);
            return false;
        }
        *val = _buffer[newb & (_capacity - 1)];
        // 说明队列中还有元素，这时相当于已经用 new_b 锁住这个元素了，所以可以退出
        if (t != newb) {
            // 那就是 t < new_b，队列中元素不止一个，直接取出用就行
            return true;
        }
        // Single last element, compete with steal()
        //
        // 走到这里证明 t = new_b，证明只剩一个元素了，需要和 steal 竞争
        //  若 _top 等于 t ，则当前取线程取成功了，修改 _top 为 t+1 ，返回 true
        //  若 _top 不等于 t ，则证明该元素被别人抢走了，将 t 改为 _top ，返回 false
        const bool popped = _top.compare_exchange_strong(
            t, t + 1, butil::memory_order_seq_cst, butil::memory_order_relaxed);
        _bottom.store(b, butil::memory_order_relaxed);
        return popped;
    }

    // Steal one item from the queue.
    // Returns true on stolen.
    // May run in parallel with push() pop() or another steal().
    bool steal(T* val) {
        size_t t = _top.load(butil::memory_order_acquire);
        size_t b = _bottom.load(butil::memory_order_acquire);

        // 队列为空直接返回 false
        if (t >= b) {
            // Permit false negative for performance considerations.
            return false;
        }

        do {
            // 保证 b 读到的是最新的
            butil::atomic_thread_fence(butil::memory_order_seq_cst);
            b = _bottom.load(butil::memory_order_acquire);
            if (t >= b) {
                // 空队列返回 false
                return false;
            }
            // 取元素
            *val = _buffer[t & (_capacity - 1)];
        } while (!_top.compare_exchange_strong(t, t + 1,butil::memory_order_seq_cst,butil::memory_order_relaxed));
        // 上面这个 do-while 的语义是：
        //  若 _top 等于 t ，则当前线程取元素成功了，修改 _top 为 t+1 ，返回 true ，退出循环走下来；
        //  若 _top 不等于 t ，则证明该元素被别人抢走了，将 t 改为 _top ，返回 false ，继续进入 while 。
        // 其他 steal 线程同理，谁拿到谁就 t+1 ，然后告诉所有线程我取走这个元素了。


        return true;
    }

    size_t volatile_size() const {
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        const size_t t = _top.load(butil::memory_order_relaxed);
        return (b <= t ? 0 : (b - t));
    }

    size_t capacity() const { return _capacity; }

private:
    // Copying a concurrent structure makes no sense.
    DISALLOW_COPY_AND_ASSIGN(WorkStealingQueue);

    butil::atomic<size_t> _bottom;                          // 队列尾部，用于其他 worker 去 steal 或者自己 pop 出来用
    size_t _capacity;                                       // 队列容量
    T* _buffer;                                             // 实际存放 bthread 的内存数组（连续）
    BAIDU_CACHELINE_ALIGNMENT butil::atomic<size_t> _top;   // 队列头部，用于 worker 直接放入 bthread
};

}  // namespace bthread

#endif  // BTHREAD_WORK_STEALING_QUEUE_H
