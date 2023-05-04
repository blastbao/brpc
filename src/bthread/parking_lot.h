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

// Date: 2017/07/27 23:07:06

#ifndef BTHREAD_PARKING_LOT_H
#define BTHREAD_PARKING_LOT_H

#include "butil/atomicops.h"
#include "bthread/sys_futex.h"



// ParkingLot 有四个类函数：
//  - signal 是唤醒 num_task 个等待在 _pending_signal 上的线程
//  - wait 是如果 _pending_signal 的当前值和先前拿到的 expected_state.val 相等的话就 wait
//  - stop 则是将停止的标识位置为 1，然后唤醒所有 wait 的线程，这里的 stop 指的就是 wait 的 stop
//  - get_state 是获取用于 wait 的状态，就是直接返回 _pending_signal 的值

// ParkingLot 的等待与唤醒都是利用系统调用 futex_wait 和 futex_wake 来实现。
// futex 的优势在于 wait 时会先判断当前的 pending_signal 是否和 expected_state 的值相等，如果不相等则直接返回，不必进入内核态，减少开销。
// 具体原理参考 futex — Linux manual page 。
namespace bthread {

// Park idle workers.
class BAIDU_CACHELINE_ALIGNMENT ParkingLot {
public:

    // 状态：封装了一个 int ，如果最低位是 1 则为 stopped 。
    class State {
    public:
        State(): val(0) {}
        bool stopped() const { return val & 1; }
    private:
    friend class ParkingLot;
        State(int val) : val(val) {}
        int val;
    };

    ParkingLot() : _pending_signal(0) {}

    // Wake up at most `num_task' workers.
    // Returns #workers woken up.
    //
    // 唤醒最多 num_task 个 worker ，返回唤醒的 worker 数目。
    //
    // 先给 _pending_signal 加上 num_task <<1（即 num_task*2 ）。
    // 这里之所以累加的数字，要经过左移操作，其目的只是为了让其成为偶数。
    // 为什么这里需要一个偶数呢？
    //
    // futex_wake_private 是对系统调用 futex 的封装。
    //
    // [唤醒]
    int signal(int num_task) {
        // 增加数值，具体增加多少并不重要，只是传递这个变化事件。
        _pending_signal.fetch_add((num_task << 1), butil::memory_order_release);
        // 唤醒最多 num_task 个 pthread 。
        return futex_wake_private(&_pending_signal, num_task);
    }

    // Get a state for later wait().
    State get_state() {
        return _pending_signal.load(butil::memory_order_acquire);
    }

    // Wait for tasks.
    // If the `expected_state' does not match, wait() may finish directly.
    //
    // [阻塞]
    //
    // 此函数只在 main_task->wait_task 中调用。
    //
    // wait 阻塞在 &_pending_signal 这里，而 expected_state 实际传入的是 _last_pl_state ，所以 wait 操作其预期值也便是 _last_pl_state.val 。
    // 如果 &_pending_signal 存储的值和 _last_pl_state.val 相同则阻塞（也就是说还没有任务出现），否则解除阻塞并返回。
    //
    // 也就是说， _pending_signal 的值和 expected_state.val 相等则阻塞，不等则返回。
    void wait(const State& expected_state) {
        futex_wait_private(&_pending_signal, expected_state.val, NULL);
    }

    // Wakeup suspended wait() and make them unwaitable ever.
    //
    // [唤醒]
    void stop() {
        // 最低位置为 1
        _pending_signal.fetch_or(1);
        // 唤醒所有 pthread
        futex_wake_private(&_pending_signal, 10000);
    }

private:

    // higher 31 bits for signalling, LSB for stopping.
    // 高 31 位存储信号、最低位存储 stop 标记。
    //
    // _pending_signal 的值其实并不表示任务的个数，尽管来任务来临时，它会做一次加法，但加的并不是任务数，并且在任务被消费后不会做减法。
    // _pending_signal 的值是没有具体意义的，其变化仅仅是一种状态 “同步” 的媒介。
    butil::atomic<int> _pending_signal;
};

}  // namespace bthread

#endif  // BTHREAD_PARKING_LOT_H
