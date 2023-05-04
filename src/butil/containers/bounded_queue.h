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

// Date: Sat Aug 18 12:42:16 CST 2012

// A thread-unsafe bounded queue(ring buffer). It can push/pop from both
// sides and is more handy than thread-safe queues in single thread. Use
// boost::lockfree::spsc_queue or boost::lockfree::queue in multi-threaded
// scenarios.

#ifndef BUTIL_BOUNDED_QUEUE_H
#define BUTIL_BOUNDED_QUEUE_H

#include "butil/macros.h"
#include "butil/logging.h"

namespace butil {

// [Create a on-stack small queue]
//   char storage[64];
//   butil::BoundedQueue<int> q(storage, sizeof(storage), butil::NOT_OWN_STORAGE);
//   q.push(1);
//   q.push(2);
//   ...
   
// [Initialize a class-member queue]
//   class Foo {
//     ...
//     BoundQueue<int> _queue;
//   };
//   int Foo::init() {
//     BoundedQueue<int> tmp(capacity);
//     if (!tmp.initialized()) {
//       LOG(ERROR) << "Fail to create _queue";
//       return -1;
//     }
//     tmp.swap(_queue);
//   }



// BoundedQueue 是一个线程不安全的有界队列。
//
// 什么是有界队列？所谓有界队列表示的就是一个队列其中的容量是有限的（固定的），不能动态扩容的队列。
//
// 这种听起来没有 vector 那种自动扩容能力的容器，主要还是全面为了性能考虑的。
// 一般也是用作生产者和消费者模式，当队列容量已满的时候，一般就表示超过了这个队列的最大吞吐能力，故而拒绝加入新的任务。
//
// 在实践中有界队列一般是基于 ring buffer（环形缓冲区）来实现的，或者说有界队列就是 ring buffer 的别名也不为过。
// ring buffer 也被称为 circular buffer 或 circular queue 。
// 整体思路就是用一段固定大小的线性连续空间来模拟循环的队列。
//
// ring buffer 的定义很简单，实现细节上有多种不同的方案，比如存储读指针和写指针，标记下一次 pop 和 push 的位置。
// 这种实现的麻烦之处是对于队列的满或者空是有歧义的。因为队列满和队列空的时候读写两个指针都是指向相同位置，要区分具体是满还是空，就要额外的操作。
// 比如总保持队列中有一个元素是不可写的，此时如果读写指针指向同一位置，则缓冲区为空，如果读指针位于写指针的相邻后一个位置，则缓冲区为满。
//
// 当然还有其他实现，比如不存储写指针，而是存储队列中写入的元素个数。每次写入的时候，通过读指针加上元素个数（需要取模）来计算出写指针的位置。
// brpc 的实现就是这种方案。


// 有界队列持有数据存储的所有权，NOT_OWN_STORAGE 表示没有。
enum StorageOwnership { OWNS_STORAGE, NOT_OWN_STORAGE };

template <typename T>
class BoundedQueue {
public:

    // You have to pass the memory for storing items at creation.
    // The queue contains at most memsize/sizeof(T) items.
    BoundedQueue(void* mem, size_t memsize, StorageOwnership ownership)
        : _count(0)
        , _cap(memsize / sizeof(T))
        , _start(0)
        , _ownership(ownership)
        , _items(mem) {
        DCHECK(_items);
    };
    
    // Construct a queue with the given capacity.
    // The malloc() may fail silently, call initialized() to test validity
    // of the queue.
    explicit BoundedQueue(size_t capacity)
        : _count(0)
        , _cap(capacity)
        , _start(0)
        , _ownership(OWNS_STORAGE)
        , _items(malloc(capacity * sizeof(T))) {
        DCHECK(_items);
    };
    
    BoundedQueue()
        : _count(0)
        , _cap(0)
        , _start(0)
        , _ownership(NOT_OWN_STORAGE)
        , _items(NULL) {
    };


    // 首先调用 clear() 函数，然后需要对数据存储的所有权做一个判断，只有明确是自己持有的情况下，采取 free() 释放掉内存，和 malloc() 成对。
    // 而对于非自己持有的，就不操作了。
    ~BoundedQueue() {
        clear();
        if (_ownership == OWNS_STORAGE) {
            free(_items);
            _items = NULL;
        }
    }

    // Push |item| into bottom side of this queue.
    // Returns true on success, false if queue is full.
    bool push(const T& item) {
        if (_count < _cap) {
            new ((T*)_items + _mod(_start + _count, _cap)) T(item);
            ++_count;
            return true;
        }
        return false;
    }

    // Push |item| into bottom side of this queue. If the queue is full,
    // pop topmost item first.
    //
    void elim_push(const T& item) {
        if (_count < _cap) {
            new ((T*)_items + _mod(_start + _count, _cap)) T(item);
            ++_count;
        } else {
            ((T*)_items)[_start] = item;
            _start = _mod(_start + 1, _cap);
        }
    }
    
    // Push a default-constructed item into bottom side of this queue
    // Returns address of the item inside this queue
    //
    // 如果队列没满，则使用 placement new 进行构造。
    //
    // 常规的 new 是不需要自己指定对象分配的堆地址的，而 placement new 则可以在指定的内存位置上构造对象。
    // 这里内存位置通过起始位置+元素个数来定位到。
    T* push() {
        if (_count < _cap) {
            return new ((T*)_items + _mod(_start + _count++, _cap)) T();
        }
        return NULL;
    }

    // Push |item| into top side of this queue
    // Returns true on success, false if queue is full.
    bool push_top(const T& item) {
        if (_count < _cap) {
            _start = _start ? (_start - 1) : (_cap - 1);
            ++_count;
            new ((T*)_items + _start) T(item);
            return true;
        }
        return false;
    }    
    
    // Push a default-constructed item into top side of this queue
    // Returns address of the item inside this queue
    T* push_top() {
        if (_count < _cap) {
            _start = _start ? (_start - 1) : (_cap - 1);
            ++_count;
            return new ((T*)_items + _start) T();
        }
        return NULL;
    }
    
    // Pop top-most item from this queue
    // Returns true on success, false if queue is empty
    bool pop() {
        if (_count) {
            --_count;
            ((T*)_items + _start)->~T();
            _start = _mod(_start + 1, _cap);
            return true;
        }
        return false;
    }

    // Pop top-most item from this queue and copy into |item|.
    // Returns true on success, false if queue is empty
    //
    // 值得注意的是，pop() 和 push() 是对称的一对操作，但是他们的函数参数不同。
    // 在有参版的 push() 中，其参数是 const T& ，而 pop() 的参数是 T* 。
    //
    // [重要]
    // 好的 C++ 编码规范（比如谷歌编码规范）都指明，当函数参数做出参的时候用指针，而作为入参的时候使用 const & 。
    bool pop(T* item) {
        if (_count) {
            --_count;
            T* const p = (T*)_items + _start;
            *item = *p;
            p->~T();
            _start = _mod(_start + 1, _cap);
            return true;
        }
        return false;
    }

    // Pop bottom-most item from this queue
    // Returns true on success, false if queue is empty
    bool pop_bottom() {
        if (_count) {
            --_count;
            ((T*)_items + _mod(_start + _count, _cap))->~T();
            return true;
        }
        return false;
    }

    // Pop bottom-most item from this queue and copy into |item|.
    // Returns true on success, false if queue is empty
    bool pop_bottom(T* item) {
        if (_count) {
            --_count;
            T* const p = (T*)_items + _mod(_start + _count, _cap);
            *item = *p;
            p->~T();
            return true;
        }
        return false;
    }

    // Pop all items
    //
    // 遍历所有元素，取显式调用其析构函数：->~T() 。
    // 这个操作是由于我们的对象是通过 placement new 构造的，->~T() 就是其必须成对的析构操作。
    // 就像 new 必须有 delete 一般。但和 new 与 delete 不同的是，不管是 placement new 还是 ->~T() 都没有对本对象去做堆中内存的分配和释放。
    // 当然如果 T 中有成员，其实还是会涉及到堆内存分配和释放的，但那是针对其中的成员，并不是 T 本身。
    void clear() {
        for (uint32_t i = 0; i < _count; ++i) {
            ((T*)_items + _mod(_start + i, _cap))->~T();
        }
        _count = 0;
        _start = 0;
    }

    // Get address of top-most item, NULL if queue is empty
    T* top() { 
        return _count ? ((T*)_items + _start) : NULL; 
    }
    const T* top() const { 
        return _count ? ((const T*)_items + _start) : NULL; 
    }

    // Randomly access item from top side.
    // top(0) == top(), top(size()-1) == bottom()
    // Returns NULL if |index| is out of range.
    T* top(size_t index) {
        if (index < _count) {
            return (T*)_items + _mod(_start + index, _cap);
        }
        return NULL;   // including _count == 0
    }
    const T* top(size_t index) const {
        if (index < _count) {
            return (const T*)_items + _mod(_start + index, _cap);
        }
        return NULL;   // including _count == 0
    }

    // Get address of bottom-most item, NULL if queue is empty
    T* bottom() { 
        return _count ? ((T*)_items + _mod(_start + _count - 1, _cap)) : NULL; 
    }
    const T* bottom() const {
        return _count ? ((const T*)_items + _mod(_start + _count - 1, _cap)) : NULL; 
    }
    
    // Randomly access item from bottom side.
    // bottom(0) == bottom(), bottom(size()-1) == top()
    // Returns NULL if |index| is out of range.
    T* bottom(size_t index) {
        if (index < _count) {
            return (T*)_items + _mod(_start + _count - index - 1, _cap);
        }
        return NULL;  // including _count == 0
    }
    const T* bottom(size_t index) const {
        if (index < _count) {
            return (const T*)_items + _mod(_start + _count - index - 1, _cap);
        }
        return NULL;  // including _count == 0
    }

    bool empty() const { return !_count; }
    bool full() const { return _cap == _count; }

    // Number of items
    size_t size() const { return _count; }

    // Maximum number of items that can be in this queue
    size_t capacity() const { return _cap; }

    // Maximum value of capacity()
    //
    // 这个函数的内容其实是编译期间就能确定的，也就是说返回的是固定值。
    // sizeof(_cap) 返回的是 _cap 的字节数，乘以 8 则是因为一个字节（byte）等于 8 个位（bit）。
    // 所以 sizeof(_cap) * 8) 表示的就是 uint32_t 类型所占的 bit 数，1UL << (sizeof(_cap) * 8)) - 1 的结果就是 uint32_t 的每一位都是 1 ，
    // 也就是 uint32_t 所能表示的最大值。
    //
    // 这个函数应该是实现的类似 vector 的 max_size() 接口，只不过这里命名成了 max_capacity() 。
    // vector 中的 max_size() 表示的是 vector 能存储的最大元素个数，有这个函数的原因是因为 vector 是能够动态扩容的。
    // 而有界队列，其实是不存在动态扩容的，所以这个max_capacity()其实也没太大用。
    size_t max_capacity() const { return (1UL << (sizeof(_cap) * 8)) - 1; }

    // True if the queue was constructed successfully.
    //
    // 检查队列是否已经初始化
    bool initialized() const { return _items != NULL; }

    // Swap internal fields with another queue.
    void swap(BoundedQueue& rhs) {
        std::swap(_count, rhs._count);
        std::swap(_cap, rhs._cap);
        std::swap(_start, rhs._start);
        std::swap(_ownership, rhs._ownership);
        std::swap(_items, rhs._items);
    }

private:
    // Since the space is possibly not owned, we disable copying.
    DISALLOW_COPY_AND_ASSIGN(BoundedQueue);
    
    // This is faster than % in this queue because most |off| are smaller
    // than |cap|. This is probably not true in other place, be careful
    // before you use this trick.
    //
    // 这个模运算使用了一个比较 Trick 的技巧，他的限制是 off 永远能保证在 [0, 2*cap) 之间。
    static uint32_t _mod(uint32_t off, uint32_t cap) {
        while (off >= cap) {
            off -= cap;
        }
        return off;
    }
    
    uint32_t _count;    // 当前元素个数
    uint32_t _cap;      // 容量
    uint32_t _start;    // 起始位置
    StorageOwnership _ownership; // 数据所有权，表示有界队列是否持有该数据
    void* _items;       // 数据指针
};

}  // namespace butil

#endif  // BUTIL_BOUNDED_QUEUE_H
