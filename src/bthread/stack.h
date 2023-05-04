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

// Date: Sun Sep  7 22:37:39 CST 2014

#ifndef BTHREAD_ALLOCATE_STACK_H
#define BTHREAD_ALLOCATE_STACK_H

#include <assert.h>
#include <gflags/gflags.h>          // DECLARE_int32
#include "bthread/types.h"
#include "bthread/context.h"        // bthread_fcontext_t
#include "butil/object_pool.h"

namespace bthread {

struct StackStorage {
     int stacksize;
     int guardsize;
    // Assume stack grows upwards.
    // http://www.boost.org/doc/libs/1_55_0/libs/context/doc/html/context/stack.html
    void* bottom;
    unsigned valgrind_stack_id;

    // Clears all members.
    void zeroize() {
        stacksize = 0;
        guardsize = 0;
        bottom = NULL;
        valgrind_stack_id = 0;
    }
};
 
// Allocate a piece of stack.
int allocate_stack_storage(StackStorage* s, int stacksize, int guardsize);
// Deallocate a piece of stack. Parameters MUST be returned or set by the
// corresponding allocate_stack_storage() otherwise behavior is undefined.
void deallocate_stack_storage(StackStorage* s);



//      栈类型	                说明	                            大小
//  STACK_TYPE_MAIN	    worker pthread的栈	                默认大小是8MB
//  STACK_TYPE_PTHREAD	使用worker pthread的栈，不需要额外分配	默认大小是8MB
//  STACK_TYPE_SMALL	小型栈	                            32KB
//  STACK_TYPE_NORMAL	默认栈	                            1MB
//  STACK_TYPE_LARGE	大型栈	                            8MB
//
// 调用 bthread_start_* 创建的 bthread 默认情况下栈大小是 1MB（STACK_TYPE_NORMAL），可通过 attr 参数来选择栈的大小。
// 如果分配栈空间失败，将其栈类型改为 STACK_TYPE_PTHREAD ，即直接在 worker pthread 的栈上运行该 bthread 。
// 另外，pthread task 的栈类型也是 STACK_TYPE_PTHREAD 。
//
// 注意，worker pthread 的栈不是 brpc 分配的，不能释放它。
// 至于 STACK_TYPE_MAIN，是为了告诉 get_stack 函数只需要分配栈控制结构，不需要分配栈空间，因为 worker pthread 已经由系统线程库分配了栈空间。
//
// 这里要注意，创建 bthread 时没有立刻为其分配栈，直到第一次运行时才会分配。
// 这个便于我们优化内存的使用，如果前一个 bthread 即将退出并且栈类型和下一个 bthread 相同，我们可以直接转移栈而不需要重新分配。
enum StackType {
    STACK_TYPE_MAIN = 0,
    STACK_TYPE_PTHREAD = BTHREAD_STACKTYPE_PTHREAD,
    STACK_TYPE_SMALL = BTHREAD_STACKTYPE_SMALL,
    STACK_TYPE_NORMAL = BTHREAD_STACKTYPE_NORMAL,
    STACK_TYPE_LARGE = BTHREAD_STACKTYPE_LARGE
};

struct ContextualStack {
    bthread_fcontext_t context;
    StackType stacktype;
    StackStorage storage;
};

// Get a stack in the `type' and run `entry' at the first time that the
// stack is jumped.
ContextualStack* get_stack(StackType type, void (*entry)(intptr_t));
// Recycle a stack. NULL does nothing.
void return_stack(ContextualStack*);

// Jump from stack `from' to stack `to'. `from' must be the stack of callsite
// (to save contexts before jumping)
//
//
//
void jump_stack(ContextualStack* from, ContextualStack* to);

}  // namespace bthread

#include "bthread/stack_inl.h"

#endif  // BTHREAD_ALLOCATE_STACK_H
