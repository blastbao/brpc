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

// Date 2014/09/24 19:34:24

#ifndef  BVAR_DETAIL__AGENT_GROUP_H
#define  BVAR_DETAIL__AGENT_GROUP_H

#include <pthread.h>                        // pthread_mutex_*
#include <stdlib.h>                         // abort

#include <new>                              // std::nothrow
#include <deque>                            // std::deque
#include <vector>                           // std::vector

#include "butil/errno.h"                     // errno
#include "butil/thread_local.h"              // thread_atexit
#include "butil/macros.h"                    // BAIDU_CACHELINE_ALIGNMENT
#include "butil/scoped_lock.h"
#include "butil/logging.h"

namespace bvar {
namespace detail {

typedef int AgentId;

// General NOTES:
// * Don't use bound-checking vector::at.
// * static functions in template class are not guaranteed to be inlined,
//   add inline keyword explicitly.
// * Put fast path in "if" branch, which is more cpu-wise.
// * don't use __builtin_expect excessively because CPU may predict the branch
//   better than you. Only hint branches that are definitely unusual.

// AgentGroup 通过 tls 数据实际保存了 bvar 在每个线程中的统计值。
template <typename Agent>
class AgentGroup {
public:
    typedef Agent   agent_type;

    // TODO: We should remove the template parameter and unify AgentGroup
    // of all bvar with a same one, to reuse the memory between different
    // type of bvar. The unified AgentGroup allocates small structs in-place
    // and large structs on heap, thus keeping batch efficiencies on small
    // structs and improving memory usage on large structs.
    const static size_t RAW_BLOCK_SIZE = 4096;
    const static size_t ELEMENTS_PER_BLOCK = (RAW_BLOCK_SIZE + sizeof(Agent) - 1) / sizeof(Agent);

    // The most generic method to allocate agents is to call ctor when
    // agent is needed, however we construct all agents when initializing
    // ThreadBlock, which has side effects:
    //  * calling ctor ELEMENTS_PER_BLOCK times is slower.
    //  * calling ctor of non-pod types may be unpredictably slow.
    //  * non-pod types may allocate space inside ctor excessively.
    //  * may return non-null for unexist id.
    //  * lifetime of agent is more complex. User has to reset the agent before
    //    destroying id otherwise when the agent is (implicitly) reused by
    //    another one who gets the reused id, things are screwed.
    // TODO(chenzhangyi01): To fix these problems, a method is to keep a bitmap
    // along with ThreadBlock* in _s_tls_blocks, each bit in the bitmap
    // represents that the agent is constructed or not. Drawback of this method
    // is that the bitmap may take 32bytes (for 256 agents, which is common) so
    // that addressing on _s_tls_blocks may be slower if identifiers distribute
    // sparsely. Another method is to put the bitmap in ThreadBlock. But this
    // makes alignment of ThreadBlock harder and to address the agent we have
    // to touch an additional cacheline: the bitmap. Whereas in the first
    // method, bitmap and ThreadBlock* are in one cacheline.
    struct BAIDU_CACHELINE_ALIGNMENT ThreadBlock {
        inline Agent* at(size_t offset) {
            return _agents + offset; // 取指定偏移位置的数组元素
        };
    private:
        Agent _agents[ELEMENTS_PER_BLOCK];
    };

    // 新建 agent 返回其 id ，这个函数由 combiner 的构造函数调用。
    inline static AgentId create_new_agent() {
        // 加锁
        BAIDU_SCOPED_LOCK(_s_mutex);
        // 判断是否有空闲的id，有就直接使用
        AgentId agent_id = 0;
        if (!_get_free_ids().empty()) {
            agent_id = _get_free_ids().back();
            _get_free_ids().pop_back();
        // 否则返回 _s_agent_kinds 的值并对 _s_agent_kinds 自增，也就是新建了一个 id
        } else {
            agent_id = _s_agent_kinds++;
        }
        // 返回 id
        return agent_id;
    }

    // 销毁 agent ，也可以理解成归还，并没有 delete 建立的对象，后续还可以重用.
    // combiner 的析构函数会调用，比如一个 bvar 析构了，对应的 combiner 析构了也就会归还 agentid 。
    inline static int destroy_agent(AgentId id) {
        // TODO: How to avoid double free?
        BAIDU_SCOPED_LOCK(_s_mutex);
        if (id < 0 || id >= _s_agent_kinds) {
            errno = EINVAL;
            return -1;
        }
        _get_free_ids().push_back(id);
        return 0;
    }


    // Note: May return non-null for unexist id, see notes on ThreadBlock
    // We need this function to be as fast as possible.
    //
    // 注意：不存在的 id 可能返回非 null ，是因为 id 和对应的数据是可以重用的。
    // 比如 id=10 的归还了，但因为 10 所指向的数据块还在，这个时候用如果用 10 来取仍然能取到，不过实际使用中没啥影响。
    //
    // 根据 agent id 拿到 agent 指针，如果 block 还没分配直接返回 NULL 。
    inline static Agent* get_tls_agent(AgentId id) {
        if (__builtin_expect(id >= 0, 1)) {
            if (_s_tls_blocks) {
                // 定位到 block
                const size_t block_id = (size_t)id / ELEMENTS_PER_BLOCK;
                if (block_id < _s_tls_blocks->size()) {
                    ThreadBlock* const tb = (*_s_tls_blocks)[block_id];
                    if (tb) {
                        // 根据块内偏移定位到 agent
                        return tb->at(id - block_id * ELEMENTS_PER_BLOCK);
                    }
                }
            }
        }
        return NULL;
    }

    // Note: May return non-null for unexist id, see notes on ThreadBlock
    //
    // 根据 agent id 拿到 agent 指针，如果 block 还没分配会进行分配。
    // 本函数和 get_tls_agent 的区别在于多了 _s_tls_blocks 为 null 和 (*_s_tls_blocks)[block_id] 为 null 的时候的空间分配，也就是 or_create 的含义。
    // 这样做是为了让 get_tls_agent 的部分实现尽可能的快。
    //
    // Combiner 在调用的时候会先调 AgentGroup::get_tls_agent(_id)，如果为 null 再调 AgentGroup::get_or_create_tls_agent(_id) 。
    // 这样，实际使用中 get_tls_agent 返回 NULL 的占比很小，精简实现能够优化性能。
    inline static Agent* get_or_create_tls_agent(AgentId id) {
        if (__builtin_expect(id < 0, 0)) {
            CHECK(false) << "Invalid id=" << id;
            return NULL;
        }

        // 检查当前 tls 中是否已经存在一个 _s_tls_blocks 的静态变量，如果不存在则新建一个空的 std::vector<ThreadBlock *> 对象，
        // 并注册一个退出回调函数 _destroy_tls_blocks 以便在线程退出时释放资源。
        if (_s_tls_blocks == NULL) {
            _s_tls_blocks = new (std::nothrow) std::vector<ThreadBlock *>;
            if (__builtin_expect(_s_tls_blocks == NULL, 0)) {
                LOG(FATAL) << "Fail to create vector, " << berror();
                return NULL;
            }
            butil::thread_atexit(_destroy_tls_blocks);
        }

        // 计算所需的 ThreadBlock 索引并检查该 block 是否已分配，如果未分配则新建一个 ThreadBlock 对象，并将其添加到 _s_tls_blocks 中对应的位置中。
        const size_t block_id = (size_t)id / ELEMENTS_PER_BLOCK; 
        if (block_id >= _s_tls_blocks->size()) {
            // The 32ul avoid pointless small resizes.
            _s_tls_blocks->resize(std::max(block_id + 1, 32ul));
        }
        ThreadBlock* tb = (*_s_tls_blocks)[block_id];
        if (tb == NULL) {
            ThreadBlock *new_block = new (std::nothrow) ThreadBlock;
            if (__builtin_expect(new_block == NULL, 0)) {
                return NULL;
            }
            tb = new_block;
            (*_s_tls_blocks)[block_id] = new_block;
        }

        // 最后，返回指定 AgentId 对应的 Agent 实例。
        return tb->at(id - block_id * ELEMENTS_PER_BLOCK);
    }

private:
    // 析构 _s_tls_blocks 里的元素并 delete _s_tls_blocks 本身，用于线程退出的时候清除 tls 存储。
    static void _destroy_tls_blocks() {
        if (!_s_tls_blocks) {
            return;
        }
        for (size_t i = 0; i < _s_tls_blocks->size(); ++i) {
            delete (*_s_tls_blocks)[i];
        }
        delete _s_tls_blocks;
        _s_tls_blocks = NULL;
    }
    // 获取空闲的已有 id ，用 deque 保存是因为它 resize 比较高效。
    inline static std::deque<AgentId> &_get_free_ids() {
        if (__builtin_expect(!_s_free_ids, 0)) {
            _s_free_ids = new (std::nothrow) std::deque<AgentId>();
            if (!_s_free_ids) {
                abort();
            }
        }
        return *_s_free_ids;
    }

    // AgentId 是 Agent 在当前线程上下文中的唯一标识符。
    // 每个线程上下文中都有多个 ThreadBlock 对象，每个 ThreadBlock 对象中包含固定数量的 Agent 对象(ELEMENTS_PER_BLOCK)。

    static pthread_mutex_t                      _s_mutex;           // 新建和销毁 agent 要用到的锁。
    static AgentId                              _s_agent_kinds;     // 当前 agent group（Agent参数相同）的 agent 数量，同时也用于构造 agentId 。
    static std::deque<AgentId>                  *_s_free_ids;       // deque 队列，保存了空闲的 agentId 用于再分配。
    static __thread std::vector<ThreadBlock *>  *_s_tls_blocks;     // tls 变量，是个 vector 指针，保存的是 ThreadBlock 指针，指向每个 thread 的 tls 数据块。
};

template <typename Agent>
pthread_mutex_t AgentGroup<Agent>::_s_mutex = PTHREAD_MUTEX_INITIALIZER;

template <typename Agent>
std::deque<AgentId>* AgentGroup<Agent>::_s_free_ids = NULL;

template <typename Agent>
AgentId AgentGroup<Agent>::_s_agent_kinds = 0;

template <typename Agent>
__thread std::vector<typename AgentGroup<Agent>::ThreadBlock *>
*AgentGroup<Agent>::_s_tls_blocks = NULL;

}  // namespace detail
}  // namespace bvar

#endif  //BVAR_DETAIL__AGENT_GROUP_H
