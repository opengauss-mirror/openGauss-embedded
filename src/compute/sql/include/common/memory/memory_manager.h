/*
 * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
 *
 * openGauss embedded is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * memory_manager.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/memory/memory_manager.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace intarkdb {

typedef int ConnectionId;

struct ConnectionState {
    uint64_t memory_used{0};
};

class MemoryManager {
   public:
    // not copyable and not movable
    MemoryManager(const MemoryManager&) = delete;
    MemoryManager& operator=(const MemoryManager&) = delete;
    MemoryManager(MemoryManager&&) = delete;
    MemoryManager& operator=(MemoryManager&&) = delete;

    static MemoryManager* GetInstance(uint64_t memory_limit) {
        std::call_once(flag_, [memory_limit] { memory_manager_ = new MemoryManager(memory_limit); });
        return memory_manager_;
    }

    // use default memory limit
    static MemoryManager* GetInstance() {
        if (memory_manager_ == nullptr) {
            // 对于不启动数据库，但使用SQL引擎的情况, 默认无内存限制
#ifdef _MSC_VER
            return GetInstance(LLONG_MAX);
#else
            return GetInstance(std::numeric_limits<int64_t>::max());
#endif
        }
        return memory_manager_;
    }

    bool ApplyMemory(int id, uint64_t memory_size) {
        std::lock_guard<std::mutex> lock(memory_used_mutex_);
        // 预留 reserve_memory_size 内存，避免单个连接耗尽内存限额
        if (memory_used_ + memory_size > memory_limit_ + reserve_memory_size_) {
            return false;
        }
        auto it = connection_states_map_.find(id);
        if (it == connection_states_map_.end()) {
            connection_states_map_[id] = ConnectionState{memory_size};
        } else {
            // 单个内存不超过 memory_limit_
            if (it->second.memory_used + memory_size > memory_limit_) {
                return false;
            }
            it->second.memory_used += memory_size;
        }
        memory_used_ += memory_size;
        return true;
    }

    void ReleaseMemory(int id, uint64_t memory_size) {
        std::lock_guard<std::mutex> lock(memory_used_mutex_);
        memory_used_ -= memory_size;
        auto it = connection_states_map_.find(id);
        if (it != connection_states_map_.end()) {
            it->second.memory_used -= memory_size;
        }
    }

    uint64_t UsedMemory() {
        std::lock_guard<std::mutex> lock(memory_used_mutex_);
        return memory_used_;
    }

    uint64_t MemoryLimit() { return memory_limit_; }

    uint64_t UsedMemory(ConnectionId id) {
        std::lock_guard<std::mutex> lock(memory_used_mutex_);
        auto it = connection_states_map_.find(id);
        return it == connection_states_map_.end() ? 0 : it->second.memory_used;
    }

   private:
    explicit MemoryManager(uint64_t memory_limit) : memory_limit_(memory_limit) {}

   private:
    static MemoryManager* memory_manager_;
    static std::once_flag flag_;
    static constexpr uint64_t reserve_memory_size_{100 * 1024 * 1024};  // 100 M reserved memory
    uint64_t memory_limit_{2 * 1024 * 1024};
    uint64_t memory_used_{0};
    std::mutex memory_used_mutex_;
    // std::unordered_map<ConnectionId, ConnectionState> connection_states_map_;
    std::unordered_map<int, ConnectionState> connection_states_map_;
};

// memmory hit the limit
class MemoryLimitException : public std::exception {
   public:
    const char* what() const noexcept override { return "Memory Limit Exceeded"; }
};

template <typename T>
class Allocator : public std::allocator<T> {
   public:
    template <typename U>
    struct rebind {
        typedef Allocator<U> other;
    };

    Allocator() noexcept {}
    template <typename U>
    Allocator(const Allocator<U>& other) noexcept {}

    T* allocate(size_t n, const void* hint = 0) {
        auto memory_manager = MemoryManager::GetInstance();
        auto r = memory_manager->ApplyMemory(0, n * sizeof(T));
        if (!r) {
            throw MemoryLimitException();
        }
        return static_cast<T*>(malloc(n * sizeof(T)));
    }

    void deallocate(T* p, size_t n) {
        auto memory_manager = MemoryManager::GetInstance();
        memory_manager->ReleaseMemory(0, n * sizeof(T));
        free(p);
    }
};

template <typename T>
class StatefullAllocator {
public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using const_pointer = const T*;
    using void_pointer = void*;
    using const_void_pointer = const void*;

    // 带参数的构造函数
    StatefullAllocator() : id_(0) {}
    StatefullAllocator(int id) : id_(id) {}

    pointer allocate(size_type n) {
        auto memory_manager = MemoryManager::GetInstance();
        auto r = memory_manager->ApplyMemory(id_, n * sizeof(T));
        if (!r) {
            throw MemoryLimitException();
        }
        return static_cast<T*>(malloc(n * sizeof(T)));
    }

    void deallocate(pointer p, size_type n) {
        auto memory_manager = MemoryManager::GetInstance();
        memory_manager->ReleaseMemory(id_, n * sizeof(T));
        free(p);
    } 
private:
    int id_;
};

template <typename T, typename U>
bool operator==(const StatefullAllocator<T>&, const StatefullAllocator<U>&) {
    return true;
}

template <typename T, typename U>
bool operator!=(const StatefullAllocator<T>&, const StatefullAllocator<U>&) {
    return false;
}

}  // namespace intarkdb
