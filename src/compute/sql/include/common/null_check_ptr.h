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
* null_check_ptr.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/null_check_ptr.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <stdexcept>

// 模板类，检查指针是否为空
template <typename T>
class NullCheckPtr {
   public:
    NullCheckPtr() : ptr_(nullptr) {}
    NullCheckPtr(T* p) : ptr_(p) {}
    NullCheckPtr(const std::unique_ptr<T>& p) : ptr_(p.get()) {}

    void CheckNull() const {
        if (!ptr_) {
            throw std::runtime_error("dereference a null pointer");
        }
    }
    operator bool() const { return ptr_; }

    T& operator*() {
        CheckNull();
        return *ptr_;
    }

    const T& operator*() const {
        CheckNull();
        return *ptr_;
    }

    T* operator->() {
        CheckNull();
        return ptr_;
    }

    const T* operator->() const {
        CheckNull();
        return ptr_;
    }

    T* get() { return ptr_; }
    const T* get() const { return ptr_; }

    bool operator==(const NullCheckPtr<T>& rhs) const { return ptr_ == rhs.ptr_; }

    bool operator!=(const NullCheckPtr<T>& rhs) const { return ptr_ != rhs.ptr_; }

   private:
    T* ptr_;
};

template <typename T, typename N>
NullCheckPtr<T> NullCheckPtrCast(N* p) {
    return NullCheckPtr<T>(reinterpret_cast<T*>(p));
}
