///////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
///////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <mutex>

namespace realm::util {
struct SensitiveBufferBase {
public:
    virtual ~SensitiveBufferBase();

protected:
    SensitiveBufferBase(size_t buffer_size);

    template <typename Func>
    void with_unprotected_buffer(Func f) const
    {
        std::lock_guard<std::mutex> lock(m_protect_mutex);
        unprotect();
        f(m_buffer);
        protect();
    }

private:
    const size_t m_size;
    void* m_buffer;
    mutable std::mutex m_protect_mutex;

    void protect() const;
    void unprotect() const;
};

template <typename T>
struct SensitiveBuffer : private SensitiveBufferBase {
    static_assert(std::is_trivial_v<T>, "SensitiveBuffer can only be used with trivial POD types.");

public:
    using element_type = T;

    SensitiveBuffer()
        : SensitiveBufferBase(sizeof(T))
    {
    }

    explicit SensitiveBuffer(const T& data)
        : SensitiveBufferBase(sizeof(T))
    {
        with_unprotected_buffer([&data](void* buffer) {
            *reinterpret_cast<T*>(buffer) = data;
        });
    }

    explicit SensitiveBuffer(T&& data)
        : SensitiveBufferBase(sizeof(T))
    {
        with_unprotected_buffer([&data](void* buffer) {
            *reinterpret_cast<T*>(buffer) = std::move(data);
        });
    }

    SensitiveBuffer(const SensitiveBuffer<T>& other)
        : SensitiveBuffer(other.data())
    {
    }

    SensitiveBuffer(SensitiveBuffer<T>&& other)
        : SensitiveBuffer(other.data())
    {
        other.~SensitiveBuffer();
    }

    SensitiveBuffer<T>& operator=(const SensitiveBuffer<T>& other)
    {
        if (this != &other) {
            with_unprotected_buffer([&other](void* buffer) {
                *(reinterpret_cast<T*>(buffer)) = other.data();
            });
        }
        return *this;
    }

    SensitiveBuffer<T>& operator=(const T& data)
    {
        with_unprotected_buffer([&data](void* buffer) {
            *reinterpret_cast<T*>(buffer) = data;
        });
    }

    T data() const
    {
        T ret;
        with_unprotected_buffer([&ret](void* buffer) {
            if (buffer != nullptr) {
                ret = *reinterpret_cast<T*>(buffer);
            }
        });
        return ret;
    }
};
} // namespace realm::util