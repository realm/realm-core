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
#include <cstring>
#include <type_traits>
#include <utility>

#include <realm/util/assert.hpp>

namespace realm::util {
struct SensitiveBufferBase {
public:
    bool engaged() const
    {
        return m_buffer != nullptr;
    }

    size_t size() const
    {
        return m_size;
    }

    virtual ~SensitiveBufferBase();

    bool operator==(const SensitiveBufferBase& rhs) const
    {
        if (this == &rhs)
            return true;

        if (m_size != rhs.m_size)
            return false;

        if (m_buffer == rhs.m_buffer)
            return true;

        if (m_buffer == nullptr || rhs.m_buffer == nullptr)
            return false;

        return std::memcmp(m_buffer, rhs.m_buffer, m_size) == 0;
    }

    bool operator!=(const SensitiveBufferBase& rhs) const
    {
        return !operator==(rhs);
    }

    static void secure_erase(void*, size_t);

protected:
    SensitiveBufferBase(size_t buffer_size);
    SensitiveBufferBase(const SensitiveBufferBase& other);
    SensitiveBufferBase(SensitiveBufferBase&& other) noexcept;

    template <typename Func>
    void with_unprotected_buffer(Func f) const
    {
        if (!m_buffer) {
            return;
        }

        unprotect();
        f(m_buffer);
        protect();
    }

private:
    const size_t m_size;
    void* m_buffer = nullptr;

    void protect() const;
    void unprotect() const;
};

template <typename T>
struct SensitiveBuffer : private SensitiveBufferBase {
    static_assert(std::is_trivial_v<T>, "SensitiveBuffer can only be used with trivial POD types.");

public:
    using element_type = T;

    struct SelfErasingStorage {
    public:
        const T& operator*() const
        {
            return m_value;
        }

        const T* operator->() const
        {
            return &m_value;
        }

        ~SelfErasingStorage()
        {
            SensitiveBufferBase::secure_erase(&m_value, sizeof(T));
        }

    private:
        friend struct SensitiveBuffer<T>;
        T m_value;
    };

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
        : SensitiveBufferBase(other)
    {
    }

    SensitiveBuffer(SensitiveBuffer<T>&& other)
        : SensitiveBufferBase(other)
    {
    }

    SensitiveBuffer<T>& operator=(const SensitiveBuffer<T>& other)
    {
        if (this != &other) {
            REALM_ASSERT(engaged());
            with_unprotected_buffer([&other](void* buffer) {
                *(reinterpret_cast<T*>(buffer)) = *other.data();
            });
        }
        return *this;
    }

    bool operator==(const SensitiveBuffer<T>& rhs) const
    {
        return SensitiveBufferBase::operator==(rhs);
    }

    bool operator!=(const SensitiveBuffer<T>& rhs) const
    {
        return SensitiveBufferBase::operator!=(rhs);
    }

    SelfErasingStorage data() const
    {
        SelfErasingStorage ret;
        with_unprotected_buffer([&ret](void* buffer) {
            ret.m_value = *reinterpret_cast<T*>(buffer);
        });
        return ret;
    }
};
} // namespace realm::util
