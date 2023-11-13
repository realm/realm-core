/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_ARRAY_COMPRESS_HPP
#define REALM_ARRAY_COMPRESS_HPP

#include <realm/alloc.hpp>

namespace realm {
//
// This class represents the basic interface that every compressed array must implement.
//
class ArrayEncode {
public:
    explicit ArrayEncode() = default;
    virtual ~ArrayEncode() = default;
    virtual void init_array_encode(MemRef) = 0;
    virtual bool encode() = 0;
    virtual bool decode() = 0;
    virtual bool is_encoded() const = 0;

    virtual MemRef get_mem_ref() const = 0;
    virtual size_t size() const = 0;
    virtual int64_t get(size_t) const = 0;
    virtual size_t byte_size() const = 0;
};

class DummyArrayEncode : public ArrayEncode {
public:
    explicit DummyArrayEncode() = default;
    virtual ~DummyArrayEncode() = default;
    void init_array_encode(MemRef) final override {}
    bool encode() final override
    {
        return false;
    }
    bool decode() final override
    {
        return false;
    }
    bool is_encoded() const final override
    {
        return false;
    }
    size_t size() const final override
    {
        return 0;
    }
    int64_t get(size_t) const final override
    {
        return 0;
    }
    MemRef get_mem_ref() const final override
    {
        return MemRef();
    }
    size_t byte_size() const final override
    {
        return 0;
    }
};


} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
