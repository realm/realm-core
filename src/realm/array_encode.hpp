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
class Array;
class ArrayEncode {
public:
    explicit ArrayEncode() = default;
    virtual ~ArrayEncode() = default;
    virtual bool encode(const Array&, Array&) const = 0;
    virtual bool decode(Array&) = 0;
    virtual bool is_encoded(const Array&) const = 0;
    virtual size_t size(const Array&) const = 0;
    virtual int64_t get(const Array&, size_t) const = 0;
    virtual size_t find_first(const Array&, int64_t value) const = 0;
};

} // namespace realm
#endif // REALM_ARRAY_COMPRESS_HPP
